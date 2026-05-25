# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Unit tests for lib/wasabi.py.

boto3 is mocked at the `_make_client` boundary so the tests don't need
actual AWS creds or network. The tests pin behavior of the new
upload_directory / health_check API + the bucket-name parser, plus
the data-loss-bug fix in archivematica_ops.move_to_ingested (verified
indirectly via the move_to_s3 shim's 0/1 contract).
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError, ProfileNotFound

# Stub the env vars BEFORE config / wasabi imports so module-level
# config reads see real (test) values rather than None.
os.environ.setdefault('API_KEY', 'test-key')
os.environ.setdefault('WASABI_ENDPOINT', 'https://s3.test.example')
os.environ.setdefault('WASABI_BUCKET', 's3://test-bucket/')
os.environ.setdefault('WASABI_PROFILE', 'test-profile')

import config  # noqa: E402
from lib import wasabi  # noqa: E402


class ParseBucketTest(unittest.TestCase):

    def test_strips_s3_scheme_and_trailing_slash(self):
        # `WASABI_BUCKET` is historically stored in CLI form
        # (`s3://name/`) for the legacy aws CLI shellout. boto3 needs
        # just the bare bucket name; the parser should strip both.
        bucket, prefix = wasabi._parse_bucket('s3://my-bucket/')
        self.assertEqual(bucket, 'my-bucket')
        self.assertEqual(prefix, '')

    def test_handles_bare_bucket_name(self):
        bucket, prefix = wasabi._parse_bucket('my-bucket')
        self.assertEqual(bucket, 'my-bucket')
        self.assertEqual(prefix, '')

    def test_extracts_base_prefix_when_present(self):
        # If staff put a path under the bucket (e.g.
        # `s3://archive/2026/`), treat it as a base prefix that
        # gets prepended to every key.
        bucket, prefix = wasabi._parse_bucket('s3://archive/2026/')
        self.assertEqual(bucket, 'archive')
        self.assertEqual(prefix, '2026/')

    def test_raises_on_empty(self):
        with self.assertRaises(RuntimeError):
            wasabi._parse_bucket('')
        with self.assertRaises(RuntimeError):
            wasabi._parse_bucket(None)


class UploadDirectoryTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        # Build a small source tree:
        #   src/file_a.txt
        #   src/sub/file_b.txt
        #   src/.DS_Store     (dotfile — should be skipped)
        self.src = os.path.join(self.tmp, 'src')
        os.makedirs(os.path.join(self.src, 'sub'))
        with open(os.path.join(self.src, 'file_a.txt'), 'w') as f:
            f.write('alpha')
        with open(os.path.join(self.src, 'sub', 'file_b.txt'), 'w') as f:
            f.write('beta')
        with open(os.path.join(self.src, '.DS_Store'), 'w') as f:
            f.write('junk')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _fake_client(self):
        client = MagicMock()
        client.upload_file = MagicMock(return_value=None)
        return client

    def test_happy_path_uploads_all_files_skipping_dotfiles(self):
        client = self._fake_client()
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.upload_directory(self.src, 'collection-x')
        self.assertTrue(result['ok'])
        self.assertEqual(result['uploaded'], 2)  # dotfile excluded
        self.assertEqual(result['failed'], 0)
        # Bytes match the file contents (5 + 4).
        self.assertEqual(result['bytes'], 9)
        # Each upload_file call hit the right bucket + key.
        calls = client.upload_file.call_args_list
        self.assertEqual(len(calls), 2)
        keys = sorted(c.args[2] for c in calls)
        self.assertEqual(
            keys,
            ['collection-x/file_a.txt', 'collection-x/sub/file_b.txt'],
        )
        for c in calls:
            self.assertEqual(c.args[1], 'test-bucket')

    def test_empty_folder_arg_uploads_into_bucket_root(self):
        # The second branch of move_to_ingested calls move_to_s3 with
        # folder=''; keys should not gain a leading '/' from the join.
        client = self._fake_client()
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.upload_directory(self.src, '')
        self.assertTrue(result['ok'])
        keys = sorted(c.args[2] for c in client.upload_file.call_args_list)
        self.assertEqual(keys, ['file_a.txt', 'sub/file_b.txt'])

    def test_one_file_failure_marks_not_ok_but_continues(self):
        # Partial failure: first upload OK, second raises. We log +
        # count + continue (best-effort) and return ok=False so the
        # caller (move_to_ingested) preserves the local source.
        client = self._fake_client()
        failure = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'denied'}},
            'PutObject',
        )
        client.upload_file.side_effect = [None, failure]
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.upload_directory(self.src, 'col-y')
        self.assertFalse(result['ok'])
        self.assertEqual(result['uploaded'], 1)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(len(result['errors']), 1)
        self.assertIn('AccessDenied', result['errors'][0])

    def test_missing_source_dir_returns_error_without_raising(self):
        with patch.object(wasabi, '_make_client', return_value=self._fake_client()):
            result = wasabi.upload_directory(os.path.join(self.tmp, 'nope'), 'x')
        self.assertFalse(result['ok'])
        self.assertEqual(result['uploaded'], 0)
        self.assertEqual(result['failed'], 0)
        self.assertTrue(any('source not found' in e for e in result['errors']))

    def test_empty_source_dir_returns_not_ok(self):
        # An empty directory should NOT be silently treated as a
        # successful no-op. The caller would interpret ok=True as
        # "safe to rmtree", and we'd quietly archive zero packages.
        empty = os.path.join(self.tmp, 'empty')
        os.makedirs(empty)
        with patch.object(wasabi, '_make_client', return_value=self._fake_client()):
            result = wasabi.upload_directory(empty, 'col-z')
        self.assertFalse(result['ok'])
        self.assertEqual(result['uploaded'], 0)
        self.assertEqual(result['failed'], 0)

    def test_base_prefix_from_bucket_url_is_prepended(self):
        # WASABI_BUCKET like `s3://archive/2026/` should put files at
        # `2026/<folder>/<rel>`.
        client = self._fake_client()
        with patch.object(config, 'WASABI_BUCKET', 's3://archive/2026/'):
            with patch.object(wasabi, '_make_client', return_value=client):
                result = wasabi.upload_directory(self.src, 'col-q')
        self.assertTrue(result['ok'])
        keys = sorted(c.args[2] for c in client.upload_file.call_args_list)
        self.assertEqual(
            keys,
            ['2026/col-q/file_a.txt', '2026/col-q/sub/file_b.txt'],
        )


class HealthCheckTest(unittest.TestCase):

    def test_ok_when_head_bucket_succeeds(self):
        client = MagicMock()
        client.head_bucket = MagicMock(return_value={})
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.health_check()
        self.assertTrue(result['ok'])
        self.assertEqual(result['bucket'], 'test-bucket')
        self.assertIsNone(result['error'])
        client.head_bucket.assert_called_once_with(Bucket='test-bucket')

    def test_returns_error_on_missing_profile(self):
        with patch.object(wasabi, '_make_client', side_effect=ProfileNotFound(profile='nope')):
            result = wasabi.health_check()
        self.assertFalse(result['ok'])
        self.assertIn('WASABI_PROFILE', result['error'])

    def test_returns_error_on_client_error_403(self):
        client = MagicMock()
        client.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '403', 'Message': 'Forbidden'}},
            'HeadBucket',
        )
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.health_check()
        self.assertFalse(result['ok'])
        self.assertIn('403', result['error'])

    def test_returns_error_on_missing_config_without_raising(self):
        # WASABI_BUCKET unset → _parse_bucket raises RuntimeError.
        # health_check must NOT propagate; it returns ok=False with
        # the error text so the startup probe can log + continue.
        with patch.object(config, 'WASABI_BUCKET', ''):
            result = wasabi.health_check()
        self.assertFalse(result['ok'])
        self.assertIn('WASABI_BUCKET', result['error'])


class ArchivematicaShimTest(unittest.TestCase):
    """
    Pins the move_to_s3 shim's 0/1 return contract. The caller in
    move_to_ingested branches on `move_result != 0` to decide whether
    to shutil.rmtree the local source — this contract is what fixes
    the prior data-loss bug.
    """

    def test_returns_zero_on_full_success(self):
        from lib import archivematica_ops
        fake_result = {
            'ok': True, 'uploaded': 3, 'failed': 0,
            'bytes': 100, 'elapsed_ms': 50, 'errors': [],
        }
        with patch.object(archivematica_ops.wasabi, 'upload_directory',
                          return_value=fake_result):
            rc = archivematica_ops.move_to_s3('/some/src', 'folder-x')
        self.assertEqual(rc, 0)

    def test_returns_one_on_partial_failure(self):
        from lib import archivematica_ops
        fake_result = {
            'ok': False, 'uploaded': 1, 'failed': 2,
            'bytes': 100, 'elapsed_ms': 50,
            'errors': ['file_b.txt: ClientError AccessDenied'],
        }
        with patch.object(archivematica_ops.wasabi, 'upload_directory',
                          return_value=fake_result):
            rc = archivematica_ops.move_to_s3('/some/src', 'folder-x')
        self.assertEqual(rc, 1)

    def test_returns_one_on_config_error(self):
        # Missing WASABI_PROFILE etc — upload_directory raises
        # RuntimeError. The shim catches it and returns 1 so the
        # route handler can record the error without crashing.
        from lib import archivematica_ops
        with patch.object(archivematica_ops.wasabi, 'upload_directory',
                          side_effect=RuntimeError('WASABI_PROFILE missing')):
            rc = archivematica_ops.move_to_s3('/some/src', 'folder-x')
        self.assertEqual(rc, 1)


if __name__ == '__main__':
    unittest.main()
