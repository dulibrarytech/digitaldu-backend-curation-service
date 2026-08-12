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

import io
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


class TransferConfigForTest(unittest.TestCase):
    """
    Part-size scaling for large streams (2026-08-05 incident: S3 allows
    at most 10,000 multipart parts; boto3's default 8 MiB parts cap any
    non-seekable upload at 80 GiB — part 10,001 is rejected with
    InvalidArgument. boto3 auto-scales only for file uploads, where it
    knows the size; our verified chunk stream must scale explicitly).
    """

    MIB = 1024 * 1024

    def test_unknown_size_returns_none(self):
        self.assertIsNone(wasabi._transfer_config_for(None))
        self.assertIsNone(wasabi._transfer_config_for(0))

    def test_small_sizes_keep_the_default_chunksize(self):
        # 66.16 GB — the production AIP that fit in 7,888 default parts.
        cfg = wasabi._transfer_config_for(66_163_797_416)
        self.assertEqual(cfg.multipart_chunksize, 8 * self.MIB)

    def test_100gb_scales_to_10mib_parts(self):
        cfg = wasabi._transfer_config_for(100_000_000_000)
        self.assertEqual(cfg.multipart_chunksize, 10 * self.MIB)

    def test_part_count_stays_within_10000_across_size_sweep(self):
        # 80 GiB (the old hard ceiling), 100 GB, 250 GB, 1 TB, 5 TB.
        for size in (
            85_899_345_920,
            100_000_000_000,
            250_000_000_000,
            1_000_000_000_000,
            5_000_000_000_000,
        ):
            cfg = wasabi._transfer_config_for(size)
            chunk = cfg.multipart_chunksize
            parts = -(-size // chunk)  # ceil division
            self.assertLessEqual(parts, 10_000, f'size={size}')
            self.assertEqual(chunk % self.MIB, 0, f'size={size}')

    def test_upload_fileobj_passes_scaled_config_to_boto3(self):
        captured = {}

        class FakeClient:
            def upload_fileobj(self, fileobj, bucket, key, Callback=None,
                               Config=None):
                captured['config'] = Config

        with patch.object(wasabi, '_make_client', return_value=FakeClient()), \
                patch.object(config, 'WASABI_BUCKET', 's3://bucket/'):
            wasabi.upload_fileobj(
                io.BytesIO(b'x'), 'k', expected_bytes=100_000_000_000
            )
        self.assertEqual(
            captured['config'].multipart_chunksize, 10 * self.MIB
        )


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
        """
        S3 client fake. upload_file records the object's true size so
        head_object (the phase-1 per-file verification probe) answers
        with the matching ContentLength — the happy path a real
        successful upload produces. Tests that exercise verification
        failure override head_object after construction.
        """
        client = MagicMock()
        stored = {}

        def _upload(local_path, bucket, key, **kwargs):
            stored[key] = os.path.getsize(local_path)

        def _head(Bucket=None, Key=None):  # noqa: N803 - boto3 casing
            return {'ContentLength': stored.get(Key)}

        client.upload_file = MagicMock(side_effect=_upload)
        client.head_object = MagicMock(side_effect=_head)
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
        # Overriding upload_file's side_effect above bypasses the fake's
        # size recording, so answer the verification head from the local
        # tree instead (key 'col-y/<rel>' → file under self.src).
        client.head_object.side_effect = lambda Bucket=None, Key=None: {
            'ContentLength': os.path.getsize(
                os.path.join(self.src, *Key.split('/')[1:])
            )
        }
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.upload_directory(self.src, 'col-y')
        self.assertFalse(result['ok'])
        self.assertEqual(result['uploaded'], 1)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(len(result['errors']), 1)
        self.assertIn('AccessDenied', result['errors'][0])

    def test_verify_size_mismatch_counts_as_failed(self):
        # Phase-1 verification (003-ingested retirement): upload_file
        # "succeeds" but the remote object's size disagrees — e.g. a
        # truncated transfer the SDK didn't surface. The file must
        # count as FAILED so move_to_ingested keeps the local source.
        client = self._fake_client()
        client.head_object.side_effect = lambda Bucket=None, Key=None: {
            'ContentLength': 1  # never matches either fixture file
        }
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.upload_directory(self.src, 'col-v')
        self.assertFalse(result['ok'])
        self.assertEqual(result['uploaded'], 0)
        self.assertEqual(result['verified'], 0)
        self.assertEqual(result['failed'], 2)
        self.assertTrue(any('verify failed' in e for e in result['errors']))

    def test_verify_head_error_counts_as_failed(self):
        # If the verification probe itself errors, the upload is NOT
        # trusted — same failed accounting, distinct error text.
        client = self._fake_client()
        client.head_object.side_effect = ClientError(
            {'Error': {'Code': '500', 'Message': 'oops'}}, 'HeadObject',
        )
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.upload_directory(self.src, 'col-w')
        self.assertFalse(result['ok'])
        self.assertEqual(result['uploaded'], 0)
        self.assertEqual(result['failed'], 2)
        self.assertTrue(any('verify head failed' in e for e in result['errors']))

    def test_happy_path_reports_verified_count(self):
        client = self._fake_client()
        with patch.object(wasabi, '_make_client', return_value=client):
            result = wasabi.upload_directory(self.src, 'col-ok')
        self.assertTrue(result['ok'])
        self.assertEqual(result['verified'], 2)
        # One head per uploaded file — verification is per-file.
        self.assertEqual(client.head_object.call_count, 2)

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


class MakeClientCredentialResolutionTest(unittest.TestCase):
    """
    Pins the credential-resolution priority in `_make_client`:
      1. Env-based access key + secret (the v1 deploy shape)
      2. Named profile (legacy ~/.aws/config path)
      3. Loud failure if neither is set

    Without this, a host that has WASABI_PROFILE set but no actual
    ~/.aws/config for the user (the original bug — interactive
    `sudo -u curation` leaving HOME=/root) raises ProfileNotFound
    deep inside boto3 instead of using the env-based keys that ARE
    available.
    """

    def setUp(self):
        # boto3.Session itself is what we're testing the call shape of.
        # Patch the class so we can assert the kwargs without actually
        # making AWS calls.
        self.session_patcher = patch.object(wasabi, 'boto3')
        self.boto3_mock = self.session_patcher.start()
        # Default to a sensible client return so chained .client(...)
        # in _make_client doesn't AttributeError.
        self.boto3_mock.Session.return_value.client.return_value = MagicMock()

    def tearDown(self):
        self.session_patcher.stop()

    def test_prefers_env_credentials_over_profile(self):
        # Both env keys AND a profile are set — env keys should win.
        # This is the v1 deploy shape: .env has both.
        with patch.object(config, 'AWS_ACCESS_KEY_ID', 'ak-env'), \
             patch.object(config, 'AWS_SECRET_ACCESS_KEY', 'sk-env'), \
             patch.object(config, 'AWS_DEFAULT_REGION', 'us-east-1'), \
             patch.object(config, 'WASABI_PROFILE', 'fernando.reyes'):
            wasabi._make_client()
        call_kwargs = self.boto3_mock.Session.call_args.kwargs
        self.assertEqual(call_kwargs.get('aws_access_key_id'), 'ak-env')
        self.assertEqual(call_kwargs.get('aws_secret_access_key'), 'sk-env')
        self.assertEqual(call_kwargs.get('region_name'), 'us-east-1')
        # CRITICAL: profile_name MUST NOT be passed — that would
        # short-circuit env creds and fail with ProfileNotFound on
        # hosts where ~/.aws/config doesn't have the profile.
        self.assertNotIn('profile_name', call_kwargs)

    def test_falls_back_to_profile_when_env_keys_missing(self):
        with patch.object(config, 'AWS_ACCESS_KEY_ID', ''), \
             patch.object(config, 'AWS_SECRET_ACCESS_KEY', ''), \
             patch.object(config, 'WASABI_PROFILE', 'wasabi-prod'):
            wasabi._make_client()
        call_kwargs = self.boto3_mock.Session.call_args.kwargs
        self.assertEqual(call_kwargs.get('profile_name'), 'wasabi-prod')
        self.assertNotIn('aws_access_key_id', call_kwargs)

    def test_raises_when_neither_creds_nor_profile_configured(self):
        with patch.object(config, 'AWS_ACCESS_KEY_ID', ''), \
             patch.object(config, 'AWS_SECRET_ACCESS_KEY', ''), \
             patch.object(config, 'WASABI_PROFILE', ''):
            with self.assertRaises(RuntimeError) as ctx:
                wasabi._make_client()
            # The error names BOTH env vars and profile so staff
            # know what to set.
            self.assertIn('AWS_ACCESS_KEY_ID', str(ctx.exception))
            self.assertIn('WASABI_PROFILE', str(ctx.exception))

    def test_raises_when_endpoint_missing(self):
        with patch.object(config, 'WASABI_ENDPOINT', ''):
            with self.assertRaises(RuntimeError) as ctx:
                wasabi._make_client()
            self.assertIn('WASABI_ENDPOINT', str(ctx.exception))

    def test_only_partial_env_creds_falls_through_to_profile(self):
        # AWS_ACCESS_KEY_ID without AWS_SECRET_ACCESS_KEY — don't try
        # to use a half-set env, fall back to the profile.
        with patch.object(config, 'AWS_ACCESS_KEY_ID', 'ak'), \
             patch.object(config, 'AWS_SECRET_ACCESS_KEY', ''), \
             patch.object(config, 'WASABI_PROFILE', 'fallback'):
            wasabi._make_client()
        call_kwargs = self.boto3_mock.Session.call_args.kwargs
        self.assertEqual(call_kwargs.get('profile_name'), 'fallback')
        self.assertNotIn('aws_access_key_id', call_kwargs)

    def test_pops_profile_env_vars_through_both_session_and_client(self):
        """
        boto3.Session() AND session.client() both read AWS_PROFILE /
        AWS_DEFAULT_PROFILE from os.environ during init, via the
        same get_scoped_config() path. If the named profile isn't
        in ~/.aws/config either call raises ProfileNotFound.

        _client_from_keys must keep the pop in effect through BOTH
        calls — popping only during Session() leaves client() to
        crash on `get_config_variable('ca_bundle')`.

        This regression bit twice on the live curation host: the
        first pop fixed Session(), then client() failed at the
        ca_bundle config lookup. Pin both points here.
        """
        captured_at_session = {}
        captured_at_client = {}

        def capture_env_at_session(**kwargs):
            captured_at_session['AWS_PROFILE'] = os.environ.get('AWS_PROFILE')
            captured_at_session['AWS_DEFAULT_PROFILE'] = os.environ.get('AWS_DEFAULT_PROFILE')

            def capture_env_at_client(*c_args, **c_kwargs):
                captured_at_client['AWS_PROFILE'] = os.environ.get('AWS_PROFILE')
                captured_at_client['AWS_DEFAULT_PROFILE'] = os.environ.get('AWS_DEFAULT_PROFILE')
                return MagicMock()

            sess = MagicMock()
            sess.client.side_effect = capture_env_at_client
            return sess

        self.boto3_mock.Session.side_effect = capture_env_at_session

        with patch.object(config, 'AWS_ACCESS_KEY_ID', 'ak-env'), \
             patch.object(config, 'AWS_SECRET_ACCESS_KEY', 'sk-env'), \
             patch.dict(os.environ, {
                 'AWS_PROFILE': 'fernando.reyes',
                 'AWS_DEFAULT_PROFILE': 'fernando.reyes',
             }, clear=False):
            wasabi._make_client()
            # Both Session() and client() must see the env vars popped.
            self.assertIsNone(captured_at_session['AWS_PROFILE'])
            self.assertIsNone(captured_at_session['AWS_DEFAULT_PROFILE'])
            self.assertIsNone(captured_at_client['AWS_PROFILE'])
            self.assertIsNone(captured_at_client['AWS_DEFAULT_PROFILE'])
            # And restored on return.
            self.assertEqual(os.environ['AWS_PROFILE'], 'fernando.reyes')
            self.assertEqual(os.environ['AWS_DEFAULT_PROFILE'], 'fernando.reyes')

    def test_does_not_pop_profile_env_when_using_profile_branch(self):
        """The profile branch SHOULD see AWS_PROFILE / AWS_DEFAULT_PROFILE.
        We only pop them on the env-keys branch (where they'd cause a
        false ProfileNotFound on a Session we're constructing with
        explicit creds anyway)."""
        captured = {}

        def capture_env(**kwargs):
            captured['AWS_PROFILE'] = os.environ.get('AWS_PROFILE')
            return MagicMock()

        self.boto3_mock.Session.side_effect = capture_env

        with patch.object(config, 'AWS_ACCESS_KEY_ID', ''), \
             patch.object(config, 'AWS_SECRET_ACCESS_KEY', ''), \
             patch.object(config, 'WASABI_PROFILE', 'wasabi-prod'), \
             patch.dict(os.environ, {'AWS_PROFILE': 'unrelated'}, clear=False):
            wasabi._make_client()
        self.assertEqual(captured['AWS_PROFILE'], 'unrelated')


class BucketRoutingTest(unittest.TestCase):
    """
    Pins the dual-bucket routing introduced by curration-api-modified-5:
    AIP operations target WASABI_AIP_BUCKET; the legacy upload_directory
    + health_check paths still target WASABI_BUCKET.

    Without this contract, AIP uploads would silently land in the
    SFTP-staging bucket on deployments that configure both buckets.
    """

    def setUp(self):
        # Three distinct values so we can tell which bucket each call
        # routed to by inspecting the bucket arg boto3 received.
        self._aip_raw = 's3://aip-bucket/aip-store/'
        self._staging_raw = 's3://staging-bucket/'

    def _fake_client(self):
        c = MagicMock()
        c.upload_fileobj = MagicMock()
        c.head_object = MagicMock(return_value={'ContentLength': 0})
        c.delete_object = MagicMock()
        c.generate_presigned_url = MagicMock(return_value='https://signed.example/')
        return c

    def test_upload_fileobj_routes_to_bucket_config_when_set(self):
        client = self._fake_client()
        with patch.object(config, 'WASABI_BUCKET', self._staging_raw):
            with patch.object(wasabi, '_make_client', return_value=client):
                wasabi.upload_fileobj(
                    MagicMock(),
                    'thing.7z',
                    bucket_config=self._aip_raw,
                )
        # boto3.upload_fileobj(file, bucket, key, ...) — second arg is
        # the bucket, third is the key.
        args = client.upload_fileobj.call_args
        self.assertEqual(args.args[1], 'aip-bucket')
        self.assertEqual(args.args[2], 'aip-store/thing.7z')

    def test_upload_fileobj_falls_back_to_wasabi_bucket_when_override_is_none(self):
        # Backward compat: legacy callers that don't pass bucket_config
        # keep targeting WASABI_BUCKET exactly as they did before.
        client = self._fake_client()
        with patch.object(config, 'WASABI_BUCKET', self._staging_raw):
            with patch.object(wasabi, '_make_client', return_value=client):
                wasabi.upload_fileobj(MagicMock(), 'thing.7z')
        self.assertEqual(client.upload_fileobj.call_args.args[1], 'staging-bucket')

    def test_head_object_honors_bucket_config(self):
        client = self._fake_client()
        with patch.object(config, 'WASABI_BUCKET', self._staging_raw):
            with patch.object(wasabi, '_make_client', return_value=client):
                wasabi.head_object('thing.7z', bucket_config=self._aip_raw)
        call = client.head_object.call_args
        self.assertEqual(call.kwargs['Bucket'], 'aip-bucket')
        self.assertEqual(call.kwargs['Key'], 'aip-store/thing.7z')

    def test_delete_object_honors_bucket_config(self):
        client = self._fake_client()
        with patch.object(config, 'WASABI_BUCKET', self._staging_raw):
            with patch.object(wasabi, '_make_client', return_value=client):
                wasabi.delete_object('thing.7z', bucket_config=self._aip_raw)
        call = client.delete_object.call_args
        self.assertEqual(call.kwargs['Bucket'], 'aip-bucket')
        self.assertEqual(call.kwargs['Key'], 'aip-store/thing.7z')

    def test_presigned_url_honors_bucket_config(self):
        client = self._fake_client()
        with patch.object(config, 'WASABI_BUCKET', self._staging_raw):
            with patch.object(wasabi, '_make_client', return_value=client):
                wasabi.generate_presigned_url(
                    'thing.7z',
                    ttl_seconds=120,
                    bucket_config=self._aip_raw,
                )
        call = client.generate_presigned_url.call_args
        self.assertEqual(call.kwargs['Params']['Bucket'], 'aip-bucket')
        self.assertEqual(call.kwargs['Params']['Key'], 'aip-store/thing.7z')
        self.assertEqual(call.kwargs['ExpiresIn'], 120)

    def test_upload_directory_continues_to_use_wasabi_bucket(self):
        # Regression: the legacy SFTP-staging path must NOT pick up the
        # AIP bucket. upload_directory has no bucket_config param at
        # all; it reads config.WASABI_BUCKET directly.
        import tempfile, shutil
        tmp = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmp, 'a.txt'), 'w') as f:
                f.write('hello')
            client = self._fake_client()
            client.upload_file = MagicMock()
            with patch.object(config, 'WASABI_BUCKET', self._staging_raw):
                with patch.object(config, 'WASABI_AIP_BUCKET', self._aip_raw):
                    with patch.object(wasabi, '_make_client', return_value=client):
                        wasabi.upload_directory(tmp, 'col')
            # upload_file(local, bucket, key, ...) — second arg is bucket.
            self.assertEqual(client.upload_file.call_args.args[1], 'staging-bucket')
        finally:
            shutil.rmtree(tmp)


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
