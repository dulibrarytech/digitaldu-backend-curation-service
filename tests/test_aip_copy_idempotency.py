# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the pre-copy idempotency probe in
lib/aip_ops.copy_aip_to_wasabi.

A HEAD that Wasabi answers with 403 proves nothing: a policy without
bucket-wide s3:ListBucket returns 403 for a missing key, and a denied
request returns it for a present one. The copy must neither report
success nor run: it fails fast, copying and deleting nothing, and the
caller retries once access is fixed.

The AM + Wasabi layers are faked at the module boundary; no network.

Run:
    python -m pytest tests/test_aip_copy_idempotency.py -v
"""

import unittest
from unittest.mock import patch

import config
from lib import aip_ops
from lib import wasabi


UUID = '43968b10-18e3-4976-b8ff-3fe9dfaadaf2'
SIZE = 1024


class FakeMetaResponse:
    status_code = 200

    @staticmethod
    def json():
        return {
            'status': 'UPLOADED',
            'size': SIZE,
            'current_path': f'/store/x_{UUID}.7z',
        }


class FakeDownloadResponse:
    """Context-manager shape of requests.get(stream=True)."""
    status_code = 200
    raw = object()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class IdempotencyProbeTests(unittest.TestCase):

    def setUp(self):
        patches = [
            patch.object(config, 'WASABI_AIP_BUCKET', 's3://aip-bucket/aip-store/'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_API', 'https://am:8000/api'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_USERNAME', 'u'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_API_KEY', 'k'),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def _run_copy(self, head):
        """
        Drive copy_aip_to_wasabi with `head` as the probe's answer.
        Returns (result, uploads, deletes); self.am_urls records every
        AM request made.
        """
        uploads = []
        deletes = []
        self.am_urls = []

        def fake_get(url, **kwargs):
            self.am_urls.append(url)
            if url.endswith('/download/'):
                return FakeDownloadResponse()
            return FakeMetaResponse()

        def fake_upload(*args, **kwargs):
            uploads.append(args)
            return {'bucket': 'aip-bucket', 'bytes': SIZE}

        with patch.object(aip_ops.requests, 'get', side_effect=fake_get), \
                patch.object(wasabi, 'head_object', return_value=head), \
                patch.object(wasabi, 'delete_object',
                             side_effect=lambda key, **kw: deletes.append(key)), \
                patch.object(wasabi, 'upload_fileobj', side_effect=fake_upload):
            result = aip_ops.copy_aip_to_wasabi(UUID, 'codu:x')
        return result, uploads, deletes

    def test_forbidden_head_fails_fast_without_copying(self):
        result, uploads, deletes = self._run_copy({
            'exists': None, 'forbidden': True, 'bucket': 'aip-bucket',
            'content_length': None,
        })
        self.assertFalse(result['ok'])
        self.assertIn('403', result['error'])
        self.assertIn('nothing was copied', result['error'])
        self.assertFalse(result.get('idempotent'))
        self.assertEqual(uploads, [])
        self.assertEqual(deletes, [])
        # No AIP download was requested from Archivematica either.
        self.assertFalse(any(u.endswith('/download/') for u in self.am_urls))

    def test_forbidden_error_is_retryable_for_repov2(self):
        # repov2 dead-letters "AM status is DELETED" and budgets
        # "not found in ..."; the 403 error must match neither.
        result, _, _ = self._run_copy({
            'exists': None, 'forbidden': True, 'bucket': 'aip-bucket',
            'content_length': None,
        })
        self.assertNotIn('AM status is DELETED', result['error'])
        self.assertNotIn('not found in', result['error'])

    def test_forbidden_head_is_logged(self):
        with self.assertLogs('lib.aip_ops', level='ERROR') as logs:
            self._run_copy({
                'exists': None, 'forbidden': True, 'bucket': 'aip-bucket',
                'content_length': None,
            })
        self.assertIn('HEAD_FORBIDDEN', '\n'.join(logs.output))

    def test_existing_copy_at_expected_size_is_skipped(self):
        result, uploads, deletes = self._run_copy({
            'exists': True, 'bucket': 'aip-bucket', 'content_length': SIZE,
        })
        self.assertTrue(result['ok'])
        self.assertTrue(result['idempotent'])
        self.assertEqual(result['bytes'], SIZE)
        self.assertEqual(uploads, [])

    def test_existing_copy_of_unknown_size_is_copied_over_not_deleted(self):
        # S3 replaces the object only when the new upload completes, so
        # a delete first would only risk the copy that is there.
        result, uploads, deletes = self._run_copy({
            'exists': True, 'bucket': 'aip-bucket', 'content_length': None,
        })
        self.assertTrue(result['ok'], result.get('error'))
        self.assertFalse(result.get('idempotent'))
        self.assertEqual(deletes, [])
        self.assertEqual(len(uploads), 1)

    def test_size_mismatch_still_deletes_then_copies(self):
        result, uploads, deletes = self._run_copy({
            'exists': True, 'bucket': 'aip-bucket', 'content_length': SIZE - 1,
        })
        self.assertTrue(result['ok'], result.get('error'))
        self.assertEqual(len(deletes), 1)
        self.assertEqual(len(uploads), 1)


if __name__ == '__main__':
    unittest.main()
