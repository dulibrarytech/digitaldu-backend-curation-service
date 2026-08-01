# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the bounded AM-download read timeout in
lib/aip_ops.copy_aip_to_wasabi (2026-07-31 stalled-copy incident: the
open-ended read turned a non-answering AM Storage Service into a
silent multi-hour hang with no log trace).

The AM + Wasabi layers are faked at the module boundary; no network.

Run:
    python -m pytest tests/test_copy_aip_download_timeout.py -v
"""

import unittest
from unittest.mock import patch

import config
from lib import aip_ops
from lib import wasabi


UUID = '43968b10-18e3-4976-b8ff-3fe9dfaadaf2'


class FakeMetaResponse:
    status_code = 200

    @staticmethod
    def json():
        return {
            'status': 'UPLOADED',
            'size': 1024,
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


class DownloadTimeoutTests(unittest.TestCase):

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

    def _run_copy(self):
        """
        Drive copy_aip_to_wasabi with fakes; returns (result, calls)
        where calls is the recorded list of requests.get invocations
        as (url, kwargs).
        """
        calls = []

        def fake_get(url, **kwargs):
            calls.append((url, kwargs))
            if url.endswith('/download/'):
                return FakeDownloadResponse()
            return FakeMetaResponse()

        with patch.object(aip_ops.requests, 'get', side_effect=fake_get), \
                patch.object(wasabi, 'head_object',
                             return_value={'exists': False, 'bucket': 'aip-bucket'}), \
                patch.object(wasabi, 'upload_fileobj',
                             return_value={'bucket': 'aip-bucket', 'bytes': 1024}):
            result = aip_ops.copy_aip_to_wasabi(UUID, 'codu:x')
        return result, calls

    def test_download_uses_bounded_read_timeout(self):
        with patch.object(config, 'AM_DOWNLOAD_READ_TIMEOUT_SECONDS', 21600):
            result, calls = self._run_copy()
        self.assertTrue(result['ok'])
        download_calls = [c for c in calls if c[0].endswith('/download/')]
        self.assertEqual(len(download_calls), 1)
        self.assertEqual(download_calls[0][1]['timeout'], (30, 21600))

    def test_zero_restores_open_ended_read(self):
        with patch.object(config, 'AM_DOWNLOAD_READ_TIMEOUT_SECONDS', 0):
            result, calls = self._run_copy()
        self.assertTrue(result['ok'])
        download_calls = [c for c in calls if c[0].endswith('/download/')]
        self.assertEqual(download_calls[0][1]['timeout'], (30, None))

    def test_config_default_is_six_hours(self):
        self.assertEqual(config.AM_DOWNLOAD_READ_TIMEOUT_SECONDS, 21600)

    def test_attempt_is_visible_in_logs_before_streaming(self):
        """
        Regression for the invisibility problem: the entry marker and
        the download-request line must appear even for attempts that
        never reach the upload phase.
        """
        with patch.object(config, 'AM_DOWNLOAD_READ_TIMEOUT_SECONDS', 21600), \
                self.assertLogs('lib.aip_ops', level='INFO') as logs:
            self._run_copy()
        joined = '\n'.join(logs.output)
        self.assertIn('copy_aip_to_wasabi START', joined)
        self.assertIn('requesting AM download', joined)
        self.assertIn('AM download streaming', joined)


if __name__ == '__main__':
    unittest.main()
