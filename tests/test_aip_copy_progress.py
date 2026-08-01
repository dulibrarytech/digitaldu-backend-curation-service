# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the AIP copy-progress plumbing:

  - lib/aip_ops progress-file helpers (write/read/clear, atomicity via
    os.replace, strict-UUID filename guard)
  - lib/wasabi._FileProgress optional hook (throttle, milestone fires,
    error swallowing)
  - GET /api/v2/aip/copy-progress/<aip_uuid> route (auth, 400 on a
    non-UUID, 404 with no file, 200 with the file's contents)

Run:
    python -m pytest tests/test_aip_copy_progress.py -v
"""

import os
import tempfile
import unittest
from unittest.mock import patch

from flask import Flask

from lib import aip_ops
from lib import wasabi
from routes.aip import aip_bp


API_KEY = 'test-key-123'
UUID = '43968b10-18e3-4976-b8ff-3fe9dfaadaf2'


class ProgressFileTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.gettempdir = patch.object(
            tempfile, 'gettempdir', return_value=self.tmp.name
        )
        self.gettempdir.start()

    def tearDown(self):
        self.gettempdir.stop()
        self.tmp.cleanup()

    def test_roundtrip_write_read_clear(self):
        aip_ops.write_copy_progress(UUID, 1024, 4096)
        progress = aip_ops.read_copy_progress(UUID)
        self.assertEqual(progress['bytes_sent'], 1024)
        self.assertEqual(progress['total_bytes'], 4096)
        self.assertEqual(progress['aip_uuid'], UUID)
        self.assertIsInstance(progress['updated_at'], int)
        aip_ops.clear_copy_progress(UUID)
        self.assertIsNone(aip_ops.read_copy_progress(UUID))

    def test_read_returns_none_when_no_file(self):
        self.assertIsNone(aip_ops.read_copy_progress(UUID))

    def test_clear_tolerates_missing_file(self):
        aip_ops.clear_copy_progress(UUID)  # must not raise

    def test_overwrite_leaves_no_tmp_debris(self):
        aip_ops.write_copy_progress(UUID, 10, 100)
        aip_ops.write_copy_progress(UUID, 90, 100)
        self.assertEqual(aip_ops.read_copy_progress(UUID)['bytes_sent'], 90)
        progress_dir = os.path.join(self.tmp.name, 'aip-copy-progress')
        self.assertEqual(
            sorted(os.listdir(progress_dir)), [f'{UUID}.json']
        )

    def test_non_uuid_is_refused_everywhere(self):
        for bad in ('', None, '../etc/passwd', 'abc', f'{UUID}/x'):
            self.assertIsNone(aip_ops._progress_path(bad))
            aip_ops.write_copy_progress(bad, 1, 2)   # no-ops, no raise
            self.assertIsNone(aip_ops.read_copy_progress(bad))
            aip_ops.clear_copy_progress(bad)

    def test_uppercase_uuid_normalized(self):
        aip_ops.write_copy_progress(UUID.upper(), 5, 10)
        self.assertEqual(aip_ops.read_copy_progress(UUID)['bytes_sent'], 5)


class FileProgressHookTests(unittest.TestCase):

    def test_hook_fires_on_milestones_and_throttle(self):
        calls = []
        cb = wasabi._FileProgress(
            'k', 100, hook=lambda s, t: calls.append((s, t)),
            hook_interval_s=9999,  # throttle never elapses in-test
        )
        cb(10)   # 10% — no milestone, throttled — but the FIRST call
        # always fires (hook_last starts as None) so the progress
        # surface goes live with the first chunk.
        self.assertEqual(calls, [(10, 100)])
        cb(10)   # 20% — throttled, no milestone
        self.assertEqual(len(calls), 1)
        cb(10)   # 30% — passes the 25 milestone → fires
        self.assertEqual(calls[-1], (30, 100))
        cb(70)   # 100% — passes 50/75/100 → fires once (coalesced)
        self.assertEqual(calls[-1], (100, 100))

    def test_hook_errors_are_swallowed(self):
        def boom(_s, _t):
            raise RuntimeError('progress sink died')
        cb = wasabi._FileProgress('k', 100, hook=boom, hook_interval_s=0)
        cb(50)  # must not raise
        cb(50)

    def test_no_hook_still_logs_milestones(self):
        cb = wasabi._FileProgress('k', 100)
        cb(100)  # must not raise


class CopyProgressRouteTests(unittest.TestCase):

    def setUp(self):
        app = Flask(__name__)
        app.register_blueprint(aip_bp)
        app.testing = True
        self.client = app.test_client()
        self.env = patch.dict(os.environ, {'API_KEY': API_KEY})
        self.env.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.gettempdir = patch.object(
            tempfile, 'gettempdir', return_value=self.tmp.name
        )
        self.gettempdir.start()

    def tearDown(self):
        self.gettempdir.stop()
        self.tmp.cleanup()
        self.env.stop()

    def _get(self, uuid):
        return self.client.get(
            f'/api/v2/aip/copy-progress/{uuid}',
            headers={'X-API-Key': API_KEY},
        )

    def test_requires_api_key(self):
        res = self.client.get(f'/api/v2/aip/copy-progress/{UUID}')
        self.assertEqual(res.status_code, 403)

    def test_rejects_non_uuid(self):
        res = self._get('not-a-uuid')
        self.assertEqual(res.status_code, 400)

    def test_404_when_no_active_copy(self):
        res = self._get(UUID)
        self.assertEqual(res.status_code, 404)
        self.assertFalse(res.get_json()['ok'])

    def test_200_with_progress(self):
        aip_ops.write_copy_progress(UUID, 2048, 8192)
        res = self._get(UUID)
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data['ok'])
        self.assertEqual(data['bytes_sent'], 2048)
        self.assertEqual(data['total_bytes'], 8192)
        self.assertEqual(data['aip_uuid'], UUID)
        self.assertIsInstance(data['updated_at'], int)


if __name__ == '__main__':
    unittest.main()
