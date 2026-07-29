# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Route tests for GET /api/v2/aip/list-objects (routes/aip.py) — the
flat AIP-store inventory page consumed by repo-backend-v2's
scripts/backfill_aip_sizes.js.

The wasabi layer is faked at the module boundary; no AWS credentials
or network access are needed.

Run:
    python -m pytest tests/test_aip_list_objects.py -v
"""

import os
import unittest
from unittest.mock import patch

from flask import Flask

import config
from routes.aip import aip_bp
from lib import wasabi


API_KEY = 'test-key-123'


def _make_client():
    app = Flask(__name__)
    app.register_blueprint(aip_bp)
    app.testing = True
    return app.test_client()


class AipListObjectsTests(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()
        self.env = patch.dict(os.environ, {'API_KEY': API_KEY})
        self.env.start()
        self.bucket = patch.object(
            config, 'WASABI_AIP_BUCKET', 's3://library-repository/aip-store/'
        )
        self.bucket.start()

    def tearDown(self):
        self.bucket.stop()
        self.env.stop()

    def _get(self, url):
        return self.client.get(url, headers={'X-API-Key': API_KEY})

    def test_requires_api_key(self):
        res = self.client.get('/api/v2/aip/list-objects')
        self.assertEqual(res.status_code, 403)

    def test_lists_one_page_with_key_and_size_only(self):
        fake = {
            'objects': [
                {'name': 'a.7z', 'key': 'a.7z', 'size': 111,
                 'last_modified': '2026-01-01T00:00:00'},
                {'name': 'b.7z', 'key': 'b.7z', 'size': 222,
                 'last_modified': None},
            ],
            'next_token': 'tok-2',
        }
        with patch.object(wasabi, 'list_objects', return_value=fake) as m:
            res = self._get('/api/v2/aip/list-objects')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data['ok'])
        # Projection: key + size only, last_modified dropped.
        self.assertEqual(
            data['objects'],
            [{'key': 'a.7z', 'size': 111}, {'key': 'b.7z', 'size': 222}],
        )
        self.assertEqual(data['next_token'], 'tok-2')
        # Recursive flat listing against the AIP bucket.
        _, kwargs = m.call_args
        self.assertTrue(kwargs['recursive'])
        self.assertEqual(kwargs['bucket_config'], config.WASABI_AIP_BUCKET)
        self.assertIsNone(kwargs['continuation_token'])

    def test_passes_continuation_token(self):
        fake = {'objects': [], 'next_token': None}
        with patch.object(wasabi, 'list_objects', return_value=fake) as m:
            res = self._get('/api/v2/aip/list-objects?token=tok-2')
        self.assertEqual(res.status_code, 200)
        self.assertIsNone(res.get_json()['next_token'])
        _, kwargs = m.call_args
        self.assertEqual(kwargs['continuation_token'], 'tok-2')

    def test_wasabi_failure_returns_ok_false_envelope(self):
        with patch.object(wasabi, 'list_objects', side_effect=RuntimeError('boom')):
            res = self._get('/api/v2/aip/list-objects')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertFalse(data['ok'])
        self.assertIn('boom', data['error'])

    def test_unconfigured_bucket_refuses_cleanly(self):
        with patch.object(config, 'WASABI_AIP_BUCKET', ''):
            res = self._get('/api/v2/aip/list-objects')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertFalse(data['ok'])
        self.assertIn('WASABI_AIP_BUCKET', data['error'])


if __name__ == '__main__':
    unittest.main()
