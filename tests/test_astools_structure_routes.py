# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Route tests for the structure-QA response shapes (feature-batch-packaging-qa).

Covers:
  * GET /api/v1/astools/workspace — returns batch OBJECTS (name, packages,
    processed, structure_errors) and includes malformed batches the legacy
    flat-name scan silently skipped.
  * GET /api/v1/astools/workspace/packages — keeps `result` as a name array
    (backward compat) and piggybacks `processed` + `structure_errors`.

Run:
    python -m pytest tests/test_astools_structure_routes.py -v
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

from routes.astools import astools_bp


API_KEY = 'test-key-123'


def _make_client():
    app = Flask(__name__)
    app.register_blueprint(astools_bp)
    app.testing = True
    return app.test_client()


def _touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('x')


class WorkspaceRouteTests(unittest.TestCase):

    def setUp(self):
        self.workspace = Path(tempfile.mkdtemp(prefix='astools_route_ws_'))
        self.client = _make_client()
        self.env = patch.dict(os.environ, {
            'API_KEY': API_KEY,
            'WORKSPACE': str(self.workspace),
        })
        self.env.start()

    def tearDown(self):
        self.env.stop()
        shutil.rmtree(self.workspace, ignore_errors=True)

    def _get(self, url):
        return self.client.get(url, headers={'X-API-Key': API_KEY})

    def test_workspace_returns_batch_objects_with_flags(self):
        # Clean unprocessed batch.
        _touch(self.workspace / 'new_a-resources_1' / 'pkg' / 'f.tif')
        # Loose-files-only batch — legacy scan made this invisible (F1).
        _touch(self.workspace / 'new_b-resources_2' / 'stray.tif')
        # Fully processed — must NOT appear here.
        _touch(self.workspace / 'new_c-resources_3' / 'pkg' / 'uri.txt')
        _touch(self.workspace / 'new_c-resources_3' / 'pkg' / 'f.tif')

        res = self._get('/api/v1/astools/workspace')
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body['errors'], [])

        names = [b['name'] for b in body['result']]
        self.assertEqual(names, ['new_a-resources_1', 'new_b-resources_2'])

        by_name = {b['name']: b for b in body['result']}
        clean = by_name['new_a-resources_1']
        self.assertEqual(clean['packages'], ['pkg'])
        self.assertEqual(clean['processed'], [])
        self.assertEqual(clean['structure_errors'], [])

        flagged = by_name['new_b-resources_2']
        codes = {f['code'] for f in flagged['structure_errors']}
        self.assertEqual(codes, {'no_packages', 'loose_files'})

    def test_workspace_requires_api_key(self):
        res = self.client.get('/api/v1/astools/workspace')
        self.assertEqual(res.status_code, 401)

    def test_workspace_missing_env_is_500(self):
        with patch.dict(os.environ, {'WORKSPACE': ''}):
            res = self._get('/api/v1/astools/workspace')
        self.assertEqual(res.status_code, 500)

    def test_packages_endpoint_piggybacks_flags(self):
        batch = 'new_a-resources_1'
        _touch(self.workspace / batch / 'pkg_a' / 'f.tif')
        _touch(self.workspace / batch / 'loose.tif')
        (self.workspace / batch / 'pkg_b').mkdir()

        res = self._get(f'/api/v1/astools/workspace/packages?batch={batch}')
        self.assertEqual(res.status_code, 200)
        body = res.get_json()

        # Backward-compatible: result stays a plain sorted name array.
        self.assertEqual(body['result'], ['pkg_a', 'pkg_b'])
        self.assertEqual(body['processed'], [])
        codes = {f['code'] for f in body['structure_errors']}
        self.assertEqual(codes, {'loose_files', 'empty_package'})

    def test_packages_endpoint_404_for_missing_batch(self):
        res = self._get('/api/v1/astools/workspace/packages?batch=nope')
        self.assertEqual(res.status_code, 404)


if __name__ == '__main__':
    unittest.main()
