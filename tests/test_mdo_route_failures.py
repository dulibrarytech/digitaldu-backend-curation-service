# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Route test: /api/v1/astools/make-digital-objects surfaces the CLI's
per-package `FAILED <pkg>: <reason>` summary lines in the response
errors[] when the script exits non-zero (2026-07-27 duplicate-ID fix).

The subprocess is faked; no ArchivesSpace or script execution happens.

Run:
    python -m pytest tests/test_mdo_route_failures.py -v
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask

from routes.astools import astools_bp


API_KEY = 'test-key-123'


def _make_client():
    app = Flask(__name__)
    app.register_blueprint(astools_bp)
    app.testing = True
    return app.test_client()


def _fake_completed(returncode, stdout='', stderr=''):
    proc = MagicMock()
    proc.returncode = returncode
    proc.stdout = stdout
    proc.stderr = stderr
    return proc


class MdoRouteFailureTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='mdo_route_'))
        self.workspace = self.tmp / 'workspace'
        (self.workspace / 'new_batch-resources_1').mkdir(parents=True)
        self.script_dir = self.tmp / 'scripts'
        self.script_dir.mkdir()
        (self.script_dir / 'make_digital_object.py').write_text('# stub')
        self.log_dir = self.tmp / 'logs'

        self.client = _make_client()
        self.env = patch.dict(os.environ, {
            'API_KEY': API_KEY,
            'WORKSPACE': str(self.workspace),
            'ASPACE_USERNAME': 'user',
            'ASPACE_PASSWORD': 'pass',
            'SCRIPT_PATH': str(self.script_dir),
            'SCRIPT_NAME_PY': 'make_digital_object.py',
            'LOG_PATH': str(self.log_dir),
        })
        self.env.start()

    def tearDown(self):
        self.env.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _post_mdo(self):
        return self.client.post(
            '/api/v1/astools/make-digital-objects',
            json={'data': {'folder': 'new_batch-resources_1', 'is_kaltura': 0}},
            headers={'X-API-Key': API_KEY},
        )

    def test_failed_lines_lifted_into_errors(self):
        stdout = (
            'Processing: pkg_a\n'
            'Processing: pkg_b\n'
            '\n'
            '==== MAKE DIGITAL OBJECTS: 1 of 2 package(s) FAILED ====\n'
            'FAILED pkg_b: Multiple objects with component ID "pkg_b" found. '
            'Check ArchivesSpace for more information.\n'
            'Fix the issues above in ArchivesSpace, then run Make Digital Objects again.\n'
        )
        with patch('routes.astools.subprocess.run',
                   return_value=_fake_completed(2, stdout=stdout)):
            res = self._post_mdo()

        self.assertEqual(res.status_code, 500)
        body = res.get_json()
        self.assertFalse(body['result']['success'])
        self.assertEqual(
            body['errors'][0],
            'FAILED pkg_b: Multiple objects with component ID "pkg_b" found. '
            'Check ArchivesSpace for more information.',
        )
        self.assertIn('return code 2', body['errors'][-1])
        # Full output still travels in the body for the collapsible log.
        self.assertIn('MAKE DIGITAL OBJECTS', body['result']['output'])

    def test_nonzero_without_failed_lines_keeps_return_code_error(self):
        with patch('routes.astools.subprocess.run',
                   return_value=_fake_completed(1, stderr='connection refused')):
            res = self._post_mdo()
        self.assertEqual(res.status_code, 500)
        body = res.get_json()
        self.assertEqual(body['errors'], ['Script execution failed with return code 1'])

    def test_clean_run_still_succeeds(self):
        with patch('routes.astools.subprocess.run',
                   return_value=_fake_completed(0, stdout='all good')):
            res = self._post_mdo()
        self.assertEqual(res.status_code, 200)
        self.assertTrue(res.get_json()['result']['success'])


if __name__ == '__main__':
    unittest.main()
