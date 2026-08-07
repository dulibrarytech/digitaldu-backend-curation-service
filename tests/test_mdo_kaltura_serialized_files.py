# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Kaltura serialized-files handling for Make Digital Objects (2026-08-07
fix). Three behaviors under test:

1. The route hands the Kaltura mapping to the CLI via an explicit
   --serialized_files <temp path> argument and no longer drops a
   serialized_files.json into the service's working directory (the old
   relative-path copy that broke Kaltura stamping end-to-end).
2. get_kaltura_id_from_file matches by filename AND, when both sides
   carry one, by package — so identically named files in different
   packages cannot pick up each other's entry IDs.
3. _process_single_file only attaches Kaltura IDs when a mapping path
   was supplied (no implicit script-directory fallback).

The subprocess is faked; no ArchivesSpace or script execution happens.

Run:
    python -m pytest tests/test_mdo_kaltura_serialized_files.py -v
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask

# make_digital_object imports asnake/magic at module import time; stub
# them so the test needs neither installed nor an ASpace connection.
sys.modules.setdefault('asnake', MagicMock())
sys.modules.setdefault('asnake.aspace', MagicMock())
sys.modules.setdefault('magic', MagicMock())

from lib.make_digital_object import get_kaltura_id_from_file  # noqa: E402
from routes.astools import _build_cli_arguments, astools_bp  # noqa: E402


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


class BuildCliArgumentsTests(unittest.TestCase):

    def _base_args(self, serialized_file_path=None):
        return _build_cli_arguments(
            '/scripts/make_digital_object.py',
            'user',
            'pass',
            '/workspace/new_batch-resources_1',
            {
                'no_kaltura': False,
                'no_caption': True,
                'no_publish': False,
                'use_test_server': False,
                'verbose': False,
            },
            serialized_file_path,
        )

    def test_includes_serialized_files_flag_when_path_given(self):
        args = self._base_args(Path('/tmp/astools_x/serialized_files.json'))
        self.assertIn('--serialized_files', args)
        idx = args.index('--serialized_files')
        self.assertEqual(args[idx + 1], '/tmp/astools_x/serialized_files.json')
        # The batch path stays the final positional argument.
        self.assertEqual(args[-1], '/workspace/new_batch-resources_1')

    def test_omits_serialized_files_flag_without_path(self):
        args = self._base_args(None)
        self.assertNotIn('--serialized_files', args)


class KalturaIdLookupTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='mdo_kaltura_'))
        self.mapping = self.tmp / 'serialized_files.json'

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, entries):
        self.mapping.write_text(json.dumps(entries))

    def test_matches_by_filename(self):
        self._write([{'file': 'clip.mov', 'entry_id': '1_abc'}])
        self.assertEqual(
            get_kaltura_id_from_file('clip.mov', str(self.mapping)), '1_abc'
        )

    def test_package_scoping_prevents_cross_package_collision(self):
        self._write([
            {'package': 'pkg_a', 'file': 'clip.mov', 'entry_id': '1_aaa'},
            {'package': 'pkg_b', 'file': 'clip.mov', 'entry_id': '1_bbb'},
        ])
        self.assertEqual(
            get_kaltura_id_from_file('clip.mov', str(self.mapping), package_name='pkg_b'),
            '1_bbb',
        )

    def test_legacy_entries_without_package_still_match(self):
        self._write([{'file': 'clip.mov', 'entry_id': '1_abc'}])
        self.assertEqual(
            get_kaltura_id_from_file('clip.mov', str(self.mapping), package_name='pkg_a'),
            '1_abc',
        )

    def test_missing_mapping_file_returns_none(self):
        self.assertIsNone(
            get_kaltura_id_from_file('clip.mov', str(self.tmp / 'nope.json'))
        )

    def test_unlisted_file_returns_none(self):
        self._write([{'file': 'other.mov', 'entry_id': '1_abc'}])
        self.assertIsNone(get_kaltura_id_from_file('clip.mov', str(self.mapping)))


class MdoRouteSerializedFilesTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='mdo_route_kaltura_'))
        self.workspace = self.tmp / 'workspace'
        (self.workspace / 'new_batch-resources_1').mkdir(parents=True)
        self.script_dir = self.tmp / 'scripts'
        self.script_dir.mkdir()
        (self.script_dir / 'make_digital_object.py').write_text('# stub')
        self.log_dir = self.tmp / 'logs'
        # A directory the route must NOT write serialized_files.json into.
        self.cwd_dir = self.tmp / 'service_cwd'
        self.cwd_dir.mkdir()

        self.client = _make_client()
        self.env = patch.dict(os.environ, {
            'API_KEY': API_KEY,
            'ASTOOLS_API_KEY': API_KEY,
            'WORKSPACE': str(self.workspace),
            'ASPACE_USERNAME': 'user',
            'ASPACE_PASSWORD': 'pass',
            'SCRIPT_PATH': str(self.script_dir),
            'SCRIPT_NAME_PY': 'make_digital_object.py',
            'LOG_PATH': str(self.log_dir),
        })
        self.env.start()
        self._old_cwd = os.getcwd()
        os.chdir(self.cwd_dir)

    def tearDown(self):
        os.chdir(self._old_cwd)
        self.env.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _post(self, data):
        return self.client.post(
            '/api/v1/astools/make-digital-objects',
            json={'data': data},
            headers={'X-API-Key': API_KEY},
        )

    def test_kaltura_run_passes_temp_mapping_path_to_cli(self):
        captured = {}

        def fake_run(cli_args, **kwargs):
            captured['args'] = cli_args
            # The mapping file must exist WITH the payload at exec time.
            idx = cli_args.index('--serialized_files')
            mapping_path = Path(cli_args[idx + 1])
            captured['mapping'] = json.loads(mapping_path.read_text())
            return _fake_completed(0, stdout='ok')

        with patch('routes.astools.subprocess.run', side_effect=fake_run):
            resp = self._post({
                'folder': 'new_batch-resources_1',
                'is_kaltura': 1,
                'files': [
                    {'package': 'pkg_a', 'file': 'clip.mov', 'entry_id': '1_abc'},
                ],
            })

        self.assertEqual(resp.status_code, 200)
        self.assertIn('--serialized_files', captured['args'])
        self.assertEqual(
            captured['mapping'],
            [{'package': 'pkg_a', 'file': 'clip.mov', 'entry_id': '1_abc'}],
        )
        # The old bug: a serialized_files.json dropped into the process
        # CWD (never cleaned up, never read by the script). Must be gone.
        self.assertFalse((self.cwd_dir / 'serialized_files.json').exists())
        # And the temp mapping is cleaned up after the run.
        self.assertFalse(Path(captured['args'][captured['args'].index('--serialized_files') + 1]).exists())

    def test_non_kaltura_run_has_no_serialized_files_flag(self):
        captured = {}

        def fake_run(cli_args, **kwargs):
            captured['args'] = cli_args
            return _fake_completed(0, stdout='ok')

        with patch('routes.astools.subprocess.run', side_effect=fake_run):
            resp = self._post({'folder': 'new_batch-resources_1'})

        self.assertEqual(resp.status_code, 200)
        self.assertNotIn('--serialized_files', captured['args'])
        self.assertFalse((self.cwd_dir / 'serialized_files.json').exists())


if __name__ == '__main__':
    unittest.main()
