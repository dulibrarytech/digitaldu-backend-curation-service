# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Regression tests for the qa-route command-injection / path-traversal fix.

Two layers are under test:
  1. routes/qa.py validates folder/uuid/package as safe path segments and
     returns 400 before touching the ops layer.
  2. lib/archivematica_ops.py runs cp/rm/chown via subprocess.run([...],
     shell=False) with a `--` end-of-options marker, so a folder name can
     never be interpreted as a shell command or a CLI flag.

Run:
    python -m pytest tests/test_qa_command_injection.py -v
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from lib import archivematica_ops as ops
from lib.safe_names import is_safe_segment, validate_segment
from routes.qa import qa_bp


API_KEY = 'test-key-123'


def _make_client():
    app = Flask(__name__)
    app.register_blueprint(qa_bp)
    app.testing = True
    return app.test_client()


def _ok_run():
    """A subprocess.run replacement that reports success."""
    cp = MagicMock()
    cp.returncode = 0
    cp.stdout = ''
    cp.stderr = ''
    return cp


# --- Layer 0: the validator itself ------------------------------------------

class SafeSegmentTests(unittest.TestCase):

    def test_accepts_real_world_names(self):
        for ok in ('new_collection', 'abc-123-def', 'pkg.001',
                   'codu_55555', 'a b c', 'Müller_2026'):
            self.assertTrue(is_safe_segment(ok), ok)
            self.assertIsNone(validate_segment(ok, 'folder'), ok)

    def test_rejects_traversal_and_separators(self):
        for bad in ('..', '.', '../etc', 'a/b', 'a\\b', '/abs', '\\abs'):
            self.assertFalse(is_safe_segment(bad), bad)
            self.assertIsNotNone(validate_segment(bad, 'folder'), bad)

    def test_rejects_leading_dash_and_control_and_empty(self):
        for bad in ('-rf', '--no-preserve-root', '', 'x\x00y', 'a\nb'):
            self.assertFalse(is_safe_segment(bad), repr(bad))

    def test_rejects_none_and_non_str(self):
        self.assertIsNotNone(validate_segment(None, 'uuid'))
        self.assertFalse(is_safe_segment(None))
        self.assertFalse(is_safe_segment(123))

    def test_rejects_overlong(self):
        self.assertFalse(is_safe_segment('a' * 256))
        self.assertTrue(is_safe_segment('a' * 255))


# --- Layer 1: route boundary rejects bad params ------------------------------

class QaRouteValidationTests(unittest.TestCase):

    def setUp(self):
        os.environ['API_KEY'] = API_KEY
        self.client = _make_client()
        self.headers = {'X-API-Key': API_KEY}

    def _get(self, path):
        return self.client.get(path, headers=self.headers)

    def test_move_to_ingested_rejects_shell_and_traversal(self):
        # A classic injection payload contains '/' (and often spaces) — the
        # separator alone is enough to reject it at the boundary.
        with patch.object(ops, 'move_to_ingested') as m:
            for bad in ('../../etc', 'a/b', '%2e%2e/x', 'x;rm -rf /srv'):
                r = self._get('/api/v2/qa/move-to-ingested'
                              '?uuid=abc-123&folder=' + bad)
                self.assertEqual(r.status_code, 400, bad)
            m.assert_not_called()

    def test_move_to_ingested_rejects_empty_after_new_strip(self):
        with patch.object(ops, 'move_to_ingested') as m:
            r = self._get('/api/v2/qa/move-to-ingested?uuid=abc-123&folder=new_')
            self.assertEqual(r.status_code, 400)
            m.assert_not_called()

    def test_move_to_ingested_accepts_valid_and_calls_ops(self):
        with patch.object(ops, 'move_to_ingested',
                          return_value={'result': 'ok', 'errors': []}) as m:
            r = self._get('/api/v2/qa/move-to-ingested'
                          '?uuid=abc-123&folder=new_coll')
            self.assertEqual(r.status_code, 200)
            m.assert_called_once_with('abc-123', 'new_coll')

    def test_move_to_ingested_collection_sentinel_is_resolved_then_validated(self):
        with patch.object(ops, 'get_collection_folder_name',
                          return_value='new_realfolder'), \
             patch.object(ops, 'move_to_ingested',
                          return_value={'result': 'ok', 'errors': []}) as m:
            r = self._get('/api/v2/qa/move-to-ingested'
                          '?uuid=abc-123&folder=collection')
            self.assertEqual(r.status_code, 200)
            m.assert_called_once_with('abc-123', 'new_realfolder')

    def test_reset_permissions_rejects_bad_folder(self):
        with patch.object(ops, 'reset_permissions') as m:
            for bad in ('-rf', '../x', 'a/b'):
                r = self._get('/api/v2/qa/reset_permissions?folder=' + bad)
                self.assertEqual(r.status_code, 400, bad)
            m.assert_not_called()

    def test_move_to_ingest_and_rollback_reject_traversal(self):
        with patch.object(ops, 'move_to_ingest') as m1, \
             patch.object(ops, 'move_from_ingest_to_ready') as m2:
            r1 = self._get('/api/v2/qa/move-to-ingest'
                           '?uuid=ok&folder=../x&package=p')
            self.assertEqual(r1.status_code, 400)
            r2 = self._get('/api/v2/qa/move-from-ingest-to-ready'
                           '?uuid=ok&folder=f&package=../p')
            self.assertEqual(r2.status_code, 400)
            m1.assert_not_called()
            m2.assert_not_called()

    def test_auth_still_required(self):
        # No X-API-Key header -> 403 (unchanged behavior), not a validation 400.
        r = self.client.get('/api/v2/qa/reset_permissions?folder=new_coll')
        self.assertEqual(r.status_code, 403)


# --- Layer 2: the ops sinks use argv lists, never a shell --------------------

class SubprocessSinkTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='curation_cmdinj_')
        self.ready = os.path.join(self.tmp, 'ready') + '/'
        self.ingest = os.path.join(self.tmp, 'ingest') + '/'
        self.ingested = os.path.join(self.tmp, 'ingested') + '/'
        for d in (self.ready, self.ingest, self.ingested):
            os.makedirs(d)
        self._patches = [
            patch.object(ops, 'ready_path', self.ready),
            patch.object(ops, 'ingest_path', self.ingest),
            patch.object(ops, 'ingested_path', self.ingested),
            patch.object(ops, 'uid', '1000'),
            patch.object(ops, 'gid', '1000'),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _assert_no_shell(self, mock_run):
        self.assertTrue(mock_run.called)
        for call in mock_run.call_args_list:
            argv = call.args[0]
            self.assertIsInstance(argv, list, 'argv must be a list, not a shell string')
            self.assertTrue(all(isinstance(a, str) for a in argv))
            self.assertNotEqual(call.kwargs.get('shell'), True)
            self.assertIn('--', argv, 'expected an end-of-options marker')

    @patch('lib.archivematica_ops.subprocess.run')
    def test_reset_permissions_uses_chown_argv(self, mock_run):
        mock_run.return_value = _ok_run()
        ops.reset_permissions('new_coll')
        self._assert_no_shell(mock_run)
        argv = mock_run.call_args_list[0].args[0]
        self.assertEqual(argv[0], 'chown')
        # The folder name is a single literal operand, not concatenated text.
        self.assertIn(self.ready + 'new_coll', argv)

    @patch('lib.archivematica_ops.move_to_s3', return_value=0)
    @patch('lib.archivematica_ops.subprocess.run')
    def test_move_to_ingested_exists_branch_uses_cp_argv(self, mock_run, _m_s3):
        mock_run.return_value = _ok_run()
        # 'exists' branch: ingested/<folder-without-new_> already present.
        os.makedirs(os.path.join(self.ingested, 'coll'))
        src = os.path.join(self.ingest, 'uuid-xyz')
        os.makedirs(src)
        with open(os.path.join(src, 'image.tif'), 'w') as f:
            f.write('bytes')

        result = ops.move_to_ingested('uuid-xyz', 'new_coll')

        self._assert_no_shell(mock_run)
        commands = [c.args[0][0] for c in mock_run.call_args_list]
        self.assertIn('cp', commands)     # per-file copy into ingested/
        self.assertIn('chown', commands)  # via reset_permissions()
        self.assertEqual(result['result'], 'packages_moved_to_ingested_folder')

    @patch('lib.archivematica_ops.move_to_s3', return_value=0)
    @patch('lib.archivematica_ops.subprocess.run')
    def test_injection_payload_is_passed_as_one_literal_arg(self, mock_run, _m_s3):
        # Even if validation were bypassed, a metacharacter-laden name must
        # arrive as a single argv element — never split or interpreted.
        mock_run.return_value = _ok_run()
        os.makedirs(os.path.join(self.ingested, 'coll'))
        src = os.path.join(self.ingest, 'uuid;reboot')
        os.makedirs(src)
        with open(os.path.join(src, 'f.txt'), 'w') as f:
            f.write('x')

        ops.move_to_ingested('uuid;reboot', 'new_coll')

        for call in mock_run.call_args_list:
            argv = call.args[0]
            if argv[0] == 'cp':
                # the source path is one element containing the literal ';'
                self.assertTrue(any('uuid;reboot' in a for a in argv))
                self.assertTrue(any(';' in a for a in argv))


if __name__ == '__main__':
    unittest.main()
