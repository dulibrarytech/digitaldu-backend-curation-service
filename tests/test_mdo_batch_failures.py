# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the Make Digital Objects duplicate-ID surfacing fix
(2026-07-27, "option 1"): the batch loop collects per-package failures,
prints a FAILED summary to stdout, and main() exits non-zero — so the
curation route returns its failure envelope and the dashboard records a
FAILED job instead of a green card.

Historically the loop swallowed per-package exceptions (duplicate
component IDs, uncataloged items) and exited 0; staff discovered the
missing uri.txt only at Description QA.

Run:
    python -m pytest tests/test_mdo_batch_failures.py -v
"""

import io
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

# make_digital_object imports asnake/magic at module import time; stub
# them so the test needs neither installed nor an ASpace connection.
import sys

sys.modules.setdefault('asnake', MagicMock())
sys.modules.setdefault('asnake.aspace', MagicMock())
sys.modules.setdefault('magic', MagicMock())

from lib import make_digital_object as mdo  # noqa: E402


class ProcessBatchTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='mdo_batch_'))
        for name in ('pkg_a', 'pkg_b', 'pkg_c'):
            (self.tmp / name).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_collects_failures_and_continues(self):
        calls = []

        def fake_process(path, *_args):
            calls.append(path.name)
            if path.name == 'pkg_b':
                raise mdo.DigitalObjectException(
                    'Multiple objects with component ID "pkg_b" found. '
                    'Check ArchivesSpace for more information.'
                )

        with patch.object(mdo, 'process', side_effect=fake_process):
            failures, total = mdo.process_batch(self.tmp, False, True, False)

        # All three packages attempted — one failure doesn't stop the run.
        self.assertEqual(calls, ['pkg_a', 'pkg_b', 'pkg_c'])
        self.assertEqual(total, 3)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0][0], 'pkg_b')
        self.assertIn('Multiple objects with component ID', failures[0][1])

    def test_clean_run_returns_no_failures(self):
        with patch.object(mdo, 'process', return_value=None):
            failures, total = mdo.process_batch(self.tmp, False, True, False)
        self.assertEqual(failures, [])
        self.assertEqual(total, 3)

    def test_empty_batch_dir(self):
        empty = self.tmp / 'nothing_here'
        empty.mkdir()
        failures, total = mdo.process_batch(empty, False, True, False)
        self.assertEqual((failures, total), ([], 0))


class ReportBatchFailuresTests(unittest.TestCase):

    def _capture(self, failures, total):
        buf = io.StringIO()
        with redirect_stdout(buf):
            mdo.report_batch_failures(failures, total)
        return buf.getvalue()

    def test_summary_lines_are_stdout_with_failed_prefix(self):
        out = self._capture(
            [('pkg_b', 'Multiple objects with component ID "pkg_b" found.')], 3
        )
        self.assertIn('1 of 3 package(s) FAILED', out)
        self.assertIn('FAILED pkg_b: Multiple objects with component ID', out)
        self.assertIn('run Make Digital Objects again', out)

    def test_no_failures_prints_nothing(self):
        self.assertEqual(self._capture([], 3), '')


class MainExitCodeTests(unittest.TestCase):
    """main() wiring: failures → exit 2; clean batch → normal return."""

    def _run_main(self, failures):
        argv = ['prog', '-u', 'u', '-p', 'p', '--batch', '--no_caption', 'folder']
        env = {'WORKSPACE': '/tmp', 'DEFAULT_URL': 'http://aspace.example'}
        with patch.object(sys, 'argv', argv), \
                patch.dict(os.environ, env), \
                patch.object(mdo, 'ASpace', MagicMock(), create=True), \
                patch.object(mdo, 'get_path', return_value=Path('/tmp/folder')), \
                patch.object(mdo, 'process_batch', return_value=(failures, 3)), \
                patch.object(mdo, 'report_batch_failures') as m_report:
            try:
                mdo.main()
                return 0, m_report
            except SystemExit as e:
                return e.code, m_report

    def test_failures_exit_2_and_report(self):
        code, m_report = self._run_main([('pkg_b', 'duplicate')])
        self.assertEqual(code, 2)
        m_report.assert_called_once_with([('pkg_b', 'duplicate')], 3)

    def test_clean_batch_exits_zero(self):
        code, m_report = self._run_main([])
        self.assertEqual(code, 0)
        m_report.assert_not_called()


if __name__ == '__main__':
    unittest.main()
