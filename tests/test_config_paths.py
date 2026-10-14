# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the staging-path handling: config._dir_path and its wiring to
READY_PATH / INGEST_PATH, the `staging paths:` startup log line in app.py,
and get_package_file_count's count and log lines.

config values are frozen at first import for the whole session
(tests/conftest.py), so the wiring is checked in a subprocess with its own
environment instead of by reloading config here.

Run:
    python -m pytest tests/test_config_paths.py -v
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import config
from lib import archivematica_ops as ops

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_NAMES = ('READY_PATH', 'INGEST_PATH', 'INGESTED_PATH')


class DirPathTests(unittest.TestCase):

    def test_adds_the_missing_slash(self):
        self.assertEqual(config._dir_path('/data/ready'), '/data/ready/')

    def test_keeps_a_single_slash(self):
        self.assertEqual(config._dir_path('/data/ready/'), '/data/ready/')

    def test_collapses_repeated_slashes(self):
        self.assertEqual(config._dir_path('/data/ready//'), '/data/ready/')

    def test_root_stays_root(self):
        self.assertEqual(config._dir_path('/'), '/')

    def test_unset_and_empty_pass_through(self):
        self.assertIsNone(config._dir_path(None))
        self.assertEqual(config._dir_path(''), '')


class ConfigWiringTests(unittest.TestCase):
    """READY_PATH and INGEST_PATH are normalized; INGESTED_PATH is left alone."""

    def _import_config(self, **values):
        env = {k: v for k, v in os.environ.items() if k not in PATH_NAMES}
        env.update(values)
        code = (
            'import json, config; print(json.dumps({'
            + ', '.join('"%s": config.%s' % (n, n) for n in PATH_NAMES)
            + '}))'
        )
        proc = subprocess.run(
            [sys.executable, '-c', code], cwd=REPO_ROOT, env=env,
            capture_output=True, text=True, check=True,
        )
        return json.loads(proc.stdout)

    def test_values_without_the_slash_gain_one(self):
        values = self._import_config(
            READY_PATH='/data/ready', INGEST_PATH='/data/ingest',
            INGESTED_PATH='/data/ingested',
        )
        self.assertEqual(values['READY_PATH'], '/data/ready/')
        self.assertEqual(values['INGEST_PATH'], '/data/ingest/')
        self.assertEqual(values['INGESTED_PATH'], '/data/ingested')

    def test_values_with_the_slash_are_unchanged(self):
        values = self._import_config(
            READY_PATH='/data/ready/', INGEST_PATH='/data/ingest/',
        )
        self.assertEqual(values['READY_PATH'], '/data/ready/')
        self.assertEqual(values['INGEST_PATH'], '/data/ingest/')


class StagingPathStartupLogTests(unittest.TestCase):
    """The `staging paths:` line create_app() logs, per outcome."""

    def setUp(self):
        from app import _log_staging_paths_at_startup
        self.log = _log_staging_paths_at_startup
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _dir(self, name, create=True):
        path = os.path.join(self.tmp.name, name) + '/'
        if create:
            os.makedirs(path)
        return path

    def test_both_present_logs_ok_at_info(self):
        ready, ingest = self._dir('ready'), self._dir('ingest')
        with patch.multiple(config, READY_PATH=ready, INGEST_PATH=ingest), \
                self.assertLogs('app', level='INFO') as logs:
            self.log()
        self.assertEqual(logs.output, [
            'INFO:app:staging paths: READY_PATH=%s ok, INGEST_PATH=%s ok' % (ready, ingest),
        ])

    def test_a_missing_or_unset_path_warns(self):
        missing = self._dir('nope', create=False)
        with patch.multiple(config, READY_PATH=missing, INGEST_PATH=None), \
                self.assertLogs('app', level='WARNING') as logs:
            self.log()
        self.assertEqual(logs.output, [
            'WARNING:app:staging paths: READY_PATH=%s MISSING, INGEST_PATH unset' % missing,
        ])

    def test_a_check_that_throws_is_logged_not_raised(self):
        with patch.multiple(config, READY_PATH='/x/', INGEST_PATH='/y/'), \
                patch('os.path.isdir', side_effect=RuntimeError('boom')), \
                self.assertLogs('app', level='WARNING') as logs:
            self.log()
        self.assertIn('staging path check threw unexpectedly: boom', '\n'.join(logs.output))


class PackageFileCountTests(unittest.TestCase):
    """The count under READY_PATH/<folder>/<package>, and its log lines."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ready = os.path.join(self.tmp.name, 'ready') + '/'
        package = os.path.join(self.ready, 'batch_1', 'U201.01.0001')
        os.makedirs(os.path.join(package, 'subdir'))
        for name in ('a.tif', 'b.tif'):
            with open(os.path.join(package, name), 'w') as fh:
                fh.write('x')
        ready_patch = patch.object(ops, 'ready_path', self.ready)
        ready_patch.start()
        self.addCleanup(ready_patch.stop)

    def test_counts_files_only_and_logs_the_count(self):
        with self.assertLogs(ops.logger, level='INFO') as logs:
            self.assertEqual(ops.get_package_file_count('batch_1', 'U201.01.0001'), 2)
        self.assertEqual(logs.output, ['INFO:lib.archivematica_ops:Package count: 2'])

    def test_a_missing_package_returns_none_and_names_the_path(self):
        with self.assertLogs(ops.logger, level='WARNING') as logs:
            self.assertIsNone(ops.get_package_file_count('batch_1', 'nope'))
        self.assertEqual(len(logs.output), 1)
        self.assertIn('Package count failed for %sbatch_1/nope: ' % self.ready, logs.output[0])


if __name__ == '__main__':
    unittest.main()
