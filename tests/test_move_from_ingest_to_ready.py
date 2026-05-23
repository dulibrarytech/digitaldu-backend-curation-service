# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the patched move_from_ingest_to_ready operation.

Drop this file into digitaldu-backend-curation-service/tests/ after
applying the lib/archivematica_ops.py.patch and routes/qa.py.patch.

Covers:
  - Successful move (happy path) with uri.txt preservation
  - Idempotency when the package is already in 001-ready
  - Source-not-found error
  - Lock contention returns move_in_progress without touching disk
  - SFTP cleanup is attempted before the local move
  - SFTP cleanup failure is non-fatal (local move still happens)
  - Actor parameter is logged

Run:
    python -m pytest tests/test_move_from_ingest_to_ready.py -v
"""

import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

# Import the module under test. The module reads ready_path / ingest_path
# from `config` at import time, so we patch them after import.
from lib import archivematica_ops as ops


class MoveFromIngestToReadyTests(unittest.TestCase):
    """Each test gets a fresh tmpdir for ready/ + ingest/ trees so the
    lockfile and disk state don't bleed between cases."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='curation_test_')
        self.ready = os.path.join(self.tmp, 'ready') + '/'
        self.ingest = os.path.join(self.tmp, 'ingest') + '/'
        os.makedirs(self.ready)
        os.makedirs(self.ingest)
        # Override the module-level paths the ops layer reads.
        self._ready_patch = patch.object(ops, 'ready_path', self.ready)
        self._ingest_patch = patch.object(ops, 'ingest_path', self.ingest)
        self._ready_patch.start()
        self._ingest_patch.start()

        # Default: stub clean_up_sftp to a no-op so most tests don't need
        # a real SFTP server. Tests that care about SFTP behavior override.
        self._sftp_patch = patch.object(ops, 'clean_up_sftp', lambda *args: None)
        self._sftp_patch.start()

    def tearDown(self):
        self._sftp_patch.stop()
        self._ingest_patch.stop()
        self._ready_patch.stop()
        # tmpdir auto-cleanup
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- Helpers -------------------------------------------------------------

    def _seed_in_ingest(self, uuid, package, files=None):
        """Create a package directory inside 002-ingest/<uuid>/."""
        pkg_dir = os.path.join(self.ingest, uuid, package)
        os.makedirs(pkg_dir)
        for name in (files or ['uri.txt', 'foo.tif']):
            with open(os.path.join(pkg_dir, name), 'w') as f:
                f.write('test-content-' + name)
        return pkg_dir

    def _seed_in_ready(self, folder, package, files=None):
        """Create a package directory inside 001-ready/<folder>/."""
        pkg_dir = os.path.join(self.ready, folder, package)
        os.makedirs(pkg_dir)
        for name in (files or ['uri.txt']):
            with open(os.path.join(pkg_dir, name), 'w') as f:
                f.write('test-content-' + name)
        return pkg_dir

    # -- Happy path ----------------------------------------------------------

    def test_moves_package_back_with_uri_txt_preserved(self):
        self._seed_in_ingest('uuid-1', 'pkg-A')
        result = ops.move_from_ingest_to_ready('uuid-1', 'col-A', 'pkg-A')
        self.assertEqual(result['result'], 'packages_moved_back_to_ready.')
        self.assertEqual(result['errors'], [])

        # uri.txt must travel back so /processed lists the folder.
        self.assertTrue(
            os.path.exists(os.path.join(self.ready, 'col-A', 'pkg-A', 'uri.txt')),
            'uri.txt was not preserved through the move'
        )
        # foo.tif also travels — sanity check the whole package moved.
        self.assertTrue(
            os.path.exists(os.path.join(self.ready, 'col-A', 'pkg-A', 'foo.tif'))
        )
        # 002-ingest/<uuid>/ should be cleaned up after the last package.
        self.assertFalse(
            os.path.exists(os.path.join(self.ingest, 'uuid-1')),
            'empty 002-ingest/<uuid>/ directory should be removed'
        )

    def test_creates_destination_batch_folder_if_missing(self):
        self._seed_in_ingest('uuid-2', 'pkg-A')
        # 001-ready/col-B does NOT exist yet.
        self.assertFalse(os.path.exists(os.path.join(self.ready, 'col-B')))
        result = ops.move_from_ingest_to_ready('uuid-2', 'col-B', 'pkg-A')
        self.assertEqual(result['result'], 'packages_moved_back_to_ready.')
        self.assertTrue(os.path.exists(os.path.join(self.ready, 'col-B', 'pkg-A')))

    # -- Idempotency ---------------------------------------------------------

    def test_idempotent_when_already_in_ready(self):
        # Package already in 001-ready (e.g. retry of a prior successful move).
        self._seed_in_ready('col-A', 'pkg-A')
        # Nothing in 002-ingest.
        result = ops.move_from_ingest_to_ready('uuid-3', 'col-A', 'pkg-A')
        self.assertEqual(result['result'], 'already_in_ready')
        self.assertEqual(result['errors'], [])

    def test_source_not_found_returns_error(self):
        # Neither side has the package.
        result = ops.move_from_ingest_to_ready('uuid-4', 'col-X', 'pkg-X')
        self.assertEqual(result['result'], 'source_not_found')
        self.assertTrue(any('not found' in e for e in result['errors']))

    # -- Lock semantics ------------------------------------------------------

    def test_lock_contention_returns_move_in_progress(self):
        self._seed_in_ingest('uuid-lock', 'pkg-A')
        # Manually acquire the lock — simulates another in-flight operation.
        acquired = ops._lock_uuid('uuid-lock', owner='test_simulated')
        self.assertTrue(acquired)
        try:
            result = ops.move_from_ingest_to_ready('uuid-lock', 'col-A', 'pkg-A')
            self.assertEqual(result['result'], 'move_in_progress')
            self.assertTrue(any('lock' in e for e in result['errors']))
            # CRITICAL: lock contention must NOT touch disk.
            self.assertTrue(
                os.path.exists(os.path.join(self.ingest, 'uuid-lock', 'pkg-A')),
                'lock contention should not move the package'
            )
        finally:
            ops._unlock_uuid('uuid-lock', owner='test_simulated')

    def test_lock_is_released_after_successful_move(self):
        self._seed_in_ingest('uuid-r1', 'pkg-A')
        ops.move_from_ingest_to_ready('uuid-r1', 'col-A', 'pkg-A')
        # After success the lockfile and the dir should both be gone.
        self.assertFalse(ops._is_locked('uuid-r1'))

    def test_lock_is_released_after_failed_move(self):
        # Source missing → returns source_not_found. Lock must still release
        # so the next call (e.g. with corrected params) isn't blocked.
        ops.move_from_ingest_to_ready('uuid-fail', 'col-X', 'pkg-X')
        self.assertFalse(ops._is_locked('uuid-fail'))

    def test_concurrent_callers_one_wins(self):
        """Two threads racing on the same uuid: one moves the package,
        the other sees move_in_progress (or already_in_ready if it lost
        the race after the move completed)."""
        self._seed_in_ingest('uuid-race', 'pkg-A')
        results = []
        barrier = threading.Barrier(2)

        def call():
            barrier.wait()
            results.append(
                ops.move_from_ingest_to_ready('uuid-race', 'col-A', 'pkg-A')
            )

        t1 = threading.Thread(target=call)
        t2 = threading.Thread(target=call)
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
        outcomes = sorted([r['result'] for r in results])
        # Acceptable outcomes:
        #   ('move_in_progress', 'packages_moved_back_to_ready.')
        #     — one took the lock, the other saw it busy
        #   ('already_in_ready', 'packages_moved_back_to_ready.')
        #     — the second arrived after the first finished and cleaned up
        self.assertIn('packages_moved_back_to_ready.', outcomes)
        self.assertIn(outcomes[0], ('already_in_ready', 'move_in_progress'))
        # The package must have ended up in ready.
        self.assertTrue(
            os.path.exists(os.path.join(self.ready, 'col-A', 'pkg-A'))
        )

    # -- SFTP cleanup --------------------------------------------------------

    def test_sftp_cleanup_is_attempted(self):
        self._seed_in_ingest('uuid-sftp1', 'pkg-A')
        calls = []
        with patch.object(ops, 'clean_up_sftp', lambda u, p: calls.append((u, p))):
            result = ops.move_from_ingest_to_ready('uuid-sftp1', 'col-A', 'pkg-A')
        self.assertEqual(calls, [('uuid-sftp1', 'pkg-A')])
        self.assertTrue(result['sftp_clean']['attempted'])
        self.assertTrue(result['sftp_clean']['ok'])
        self.assertIsNone(result['sftp_clean']['err'])

    def test_sftp_cleanup_failure_is_non_fatal(self):
        self._seed_in_ingest('uuid-sftp2', 'pkg-A')

        def boom(*args):
            raise RuntimeError('sftp gone')

        with patch.object(ops, 'clean_up_sftp', boom):
            result = ops.move_from_ingest_to_ready('uuid-sftp2', 'col-A', 'pkg-A')

        # Local move still succeeded.
        self.assertEqual(result['result'], 'packages_moved_back_to_ready.')
        # SFTP failure is captured for the audit trail.
        self.assertTrue(result['sftp_clean']['attempted'])
        self.assertFalse(result['sftp_clean']['ok'])
        self.assertIn('sftp gone', result['sftp_clean']['err'])
        # Package is in 001-ready.
        self.assertTrue(
            os.path.exists(os.path.join(self.ready, 'col-A', 'pkg-A'))
        )

    def test_sftp_cleanup_skipped_when_idempotent_success(self):
        """If the package is already in ready, no SFTP cleanup needed
        (there's nothing in 002-ingest)."""
        self._seed_in_ready('col-A', 'pkg-A')
        calls = []
        with patch.object(ops, 'clean_up_sftp', lambda u, p: calls.append((u, p))):
            result = ops.move_from_ingest_to_ready('uuid-skip', 'col-A', 'pkg-A')
        self.assertEqual(result['result'], 'already_in_ready')
        self.assertEqual(calls, [], 'should not attempt SFTP cleanup on idempotent path')

    # -- Actor logging -------------------------------------------------------

    def test_actor_appears_in_log_when_provided(self):
        import logging
        self._seed_in_ingest('uuid-actor', 'pkg-A')
        with self.assertLogs(ops.logger, level='INFO') as cm:
            ops.move_from_ingest_to_ready(
                'uuid-actor', 'col-A', 'pkg-A', actor='jdoe@du.edu'
            )
        joined = '\n'.join(cm.output)
        self.assertIn('actor=jdoe@du.edu', joined)

    def test_actor_logs_as_unset_when_omitted(self):
        self._seed_in_ingest('uuid-noactor', 'pkg-A')
        with self.assertLogs(ops.logger, level='INFO') as cm:
            ops.move_from_ingest_to_ready('uuid-noactor', 'col-A', 'pkg-A')
        joined = '\n'.join(cm.output)
        self.assertIn('actor=<unset>', joined)


class MoveToIngestLockTests(unittest.TestCase):
    """The forward direction (move_to_ingest) shares the lock with the
    reverse direction. A cancel that races against an in-flight upload
    must not interleave with a fresh move_to_ingest call."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='curation_test_')
        self.ready = os.path.join(self.tmp, 'ready') + '/'
        self.ingest = os.path.join(self.tmp, 'ingest') + '/'
        os.makedirs(self.ready)
        os.makedirs(self.ingest)
        self._patches = [
            patch.object(ops, 'ready_path', self.ready),
            patch.object(ops, 'ingest_path', self.ingest),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_move_to_ingest_returns_move_in_progress_on_lock_contention(self):
        # Pre-seed a package in 001-ready.
        os.makedirs(os.path.join(self.ready, 'col-A', 'pkg-A'))
        # Simulate another op holding the lock for this uuid.
        acquired = ops._lock_uuid('uuid-contention', owner='test_other')
        self.assertTrue(acquired)
        try:
            result = ops.move_to_ingest('uuid-contention', 'col-A', 'pkg-A')
            self.assertEqual(result['result'], 'move_in_progress')
            # The package must not have moved.
            self.assertTrue(
                os.path.exists(os.path.join(self.ready, 'col-A', 'pkg-A'))
            )
        finally:
            ops._unlock_uuid('uuid-contention', owner='test_other')


if __name__ == '__main__':
    unittest.main()
