# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the upload-progress path used by the repo-backend-v2 dashboard.

Two concerns:

  1. _local_dir_size — the upload *total* (local 002-ingest/<uuid> size)
     check_sftp reports alongside the remote uploaded byte sum so the
     dashboard can render a byte-accurate upload %.

  2. The ASYNC upload (move_to_sftp + check_sftp). move_to_sftp now runs
     the SFTP put in a background thread and returns immediately, so the
     Node poller can watch progress while the bytes move (the legacy
     synchronous put made the dashboard sit on UPLOADING with no progress
     bar for the entire upload). Failure of the background put is surfaced
     to the poller via the .upload_error marker, which check_sftp reports
     as message='upload_failed'.

These mock the SFTP layer (_open_sftp / _sftp_put_r) and point ingest_path
at a tempdir, so they need no real Archivematica SFTP host.

Run:
    python -m pytest tests/test_upload_progress.py -v
"""

import os
import tempfile
import threading
import unittest
from unittest import mock

from lib import archivematica_ops as ops


class LocalDirSizeTests(unittest.TestCase):
    def test_sums_regular_files_recursively(self):
        d = tempfile.mkdtemp(prefix='lds_')
        os.makedirs(os.path.join(d, 'sub'))
        with open(os.path.join(d, 'a.bin'), 'wb') as f:
            f.write(b'x' * 1000)
        with open(os.path.join(d, 'sub', 'b.bin'), 'wb') as f:
            f.write(b'y' * 2048)
        self.assertEqual(ops._local_dir_size(d), 3048)

    def test_empty_dir_is_zero(self):
        d = tempfile.mkdtemp(prefix='lds_empty_')
        self.assertEqual(ops._local_dir_size(d), 0)

    def test_missing_path_returns_zero(self):
        # Missing/unreadable → 0, which the Node side treats as "unknown
        # total" and falls back to the file-count readout.
        self.assertEqual(ops._local_dir_size('/no/such/path/xyz-123'), 0)


class _IngestTmpMixin(unittest.TestCase):
    """Point ops.ingest_path at a tempdir with a 002-ingest/<pid> staging
    dir, so marker writes/reads stay hermetic. Also stub ops.sftp_path —
    it is None on hosts without SFTP_REMOTE_PATH in the env, and
    check_sftp concatenates it into the remote package path."""

    PID = 'codu-test'

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='upl_')
        os.makedirs(os.path.join(self.tmp, self.PID))
        self._orig_ingest = ops.ingest_path
        self._orig_sftp_path = ops.sftp_path
        # functions use `ingest_path + pid`, so keep the trailing sep.
        ops.ingest_path = self.tmp + os.sep
        ops.sftp_path = '/remote/ingest'

    def tearDown(self):
        ops.ingest_path = self._orig_ingest
        ops.sftp_path = self._orig_sftp_path

    def marker_path(self):
        return os.path.join(self.tmp, self.PID, ops.UPLOAD_ERROR_MARKER)


class _FakeClient:
    def close(self):
        pass


class MoveToSftpAsyncTests(_IngestTmpMixin):
    """Regression guard for the no-progress-bar bug: move_to_sftp must
    background the put and return immediately, not block until the whole
    upload finishes."""

    def test_returns_immediately_without_blocking_on_the_put(self):
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        class FakeSftp:
            def listdir(self, _p):
                return [MoveToSftpAsyncTests.PID]  # package present → success

            def close(self):
                pass

        def fake_put_r(sftp, local, remote, preserve_mtime=True, **kwargs):
            started.set()
            release.wait(5)  # simulate a long-running upload
            finished.set()

        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), FakeSftp())), \
                mock.patch.object(ops, '_sftp_put_r', fake_put_r):
            res = ops.move_to_sftp(self.PID)
            # move_to_sftp has already returned while the put is still blocked.
            self.assertEqual(res.get('message'), 'upload_started')
            self.assertTrue(started.wait(5), 'background put thread should have started')
            self.assertFalse(finished.is_set(), 'put must not finish before we release it')
            release.set()
            self.assertTrue(finished.wait(5), 'background put should finish after release')

        # A clean run leaves no error marker.
        self.assertFalse(os.path.exists(self.marker_path()))


class DoSftpPutMarkerTests(_IngestTmpMixin):
    def test_writes_marker_on_failure(self):
        def boom():
            raise IOError('sftp connection refused')

        with mock.patch.object(ops, '_open_sftp', boom):
            ops._do_sftp_put(self.PID)  # run the worker inline

        marker = self.marker_path()
        self.assertTrue(os.path.exists(marker), 'a failed put must drop the .upload_error marker')
        with open(marker) as fh:
            self.assertIn('refused', fh.read())

    def test_clears_stale_marker_on_success(self):
        marker = self.marker_path()
        with open(marker, 'w') as fh:
            fh.write('stale failure from a previous attempt')

        class FakeSftp:
            def listdir(self, _p):
                return [DoSftpPutMarkerTests.PID]

            def close(self):
                pass

        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), FakeSftp())), \
                mock.patch.object(ops, '_sftp_put_r', lambda *a, **k: None):
            ops._do_sftp_put(self.PID)

        self.assertFalse(os.path.exists(marker), 'a successful put should clear the stale marker')

    def test_marker_written_when_package_missing_after_put(self):
        # The put "succeeded" but the package isn't on the remote listing —
        # treated as a failure so the row halts rather than false-completing.
        class FakeSftp:
            def listdir(self, _p):
                return ['some-other-package']

            def close(self):
                pass

        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), FakeSftp())), \
                mock.patch.object(ops, '_sftp_put_r', lambda *a, **k: None):
            ops._do_sftp_put(self.PID)

        self.assertTrue(os.path.exists(self.marker_path()))


class CheckSftpFailureTests(_IngestTmpMixin):
    def test_surfaces_upload_failed_when_marker_present_without_touching_sftp(self):
        with open(self.marker_path(), 'w') as fh:
            fh.write('disk full on sftp host')

        # _open_sftp must NOT be called — the marker short-circuits before any
        # SFTP round-trip. Patch it to blow up so a regression is caught.
        def fail_open():
            raise AssertionError('check_sftp opened SFTP despite the error marker')

        with mock.patch.object(ops, '_open_sftp', fail_open):
            res = ops.check_sftp(self.PID, 5)

        self.assertEqual(res.get('message'), 'upload_failed')
        self.assertIn('disk full', res.get('error', ''))

    def test_missing_remote_dir_reads_as_in_progress_zero(self):
        # Early poll: the background put hasn't created sftp_path/<uuid> yet.
        # check_sftp should report in_progress(0 remote) — not raise/500 — and
        # still measure the local total so the dashboard shows a determinate bar.
        with open(os.path.join(self.tmp, self.PID, 'a.bin'), 'wb') as f:
            f.write(b'x' * 100)

        class FakeSftp:
            def listdir_attr(self, _p):
                raise IOError('No such file')

            def close(self):
                pass

        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), FakeSftp())):
            res = ops.check_sftp(self.PID, 5)

        self.assertEqual(res.get('message'), 'in_progress')
        self.assertEqual(res.get('remote_file_count'), 0)
        self.assertEqual(res.get('local_package_size_bytes'), 100)


class UploadCancelTests(_IngestTmpMixin):
    """Cancel + single-flight guards on the background put (2026-07-30:
    'Halt entire batch' left daemon-thread puts running, and duplicate
    puts across gunicorn workers produced 'size mismatch in put!')."""

    def cancel_path(self):
        return os.path.join(self.tmp, self.PID, ops.UPLOAD_CANCEL_MARKER)

    def test_request_upload_cancel_writes_flag(self):
        res = ops.request_upload_cancel(self.PID)
        self.assertEqual(res['message'], 'cancel_requested')
        self.assertTrue(os.path.exists(self.cancel_path()))
        # No staging dir → no-op result.
        res2 = ops.request_upload_cancel('no-such-pid')
        self.assertEqual(res2['message'], 'no_staging_dir')

    def test_put_aborts_between_files_when_flag_appears(self):
        put_files = []

        class FakeSftp:
            def listdir(self, _p):
                return [UploadCancelTests.PID]

            def close(self):
                pass

        def fake_put_r(sftp, local, remote, preserve_mtime=True, should_abort=None):
            # Simulate two files; flag lands after the first.
            put_files.append('f1')
            with open(self.cancel_path(), 'w') as fh:
                fh.write('x')
            if should_abort and should_abort():
                raise ops.UploadCancelled()
            put_files.append('f2')

        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), FakeSftp())), \
                mock.patch.object(ops, '_sftp_put_r', fake_put_r):
            ops._do_sftp_put(self.PID)

        self.assertEqual(put_files, ['f1'])
        # Cancelled put is NOT an error — no .upload_error marker, and
        # the cancel flag is consumed.
        self.assertFalse(os.path.exists(self.marker_path()))
        self.assertFalse(os.path.exists(self.cancel_path()))

    def test_sftp_put_r_aborts_promptly_via_should_abort(self):
        d = tempfile.mkdtemp(prefix='putr_')
        for name in ('a.bin', 'b.bin'):
            with open(os.path.join(d, name), 'wb') as f:
                f.write(b'x' * 10)
        calls = []

        class FakeSftp:
            def put(self, local, remote, callback=None):
                calls.append(os.path.basename(local))

            def utime(self, *_a):
                pass

            def stat(self, _p):
                return None  # dirs "exist" — skip mkdir

            def mkdir(self, _p):
                pass

        aborts = iter([False, True])  # first file allowed, then abort
        with self.assertRaises(ops.UploadCancelled):
            ops._sftp_put_r(
                FakeSftp(), d, '/remote', preserve_mtime=False,
                should_abort=lambda: next(aborts),
            )
        # Exactly one of the two files went up (os.walk order is
        # platform-dependent) — the abort fired before the second.
        self.assertEqual(len(calls), 1)

    def test_second_process_bows_out_via_put_lock(self):
        import fcntl as _fcntl
        lock_dir = tempfile.mkdtemp(prefix='putlock_')
        with mock.patch.object(ops, 'SFTP_LOCK_DIR', lock_dir):
            # Hold the put lock as if another worker owns the upload.
            fd = os.open(ops._put_lock_path(self.PID), os.O_CREAT | os.O_RDWR)
            _fcntl.flock(fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            try:
                def explode():
                    raise AssertionError('must not open SFTP while another put holds the lock')
                with mock.patch.object(ops, '_open_sftp', explode):
                    ops._do_sftp_put(self.PID)  # bows out silently
            finally:
                os.close(fd)


class PerPackagePutTests(_IngestTmpMixin):
    """The 2026-10-14 scoping: with `package`, the put and check_sftp work on
    002-ingest/<pid>/<package> only. A leftover sibling (a package whose
    Wasabi archive failed) must be neither re-sent nor counted."""

    PKG = 'U201.01.0001'
    REMOTE = '/remote/ingest'

    def setUp(self):
        super().setUp()
        self.pkg_dir = os.path.join(self.tmp, self.PID, self.PKG)
        os.makedirs(self.pkg_dir)
        with open(os.path.join(self.pkg_dir, 'a.bin'), 'wb') as f:
            f.write(b'x' * 100)
        # A leftover sibling from an earlier package of the same collection.
        os.makedirs(os.path.join(self.tmp, self.PID, 'leftover'))
        with open(os.path.join(self.tmp, self.PID, 'leftover', 'b.bin'), 'wb') as f:
            f.write(b'y' * 50)

    def pkg_marker_path(self):
        return os.path.join(self.tmp, self.PID, ops.UPLOAD_ERROR_MARKER + '--' + self.PKG)

    def _fake_sftp(self, listing):
        """listdir answers from `listing` (path -> names)."""
        class FakeSftp:
            def listdir(self, path):
                return listing.get(path, [])

            def close(self):
                pass
        return FakeSftp()

    def test_put_uploads_only_the_package_folder(self):
        puts = []

        def fake_put_r(sftp, local, remote, preserve_mtime=True, should_abort=None):
            puts.append((local, remote))

        listing = {self.REMOTE + '/' + self.PID: [self.PKG, 'leftover']}
        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), self._fake_sftp(listing))), \
                mock.patch.object(ops, '_sftp_put_r', fake_put_r):
            ops._do_sftp_put(self.PID, self.PKG)

        self.assertEqual(puts, [(self.pkg_dir, self.REMOTE + '/' + self.PID + '/' + self.PKG)])
        self.assertFalse(os.path.exists(self.pkg_marker_path()))
        self.assertFalse(os.path.exists(self.marker_path()))

    def test_missing_package_folder_fails_without_opening_sftp(self):
        def explode():
            raise AssertionError('must not open SFTP for a package that is not staged')

        with mock.patch.object(ops, '_open_sftp', explode):
            ops._do_sftp_put(self.PID, 'not-staged')

        marker = os.path.join(self.tmp, self.PID, ops.UPLOAD_ERROR_MARKER + '--not-staged')
        with open(marker) as fh:
            self.assertIn('not found in 002-ingest', fh.read())
        # The failure is that package's alone.
        self.assertFalse(os.path.exists(self.marker_path()))
        self.assertFalse(os.path.exists(self.pkg_marker_path()))

    def test_package_missing_from_the_remote_listing_is_a_failure(self):
        listing = {self.REMOTE + '/' + self.PID: ['leftover']}
        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), self._fake_sftp(listing))), \
                mock.patch.object(ops, '_sftp_put_r', lambda *a, **k: None):
            ops._do_sftp_put(self.PID, self.PKG)

        with open(self.pkg_marker_path()) as fh:
            self.assertIn('not found on sftp after put', fh.read())

    def test_check_sftp_measures_the_package_only(self):
        walked = []

        def fake_walk(sftp, remote_dir, on_file=None, on_dir=None, on_other=None):
            walked.append(remote_dir)
            on_file(remote_dir + '/a.bin')

        sized = []

        def fake_dir_size(sftp, remote_dir):
            sized.append(remote_dir)
            return 60

        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), self._fake_sftp({}))), \
                mock.patch.object(ops, '_sftp_walk', fake_walk), \
                mock.patch.object(ops, '_sftp_dir_size', fake_dir_size):
            in_progress = ops.check_sftp(self.PID, 2, self.PKG)
            complete = ops.check_sftp(self.PID, 1, self.PKG)

        remote_pkg = self.REMOTE + '/' + self.PID + '/' + self.PKG
        self.assertEqual(walked, [remote_pkg, remote_pkg])
        self.assertEqual(sized, [remote_pkg, remote_pkg])
        self.assertEqual(in_progress['message'], 'in_progress')
        self.assertEqual(in_progress['remote_file_count'], 1)
        self.assertEqual(in_progress['remote_package_size_bytes'], 60)
        # The local total is the package's 100 bytes, not the folder's 150.
        self.assertEqual(in_progress['local_package_size_bytes'], 100)
        self.assertEqual(complete['message'], 'upload_complete')

    def test_error_marker_is_read_for_that_package_only(self):
        with open(self.pkg_marker_path(), 'w') as fh:
            fh.write('disk full on sftp host')

        def fake_walk(sftp, remote_dir, **kwargs):
            pass

        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), self._fake_sftp({}))), \
                mock.patch.object(ops, '_sftp_walk', fake_walk), \
                mock.patch.object(ops, '_sftp_dir_size', lambda *a: 0):
            failed = ops.check_sftp(self.PID, 1, self.PKG)
            other = ops.check_sftp(self.PID, 1, 'leftover')
            legacy = ops.check_sftp(self.PID, 1)

        self.assertEqual(failed['message'], 'upload_failed')
        self.assertIn('disk full', failed['error'])
        self.assertEqual(other['message'], 'in_progress')
        self.assertEqual(legacy['message'], 'in_progress')

    def test_cancel_flag_is_per_package(self):
        res = ops.request_upload_cancel(self.PID, self.PKG)
        self.assertEqual(res['message'], 'cancel_requested')
        self.assertTrue(os.path.exists(
            os.path.join(self.tmp, self.PID, ops.UPLOAD_CANCEL_MARKER + '--' + self.PKG)
        ))
        self.assertFalse(os.path.exists(os.path.join(self.tmp, self.PID, ops.UPLOAD_CANCEL_MARKER)))
        self.assertEqual(ops.request_upload_cancel(self.PID, 'not-staged')['message'], 'no_staging_dir')

    def test_put_honours_its_own_cancel_flag(self):
        put_files = []

        def fake_put_r(sftp, local, remote, preserve_mtime=True, should_abort=None):
            put_files.append('f1')
            ops.request_upload_cancel(self.PID, self.PKG)
            if should_abort and should_abort():
                raise ops.UploadCancelled()
            put_files.append('f2')

        listing = {self.REMOTE + '/' + self.PID: [self.PKG]}
        with mock.patch.object(ops, '_open_sftp', lambda: (_FakeClient(), self._fake_sftp(listing))), \
                mock.patch.object(ops, '_sftp_put_r', fake_put_r):
            ops._do_sftp_put(self.PID, self.PKG)

        self.assertEqual(put_files, ['f1'])
        self.assertFalse(os.path.exists(self.pkg_marker_path()))
        self.assertFalse(os.path.exists(
            os.path.join(self.tmp, self.PID, ops.UPLOAD_CANCEL_MARKER + '--' + self.PKG)
        ))

    def test_put_lock_is_per_package(self):
        self.assertNotEqual(ops._put_lock_path(ops._put_key(self.PID, 'A')),
                            ops._put_lock_path(ops._put_key(self.PID, 'B')))
        self.assertEqual(ops._put_lock_path(ops._put_key(self.PID)), ops._put_lock_path(self.PID))
        self.assertNotIn('/', os.path.basename(ops._put_lock_path(ops._put_key(self.PID, 'A'))))


if __name__ == '__main__':
    unittest.main()
