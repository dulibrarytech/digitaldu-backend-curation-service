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


if __name__ == '__main__':
    unittest.main()


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
