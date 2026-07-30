# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
_open_sftp connect retry + throttle tests (2026-07-29 burst hardening).

A large batch opens several SSH sessions in quick succession and the
AM sshd MaxStartups cap drops the excess pre-banner ("Error reading
SSH protocol banner"). _open_sftp must:

  * retry connection SETUP with backoff,
  * pass the widened banner/tcp/auth timeouts to paramiko,
  * cap concurrent sessions per process (SFTP_MAX_SESSIONS slots,
    released when the caller closes the client), and
  * space successive connection attempts (SFTP_CONNECT_MIN_INTERVAL_S)
    so polls never hammer the vendor's sshd with rapid logins.

paramiko is faked at the class boundary; time.sleep is patched so the
suite stays instant. Tests that exercise slots swap in a fresh
semaphore so module state never leaks between tests.

Run:
    python -m pytest tests/test_sftp_connect_retry.py -v
"""

import tempfile
import threading
import unittest
from unittest.mock import patch, MagicMock

import paramiko

from lib import archivematica_ops as ops


def _fake_client_factory(fail_times, error=None):
    """Return (factory, state): factory() yields clients whose connect()
    raises `error` for the first `fail_times` calls, then succeeds."""
    state = {'connects': 0, 'closed': 0, 'kwargs': []}
    err = error or paramiko.SSHException('Error reading SSH protocol banner')

    def factory():
        client = MagicMock()

        def connect(**kwargs):
            state['connects'] += 1
            state['kwargs'].append(kwargs)
            if state['connects'] <= fail_times:
                raise err

        client.connect.side_effect = connect

        def close():
            state['closed'] += 1

        client.close = close  # plain attr so _release_slot_on_close can wrap
        client.open_sftp.return_value = MagicMock()
        return client

    return factory, state


def _quiet_throttles():
    """Disable pacing + give the test its own slot-lock directory."""
    return (
        patch.object(ops, 'SFTP_CONNECT_MIN_INTERVAL_S', 0),
        patch.object(ops, 'SFTP_LOCK_DIR', tempfile.mkdtemp(prefix='sftp-slots-')),
    )


class OpenSftpRetryTests(unittest.TestCase):

    def test_retries_banner_error_then_succeeds(self):
        factory, state = _fake_client_factory(fail_times=2)
        sleeps = []
        pace_off, fresh_slots = _quiet_throttles()
        with patch.object(paramiko, 'SSHClient', side_effect=factory), \
             patch.object(ops.time, 'sleep', side_effect=sleeps.append), \
             pace_off, fresh_slots:
            client, sftp = ops._open_sftp()
            client.close()
        self.assertIsNotNone(sftp)
        self.assertEqual(state['connects'], 3)
        # Two failed clients closed + the final close() above.
        self.assertEqual(state['closed'], 3)
        self.assertEqual(len(sleeps), 2)
        # Exponential shape with 0.5-1.5 jitter: 1-3s then 2-6s.
        self.assertTrue(1.0 <= sleeps[0] <= 3.0, sleeps)
        self.assertTrue(2.0 <= sleeps[1] <= 6.0, sleeps)

    def test_passes_widened_timeouts_to_paramiko(self):
        factory, state = _fake_client_factory(fail_times=0)
        pace_off, fresh_slots = _quiet_throttles()
        with patch.object(paramiko, 'SSHClient', side_effect=factory), \
             pace_off, fresh_slots:
            client, _ = ops._open_sftp()
            client.close()
        kwargs = state['kwargs'][0]
        self.assertEqual(kwargs['banner_timeout'], ops.SFTP_BANNER_TIMEOUT_S)
        self.assertEqual(kwargs['timeout'], ops.SFTP_TCP_TIMEOUT_S)
        self.assertEqual(kwargs['auth_timeout'], ops.SFTP_AUTH_TIMEOUT_S)

    def test_raises_last_error_after_exhausting_attempts(self):
        factory, state = _fake_client_factory(fail_times=99)
        pace_off, fresh_slots = _quiet_throttles()
        with patch.object(paramiko, 'SSHClient', side_effect=factory), \
             patch.object(ops.time, 'sleep'), pace_off, fresh_slots:
            with self.assertRaises(paramiko.SSHException):
                ops._open_sftp()
        self.assertEqual(state['connects'], ops.SFTP_CONNECT_ATTEMPTS)

    def test_oserror_also_retried_up_to_budget(self):
        factory, state = _fake_client_factory(
            fail_times=99, error=OSError('Connection refused')
        )
        pace_off, fresh_slots = _quiet_throttles()
        with patch.object(paramiko, 'SSHClient', side_effect=factory), \
             patch.object(ops.time, 'sleep'), pace_off, fresh_slots:
            with self.assertRaises(OSError):
                ops._open_sftp()
        self.assertEqual(state['connects'], ops.SFTP_CONNECT_ATTEMPTS)


class SessionSlotTests(unittest.TestCase):
    """flock-backed slots are HOST-global: distinct fds on the same
    lock file conflict even within one process, so these tests exercise
    the real cross-worker semantics."""

    def test_slot_released_on_client_close_and_on_failure(self):
        factory, _ = _fake_client_factory(fail_times=0)
        pace_off, fresh_dir = _quiet_throttles()
        one_slot = patch.object(ops, 'SFTP_MAX_SESSIONS', 1)
        fast_timeout = patch.object(ops, 'SFTP_SLOT_ACQUIRE_TIMEOUT_S', 0.01)
        with patch.object(paramiko, 'SSHClient', side_effect=factory), \
             pace_off, fresh_dir, one_slot, fast_timeout:
            client, _ = ops._open_sftp()
            # Slot is held while the session is open...
            with self.assertRaises(RuntimeError):
                ops._acquire_global_slot()
            client.close()
            # ...and freed exactly once on close (double-close safe).
            client.close()
            fd = ops._acquire_global_slot()
            import os as _os
            _os.close(fd)

            # Failure path releases too.
            fail_factory, _ = _fake_client_factory(fail_times=99)
            with patch.object(paramiko, 'SSHClient', side_effect=fail_factory), \
                 patch.object(ops.time, 'sleep'):
                with self.assertRaises(paramiko.SSHException):
                    ops._open_sftp()
            fd = ops._acquire_global_slot()
            _os.close(fd)

    def test_exhausted_slots_refuse_instead_of_piling_on(self):
        factory, _ = _fake_client_factory(fail_times=0)
        pace_off, fresh_dir = _quiet_throttles()
        with patch.object(paramiko, 'SSHClient', side_effect=factory), \
             pace_off, fresh_dir, \
             patch.object(ops, 'SFTP_MAX_SESSIONS', 1), \
             patch.object(ops, 'SFTP_SLOT_ACQUIRE_TIMEOUT_S', 0.01):
            client, _ = ops._open_sftp()
            with self.assertRaises(RuntimeError):
                ops._open_sftp()
            client.close()


class MoveToSftpSingleFlightTests(unittest.TestCase):

    def test_second_start_for_same_pid_is_a_noop(self):
        started = []
        fake_thread = MagicMock()
        fake_thread_cls = MagicMock(
            side_effect=lambda **kw: started.append(kw) or fake_thread
        )
        with patch.object(ops.threading, 'Thread', fake_thread_cls):
            try:
                r1 = ops.move_to_sftp('pid-single')
                r2 = ops.move_to_sftp('pid-single')
                self.assertEqual(r1['message'], 'upload_started')
                self.assertEqual(r2['message'], 'upload_already_running')
                self.assertEqual(len(started), 1)
                # Once the put finishes (registry cleared), a new start works.
                with ops._sftp_puts_lock:
                    ops._sftp_puts_in_flight.discard('pid-single')
                r3 = ops.move_to_sftp('pid-single')
                self.assertEqual(r3['message'], 'upload_started')
                self.assertEqual(len(started), 2)
            finally:
                with ops._sftp_puts_lock:
                    ops._sftp_puts_in_flight.discard('pid-single')


class ConnectPacingTests(unittest.TestCase):

    def test_second_connect_waits_out_the_minimum_interval(self):
        factory, _ = _fake_client_factory(fail_times=0)
        sleeps = []
        with patch.object(paramiko, 'SSHClient', side_effect=factory), \
             patch.object(ops.time, 'sleep', side_effect=sleeps.append), \
             patch.object(ops, 'SFTP_CONNECT_MIN_INTERVAL_S', 5.0), \
             patch.object(ops, 'SFTP_LOCK_DIR', tempfile.mkdtemp(prefix='sftp-slots-')), \
             patch.object(ops, '_sftp_last_connect_at', [None]):
            c1, _ = ops._open_sftp()
            c1.close()
            c2, _ = ops._open_sftp()
            c2.close()
        # First connect starts the clock (no wait); the second — issued
        # instantly — must wait out (nearly) the whole interval.
        self.assertEqual(len(sleeps), 1)
        self.assertTrue(4.5 < sleeps[0] <= 5.0, sleeps)


if __name__ == '__main__':
    unittest.main()
