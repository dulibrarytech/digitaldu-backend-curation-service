# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Unit tests for scripts/abort_stale_multipart_uploads.py.

Covers the age partitioning, report-vs-apply safety (nothing aborted
without --apply), NoSuchUpload tolerance, and the lifecycle merge
behavior: existing rules preserved, no-op when an abort rule is already
present, clean 'unsupported' outcome when the endpoint rejects the
configuration.

Run:
    python -m pytest tests/test_abort_stale_multipart_uploads.py -v
"""

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from botocore.exceptions import ClientError  # noqa: E402

from scripts import abort_stale_multipart_uploads as hygiene  # noqa: E402


# Anchor fixture ages to REAL now: the script computes its cutoff from
# datetime.now(), so a fixed date here rots — a "1-day-old" upload
# frozen at an absolute date becomes stale for real within days of
# writing the test (bitten 2026-08-01).
NOW = datetime.now(timezone.utc)


def _upload(key, age_days, upload_id='u-1'):
    return {
        'Key': key,
        'UploadId': upload_id,
        'Initiated': NOW - timedelta(days=age_days),
    }


class FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, Bucket=None):  # noqa: N803 - boto3 casing
        for page in self._pages:
            yield page


class FakeClient:
    """
    S3 client fake: canned multipart-upload listing + recorded aborts +
    scriptable lifecycle read/write behavior.
    """

    def __init__(self, uploads=None, lifecycle_rules=None,
                 lifecycle_read_error=None, lifecycle_write_error=None,
                 abort_error_codes=None):
        self._uploads = uploads or []
        self._lifecycle_rules = lifecycle_rules
        self._lifecycle_read_error = lifecycle_read_error
        self._lifecycle_write_error = lifecycle_write_error
        self._abort_error_codes = dict(abort_error_codes or {})
        self.aborts = []
        self.lifecycle_puts = []

    def get_paginator(self, _name):
        return FakePaginator([{'Uploads': self._uploads}])

    def abort_multipart_upload(self, Bucket=None, Key=None, UploadId=None):  # noqa: N803
        code = self._abort_error_codes.get(Key)
        if code:
            raise ClientError({'Error': {'Code': code}}, 'AbortMultipartUpload')
        self.aborts.append((Bucket, Key, UploadId))

    def get_bucket_lifecycle_configuration(self, Bucket=None):  # noqa: N803
        if self._lifecycle_read_error:
            raise ClientError(
                {'Error': {'Code': self._lifecycle_read_error}},
                'GetBucketLifecycleConfiguration',
            )
        return {'Rules': list(self._lifecycle_rules or [])}

    def put_bucket_lifecycle_configuration(self, Bucket=None, LifecycleConfiguration=None):  # noqa: N803
        if self._lifecycle_write_error:
            raise ClientError(
                {'Error': {'Code': self._lifecycle_write_error}},
                'PutBucketLifecycleConfiguration',
            )
        self.lifecycle_puts.append((Bucket, LifecycleConfiguration))


class SplitStaleTests(unittest.TestCase):
    def test_partitions_by_cutoff(self):
        cutoff = NOW - timedelta(days=3)
        stale, fresh = hygiene.split_stale(
            [_upload('old.7z', 5), _upload('new.7z', 1)], cutoff
        )
        self.assertEqual([u['Key'] for u in stale], ['old.7z'])
        self.assertEqual([u['Key'] for u in fresh], ['new.7z'])

    def test_undated_upload_counts_as_fresh(self):
        cutoff = NOW - timedelta(days=3)
        stale, fresh = hygiene.split_stale([{'Key': 'mystery.7z'}], cutoff)
        self.assertEqual(stale, [])
        self.assertEqual([u['Key'] for u in fresh], ['mystery.7z'])


class AbortTests(unittest.TestCase):
    def test_aborts_each_upload(self):
        client = FakeClient()
        aborted, failed = hygiene.abort_uploads(
            client, 'b', [_upload('a.7z', 5, 'u-a'), _upload('b.7z', 6, 'u-b')]
        )
        self.assertEqual((aborted, failed), (2, 0))
        self.assertEqual(
            client.aborts, [('b', 'a.7z', 'u-a'), ('b', 'b.7z', 'u-b')]
        )

    def test_no_such_upload_counts_as_success(self):
        client = FakeClient(abort_error_codes={'gone.7z': 'NoSuchUpload'})
        aborted, failed = hygiene.abort_uploads(
            client, 'b', [_upload('gone.7z', 5)]
        )
        self.assertEqual((aborted, failed), (1, 0))

    def test_other_errors_count_as_failed(self):
        client = FakeClient(abort_error_codes={'locked.7z': 'AccessDenied'})
        aborted, failed = hygiene.abort_uploads(
            client, 'b', [_upload('locked.7z', 5)]
        )
        self.assertEqual((aborted, failed), (0, 1))


class LifecycleTests(unittest.TestCase):
    def test_installs_rule_preserving_existing(self):
        existing = {
            'ID': 'expire-old-logs',
            'Status': 'Enabled',
            'Filter': {'Prefix': 'logs/'},
            'Expiration': {'Days': 30},
        }
        client = FakeClient(lifecycle_rules=[existing])
        outcome = hygiene.ensure_lifecycle(client, 'b', 3)
        self.assertEqual(outcome, 'installed')
        self.assertEqual(len(client.lifecycle_puts), 1)
        rules = client.lifecycle_puts[0][1]['Rules']
        self.assertEqual(rules[0], existing)  # untouched
        self.assertEqual(rules[1]['ID'], hygiene.LIFECYCLE_RULE_ID)
        self.assertEqual(
            rules[1]['AbortIncompleteMultipartUpload'],
            {'DaysAfterInitiation': 3},
        )

    def test_installs_rule_when_no_config_exists(self):
        client = FakeClient(
            lifecycle_read_error='NoSuchLifecycleConfiguration'
        )
        outcome = hygiene.ensure_lifecycle(client, 'b', 3)
        self.assertEqual(outcome, 'installed')
        self.assertEqual(len(client.lifecycle_puts), 1)

    def test_noop_when_enabled_abort_rule_present(self):
        client = FakeClient(lifecycle_rules=[{
            'ID': 'someone-elses-rule',
            'Status': 'Enabled',
            'Filter': {},
            'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7},
        }])
        outcome = hygiene.ensure_lifecycle(client, 'b', 3)
        self.assertEqual(outcome, 'already_present')
        self.assertEqual(client.lifecycle_puts, [])

    def test_disabled_abort_rule_does_not_count(self):
        client = FakeClient(lifecycle_rules=[{
            'ID': 'off',
            'Status': 'Disabled',
            'Filter': {},
            'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7},
        }])
        outcome = hygiene.ensure_lifecycle(client, 'b', 3)
        self.assertEqual(outcome, 'installed')

    def test_unsupported_write_reports_cleanly(self):
        client = FakeClient(
            lifecycle_read_error='NoSuchLifecycleConfiguration',
            lifecycle_write_error='NotImplemented',
        )
        outcome = hygiene.ensure_lifecycle(client, 'b', 3)
        self.assertTrue(outcome.startswith('unsupported'))

    def test_unsupported_read_reports_cleanly(self):
        client = FakeClient(lifecycle_read_error='MethodNotAllowed')
        outcome = hygiene.ensure_lifecycle(client, 'b', 3)
        self.assertTrue(outcome.startswith('unsupported'))
        self.assertEqual(client.lifecycle_puts, [])


class MainFlowTests(unittest.TestCase):
    """
    End-to-end through main() with the client + config patched in.
    """

    def _run(self, argv, client, aip='s3://aip-bucket/aip-store/',
             batch='s3://batch-bucket/'):
        orig_client = hygiene.wasabi._make_client
        orig_aip = hygiene.config.WASABI_AIP_BUCKET
        orig_batch = hygiene.config.WASABI_BUCKET
        hygiene.wasabi._make_client = lambda: client
        hygiene.config.WASABI_AIP_BUCKET = aip
        hygiene.config.WASABI_BUCKET = batch
        try:
            return hygiene.main(argv)
        finally:
            hygiene.wasabi._make_client = orig_client
            hygiene.config.WASABI_AIP_BUCKET = orig_aip
            hygiene.config.WASABI_BUCKET = orig_batch

    def test_report_mode_never_aborts(self):
        client = FakeClient(uploads=[_upload('old.7z', 10)])
        code = self._run(['--target', 'aip'], client)
        self.assertEqual(code, 0)
        self.assertEqual(client.aborts, [])

    def test_apply_aborts_only_stale(self):
        client = FakeClient(
            uploads=[_upload('old.7z', 10, 'u-old'), _upload('new.7z', 1, 'u-new')]
        )
        code = self._run(['--target', 'aip', '--apply'], client)
        self.assertEqual(code, 0)
        self.assertEqual(client.aborts, [('aip-bucket', 'old.7z', 'u-old')])

    def test_missing_env_returns_config_error(self):
        client = FakeClient()
        code = self._run(['--target', 'aip'], client, aip='')
        self.assertEqual(code, 1)

    def test_set_lifecycle_unsupported_returns_2(self):
        client = FakeClient(
            lifecycle_read_error='NoSuchLifecycleConfiguration',
            lifecycle_write_error='NotImplemented',
        )
        code = self._run(['--target', 'aip', '--set-lifecycle'], client)
        self.assertEqual(code, 2)

    def test_both_dedupes_shared_bucket(self):
        client = FakeClient(uploads=[_upload('old.7z', 10)])
        code = self._run(
            ['--target', 'both', '--apply'], client,
            aip='s3://same-bucket/aip-store/', batch='s3://same-bucket/',
        )
        self.assertEqual(code, 0)
        # One bucket, listed and aborted once — not twice.
        self.assertEqual(len(client.aborts), 1)


if __name__ == '__main__':
    unittest.main()
