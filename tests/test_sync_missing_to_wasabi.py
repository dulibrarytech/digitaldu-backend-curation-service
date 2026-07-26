# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Unit tests for scripts/sync_missing_to_wasabi.py (003-ingested
remediation uploader).

Covers the pure upload-planning logic, the summary-CSV batch selection,
and sync_batch's safety behavior: dry-run never uploads, execute uploads
missing files only, size-mismatched files are skipped without
--overwrite-mismatch, and the post-upload head_object verification
counts a size disagreement as failed.

Run:
    python -m pytest tests/test_sync_missing_to_wasabi.py -v
"""

import csv
import shutil
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts import sync_missing_to_wasabi as sync  # noqa: E402


def _touch(path, content=b'x'):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


class FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, Bucket=None, Prefix=None):  # noqa: N803 - boto3 casing
        for page in self._pages:
            yield page


class FakeClient:
    """S3 client fake: canned listing + recording uploads + head_object."""

    def __init__(self, remote_keys_with_sizes, prefix, head_size_override=None):
        self._remote = dict(remote_keys_with_sizes)
        self._prefix = prefix
        self._head_size_override = head_size_override
        self.uploads = []

    def get_paginator(self, _name):
        contents = [
            {'Key': self._prefix + rel, 'Size': size}
            for rel, size in self._remote.items()
        ]
        return FakePaginator([{'Contents': contents}])

    def upload_file(self, local_path, bucket, key):
        self.uploads.append((local_path, bucket, key))
        rel = key[len(self._prefix):]
        self._remote[rel] = Path(local_path).stat().st_size

    def head_object(self, Bucket=None, Key=None):  # noqa: N803 - boto3 casing
        rel = Key[len(self._prefix):]
        size = self._remote[rel]
        if self._head_size_override is not None:
            size = self._head_size_override
        return {'ContentLength': size}


class PlanUploadsTests(unittest.TestCase):

    def test_missing_and_mismatched_partitioned(self):
        local = {'a': 1, 'b': 2, 'c': 3}
        remote = {'b': 2, 'c': 99}
        missing, mismatched = sync.plan_uploads(local, remote)
        self.assertEqual(missing, [('a', 1)])
        self.assertEqual(mismatched, [('c', 3, 99)])

    def test_fully_synced_plans_nothing(self):
        local = {'a': 1}
        missing, mismatched = sync.plan_uploads(local, dict(local))
        self.assertEqual(missing, [])
        self.assertEqual(mismatched, [])


class BatchesFromSummaryTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='sync_csv_'))
        self.csv_path = self.tmp / 'summary.csv'
        with open(self.csv_path, 'w', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=['batch', 'verdict'])
            writer.writeheader()
            writer.writerow({'batch': 'ok_batch', 'verdict': 'VERIFIED'})
            writer.writerow({'batch': 'gone_batch', 'verdict': 'MISSING'})
            writer.writerow({'batch': 'odd_batch', 'verdict': 'MISMATCH'})
            writer.writerow({'batch': 'empty_batch', 'verdict': 'EMPTY_LOCAL'})

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_missing_only_by_default(self):
        self.assertEqual(
            sync.batches_from_summary(self.csv_path, include_mismatch=False),
            ['gone_batch'],
        )

    def test_mismatch_included_when_opted_in(self):
        self.assertEqual(
            sync.batches_from_summary(self.csv_path, include_mismatch=True),
            ['gone_batch', 'odd_batch'],
        )


class SyncBatchTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='sync_batch_'))
        self.ingested = self.tmp / '003-ingested'
        self.batch = 'codu_100'
        _touch(self.ingested / self.batch / 'pkg_a' / 'present.tif', b'12345')
        _touch(self.ingested / self.batch / 'pkg_a' / 'absent.tif', b'123')
        _touch(self.ingested / self.batch / 'pkg_a' / '.DS_Store', b'junk')
        self.prefix = 'codu_100/'

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _client(self, **kwargs):
        # Remote already has present.tif at the right size.
        return FakeClient({'pkg_a/present.tif': 5}, self.prefix, **kwargs)

    def test_dry_run_plans_but_never_uploads(self):
        client = self._client()
        report = sync.sync_batch(
            client, 'bucket', '', str(self.ingested), self.batch,
            execute=False, overwrite_mismatch=False,
        )
        self.assertEqual(report['planned'], 1)
        self.assertEqual(report['uploaded'], 0)
        self.assertEqual(client.uploads, [])

    def test_execute_uploads_only_missing_and_verifies(self):
        client = self._client()
        report = sync.sync_batch(
            client, 'bucket', '', str(self.ingested), self.batch,
            execute=True, overwrite_mismatch=False,
        )
        self.assertEqual(report['uploaded'], 1)
        self.assertEqual(report['verify_failed'], 0)
        self.assertEqual(len(client.uploads), 1)
        self.assertEqual(client.uploads[0][2], 'codu_100/pkg_a/absent.tif')
        # Dotfile never uploaded.
        self.assertNotIn('.DS_Store', client.uploads[0][2])

    def test_mismatch_skipped_without_flag(self):
        client = FakeClient({'pkg_a/present.tif': 999,   # wrong size remotely
                             'pkg_a/absent.tif': 3}, self.prefix)
        report = sync.sync_batch(
            client, 'bucket', '', str(self.ingested), self.batch,
            execute=True, overwrite_mismatch=False,
        )
        self.assertEqual(report['skipped_mismatch'], 1)
        self.assertEqual(report['uploaded'], 0)
        self.assertEqual(client.uploads, [])

    def test_mismatch_uploaded_with_flag(self):
        client = FakeClient({'pkg_a/present.tif': 999,
                             'pkg_a/absent.tif': 3}, self.prefix)
        report = sync.sync_batch(
            client, 'bucket', '', str(self.ingested), self.batch,
            execute=True, overwrite_mismatch=True,
        )
        self.assertEqual(report['skipped_mismatch'], 0)
        self.assertEqual(report['uploaded'], 1)
        self.assertEqual(client.uploads[0][2], 'codu_100/pkg_a/present.tif')

    def test_head_verification_failure_counts_as_failed(self):
        client = self._client(head_size_override=42)
        report = sync.sync_batch(
            client, 'bucket', '', str(self.ingested), self.batch,
            execute=True, overwrite_mismatch=False,
        )
        self.assertEqual(report['uploaded'], 0)
        self.assertEqual(report['verify_failed'], 1)

    def test_listing_error_isolated(self):
        class ExplodingClient:
            def get_paginator(self, _name):
                raise RuntimeError('boom')

        report = sync.sync_batch(
            ExplodingClient(), 'bucket', '', str(self.ingested), self.batch,
            execute=True, overwrite_mismatch=False,
        )
        self.assertIn('boom', report['error'])
        self.assertEqual(report['uploaded'], 0)


if __name__ == '__main__':
    unittest.main()
