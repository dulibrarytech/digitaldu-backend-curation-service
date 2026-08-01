# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Unit tests for scripts/restore_ingested_from_wasabi.py (Wasabi → 003-ingested
package/file restore).

Covers the pure selection/planning logic and the download safety behavior:
dry-run never downloads, only locally-missing files are fetched, size-
mismatched local files are skipped without --overwrite-mismatch, and a
short/failed transfer never leaves a .part or a plausible-looking partial
file in place.

Run:
    python -m pytest tests/test_restore_ingested_from_wasabi.py -v
"""

import shutil
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts import restore_ingested_from_wasabi as restore  # noqa: E402


class FakeClient:
    """S3 client fake: canned {rel: size} listing + recording downloads.

    download_file writes `size` bytes of filler (or `short_bytes` when set,
    to simulate a truncated transfer), or raises when `raise_on` matches.
    """

    def __init__(self, remote, prefix, short_bytes=None, raise_on=None):
        self._remote = dict(remote)
        self._prefix = prefix
        self._short_bytes = short_bytes
        self._raise_on = raise_on
        self.downloads = []

    def download_file(self, bucket, key, filename):
        rel = key[len(self._prefix):]
        if self._raise_on and rel == self._raise_on:
            raise IOError('connection reset by Wasabi')
        self.downloads.append((bucket, key, filename))
        size = self._remote[rel]
        if self._short_bytes is not None:
            size = self._short_bytes
        Path(filename).write_bytes(b'x' * size)


class SelectRemoteTests(unittest.TestCase):

    REMOTE = {
        'pkg_a/one.tif': 10,
        'pkg_a/sub/two.tif': 20,
        'pkg_b/three.tif': 30,
        'loose.txt': 5,
    }

    def test_package_selector_matches_by_prefix(self):
        selected, unmatched = restore.select_remote(self.REMOTE, ['pkg_a'], None)
        self.assertEqual(set(selected), {'pkg_a/one.tif', 'pkg_a/sub/two.tif'})
        self.assertEqual(unmatched, [])

    def test_package_selector_does_not_match_partial_names(self):
        # 'pkg' must not glob onto pkg_a/pkg_b — prefix match is on 'pkg/'.
        selected, unmatched = restore.select_remote(self.REMOTE, ['pkg'], None)
        self.assertEqual(selected, {})
        self.assertEqual(unmatched, [('package', 'pkg')])

    def test_file_selector_exact_match_and_miss(self):
        selected, unmatched = restore.select_remote(
            self.REMOTE, None, ['pkg_b/three.tif', 'pkg_b/nope.tif'])
        self.assertEqual(set(selected), {'pkg_b/three.tif'})
        self.assertEqual(unmatched, [('file', 'pkg_b/nope.tif')])

    def test_selectors_union_without_duplicates(self):
        selected, unmatched = restore.select_remote(
            self.REMOTE, ['pkg_a'], ['pkg_a/one.tif', 'loose.txt'])
        self.assertEqual(
            set(selected),
            {'pkg_a/one.tif', 'pkg_a/sub/two.tif', 'loose.txt'})
        self.assertEqual(unmatched, [])


class PlanDownloadsTests(unittest.TestCase):

    def test_partitions_missing_mismatched_present(self):
        selected = {'a': 1, 'b': 2, 'c': 3}
        local = {'b': 2, 'c': 99}
        missing, mismatched, present = restore.plan_downloads(selected, local)
        self.assertEqual(missing, [('a', 1)])
        self.assertEqual(mismatched, [('c', 3, 99)])
        self.assertEqual(present, 1)

    def test_fully_present_plans_nothing(self):
        selected = {'a': 1}
        missing, mismatched, present = restore.plan_downloads(
            selected, dict(selected))
        self.assertEqual(missing, [])
        self.assertEqual(mismatched, [])
        self.assertEqual(present, 1)


class GroupByPackageTests(unittest.TestCase):

    def test_groups_counts_and_bytes(self):
        remote = {
            'pkg_a/one.tif': 10,
            'pkg_a/two.tif': 20,
            'pkg_b/three.tif': 30,
            'loose.txt': 5,
        }
        groups = restore.group_by_package(remote)
        self.assertEqual(groups['pkg_a'], (2, 30))
        self.assertEqual(groups['pkg_b'], (1, 30))
        self.assertEqual(groups['(batch root)'], (1, 5))


class DownloadOneTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='restore_'))
        self.prefix = 'codu_100/'

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_success_renames_part_into_place(self):
        client = FakeClient({'pkg_a/one.tif': 10}, self.prefix)
        dest = self.tmp / 'pkg_a' / 'one.tif'
        error = restore.download_one(
            client, 'bucket', self.prefix + 'pkg_a/one.tif', dest, 10)
        self.assertIsNone(error)
        self.assertEqual(dest.stat().st_size, 10)
        self.assertFalse(Path(str(dest) + '.part').exists())

    def test_short_transfer_leaves_no_part_and_no_dest(self):
        client = FakeClient({'pkg_a/one.tif': 10}, self.prefix, short_bytes=4)
        dest = self.tmp / 'pkg_a' / 'one.tif'
        error = restore.download_one(
            client, 'bucket', self.prefix + 'pkg_a/one.tif', dest, 10)
        self.assertIn('size 4', error)
        self.assertFalse(dest.exists())
        self.assertFalse(Path(str(dest) + '.part').exists())

    def test_transport_error_cleans_up_part(self):
        client = FakeClient({'pkg_a/one.tif': 10}, self.prefix,
                            raise_on='pkg_a/one.tif')
        dest = self.tmp / 'pkg_a' / 'one.tif'
        error = restore.download_one(
            client, 'bucket', self.prefix + 'pkg_a/one.tif', dest, 10)
        self.assertIn('connection reset', error)
        self.assertFalse(dest.exists())
        self.assertFalse(Path(str(dest) + '.part').exists())

    def test_overwrite_replaces_existing_file(self):
        dest = self.tmp / 'pkg_a' / 'one.tif'
        dest.parent.mkdir(parents=True)
        dest.write_bytes(b'old-and-wrong')
        client = FakeClient({'pkg_a/one.tif': 10}, self.prefix)
        error = restore.download_one(
            client, 'bucket', self.prefix + 'pkg_a/one.tif', dest, 10)
        self.assertIsNone(error)
        self.assertEqual(dest.read_bytes(), b'x' * 10)


class RestoreTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='restore_batch_'))
        self.batch_path = self.tmp / 'codu_100'
        self.prefix = 'codu_100/'
        self.downloads = [('pkg_a/one.tif', 10), ('pkg_a/two.tif', 20)]

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_dry_run_plans_but_never_downloads(self):
        client = FakeClient(dict(self.downloads), self.prefix)
        report = restore.restore(
            client, 'bucket', self.prefix, self.batch_path,
            self.downloads, execute=False)
        self.assertEqual(client.downloads, [])
        self.assertEqual(report['downloaded'], 0)
        self.assertFalse(self.batch_path.exists())

    def test_execute_downloads_all_and_counts_bytes(self):
        client = FakeClient(dict(self.downloads), self.prefix)
        report = restore.restore(
            client, 'bucket', self.prefix, self.batch_path,
            self.downloads, execute=True)
        self.assertEqual(report['downloaded'], 2)
        self.assertEqual(report['failed'], 0)
        self.assertEqual(report['bytes_downloaded'], 30)
        self.assertEqual((self.batch_path / 'pkg_a' / 'one.tif').stat().st_size, 10)
        self.assertEqual((self.batch_path / 'pkg_a' / 'two.tif').stat().st_size, 20)

    def test_one_failure_marks_failed_but_continues(self):
        client = FakeClient(dict(self.downloads), self.prefix,
                            raise_on='pkg_a/one.tif')
        report = restore.restore(
            client, 'bucket', self.prefix, self.batch_path,
            self.downloads, execute=True)
        self.assertEqual(report['downloaded'], 1)
        self.assertEqual(report['failed'], 1)
        self.assertTrue((self.batch_path / 'pkg_a' / 'two.tif').exists())
        self.assertFalse((self.batch_path / 'pkg_a' / 'one.tif').exists())


if __name__ == '__main__':
    unittest.main()
