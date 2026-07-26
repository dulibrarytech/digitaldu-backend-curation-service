# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Unit tests for scripts/reconcile_ingested_wasabi.py (003-ingested
retirement plan, verification gate).

Covers the pure comparison logic, the local manifest builder's
upload-filter parity (dot-files skipped, symlinks counted), the remote
manifest prefix-stripping, and the per-batch verdict mapping — all
without any real S3: the client is faked with canned pages.

Run:
    python -m pytest tests/test_reconcile_ingested_wasabi.py -v
"""

import shutil
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts import reconcile_ingested_wasabi as recon  # noqa: E402


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
    """Minimal stand-in for the boto3 S3 client (list side only)."""

    def __init__(self, keys_with_sizes, prefix):
        contents = [
            {'Key': prefix + rel, 'Size': size}
            for rel, size in keys_with_sizes.items()
        ]
        # Split into two pages to exercise pagination handling.
        mid = len(contents) // 2
        self._pages = [
            {'Contents': contents[:mid]},
            {'Contents': contents[mid:]},
        ]

    def get_paginator(self, _name):
        return FakePaginator(self._pages)


class CompareManifestsTests(unittest.TestCase):

    def test_identical_manifests_verify(self):
        local = {'pkg/a.tif': 10, 'pkg/b.tif': 20}
        result = recon.compare_manifests(local, dict(local))
        self.assertEqual(result['missing'], [])
        self.assertEqual(result['size_mismatch'], [])
        self.assertEqual(result['extra'], [])
        self.assertEqual(result['matched'], 2)
        self.assertEqual(recon.verdict_for(len(local), result), 'VERIFIED')

    def test_missing_remote_file_detected(self):
        local = {'pkg/a.tif': 10, 'pkg/b.tif': 20}
        remote = {'pkg/a.tif': 10}
        result = recon.compare_manifests(local, remote)
        self.assertEqual(result['missing'], ['pkg/b.tif'])
        self.assertEqual(recon.verdict_for(len(local), result), 'MISSING')

    def test_size_mismatch_detected(self):
        local = {'pkg/a.tif': 10}
        remote = {'pkg/a.tif': 9}
        result = recon.compare_manifests(local, remote)
        self.assertEqual(result['size_mismatch'], [('pkg/a.tif', 10, 9)])
        self.assertEqual(recon.verdict_for(len(local), result), 'MISMATCH')

    def test_missing_outranks_mismatch_in_verdict(self):
        local = {'a': 1, 'b': 2}
        remote = {'b': 3}
        result = recon.compare_manifests(local, remote)
        self.assertEqual(recon.verdict_for(len(local), result), 'MISSING')

    def test_extra_remote_objects_do_not_block_verified(self):
        local = {'pkg/a.tif': 10}
        remote = {'pkg/a.tif': 10, 'pkg/old_leftover.tif': 5}
        result = recon.compare_manifests(local, remote)
        self.assertEqual(result['extra'], ['pkg/old_leftover.tif'])
        self.assertEqual(recon.verdict_for(len(local), result), 'VERIFIED')

    def test_empty_local_is_flagged_not_verified(self):
        result = recon.compare_manifests({}, {'pkg/a.tif': 10})
        self.assertEqual(recon.verdict_for(0, result), 'EMPTY_LOCAL')


class LocalManifestTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='recon_local_'))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_walk_collects_relative_posix_paths_and_sizes(self):
        _touch(self.tmp / 'pkg_a' / 'file1.tif', b'12345')
        _touch(self.tmp / 'pkg_a' / 'uri.txt', b'/repositories/2/x')
        _touch(self.tmp / 'pkg_b' / 'nested' / 'deep.pdf', b'123')

        manifest, symlinks = recon.build_local_manifest(self.tmp)

        self.assertEqual(manifest, {
            'pkg_a/file1.tif': 5,
            'pkg_a/uri.txt': 17,
            'pkg_b/nested/deep.pdf': 3,
        })
        self.assertEqual(symlinks, 0)

    def test_dot_files_skipped_matching_upload_filter(self):
        _touch(self.tmp / 'pkg_a' / 'file1.tif')
        _touch(self.tmp / 'pkg_a' / '.DS_Store')
        _touch(self.tmp / '.hidden_root_file')

        manifest, _ = recon.build_local_manifest(self.tmp)
        self.assertEqual(list(manifest), ['pkg_a/file1.tif'])

    def test_dangling_symlink_counted_not_fatal(self):
        _touch(self.tmp / 'pkg_a' / 'file1.tif')
        (self.tmp / 'pkg_a' / 'gone.tif').symlink_to(self.tmp / 'nonexistent')

        manifest, symlinks = recon.build_local_manifest(self.tmp)
        self.assertEqual(list(manifest), ['pkg_a/file1.tif'])
        self.assertEqual(symlinks, 1)


class RemoteManifestTests(unittest.TestCase):

    def test_prefix_stripped_and_pages_merged(self):
        prefix = 'base/codu_100/'
        keys = {'pkg_a/file1.tif': 5, 'pkg_a/uri.txt': 17, 'pkg_b/x.pdf': 3}
        client = FakeClient(keys, prefix)

        manifest = recon.build_remote_manifest(client, 'bucket', prefix)
        self.assertEqual(manifest, keys)

    def test_prefix_marker_key_ignored(self):
        # A zero-byte "directory marker" object AT the prefix itself
        # (some S3 tools create these) must not appear as rel=''.
        prefix = 'codu_100/'
        client = FakeClient({'': 0, 'pkg/a.tif': 1}, prefix)
        manifest = recon.build_remote_manifest(client, 'bucket', prefix)
        self.assertEqual(manifest, {'pkg/a.tif': 1})


class ReconcileBatchTests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='recon_batch_'))
        self.ingested = self.tmp / '003-ingested'
        self.batch = 'codu_100'
        _touch(self.ingested / self.batch / 'pkg_a' / 'file1.tif', b'12345')
        _touch(self.ingested / self.batch / 'pkg_a' / 'uri.txt', b'uri')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_verified_batch_report(self):
        prefix = 'base/codu_100/'
        client = FakeClient({'pkg_a/file1.tif': 5, 'pkg_a/uri.txt': 3}, prefix)

        report = recon.reconcile_batch(
            client, 'bucket', 'base/', str(self.ingested), self.batch, False
        )
        self.assertEqual(report['verdict'], 'VERIFIED')
        self.assertEqual(report['local_files'], 2)
        self.assertEqual(report['remote_objects'], 2)
        self.assertEqual(report['matched'], 2)
        self.assertEqual(report['error'], None)

    def test_missing_batch_report_lists_files(self):
        prefix = 'base/codu_100/'
        client = FakeClient({'pkg_a/uri.txt': 3}, prefix)

        report = recon.reconcile_batch(
            client, 'bucket', 'base/', str(self.ingested), self.batch, False
        )
        self.assertEqual(report['verdict'], 'MISSING')
        self.assertEqual(report['missing'], ['pkg_a/file1.tif'])

    def test_listing_error_yields_error_verdict(self):
        class ExplodingClient:
            def get_paginator(self, _name):
                raise RuntimeError('boom')

        report = recon.reconcile_batch(
            ExplodingClient(), 'bucket', '', str(self.ingested), self.batch, False
        )
        self.assertEqual(report['verdict'], 'ERROR')
        self.assertIn('boom', report['error'])


class ListLocalBatchesTests(unittest.TestCase):

    def test_sorted_non_hidden_dirs_only(self):
        tmp = Path(tempfile.mkdtemp(prefix='recon_list_'))
        try:
            (tmp / 'codu_b').mkdir()
            (tmp / 'codu_a').mkdir()
            (tmp / '.hidden').mkdir()
            _touch(tmp / 'loose_file.txt')
            self.assertEqual(recon.list_local_batches(tmp), ['codu_a', 'codu_b'])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_missing_root_raises(self):
        with self.assertRaises(FileNotFoundError):
            recon.list_local_batches('/nonexistent/path/for/test')


if __name__ == '__main__':
    unittest.main()
