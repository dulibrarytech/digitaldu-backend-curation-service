# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Regression tests for the ready-stage structure fixes
(feature-batch-packaging-qa, findings F3 + F8).

F3 — lib/archivematica_ops.py: bare os.listdir treated a loose file in
     001-ready/<batch>/ as a package name; os.listdir(<file>) then raised
     NotADirectoryError and the qa routes returned raw 500s. Worse,
     check_package_names would silently RENAME the stray file. The listers
     now keep only real directories and report loose files as errors.

F8 — lib/archivesspace_ops.get_metadata_ready_folders: unbounded os.walk
     misreported the WORKSPACE dir (loose uri.txt in a batch) or a package
     name (uri.txt nested too deep) as batch names. Scan is now bounded to
     <root>/<batch>/<package>/uri.txt.

Run:
    python -m pytest tests/test_ready_stage_structure_fixes.py -v
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lib import archivematica_ops as ops
from lib import archivesspace_ops as as_ops


def _touch(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('x')


class ReadyStageLooseFileTests(unittest.TestCase):
    """F3: loose files in a ready batch must degrade to errors, not 500s."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='curation_f3_')
        self.ready = os.path.join(self.tmp, 'ready') + '/'
        os.makedirs(self.ready)
        self.errors_file = os.path.join(self.tmp, 'errors.txt')
        self._patches = [
            patch.object(ops, 'ready_path', self.ready),
            patch.object(ops, 'errors_file', self.errors_file),
        ]
        for p in self._patches:
            p.start()

        # Batch with one real package and one loose file.
        self.folder = 'new_batch-resources_1'
        _touch(Path(self.ready) / self.folder / 'pkg_a' / 'file1.tif')
        _touch(Path(self.ready) / self.folder / 'pkg_a' / 'uri.txt')
        _touch(Path(self.ready) / self.folder / 'stray.tif')

    def tearDown(self):
        for p in self._patches:
            p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_get_package_names_returns_only_directories(self):
        self.assertEqual(ops.get_package_names(self.folder), ['pkg_a'])

    def test_check_package_names_reports_loose_file_and_does_not_rename_it(self):
        result = ops.check_package_names(self.folder)
        self.assertTrue(
            any('stray.tif' in str(e) for e in result['errors']),
            f'expected loose-file error, got: {result["errors"]}'
        )
        # The stray file must still exist under its ORIGINAL name.
        self.assertTrue((Path(self.ready) / self.folder / 'stray.tif').exists())

    def test_check_file_names_reports_loose_file_instead_of_crashing(self):
        result = ops.check_file_names(self.folder)
        self.assertTrue(
            any('stray.tif' in str(e) for e in result['errors']),
            f'expected loose-file error, got: {result["errors"]}'
        )
        # Count covers only files inside package folders (uri.txt + tif).
        self.assertEqual(result['result'], 2)

    def test_check_file_names_with_only_loose_files(self):
        folder = 'new_loose_only-resources_2'
        _touch(Path(self.ready) / folder / 'a.tif')
        result = ops.check_file_names(folder)
        self.assertEqual(result['result'], 0)
        self.assertTrue(any('a.tif' in str(e) for e in result['errors']))

    def test_check_uri_txt_skips_loose_files(self):
        result = ops.check_uri_txt(self.folder)
        # pkg_a has uri.txt; the loose file must not appear as a package.
        self.assertEqual(result['errors'], [])


class MetadataReadyFoldersBoundedScanTests(unittest.TestCase):
    """F8: /processed classification bounded to batch/package depth."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='curation_f8_'))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_correct_layout_is_detected(self):
        _touch(self.root / 'batch_a' / 'pkg' / 'uri.txt')
        _touch(self.root / 'batch_b' / 'pkg' / 'file.tif')
        self.assertEqual(
            as_ops.get_metadata_ready_folders(str(self.root)), ['batch_a']
        )

    def test_loose_uri_txt_in_batch_does_not_create_phantom_row(self):
        # Legacy walk reported the WORKSPACE dir's own name here.
        _touch(self.root / 'batch_a' / 'uri.txt')
        self.assertEqual(as_ops.get_metadata_ready_folders(str(self.root)), [])

    def test_deeply_nested_uri_txt_does_not_create_phantom_row(self):
        # Legacy walk reported 'pkg' (the package) as a batch name here.
        _touch(self.root / 'batch_a' / 'pkg' / 'nested' / 'uri.txt')
        self.assertEqual(as_ops.get_metadata_ready_folders(str(self.root)), [])

    def test_ready_and_hidden_folders_skipped(self):
        _touch(self.root / 'ready' / 'pkg' / 'uri.txt')
        _touch(self.root / '.hidden' / 'pkg' / 'uri.txt')
        self.assertEqual(as_ops.get_metadata_ready_folders(str(self.root)), [])


if __name__ == '__main__':
    unittest.main()
