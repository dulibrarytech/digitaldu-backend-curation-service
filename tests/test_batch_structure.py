# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Unit tests for lib/batch_structure.py (feature-batch-packaging-qa).

Each test builds a throwaway directory tree and asserts the flags the
scan produces. Codes under test map to findings F1-F9 in
repo/BATCH_PACKAGING_QA_FINDINGS.md.

Run:
    python -m pytest tests/test_batch_structure.py -v
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path

from lib import batch_structure as bs


GOOD_BATCH = 'new_test_collection-resources_123'


def _touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('x')


class ScanBatchTests(unittest.TestCase):

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='batch_structure_test_'))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _make_batch(self, name=GOOD_BATCH):
        batch = self.root / name
        batch.mkdir()
        return batch

    def _codes(self, scan):
        return {f['code'] for f in scan['structure_errors']}

    def _flag(self, scan, code):
        for f in scan['structure_errors']:
            if f['code'] == code:
                return f
        return None

    # --- well-formed batch -------------------------------------------

    def test_clean_batch_has_no_flags(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'file1.tif')
        _touch(batch / 'pkg_b' / 'file1.pdf')

        scan = bs.scan_batch(batch)

        self.assertEqual(scan['name'], GOOD_BATCH)
        self.assertEqual(scan['packages'], ['pkg_a', 'pkg_b'])
        self.assertEqual(scan['processed'], [])
        self.assertEqual(scan['structure_errors'], [])

    def test_archival_objects_tail_is_accepted(self):
        batch = self._make_batch('new_x-archival_objects_77')
        _touch(batch / 'pkg' / 'f.tif')
        self.assertEqual(bs.scan_batch(batch)['structure_errors'], [])

    def test_junk_and_hidden_files_are_ignored(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'file1.tif')
        _touch(batch / '.DS_Store')
        _touch(batch / 'Thumbs.db')
        _touch(batch / 'pkg_a' / '.hidden')
        _touch(batch / 'pkg_a' / 'Thumbs.db')

        scan = bs.scan_batch(batch)
        self.assertEqual(scan['structure_errors'], [])

    # --- F1: no packages ---------------------------------------------

    def test_loose_files_only_flags_no_packages_and_loose_files(self):
        batch = self._make_batch()
        _touch(batch / 'scan1.tif')
        _touch(batch / 'scan2.tif')

        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'no_packages', 'loose_files'})
        loose = self._flag(scan, 'loose_files')
        self.assertEqual(loose['items'], ['scan1.tif', 'scan2.tif'])
        self.assertEqual(loose['total'], 2)
        self.assertEqual(loose['severity'], 'error')

    def test_empty_batch_folder_flags_no_packages(self):
        batch = self._make_batch()
        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'no_packages'})

    # --- F2: mixed loose files + packages ----------------------------

    def test_mixed_batch_flags_loose_files_only(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'file1.tif')
        _touch(batch / 'notes.docx')

        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'loose_files'})
        self.assertEqual(self._flag(scan, 'loose_files')['items'], ['notes.docx'])

    # --- F4: nested directories --------------------------------------

    def test_nested_dirs_flagged_with_package_prefix(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'originals' / 'file1.tif')
        _touch(batch / 'pkg_a' / 'file0.tif')

        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'nested_dirs'})
        self.assertEqual(
            self._flag(scan, 'nested_dirs')['items'], ['pkg_a/originals']
        )

    def test_package_with_only_a_subdir_flags_nested_not_empty(self):
        # Zero top-level files but a subdir: nested_dirs already blocks;
        # empty_package would double-report the same folder.
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'originals' / 'file1.tif')

        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'nested_dirs'})

    # --- F5: empty package -------------------------------------------

    def test_empty_package_flagged(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'file1.tif')
        (batch / 'pkg_b').mkdir()

        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'empty_package'})
        self.assertEqual(self._flag(scan, 'empty_package')['items'], ['pkg_b'])

    def test_package_with_only_uri_txt_counts_as_empty(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'uri.txt')
        _touch(batch / 'pkg_b' / 'file1.tif')

        scan = bs.scan_batch(batch)
        self.assertIn('empty_package', self._codes(scan))
        self.assertEqual(self._flag(scan, 'empty_package')['items'], ['pkg_a'])

    # --- F6: partial processing --------------------------------------

    def test_partially_processed_lists_unprocessed_packages(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'file1.tif')
        _touch(batch / 'pkg_a' / 'uri.txt')
        _touch(batch / 'pkg_b' / 'file1.tif')

        scan = bs.scan_batch(batch)
        self.assertEqual(scan['processed'], ['pkg_a'])
        self.assertEqual(self._codes(scan), {'partially_processed'})
        flag = self._flag(scan, 'partially_processed')
        self.assertEqual(flag['items'], ['pkg_b'])
        self.assertEqual(flag['severity'], 'info')

    def test_fully_processed_batch_has_no_partial_flag(self):
        batch = self._make_batch()
        _touch(batch / 'pkg_a' / 'file1.tif')
        _touch(batch / 'pkg_a' / 'uri.txt')

        scan = bs.scan_batch(batch)
        self.assertEqual(scan['processed'], ['pkg_a'])
        self.assertEqual(scan['structure_errors'], [])

    # --- F7: folder naming -------------------------------------------

    def test_bad_folder_name_subcodes(self):
        batch = self._make_batch('my_collection')
        _touch(batch / 'pkg_a' / 'file1.tif')

        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'bad_folder_name'})
        self.assertEqual(
            sorted(self._flag(scan, 'bad_folder_name')['items']),
            ['missing_new_prefix', 'missing_resources_id_tail'],
        )

    def test_folder_name_missing_only_the_id_tail(self):
        batch = self._make_batch('new_collection-resources_abc')
        _touch(batch / 'pkg_a' / 'file1.tif')

        scan = bs.scan_batch(batch)
        self.assertEqual(
            self._flag(scan, 'bad_folder_name')['items'],
            ['missing_resources_id_tail'],
        )

    # --- F9: name hygiene --------------------------------------------

    def test_spaces_in_names_are_warnings(self):
        batch = self._make_batch()
        _touch(batch / 'pkg a' / 'my file.tif')
        _touch(batch / 'pkg_b' / 'file1.tif')

        scan = bs.scan_batch(batch)
        self.assertEqual(self._codes(scan), {'name_hygiene'})
        flag = self._flag(scan, 'name_hygiene')
        self.assertEqual(flag['severity'], 'warn')
        self.assertEqual(sorted(flag['items']), ['pkg a', 'pkg a/my file.tif'])
        self.assertFalse(bs.has_blocking_errors(scan))

    # --- caps ---------------------------------------------------------

    def test_items_capped_but_total_accurate(self):
        batch = self._make_batch()
        for i in range(bs.ITEMS_CAP + 5):
            _touch(batch / f'loose_{i:03d}.tif')

        scan = bs.scan_batch(batch)
        loose = self._flag(scan, 'loose_files')
        self.assertEqual(len(loose['items']), bs.ITEMS_CAP)
        self.assertEqual(loose['total'], bs.ITEMS_CAP + 5)

    # --- severity helper ---------------------------------------------

    def test_has_blocking_errors(self):
        batch = self._make_batch()
        _touch(batch / 'stray.tif')
        _touch(batch / 'pkg_a' / 'file1.tif')
        self.assertTrue(bs.has_blocking_errors(bs.scan_batch(batch)))

        clean = self._make_batch('new_clean-resources_9')
        _touch(clean / 'pkg_a' / 'file1.tif')
        self.assertFalse(bs.has_blocking_errors(bs.scan_batch(clean)))


class GetWorkspaceBatchesTests(unittest.TestCase):

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='batch_structure_ws_'))

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _names(self, batches):
        return [b['name'] for b in batches]

    def test_includes_unprocessed_and_malformed_excludes_processed(self):
        # Normal unprocessed batch.
        _touch(self.root / 'new_a-resources_1' / 'pkg' / 'f.tif')
        # Loose-files-only batch — previously invisible (F1).
        _touch(self.root / 'new_b-resources_2' / 'stray.tif')
        # Partially processed — previously vanished from this view (F6).
        _touch(self.root / 'new_c-resources_3' / 'pkg1' / 'uri.txt')
        _touch(self.root / 'new_c-resources_3' / 'pkg1' / 'f.tif')
        _touch(self.root / 'new_c-resources_3' / 'pkg2' / 'f.tif')
        # Fully processed — belongs to the QA view, excluded here.
        _touch(self.root / 'new_d-resources_4' / 'pkg' / 'uri.txt')
        _touch(self.root / 'new_d-resources_4' / 'pkg' / 'f.tif')
        # Skipped names.
        (self.root / 'ready').mkdir()
        (self.root / '.hidden_dir').mkdir()
        _touch(self.root / 'loose_at_root.txt')

        batches = bs.get_workspace_batches(self.root)

        self.assertEqual(
            self._names(batches),
            ['new_a-resources_1', 'new_b-resources_2', 'new_c-resources_3'],
        )

        by_name = {b['name']: b for b in batches}
        self.assertEqual(by_name['new_a-resources_1']['structure_errors'], [])
        self.assertEqual(
            {f['code'] for f in by_name['new_b-resources_2']['structure_errors']},
            {'no_packages', 'loose_files'},
        )
        self.assertEqual(
            {f['code'] for f in by_name['new_c-resources_3']['structure_errors']},
            {'partially_processed'},
        )

    def test_empty_batch_folder_is_included_and_flagged(self):
        (self.root / 'new_empty-resources_5').mkdir()
        batches = bs.get_workspace_batches(self.root)
        self.assertEqual(self._names(batches), ['new_empty-resources_5'])
        self.assertEqual(
            [f['code'] for f in batches[0]['structure_errors']],
            ['no_packages'],
        )

    def test_missing_root_raises(self):
        with self.assertRaises(FileNotFoundError):
            bs.get_workspace_batches(self.root / 'does_not_exist')


if __name__ == '__main__':
    unittest.main()
