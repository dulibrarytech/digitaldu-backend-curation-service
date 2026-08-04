# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Behavior tests for the phase-3 move_to_ingested rewrite (003-ingested
retirement — repo/INGESTED_RETIREMENT_PLAN.md).

Pins the new contract:
  * No local archive copy is written anywhere (ingested_path retired).
  * Upload goes straight from 002-ingest/<uuid>/ to Wasabi with the
    batch name (minus `new_`) as the S3 prefix.
  * The staging source is removed ONLY on verified S3 success
    (2026-05-24 data-loss invariant, now backed by per-file
    head_object verification inside wasabi.upload_directory).
  * On S3 failure the source is preserved and the error is reported —
    repov2 Stage 5 turns that into a FAILED archive_to_wasabi job.
  * Missing source is an explicit error, not a silent success.

Run:
    python -m pytest tests/test_move_to_ingested_phase3.py -v
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lib import archivematica_ops as ops


class MoveToIngestedPhase3Tests(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='curation_phase3_'))
        self.ready = str(self.tmp / 'ready') + '/'
        self.ingest = str(self.tmp / 'ingest') + '/'
        os.makedirs(self.ready)
        os.makedirs(self.ingest)
        self._patches = [
            patch.object(ops, 'ready_path', self.ready),
            patch.object(ops, 'ingest_path', self.ingest),
            # reset_permissions shells out to chgrp/chmod; neutralize it.
            patch.object(ops, 'reset_permissions', lambda folder: 'ok'),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _seed_source(self, uuid='col-uuid-1'):
        src = Path(self.ingest) / uuid
        (src / 'pkg_a').mkdir(parents=True)
        (src / 'pkg_a' / 'file1.tif').write_text('x')
        return src

    def test_success_uploads_with_stripped_prefix_and_removes_source(self):
        src = self._seed_source()
        with patch.object(ops, 'move_to_s3', return_value=0) as m_s3:
            result = ops.move_to_ingested('col-uuid-1', 'new_my_collection-resources_9')

        m_s3.assert_called_once_with(str(src) + '/', 'my_collection-resources_9')
        self.assertEqual(result['result'], 'packages_moved_to_ingested_folder')
        self.assertEqual(result['errors'], [])
        self.assertFalse(src.exists())
        # Nothing was written anywhere else under the tmp tree — the
        # 003-ingested copy is gone from the workflow.
        self.assertEqual(
            sorted(p.name for p in self.tmp.iterdir()), ['ingest', 'ready']
        )

    def test_s3_failure_preserves_source_and_reports_error(self):
        src = self._seed_source()
        with patch.object(ops, 'move_to_s3', return_value=1):
            result = ops.move_to_ingested('col-uuid-1', 'new_my_collection-resources_9')

        self.assertEqual(result['result'], 'packages_not_moved_to_ingested_folder')
        self.assertTrue(any('wasabi' in e for e in result['errors']))
        self.assertTrue(src.exists())
        self.assertTrue((src / 'pkg_a' / 'file1.tif').exists())

    def test_missing_source_is_an_error(self):
        with patch.object(ops, 'move_to_s3', return_value=0) as m_s3:
            result = ops.move_to_ingested('nope-uuid', 'new_x-resources_1')

        m_s3.assert_not_called()
        self.assertEqual(result['result'], 'packages_not_moved_to_ingested_folder')
        self.assertTrue(any('Source not found' in e for e in result['errors']))

    def test_folder_without_new_prefix_used_verbatim(self):
        src = self._seed_source('col-uuid-2')
        with patch.object(ops, 'move_to_s3', return_value=0) as m_s3:
            ops.move_to_ingested('col-uuid-2', 'legacy_folder')
        m_s3.assert_called_once_with(str(src) + '/', 'legacy_folder')

    def test_move_to_s3_exception_is_contained(self):
        src = self._seed_source('col-uuid-3')
        with patch.object(ops, 'move_to_s3', side_effect=RuntimeError('boom')):
            result = ops.move_to_ingested('col-uuid-3', 'new_c-resources_2')
        self.assertEqual(result['result'], 'packages_not_moved_to_ingested_folder')
        self.assertTrue(result['errors'])
        self.assertTrue(src.exists())


if __name__ == '__main__':
    unittest.main()
