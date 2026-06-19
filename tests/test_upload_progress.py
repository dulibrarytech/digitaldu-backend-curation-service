# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the upload-progress byte helper (_local_dir_size) added so the
repo-backend-v2 dashboard can show a byte-accurate upload % during the
Archivematica SFTP push. check_sftp uses it to report the upload total
(local 002-ingest/<uuid> size) alongside the remote uploaded byte sum.

Run:
    python -m pytest tests/test_upload_progress.py -v
"""

import os
import tempfile
import unittest

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


if __name__ == '__main__':
    unittest.main()
