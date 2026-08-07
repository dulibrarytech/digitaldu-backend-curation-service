# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for the large-AIP source routing in aip_ops.copy_aip_to_wasabi:
AIPs at or above AIP_DURACLOUD_THRESHOLD_BYTES are copied from
DuraCloud by default (Artefactual recommendation, 2026-08-03); smaller
AIPs keep the AM path; unconfigured DuraCloud falls back to AM with a
warning; threshold 0 disables routing.

Run:
    python -m pytest tests/test_copy_routing.py -v
"""

import unittest
from unittest.mock import patch

import config
from lib import aip_ops
from lib import duracloud_ops
from lib import wasabi


UUID = '43968b10-18e3-4976-b8ff-3fe9dfaadaf2'


def _meta_response(size):
    class MetaRes:
        status_code = 200

        @staticmethod
        def json():
            return {
                'status': 'UPLOADED',
                'size': size,
                'current_path': f'/store/x_{UUID}.7z',
            }
    return MetaRes()


class LargeAipRoutingTests(unittest.TestCase):

    def setUp(self):
        patches = [
            patch.object(config, 'WASABI_AIP_BUCKET', 's3://aip-bucket/aip-store/'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_API', 'https://am:8000/api'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_USERNAME', 'u'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_API_KEY', 'k'),
            patch.object(config, 'DURACLOUD_API', 'archivesdu.duracloud.org/durastore/'),
            patch.object(config, 'DURACLOUD_USER', 'u'),
            patch.object(config, 'DURACLOUD_PWD', 'p'),
            patch.object(config, 'AIP_DURACLOUD_THRESHOLD_BYTES', 1_000_000_000),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        for name in ('write_copy_progress', 'clear_copy_progress'):
            p = patch.object(aip_ops, name)
            p.start()
            self.addCleanup(p.stop)

    def _run(self, size):
        dc_result = {'ok': True, 'key': 'k', 'bytes': size, 'source': 'duracloud'}
        with patch.object(aip_ops.requests, 'get',
                          return_value=_meta_response(size)), \
                patch.object(duracloud_ops, 'copy_aip_from_duracloud',
                             return_value=dc_result) as dc_copy, \
                patch.object(wasabi, 'head_object',
                             return_value={'exists': True, 'bucket': 'aip-bucket',
                                           'content_length': size}) as head:
            result = aip_ops.copy_aip_to_wasabi(UUID, 'pid-1')
        return result, dc_copy, head

    def test_routes_large_aip_to_duracloud(self):
        result, dc_copy, head = self._run(66_163_797_416)
        dc_copy.assert_called_once_with(UUID, 'pid-1')
        self.assertEqual(result['source'], 'duracloud')
        # The AM path's own probe never ran — routing happened first.
        head.assert_not_called()

    def test_exactly_at_threshold_routes(self):
        _result, dc_copy, _head = self._run(1_000_000_000)
        dc_copy.assert_called_once()

    def test_small_aip_keeps_am_path(self):
        result, dc_copy, head = self._run(327_204_835)
        dc_copy.assert_not_called()
        head.assert_called_once()
        # Idempotent AM-path short-circuit (object exists at size).
        self.assertTrue(result['ok'])
        self.assertNotIn('source', result)

    def test_threshold_zero_disables_routing(self):
        with patch.object(config, 'AIP_DURACLOUD_THRESHOLD_BYTES', 0):
            _result, dc_copy, head = self._run(66_163_797_416)
        dc_copy.assert_not_called()
        head.assert_called_once()

    def test_unconfigured_duracloud_falls_back_to_am(self):
        with patch.object(config, 'DURACLOUD_API', None):
            result, dc_copy, head = self._run(66_163_797_416)
        dc_copy.assert_not_called()
        head.assert_called_once()
        self.assertTrue(result['ok'])

    def test_config_default_is_one_gb(self):
        self.assertEqual(config.AIP_DURACLOUD_THRESHOLD_BYTES, 1_000_000_000)


if __name__ == '__main__':
    unittest.main()
