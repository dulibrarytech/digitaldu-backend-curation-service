# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Tests for POST /api/v2/dip/copy-to-wasabi (routes/dip.py) and the
dip_ops.copy_dip it runs: validation, the completeness gate, the copy of
the served files only, the resume, and the failure shapes (200 with
ok=false, never a 5xx).

DCP and Wasabi are the in-memory fake S3 of test_dip_backfill.py, so the
real copy code and its thread pool run.

Run:
    python -m pytest tests/test_dip_routes.py -v
"""

import os
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

import config
from lib import dcp, wasabi
from tests.test_dip_backfill import FakeS3, md5

SIP = '2259082c-f0b1-407f-880c-73ef9f9ea200'
DIP_PATH = '0000/f75f/3cf8/4b3d/b494/e0ec/2017/2508/pkg_U158.03.0157.00096'
DCP_LOC = ('dcp-test-dip-store', 'dip-store/')
WASABI_LOC = ('test-dip-store', 'dip-store/')
AUTH = {'X-API-Key': 'test-key-123'}

FILES = {
    f'METS.{SIP}.xml': b'<mets/>',
    'thumbnails/a.jpg': b'thumb-a',
    'objects/a-page.pdf': b'%PDF-1.7 a',
    'objects/a-photo.jpg': b'jpeg-a',
    'objects/a-master.tif': b'II*\x00 tiff bytes',
}
SERVED = {f'METS.{SIP}.xml', 'thumbnails/a.jpg', 'objects/a-page.pdf', 'objects/a-photo.jpg'}
TOTAL = sum(len(data) for data in FILES.values())


def dcp_store(files=None, etags=None):
    files = FILES if files is None else files
    return FakeS3(
        {(DCP_LOC[0], f'{DCP_LOC[1]}{DIP_PATH}/{name}'): data for name, data in files.items()},
        {(DCP_LOC[0], f'{DCP_LOC[1]}{DIP_PATH}/{name}'): e for name, e in (etags or {}).items()},
    )


class CopyDipRouteTests(unittest.TestCase):

    def setUp(self):
        from flask import Flask
        from routes.dip import dip_bp
        app = Flask(__name__)
        app.register_blueprint(dip_bp)
        app.testing = True
        self.client = app.test_client()
        for p in (
            patch.dict(os.environ, {'API_KEY': AUTH['X-API-Key']}),
            patch.multiple(
                config,
                WASABI_DIP_BUCKET=f's3://{WASABI_LOC[0]}/{WASABI_LOC[1]}',
                DCP_DIP_LOCATION=f's3://{DCP_LOC[0]}/{DCP_LOC[1]}',
            ),
        ):
            p.start()
            self.addCleanup(p.stop)
        self.source = dcp_store()
        self.target = FakeS3()

    def post(self, **overrides):
        body = {'sip_uuid': SIP, 'dip_path': DIP_PATH, 'expected_size': TOTAL}
        body.update(overrides)
        with patch.object(dcp, 'make_client', return_value=self.source), \
                patch.object(wasabi, '_make_client', return_value=self.target):
            return self.client.post('/api/v2/dip/copy-to-wasabi', json=body, headers=AUTH)

    def copied_names(self):
        prefix = f'{WASABI_LOC[1]}{DIP_PATH}/'
        return {k[len(prefix):] for (b, k) in self.target.objects if b == WASABI_LOC[0]}

    def test_needs_the_api_key(self):
        res = self.client.post('/api/v2/dip/copy-to-wasabi', json={})
        self.assertEqual(res.status_code, 403)

    def test_refuses_a_bad_request_with_400(self):
        for overrides in (
            {'sip_uuid': ''},
            {'sip_uuid': 'not-a-uuid'},
            {'dip_path': '0000/f75f/3cf8/4b3d/b494/e0ec/2017/pkg'},     # 7 segments
            {'dip_path': f'{DIP_PATH}/objects'},                          # a subfolder
            {'dip_path': '0000/f75f/3cf8/4b3d/b494/e0ec/2017/2508/..'},
            {'expected_size': None},
            {'expected_size': 0},
            {'expected_size': -5},
            {'expected_size': True},
            {'expected_size': 1.5},
            {'expected_size': 'abc'},
            {'expected_size': str(TOTAL)},  # a digit string isn't a JSON integer
        ):
            res = self.post(**overrides)
            self.assertEqual(res.status_code, 400, overrides)
            self.assertFalse(res.get_json()['ok'])
        self.assertEqual(self.target.puts, [])

    def test_copies_only_the_served_files_once_the_dip_is_complete(self):
        res = self.post()
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertTrue(body['ok'], body)
        self.assertEqual(body['status'], 'copied')
        self.assertEqual(body['files'], {'total': 4, 'copied': 4, 'skipped': 0, 'failed': 0})
        served_bytes = sum(len(FILES[name]) for name in SERVED)
        self.assertEqual(body['bytes'], {'total': served_bytes, 'copied': served_bytes})
        self.assertEqual(body['verified'], {'md5': 4, 'parts': 0, 'size': 0})
        self.assertEqual((body['dcp_bytes'], body['expected_bytes']), (TOTAL, TOTAL))
        self.assertIsNone(body['error'])
        self.assertEqual(self.copied_names(), SERVED)
        # One DCP listing serves both the completeness check and the copy.
        self.assertEqual(
            [c for c in self.source.calls if c[0] == 'list'],
            [('list', f'{DCP_LOC[1]}{DIP_PATH}/')],
        )
        types = {p['key'].rsplit('/', 1)[-1]: p['type'] for p in self.target.puts}
        self.assertEqual(types['a-page.pdf'], 'application/pdf')
        self.assertEqual(types[f'METS.{SIP}.xml'], 'application/xml')

    def test_a_second_call_skips_everything(self):
        self.post()
        self.target.puts.clear()
        body = self.post().get_json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['files'], {'total': 4, 'copied': 0, 'skipped': 4, 'failed': 0})
        self.assertEqual(self.target.puts, [])

    def test_an_incomplete_dip_copies_nothing_and_reports_both_byte_counts(self):
        body = self.post(expected_size=TOTAL + 1000).get_json()
        self.assertFalse(body['ok'])
        self.assertEqual(body['status'], 'incomplete')
        self.assertEqual((body['dcp_bytes'], body['expected_bytes']), (TOTAL, TOTAL + 1000))
        self.assertIn(f'DCP holds {TOTAL:,} of {TOTAL + 1000:,} bytes', body['error'])
        self.assertEqual(self.target.puts, [])

    def test_a_dip_without_its_mets_is_incomplete(self):
        files = {k: v for k, v in FILES.items() if not k.startswith('METS.')}
        self.source = dcp_store(files)
        size = sum(len(v) for v in files.values())
        body = self.post(expected_size=size).get_json()
        self.assertEqual(body['status'], 'incomplete')
        self.assertFalse(body['mets'])
        self.assertIn(f'METS.{SIP}.xml is not in DCP', body['error'])
        self.assertEqual(self.target.puts, [])

    def test_one_bad_file_fails_the_call_and_the_rest_are_copied(self):
        self.source = dcp_store(etags={'objects/a-page.pdf': md5(b'the original')})
        body = self.post().get_json()
        self.assertFalse(body['ok'])
        self.assertEqual(body['status'], 'failed')
        self.assertEqual(body['files']['failed'], 1)
        self.assertEqual(body['files']['copied'], 3)
        self.assertEqual([f['key'] for f in body['failures']], [f'{DIP_PATH}/objects/a-page.pdf'])
        self.assertIn('1 of 4 files failed', body['error'])
        self.assertEqual(self.copied_names(), SERVED - {'objects/a-page.pdf'})

    def test_a_missing_setting_is_a_failed_200(self):
        with patch.object(config, 'WASABI_DIP_BUCKET', None):
            res = self.post()
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual((body['ok'], body['status']), (False, 'failed'))
        self.assertIn('WASABI_DIP_BUCKET', body['error'])

    def test_a_listing_error_is_a_failed_200(self):
        def denied(**kwargs):
            raise ClientError({'Error': {'Code': 'AccessDenied', 'Message': 'denied'},
                               'ResponseMetadata': {'HTTPStatusCode': 403}}, 'ListObjectsV2')

        with patch.object(self.target, 'list_objects_v2', denied):
            res = self.post()
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body['status'], 'failed')
        self.assertIn('AccessDenied', body['error'])
        self.assertEqual(self.target.puts, [])


if __name__ == '__main__':
    unittest.main()
