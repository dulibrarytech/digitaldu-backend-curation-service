# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Unit tests for lib/dip_ops.py: the served-file rule, the Wasabi listing,
the verified copy and the completeness check.

DCP and Wasabi are real boto3 clients with botocore Stubbers, so nothing
touches the network. The clients are built with wasabi._session_from_keys,
so a profile name in the environment can't get in the way.

Run:
    python -m pytest tests/test_dip_ops.py -v
"""

import base64
import hashlib
import io
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber

import config
from lib import dip_ops, wasabi

FOLDER = '0000/f75f/3cf8/4b3d/b494/e0ec/2017/2508/pkg-uuid_U158.03.0157.00096'
DCP_LOC = ('dcp-test-dip-store', 'dip-store/')
WASABI_LOC = ('library-repository', 'dip-store/')
LOCS = (DCP_LOC, WASABI_LOC)


def md5(data):
    return hashlib.md5(data).hexdigest()


def item(rel, data, etag=None):
    """A served_keys entry for `data` at `rel`."""
    return {
        'key': DCP_LOC[1] + rel,
        'rel': rel,
        'size': len(data),
        'etag': md5(data) if etag is None else etag,
    }


def body(data):
    return StreamingBody(io.BytesIO(data), len(data))


def s3_client():
    return wasabi._session_from_keys('AKIATEST', 'test-secret', 'us-east-1').client('s3')


class ServedRuleTests(unittest.TestCase):

    def test_serves_thumbnails_the_mets_and_jpg_pdf_objects(self):
        for rel in (
            f'{FOLDER}/thumbnails/3f2b.jpg',
            f'{FOLDER}/METS.4b3db494-e0ec-2017-2508-000000000000.xml',
            f'{FOLDER}/objects/3f2b-page.pdf',
            f'{FOLDER}/objects/3f2b-photo.jpg',
            f'{FOLDER}/objects/3f2b-photo.jpeg',
            f'{FOLDER}/objects/3f2b-SCAN.PDF',
        ):
            self.assertTrue(dip_ops.is_served(rel), rel)

    def test_skips_everything_else(self):
        for rel in (
            f'{FOLDER}/objects/3f2b-master.tif',
            f'{FOLDER}/objects/3f2b-master.jp2',
            f'{FOLDER}/objects/3f2b-interview.mov',
            f'{FOLDER}/objects/3f2b-interview.wav',
            f'{FOLDER}/objects/3f2b-data.xml',
            f'{FOLDER}/objects/3f2b-page.pdf.dura-chunk-0001',
            f'{FOLDER}/objects/3f2b-page.pdf.dura-manifest',
            f'{FOLDER}/thumbnails/3f2b.jpg.dura-chunk-0000',
            f'{FOLDER}/objects/sub/3f2b-page.pdf',
            f'{FOLDER}/logs/3f2b-page.pdf',
            f'{FOLDER}/processingMCP.xml',
            f'{FOLDER}/thumbnails/',
            '0000/f75f/3cf8/4b3d/b494/e0ec/2017/pkg/objects/x.pdf',
            'stray-root-object.pdf',
        ):
            self.assertFalse(dip_ops.is_served(rel), rel)

    def test_served_keys_filters_a_listing_and_adds_rel(self):
        listing = [
            {'key': f'dip-store/{FOLDER}/thumbnails/a.jpg', 'size': 1, 'etag': 'e'},
            {'key': f'dip-store/{FOLDER}/objects/a.tif', 'size': 1, 'etag': 'e'},
            {'key': f'elsewhere/{FOLDER}/thumbnails/a.jpg', 'size': 1, 'etag': 'e'},
        ]
        out = list(dip_ops.served_keys(listing, 'dip-store/'))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]['rel'], f'{FOLDER}/thumbnails/a.jpg')
        self.assertEqual(out[0]['key'], f'dip-store/{FOLDER}/thumbnails/a.jpg')

    def test_content_type_comes_from_the_extension(self):
        self.assertEqual(dip_ops.content_type_for('x/a.JPG'), 'image/jpeg')
        self.assertEqual(dip_ops.content_type_for('x/a.jpeg'), 'image/jpeg')
        self.assertEqual(dip_ops.content_type_for('x/a.pdf'), 'application/pdf')
        self.assertEqual(dip_ops.content_type_for('x/METS.u.xml'), 'application/xml')


class LocationsTests(unittest.TestCase):

    def test_needs_wasabi_dip_bucket(self):
        with patch.object(config, 'WASABI_DIP_BUCKET', None):
            with self.assertRaisesRegex(RuntimeError, 'WASABI_DIP_BUCKET'):
                dip_ops.locations()

    def test_parses_both_locations(self):
        with patch.multiple(
            config,
            WASABI_DIP_BUCKET='s3://library-repository/dip-store/',
            DCP_DIP_LOCATION='s3://dcp-archivesdu-dip-store/dip-store/',
        ):
            self.assertEqual(
                dip_ops.locations(),
                (('dcp-archivesdu-dip-store', 'dip-store/'),
                 ('library-repository', 'dip-store/')),
            )


class WasabiCopiesTests(unittest.TestCase):

    def setUp(self):
        self.wasabi = s3_client()
        self.stub = Stubber(self.wasabi)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)

    def page(self, prefix, entries, token=None, next_token=None):
        resp = {
            'IsTruncated': next_token is not None,
            'Contents': [
                {'Key': WASABI_LOC[1] + rel, 'Size': size, 'ETag': f'"{etag}"'}
                for rel, size, etag in entries
            ],
        }
        if next_token:
            resp['NextContinuationToken'] = next_token
        params = {'Bucket': WASABI_LOC[0], 'Prefix': prefix}
        if token:
            params['ContinuationToken'] = token
        self.stub.add_response('list_objects_v2', resp, params)

    def test_indexes_every_page_of_the_listing_by_rel(self):
        self.page(WASABI_LOC[1], [(f'{FOLDER}/thumbnails/a.jpg', 4, 'aa')], next_token='t1')
        self.page(WASABI_LOC[1], [(f'{FOLDER}/objects/a.pdf', 9, 'bb-2')], token='t1')
        out = dip_ops.wasabi_copies(self.wasabi, LOCS)
        self.assertEqual(out, {
            f'{FOLDER}/thumbnails/a.jpg': (4, 'aa'),
            f'{FOLDER}/objects/a.pdf': (9, 'bb-2'),
        })
        self.stub.assert_no_pending_responses()

    def test_lists_only_the_given_prefixes(self):
        self.page(f'{WASABI_LOC[1]}{FOLDER}/', [(f'{FOLDER}/thumbnails/a.jpg', 4, 'aa')])
        self.page(f'{WASABI_LOC[1]}other/', [])
        out = dip_ops.wasabi_copies(self.wasabi, LOCS, [f'{FOLDER}/', '/other/'])
        self.assertEqual(out, {f'{FOLDER}/thumbnails/a.jpg': (4, 'aa')})
        self.stub.assert_no_pending_responses()

    def test_a_listing_error_raises(self):
        # Nothing can be skipped without the listing, so the caller stops.
        self.stub.add_client_error('list_objects_v2', service_error_code='AccessDenied',
                                   http_status_code=403)
        with self.assertRaises(ClientError):
            dip_ops.wasabi_copies(self.wasabi, LOCS)


class CopyFileTests(unittest.TestCase):

    def setUp(self):
        self.dcp = s3_client()
        self.wasabi = s3_client()
        self.dcp_stub = Stubber(self.dcp)
        self.wasabi_stub = Stubber(self.wasabi)
        self.dcp_stub.activate()
        self.wasabi_stub.activate()
        self.addCleanup(self.dcp_stub.deactivate)
        self.addCleanup(self.wasabi_stub.deactivate)

    def get(self, rel, data):
        self.dcp_stub.add_response(
            'get_object', {'Body': body(data), 'ContentLength': len(data)},
            {'Bucket': DCP_LOC[0], 'Key': DCP_LOC[1] + rel},
        )

    def put(self, rel, data, content_type, etag=None):
        self.wasabi_stub.add_response(
            'put_object', {'ETag': f'"{etag or md5(data)}"'},
            {
                'Bucket': WASABI_LOC[0],
                'Key': WASABI_LOC[1] + rel,
                'Body': ANY,
                'ContentLength': len(data),
                'ContentMD5': base64.b64encode(hashlib.md5(data).digest()).decode(),
                'ContentType': content_type,
            },
        )

    def done(self):
        # Every stubbed call was made, and nothing else: a call with no
        # stub left raises inside copy_file and fails the test.
        self.dcp_stub.assert_no_pending_responses()
        self.wasabi_stub.assert_no_pending_responses()

    def test_skips_a_file_wasabi_holds_with_the_same_size_and_etag(self):
        rel = f'{FOLDER}/thumbnails/a.jpg'
        data = b'jpeg'
        out = dip_ops.copy_file(item(rel, data), self.dcp, self.wasabi, LOCS,
                                existing=(len(data), md5(data)))
        self.assertEqual(out['outcome'], 'skipped')
        self.done()

    def test_copies_a_missing_file_with_content_md5_and_type(self):
        rel = f'{FOLDER}/objects/a-page.pdf'
        data = b'%PDF-1.7 some bytes'
        self.get(rel, data)
        self.put(rel, data, 'application/pdf')
        out = dip_ops.copy_file(item(rel, data), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out, {
            'key': rel, 'outcome': 'copied', 'bytes': len(data), 'error': None,
            'verified': 'md5',
        })
        self.done()

    def test_a_wasabi_put_error_is_a_failure_not_a_raise(self):
        rel = f'{FOLDER}/objects/a-page.pdf'
        data = b'%PDF-1.7 some bytes'
        self.get(rel, data)
        self.wasabi_stub.add_client_error(
            'put_object', service_error_code='AccessDenied', http_status_code=403,
        )
        out = dip_ops.copy_file(item(rel, data), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'failed')
        self.assertIn('wasabi put failed', out['error'])
        self.assertIn('AccessDenied', out['error'])
        self.done()

    def test_a_read_that_breaks_mid_stream_is_a_failure_not_a_raise(self):
        # botocore raises a BotoCoreError (not a ClientError) when the
        # stream ends early or the connection drops while reading.
        rel = f'{FOLDER}/objects/a-page.pdf'
        data = b'%PDF-1.7 some bytes'
        truncated = StreamingBody(io.BytesIO(data[:5]), len(data))
        self.dcp_stub.add_response(
            'get_object', {'Body': truncated, 'ContentLength': len(data)},
            {'Bucket': DCP_LOC[0], 'Key': DCP_LOC[1] + rel},
        )
        out = dip_ops.copy_file(item(rel, data), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'failed')
        self.assertIn('dcp read failed', out['error'])
        self.done()

    def test_recopies_when_the_wasabi_etag_differs(self):
        rel = f'{FOLDER}/thumbnails/a.jpg'
        data = b'jpeg'
        self.get(rel, data)
        self.put(rel, data, 'image/jpeg')
        out = dip_ops.copy_file(item(rel, data), self.dcp, self.wasabi, LOCS,
                                existing=(len(data), md5(b'othr')))
        self.assertEqual(out['outcome'], 'copied')
        self.done()

    def test_an_md5_mismatch_fails_before_anything_is_written(self):
        rel = f'{FOLDER}/objects/a-page.pdf'
        data = b'%PDF-1.7 corrupted in transit'
        self.get(rel, data)
        out = dip_ops.copy_file(item(rel, data, etag=md5(b'original')), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'failed')
        self.assertIn('does not match dcp etag', out['error'])
        self.done()

    def test_a_short_read_fails_before_anything_is_written(self):
        rel = f'{FOLDER}/objects/a-page.pdf'
        data = b'%PDF'
        listed = dict(item(rel, data), size=10)
        self.get(rel, data)
        out = dip_ops.copy_file(listed, self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'failed')
        self.assertIn('read 4 of 10', out['error'])
        self.done()

    def test_a_multipart_dcp_object_is_compared_by_size(self):
        rel = f'{FOLDER}/objects/big.pdf'
        data = b'%PDF big'
        out = dip_ops.copy_file(item(rel, data, etag='abcdef-3'), self.dcp, self.wasabi, LOCS,
                                existing=(len(data), md5(data)))
        self.assertEqual(out['outcome'], 'skipped')
        self.done()

    def multipart(self, data, part_size):
        """(etag, part sizes) of `data` uploaded in parts of `part_size`."""
        parts = [data[i:i + part_size] for i in range(0, len(data), part_size)]
        digests = b''.join(hashlib.md5(part).digest() for part in parts)
        return f'{hashlib.md5(digests).hexdigest()}-{len(parts)}', [len(p) for p in parts]

    def head_parts(self, rel, sizes):
        for number, size in enumerate(sizes, 1):
            self.dcp_stub.add_response(
                'head_object', {'ContentLength': size, 'PartsCount': len(sizes)},
                {'Bucket': DCP_LOC[0], 'Key': DCP_LOC[1] + rel, 'PartNumber': number},
            )

    def test_a_multipart_dcp_object_is_verified_part_by_part(self):
        # The usual case from P3 on: Archivematica uploads every file of
        # 8 MiB or more in parts.
        rel = f'{FOLDER}/objects/big.pdf'
        data = bytes(range(256)) * 40  # 10,240 bytes in parts of 4,096
        etag, sizes = self.multipart(data, 4096)
        self.head_parts(rel, sizes)
        self.get(rel, data)
        self.put(rel, data, 'application/pdf')
        out = dip_ops.copy_file(item(rel, data, etag=etag), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'copied')
        self.assertEqual(out['verified'], 'parts')
        self.done()

    def test_a_multipart_dcp_object_of_another_size_is_recopied(self):
        rel = f'{FOLDER}/objects/big.pdf'
        data = bytes(range(256)) * 40
        etag, sizes = self.multipart(data, 4096)
        self.head_parts(rel, sizes)
        self.get(rel, data)
        self.put(rel, data, 'application/pdf')
        out = dip_ops.copy_file(item(rel, data, etag=etag), self.dcp, self.wasabi, LOCS,
                                existing=(len(data) - 1, md5(data[:-1])))
        self.assertEqual(out['outcome'], 'copied')
        self.done()

    def test_a_multipart_dcp_object_whose_bytes_differ_fails_before_the_put(self):
        rel = f'{FOLDER}/objects/big.pdf'
        data = bytes(range(256)) * 40
        etag, sizes = self.multipart(data, 4096)
        self.head_parts(rel, sizes)
        self.get(rel, data[:-1] + b'X')  # same size, one byte off
        out = dip_ops.copy_file(item(rel, data, etag=etag), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'failed')
        self.assertIn('multipart etag', out['error'])
        self.done()

    def test_a_multipart_dcp_object_falls_back_to_a_size_check_without_part_sizes(self):
        rel = f'{FOLDER}/objects/big.pdf'
        data = b'%PDF big'
        self.dcp_stub.add_client_error(
            'head_object', service_error_code='InvalidRequest', http_status_code=400,
        )
        self.get(rel, data)
        self.put(rel, data, 'application/pdf')
        out = dip_ops.copy_file(item(rel, data, etag='abcdef-3'), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'copied')
        self.assertEqual(out['verified'], 'size')
        self.done()

    def test_part_digests_split_a_stream_at_the_part_boundaries(self):
        data = bytes(range(256)) * 40
        etag, sizes = self.multipart(data, 4096)
        tracker = dip_ops._PartDigests(sizes)
        for i in range(0, len(data), 1000):  # chunks that straddle the parts
            tracker.update(data[i:i + 1000])
        self.assertEqual(tracker.etag(), etag)
        with self.assertRaises(ValueError):
            tracker.update(b'one byte too many')

    def test_a_wasabi_etag_that_is_not_the_md5_fails(self):
        rel = f'{FOLDER}/thumbnails/a.jpg'
        data = b'jpeg'
        self.get(rel, data)
        self.put(rel, data, 'image/jpeg', etag='0' * 32)
        out = dip_ops.copy_file(item(rel, data), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'failed')
        self.assertIn('wasabi etag', out['error'])
        self.done()

    def test_dry_run_reads_and_writes_nothing(self):
        rel = f'{FOLDER}/thumbnails/a.jpg'
        out = dip_ops.copy_file(item(rel, b'jpeg'), self.dcp, self.wasabi, LOCS, execute=False)
        self.assertEqual(out['outcome'], 'would_copy')
        self.done()

    def test_dry_run_reports_a_file_already_there_as_skipped(self):
        rel = f'{FOLDER}/thumbnails/a.jpg'
        data = b'jpeg'
        out = dip_ops.copy_file(item(rel, data), self.dcp, self.wasabi, LOCS, execute=False,
                                existing=(len(data), md5(data)))
        self.assertEqual(out['outcome'], 'skipped')
        self.done()

    def test_a_dcp_read_error_is_a_failure(self):
        rel = f'{FOLDER}/thumbnails/a.jpg'
        self.dcp_stub.add_client_error('get_object', service_error_code='NoSuchKey',
                                       http_status_code=404)
        out = dip_ops.copy_file(item(rel, b'jpeg'), self.dcp, self.wasabi, LOCS)
        self.assertEqual(out['outcome'], 'failed')
        self.assertIn('dcp read failed', out['error'])
        self.done()


class CheckCompleteTests(unittest.TestCase):

    SIP = '4b3db494-e0ec-2017-2508-000000000000'

    def setUp(self):
        self.dcp = s3_client()
        self.stub = Stubber(self.dcp)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)
        patcher = patch.multiple(
            config,
            WASABI_DIP_BUCKET='s3://library-repository/dip-store/',
            DCP_DIP_LOCATION=f's3://{DCP_LOC[0]}/{DCP_LOC[1]}',
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def listing(self, names_sizes):
        self.stub.add_response(
            'list_objects_v2',
            {
                'IsTruncated': False,
                'Contents': [
                    {'Key': f'dip-store/{FOLDER}/{name}', 'Size': size}
                    for name, size in names_sizes
                ],
            },
            {'Bucket': DCP_LOC[0], 'Prefix': f'dip-store/{FOLDER}/'},
        )

    def test_complete_when_the_total_matches_and_the_mets_is_there(self):
        self.listing([
            (f'METS.{self.SIP}.xml', 100),
            ('objects/a.tif', 900),
            ('thumbnails/a.jpg', 10),
            ('objects/a.tif.dura-chunk-0000', 5000),
        ])
        out = dip_ops.check_complete(FOLDER, 1010, self.dcp, sip_uuid=self.SIP)
        self.assertEqual(out, {'complete': True, 'dcp_bytes': 1010,
                               'expected_bytes': 1010, 'files': 3, 'mets': True})

    def test_incomplete_while_bytes_are_missing(self):
        self.listing([(f'METS.{self.SIP}.xml', 100), ('objects/a.tif', 400)])
        out = dip_ops.check_complete(FOLDER, 1010, self.dcp, sip_uuid=self.SIP)
        self.assertFalse(out['complete'])
        self.assertEqual((out['dcp_bytes'], out['expected_bytes']), (500, 1010))

    def test_incomplete_without_the_mets(self):
        self.listing([('objects/a.tif', 1010)])
        out = dip_ops.check_complete(FOLDER, 1010, self.dcp, sip_uuid=self.SIP)
        self.assertFalse(out['complete'])
        self.assertFalse(out['mets'])

    def test_the_mets_must_name_the_sip_when_one_is_given(self):
        self.listing([('METS.other-sip.xml', 100), ('objects/a.tif', 910)])
        out = dip_ops.check_complete(FOLDER, 1010, self.dcp, sip_uuid=self.SIP)
        self.assertFalse(out['mets'])


if __name__ == '__main__':
    unittest.main()
