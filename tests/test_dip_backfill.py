# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Tests for scripts/dip_backfill.py: dry run, copying with several workers,
resuming, one failure not stopping the rest, --prefix and --limit, and
main()'s exit codes.

Both stores are an in-memory fake S3 keyed by bucket and key, safe to call
from several threads, so the tests run the real worker pool.

Run:
    python -m pytest tests/test_dip_backfill.py -v
"""

import base64
import hashlib
import io
import json
import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError
from botocore.response import StreamingBody

import config
from lib import dcp, wasabi
from scripts import dip_backfill

FOLDER = '0000/f75f/3cf8/4b3d/b494/e0ec/2017/2508/pkg-uuid_U158.03.0157.00096'
OTHER = '1111/f75f/3cf8/4b3d/b494/e0ec/2017/2508/pkg-other'
DCP_LOC = ('dcp-test-dip-store', 'dip-store/')
WASABI_LOC = ('library-repository', 'dip-store/')
LOCS = (DCP_LOC, WASABI_LOC)


def md5(data):
    return hashlib.md5(data).hexdigest()


def not_found(op):
    return ClientError({'Error': {'Code': '404', 'Message': 'Not Found'},
                        'ResponseMetadata': {'HTTPStatusCode': 404}}, op)


class FakeS3:
    """Just enough of an S3 client, keyed by (bucket, key)."""

    def __init__(self, objects=None, etags=None):
        self.objects = dict(objects or {})
        self.etags = dict(etags or {})
        self.puts = []
        self.lock = threading.Lock()

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        with self.lock:
            keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
            return {
                'IsTruncated': False,
                'Contents': [
                    {'Key': k, 'Size': len(self.objects[(Bucket, k)]),
                     'ETag': f'"{self.etag(Bucket, k)}"'}
                    for k in keys
                ],
            }

    def etag(self, bucket, key):
        return self.etags.get((bucket, key)) or md5(self.objects[(bucket, key)])

    def head_object(self, Bucket, Key):
        with self.lock:
            if (Bucket, Key) not in self.objects:
                raise not_found('HeadObject')
            data = self.objects[(Bucket, Key)]
            return {'ContentLength': len(data), 'ETag': f'"{self.etag(Bucket, Key)}"'}

    def get_object(self, Bucket, Key):
        with self.lock:
            if (Bucket, Key) not in self.objects:
                raise not_found('GetObject')
            data = self.objects[(Bucket, Key)]
        return {'Body': StreamingBody(io.BytesIO(data), len(data))}

    def put_object(self, Bucket, Key, Body, ContentLength, ContentMD5, ContentType):
        data = Body.read()
        assert len(data) == ContentLength
        assert base64.b64encode(hashlib.md5(data).digest()).decode() == ContentMD5
        with self.lock:
            self.objects[(Bucket, Key)] = data
            self.puts.append({'key': Key, 'type': ContentType})
        return {'ETag': f'"{md5(data)}"'}


def dcp_store(extra=None, etags=None):
    """A DCP DIP store holding two DIP folders: served files and others."""
    files = {
        f'{FOLDER}/thumbnails/a.jpg': b'thumb-a',
        f'{FOLDER}/METS.sip-1.xml': b'<mets/>',
        f'{FOLDER}/objects/a-page.pdf': b'%PDF-1.7 a',
        f'{FOLDER}/objects/a-master.tif': b'II*\x00 tiff',
        f'{FOLDER}/objects/a-master.tif.dura-chunk-0000': b'chunk',
        f'{OTHER}/thumbnails/b.jpg': b'thumb-b',
        f'{OTHER}/objects/b-interview.mov': b'mov',
        f'{OTHER}/objects/b-photo.jpg': b'jpeg-b',
    }
    files.update(extra or {})
    return FakeS3(
        {(DCP_LOC[0], DCP_LOC[1] + rel): data for rel, data in files.items()},
        {(DCP_LOC[0], DCP_LOC[1] + rel): e for rel, e in (etags or {}).items()},
    )


SERVED = {
    f'{FOLDER}/thumbnails/a.jpg',
    f'{FOLDER}/METS.sip-1.xml',
    f'{FOLDER}/objects/a-page.pdf',
    f'{OTHER}/thumbnails/b.jpg',
    f'{OTHER}/objects/b-photo.jpg',
}


def run(dcp_client, wasabi_client, **kwargs):
    out = io.StringIO()
    summary = dip_backfill.run(dcp_client, wasabi_client, LOCS, out=out, **kwargs)
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    return summary, lines[:-1], lines[-1]


class RunTests(unittest.TestCase):

    def test_a_run_larger_than_the_pool_window_records_every_file(self):
        """
        The pool takes workers x 4 files at a time and records each as it
        finishes; a 108,000-file run lives in that branch. 30 files on 2
        workers (window 8), one of them bad.
        """
        extra = {f'{FOLDER}/thumbnails/t{n:02d}.jpg': f'thumb-{n}'.encode() for n in range(25)}
        source = dcp_store(extra, etags={f'{FOLDER}/thumbnails/t13.jpg': md5(b'wrong')})
        target = FakeS3()
        summary, lines, last = run(source, target, workers=2, execute=True)
        self.assertEqual(summary['files'], 30)
        self.assertEqual(len(lines), 30)
        self.assertEqual(summary['copied'], 29)
        self.assertEqual(summary['failed'], 1)
        self.assertEqual(len(target.puts), 29)
        failed = [l for l in lines if l['outcome'] == 'failed']
        self.assertEqual([f['key'] for f in failed], [f'{FOLDER}/thumbnails/t13.jpg'])
        self.assertIn('md5', failed[0]['error'])
        self.assertEqual(last['summary'], summary)

    def test_dry_run_plans_only_the_served_files_and_writes_nothing(self):
        source, target = dcp_store(), FakeS3()
        summary, results, last = run(source, target, workers=2, execute=False)
        self.assertEqual({r['key'] for r in results}, SERVED)
        self.assertTrue(all(r['outcome'] == 'would_copy' for r in results))
        self.assertEqual(summary['would_copy'], 5)
        self.assertEqual(target.puts, [])
        self.assertEqual(last, {'summary': summary})

    def test_copies_every_served_file_with_several_workers(self):
        source, target = dcp_store(), FakeS3()
        summary, results, _ = run(source, target, workers=4, execute=True)
        self.assertEqual(summary['copied'], 5)
        self.assertEqual(summary['failed'], 0)
        copied = {k for (b, k) in target.objects if b == WASABI_LOC[0]}
        self.assertEqual(copied, {WASABI_LOC[1] + rel for rel in SERVED})
        self.assertEqual(
            target.objects[(WASABI_LOC[0], f'dip-store/{FOLDER}/objects/a-page.pdf')],
            b'%PDF-1.7 a',
        )
        types = {p['key'].rsplit('/', 1)[-1]: p['type'] for p in target.puts}
        self.assertEqual(types['a-page.pdf'], 'application/pdf')
        self.assertEqual(types['a.jpg'], 'image/jpeg')
        self.assertEqual(types['METS.sip-1.xml'], 'application/xml')

    def test_a_second_run_skips_everything(self):
        source, target = dcp_store(), FakeS3()
        run(source, target, workers=4, execute=True)
        target.puts.clear()
        summary, _, _ = run(source, target, workers=4, execute=True)
        self.assertEqual(summary['skipped'], 5)
        self.assertEqual(summary['copied'], 0)
        self.assertEqual(target.puts, [])

    def test_one_failure_does_not_stop_the_rest(self):
        source = dcp_store(etags={f'{FOLDER}/objects/a-page.pdf': md5(b'the original')})
        target = FakeS3()
        summary, results, _ = run(source, target, workers=2, execute=True)
        self.assertEqual(summary['failed'], 1)
        self.assertEqual(summary['copied'], 4)
        failed = [r for r in results if r['outcome'] == 'failed']
        self.assertEqual(failed[0]['key'], f'{FOLDER}/objects/a-page.pdf')
        self.assertNotIn((WASABI_LOC[0], f'dip-store/{FOLDER}/objects/a-page.pdf'),
                         target.objects)

    def test_prefix_restricts_the_run_to_one_folder(self):
        source, target = dcp_store(), FakeS3()
        summary, results, _ = run(source, target, prefixes=[f'{OTHER}/'], execute=True)
        self.assertEqual({r['key'] for r in results},
                         {f'{OTHER}/thumbnails/b.jpg', f'{OTHER}/objects/b-photo.jpg'})
        self.assertEqual(summary['copied'], 2)

    def test_limit_stops_after_n_served_files(self):
        source, target = dcp_store(), FakeS3()
        summary, results, _ = run(source, target, limit=2, workers=1, execute=True)
        self.assertEqual(len(results), 2)
        self.assertEqual(summary['files'], 2)


class MainTests(unittest.TestCase):

    def setUp(self):
        patcher = patch.multiple(
            config,
            WASABI_DIP_BUCKET=f's3://{WASABI_LOC[0]}/{WASABI_LOC[1]}',
            DCP_DIP_LOCATION=f's3://{DCP_LOC[0]}/{DCP_LOC[1]}',
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def clients(self, source, target):
        return (patch.object(dcp, 'make_client', return_value=source),
                patch.object(wasabi, '_make_client', return_value=target))

    def test_exit_0_and_a_log_file_when_everything_copies(self):
        source, target = dcp_store(), FakeS3()
        with tempfile.TemporaryDirectory() as tmp:
            log = os.path.join(tmp, 'run.jsonl')
            a, b = self.clients(source, target)
            with a, b:
                code = dip_backfill.main(['--execute', '--workers', '2', '--log', log])
            with open(log) as fh:
                lines = [json.loads(line) for line in fh]
        self.assertEqual(code, 0)
        self.assertEqual(lines[-1]['summary']['copied'], 5)

    def test_exit_2_when_a_file_fails(self):
        source = dcp_store(etags={f'{FOLDER}/thumbnails/a.jpg': md5(b'x')})
        a, b = self.clients(source, FakeS3())
        with a, b, patch('sys.stdout', new_callable=io.StringIO):
            self.assertEqual(dip_backfill.main(['--execute']), 2)

    def test_dry_run_is_the_default(self):
        source, target = dcp_store(), FakeS3()
        a, b = self.clients(source, target)
        with a, b, patch('sys.stdout', new_callable=io.StringIO):
            self.assertEqual(dip_backfill.main([]), 0)
        self.assertEqual(target.puts, [])

    def test_exit_1_on_a_configuration_error(self):
        with patch.object(config, 'WASABI_DIP_BUCKET', None):
            self.assertEqual(dip_backfill.main([]), 1)

    def test_exit_3_when_nothing_is_listed(self):
        # A --prefix that already includes dip-store/ lists nothing: that
        # must not look like a clean run.
        source, target = dcp_store(), FakeS3()
        a, b = self.clients(source, target)
        with a, b, patch('sys.stdout', new_callable=io.StringIO), \
                self.assertLogs('dip_backfill', level='WARNING') as logs:
            self.assertEqual(dip_backfill.main(['--execute', '--prefix', 'dip-store/']), 3)
        self.assertIn('nothing listed', '\n'.join(logs.output))
        self.assertEqual(target.puts, [])

    def test_exit_130_when_interrupted(self):
        source, target = dcp_store(), FakeS3()
        a, b = self.clients(source, target)
        real_wait = dip_backfill.wait

        def interrupt_once(*args, **kwargs):
            dip_backfill.wait = real_wait
            raise KeyboardInterrupt

        with a, b, patch('sys.stdout', new_callable=io.StringIO) as out, \
                patch.object(dip_backfill, 'wait', interrupt_once):
            # workers=1 queues four files before the first wait; the
            # interrupt arrives there.
            code = dip_backfill.main(['--execute', '--workers', '1'])
        self.assertEqual(code, 130)
        lines = [json.loads(line) for line in out.getvalue().splitlines()]
        summary = lines[-1]['summary']
        self.assertTrue(summary['interrupted'])
        # Whatever finished was recorded; nothing was lost to the pool.
        self.assertEqual(summary['files'], len(lines) - 1)
        self.assertEqual(summary['copied'], len(target.puts))

    def test_builds_each_client_once_for_the_whole_run(self):
        source, target = dcp_store(), FakeS3()
        with patch.object(dcp, 'make_client', return_value=source) as make_dcp, \
                patch.object(wasabi, '_make_client', return_value=target) as make_wasabi, \
                patch('sys.stdout', new_callable=io.StringIO):
            dip_backfill.main(['--execute', '--workers', '4'])
        self.assertEqual(make_dcp.call_count, 1)
        self.assertEqual(make_wasabi.call_count, 1)


if __name__ == '__main__':
    unittest.main()
