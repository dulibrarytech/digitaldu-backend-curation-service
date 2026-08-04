# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Unit tests for lib/duracloud_ops.py — the DuraCloud → Wasabi AIP-copy
failover. Covers contentId derivation, manifest parsing, the
chunk-concatenating reader (order, per-chunk + whole-file MD5 verify,
EOF semantics), and the copy flow's decision points (idempotent skip,
single-object vs chunked, verification-failure cleanup, replication-lag
404). All network + Wasabi calls are faked; no credentials needed.

Run:
    python -m pytest tests/test_duracloud_ops.py -v
"""

import hashlib
import io
import unittest
from unittest.mock import patch

import config
from lib import aip_ops
from lib import duracloud_ops as dc
from lib import wasabi


AIP_UUID = '08785001-396b-4877-8a15-8d692a9ed2b4'
PAIRS = '0878/5001/396b/4877/8a15/8d69/2a9e/d2b4'
BASENAME = 'x_D009.23.0007.0045.00001_transfer-08785001-396b-4877-8a15-8d692a9ed2b4.7z'


def _manifest_xml(chunks, total=None, md5=None):
    body = b''.join(c for c in chunks)
    total = total if total is not None else len(body)
    md5 = md5 if md5 is not None else hashlib.md5(body).hexdigest()
    chunk_xml = ''.join(
        f'<chunk chunkId="c-{i}" index="{i}">'
        f'<byteSize>{len(c)}</byteSize>'
        f'<md5>{hashlib.md5(c).hexdigest()}</md5>'
        f'</chunk>'
        for i, c in enumerate(chunks)
    )
    return f"""<?xml version='1.0' encoding='UTF-8'?>
<dur:chunksManifest xmlns:dur="duracloud.org">
  <header schemaVersion="0.2">
    <sourceContent contentId="aip-store/{PAIRS}/{BASENAME}">
      <mimetype>application/octet-stream</mimetype>
      <byteSize>{total}</byteSize>
      <md5>{md5}</md5>
    </sourceContent>
  </header>
  <chunks>{chunk_xml}</chunks>
</dur:chunksManifest>"""


class FakeStreamResponse:
    """requests.Response stand-in exposing .raw with read(n)."""

    def __init__(self, payload, headers=None):
        self.raw = io.BytesIO(payload)
        self.headers = headers or {}
        self.closed = False

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class ContentIdTests(unittest.TestCase):
    def test_uuid_pairs(self):
        self.assertEqual(dc.uuid_pairs(AIP_UUID), PAIRS)

    def test_uuid_pairs_rejects_garbage(self):
        for bad in (None, '', 'not-a-uuid', AIP_UUID + 'ff'):
            self.assertIsNone(dc.uuid_pairs(bad))

    def test_content_id_for(self):
        self.assertEqual(
            dc.content_id_for(AIP_UUID, BASENAME),
            f'aip-store/{PAIRS}/{BASENAME}',
        )
        self.assertIsNone(dc.content_id_for('junk', BASENAME))
        self.assertIsNone(dc.content_id_for(AIP_UUID, ''))


class ManifestTests(unittest.TestCase):
    def test_parses_real_shape(self):
        chunks = [b'a' * 10, b'b' * 10, b'c' * 4]
        parsed = dc.parse_manifest(_manifest_xml(chunks))
        self.assertEqual(parsed['total_bytes'], 24)
        self.assertEqual(len(parsed['chunks']), 3)
        self.assertEqual(
            [c['chunk_id'] for c in parsed['chunks']], ['c-0', 'c-1', 'c-2']
        )
        self.assertEqual(parsed['md5'], hashlib.md5(b''.join(chunks)).hexdigest())

    def test_sorts_chunks_by_index(self):
        xml = _manifest_xml([b'aa', b'bb'])
        # Swap document order of the two <chunk> elements.
        first = xml.index('<chunk ')
        second = xml.index('<chunk ', first + 1)
        end = xml.index('</chunks>')
        reordered = xml[:first] + xml[second:end] + xml[first:second] + xml[end:]
        parsed = dc.parse_manifest(reordered)
        self.assertEqual(
            [c['chunk_id'] for c in parsed['chunks']], ['c-0', 'c-1']
        )

    def test_rejects_gapped_sequence(self):
        xml = _manifest_xml([b'aa', b'bb']).replace('index="1"', 'index="2"')
        with self.assertRaises(ValueError):
            dc.parse_manifest(xml)

    def test_rejects_non_xml_and_missing_fields(self):
        with self.assertRaises(ValueError):
            dc.parse_manifest('this is not xml')
        with self.assertRaises(ValueError):
            dc.parse_manifest(
                _manifest_xml([b'aa']).replace(
                    '<byteSize>2</byteSize>', '', 1
                )
            )


class ChunkStreamReaderTests(unittest.TestCase):
    def _reader(self, chunk_payloads, tamper=None):
        payloads = {f'c-{i}': p for i, p in enumerate(chunk_payloads)}
        if tamper:
            payloads.update(tamper)
        manifest = dc.parse_manifest(_manifest_xml(chunk_payloads))
        return (
            dc.ChunkStreamReader(
                manifest['chunks'],
                lambda cid, offset=0: FakeStreamResponse(payloads[cid][offset:]),
            ),
            manifest,
        )

    def test_concatenates_in_order(self):
        chunks = [b'first-', b'second-', b'third']
        reader, manifest = self._reader(chunks)
        out = b''
        while True:
            piece = reader.read(4)  # tiny reads to cross chunk boundaries
            if not piece:
                break
            out += piece
        self.assertEqual(out, b'first-second-third')
        reader.verify_total(manifest['md5'], manifest['total_bytes'])  # no raise
        self.assertEqual(reader.bytes_read, 18)

    def test_read_all_at_once(self):
        chunks = [b'aa', b'bb']
        reader, _m = self._reader(chunks)
        self.assertEqual(reader.read(-1), b'aabb')
        self.assertEqual(reader.read(8), b'')  # EOF stays EOF

    def test_corrupt_chunk_fails_fast(self):
        chunks = [b'good-chunk', b'also-good']
        reader, _m = self._reader(chunks, tamper={'c-0': b'evil-chunk'})
        with self.assertRaises(dc.ChunkVerificationError):
            reader.read(-1)

    def test_truncated_chunk_fails_on_size(self):
        chunks = [b'0123456789']
        reader, _m = self._reader(chunks, tamper={'c-0': b'01234'})
        with self.assertRaises(dc.ChunkVerificationError):
            reader.read(-1)

    def test_whole_file_md5_mismatch_raises(self):
        chunks = [b'aa', b'bb']
        reader, manifest = self._reader(chunks)
        reader.read(-1)
        with self.assertRaises(dc.ChunkVerificationError):
            reader.verify_total('0' * 32, manifest['total_bytes'])

    def test_unknown_size_skips_per_chunk_size_check(self):
        reader = dc.ChunkStreamReader(
            [{'index': 0, 'chunk_id': 'c-0', 'bytes': None, 'md5': ''}],
            lambda _cid, offset=0: FakeStreamResponse(b'whatever-length'),
        )
        self.assertEqual(reader.read(-1), b'whatever-length')

    def test_reads_fill_requested_size_across_chunk_boundaries(self):
        """
        s3transfer builds one S3 part per read() call and S3 rejects
        non-final parts under 5 MiB at CompleteMultipartUpload
        (2026-08-02 EntityTooSmall at 99%). Every read must therefore
        return the FULL requested size across chunk boundaries; only
        the final read may be short.
        """
        chunks = [b'0123456789', b'abcdefghij', b'KLMNO']  # 10+10+5 = 25
        reader, manifest = self._reader(chunks)
        reads = []
        while True:
            piece = reader.read(8)
            if not piece:
                break
            reads.append(piece)
        # Every read except the last is exactly the requested 8 bytes,
        # even though 8 never divides the 10-byte chunks.
        self.assertEqual([len(r) for r in reads], [8, 8, 8, 1])
        self.assertEqual(b''.join(reads), b'0123456789abcdefghijKLMNO')
        reader.verify_total(manifest['md5'], manifest['total_bytes'])

    def test_corrupt_chunk_is_redownloaded_and_never_forwarded(self):
        """
        Production case 2026-08-02 #2: a chunk arrived at the right SIZE
        but the wrong MD5 (upstream transient — an independent
        re-download hashed clean). Verify-before-forward must catch it
        in the spool, re-download just that chunk, and serve only the
        clean bytes — the consumer never sees the corrupt attempt.
        """
        chunks = [b'0123456789', b'abcdefghij']
        manifest = dc.parse_manifest(_manifest_xml(chunks))
        attempts = {'c-0': 0}

        def open_stream(cid, offset=0):
            if cid == 'c-0':
                attempts['c-0'] += 1
                if attempts['c-0'] == 1:
                    # Same length, wrong bytes — the observed failure.
                    return FakeStreamResponse(b'CORRUPTED!'[offset:])
            return FakeStreamResponse({'c-0': chunks[0], 'c-1': chunks[1]}[cid][offset:])

        reader = dc.ChunkStreamReader(manifest['chunks'], open_stream)
        out = reader.read(-1)
        self.assertEqual(out, b'0123456789abcdefghij')
        self.assertEqual(attempts['c-0'], 2)  # one corrupt + one clean
        # Whole-file hash contains only the verified bytes.
        reader.verify_total(manifest['md5'], manifest['total_bytes'])

    def test_truncated_chunk_resumes_via_range_and_completes(self):
        """
        Production case 2026-08-02: a chunk's stream ended 123 KB short
        with a clean-looking EOF (urllib3 1.x doesn't enforce
        Content-Length). The reader must resume with a Range offset and
        finish the chunk — MD5s continuing across the seam — instead of
        failing the whole 67-chunk copy.
        """
        chunks = [b'0123456789', b'abcdefghij']
        manifest = dc.parse_manifest(_manifest_xml(chunks))
        payloads = {'c-0': chunks[0], 'c-1': chunks[1]}
        opens = []

        def open_stream(cid, offset=0):
            opens.append((cid, offset))
            payload = payloads[cid][offset:]
            if cid == 'c-0' and offset == 0:
                payload = payload[:6]  # truncate first attempt
            return FakeStreamResponse(payload)

        reader = dc.ChunkStreamReader(manifest['chunks'], open_stream)
        out = reader.read(-1)
        self.assertEqual(out, b'0123456789abcdefghij')
        reader.verify_total(manifest['md5'], manifest['total_bytes'])
        self.assertIn(('c-0', 6), opens)  # resumed at the exact offset

    def test_resume_budget_exhaustion_fails_with_byte_counts(self):
        chunks = [b'0123456789']
        manifest = dc.parse_manifest(_manifest_xml(chunks))

        def always_truncated(cid, offset=0):
            return FakeStreamResponse(b'01234'[offset:] if offset < 5 else b'')

        reader = dc.ChunkStreamReader(
            manifest['chunks'], always_truncated, max_resumes=2
        )
        with self.assertRaises(dc.ChunkVerificationError) as ctx:
            reader.read(-1)
        self.assertIn('got 5 bytes', str(ctx.exception))

    def test_unknown_size_adopts_content_length_and_resumes(self):
        """Single-object path: no manifest size, but the response's
        Content-Length makes truncation detectable + resumable."""
        payload = b'full-payload-bytes'

        def open_stream(_cid, offset=0):
            body = payload[offset:]
            if offset == 0:
                body = body[:7]  # truncated first attempt
            return FakeStreamResponse(
                body, headers={'Content-Length': str(len(payload))}
            )

        reader = dc.ChunkStreamReader(
            [{'index': 0, 'chunk_id': 'c-0', 'bytes': None, 'md5': ''}],
            open_stream,
        )
        self.assertEqual(reader.read(-1), payload)


class OpenStreamEncodingTests(unittest.TestCase):
    """
    _open_dc_stream must force identity encoding and refuse anything
    else. Root cause of the 2026-08-02 corruption incidents: requests
    advertises gzip by default, DuraCloud's Apache compresses the
    chunk, and raw reads then consume the gzip stream — ~0.012% short
    for incompressible .7z data ("truncation"), or, after a Range
    resume splices an identity tail onto a gzip prefix, EXACTLY the
    manifest size with a wrong MD5.
    """

    def setUp(self):
        for name, value in (
            ('DURACLOUD_API', 'archivesdu.duracloud.org/durastore/'),
            ('DURACLOUD_USER', 'u'),
            ('DURACLOUD_PWD', 'p'),
        ):
            p = patch.object(config, name, value)
            p.start()
            self.addCleanup(p.stop)

    def _fake_response(self, status=200, headers=None):
        class R:
            status_code = status
            raw = io.BytesIO(b'x')

            def close(self):
                pass
        R.headers = headers or {}
        return R()

    def test_requests_identity_and_accepts_identity_response(self):
        seen = {}

        def fake_get(url, **kwargs):
            seen.update(kwargs)
            return self._fake_response(headers={})

        with patch.object(dc.requests, 'get', side_effect=fake_get):
            dc._open_dc_stream('aip-store/x/y.7z')
        self.assertEqual(seen['headers']['Accept-Encoding'], 'identity')

    def test_refuses_gzip_response(self):
        with patch.object(
            dc.requests, 'get',
            return_value=self._fake_response(
                headers={'Content-Encoding': 'gzip'}
            ),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                dc._open_dc_stream('aip-store/x/y.7z')
        self.assertIn('Content-Encoding gzip', str(ctx.exception))


class CopyFlowTests(unittest.TestCase):
    def setUp(self):
        patches = [
            patch.object(config, 'WASABI_AIP_BUCKET', 's3://aip-bucket/aip-store/'),
            patch.object(config, 'DURACLOUD_API', 'archivesdu.duracloud.org/durastore/'),
            patch.object(config, 'DURACLOUD_USER', 'u'),
            patch.object(config, 'DURACLOUD_PWD', 'p'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_API', 'https://am:8000/api'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_USERNAME', 'u'),
            patch.object(config, 'ARCHIVEMATICA_STORAGE_API_KEY', 'k'),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        # Silence the progress-file side channel.
        for name in ('write_copy_progress', 'clear_copy_progress'):
            p = patch.object(aip_ops, name)
            p.start()
            self.addCleanup(p.stop)

    def _am_meta(self, size):
        class MetaRes:
            status_code = 200

            @staticmethod
            def json():
                return {
                    'status': 'UPLOADED',
                    'size': size,
                    'current_path': f'/store/{BASENAME}',
                }
        return MetaRes()

    def test_idempotent_skip_when_key_exists_at_size(self):
        with patch.object(dc.requests, 'get', return_value=self._am_meta(24)), \
                patch.object(wasabi, 'head_object', return_value={
                    'exists': True, 'bucket': 'aip-bucket', 'content_length': 24,
                }):
            result = dc.copy_aip_from_duracloud(AIP_UUID, 'pid-1')
        self.assertTrue(result['ok'])
        self.assertTrue(result['idempotent'])
        self.assertEqual(result['source'], 'duracloud')

    def test_chunked_copy_verifies_and_uploads(self):
        chunks = [b'first-', b'second-', b'third']
        manifest_xml = _manifest_xml(chunks)
        payloads = {f'c-{i}': p for i, p in enumerate(chunks)}
        uploaded = {}

        def fake_get(url, **kwargs):
            if url.endswith('.dura-manifest'):
                class M:
                    status_code = 200
                    text = manifest_xml
                return M()
            return self._am_meta(len(b''.join(chunks)))

        def fake_upload(reader, key, expected_bytes=None, bucket_config=None,
                        progress_hook=None):
            uploaded['body'] = reader.read(-1)
            uploaded['key'] = key
            return {'bucket': 'aip-bucket', 'bytes': len(uploaded['body'])}

        with patch.object(dc.requests, 'get', side_effect=fake_get), \
                patch.object(dc, '_open_dc_stream',
                             side_effect=lambda cid, offset=0: FakeStreamResponse(payloads[cid][offset:])), \
                patch.object(wasabi, 'head_object',
                             return_value={'exists': False, 'bucket': 'aip-bucket'}), \
                patch.object(wasabi, 'upload_fileobj', side_effect=fake_upload):
            result = dc.copy_aip_from_duracloud(AIP_UUID, 'pid-1')

        self.assertTrue(result['ok'], result.get('error'))
        self.assertEqual(uploaded['body'], b'first-second-third')
        self.assertEqual(uploaded['key'], BASENAME)
        self.assertEqual(result['bytes'], 18)

    def test_verification_failure_deletes_uploaded_object(self):
        chunks = [b'first-', b'second-', b'third']
        manifest_xml = _manifest_xml(chunks)
        payloads = {f'c-{i}': p for i, p in enumerate(chunks)}
        payloads['c-1'] = b'TAMPER-'  # same length, wrong bytes
        deleted = []

        def fake_get(url, **kwargs):
            if url.endswith('.dura-manifest'):
                class M:
                    status_code = 200
                    text = manifest_xml
                return M()
            return self._am_meta(18)

        with patch.object(dc.requests, 'get', side_effect=fake_get), \
                patch.object(dc, '_open_dc_stream',
                             side_effect=lambda cid, offset=0: FakeStreamResponse(payloads[cid][offset:])), \
                patch.object(wasabi, 'head_object',
                             return_value={'exists': False, 'bucket': 'aip-bucket'}), \
                patch.object(wasabi, 'delete_object',
                             side_effect=lambda key, **kw: deleted.append(key)), \
                patch.object(wasabi, 'upload_fileobj',
                             side_effect=lambda reader, *a, **kw: {'bytes': len(reader.read(-1))}):
            result = dc.copy_aip_from_duracloud(AIP_UUID, 'pid-1')

        self.assertFalse(result['ok'])
        self.assertIn('verification failed', result['error'])
        self.assertEqual(deleted, [BASENAME])

    def test_single_object_404_reports_not_replicated(self):
        def fake_get(url, **kwargs):
            if url.endswith('.dura-manifest'):
                class M:
                    status_code = 404
                    text = ''
                return M()
            return self._am_meta(10)

        def raise_404(_cid, offset=0):
            raise RuntimeError('duracloud GET x returned HTTP 404')

        with patch.object(dc.requests, 'get', side_effect=fake_get), \
                patch.object(dc, '_open_dc_stream', side_effect=raise_404), \
                patch.object(wasabi, 'head_object',
                             return_value={'exists': False, 'bucket': 'aip-bucket'}):
            result = dc.copy_aip_from_duracloud(AIP_UUID, 'pid-1')
        self.assertFalse(result['ok'])
        self.assertIn('not replicated yet', result['error'])

    def test_refuses_without_duracloud_config(self):
        with patch.object(config, 'DURACLOUD_API', None):
            result = dc.copy_aip_from_duracloud(AIP_UUID, 'pid-1')
        self.assertFalse(result['ok'])
        self.assertIn('DuraCloud is not configured', result['error'])

    def test_am_meta_down_falls_back_to_dc_listing(self):
        listing_xml = (
            "<?xml version='1.0' encoding='UTF-8'?>"
            '<space id="aip-store">'
            f'<item>aip-store/{PAIRS}/{BASENAME}.dura-chunk-0000</item>'
            f'<item>aip-store/{PAIRS}/{BASENAME}.dura-manifest</item>'
            '</space>'
        )
        chunks = [b'payload']
        manifest_xml = _manifest_xml(chunks)
        payloads = {'c-0': chunks[0]}

        def fake_get(url, **kwargs):
            if '/api/v2/file/' in url:
                raise dc.requests.ConnectionError('AM is down')
            if url.endswith('.dura-manifest'):
                class M:
                    status_code = 200
                    text = manifest_xml
                return M()
            class L:
                status_code = 200
                text = listing_xml
            return L()

        with patch.object(dc.requests, 'get', side_effect=fake_get), \
                patch.object(dc, '_open_dc_stream',
                             side_effect=lambda cid, offset=0: FakeStreamResponse(payloads[cid][offset:])), \
                patch.object(wasabi, 'head_object',
                             return_value={'exists': False, 'bucket': 'aip-bucket'}), \
                patch.object(wasabi, 'upload_fileobj',
                             side_effect=lambda reader, *a, **kw: {'bytes': len(reader.read(-1))}):
            result = dc.copy_aip_from_duracloud(AIP_UUID, 'pid-1')

        self.assertTrue(result['ok'], result.get('error'))
        self.assertEqual(result['key'], BASENAME)


class CopyFromDuracloudRouteTests(unittest.TestCase):
    def setUp(self):
        import os
        from flask import Flask
        from routes.aip import aip_bp
        app = Flask(__name__)
        app.register_blueprint(aip_bp)
        app.testing = True
        self.client = app.test_client()
        self.env = patch.dict(os.environ, {'API_KEY': 'test-key-123'})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_requires_api_key_and_body_fields(self):
        res = self.client.post(
            '/api/v2/aip/copy-from-duracloud', json={'aip_uuid': AIP_UUID, 'repo_uuid': 'p'}
        )
        self.assertEqual(res.status_code, 403)
        res = self.client.post(
            '/api/v2/aip/copy-from-duracloud',
            json={'repo_uuid': 'p'},
            headers={'X-API-Key': 'test-key-123'},
        )
        self.assertEqual(res.status_code, 400)

    def test_forwards_ops_result(self):
        fake = {'ok': True, 'key': BASENAME, 'bytes': 18, 'source': 'duracloud'}
        with patch.object(dc, 'copy_aip_from_duracloud', return_value=fake):
            res = self.client.post(
                '/api/v2/aip/copy-from-duracloud',
                json={'aip_uuid': AIP_UUID, 'repo_uuid': 'p'},
                headers={'X-API-Key': 'test-key-123'},
            )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json()['source'], 'duracloud')


if __name__ == '__main__':
    unittest.main()
