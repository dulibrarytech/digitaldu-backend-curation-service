# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
AIP-copy failover source: DuraCloud aip-store → Wasabi.

Archivematica replicates every AIP to DuraCloud's `aip-store` space.
When AM Storage Service's own /download/ endpoint cannot serve a large
AIP (observed 2026-07-31: silent hangs, then 502s at 66-75 GB), this
module copies the SAME bytes to Wasabi from DuraCloud instead.

Layout facts (verified live against durastore, 2026-08-01):

  - contentId = `aip-store/<uuid-pairs-of-AIP-uuid>/<basename>` where
    <basename> is exactly the Wasabi key the AM path derives from AM's
    current_path (aip_ops._resolve_wasabi_key). uuid-pairs = the AIP
    uuid with dashes stripped, split into 4-hex-char path segments:
    08785001-396b-... → 0878/5001/396b/4877/8a15/8d69/2a9e/d2b4

  - Content at or under DuraCloud's 1 GB threshold is ONE object at the
    contentId. Larger content is stored as `<contentId>.dura-chunk-NNNN`
    slices (1,000,000,000 bytes each; arbitrary byte offsets of the
    compressed .7z, NOT archive-aware) plus `<contentId>.dura-manifest`
    (dur:chunksManifest, schemaVersion 0.2) carrying the total byte
    size, the WHOLE-FILE MD5, and a per-chunk byteSize + MD5.

  - The manifest MD5s make this path verifiable end-to-end: each chunk
    is hash-checked as it streams (fail fast) and the whole file's MD5
    is verified before the copy is declared ok — stronger than the AM
    path, which can only compare sizes. Wasabi's multipart ETag is NOT
    a content MD5, so the streaming hash computed here is the
    load-bearing integrity check.

Nothing is written to local disk: chunks are streamed sequentially
through one continuous reader into wasabi.upload_fileobj, exactly like
the AM path pipes its download. The shared copy-progress file (see
aip_ops.write_copy_progress) is reused, so the dashboard's progress
bar and the Node side's copy-progress poll work unchanged regardless
of which source served the bytes.
"""

import hashlib
import logging
import time
import xml.etree.ElementTree as ET

import requests
from botocore.exceptions import ClientError

import config
from lib import aip_ops
from lib import wasabi

logger = logging.getLogger(__name__)

# Read timeout for DuraCloud GETs. DuraCloud serves first bytes fast
# (~0.7 s observed) — nothing like AM's hours-long prep — so a modest
# per-read budget is enough; the retry loop above handles blips.
_DC_TIMEOUT = (30, 300)


def is_configured():
    return bool(config.DURACLOUD_API and config.DURACLOUD_USER and config.DURACLOUD_PWD)


def _dc_host():
    """DURACLOUD_API env value normalized to a bare host."""
    raw = (config.DURACLOUD_API or '').strip()
    raw = raw.replace('https://', '').replace('http://', '')
    return raw.split('/')[0]


def _dc_url(content_id):
    return f'https://{_dc_host()}/durastore/aip-store/{content_id}'


def _dc_auth():
    return (config.DURACLOUD_USER, config.DURACLOUD_PWD)


def uuid_pairs(aip_uuid):
    """
    AM/DuraCloud uuid-pairs path for an AIP uuid:
    '08785001-396b-…-8d692a9ed2b4' → '0878/5001/…/2a9e/d2b4'.
    Returns None for anything that isn't a 32-hex-char uuid.
    """
    hex_only = (aip_uuid or '').replace('-', '').lower()
    if len(hex_only) != 32 or not all(c in '0123456789abcdef' for c in hex_only):
        return None
    return '/'.join(hex_only[i:i + 4] for i in range(0, 32, 4))


def content_id_for(aip_uuid, basename):
    pairs = uuid_pairs(aip_uuid)
    if not pairs or not basename:
        return None
    return f'aip-store/{pairs}/{basename}'


def parse_manifest(xml_text):
    """
    Parse a dur:chunksManifest into:
        { 'total_bytes': int, 'md5': str,
          'chunks': [ {'chunk_id': str, 'bytes': int, 'md5': str}, … ] }
    Chunks are returned sorted by their index attribute — reassembly
    order is the contract, so we never trust document order alone.
    Raises ValueError on anything structurally unusable.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise ValueError(f'manifest is not valid XML: {e}') from e

    def _int_text(el, tag):
        raw = el.findtext(tag)
        if raw is None:
            raise ValueError(f'manifest missing {tag}')
        return int(raw)

    # Only the ROOT element lives in the dur: namespace in real
    # manifests — every child (header/sourceContent/byteSize/chunk/…)
    # is unqualified. Verified against a live dura-manifest 2026-08-01.
    source = root.find('.//sourceContent')
    if source is None:
        raise ValueError('manifest has no sourceContent')
    total_bytes = _int_text(source, 'byteSize')
    md5 = (source.findtext('md5') or '').strip().lower()
    chunks = []
    for chunk in root.findall('.//chunk'):
        chunks.append({
            'index': int(chunk.get('index')),
            'chunk_id': chunk.get('chunkId'),
            'bytes': _int_text(chunk, 'byteSize'),
            'md5': (chunk.findtext('md5') or '').strip().lower(),
        })
    if not chunks:
        raise ValueError('manifest has no chunks')
    chunks.sort(key=lambda c: c['index'])
    for expected_index, chunk in enumerate(chunks):
        if chunk['index'] != expected_index or not chunk['chunk_id']:
            raise ValueError(f'manifest chunk sequence broken at index {expected_index}')
    return {'total_bytes': total_bytes, 'md5': md5, 'chunks': chunks}


class ChunkVerificationError(Exception):
    """A chunk's bytes did not match its manifest MD5/size."""


class ChunkStreamReader:
    """
    File-like reader that concatenates DuraCloud chunk streams into one
    continuous byte stream, verifying as it goes.

    boto3's upload_fileobj drives this via read(n); when the current
    chunk is exhausted its MD5 + byte count are checked against the
    manifest (raising ChunkVerificationError on mismatch — which
    aborts the upload immediately instead of after 66 GB), then the
    next chunk's HTTP stream is opened lazily. A whole-file MD5 is
    accumulated in parallel; call verify_total() after EOF.

    `open_stream(chunk_id)` is injected for testability; production
    passes _open_dc_stream.
    """

    def __init__(self, chunks, open_stream):
        self._chunks = chunks
        self._open_stream = open_stream
        self._chunk_index = -1
        self._current = None          # active urllib3 response stream
        self._current_response = None  # owning requests.Response (for close)
        self._chunk_hash = None
        self._chunk_bytes = 0
        self.total_hash = hashlib.md5()
        self.bytes_read = 0

    def _advance(self):
        """Close the finished chunk (verifying it) and open the next."""
        if self._current is not None:
            self._verify_current()
            self._current_response.close()
            self._current = None
            self._current_response = None
        self._chunk_index += 1
        if self._chunk_index >= len(self._chunks):
            return False
        chunk = self._chunks[self._chunk_index]
        response = self._open_stream(chunk['chunk_id'])
        self._current_response = response
        self._current = response.raw
        self._chunk_hash = hashlib.md5()
        self._chunk_bytes = 0
        return True

    def _verify_current(self):
        chunk = self._chunks[self._chunk_index]
        # bytes=None means "size unknown" (single-object path without an
        # AM-reported size) — skip the per-chunk size check.
        if chunk['bytes'] is not None and self._chunk_bytes != chunk['bytes']:
            raise ChunkVerificationError(
                f'chunk {chunk["chunk_id"]}: got {self._chunk_bytes} bytes, '
                f'manifest says {chunk["bytes"]}'
            )
        digest = self._chunk_hash.hexdigest()
        if chunk['md5'] and digest != chunk['md5']:
            raise ChunkVerificationError(
                f'chunk {chunk["chunk_id"]}: md5 {digest} != manifest {chunk["md5"]}'
            )

    def read(self, size=-1):
        """
        Return up to `size` bytes (all remaining when size < 0); b''
        only at true end-of-stream — boto3's EOF signal.
        """
        if size is not None and size < 0:
            pieces = []
            while True:
                piece = self.read(8 * 1024 * 1024)
                if not piece:
                    break
                pieces.append(piece)
            return b''.join(pieces)
        while True:
            if self._current is None:
                if self._chunk_index >= len(self._chunks):
                    return b''
                if not self._advance():
                    return b''
            data = self._current.read(size)
            if data:
                self._chunk_hash.update(data)
                self._chunk_bytes += len(data)
                self.total_hash.update(data)
                self.bytes_read += len(data)
                return data
            # Chunk exhausted — verify it, open the next, loop to read.
            if not self._advance():
                return b''

    def verify_total(self, expected_md5, expected_bytes):
        """Whole-file check after EOF; raises ChunkVerificationError."""
        if expected_bytes is not None and self.bytes_read != int(expected_bytes):
            raise ChunkVerificationError(
                f'total bytes {self.bytes_read} != manifest {expected_bytes}'
            )
        digest = self.total_hash.hexdigest()
        if expected_md5 and digest != expected_md5:
            raise ChunkVerificationError(
                f'total md5 {digest} != manifest {expected_md5}'
            )

    def close(self):
        if self._current_response is not None:
            self._current_response.close()
            self._current = None
            self._current_response = None


def _open_dc_stream(content_id):
    """Open a streamed GET for one DuraCloud content item (200 only)."""
    response = requests.get(
        _dc_url(content_id),
        auth=_dc_auth(),
        stream=True,
        timeout=_DC_TIMEOUT,
    )
    if response.status_code != 200:
        response.close()
        raise RuntimeError(
            f'duracloud GET {content_id} returned HTTP {response.status_code}'
        )
    return response


def _resolve_key_and_size(aip_uuid):
    """
    (wasabi_key, expected_bytes, error) — key + size for the AIP.

    Primary: AM Storage metadata (fast + still healthy even when AM's
    download path is broken). Fallback when AM is fully unreachable:
    derive the basename by listing the AIP's uuid-pairs prefix in
    DuraCloud (size then comes from the manifest or content itself, so
    None is fine here).
    """
    try:
        meta_res = requests.get(
            aip_ops._am_file_url(aip_uuid),
            headers=aip_ops._am_storage_auth_header(),
            timeout=60,
        )
        if meta_res.status_code == 200:
            meta = meta_res.json()
            key = aip_ops._resolve_wasabi_key(meta)
            size = meta.get('size')
            if isinstance(size, str):
                size = int(size) if size.isdigit() else None
            if key:
                return key, size, None
    except (requests.RequestException, ValueError) as e:
        logger.warning(
            'copy_from_duracloud AM meta unavailable aip_uuid=%s err=%s — '
            'falling back to DuraCloud listing', aip_uuid, e,
        )

    pairs = uuid_pairs(aip_uuid)
    if not pairs:
        return None, None, f'{aip_uuid} is not a valid AIP uuid'
    prefix = f'aip-store/{pairs}/'
    try:
        listing = requests.get(
            f'https://{_dc_host()}/durastore/aip-store',
            params={'prefix': prefix},
            auth=_dc_auth(),
            timeout=_DC_TIMEOUT,
        )
    except requests.RequestException as e:
        return None, None, f'duracloud listing failed: {e}'
    if listing.status_code != 200:
        return None, None, f'duracloud listing returned HTTP {listing.status_code}'
    try:
        items = [
            el.text for el in ET.fromstring(listing.text).findall('.//item')
            if el.text
        ]
    except ET.ParseError as e:
        return None, None, f'duracloud listing unparseable: {e}'
    basenames = {
        item[len(prefix):].split('.dura-')[0]
        for item in items
        if item.startswith(prefix)
    }
    basenames.discard('')
    if len(basenames) != 1:
        return None, None, (
            f'expected exactly one AIP under {prefix}, found {sorted(basenames)}'
        )
    return basenames.pop(), None, None


def copy_aip_from_duracloud(aip_uuid, repo_uuid):
    """
    Copy one AIP from DuraCloud's aip-store to Wasabi. Result dict has
    the same shape as aip_ops.copy_aip_to_wasabi (ok / bucket / key /
    bytes / elapsed_ms / error) plus source='duracloud', so the route
    and the Node side treat both paths interchangeably.
    """
    started = time.monotonic()
    logger.info(
        'copy_aip_from_duracloud START aip_uuid=%s repo_uuid=%s',
        aip_uuid, repo_uuid,
    )
    out = {
        'ok': False,
        'bucket': None,
        'key': None,
        'bytes': None,
        'elapsed_ms': 0,
        'error': None,
        'repo_uuid': repo_uuid,
        'source': 'duracloud',
    }

    def done(error=None):
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        if error:
            out['error'] = error
        return out

    if not config.WASABI_AIP_BUCKET:
        return done(
            'WASABI_AIP_BUCKET is not configured. Set it in the curation '
            'service .env and restart.'
        )
    if not is_configured():
        return done(
            'DuraCloud is not configured. Set DURACLOUD_API / DURACLOUD_USER '
            '/ DURACLOUD_PWD in the curation service .env and restart.'
        )

    key, expected_bytes, resolve_error = _resolve_key_and_size(aip_uuid)
    if not key:
        return done(resolve_error or 'could not resolve AIP key')
    content_id = content_id_for(aip_uuid, key)
    if not content_id:
        return done(f'{aip_uuid} is not a valid AIP uuid')

    # Idempotency probe — identical semantics to the AM path.
    try:
        head = wasabi.head_object(key, bucket_config=config.WASABI_AIP_BUCKET)
    except Exception as e:
        return done(f'wasabi head_object failed: {e}')
    bucket_name = head.get('bucket')
    if head.get('exists'):
        existing_size = head.get('content_length')
        if (
            expected_bytes is None
            or existing_size is None
            or int(existing_size) == int(expected_bytes)
        ):
            out.update({
                'ok': True,
                'bucket': bucket_name,
                'key': key,
                'bytes': int(existing_size) if existing_size is not None else expected_bytes,
                'idempotent': True,
            })
            logger.info(
                'copy_aip_from_duracloud IDEMPOTENT_SKIP key=%s size=%s',
                key, out['bytes'],
            )
            return done()
        logger.warning(
            'copy_aip_from_duracloud SIZE_MISMATCH key=%s existing=%s '
            'expected=%s — deleting and re-copying',
            key, existing_size, expected_bytes,
        )
        try:
            wasabi.delete_object(key, bucket_config=config.WASABI_AIP_BUCKET)
        except Exception as e:
            return done(f'wasabi delete of stale object failed: {e}')

    # --- Resolve the DuraCloud shape: single object or chunked ----------
    manifest = None
    try:
        manifest_res = requests.get(
            _dc_url(f'{content_id}.dura-manifest'),
            auth=_dc_auth(),
            timeout=_DC_TIMEOUT,
        )
        if manifest_res.status_code == 200:
            manifest = parse_manifest(manifest_res.text)
        elif manifest_res.status_code != 404:
            return done(
                f'duracloud manifest returned HTTP {manifest_res.status_code}'
            )
    except requests.RequestException as e:
        return done(f'duracloud manifest fetch failed: {e}')
    except ValueError as e:
        return done(f'duracloud manifest unparseable: {e}')

    if manifest and expected_bytes is None:
        expected_bytes = manifest['total_bytes']

    if expected_bytes:
        aip_ops.write_copy_progress(aip_uuid, 0, expected_bytes)
    progress_hook = (
        lambda sent, total: aip_ops.write_copy_progress(aip_uuid, sent, total)
    )

    try:
        if manifest is None:
            # Single-object AIP (≤1 GB): stream it straight through,
            # verifying total bytes below (no manifest = no MD5s; same
            # size-based assurance as the AM path).
            #
            # A replication-lag 404 lands HERE (no manifest AND no
            # content) — reported as a plain retryable error.
            try:
                dl = _open_dc_stream(content_id)
            except RuntimeError as e:
                if 'HTTP 404' in str(e):
                    return done(
                        f'AIP not found in DuraCloud (not replicated yet?): '
                        f'{content_id}'
                    )
                return done(str(e))
            with dl:
                reader = ChunkStreamReader(
                    [{
                        'index': 0,
                        'chunk_id': content_id,
                        # None disables the per-chunk size check when AM
                        # didn't report a size; no manifest = no MD5.
                        'bytes': expected_bytes,
                        'md5': '',
                    }],
                    lambda _cid: dl,
                )
                uploaded = wasabi.upload_fileobj(
                    reader, key,
                    expected_bytes=expected_bytes,
                    bucket_config=config.WASABI_AIP_BUCKET,
                    progress_hook=progress_hook,
                )
                if expected_bytes is not None and reader.bytes_read != int(expected_bytes):
                    raise ChunkVerificationError(
                        f'total bytes {reader.bytes_read} != expected {expected_bytes}'
                    )
        else:
            reader = ChunkStreamReader(manifest['chunks'], _open_dc_stream)
            try:
                uploaded = wasabi.upload_fileobj(
                    reader, key,
                    expected_bytes=manifest['total_bytes'],
                    bucket_config=config.WASABI_AIP_BUCKET,
                    progress_hook=progress_hook,
                )
                reader.verify_total(manifest['md5'], manifest['total_bytes'])
            finally:
                reader.close()
    except ChunkVerificationError as e:
        # Bytes are provably wrong — remove the bad Wasabi object so a
        # corrupt copy can never masquerade as a preservation copy.
        logger.error(
            'copy_aip_from_duracloud VERIFICATION_FAILED key=%s err=%s — '
            'deleting uploaded object', key, e,
        )
        try:
            wasabi.delete_object(key, bucket_config=config.WASABI_AIP_BUCKET)
        except Exception as del_err:
            logger.error(
                'copy_aip_from_duracloud cleanup delete failed key=%s err=%s',
                key, del_err,
            )
        return done(f'verification failed: {e}')
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', 'unknown')
        return done(f'wasabi upload ClientError {code}: {e}')
    except (requests.RequestException, RuntimeError) as e:
        return done(f'duracloud download failed: {e}')
    except Exception as e:
        return done(f'copy failed: {e}')
    finally:
        aip_ops.clear_copy_progress(aip_uuid)

    out.update({
        'ok': True,
        'bucket': uploaded.get('bucket') or bucket_name,
        'key': key,
        'bytes': (
            reader.bytes_read
            or uploaded.get('bytes')
            or expected_bytes
        ),
    })
    logger.info(
        'copy_aip_from_duracloud OK aip_uuid=%s key=%s bytes=%s elapsed_ms=%d',
        aip_uuid, key, out['bytes'],
        int((time.monotonic() - started) * 1000),
    )
    return done()
