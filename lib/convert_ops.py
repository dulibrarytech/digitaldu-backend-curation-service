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
TIFF -> JPG derivative generation against the DuraCloud dip-store.

  - synchronous, truthful results
  - free-space preflight BEFORE accepting work (InsufficientStorage ->
    HTTP 507 at the route layer)
  - atomic writes: temp file + rename, size-verified, temp removed on
    any failure — readers can never observe a partial or 0-byte file

The dip-store fetch mirrors lib/duracloud_ops' hardened streaming
(identity encoding is load-bearing there; kept here for the same
reason) but is deliberately separate: duracloud_ops speaks the
aip-store space, derivatives come from dip-store, whose content ids
carry a leading 'dip-store/' segment (URL =
/durastore/dip-store/dip-store/<path> — verified against the live
store).
"""

import io
import logging
import os
import time
from urllib.parse import quote

import requests
from PIL import Image

import config

logger = logging.getLogger(__name__)

# (connect, read) — DuraCloud serves first bytes fast; reads are chunked.
_DC_TIMEOUT = (30, 300)
_FETCH_CHUNK_BYTES = 1024 * 1024

# Archival TIFFs (large maps, oversize scans) legitimately exceed
# Pillow's ~178 MP decompression-bomb default. Sources are our own
# repository content and the byte size is capped before decode, so the
# pixel-count guard adds nothing but false failures here.
Image.MAX_IMAGE_PIXELS = None


class ConvertError(Exception):
    """Base class: carries the HTTP status the route should return."""

    status = 500


class InvalidRequest(ConvertError):
    status = 400


class SourceNotFound(ConvertError):
    status = 404


class UnreadableSource(ConvertError):
    """
    The fetched bytes cannot be decoded as an image. Almost always a
    corrupt/truncated copy at rest in the dip-store, not a service
    fault: 422
    so the repov2 queue records a per-source verdict, not a server
    error.
    """

    status = 422


class InsufficientStorage(ConvertError):
    status = 507


def storage_dir():
    """DERIVATIVE_STORAGE_PATH resolved to an absolute path."""
    raw = getattr(config, 'DERIVATIVE_STORAGE_PATH', None)
    if not raw:
        raise ConvertError('DERIVATIVE_STORAGE_PATH is not configured')
    return os.path.abspath(raw)


def derivative_name(object_name):
    """
    On-disk derivative name for a payload object_name: basename only
    (kills traversal), source extension replaced with .jpg.
    '52aafea5-...-B463.01.0016.00001.tif' -> '...00001.jpg'
    """
    base = os.path.basename(str(object_name or '').strip())
    if not base:
        raise InvalidRequest('object_name is required')
    stem, _, _ = base.rpartition('.')
    return (stem or base) + '.jpg'


def image_path(filename):
    """
    Absolute path for a stored derivative, or None when the name is not
    a plain .jpg basename (traversal attempts, other extensions).
    """
    base = os.path.basename(str(filename or '').strip())
    if not base or base != filename or not base.lower().endswith('.jpg'):
        return None
    return os.path.join(storage_dir(), base)


def check_storage():
    """
    Refuse new conversions when the derivative share is low on space.
    A full volume makes writes produce 0-byte files; better to refuse
    loudly (507) so the repov2 queue records a clear failure.
    Returns free bytes.
    """
    stats = os.statvfs(storage_dir())
    free_bytes = stats.f_bavail * stats.f_frsize
    minimum = int(getattr(config, 'DERIVATIVE_MIN_FREE_BYTES', 0) or 0)
    if free_bytes < minimum:
        raise InsufficientStorage(
            'Insufficient storage: %dMB free, %dMB required '
            '(DERIVATIVE_MIN_FREE_BYTES). Conversions refused until space '
            'is freed.' % (free_bytes // (1024 * 1024), minimum // (1024 * 1024))
        )
    return free_bytes


def fetch_tiff(full_path):
    """
    Stream one dip-store object into memory and return its bytes.

    Accept-Encoding: identity mirrors duracloud_ops — wire bytes must
    equal entity bytes. The size cap (DERIVATIVE_MAX_SOURCE_BYTES)
    aborts oversized downloads early; DuraCloud chunks objects >1GB
    into .dura-chunk-* parts that cannot be fetched raw anyway.
    """
    if not (config.DURACLOUD_API and config.DURACLOUD_USER and config.DURACLOUD_PWD):
        raise ConvertError('DuraCloud is not configured (DURACLOUD_* env)')

    host = (config.DURACLOUD_API or '').strip()
    host = host.replace('https://', '').replace('http://', '').split('/')[0]
    url = 'https://%s/durastore/dip-store/dip-store/%s' % (
        host,
        quote(str(full_path), safe='/'),
    )
    max_bytes = int(getattr(config, 'DERIVATIVE_MAX_SOURCE_BYTES', 0) or 0)

    response = requests.get(
        url,
        auth=(config.DURACLOUD_USER, config.DURACLOUD_PWD),
        headers={'Accept-Encoding': 'identity'},
        stream=True,
        timeout=_DC_TIMEOUT,
    )
    try:
        if response.status_code == 404:
            raise SourceNotFound('Source not found in dip-store: %s' % full_path)
        if response.status_code != 200:
            raise ConvertError(
                'dip-store GET returned HTTP %d for %s'
                % (response.status_code, full_path)
            )

        declared = int(response.headers.get('content-length') or 0)
        if max_bytes and declared and declared > max_bytes:
            raise InvalidRequest(
                'Source is %d bytes, over the %d-byte conversion cap '
                '(DERIVATIVE_MAX_SOURCE_BYTES)' % (declared, max_bytes)
            )

        buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=_FETCH_CHUNK_BYTES):
            buffer.write(chunk)
            if max_bytes and buffer.tell() > max_bytes:
                raise InvalidRequest(
                    'Source exceeded the %d-byte conversion cap mid-stream'
                    % max_bytes
                )
        data = buffer.getvalue()
    finally:
        response.close()

    if not data:
        raise ConvertError('dip-store returned an empty body for %s' % full_path)
    if declared and len(data) != declared:
        # A short body that urllib3 didn't reject: distinguish in-transit
        # truncation from corruption at rest before anyone diffs bytes.
        raise ConvertError(
            'dip-store fetch truncated in transit for %s: received %d of '
            '%d declared bytes — retry may succeed' % (full_path, len(data), declared)
        )
    return data


# Pillow's decode-side failures for damaged sources. Deliberately NOT a
# bare Exception: MemoryError and friends are genuine server faults and
# must keep surfacing as 500s. UnidentifiedImageError subclasses OSError;
# ValueError covers tag-level damage ("Invalid dimensions" — the
# truncated-at-upload signature, since these TIFFs keep their IFD at the
# file tail); SyntaxError covers malformed headers.
_DECODE_ERRORS = (OSError, ValueError, SyntaxError)


def convert_to_jpeg(tiff_bytes):
    """
    Decode a TIFF and return JPEG bytes at DERIVATIVE_JPEG_QUALITY.
    Non-RGB modes (CMYK scans, palette, greyscale+alpha) are normalized
    to RGB — JPEG has no alpha, and Pillow raises on some modes
    otherwise. Undecodable bytes raise UnreadableSource (422): the
    source object is damaged, and the queue error should say so instead
    of a generic 500.
    """
    quality = int(getattr(config, 'DERIVATIVE_JPEG_QUALITY', 85) or 85)
    try:
        with Image.open(io.BytesIO(tiff_bytes)) as image:
            if image.mode not in ('RGB', 'L'):
                image = image.convert('RGB')
            out = io.BytesIO()
            image.save(out, format='JPEG', quality=quality)
    except _DECODE_ERRORS as error:
        raise UnreadableSource(
            'source TIFF is undecodable (%s: %s) — the dip-store copy is '
            'likely corrupt or truncated; compare its size and checksum '
            'against the original before requeueing'
            % (type(error).__name__, error)
        ) from error
    jpeg = out.getvalue()
    if not jpeg:
        raise ConvertError('conversion produced no output')
    return jpeg


def write_derivative(jpeg_bytes, object_name):
    """
    Write atomically and verify: temp file, size check against the
    buffer (a short write is the disk-full signature), rename into
    place. Returns {'file_name', 'bytes'}. The temp file is removed on
    any failure.
    """
    if not jpeg_bytes:
        raise ConvertError('refusing to write an empty derivative')

    file_name = derivative_name(object_name)
    dest = os.path.join(storage_dir(), file_name)
    tmp = dest + '.tmp'

    try:
        with open(tmp, 'wb') as handle:
            handle.write(jpeg_bytes)
        written = os.stat(tmp).st_size
        if written != len(jpeg_bytes):
            raise ConvertError(
                'short write for %s: expected %d bytes, wrote %d (disk full?)'
                % (file_name, len(jpeg_bytes), written)
            )
        os.replace(tmp, dest)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

    return {'file_name': file_name, 'bytes': written}


def convert_and_store(payload):
    """
    Full pipeline for one conversion payload
    ({sip_uuid, full_path, object_name, mime_type}). Returns
    {'file_name', 'bytes', 'processing_time_ms'}; raises ConvertError
    subclasses with the HTTP status the route should use.
    """
    started = time.monotonic()

    full_path = str(payload.get('full_path') or '').strip()
    object_name = str(payload.get('object_name') or '').strip()
    if not full_path:
        raise InvalidRequest('full_path is required')
    if not object_name:
        raise InvalidRequest('object_name is required')

    check_storage()
    tiff = fetch_tiff(full_path)
    jpeg = convert_to_jpeg(tiff)
    written = write_derivative(jpeg, object_name)

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        'converted %s -> %s (%d bytes, %dms)',
        object_name, written['file_name'], written['bytes'], elapsed_ms,
    )
    return {
        'file_name': written['file_name'],
        'bytes': written['bytes'],
        'processing_time_ms': elapsed_ms,
    }
