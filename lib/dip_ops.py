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
The DIP copy core: DuraCloud Preserve (DCP) to Wasabi, for the files the
public site serves. scripts/dip_backfill.py uses it for the backfill, and
routes/dip.py calls copy_dip() for each new DIP at ingest.

What is copied, under a DIP folder (<eight 4-hex-digit segments>/<folder>/):
  - everything in thumbnails/
  - the METS, METS.<sip_uuid>.xml, at the top of the folder
  - files in objects/ ending in .jpg, .jpeg or .pdf
Never a leftover DuraCloud chunk artifact (*.dura-chunk-*, *.dura-manifest).
TIFF, JP2 and audio/video stay in DCP: conversions read TIFFs there, and
nothing reads the rest from the DIP store.

copy_file verifies each file: the MD5 of the bytes read from DCP must
match DCP's ETag (part by part for a multipart object, with the part
sizes from HeadObject), and the upload is one PutObject carrying
Content-MD5. Nothing is ever deleted: a PutObject replaces an object only
once it is complete. The part-by-part check is the main path, not an edge
case: Archivematica uploads every file of 8 MiB or more in parts, so only
the backfill's 6 multipart files make it look rare.

What Wasabi already holds comes from wasabi_copies(), one listing per run
rather than a HeadObject per file. The caller builds the two clients once
(about 45 ms each; safe to share between threads) and passes them in.
"""

import base64
import hashlib
import logging
import mimetypes
import re
import tempfile

from botocore.exceptions import BotoCoreError, ClientError

import config
from lib import dcp, wasabi

logger = logging.getLogger(__name__)

# <eight 4-hex-digit segments>/<package folder>/<rest>, relative to the
# location prefix.
_DIP_PATH_RE = re.compile(r'^(?:[0-9a-fA-F]{4}/){8}[^/]+/(?P<rest>.+)$')
_METS_RE = re.compile(r'^METS\.[^/]+\.xml$')
_CHUNK_RE = re.compile(r'\.dura-(?:chunk-\d+|manifest)$')
SERVED_OBJECT_EXTENSIONS = ('.jpg', '.jpeg', '.pdf')

# Content-Type by extension. DCP's stored types can't be trusted: every
# object there was written by the migration, and the old DuraCloud
# manifests record application/octet-stream.
_CONTENT_TYPES = {
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.pdf': 'application/pdf',
    '.xml': 'application/xml',
}

# Files up to this size stay in memory while they are verified; larger
# ones spill to a temporary file.
_SPOOL_BYTES = 64 * 1024 * 1024
_READ_BYTES = 1024 * 1024

# copy_dip tries a file whose read or put broke this many more times
# before counting it as failed (copy_file names both with these prefixes).
_TRANSIENT_RETRIES = 2
_TRANSIENT_ERROR_PREFIXES = ('dcp read ', 'wasabi put failed')


def _transient_failure(result):
    return result['outcome'] == 'failed' and (result['error'] or '').startswith(
        _TRANSIENT_ERROR_PREFIXES
    )


def locations():
    """
    ((dcp_bucket, dcp_prefix), (wasabi_bucket, wasabi_prefix)) from
    DCP_DIP_LOCATION and WASABI_DIP_BUCKET. A stored DIP path is the same
    relative key under either prefix. Raises RuntimeError naming the
    setting that is missing.
    """
    if not config.WASABI_DIP_BUCKET:
        raise RuntimeError('WASABI_DIP_BUCKET is not configured')
    return dcp.location('DCP_DIP_LOCATION'), wasabi._parse_bucket(
        config.WASABI_DIP_BUCKET
    )


def is_served(rel):
    """True when `rel`, a key relative to the location prefix, is served."""
    if _CHUNK_RE.search(rel):
        return False
    m = _DIP_PATH_RE.match(rel)
    if not m:
        return False
    rest = m.group('rest')
    if '/' not in rest:
        return bool(_METS_RE.match(rest))
    folder, _, name = rest.partition('/')
    if not name or '/' in name:
        return False
    if folder == 'thumbnails':
        return True
    if folder == 'objects':
        return name.lower().endswith(SERVED_OBJECT_EXTENSIONS)
    return False


def served_keys(listing, prefix):
    """
    The served entries of a DCP listing (dcp.iter_objects items, whose
    keys include `prefix`). Each is yielded with a 'rel' key added: the
    key relative to the prefix.
    """
    for item in listing:
        key = item['key']
        if not key.startswith(prefix):
            continue
        rel = key[len(prefix):]
        if is_served(rel):
            yield dict(item, rel=rel)


def content_type_for(rel):
    """Content-Type for a served file, from its extension."""
    dot = rel.rfind('.')
    ext = rel[dot:].lower() if dot != -1 else ''
    if ext in _CONTENT_TYPES:
        return _CONTENT_TYPES[ext]
    return mimetypes.guess_type(rel)[0] or 'application/octet-stream'


def _single_part(etag):
    """An S3 ETag is the object's MD5 only when it has no '-N' suffix."""
    return bool(etag) and '-' not in etag


def _part_count(etag):
    """The N of a multipart ETag ('<md5>-N'), or None."""
    if not etag or '-' not in etag:
        return None
    try:
        return int(etag.rsplit('-', 1)[1])
    except ValueError:
        return None


def _part_sizes(dcp_client, bucket, key, count):
    """Part sizes from HeadObject(PartNumber), or None when DCP won't say."""
    sizes = []
    try:
        for number in range(1, count + 1):
            resp = dcp_client.head_object(Bucket=bucket, Key=key, PartNumber=number)
            sizes.append(int(resp['ContentLength']))
    except (ClientError, BotoCoreError, KeyError, ValueError) as err:
        logger.warning('part sizes unavailable for %s: %s', key, err)
        return None
    return sizes


class _PartDigests:
    """
    MD5 per part of a stream split at the given sizes: the multipart ETag
    (MD5 of the concatenated part digests, then '-N') in one pass.
    """

    def __init__(self, sizes):
        self._sizes = list(sizes)
        self._digests = []
        self._current = hashlib.md5()
        self._left = self._sizes[0] if self._sizes else 0

    def update(self, chunk):
        view = memoryview(chunk)
        while len(view):
            if not self._left:
                if len(self._digests) >= len(self._sizes):
                    raise ValueError('more bytes than the parts add up to')
                self._current = hashlib.md5()
                self._left = self._sizes[len(self._digests)]
            take = view[:self._left]
            self._current.update(take)
            self._left -= len(take)
            view = view[len(take):]
            if not self._left:
                self._digests.append(self._current.digest())

    def etag(self):
        if len(self._digests) != len(self._sizes):
            raise ValueError('fewer bytes than the parts add up to')
        return hashlib.md5(b''.join(self._digests)).hexdigest() + f'-{len(self._digests)}'


def wasabi_copies(wasabi_client, locs, prefixes=None):
    """
    {rel: (size, etag)} for every object under the Wasabi DIP prefix, or
    under each of `prefixes` (relative to it): about 110 listing requests
    for the whole store against 108,000 HeadObjects. The backfill lists
    the run's prefixes; the ingest route lists one DIP folder. A listing error
    raises: without it, nothing can be skipped.
    """
    bucket, base = locs[1]
    found = {}
    for p in prefixes or ['']:
        for obj in dcp.iter_objects(bucket, base + p.lstrip('/'), client=wasabi_client):
            if obj['key'].startswith(base):
                found[obj['key'][len(base):]] = (obj['size'], obj['etag'])
    return found


def _result(item, outcome, error=None, verified=None):
    return {
        'key': item['rel'],
        'outcome': outcome,
        'bytes': item.get('size'),
        'error': error,
        'verified': verified,
    }


def copy_file(item, dcp_client, wasabi_client, locs, execute=True, existing=None):
    """
    Copy one served file and verify it. `item` is a served_keys entry
    (key, rel, size, etag from the DCP listing); `locs` is locations();
    `existing` is the (size, etag) of the Wasabi copy from
    wasabi_copies(), or None when Wasabi has none.

    Returns {'key': rel, 'outcome', 'bytes', 'error', 'verified'}:
      'skipped'    Wasabi holds it: same size and, for a single-part DCP
                   object, the same ETag. A multipart ETag is not an MD5,
                   so those compare by size alone.
      'would_copy' execute=False and the file isn't there yet.
      'copied'     read, checked and written. `verified`: 'md5' against a
                   single-part ETag, 'parts' against a multipart ETag, or
                   'size' only when DCP wouldn't give the part sizes.
      'failed'     anything else; `error` says what. A re-run tries again.
    Never raises for one file's trouble, so one failure doesn't stop a run.
    """
    (dcp_bucket, _), (wasabi_bucket, wasabi_prefix) = locs
    target = wasabi_prefix + item['rel']
    size = item.get('size')
    etag = (item.get('etag') or '').lower()

    if existing is not None:
        have_size, have_etag = existing
        same_size = have_size is not None and size is not None and int(have_size) == int(size)
        if same_size and (not _single_part(etag) or have_etag.lower() == etag):
            return _result(item, 'skipped')
    if not execute:
        return _result(item, 'would_copy')

    # How the bytes read from DCP are checked against its ETag.
    parts = None
    verified = 'md5' if _single_part(etag) else 'size'
    count = _part_count(etag)
    if count:
        sizes = _part_sizes(dcp_client, dcp_bucket, item['key'], count)
        if sizes is not None and (size is None or sum(sizes) == int(size)):
            parts = _PartDigests(sizes)
            verified = 'parts'

    md5 = hashlib.md5()
    read = 0
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL_BYTES) as spool:
        try:
            resp = dcp_client.get_object(Bucket=dcp_bucket, Key=item['key'])
            body = resp['Body']
            try:
                while True:
                    chunk = body.read(_READ_BYTES)
                    if not chunk:
                        break
                    md5.update(chunk)
                    if parts is not None:
                        parts.update(chunk)
                    spool.write(chunk)
                    read += len(chunk)
            finally:
                body.close()
        except (ClientError, BotoCoreError, OSError, ValueError) as err:
            return _result(item, 'failed', f'dcp read failed: {err}')

        if size is not None and read != int(size):
            return _result(item, 'failed', f'dcp read {read} of {size} bytes')
        digest = md5.hexdigest()
        if verified == 'md5' and digest != etag:
            return _result(item, 'failed', f'md5 {digest} does not match dcp etag {etag}')
        if verified == 'parts':
            computed = parts.etag()
            if computed != etag:
                return _result(
                    item, 'failed',
                    f'multipart etag {computed} does not match dcp etag {etag}',
                )

        spool.seek(0)
        try:
            put = wasabi_client.put_object(
                Bucket=wasabi_bucket,
                Key=target,
                Body=spool,
                ContentLength=read,
                ContentMD5=base64.b64encode(md5.digest()).decode('ascii'),
                ContentType=content_type_for(item['rel']),
            )
        except (ClientError, BotoCoreError) as err:
            return _result(item, 'failed', f'wasabi put failed: {err}')

    put_etag = (put.get('ETag') or '').strip('"').lower()
    if put_etag != digest:
        return _result(item, 'failed', f'wasabi etag {put_etag} does not match md5 {digest}')
    result = _result(item, 'copied', verified=verified)
    result['bytes'] = read
    return result


def check_complete(objects, folder, expected_size, sip_uuid=None):
    """
    Is the whole DIP in DCP? The ingest gate: every file in the folder
    (served or not, chunk artifacts excluded) must add up to
    Archivematica's recorded size, and the METS must be there (named
    METS.<sip_uuid>.xml when `sip_uuid` is given). `objects` is the DCP
    listing of the folder (dcp.iter_objects items) and `folder` its full
    key prefix, ending in '/'.

    Returns {'complete', 'dcp_bytes', 'expected_bytes', 'files', 'mets'}.
    """
    total = 0
    files = 0
    mets = False
    mets_name = f'METS.{sip_uuid}.xml' if sip_uuid else None
    for item in objects:
        name = item['key'][len(folder):]
        if _CHUNK_RE.search(name):
            continue
        total += item['size'] or 0
        files += 1
        if '/' not in name and (name == mets_name if mets_name else _METS_RE.match(name)):
            mets = True
    return {
        'complete': mets and total == int(expected_size),
        'dcp_bytes': total,
        'expected_bytes': int(expected_size),
        'files': files,
        'mets': mets,
    }


def copy_dip(dip_path, expected_size, sip_uuid=None):
    """
    Copy one new DIP's served files, once the whole DIP is in DCP: the P3
    ingest copy behind POST /api/v2/dip/copy-to-wasabi, which returns this
    dict as its body. `dip_path` is the folder relative to the location
    prefix and `expected_size` is Archivematica's recorded size for the DIP
    package. One DCP listing serves both the check and the copy; the files
    are copied one at a time, so at most one is spooled to disk.

    Returns {'ok', 'status', 'dcp_bytes', 'expected_bytes', 'mets',
    'files', 'bytes', 'verified', 'failures', 'error'}, where status is
      'incomplete' check_complete didn't pass; nothing was copied, and the
                   caller asks again later. `error` names the shortfall.
      'copied'     every served file is in Wasabi, now or from before
                   (ok=true).
      'failed'     at least one file failed (`failures`: key and error,
                   the first 10). The others were copied; a retry skips
                   them. A read or put that breaks is tried three times
                   in all first: botocore retries requests, not a body read.
    Raises RuntimeError for a missing setting, and ClientError or
    BotoCoreError when a listing fails.
    """
    locs = locations()
    (dcp_bucket, dcp_prefix), _ = locs
    rel_folder = dip_path.strip('/') + '/'
    dcp_client = dcp.make_client()
    listing = list(dcp.iter_objects(dcp_bucket, dcp_prefix + rel_folder, client=dcp_client))
    check = check_complete(listing, dcp_prefix + rel_folder, expected_size, sip_uuid)
    out = {
        'ok': False,
        'status': 'incomplete',
        'dcp_bytes': check['dcp_bytes'],
        'expected_bytes': check['expected_bytes'],
        'mets': check['mets'],
        'files': {'total': 0, 'copied': 0, 'skipped': 0, 'failed': 0},
        'bytes': {'total': 0, 'copied': 0},
        'verified': {'md5': 0, 'parts': 0, 'size': 0},
        'failures': [],
        'error': None,
    }
    if not check['complete']:
        if check['dcp_bytes'] != check['expected_bytes']:
            out['error'] = (
                f"DCP holds {check['dcp_bytes']:,} of {check['expected_bytes']:,} bytes"
            )
        else:
            out['error'] = f"{f'METS.{sip_uuid}.xml' if sip_uuid else 'The METS'} is not in DCP"
        return out

    wasabi_client = wasabi._make_client()
    existing = wasabi_copies(wasabi_client, locs, [rel_folder])
    for item in served_keys(listing, dcp_prefix):
        result = copy_file(item, dcp_client, wasabi_client, locs,
                           existing=existing.get(item['rel']))
        for _ in range(_TRANSIENT_RETRIES):
            if not _transient_failure(result):
                break
            logger.warning('dip copy retrying %s after: %s', result['key'], result['error'])
            result = copy_file(item, dcp_client, wasabi_client, locs,
                               existing=existing.get(item['rel']))
        outcome = result['outcome']
        logger.info('dip copy %s %s verified=%s', outcome, result['key'], result['verified'])
        out['files']['total'] += 1
        out['files'][outcome] += 1
        out['bytes']['total'] += item.get('size') or 0
        if outcome == 'copied':
            out['bytes']['copied'] += result['bytes'] or 0
            out['verified'][result['verified']] += 1
        elif outcome == 'failed' and len(out['failures']) < 10:
            out['failures'].append({'key': result['key'], 'error': result['error']})

    if out['files']['failed']:
        out['status'] = 'failed'
        first = out['failures'][0]
        out['error'] = (
            f"{out['files']['failed']} of {out['files']['total']} files failed; "
            f"first: {first['key']}: {first['error']}"
        )
    else:
        out['status'] = 'copied'
        out['ok'] = True
    return out
