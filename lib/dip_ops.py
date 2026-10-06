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
public site serves (plan P2). scripts/dip_backfill.py uses it now; the
P3 ingest route will use it for each new DIP.

What is copied, under a DIP folder (<eight 4-hex-digit segments>/<folder>/):
  - everything in thumbnails/
  - the METS, METS.<sip_uuid>.xml, at the top of the folder
  - files in objects/ ending in .jpg, .jpeg or .pdf
Never a leftover DuraCloud chunk artifact (*.dura-chunk-*, *.dura-manifest).
TIFF, JP2 and audio/video stay in DCP: conversions read TIFFs there, and
nothing reads the rest from the DIP store.

copy_file verifies each file on the way: the MD5 of the bytes read from
DCP must match DCP's ETag (for a multipart object, the MD5 of each part's
MD5, with the part sizes from HeadObject), and the upload is one PutObject
carrying Content-MD5, so Wasabi rejects corrupted bytes itself. Nothing is
ever deleted: a PutObject replaces an object only once it is complete.

The part-by-part check is the main path for new DIPs, not an edge case.
Archivematica uploads every file of 8 MiB or more in parts (boto3
upload_fileobj with the default TransferConfig), so from P3 on, every
served file that size has a multipart ETag. Only 6 files in the backfill
are multipart, which makes the check look rare; without it, every large
new file would be checked by size alone.

What Wasabi already holds comes from wasabi_copies(), one listing for a
whole run, rather than a HeadObject per file.

The caller builds the two clients once (dcp.make_client(),
wasabi._make_client()) and passes them in. A build costs about 45 ms, and
a built client is safe to share between threads.
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
    """
    The size of each part of a multipart object, from HeadObject with
    PartNumber. None when DCP won't say; a plain size check is then all
    that is left.
    """
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
    MD5 per part of a stream, split at the given part sizes, so a
    multipart ETag can be recomputed in the same pass over the bytes:
    the MD5 of the concatenated part digests, then '-N'.
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
    under each of `prefixes` (relative to it, as in served_keys). One
    listing of 1,000 keys a page: about 110 requests for the whole store,
    where a HeadObject per file would be about 108,000. The backfill lists
    the run's prefixes; the P3 route lists the one DIP folder.

    A listing error raises: without the listing, nothing can be skipped.
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

    Returns {'key': rel, 'outcome', 'bytes', 'error', 'verified'}, where
    outcome is
      'skipped'    Wasabi already holds it: same size and, for a
                   single-part DCP object, the same ETag (both MD5s).
                   A multipart DCP object is compared by size alone:
                   its ETag is not an MD5, so it can never equal the
                   one Wasabi gives our single PutObject.
      'would_copy' execute=False and the file isn't there yet.
      'copied'     read, checked and written. `verified` says how the
                   bytes read from DCP were checked: 'md5' against a
                   single-part ETag; 'parts' against a multipart ETag,
                   with the part sizes from HeadObject (every new file
                   of 8 MiB or more; see the module docstring); 'size' only,
                   when DCP wouldn't give the part sizes.
      'failed'     anything else; `error` says what. Nothing was deleted,
                   and a re-run tries again.
    Never raises for a single file's trouble, so one failure doesn't
    stop a run.
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


def check_complete(dip_path, expected_size, dcp_client, sip_uuid=None):
    """
    Is the whole DIP in DCP? P3's ingest gate, run before anything is
    copied: every file in the folder, served or not, must add up to
    Archivematica's recorded size, and the METS must be there.

    `dip_path` is the folder relative to DCP_DIP_LOCATION's prefix.
    Leftover chunk artifacts are left out of the sum. With `sip_uuid`,
    the METS must be METS.<sip_uuid>.xml; without it, any METS.*.xml at
    the top of the folder counts.

    Returns {'complete', 'dcp_bytes', 'expected_bytes', 'files', 'mets'}.
    Reads DCP only, so it needs DCP_DIP_LOCATION but not WASABI_DIP_BUCKET.
    """
    bucket, prefix = dcp.location('DCP_DIP_LOCATION')
    folder = prefix + dip_path.strip('/') + '/'
    total = 0
    files = 0
    mets = False
    mets_name = f'METS.{sip_uuid}.xml' if sip_uuid else None
    for item in dcp.iter_objects(bucket, folder, client=dcp_client):
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
