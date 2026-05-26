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
AIP-store operations — Archivematica Storage Service → Wasabi pipe.

One public function:

    copy_aip_to_wasabi(aip_uuid, repo_uuid) -> dict

The Stage 6 worker in repo-backend-v2 calls this via the
/api/v2/aip/copy-to-wasabi HTTP route. Two-step flow:

  1. Look up the AIP in AM Storage Service to get its size, name,
     and storage backend. AM responds with JSON metadata at
     GET /api/v2/file/<aip_uuid>/.
  2. Stream the AIP bytes from AM's download endpoint
     (GET /api/v2/file/<aip_uuid>/download/) directly into Wasabi
     via boto3 upload_fileobj. We never write the AIP to local disk
     — these can be 10+ GB.

Idempotency:
  Before downloading, we head_object the destination Wasabi key. If
  it exists at the expected size, return ok=True with the existing
  metadata — the upload is skipped. This makes Stage 6 retries safe:
  a crash mid-upload (or a retry from a prior failed call) re-runs
  and the second call short-circuits.

Naming:
  The Wasabi key follows the legacy convention so the dashboard's
  JOIN with tbl_aip_store rows from the original migration stays
  symmetric:

      <basename-of-am-current-path>

  where the basename is e.g.
  "17246c64-6344-44a9-a903-057524b3ec2e_M123.03.0038.0007.00001-43968b10-18e3-4976-b8ff-3fe9dfaadaf2.7z"

  No directory prefix in the key — same shape as the legacy
  tbl_aip_store.aip column. The bucket-level prefix (if any) is
  applied by lib.wasabi from WASABI_BUCKET.
"""

import logging
import time

import requests
from botocore.exceptions import ClientError

import config
from lib import wasabi

logger = logging.getLogger(__name__)

# AM Storage Service uses ApiKey header auth (legacy username:key).
# Mirror the format the existing archivematica_ops.py uses.
def _am_storage_auth_header():
    return {
        'Authorization': (
            f'ApiKey {config.ARCHIVEMATICA_STORAGE_USERNAME}:'
            f'{config.ARCHIVEMATICA_STORAGE_API_KEY}'
        )
    }


def _am_file_url(aip_uuid):
    base = (config.ARCHIVEMATICA_STORAGE_API or '').rstrip('/')
    return f'{base}/api/v2/file/{aip_uuid}/'


def _am_download_url(aip_uuid):
    base = (config.ARCHIVEMATICA_STORAGE_API or '').rstrip('/')
    return f'{base}/api/v2/file/{aip_uuid}/download/'


def _resolve_wasabi_key(am_metadata):
    """
    Pick the basename of AM's `current_path` as the Wasabi key.

    AM packages have paths like:
      '/storage-location/.../<uuid>_<package>-<aip-uuid>.7z'
      '/storage-location/.../<uuid>_<package>-<aip-uuid>/'  (uncompressed)

    Take the last path segment as the key. Strip a trailing '/' for
    uncompressed packages so the key matches the file name AM would
    serve from /download/.
    """
    current_path = (am_metadata or {}).get('current_path') or ''
    last = current_path.rstrip('/').split('/')[-1]
    return last


def copy_aip_to_wasabi(aip_uuid, repo_uuid):
    """
    Download the AIP for `aip_uuid` from AM Storage Service and
    upload it to Wasabi. See module docstring for the wire contract.

    Returns a dict with the shape the route forwards directly:
        {
            'ok': bool,
            'bucket': str | None,
            'key': str | None,
            'bytes': int | None,
            'elapsed_ms': int,
            'error': str | None,   # only when ok is False
        }
    """
    started = time.monotonic()
    out = {
        'ok': False,
        'bucket': None,
        'key': None,
        'bytes': None,
        'elapsed_ms': 0,
        'error': None,
        'repo_uuid': repo_uuid,
    }

    # --- Step 1: AM metadata lookup -------------------------------------
    try:
        meta_res = requests.get(
            _am_file_url(aip_uuid),
            headers=_am_storage_auth_header(),
            timeout=60,
        )
    except requests.RequestException as e:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = f'am storage lookup failed: {e}'
        logger.warning('copy_aip_to_wasabi am-lookup-failed aip_uuid=%s err=%s',
                       aip_uuid, e)
        return out

    if meta_res.status_code == 404:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = f'AIP {aip_uuid} not found in AM Storage Service'
        return out
    if meta_res.status_code != 200:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = f'am storage returned HTTP {meta_res.status_code}'
        return out

    try:
        meta = meta_res.json()
    except ValueError:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = 'am storage returned non-JSON body'
        return out

    # AM reports `status` for stored packages: UPLOADED, DEL_REQ, etc.
    # We only want to copy UPLOADED. A stage-6 row that arrives here
    # while AM is still finalizing (rare; AM was already polled to
    # INGEST_COMPLETE) is a temporary error — Stage 6 retry will
    # pick it up on a later tick.
    am_status = (meta.get('status') or '').upper()
    if am_status and am_status != 'UPLOADED':
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = f'AM status is {am_status}, not UPLOADED — will retry'
        return out

    expected_bytes = meta.get('size')
    if isinstance(expected_bytes, str):
        try:
            expected_bytes = int(expected_bytes)
        except ValueError:
            expected_bytes = None
    key = _resolve_wasabi_key(meta)
    if not key:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = 'could not derive Wasabi key from AM current_path'
        return out

    # --- Step 2: Idempotency probe --------------------------------------
    # If the object already exists at the expected size, short-circuit.
    # This makes Stage 6 retries safe (a crashed mid-upload comes back
    # here and no-ops).
    try:
        head = wasabi.head_object(key)
    except Exception as e:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = f'wasabi head_object failed: {e}'
        return out

    bucket_name = head.get('bucket')
    if head.get('exists'):
        existing_size = head.get('content_length')
        if (
            expected_bytes is None
            or existing_size is None
            or int(existing_size) == int(expected_bytes)
        ):
            out['ok'] = True
            out['bucket'] = bucket_name
            out['key'] = key
            out['bytes'] = (
                int(existing_size) if existing_size is not None else expected_bytes
            )
            out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
            out['idempotent'] = True
            logger.info(
                'copy_aip_to_wasabi IDEMPOTENT_SKIP key=%s size=%s',
                key, out['bytes'],
            )
            return out
        # Size mismatch — the prior upload was partial or corrupted.
        # Delete and re-upload. Logged loudly so an operator can
        # cross-reference if this happens often.
        logger.warning(
            'copy_aip_to_wasabi SIZE_MISMATCH key=%s existing=%s expected=%s — '
            'deleting and re-uploading',
            key, existing_size, expected_bytes,
        )
        try:
            wasabi.delete_object(key)
        except Exception as e:
            out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
            out['error'] = f'wasabi delete of stale object failed: {e}'
            return out

    # --- Step 3: Stream-pipe AM → Wasabi --------------------------------
    try:
        with requests.get(
            _am_download_url(aip_uuid),
            headers=_am_storage_auth_header(),
            stream=True,
            timeout=(30, None),  # connect 30s, read open-ended
        ) as dl:
            if dl.status_code != 200:
                out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
                out['error'] = f'am download returned HTTP {dl.status_code}'
                return out
            try:
                uploaded = wasabi.upload_fileobj(
                    dl.raw, key,
                    expected_bytes=expected_bytes,
                )
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code', 'unknown')
                out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
                out['error'] = f'wasabi upload ClientError {code}: {e}'
                return out
            except Exception as e:
                out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
                out['error'] = f'wasabi upload failed: {e}'
                return out
    except requests.RequestException as e:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = f'am download failed: {e}'
        return out

    out['ok'] = True
    out['bucket'] = uploaded.get('bucket') or bucket_name
    out['key'] = key
    out['bytes'] = uploaded.get('bytes') or expected_bytes
    out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
    logger.info(
        'copy_aip_to_wasabi OK aip_uuid=%s key=%s bytes=%s elapsed_ms=%d',
        aip_uuid, key, out['bytes'], out['elapsed_ms'],
    )
    return out
