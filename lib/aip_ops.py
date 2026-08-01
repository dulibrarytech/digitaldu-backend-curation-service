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

import json
import logging
import os
import re
import tempfile
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


def _am_storage_base():
    """Normalize ARCHIVEMATICA_STORAGE_API to a host base WITHOUT the
    /api suffix. The two repos grew different env conventions —
    repo-backend-v2 stores `https://host:8000/api/` (and appends
    `v2/...`), while this module appends the full `/api/v2/...` path.
    An operator mirroring the repov2 value here (the natural move, and
    exactly what the deploy doc suggested) produced
    `.../api/api/v2/file/<uuid>/` — which AM answers with a clean 404
    for EVERY AIP (2026-07-30 staging incident, masqueraded as
    "AIP not found in AM Storage Service"). Accept both shapes."""
    base = (config.ARCHIVEMATICA_STORAGE_API or '').rstrip('/')
    if base.endswith('/api'):
        base = base[:-len('/api')]
    return base


def _am_file_url(aip_uuid):
    return f'{_am_storage_base()}/api/v2/file/{aip_uuid}/'


def _am_download_url(aip_uuid):
    return f'{_am_storage_base()}/api/v2/file/{aip_uuid}/download/'


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


# --- copy-progress file ----------------------------------------------------
#
# Live byte progress for the /copy-to-wasabi call, persisted to a small
# per-AIP JSON file so ANOTHER gunicorn worker can serve it from the
# GET /copy-progress/<aip_uuid> route (workers are separate processes;
# in-memory state is invisible across them). The uploading worker
# writes it via a throttled _FileProgress hook; the file is deleted
# when the copy settles, so "no file" cleanly means "no active copy".
# All writes are best-effort — progress must never fail a copy.

# Strict UUID shape (AM package UUIDs). Doubles as path-traversal
# protection: the uuid becomes a filename component.
_PROGRESS_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)


def _progress_path(aip_uuid):
    """Progress-file path for a valid AIP uuid, else None."""
    if not _PROGRESS_UUID_RE.match(aip_uuid or ''):
        return None
    return os.path.join(
        tempfile.gettempdir(), 'aip-copy-progress', f'{aip_uuid.lower()}.json'
    )


def write_copy_progress(aip_uuid, bytes_sent, total_bytes):
    """
    Atomically persist {bytes_sent, total_bytes, updated_at}. Write to
    a temp name + os.replace so a concurrent read never sees a torn
    file. Swallows every error.
    """
    path = _progress_path(aip_uuid)
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f'{path}.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({
                'aip_uuid': aip_uuid.lower(),
                'bytes_sent': int(bytes_sent),
                'total_bytes': int(total_bytes),
                'updated_at': int(time.time()),
            }, f)
        os.replace(tmp, path)
    except Exception:
        logger.debug(
            'write_copy_progress failed aip_uuid=%s', aip_uuid, exc_info=True,
        )


def read_copy_progress(aip_uuid):
    """Progress dict for an active copy, or None (no file / unreadable)."""
    path = _progress_path(aip_uuid)
    if not path:
        return None
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug(
            'read_copy_progress failed aip_uuid=%s', aip_uuid, exc_info=True,
        )
        return None


def clear_copy_progress(aip_uuid):
    """Remove the progress file; missing is fine, errors are swallowed."""
    path = _progress_path(aip_uuid)
    if not path:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception:
        logger.debug(
            'clear_copy_progress failed aip_uuid=%s', aip_uuid, exc_info=True,
        )


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

    # Refuse if the AIP-store bucket isn't configured. Falling back
    # to WASABI_BUCKET here would silently route AIPs into the SFTP-
    # staging archive (the curation host has BOTH buckets in play and
    # they target different storage tiers). Better to fail loudly so
    # the operator sets WASABI_AIP_BUCKET than to spend a multi-GB
    # upload writing to the wrong place.
    if not config.WASABI_AIP_BUCKET:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = (
            'WASABI_AIP_BUCKET is not configured. Set it in the curation '
            'service .env (e.g. s3://library-repository/aip-store/) and '
            'restart before running Stage 6 / the backfill.'
        )
        logger.error(
            'copy_aip_to_wasabi REFUSED — WASABI_AIP_BUCKET unset '
            'aip_uuid=%s repo_uuid=%s', aip_uuid, repo_uuid,
        )
        return out

    # Refuse if AM Storage Service env values are missing. The same
    # philosophy: an unhandled AttributeError (or worse, a 401 from
    # AM on every call) is harder to triage than a clean error
    # string surfaced in the v2 AIPs dashboard. The three values
    # are required as a set — if any is missing, AM authentication
    # would fail. Listing each missing var in the error helps the
    # operator know exactly what to add to the .env.
    missing_am_env = [
        name for name, val in (
            ('ARCHIVEMATICA_STORAGE_API', config.ARCHIVEMATICA_STORAGE_API),
            ('ARCHIVEMATICA_STORAGE_USERNAME', config.ARCHIVEMATICA_STORAGE_USERNAME),
            ('ARCHIVEMATICA_STORAGE_API_KEY', config.ARCHIVEMATICA_STORAGE_API_KEY),
        ) if not val
    ]
    if missing_am_env:
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = (
            'Archivematica Storage Service is not configured. Missing '
            'curation-service .env vars: ' + ', '.join(missing_am_env) + '. '
            'These are the same values v2 uses for ARCHIVEMATICA_STORAGE_*; '
            'copy them into the curation service .env and restart.'
        )
        logger.error(
            'copy_aip_to_wasabi REFUSED - missing AM env: %s '
            'aip_uuid=%s repo_uuid=%s',
            ', '.join(missing_am_env), aip_uuid, repo_uuid,
        )
        return out

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
    # here and no-ops). bucket_config pins this to WASABI_AIP_BUCKET so
    # we're checking the AIP-store bucket, not the SFTP-staging bucket.
    try:
        head = wasabi.head_object(key, bucket_config=config.WASABI_AIP_BUCKET)
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
            wasabi.delete_object(key, bucket_config=config.WASABI_AIP_BUCKET)
        except Exception as e:
            out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
            out['error'] = f'wasabi delete of stale object failed: {e}'
            return out

    # --- Step 3: Stream-pipe AM → Wasabi --------------------------------
    # Seed the copy-progress file at 0 bytes before the stream opens so
    # the dashboard's side-poll shows a (0%) bar immediately, then let
    # the throttled _FileProgress hook advance it as chunks upload. The
    # finally-clear runs on EVERY exit — success, upload error, download
    # error — so a dead copy never leaves a stale "in progress" file
    # behind for the copy-progress route to serve.
    if expected_bytes:
        write_copy_progress(aip_uuid, 0, expected_bytes)
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
                    bucket_config=config.WASABI_AIP_BUCKET,
                    progress_hook=(
                        lambda sent, total:
                            write_copy_progress(aip_uuid, sent, total)
                    ),
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
    finally:
        clear_copy_progress(aip_uuid)

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
