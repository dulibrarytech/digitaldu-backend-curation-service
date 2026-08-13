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
  The Wasabi key is <basename-of-am-current-path> with NO directory
  prefix — the shape tbl_aip_store.aip holds, which the dashboard's
  JOIN depends on. The bucket-level prefix, if any, is applied by
  lib.wasabi from the bucket env value.

Design history and rationale: repo/notes/CURATION_API_CODE_NOTES.md
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

# AM Storage Service uses ApiKey header auth: `ApiKey <username>:<key>`.
def _am_storage_auth_header():
    return {
        'Authorization': (
            f'ApiKey {config.ARCHIVEMATICA_STORAGE_USERNAME}:'
            f'{config.ARCHIVEMATICA_STORAGE_API_KEY}'
        )
    }


def _am_storage_base():
    """Normalize ARCHIVEMATICA_STORAGE_API to a host base WITHOUT the
    /api suffix, so both env shapes work: `https://host:8000` and
    repo-backend-v2's `https://host:8000/api/`. A doubled `/api/api/`
    path makes AM answer 404 for every AIP."""
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
# per-AIP JSON file so any gunicorn worker can serve GET
# /copy-progress/<aip_uuid>. The file is deleted when the copy settles,
# so "no file" means "no active copy". All writes are best-effort —
# progress must never fail a copy.

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
    # Entry marker: every attempt must leave a curation-side log line,
    # including one that fails or hangs before the upload phase.
    logger.info(
        'copy_aip_to_wasabi START aip_uuid=%s repo_uuid=%s',
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
    }

    # Refuse if the AIP-store bucket isn't configured — never fall back
    # to WASABI_BUCKET, which is a different storage tier.
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

    # The three AM Storage values are required as a set. Refuse up front,
    # naming each missing var, rather than failing later on a 401.
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

    # AM reports `status` for stored packages (UPLOADED, DEL_REQ, ...).
    # Only UPLOADED is copyable; anything else is a temporary error the
    # caller's retry picks up on a later tick.
    am_status = (meta.get('status') or '').upper()
    if am_status == 'DELETED':
        # Terminal: AM says the AIP no longer exists — deleted via the
        # Storage Service. No retry can ever succeed; the caller
        # classifies this as permanently-decided (2026-08-11 backfill:
        # a DELETED AIP burned retry budget as "will retry" forever).
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        out['error'] = 'AM status is DELETED — the AIP was deleted from Archivematica; skipping permanently'
        return out
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

    # --- Large-AIP routing: DuraCloud is the DEFAULT source ------------
    # Artefactual's recommendation (2026-08-03) after the SS /download/
    # path hung and then 502'd on 66-75 GB AIPs: retrieve large AIPs
    # directly from DuraCloud. Routing here (rather than in the caller)
    # means EVERY consumer of this function — Stage 6, the backfill
    # tool, dashboard retries — inherits the policy. The DuraCloud copy
    # is also the stronger path: chunk + whole-file MD5 verification
    # against the .dura-manifest vs the AM path's size-only check.
    # A not-yet-replicated AIP surfaces as a retryable "not found in
    # DuraCloud" error — the caller's retry budget covers replication
    # lag, and staff can always retry later from the AIPs dashboard.
    threshold = config.AIP_DURACLOUD_THRESHOLD_BYTES
    if threshold and expected_bytes and expected_bytes >= threshold:
        # Local import: duracloud_ops imports this module (progress
        # helpers, AM URL builders), so a top-level import would be
        # circular.
        from lib import duracloud_ops
        if duracloud_ops.is_configured():
            logger.info(
                'copy_aip_to_wasabi ROUTING to duracloud aip_uuid=%s '
                'size=%s >= threshold=%s',
                aip_uuid, expected_bytes, threshold,
            )
            return duracloud_ops.copy_aip_from_duracloud(aip_uuid, repo_uuid)
        logger.warning(
            'copy_aip_to_wasabi: aip %s is %s bytes (>= %s) but DuraCloud '
            'is not configured — falling back to the AM download path, '
            'which is unreliable at this size. Set DURACLOUD_* in the '
            'curation .env.',
            aip_uuid, expected_bytes, threshold,
        )

    # --- Step 2: Idempotency probe --------------------------------------
    # An object already present at the expected size short-circuits the
    # copy, which is what makes caller retries safe.
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
        # Delete and re-upload, logged at WARNING.
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
    # Seed the progress file at 0 bytes before the stream opens so the
    # dashboard shows a bar immediately; the throttled hook advances it.
    # The finally-clear runs on EVERY exit, so a dead copy never leaves a
    # stale "in progress" file for the copy-progress route to serve.
    if expected_bytes:
        write_copy_progress(aip_uuid, 0, expected_bytes)
    # Bound the download's silence: the read timeout covers time-to-first-
    # byte AND mid-stream gaps. Generous but finite — AM is silent while
    # it prepares a large package, yet a dead AM must still fail.
    read_timeout_s = config.AM_DOWNLOAD_READ_TIMEOUT_SECONDS
    dl_timeout = (30, read_timeout_s if read_timeout_s and read_timeout_s > 0 else None)
    logger.info(
        'copy_aip_to_wasabi requesting AM download aip_uuid=%s '
        'expected_bytes=%s read_timeout_s=%s — first byte can take a '
        'long time for large AIPs; silence here is AM preparing the '
        'package', aip_uuid, expected_bytes, dl_timeout[1],
    )
    try:
        with requests.get(
            _am_download_url(aip_uuid),
            # identity: raw reads must see entity bytes, not a
            # negotiated gzip stream (see duracloud_ops 2026-08-02).
            headers={**_am_storage_auth_header(),
                     'Accept-Encoding': 'identity'},
            stream=True,
            timeout=dl_timeout,
        ) as dl:
            if dl.status_code != 200:
                out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
                out['error'] = f'am download returned HTTP {dl.status_code}'
                return out
            logger.info(
                'copy_aip_to_wasabi AM download streaming aip_uuid=%s '
                'first_byte_after_ms=%d',
                aip_uuid, int((time.monotonic() - started) * 1000),
            )
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
