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
AIP-store routes — v2 ingest Stage 6 entry points.

The repo-backend-v2 ingester calls these endpoints to copy AM-produced
AIP packages from Archivematica Storage Service to Wasabi S3. All
Wasabi credential handling and boto3 work stays on this side.

Endpoints:

    POST /api/v2/aip/copy-to-wasabi
        Body: {"aip_uuid": "<am-uuid>", "repo_uuid": "<repo-pid>"}
        Returns: 200 with {ok, bucket, key, bytes, elapsed_ms, error?}

        Synchronous. Downloads the AIP from AM Storage Service via
        /api/v2/file/<aip_uuid>/download/ and streams to Wasabi via
        boto3 upload_fileobj. Idempotent: a second call for an
        aip_uuid whose key already exists in Wasabi at the expected
        size is a no-op that still returns ok=true with the existing
        metadata, which is what makes caller retries safe.

    POST /api/v2/aip/presigned-url
        Body: {"key": "<wasabi-object-key>", "ttl_seconds": 900}
        Returns: 200 with {ok, url, expires_at, error?}

        Mints a short-lived presigned GET URL. The dashboard
        download flow 302-redirects the browser to this URL so the
        actual AIP bytes never transit repo-backend-v2 or the curation
        service.

    GET /api/v2/aip/list-objects?token=<continuation>
        Returns: 200 with {ok, objects: [{key, size}], next_token,
        error?}

        One page (up to 1000) of the AIP-store bucket's full flat
        inventory — key + size only. Consumed by repo-backend-v2's
        scripts/backfill_aip_sizes.js.

Auth: shared X-API-Key (same scheme as /api/v2/qa/).

Design history and rationale: repo/notes/CURATION_API_CODE_NOTES.md
"""

import logging
import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, jsonify, request

import config

from auth import require_api_key_qa
from lib import wasabi
from lib import aip_ops
from lib import duracloud_ops
from lib import storage_usage

logger = logging.getLogger(__name__)

aip_bp = Blueprint('aip', __name__, url_prefix='/api/v2/aip')


@aip_bp.route('/copy-to-wasabi', methods=['POST'])
@require_api_key_qa
def copy_to_wasabi():
    """
    Copy a single AM-produced AIP from Archivematica Storage Service
    into Wasabi. See module docstring for the wire contract.

    CONTRACT: a bad request is 400; a failed attempt is 200 with
    ok=false, never a 5xx.
    """
    body = request.get_json(silent=True) or {}
    aip_uuid = (body.get('aip_uuid') or '').strip()
    repo_uuid = (body.get('repo_uuid') or '').strip()
    if not aip_uuid:
        return jsonify({'ok': False, 'error': 'aip_uuid is required'}), 400
    if not repo_uuid:
        return jsonify({'ok': False, 'error': 'repo_uuid is required'}), 400

    started = time.monotonic()
    try:
        result = aip_ops.copy_aip_to_wasabi(aip_uuid=aip_uuid, repo_uuid=repo_uuid)
    except Exception as e:
        # Unexpected failures still return 200 + ok=false, so the caller
        # records the row as failed instead of entering a transport-error
        # retry loop.
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.exception(
            'copy_to_wasabi: unhandled exception aip_uuid=%s repo_uuid=%s',
            aip_uuid, repo_uuid,
        )
        return jsonify({
            'ok': False,
            'bucket': None,
            'key': None,
            'bytes': None,
            'elapsed_ms': elapsed_ms,
            'error': f'unhandled: {e}',
        }), 200

    return jsonify(result), 200


@aip_bp.route('/copy-from-duracloud', methods=['POST'])
@require_api_key_qa
def copy_from_duracloud():
    """
    Failover twin of /copy-to-wasabi: same body, same response
    envelope (plus source='duracloud'), but the AIP bytes come from
    DuraCloud's aip-store replica instead of AM Storage Service —
    chunk-reassembled and MD5-verified against the .dura-manifest.
    Used when AM's download path cannot serve large AIPs.
    """
    body = request.get_json(silent=True) or {}
    aip_uuid = (body.get('aip_uuid') or '').strip()
    repo_uuid = (body.get('repo_uuid') or '').strip()
    if not aip_uuid:
        return jsonify({'ok': False, 'error': 'aip_uuid is required'}), 400
    if not repo_uuid:
        return jsonify({'ok': False, 'error': 'repo_uuid is required'}), 400

    started = time.monotonic()
    try:
        result = duracloud_ops.copy_aip_from_duracloud(
            aip_uuid=aip_uuid, repo_uuid=repo_uuid
        )
    except Exception as e:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.exception(
            'copy_from_duracloud: unhandled exception aip_uuid=%s repo_uuid=%s',
            aip_uuid, repo_uuid,
        )
        return jsonify({
            'ok': False,
            'bucket': None,
            'key': None,
            'bytes': None,
            'elapsed_ms': elapsed_ms,
            'error': f'unhandled: {e}',
            'source': 'duracloud',
        }), 200

    return jsonify(result), 200


@aip_bp.route('/bucket-usage', methods=['GET'])
@require_api_key_qa
def bucket_usage():
    """
    Cached storage-utilization readout for both Wasabi buckets
    (batch backups + AIP store). Serves the cache instantly; when the
    cache is missing or older than WASABI_USAGE_TTL_SECONDS (24h
    default) a background recompute starts and `computing: true`
    tells the caller to poll. Completed objects only — multipart
    debris is invisible to listings (see lib/storage_usage.py).
    """
    return jsonify({'ok': True, **storage_usage.get_usage()}), 200


@aip_bp.route('/bucket-usage/refresh', methods=['POST'])
@require_api_key_qa
def bucket_usage_refresh():
    """Force a background recompute regardless of cache freshness."""
    storage_usage.trigger_recompute(force=True)
    return jsonify({'ok': True, **storage_usage.get_usage()}), 200


@aip_bp.route('/copy-progress/<aip_uuid>', methods=['GET'])
@require_api_key_qa
def copy_progress(aip_uuid):
    """
    Live byte progress for an in-flight /copy-to-wasabi call.

    Returns 200 {ok, aip_uuid, bytes_sent, total_bytes, updated_at}
    while a copy is streaming (the uploading worker maintains a
    per-AIP progress file — see lib/aip_ops.write_copy_progress), and
    404 {ok: false} when there is no active copy — not started, or
    already finished (the file is cleared on every exit).

    Cheap local file read only: no AM or Wasabi round-trips, since
    callers poll this alongside the long synchronous copy call. The 404
    departs from the copy routes' 200+ok=false convention; callers treat
    any non-200 as "no data".
    """
    aip_uuid = (aip_uuid or '').strip()
    if not aip_ops._PROGRESS_UUID_RE.match(aip_uuid):
        return jsonify({'ok': False, 'error': 'aip_uuid must be a UUID'}), 400
    progress = aip_ops.read_copy_progress(aip_uuid)
    if progress is None:
        return jsonify({'ok': False, 'error': 'no active copy'}), 404
    return jsonify({
        'ok': True,
        'aip_uuid': progress.get('aip_uuid') or aip_uuid.lower(),
        'bytes_sent': progress.get('bytes_sent'),
        'total_bytes': progress.get('total_bytes'),
        'updated_at': progress.get('updated_at'),
    }), 200


@aip_bp.route('/list-objects', methods=['GET'])
@require_api_key_qa
def list_aip_objects():
    """
    One page of the AIP-store bucket inventory (key + size). Pass the
    returned next_token back as ?token= to walk the whole bucket —
    ~21 pages for the current ~20.9k-object store. Same 200 + ok=false
    error envelope as the other AIP routes.
    """
    if not config.WASABI_AIP_BUCKET:
        return jsonify({
            'ok': False,
            'error': (
                'WASABI_AIP_BUCKET is not configured. Set it in the '
                'curation service .env and restart.'
            ),
        }), 200

    token = (request.args.get('token') or '').strip() or None
    try:
        res = wasabi.list_objects(
            '',
            continuation_token=token,
            bucket_config=config.WASABI_AIP_BUCKET,
            max_keys=1000,
            recursive=True,
        )
    except Exception as e:
        logger.exception('list_aip_objects: listing failed')
        return jsonify({'ok': False, 'error': str(e)}), 200

    return jsonify({
        'ok': True,
        'objects': [
            {'key': o['key'], 'size': o['size']} for o in res['objects']
        ],
        'next_token': res['next_token'],
    }), 200


@aip_bp.route('/presigned-url', methods=['POST'])
@require_api_key_qa
def presigned_url():
    """
    Mint a presigned GET URL for an AIP key in Wasabi. The dashboard
    download flow 302-redirects to this URL; the browser pulls the
    bytes directly from Wasabi without transiting either service.

    `ttl_seconds` is clamped to the [60, 3600] range. The Node config
    default is 900 (15 min) — short enough that a leaked URL has
    minimal blast radius but long enough for a staff member to
    actually click the download link.
    """
    body = request.get_json(silent=True) or {}
    key = (body.get('key') or '').strip()
    ttl_seconds_raw = body.get('ttl_seconds') or 900
    try:
        ttl_seconds = int(ttl_seconds_raw)
    except (TypeError, ValueError):
        return jsonify({'ok': False, 'error': 'ttl_seconds must be an integer'}), 400
    ttl_seconds = max(60, min(3600, ttl_seconds))

    if not key:
        return jsonify({'ok': False, 'error': 'key is required'}), 400

    # AIP presigned URLs must point at the AIP-store bucket, not the
    # SFTP-staging bucket. Refuse cleanly if the operator hasn't set
    # WASABI_AIP_BUCKET — same gate aip_ops.copy_aip_to_wasabi uses.
    if not config.WASABI_AIP_BUCKET:
        return jsonify({
            'ok': False,
            'error': (
                'WASABI_AIP_BUCKET is not configured. Set it in the '
                'curation service .env and restart before issuing '
                'AIP download URLs.'
            ),
        }), 200

    try:
        url = wasabi.generate_presigned_url(
            key,
            ttl_seconds=ttl_seconds,
            bucket_config=config.WASABI_AIP_BUCKET,
        )
    except Exception as e:
        logger.exception('presigned_url: failed for key=%s', key)
        return jsonify({'ok': False, 'error': str(e)}), 200

    expires_at = (
        datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    ).isoformat()
    return jsonify({
        'ok': True,
        'url': url,
        'expires_at': expires_at,
    }), 200
