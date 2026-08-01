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
Batch-archive browser routes (read-only).

Serves the repov2 dashboard's "Ingested Batch Archive" admin view — a
browse + per-file-download surface over the Wasabi batch archive
(WASABI_BUCKET, layout `<collection>/<package>/<files>`).

READ-ONLY BY CONSTRUCTION: this blueprint contains listing calls and
presigned-GET minting only. No write, delete, or copy operation exists
here, so no request through this surface can mutate the archive.

    GET  /api/v2/archive/collections
        → {result: {collections: [<name>, ...]}, errors: []}
        Top-level prefixes. Follows pagination internally. No sizes or
        counts — computing those means paginating every key in the bucket.

    GET  /api/v2/archive/collections/<collection>/packages?token=
        → {result: {packages: [...], next_token}, errors: []}
        One page (S3 caps 1000) of package names; `token` continues.

    GET  /api/v2/archive/collections/<collection>/packages/<package>/files?token=
        → {result: {files: [{name, key, size, last_modified}],
                    folders: [<name>, ...], next_token}, errors: []}
        Files directly inside the package — ONE level only. `folders`
        names any nested directories (some historical batches have them)
        so the snapshot is truthful, but does not descend into them.

    POST /api/v2/archive/download-url
        Body {"key": "<collection>/<package>/<file>", "ttl_seconds": 900}
        → {ok, url, expires_at} | {ok: false, error}
        Mints a presigned GET for ONE object in the batch bucket. The
        dashboard 302-redirects the browser to it, so file bytes never
        transit repo-backend-v2 or this service.

Auth: shared X-API-Key (same scheme as /api/v2/qa/).

Design history and rationale: repo/notes/CURATION_API_CODE_NOTES.md
"""

import logging
from datetime import datetime, timezone, timedelta

from botocore.exceptions import BotoCoreError, ClientError
from flask import Blueprint, jsonify, request

from auth import require_api_key_qa
from lib import wasabi
from lib.safe_names import validate_segment

logger = logging.getLogger(__name__)

archive_bp = Blueprint('archive', __name__, url_prefix='/api/v2/archive')

# Presigned-URL TTL clamp, matching the AIP presigned route's posture.
TTL_DEFAULT = 900
TTL_MIN = 60
TTL_MAX = 3600


def _error(status, message):
    return jsonify({'result': None, 'errors': [message]}), status


def _validate_key(key):
    """
    Validate a download key: a relative, traversal-free, multi-segment
    path (`<collection>/<package>/<file...>`). Reuses the per-segment
    rules from lib.safe_names on every segment, which rejects `..`,
    control characters, backslashes-in-segment, and leading dashes.
    Returns an error string or None.
    """
    if not key or not isinstance(key, str):
        return 'missing required parameter: key'
    if len(key) > 1024:
        return 'invalid key: too long'
    if key.startswith('/') or key.endswith('/'):
        return 'invalid key: must be a relative object path'
    segments = key.split('/')
    if len(segments) < 2:
        return 'invalid key: expected <collection>/<package or file> path'
    for segment in segments:
        err = validate_segment(segment, 'key segment')
        if err:
            return err
    return None


@archive_bp.route('/collections', methods=['GET'])
@require_api_key_qa
def list_collections():
    """Top-level collection folder names in the batch archive."""
    try:
        names = wasabi.list_all_prefixes('')
        return jsonify({'result': {'collections': names}, 'errors': []}), 200
    except (ClientError, BotoCoreError, RuntimeError) as e:
        logger.error('archive list_collections failed: %s', e)
        return _error(502, 'Could not list the archive (Wasabi unavailable or misconfigured)')


@archive_bp.route('/collections/<collection>/packages', methods=['GET'])
@require_api_key_qa
def list_packages(collection):
    """One page of package folder names inside a collection.

    Optional `q` = server-side S3 prefix search: matches
    package names STARTING WITH q, bucket-side — a migrated collection
    can hold thousands of packages, and the loaded-page filter could
    not see past pagination."""
    err = validate_segment(collection, 'collection')
    if err:
        return _error(400, err)
    token = request.args.get('token') or None
    q = (request.args.get('q') or '').strip()
    if q:
        err = validate_segment(q, 'q')
        if err:
            return _error(400, err)
    try:
        page = (
            wasabi.search_prefixes(collection + '/', q, continuation_token=token)
            if q
            else wasabi.list_prefixes(collection + '/', continuation_token=token)
        )
        return jsonify({
            'result': {
                'packages': page['prefixes'],
                'next_token': page['next_token'],
            },
            'errors': [],
        }), 200
    except (ClientError, BotoCoreError, RuntimeError) as e:
        logger.error('archive list_packages failed for %s: %s', collection, e)
        return _error(502, 'Could not list the archive (Wasabi unavailable or misconfigured)')


@archive_bp.route(
    '/collections/<collection>/packages/<package>/files', methods=['GET']
)
@require_api_key_qa
def list_files(collection, package):
    """One page of files (and any nested folders) inside a package."""
    err = validate_segment(collection, 'collection') or validate_segment(package, 'package')
    if err:
        return _error(400, err)
    token = request.args.get('token') or None
    prefix = collection + '/' + package + '/'
    try:
        page = wasabi.list_objects(prefix, continuation_token=token)
        # Nested folders surface truthfully (some historical batches
        # have them) — one extra listing call, same page semantics.
        folders = wasabi.list_prefixes(prefix)['prefixes'] if not token else []
        return jsonify({
            'result': {
                'files': page['objects'],
                'folders': folders,
                'next_token': page['next_token'],
            },
            'errors': [],
        }), 200
    except (ClientError, BotoCoreError, RuntimeError) as e:
        logger.error('archive list_files failed for %s/%s: %s', collection, package, e)
        return _error(502, 'Could not list the archive (Wasabi unavailable or misconfigured)')


@archive_bp.route('/download-url', methods=['POST'])
@require_api_key_qa
def download_url():
    """Mint a presigned GET URL for one archive object."""
    try:
        data = request.get_json(force=False, silent=False)
    except Exception:
        return jsonify({'ok': False, 'error': 'Invalid JSON in request body'}), 400
    if not isinstance(data, dict):
        return jsonify({'ok': False, 'error': 'Request body must be a JSON object'}), 400

    key = data.get('key')
    err = _validate_key(key)
    if err:
        return jsonify({'ok': False, 'error': err}), 400

    ttl = data.get('ttl_seconds', TTL_DEFAULT)
    if not isinstance(ttl, int):
        ttl = TTL_DEFAULT
    ttl = max(TTL_MIN, min(TTL_MAX, ttl))

    try:
        # bucket_config=None → WASABI_BUCKET (the batch archive).
        url = wasabi.generate_presigned_url(key, ttl_seconds=ttl)
    except (ClientError, BotoCoreError, RuntimeError) as e:
        logger.error('archive download-url failed for %s: %s', key, e)
        return jsonify({
            'ok': False,
            'error': 'Could not mint a download URL (Wasabi unavailable or misconfigured)',
        }), 200

    expires_at = (
        datetime.now(timezone.utc) + timedelta(seconds=ttl)
    ).isoformat()
    logger.info('archive download-url minted key=%s ttl=%d', key, ttl)
    return jsonify({'ok': True, 'url': url, 'expires_at': expires_at}), 200
