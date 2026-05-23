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
Archivematica staging routes (legacy: digitaldu-backend-qa).

URL prefix preserved as /api/v2/qa/ so the ingest service does not need code
changes during cutover. Response shapes preserved verbatim from the legacy
service — Stage 4 will normalize them to {result, errors}.

2026-05-23 cancel-flow safety patch (curration-api-modified):
  - `/move-to-ingest` now surfaces HTTP 409 when ops returns
    result='move_in_progress' so the Node side can distinguish lock
    contention from other failures.
  - `/move-from-ingest-to-ready` accepts optional `?actor=<du_id>` for
    cross-reference logging, and now maps the ops result to:
        200 for success / already_in_ready
        409 for move_in_progress (per-uuid lock held)
        500 for move_failed / destination_create_failed / source_not_found
"""

import json

from flask import Blueprint, request

from auth import require_api_key_qa
from lib import archivematica_ops as ops

qa_bp = Blueprint('qa', __name__, url_prefix='/api/v2/qa')


@qa_bp.route('/list-ready-folders', methods=['GET'])
@require_api_key_qa
def list_ready_folders():
    return json.dumps(ops.get_ready_folders()), 200


@qa_bp.route('/set-collection-folder', methods=['GET'])
@require_api_key_qa
def set_collection_folder_name():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    is_set = ops.set_collection_folder_name(folder)
    return json.dumps(dict(is_set=is_set)), 200


@qa_bp.route('/check-collection-folder', methods=['GET'])
@require_api_key_qa
def check_collection_folder_name():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    folder_name_results = ops.check_folder_name(folder)
    return json.dumps(dict(folder_name_results=folder_name_results)), 200


@qa_bp.route('/package-names', methods=['GET'])
@require_api_key_qa
def get_package_names():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    packages = ops.get_package_names(folder)
    return json.dumps(dict(packages=packages)), 200


@qa_bp.route('/check-package-names', methods=['GET'])
@require_api_key_qa
def check_package_names():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    package_name_results = ops.check_package_names(folder)
    return json.dumps(dict(package_name_results=package_name_results)), 200


@qa_bp.route('/check-file-names', methods=['GET'])
@require_api_key_qa
def check_file_names():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    file_count_results = ops.check_file_names(folder)
    return json.dumps(dict(file_count_results=file_count_results)), 200


@qa_bp.route('/check-uri-txt', methods=['GET'])
@require_api_key_qa
def check_uri_txt():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    uri_results = ops.check_uri_txt(folder)
    return json.dumps(dict(uri_results=uri_results)), 200


@qa_bp.route('/get-uri-txt', methods=['GET'])
@require_api_key_qa
def get_uri_txt():
    folder = request.args.get('folder')
    package = request.args.get('package')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    if package is None:
        return json.dumps(['Bad Request: Missing package param']), 400
    uri = ops.get_uri_txt(folder, package)
    return json.dumps(dict(uri_results=uri)), 200


@qa_bp.route('/get-total-batch-size', methods=['GET'])
@require_api_key_qa
def get_total_batch_size():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    total_batch_size = ops.get_total_batch_size(folder)
    return json.dumps(dict(total_batch_size=total_batch_size)), 200


@qa_bp.route('/package-file-count', methods=['GET'])
@require_api_key_qa
def get_package_file_count():
    folder = request.args.get('folder')
    package = request.args.get('package')
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param']), 400
    file_count = ops.get_package_file_count(folder, package)
    if file_count is None:
        return json.dumps(['Package not found']), 404
    return json.dumps(dict(file_count=file_count)), 200


@qa_bp.route('/move-to-ingest', methods=['GET'])
@require_api_key_qa
def move_to_ingest():
    """
    Forward direction: move a package from 001-ready/<folder>/<package>
    into 002-ingest/<uuid>/<package>. Called by the ingest service's
    Stage 2 upload worker.

    Required query params:
      uuid    — destination directory name in 002-ingest
      folder  — source batch folder name (relative to ready_path)
      package — source package directory inside the batch folder

    Responses:
      200 — move succeeded
      400 — missing required param
      409 — per-uuid lock held by another in-flight operation
            (concurrent move_from_ingest_to_ready or another
            move_to_ingest in flight); caller should retry after a backoff
    """
    uuid = request.args.get('uuid')
    folder = request.args.get('folder')
    package = request.args.get('package')
    if uuid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param.']), 400
    # Note: legacy did not check `package`; preserved here so existing
    # callers don't regress. ops.move_to_ingest tolerates None package
    # by surfacing the error inside the move itself.
    results = ops.move_to_ingest(uuid, folder, package)
    status = 409 if results.get('result') == 'move_in_progress' else 200
    return json.dumps(results), status


@qa_bp.route('/move-from-ingest-to-ready', methods=['GET'])
@require_api_key_qa
def move_from_ingest_to_ready():
    """
    Rollback inverse of /move-to-ingest. Used by the ingest service's:
      - pre-ingest rollback action (legacy: ROLLED_BACK_TO_READY)
      - return-to-packaging action (post-cancel cleanup)

    Required query params (all three):
      uuid    — directory name in 002-ingest (set by move_to_ingest)
      folder  — destination batch folder name (relative to ready_path)
      package — package directory inside 002-ingest/<uuid>/

    Optional query param:
      actor   — staff identifier (du_id or email). Logged at INFO for
                cross-reference with the ingest service's audit trail.
                Never used for authz; the X-API-Key header is the gate.

    Behavior summary (see lib/archivematica_ops.move_from_ingest_to_ready
    for full details):
      * Per-uuid lock prevents concurrent moves.
      * Best-effort cleans the Archivematica SFTP staging copy first
        (handles the case where Stage 2's move_to_sftp had progressed
        before the cancel landed).
      * Moves the package back to 001-ready/<folder>/<package>;
        uri.txt is preserved so the folder reappears in /processed
        (Packaging and Ingesting dashboard view).
      * Removes the empty 002-ingest/<uuid>/ directory after the move.
      * Idempotent: if the package is already in 001-ready, returns
        success without touching disk.

    Responses:
      200 — move succeeded OR package was already in 001-ready
      400 — missing required param
      409 — per-uuid lock held by another in-flight operation
      500 — move failed; see `errors` and `sftp_clean` in the body
    """
    uuid = request.args.get('uuid')
    folder = request.args.get('folder')
    package = request.args.get('package')
    actor = request.args.get('actor')  # Optional; threaded into ops for audit.
    if uuid is None:
        return json.dumps(['Bad Request: Missing uuid param.']), 400
    if folder is None:
        return json.dumps(['Bad Request: Missing folder param.']), 400
    if package is None:
        return json.dumps(['Bad Request: Missing package param.']), 400

    results = ops.move_from_ingest_to_ready(uuid, folder, package, actor=actor)

    # Map the structured result to an HTTP status the Node side can
    # branch on:
    #   move_in_progress       -> 409 Conflict (retry-friendly)
    #   move_failed / dest_*   -> 500 Internal Server Error
    #   everything else        -> 200 OK
    result = results.get('result')
    if result == 'move_in_progress':
        status = 409
    elif result in ('move_failed', 'destination_create_failed', 'source_not_found'):
        status = 500
    else:
        status = 200
    return json.dumps(results), status


@qa_bp.route('/move-to-sftp', methods=['GET'])
@require_api_key_qa
def move_to_sftp():
    uuid = request.args.get('uuid')
    if uuid is None:
        return json.dumps(['Bad Request: Missing pid param.']), 400
    ops.move_to_sftp(uuid)
    return json.dumps(dict(message='Uploading packages to Archivematica sftp')), 200


@qa_bp.route('/upload-status', methods=['GET'])
@require_api_key_qa
def check_sftp():
    uuid = request.args.get('uuid')
    total_batch_file_count = request.args.get('total_batch_file_count')
    if uuid is None:
        return json.dumps(['Bad Request: Missing uuid param.']), 400
    if total_batch_file_count is None:
        return json.dumps(dict(message='File count not found.', data=[])), 200
    results = ops.check_sftp(uuid, total_batch_file_count)
    return json.dumps(results), 200


@qa_bp.route('/move-to-ingested', methods=['GET'])
@require_api_key_qa
def move_to_ingested():
    folder = request.args.get('folder')
    uuid = request.args.get('uuid')
    if folder == 'collection':
        folder = ops.get_collection_folder_name()
    results = ops.move_to_ingested(uuid, folder)
    return json.dumps(results), 200


@qa_bp.route('/reset_permissions', methods=['GET'])
@require_api_key_qa
def reset_permissions():
    folder = request.args.get('folder')
    if folder is None:
        return json.dumps(['Bad Request.']), 400
    is_reset = ops.reset_permissions(folder)
    return json.dumps(is_reset), 200


@qa_bp.route('/cleanup_sftp', methods=['GET'])
@require_api_key_qa
def clean_up_sftp():
    uuid = request.args.get('uuid')
    archival_package = request.args.get('archival_package')
    ops.clean_up_sftp(uuid, archival_package)
    return json.dumps('collection folder removed'), 200
