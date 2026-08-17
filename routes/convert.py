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
Derivative conversion + serving routes.

    POST /api/v1/convert/tiff
        Body {sip_uuid, full_path, object_name, mime_type}.
        SYNCHRONOUS — the response reports the real outcome (200 with
        {file_name, bytes}), unlike the Node service's 202-before-
        converting, which is how a full disk reported success for a
        day. 507 when the share is low on space; 404 when the source
        TIFF is not in the dip-store.

    GET /api/v1/image?filename=<name>.jpg
        Byte-serves one derivative with Range support. Status shapes
        are the repov2 verifier's classification contract:
        200/206 real file · 400 {errors:['File is empty']} 0-byte ·
        404 {error:true} missing.

Auth: shared X-API-Key / api_key (same scheme as the other blueprints),
with the Node service's 401 JSON shape.
"""

import json
import logging
import os
from functools import wraps

from flask import Blueprint, jsonify, request, send_file

from auth import _extract_and_check_key
from lib import convert_ops

logger = logging.getLogger(__name__)

convert_bp = Blueprint('convert', __name__)


def require_api_key_convert(f):
    """Convert-shape auth: 401 {error:true, message} on failure."""
    @wraps(f)
    def decorated(*args, **kwargs):
        ok, expected_present = _extract_and_check_key()
        if not expected_present:
            logger.error('API_KEY environment variable not configured')
            return jsonify({'error': True, 'status': 500,
                            'message': 'Server configuration error'}), 500
        if not ok:
            return jsonify({'error': True, 'status': 401,
                            'message': 'Unauthorized request'}), 401
        return f(*args, **kwargs)
    return decorated


def _validation_errors(body):
    """Payload validation, mirroring the Node service's checks."""
    errors = []
    if not isinstance(body, dict):
        return ['Request body must be a valid object']
    for field in ('sip_uuid', 'full_path', 'object_name'):
        value = body.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append('%s is required' % field)
    mime = body.get('mime_type')
    if mime is not None and mime != '' and not str(mime).startswith('image/'):
        errors.append('mime_type must be an image type')
    return errors


@convert_bp.route('/api/v1/convert/tiff', methods=['POST'])
@require_api_key_convert
def convert_tiff():
    body = request.get_json(silent=True)
    errors = _validation_errors(body)
    if errors:
        return jsonify({'error': True, 'status': 400,
                        'message': 'Bad request', 'errors': errors}), 400

    object_name = str(body.get('object_name') or '').strip()
    try:
        result = convert_ops.convert_and_store(body)
    except convert_ops.ConvertError as error:
        logger.error('conversion failed for %s: %s', object_name, error)
        return jsonify({'error': True, 'status': error.status,
                        'message': str(error),
                        'data': {'object_name': object_name}}), error.status
    except Exception as error:  # noqa: BLE001 — route boundary
        logger.exception('unhandled conversion error for %s', object_name)
        return jsonify({'error': True, 'status': 500,
                        'message': 'Failed to process %s' % object_name,
                        'data': {'object_name': object_name}}), 500

    return jsonify({
        'error': False,
        'status': 200,
        'message': 'Successfully processed %s' % object_name,
        'data': {
            'object_name': object_name,
            'file_name': result['file_name'],
            'bytes': result['bytes'],
            'processing_time_ms': result['processing_time_ms'],
        },
    }), 200


@convert_bp.route('/api/v1/image', methods=['GET'])
@require_api_key_convert
def get_image():
    filename = str(request.args.get('filename') or '').strip()
    path = convert_ops.image_path(filename)
    if path is None:
        return jsonify({'error': True, 'status': 400,
                        'message': 'Invalid filename',
                        'errors': ['filename must be a plain .jpg name']}), 400

    try:
        stats = os.stat(path)
    except OSError:
        return jsonify({'error': True, 'status': 404,
                        'message': 'Resource not found'}), 404

    if stats.st_size == 0:
        # The 0-byte signature (disk-full incident). Same shape the
        # Node service uses — the repov2 verifier keys on it.
        return jsonify({'error': True, 'status': 400,
                        'message': 'Invalid file',
                        'errors': ['File is empty']}), 400

    # conditional=True gives Range (206) + ETag/Last-Modified handling.
    return send_file(path, mimetype='image/jpeg', conditional=True)


# QA-style probe used by deploy smoke tests.
@convert_bp.route('/api/v1/convert/status', methods=['GET'])
@require_api_key_convert
def convert_status():
    try:
        free_bytes = convert_ops.check_storage()
        return json.dumps({'ok': True, 'free_bytes': free_bytes}), 200
    except convert_ops.ConvertError as error:
        return json.dumps({'ok': False, 'error': str(error)}), 200
