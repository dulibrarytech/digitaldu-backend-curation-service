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
API key authentication.
"""

import hmac
import json
import logging
import os
from functools import wraps

from flask import jsonify, request

logger = logging.getLogger(__name__)


def _extract_and_check_key():
    """Return (ok, expected_present).

    ok=True means the request is authenticated.
    expected_present=False means the server is misconfigured (no API_KEY env).
    """
    provided = (
        request.headers.get('X-API-Key')
        or request.args.get('api_key', '')
    ).strip()
    expected = os.getenv('API_KEY')

    if not expected:
        return False, False

    if not provided:
        return False, True

    if len(provided) > 256:
        return False, True

    return hmac.compare_digest(provided, expected), True


def require_api_key_qa(f):
    """Legacy QA-shape auth decorator.

    On failure: json.dumps(['Access denied.']), 403
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        ok, _ = _extract_and_check_key()
        if not ok:
            return json.dumps(['Access denied.']), 403
        return f(*args, **kwargs)
    return decorated


def require_api_key_astools(f):
    """Legacy ASTools-shape auth decorator.

    On failure: jsonify({'result': None, 'errors': [...]}), 401
    On server misconfiguration: jsonify({...}), 500
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        ok, expected_present = _extract_and_check_key()
        if not expected_present:
            logger.error('API_KEY environment variable not configured')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500
        if not ok:
            logger.warning(
                'Auth failure from %s', request.remote_addr
            )
            return jsonify({
                'result': None,
                'errors': ['Access denied. Invalid API key.']
            }), 401
        return f(*args, **kwargs)
    return decorated
