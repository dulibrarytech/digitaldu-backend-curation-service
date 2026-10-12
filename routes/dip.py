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
DIP routes: the ingest's copy of a new DIP's served files from DuraCloud
Preserve to Wasabi (Stage 4 in repo-backend-v2, once Archivematica writes
to DCP).

    POST /api/v2/dip/copy-to-wasabi
        Body: {"sip_uuid", "dip_path", "expected_size"}
          dip_path       <eight 4-hex-digit segments>/<folder>, as
                         repo-backend-v2's get_dip_path returns it (the
                         segments are the DIP's uuid)
          expected_size  Archivematica's recorded size of the DIP
                         package, in bytes (a JSON integer)
        Returns: 200 with dip_ops.copy_dip's dict plus elapsed_ms: {ok,
            status, dcp_bytes, expected_bytes, mets, files: {total,
            copied, skipped, failed}, bytes: {total, copied}, verified:
            {md5, parts, size}, failures: [{key, error}], error}
          status 'copied'      every served file is in Wasabi (ok=true)
                 'incomplete'  the DCP folder doesn't add up to
                               expected_size yet, or its METS isn't
                               there; nothing copied, ask again later
                 'failed'      a file, a listing or a setting failed

        Synchronous: seconds for most DIPs, minutes for one with large
        PDFs. Never calls Archivematica: repo-backend-v2 has already
        checked that the DIP package is UPLOADED and passes its size.
        Idempotent: files Wasabi already holds are skipped, so a retry
        resumes.

        Disk: files are copied one at a time, and one over 64 MiB is
        spooled to the service's temporary directory while it is
        verified: up to 2.1 GB (the largest PDF) in TMPDIR (/tmp unless
        the unit sets it).

Auth: shared X-API-Key (same scheme as /api/v2/aip/).
"""

import logging
import re
import time

from flask import Blueprint, jsonify, request

from auth import require_api_key_qa
from lib import dip_ops

logger = logging.getLogger(__name__)

dip_bp = Blueprint('dip', __name__, url_prefix='/api/v2/dip')

_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE,
)
# Eight 4-hex-digit segments, then one folder name (not . or ..).
_DIP_PATH_RE = re.compile(r'^(?:[0-9a-fA-F]{4}/){8}(?!\.\.?$)[^/]+$')


@dip_bp.route('/copy-to-wasabi', methods=['POST'])
@require_api_key_qa
def copy_to_wasabi():
    """
    Copy one DIP's served files to Wasabi. See the module docstring.

    CONTRACT (as for the AIP routes): a bad request is 400; a failed
    attempt is 200 with ok=false, never a 5xx.
    """
    body = request.get_json(silent=True) or {}
    sip_uuid = str(body.get('sip_uuid') or '').strip()
    dip_path = str(body.get('dip_path') or '').strip().strip('/')
    expected_size = body.get('expected_size')
    if not _UUID_RE.match(sip_uuid):
        return jsonify({'ok': False, 'error': 'sip_uuid must be a UUID'}), 400
    if not _DIP_PATH_RE.match(dip_path):
        return jsonify({
            'ok': False,
            'error': 'dip_path must be eight 4-hex-digit segments and a folder name',
        }), 400
    if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size < 1:
        return jsonify({'ok': False, 'error': 'expected_size must be a positive integer'}), 400

    started = time.monotonic()
    try:
        out = dip_ops.copy_dip(dip_path, expected_size, sip_uuid=sip_uuid)
    except Exception as e:
        # Settings and listings: ok=false, so repo-backend-v2 halts the row
        # with this text instead of retrying a transport error.
        logger.exception('dip copy failed dip_path=%s', dip_path)
        out = {'ok': False, 'status': 'failed', 'error': f'{type(e).__name__}: {e}'}
    out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
    logger.info(
        'dip copy %s sip_uuid=%s dip_path=%s files=%s bytes=%s verified=%s elapsed_ms=%d',
        out['status'], sip_uuid, dip_path, out.get('files'), out.get('bytes'),
        out.get('verified'), out['elapsed_ms'],
    )
    return jsonify(out), 200
