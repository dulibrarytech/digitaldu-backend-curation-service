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
Central configuration for the curation API.

All env vars are loaded once at module import and exposed as module-level
attributes. Route handlers additionally re-check the env vars they need,
so a deployment gap surfaces per endpoint as well as at startup.
"""

import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


# --- Auth (shared) -----------------------------------------------------------
API_KEY = os.getenv('API_KEY')

# --- Server ------------------------------------------------------------------
APP_PORT = os.getenv('APP_PORT', '8185')
APP_VERSION = os.getenv('APP_VERSION', 'curation-api 1.0.0')

# --- Archivematica side (was Service A / qa-service) -------------------------
READY_PATH = os.getenv('READY_PATH')
INGEST_PATH = os.getenv('INGEST_PATH')
INGESTED_PATH = os.getenv('INGESTED_PATH')
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_ID = os.getenv('SFTP_ID')
SFTP_PWD = os.getenv('SFTP_PWD')
SFTP_REMOTE_PATH = os.getenv('SFTP_REMOTE_PATH')
WASABI_ENDPOINT = os.getenv('WASABI_ENDPOINT')
WASABI_BUCKET = os.getenv('WASABI_BUCKET')
# Deployment uses TWO Wasabi buckets:
#   WASABI_BUCKET     → batch archive (move_to_ingested writes here)
#   WASABI_AIP_BUCKET → AM-produced AIP packages (Stage 6 writes here),
#                       conventionally s3://bucket/aip-store/
# WASABI_AIP_BUCKET is REQUIRED by /api/v2/aip/*; unset, those routes
# refuse with ok=false rather than writing AIPs to the batch bucket.
WASABI_AIP_BUCKET = os.getenv('WASABI_AIP_BUCKET')
WASABI_PROFILE = os.getenv('WASABI_PROFILE')
# Numeric gid of the shared staff group (e.g. `domain users`) that
# reset_permissions restores on 001-ready batch folders. UID is retired:
# batch owners are individual staff accounts and are never rewritten.
GID = os.getenv('GID')
ERRORS_FILE = os.getenv('ERRORS_FILE')
BATCH_SIZE_LIMIT = os.getenv('BATCH_SIZE_LIMIT')
AWS_DEFAULT_PROFILE = os.getenv('AWS_DEFAULT_PROFILE')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')

# --- Archivematica Storage Service (AIP retrieval) ---------------------------
# Used by lib/aip_ops.py to fetch AM-produced AIPs and pipe them to Wasabi.
# Auth scheme is the AM Storage Service's ApiKey header:
#   Authorization: ApiKey <username>:<api_key>
# Conventionally the same three values as repo-backend-v2's
# ARCHIVEMATICA_STORAGE_* env. All three are REQUIRED by /api/v2/aip/*;
# a missing one fails on the first HTTP call (surfacing as a per-row error
# in the v2 AIPs dashboard), not at import time.
ARCHIVEMATICA_STORAGE_API = os.getenv('ARCHIVEMATICA_STORAGE_API')
ARCHIVEMATICA_STORAGE_USERNAME = os.getenv('ARCHIVEMATICA_STORAGE_USERNAME')
ARCHIVEMATICA_STORAGE_API_KEY = os.getenv('ARCHIVEMATICA_STORAGE_API_KEY')

# Read timeout (seconds) for the AM Storage Service /download/ stream in
# aip_ops.copy_aip_to_wasabi, bounding both time-to-first-byte and
# mid-stream silence. Keep it GENEROUS — AM is silent while it prepares a
# package, which takes hours at tens of GB. Default 6h; 0 = open-ended.
try:
    AM_DOWNLOAD_READ_TIMEOUT_SECONDS = int(
        os.getenv('AM_DOWNLOAD_READ_TIMEOUT_SECONDS', '21600')
    )
except ValueError:
    AM_DOWNLOAD_READ_TIMEOUT_SECONDS = 21600

# --- DuraCloud (AIP-copy failover source) -----------------------------------
# AM replicates every AIP to DuraCloud's aip-store space; when AM's own
# /download/ endpoint can't serve a large AIP, lib/duracloud_ops.py copies the
# AIP to Wasabi FROM DuraCloud instead. Conventionally the same three
# values as repo-backend-v2's DURACLOUD_* env. Optional — when unset,
# only the /aip/copy-from-duracloud route refuses; the AM path is
# unaffected.
DURACLOUD_API = os.getenv('DURACLOUD_API')
DURACLOUD_USER = os.getenv('DURACLOUD_USER')
DURACLOUD_PWD = os.getenv('DURACLOUD_PWD')

# AIPs at or above this size are copied FROM DURACLOUD BY DEFAULT
# (Artefactual's recommendation: download large AIPs
# directly from DuraCloud — the Storage Service /download/ path is not
# suited to them; it hung, then 502'd, at 66-75 GB). Default 1 GB —
# the same threshold at which DuraCloud chunks content, so "large"
# here means exactly "chunked in DuraCloud". Below the threshold the
# AM path is used as before (fast, no chunk overhead). Set 0 to
# disable routing (AM always, except explicit retry-from-DuraCloud).
# Requires DURACLOUD_* above; when unconfigured, routing is skipped
# with a warning and the AM path is used.
try:
    AIP_DURACLOUD_THRESHOLD_BYTES = int(
        os.getenv('AIP_DURACLOUD_THRESHOLD_BYTES', '1000000000')
    )
except ValueError:
    AIP_DURACLOUD_THRESHOLD_BYTES = 1000000000

# --- TIFF->JPG derivatives (routes/convert.py, lib/convert_ops.py) ----------
# Where derivative JPGs are written/served — a directory on the 9TB share.
# Optional: when unset, the convert routes refuse per-request; the rest of
# the app is unaffected.
DERIVATIVE_STORAGE_PATH = os.getenv('DERIVATIVE_STORAGE_PATH')
# Refuse conversions (HTTP 507) below this much free space (default 10GB) —
# a full volume writes 0-byte files while reporting success (2026-08-04).
DERIVATIVE_MIN_FREE_BYTES = int(
    os.getenv('DERIVATIVE_MIN_FREE_BYTES', str(10 * 1024 * 1024 * 1024))
)
# JPEG quality for generated derivatives (they are the IIIF zoom source).
DERIVATIVE_JPEG_QUALITY = int(os.getenv('DERIVATIVE_JPEG_QUALITY', '85'))
# Refuse sources larger than this (DuraCloud chunks >1GB objects anyway).
DERIVATIVE_MAX_SOURCE_BYTES = int(
    os.getenv('DERIVATIVE_MAX_SOURCE_BYTES', str(2 * 1024 * 1024 * 1024))
)

# --- ArchivesSpace ---------------------
WORKSPACE = os.getenv('WORKSPACE')
ASPACE_USERNAME = os.getenv('ASPACE_USERNAME')
ASPACE_PASSWORD = os.getenv('ASPACE_PASSWORD')
SCRIPT_PATH = os.getenv('SCRIPT_PATH')
SCRIPT_NAME_PY = os.getenv('SCRIPT_NAME_PY', 'make_digital_object.py')
LOG_PATH = os.getenv('LOG_PATH')


def validate_required():
    """Verify env vars that must be set for the app to start.

    Raises RuntimeError listing every missing variable. Per-domain optional
    vars (e.g. SFTP_*) are not checked here — their absence is reported by the
    relevant endpoint when it runs.
    """
    required = {'API_KEY': API_KEY}
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            'Missing required environment variables: ' + ', '.join(missing)
        )
