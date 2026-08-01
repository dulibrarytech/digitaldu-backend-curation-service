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
attributes. Per-endpoint env-var checks remain in the route handlers (they
existed in the legacy services and may catch deployment errors that startup
validation misses).
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
# Separate bucket for AIP-store operations (see lib/aip_ops.py +
# routes/aip.py). DU's deployment uses TWO Wasabi buckets:
#
#   WASABI_BUCKET     → SFTP-staging archive (move_to_ingested writes
#                       here; staff `<package>/` folders land at the
#                       configured base prefix).
#   WASABI_AIP_BUCKET → AM-produced AIP packages (Stage 6 writes here;
#                       legacy migration's ~20k rows already live here).
#
# Conventional value in prod:
#   WASABI_AIP_BUCKET=s3://library-repository/aip-store/
#
# Required for /api/v2/aip/* endpoints. If unset, those routes
# refuse with ok=false rather than silently routing AIPs to the
# SFTP-staging bucket.
WASABI_AIP_BUCKET = os.getenv('WASABI_AIP_BUCKET')
WASABI_PROFILE = os.getenv('WASABI_PROFILE')
UID = os.getenv('UID')
GID = os.getenv('GID')
ERRORS_FILE = os.getenv('ERRORS_FILE')
BATCH_SIZE_LIMIT = os.getenv('BATCH_SIZE_LIMIT')
AWS_DEFAULT_PROFILE = os.getenv('AWS_DEFAULT_PROFILE')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')

# --- Archivematica Storage Service (AIP retrieval) ---------------------------
# Used by lib/aip_ops.py to fetch AM-produced AIPs and pipe them to
# Wasabi (Stage 6 + backfill). NOT used by the existing
# archivematica_ops.py — that one handles filesystem moves, not API
# calls. These values are conventionally the same as the v2 ingest
# worker's ARCHIVEMATICA_STORAGE_* env values (the AM Storage Service
# only has one set of credentials per instance), so the simplest
# setup is to copy them from repo-backend-v2's .env into the
# curation service's .env.
#
# Auth scheme is the AM Storage Service's ApiKey header:
#   Authorization: ApiKey <username>:<api_key>
#
# All three are REQUIRED for the AIP endpoints (/api/v2/aip/*). If
# any is unset, aip_ops.copy_aip_to_wasabi fails on the first HTTP
# call with an auth/transport error rather than at import time, so
# the failure surfaces in the v2 AIPs dashboard as a per-row error
# string rather than crashing the worker.
ARCHIVEMATICA_STORAGE_API = os.getenv('ARCHIVEMATICA_STORAGE_API')
ARCHIVEMATICA_STORAGE_USERNAME = os.getenv('ARCHIVEMATICA_STORAGE_USERNAME')
ARCHIVEMATICA_STORAGE_API_KEY = os.getenv('ARCHIVEMATICA_STORAGE_API_KEY')

# Read timeout (seconds) for the AM Storage Service /download/ stream
# in aip_ops.copy_aip_to_wasabi. This bounds BOTH the time-to-first-
# byte and any mid-stream silence. It must be GENEROUS: AM Storage
# spends a long time preparing a package before the first byte arrives
# (~68s observed for a 327 MB AIP, 2026-07-31 — hours for a 66 GB one),
# and during that window the connection is silent by design. Too small
# kills legitimate large-AIP downloads mid-prep; the pre-2026-07-31
# open-ended value turned a genuinely dead AM into an invisible hang
# bounded only by the caller's 12h budget. Default 6h. Set 0 to
# restore the open-ended behavior.
try:
    AM_DOWNLOAD_READ_TIMEOUT_SECONDS = int(
        os.getenv('AM_DOWNLOAD_READ_TIMEOUT_SECONDS', '21600')
    )
except ValueError:
    AM_DOWNLOAD_READ_TIMEOUT_SECONDS = 21600

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
