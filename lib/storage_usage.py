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
Storage-utilization readout: the Wasabi buckets and, once curation has
its DCP key, DuraCloud Preserve's DIP and AIP locations (repo-backend-v2's
home-page storage cards read those two).

S3 has no "bucket size" call — usage is a full ListObjectsV2 walk
summing object sizes. The AIP store is quick (~21k objects ≈ 21 pages)
but the batch-backup bucket can hold hundreds of thousands of objects,
and DCP's DIP location ~266k, so a walk is far too slow for a dashboard
request. Instead:

  - Results are computed in a BACKGROUND thread and cached to a small
    JSON file (same cross-gunicorn-worker file pattern as the AIP
    copy-progress file — workers are separate processes, so in-memory
    caching would be per-worker).
  - `get_usage()` serves the cache instantly, and (optionally) kicks
    off a recompute when the cache is older than the TTL. A marker
    file debounces concurrent recomputes across workers.
  - The dashboard renders the cached numbers with their computed-at
    stamp; a Refresh action forces a recompute.

The numbers are the sum of COMPLETED objects only: in-progress /
abandoned multipart parts are invisible to ListObjectsV2 (they do
count on Wasabi's bill — the console/Stats API are the billing-grade
sources; this readout is the operational one).
"""

import json
import logging
import os
import tempfile
import threading
import time

import config
from lib import dcp, wasabi

logger = logging.getLogger(__name__)

_CACHE_PATH = os.path.join(tempfile.gettempdir(), 'wasabi-bucket-usage.json')
_COMPUTING_MARKER = os.path.join(tempfile.gettempdir(), 'wasabi-bucket-usage.computing')

# A marker older than this is presumed dead (worker recycled mid-walk)
# and no longer blocks a fresh recompute.
_MARKER_STALE_S = 30 * 60


def _ttl_seconds():
    try:
        return int(os.getenv('WASABI_USAGE_TTL_SECONDS', '86400'))  # 24h
    except ValueError:
        return 86400


def _targets():
    """
    (label, env_value, make_client, setting) for the stores to measure. An
    unset Wasabi bucket is skipped — the readout shows what is configured.
    The DCP locations are measured with the DCP client once its key is
    set, and an unset one is then recorded as an error naming its
    `setting`: with the key, DCP is the source for whatever reads that
    location (/health/dcp reports it the same way).
    """
    targets = [
        ('batch_backups', config.WASABI_BUCKET, wasabi._make_client, None),
        ('aip_store', config.WASABI_AIP_BUCKET, wasabi._make_client, None),
    ]
    if dcp.is_configured():
        targets += [
            ('dcp_dip_store', config.DCP_DIP_LOCATION, dcp.make_client, 'DCP_DIP_LOCATION'),
            ('dcp_aip_store', config.DCP_AIP_LOCATION, dcp.make_client, 'DCP_AIP_LOCATION'),
        ]
    return targets


def _measure_bucket(raw_bucket_value, make_client):
    """
    Walk one bucket (scoped to its base prefix) and sum sizes.
    Returns {'bucket', 'prefix', 'objects', 'bytes', 'duration_ms'}.
    """
    started = time.monotonic()
    bucket, base_prefix = wasabi._parse_bucket(raw_bucket_value)
    client = make_client()
    objects = 0
    total_bytes = 0
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get('Contents', []):
            objects += 1
            total_bytes += int(obj.get('Size') or 0)
    return {
        'bucket': bucket,
        'prefix': base_prefix,
        'objects': objects,
        'bytes': total_bytes,
        'duration_ms': int((time.monotonic() - started) * 1000),
    }


def compute_usage():
    """
    Measure every configured bucket and atomically write the cache.
    Per-bucket errors are recorded in place of numbers so one broken
    bucket doesn't hide the other's result. Returns the cache dict.
    """
    buckets = {}
    for label, raw, make_client, setting in _targets():
        if not raw:
            if setting:
                buckets[label] = {'error': f'{setting} is not configured'}
            continue
        try:
            buckets[label] = _measure_bucket(raw, make_client)
        except Exception as e:
            logger.warning('bucket usage failed for %s: %s', label, e)
            buckets[label] = {'error': str(e)[:500]}
    cache = {'computed_at': int(time.time()), 'buckets': buckets}
    try:
        tmp = f'{_CACHE_PATH}.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(cache, f)
        os.replace(tmp, _CACHE_PATH)
    except Exception:
        logger.warning('bucket usage cache write failed', exc_info=True)
    return cache


def _read_cache():
    try:
        with open(_CACHE_PATH, encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug('bucket usage cache read failed', exc_info=True)
        return None


def _computing():
    """True while a recompute marker is present and not presumed dead."""
    try:
        age = time.time() - os.path.getmtime(_COMPUTING_MARKER)
        return age < _MARKER_STALE_S
    except OSError:
        return False


def _spawn(fn):
    """Thread launcher — separated so tests can run the walk inline."""
    threading.Thread(target=fn, daemon=True).start()


def trigger_recompute(force=False):
    """
    Start a background recompute unless one is already running.
    Returns True when a recompute was started (or already running).
    """
    if _computing() and not force:
        return True
    try:
        with open(_COMPUTING_MARKER, 'w', encoding='utf-8') as f:
            f.write(str(int(time.time())))
    except Exception:
        logger.debug('bucket usage marker write failed', exc_info=True)

    def _run():
        try:
            compute_usage()
        finally:
            try:
                os.remove(_COMPUTING_MARKER)
            except OSError:
                pass

    _spawn(_run)
    return True


def _missing_targets(cache):
    """
    Labels of configured targets the cache has no entry for: a cache
    written before the DCP key went in holds no DCP locations until the
    next walk. A failed walk leaves an error entry, which counts.
    """
    buckets = (cache or {}).get('buckets') or {}
    return [
        label for label, raw, _, setting in _targets()
        if (raw or setting) and label not in buckets
    ]


def get_usage(trigger_if_stale=True):
    """
    The dashboard read: cached usage + freshness flags.
        { 'usage': <cache dict or None>,
          'computing': bool,
          'stale': bool }
    When the cache is missing, older than the TTL, or without an entry
    for a configured location (and trigger_if_stale), a background
    recompute is kicked off — the caller renders whatever is cached NOW
    and polls for the update.
    """
    cache = _read_cache()
    age = time.time() - cache['computed_at'] if cache else None
    stale = cache is None or age > _ttl_seconds() or bool(_missing_targets(cache))
    if stale and trigger_if_stale:
        trigger_recompute()
    return {
        'usage': cache,
        'computing': _computing(),
        'stale': stale,
    }
