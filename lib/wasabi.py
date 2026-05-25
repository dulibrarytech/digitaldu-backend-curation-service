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
Wasabi S3 operations.

Replaces the previous `os.system('aws s3 cp ...')` shellout in
archivematica_ops.move_to_s3. Two reasons for the rewrite:

  1. The shellout had a silent data-loss bug: the caller checked
     `if move_result == 1` against the raw return of `os.system`, which
     on POSIX returns the shell-encoded status `(exit_code << 8) | signal`.
     An AWS CLI exit code of 1 came back as 256 — the `== 1` check
     was always false, so the caller would `shutil.rmtree(source)` and
     delete the local files even when the upload had failed. Using
     boto3 lets us return clean 0/1 from one place and raise on
     unexpected errors.

  2. The shellout had no observability. `os.system` discards stdout/
     stderr by default (where it goes depends on how gunicorn was
     started), so a failed upload left no trace anywhere. boto3 gives
     us per-file logging, structured exceptions, and a Callback hook
     for progress on large files.

Auth is unchanged: same `WASABI_PROFILE` from `~/.aws/config`, same
`WASABI_ENDPOINT` URL, same `WASABI_BUCKET` env var as before. No
.env changes needed if those are already populated.
"""

import logging
import os
import posixpath
import time

import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    ProfileNotFound,
)

import config

logger = logging.getLogger(__name__)

# Retry config applied to every S3 API call. "adaptive" mode adds
# client-side backoff on top of the standard exponential retry, which
# is the right default for Wasabi (rare throttling but it does happen
# on large concurrent batches). 5 attempts × adaptive backoff caps a
# transient flake's recovery at ~30s, well under our per-call budget.
_RETRY_CONFIG = Config(
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    # Wasabi sometimes takes a beat to acknowledge large multipart
    # uploads; bump from boto3's 60s default so we don't trip on
    # slow-but-healthy network paths.
    read_timeout=180,
    connect_timeout=30,
)


def _parse_bucket(raw):
    """
    Extract bucket name + optional base key prefix from the WASABI_BUCKET
    env value, which is historically stored in `s3://<bucket>[/<prefix>]/`
    form (the AWS CLI command line shape).

    Returns (bucket_name, base_prefix). base_prefix is '' or a string
    ending in '/'.
    """
    if not raw:
        raise RuntimeError('WASABI_BUCKET is not configured')
    s = raw.strip()
    if s.startswith('s3://'):
        s = s[len('s3://'):]
    s = s.strip('/')
    if '/' not in s:
        return s, ''
    bucket, prefix = s.split('/', 1)
    return bucket, prefix.strip('/') + '/'


def _make_client():
    """
    Build a boto3 S3 client against the configured Wasabi endpoint.

    Uses the named profile in `~/.aws/config` for credentials — same
    profile the AWS CLI used. We deliberately do NOT fall through to
    the env-level AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY pair even
    when those are set: production runs on the profile, and reading
    env vars here would mask "profile broken" failures by quietly
    using a different cred set. Raise loudly if the profile is missing.
    """
    if not config.WASABI_PROFILE:
        raise RuntimeError('WASABI_PROFILE is not configured')
    if not config.WASABI_ENDPOINT:
        raise RuntimeError('WASABI_ENDPOINT is not configured')
    session = boto3.Session(profile_name=config.WASABI_PROFILE)
    return session.client(
        's3',
        endpoint_url=config.WASABI_ENDPOINT,
        config=_RETRY_CONFIG,
    )


class _FileProgress:
    """
    boto3 upload_file Callback. Receives the bytes-transferred delta
    on each chunk. We log at 25/50/75/100% so a large file shows
    progress in the curation-service log without flooding it.

    Single-file scope: instantiate per upload_file call. boto3 calls
    the instance like a function; we keep the running total inside.
    """

    def __init__(self, label, total_bytes):
        self._label = label
        self._total = max(total_bytes, 1)
        self._sent = 0
        self._milestones = {25, 50, 75, 100}

    def __call__(self, bytes_transferred):
        self._sent += bytes_transferred
        pct = int(self._sent * 100 / self._total)
        # Coalesce milestones we've passed since last call.
        passed = [m for m in self._milestones if pct >= m]
        for m in passed:
            self._milestones.discard(m)
            logger.info(
                'wasabi upload %s — %d%% (%d/%d bytes)',
                self._label, m, self._sent, self._total,
            )


def upload_directory(source_dir, folder):
    """
    Recursively upload every regular file under `source_dir` to Wasabi.

    Mirrors the prior AWS-CLI semantics: a file at `<source>/<rel>`
    lands at the key `<base_prefix><folder>/<rel>` (POSIX-joined,
    slashes regardless of host OS).

    Args:
        source_dir: local directory to upload. Trailing slash optional.
        folder:     S3 key prefix segment for THIS upload (the
                    collection / package folder name). May be ''
                    in which case files land at `<base_prefix><rel>`.

    Returns:
        dict with shape {
            'ok': bool,
            'uploaded': int,        # successful file count
            'failed': int,          # failed file count
            'bytes': int,           # total bytes uploaded
            'elapsed_ms': int,
            'errors': [str, ...],   # error messages, capped at 10
        }
        `ok` is True iff every file succeeded AND at least one file
        was uploaded. An empty source directory returns ok=False with
        no errors — we don't silently no-op a vanished source.

    Never raises for per-file errors — they're logged and counted.
    Configuration errors (missing profile, bad endpoint) DO raise so
    they fail loudly at startup or first call.
    """
    started = time.monotonic()
    bucket, base_prefix = _parse_bucket(config.WASABI_BUCKET)
    client = _make_client()

    # Normalize source — strip a trailing slash so os.walk's relpath
    # math doesn't double-up. Build the key prefix once.
    src = source_dir.rstrip(os.sep) or source_dir
    key_prefix = base_prefix
    if folder:
        key_prefix = key_prefix + folder.strip('/') + '/'

    logger.info(
        'wasabi upload START source=%s bucket=%s prefix=%s',
        src, bucket, key_prefix or '(root)',
    )

    if not os.path.isdir(src):
        logger.error('wasabi upload: source does not exist or is not a directory: %s', src)
        return {
            'ok': False,
            'uploaded': 0,
            'failed': 0,
            'bytes': 0,
            'elapsed_ms': int((time.monotonic() - started) * 1000),
            'errors': [f'source not found: {src}'],
        }

    uploaded = 0
    failed = 0
    total_bytes = 0
    errors = []

    for root, _dirs, files in os.walk(src):
        for fname in files:
            # Skip OS junk that shouldn't archive (matches the
            # `[f for f in os.listdir(...) if not f.startswith('.')]`
            # filter the previous version used in the "exists" branch).
            if fname.startswith('.'):
                continue
            local_path = os.path.join(root, fname)
            try:
                size = os.path.getsize(local_path)
            except OSError as e:
                logger.warning('wasabi upload: stat failed %s: %s', local_path, e)
                failed += 1
                if len(errors) < 10:
                    errors.append(f'stat failed {fname}: {e}')
                continue
            rel = os.path.relpath(local_path, src).replace(os.sep, '/')
            key = posixpath.join(key_prefix, rel) if key_prefix else rel
            label = posixpath.join(folder or '(root)', rel)
            logger.info(
                'wasabi upload file=%s size=%d → s3://%s/%s',
                rel, size, bucket, key,
            )
            try:
                client.upload_file(
                    local_path,
                    bucket,
                    key,
                    Callback=_FileProgress(label, size) if size > 0 else None,
                )
                uploaded += 1
                total_bytes += size
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code', 'unknown')
                msg = f'{fname}: ClientError {code}'
                logger.error('wasabi upload FAILED file=%s err=%s', rel, e)
                failed += 1
                if len(errors) < 10:
                    errors.append(msg)
            except (BotoCoreError, NoCredentialsError, ProfileNotFound) as e:
                # NoCredentialsError / ProfileNotFound shouldn't reach
                # here (caught at _make_client), but be defensive in
                # case AWS_ACCESS_KEY env var sets get torn down mid-run.
                logger.error('wasabi upload FAILED file=%s err=%s', rel, e)
                failed += 1
                if len(errors) < 10:
                    errors.append(f'{fname}: {e.__class__.__name__}')
            except OSError as e:
                logger.error('wasabi upload OS error file=%s err=%s', rel, e)
                failed += 1
                if len(errors) < 10:
                    errors.append(f'{fname}: {e}')

    elapsed_ms = int((time.monotonic() - started) * 1000)
    ok = uploaded > 0 and failed == 0
    logger.info(
        'wasabi upload END uploaded=%d failed=%d bytes=%d elapsed_ms=%d ok=%s',
        uploaded, failed, total_bytes, elapsed_ms, ok,
    )
    return {
        'ok': ok,
        'uploaded': uploaded,
        'failed': failed,
        'bytes': total_bytes,
        'elapsed_ms': elapsed_ms,
        'errors': errors,
    }


def health_check():
    """
    Probe Wasabi reachability + bucket auth. Called once at app
    startup so config / cred issues surface in the journald log
    immediately rather than at the first ingest.

    Returns a dict — does NOT raise. The caller decides whether a
    failed probe should block startup; today we choose "log and
    continue" because a transient Wasabi outage shouldn't take down
    the whole curation API (the SFTP + local-fs endpoints are still
    valuable).
    """
    started = time.monotonic()
    out = {'ok': False, 'bucket': None, 'error': None}
    try:
        bucket, _ = _parse_bucket(config.WASABI_BUCKET)
        out['bucket'] = bucket
        client = _make_client()
        client.head_bucket(Bucket=bucket)
        out['ok'] = True
    except ProfileNotFound as e:
        out['error'] = f'WASABI_PROFILE not in ~/.aws/config: {e}'
    except NoCredentialsError as e:
        out['error'] = f'no credentials for profile: {e}'
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', 'unknown')
        out['error'] = f'head_bucket failed ({code}): {e}'
    except BotoCoreError as e:
        out['error'] = f'BotoCoreError: {e}'
    except RuntimeError as e:
        # Config-level failure (missing env). Surface cleanly.
        out['error'] = str(e)
    out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
    return out
