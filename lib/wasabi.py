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


# AWS_PROFILE / AWS_DEFAULT_PROFILE env vars cause boto3.Session() to
# load a named profile from `~/.aws/config` during _setup_loader() —
# EVEN WHEN aws_access_key_id + aws_secret_access_key are passed
# explicitly. If the env-var-named profile isn't in the config file,
# the constructor raises ProfileNotFound before we ever get a chance
# to use the explicit credentials. The .env shape on the curation
# host sets AWS_DEFAULT_PROFILE alongside the actual access keys, so
# this trips for every recovery / cron / interactive invocation
# unless we strip those env vars during Session construction.
_PROFILE_ENV_VARS = ('AWS_PROFILE', 'AWS_DEFAULT_PROFILE')


def _make_client():
    """
    Build a boto3 S3 client against the configured Wasabi endpoint.

    Three credential paths, in priority order:

      1. Explicit env vars — AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
         (with optional AWS_DEFAULT_REGION). This is the v1 deployment
         shape: keys live in `.env`, loaded by systemd's
         EnvironmentFile= directive. Preferred because it does NOT
         depend on `~/.aws/config` being present for whatever user
         the process happens to run as.

      2. Named profile — `WASABI_PROFILE` from `~/.aws/config`. Same
         file the AWS CLI uses. Used only if env vars above are unset.

      3. None — RuntimeError. No silent fallback to instance metadata.

    Raises RuntimeError if no usable credentials are configured at
    all, so the failure mode is loud rather than "uses some random
    creds that happen to be on the host".
    """
    if not config.WASABI_ENDPOINT:
        raise RuntimeError('WASABI_ENDPOINT is not configured')

    if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
        return _client_from_keys()
    if config.WASABI_PROFILE:
        return _client_from_profile()
    raise RuntimeError(
        'No Wasabi credentials configured. Set AWS_ACCESS_KEY_ID + '
        'AWS_SECRET_ACCESS_KEY (preferred) or WASABI_PROFILE in '
        'the service env.'
    )


def _client_from_keys():
    """
    Build an S3 client from explicit access-key + secret. See
    _PROFILE_ENV_VARS comment for why we pop those env vars
    AROUND BOTH `boto3.Session()` AND `session.client()`.

    Earlier iterations only popped during Session() and restored
    before client() — botocore's `create_client` then hit
    `get_config_variable('ca_bundle')`, which goes through the same
    `get_scoped_config()` machinery and triggered the same
    ProfileNotFound. Keep the pop in effect through BOTH calls;
    Python's `try/return/finally` runs `finally` after the return
    value is computed but before control hands back to the caller,
    so by the time upload_file() runs the env vars are restored
    and the constructed client doesn't need them anyway (its
    credentials are baked in at construction time).
    """
    session_kwargs = {
        'aws_access_key_id': config.AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': config.AWS_SECRET_ACCESS_KEY,
    }
    if config.AWS_DEFAULT_REGION:
        session_kwargs['region_name'] = config.AWS_DEFAULT_REGION

    saved = {k: os.environ.pop(k) for k in _PROFILE_ENV_VARS if k in os.environ}
    try:
        session = boto3.Session(**session_kwargs)
        return session.client(
            's3',
            endpoint_url=config.WASABI_ENDPOINT,
            config=_RETRY_CONFIG,
        )
    finally:
        os.environ.update(saved)


def _client_from_profile():
    """Build a Session from a named profile in ~/.aws/config."""
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

    def __init__(self, label, total_bytes, hook=None, hook_interval_s=3.0):
        self._label = label
        self._total = max(total_bytes, 1)
        self._sent = 0
        self._milestones = {25, 50, 75, 100}
        # Optional live-progress hook: called as hook(bytes_sent,
        # total_bytes), throttled to every `hook_interval_s` seconds
        # (plus every milestone). Used by aip_ops to persist a
        # progress file the copy-progress route serves — boto3 fires
        # this callback per chunk, far too often to write through
        # unthrottled. Hook errors are swallowed: progress reporting
        # must never fail an upload.
        self._hook = hook
        self._hook_interval_s = hook_interval_s
        # None = never fired; the first chunk always writes so the
        # progress surface goes live as soon as bytes start moving.
        self._hook_last = None

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
        if self._hook is not None:
            now = time.monotonic()
            if (
                passed
                or self._hook_last is None
                or (now - self._hook_last) >= self._hook_interval_s
            ):
                self._hook_last = now
                try:
                    self._hook(self._sent, self._total)
                except Exception:
                    logger.debug(
                        'progress hook failed for %s', self._label,
                        exc_info=True,
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
            'uploaded': int,        # successful AND verified file count
            'failed': int,          # failed file count
            'verified': int,        # head_object-confirmed uploads
                                    # (== uploaded; kept as an explicit
                                    # signal for callers/logs)
            'bytes': int,           # total bytes uploaded
            'elapsed_ms': int,
            'errors': [str, ...],   # error messages, capped at 10
        }
        `ok` is True iff every file succeeded AND at least one file
        was uploaded. An empty source directory returns ok=False with
        no errors — we don't silently no-op a vanished source.

    2026-07-26 (003-ingested retirement, phase 1): every upload is now
    verified with an immediate head_object size check. A file whose
    remote size disagrees with the local size counts as FAILED even
    though upload_file itself did not raise. Rationale: once the local
    003-ingested copies are retired, this upload is the batch
    snapshot's only custodian — "the SDK call returned" is not a
    strong enough success signal to gate 002-ingest deletion on.
    (The 2026-07-26 reconciliation found one 132 MB file truncated by
    the parallel UNVERIFIED local copy path; verification is what
    separates the surviving path from the retired one.)

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
    verified = 0
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
                # Immediate per-file verification (see docstring). A
                # head that errors or disagrees on size means we do NOT
                # count the file as uploaded — callers gate local
                # cleanup on this result.
                try:
                    head = client.head_object(Bucket=bucket, Key=key)
                    remote_size = head.get('ContentLength')
                except (ClientError, BotoCoreError) as e:
                    logger.error(
                        'wasabi VERIFY head failed file=%s err=%s', rel, e,
                    )
                    failed += 1
                    if len(errors) < 10:
                        errors.append(f'{fname}: uploaded but verify head failed')
                    continue
                if remote_size != size:
                    logger.error(
                        'wasabi VERIFY FAILED file=%s local=%d remote=%s',
                        rel, size, remote_size,
                    )
                    failed += 1
                    if len(errors) < 10:
                        errors.append(
                            f'{fname}: verify failed (local {size} != remote {remote_size})'
                        )
                    continue
                uploaded += 1
                verified += 1
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
        'wasabi upload END uploaded=%d verified=%d failed=%d bytes=%d elapsed_ms=%d ok=%s',
        uploaded, verified, failed, total_bytes, elapsed_ms, ok,
    )
    return {
        'ok': ok,
        'uploaded': uploaded,
        'failed': failed,
        'verified': verified,
        'bytes': total_bytes,
        'elapsed_ms': elapsed_ms,
        'errors': errors,
    }


def _resolve_bucket(bucket_config):
    """
    Resolve which raw WASABI_BUCKET-style value to parse.

    bucket_config is the caller-supplied override (typically
    config.WASABI_AIP_BUCKET for AIP-store operations). When None
    or empty, we fall back to config.WASABI_BUCKET for backward
    compatibility — historic callers (upload_directory,
    health_check) don't pass an override and keep targeting the
    SFTP-staging bucket they always have.

    The hard requirement is "_parse_bucket gets a non-empty
    string"; the choice of WHICH string is the routing decision.
    Centralizing this here keeps the four AIP-touching functions
    from each re-implementing the same fallback rule.
    """
    raw = bucket_config if bucket_config else config.WASABI_BUCKET
    return _parse_bucket(raw)


def upload_fileobj(file_obj, key, expected_bytes=None, bucket_config=None,
                   progress_hook=None):
    """
    Stream-upload a file-like object to Wasabi at <bucket>/<base_prefix><key>.

    Used by aip_ops.copy_aip_to_wasabi to pipe AM Storage Service's
    download response straight into Wasabi without writing the
    intermediate bytes to local disk. boto3's upload_fileobj does
    multipart automatically once the body exceeds the configured
    threshold (8 MB default) so multi-GB AIPs work cleanly.

    Args:
        file_obj:        file-like with .read() (e.g. requests.Response.raw)
        key:             Wasabi object key (no bucket prefix; this fn
                         applies the resolved base_prefix)
        expected_bytes:  optional int — used only for progress logging.
                         If unknown, pass None and the callback skips
                         milestone logs.
        progress_hook:   optional callable(bytes_sent, total_bytes),
                         invoked (throttled — see _FileProgress) as the
                         upload streams. Requires expected_bytes; with
                         an unknown total there is no callback at all,
                         so the hook is never fired.
        bucket_config:   optional override of which WASABI_*BUCKET env
                         value to use. AIP callers pass
                         config.WASABI_AIP_BUCKET so AIPs route to the
                         right bucket; legacy callers (none — kept for
                         symmetry) leave it None to target
                         config.WASABI_BUCKET.

    Returns:
        {'bucket': str, 'key': str, 'bytes': int | None}

        Bytes value is best-effort — for a streamed upload boto3
        doesn't return the final count, so we report expected_bytes
        when provided and None otherwise. Callers that need the
        authoritative size should head_object() after the upload.
    """
    bucket, base_prefix = _resolve_bucket(bucket_config)
    full_key = (base_prefix + key) if base_prefix else key
    client = _make_client()

    callback = (
        _FileProgress(full_key, expected_bytes, hook=progress_hook)
        if expected_bytes else None
    )

    logger.info(
        'wasabi upload_fileobj START key=%s expected_bytes=%s',
        full_key, expected_bytes,
    )
    client.upload_fileobj(file_obj, bucket, full_key, Callback=callback)
    logger.info('wasabi upload_fileobj END key=%s', full_key)
    return {
        'bucket': bucket,
        'key': full_key,
        'bytes': expected_bytes,
    }


def head_object(key, bucket_config=None):
    """
    HEAD a Wasabi object. Returns:
        {'exists': bool, 'bucket': str, 'key': str,
         'content_length': int | None}

    Used by aip_ops as the idempotency probe before a copy — a key
    that already exists at the expected size means we can skip the
    upload entirely.

    Distinguishes "not there" (returns exists=False, no raise) from
    transport / auth errors (raises). 403 Forbidden is treated as
    "exists but not accessible" → exists=True with content_length=None
    so the caller can choose to skip or warn rather than re-upload.

    `bucket_config` mirrors upload_fileobj — AIP callers pass
    config.WASABI_AIP_BUCKET; otherwise we fall back to
    config.WASABI_BUCKET.
    """
    bucket, base_prefix = _resolve_bucket(bucket_config)
    full_key = (base_prefix + key) if base_prefix else key
    client = _make_client()
    try:
        res = client.head_object(Bucket=bucket, Key=full_key)
        return {
            'exists': True,
            'bucket': bucket,
            'key': full_key,
            'content_length': res.get('ContentLength'),
        }
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        # 404 (NoSuchKey) on head_object surfaces with Error.Code='404'
        # in Wasabi/boto3. Treat as a clean "not there".
        if code in ('404', 'NoSuchKey', 'NotFound'):
            return {
                'exists': False,
                'bucket': bucket,
                'key': full_key,
                'content_length': None,
            }
        # 403 — bucket policy issue or temporary auth glitch. We
        # surface the assertion that something IS there (defensive
        # default) but with no size info; the caller can decide
        # whether that's enough to skip the upload.
        if code == '403':
            return {
                'exists': True,
                'bucket': bucket,
                'key': full_key,
                'content_length': None,
            }
        raise


def delete_object(key, bucket_config=None):
    """
    Delete a Wasabi object. Used by aip_ops to clear a partial /
    size-mismatched object before re-uploading. Idempotent — Wasabi
    returns 204 whether or not the key was actually there.

    `bucket_config` mirrors upload_fileobj.
    """
    bucket, base_prefix = _resolve_bucket(bucket_config)
    full_key = (base_prefix + key) if base_prefix else key
    client = _make_client()
    client.delete_object(Bucket=bucket, Key=full_key)
    logger.info('wasabi delete_object key=%s', full_key)


def generate_presigned_url(key, ttl_seconds=900, bucket_config=None):
    """
    Mint a presigned GET URL for a Wasabi key. Used by the dashboard
    download flow — the browser is 302-redirected to the returned URL
    and downloads bytes directly from Wasabi.

    ttl_seconds is clamped at the route layer to [60, 3600]; here we
    just pass it through to boto3 so a misconfigured caller from a
    different surface still works.

    `bucket_config` mirrors upload_fileobj — AIP callers pass
    config.WASABI_AIP_BUCKET so the presigned URL points at the
    same bucket Stage 6 uploaded to.

    Returns the URL string. Raises on any cred / config failure so
    the caller (the route) can return ok=false with the message.
    """
    bucket, base_prefix = _resolve_bucket(bucket_config)
    full_key = (base_prefix + key) if base_prefix else key
    client = _make_client()
    return client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': full_key},
        ExpiresIn=ttl_seconds,
    )


def list_prefixes(prefix, continuation_token=None, bucket_config=None,
                  max_keys=1000):
    """
    One page of "subfolder" names under `prefix` (Delimiter='/').

    Read-only. Used by the archive-browser routes (routes/archive.py)
    to walk the batch archive's <collection>/<package>/<files> layout
    one level at a time without ever listing the full 88k-object
    bucket.

    Args:
        prefix              key prefix to list under ('' for the bucket
                            top level; otherwise MUST end with '/')
        continuation_token  opaque S3 token from a previous page, or None
        bucket_config       WASABI_*BUCKET override (None → WASABI_BUCKET)
        max_keys            page size (S3 caps at 1000)

    Returns:
        {'prefixes': [<name>, ...],   # child names, no parent prefix,
                                      # no trailing slash, S3 sort order
         'next_token': str | None}
    """
    bucket, base_prefix = _resolve_bucket(bucket_config)
    full_prefix = base_prefix + prefix
    client = _make_client()

    kwargs = {
        'Bucket': bucket,
        'Prefix': full_prefix,
        'Delimiter': '/',
        'MaxKeys': max_keys,
    }
    if continuation_token:
        kwargs['ContinuationToken'] = continuation_token
    res = client.list_objects_v2(**kwargs)

    prefixes = []
    for entry in res.get('CommonPrefixes', []):
        name = entry.get('Prefix', '')[len(full_prefix):].rstrip('/')
        if name:
            prefixes.append(name)
    return {
        'prefixes': prefixes,
        'next_token': res.get('NextContinuationToken') if res.get('IsTruncated') else None,
    }


def search_prefixes(parent, q, continuation_token=None, bucket_config=None,
                    max_keys=1000):
    """
    One page of child "subfolder" names under `parent` whose names
    START WITH `q` — a server-side S3 prefix search (2026-07-30).

    Built for the archive browser's package level: a migrated
    collection can hold thousands of package folders, and the
    client-side "filter loaded names" box could not see past the
    loaded page — a freshly archived package looked missing when it
    was merely pages deep. S3 prefix listing makes typing the start
    of a package id an exact, bucket-side search.

    Args:
        parent   level prefix, '' or ending with '/'
                 (e.g. 'B002_..._496/')
        q        partial child name to match from the start
    Returns:
        same shape as list_prefixes: {'prefixes': [name, ...],
        'next_token': str | None} — names are full child names
        (q re-included), no trailing slash.
    """
    bucket, base_prefix = _resolve_bucket(bucket_config)
    full_parent = base_prefix + parent
    client = _make_client()

    kwargs = {
        'Bucket': bucket,
        'Prefix': full_parent + q,
        'Delimiter': '/',
        'MaxKeys': max_keys,
    }
    if continuation_token:
        kwargs['ContinuationToken'] = continuation_token
    res = client.list_objects_v2(**kwargs)

    names = []
    for cp in res.get('CommonPrefixes', []):
        name = cp.get('Prefix', '')[len(full_parent):].rstrip('/')
        if name:
            names.append(name)
    return {
        'prefixes': names,
        'next_token': res.get('NextContinuationToken')
        if res.get('IsTruncated') else None,
    }


def list_all_prefixes(prefix, bucket_config=None):
    """
    Every "subfolder" name under `prefix`, following pagination to the
    end. Safe ONLY for levels known to be small (the archive browser
    uses it for the ~hundred top-level collections); deeper levels go
    through the paged list_prefixes so the UI can lazy-load.
    """
    names = []
    token = None
    while True:
        page = list_prefixes(prefix, continuation_token=token,
                             bucket_config=bucket_config)
        names.extend(page['prefixes'])
        token = page['next_token']
        if not token:
            break
    return names


def list_objects(prefix, continuation_token=None, bucket_config=None,
                 max_keys=1000, recursive=False):
    """
    One page of OBJECTS directly under `prefix` (Delimiter='/'), with
    size + last-modified. Companion to list_prefixes for the archive
    browser's file level; a level can contain both subfolders and
    files, so callers wanting a complete picture use both.

    With recursive=True the Delimiter is dropped, so the page walks
    EVERY object under the prefix regardless of nesting — used by the
    AIP-store size listing, where the caller wants the whole flat
    inventory rather than one browse level.

    Returns:
        {'objects': [{'name': <basename>, 'key': <full key minus
                      base_prefix>, 'size': int,
                      'last_modified': ISO-8601 str | None}, ...],
         'next_token': str | None}
    """
    bucket, base_prefix = _resolve_bucket(bucket_config)
    full_prefix = base_prefix + prefix
    client = _make_client()

    kwargs = {
        'Bucket': bucket,
        'Prefix': full_prefix,
        'MaxKeys': max_keys,
    }
    if not recursive:
        kwargs['Delimiter'] = '/'
    if continuation_token:
        kwargs['ContinuationToken'] = continuation_token
    res = client.list_objects_v2(**kwargs)

    objects = []
    for obj in res.get('Contents', []):
        key = obj.get('Key', '')
        name = key[len(full_prefix):]
        # Skip the zero-byte "directory marker" some S3 tools create at
        # the prefix itself.
        if not name:
            continue
        last_modified = obj.get('LastModified')
        objects.append({
            'name': name,
            'key': key[len(base_prefix):],
            'size': obj.get('Size', 0),
            'last_modified': last_modified.isoformat() if last_modified else None,
        })
    return {
        'objects': objects,
        'next_token': res.get('NextContinuationToken') if res.get('IsTruncated') else None,
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
