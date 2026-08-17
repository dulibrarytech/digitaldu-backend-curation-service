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
Multipart-upload hygiene for the Wasabi buckets.

A failed or interrupted multipart upload — a reaped gunicorn worker, a
timed-out Stage 6 copy-to-wasabi, a killed sync script — leaves its
already-uploaded parts behind in the bucket. Those parts are invisible
to normal object listings (the object never "completed") but they are
stored, and billed, until explicitly aborted. This script keeps that
debris from accumulating, two ways:

  --set-lifecycle
      Install an AbortIncompleteMultipartUpload lifecycle rule on the
      target bucket(s) so the storage side cleans up automatically.
      Preferred: one-time, no cron needed. Existing lifecycle rules are
      preserved (the configuration is fetched and the abort rule is
      APPENDED — put_bucket_lifecycle_configuration replaces the whole
      config, so a blind put would wipe unrelated rules). If the bucket
      already carries an enabled abort rule this is a no-op. Wasabi
      support for lifecycle configuration has varied by release; a
      rejection is reported cleanly — fall back to the cron mode below.

  default (report) / --apply (abort)
      List — or with --apply, abort — incomplete multipart uploads
      whose initiation is older than --days. Works against any
      S3-compatible endpoint regardless of lifecycle support, so it is
      the fallback when --set-lifecycle is rejected. Cron shape:

        17 3 * * * cd /path/digitaldu-backend-curation-service \
            && .venv/bin/python scripts/abort_stale_multipart_uploads.py \
               --target both --apply >> /var/log/curation-api/multipart-hygiene.log 2>&1

Safety:
  - abort_multipart_upload discards parts of UNFINISHED uploads only —
    a completed object cannot be touched through this API.
  - The --days guard (default 3) keeps a slow-but-alive transfer well
    out of range: the longest single-transfer budget anywhere in the
    pipeline is 12 h (gunicorn --timeout / AIP_STORE_COPY_TIMEOUT_MS).
  - Default mode is report-only; nothing is aborted without --apply.

Targets (--target):
  aip     WASABI_AIP_BUCKET  — the AIP store Stage 6 writes
  batch   WASABI_BUCKET      — the SFTP-staging batch archive
  both    both of the above (buckets are deduplicated if they share a
          name; an unset env var is reported and skipped)

Exit codes:
  0  success (including "nothing stale found" and lifecycle no-op)
  1  configuration / usage error (no resolvable target bucket)
  2  partial failure: lifecycle rejected by the endpoint, or one or
     more aborts errored — details on stdout
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running as `python scripts/abort_stale_multipart_uploads.py`
# from the repo root: put the repo root on sys.path so `config` / `lib`
# import.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402
from lib import wasabi  # noqa: E402

from botocore.exceptions import ClientError  # noqa: E402

logger = logging.getLogger('multipart_hygiene')

LIFECYCLE_RULE_ID = 'abort-incomplete-multipart-uploads'

# ClientError codes that mean "this endpoint does not accept lifecycle
# configuration" (observed shapes across S3-compatible stores) — as
# opposed to a transient/auth failure worth retrying.
_LIFECYCLE_UNSUPPORTED_CODES = {
    'NotImplemented',
    'MalformedXML',
    'InvalidRequest',
    'MethodNotAllowed',
    'UnsupportedOperation',
}


def resolve_targets(target):
    """
    Map a --target choice onto [(label, bucket, base_prefix)].

    Unset env values are skipped with a notice (the curation host has
    historically carried WASABI_BUCKET but not always WASABI_AIP_BUCKET
    — see .env.example drift). Duplicate bucket NAMES are collapsed so
    `--target both` on a host where both env vars point at the same
    bucket doesn't double-process it: multipart uploads are listed per
    bucket, not per prefix.
    """
    raw_by_label = {
        'aip': config.WASABI_AIP_BUCKET,
        'batch': config.WASABI_BUCKET,
    }
    wanted = ['aip', 'batch'] if target == 'both' else [target]
    out = []
    seen_buckets = set()
    for label in wanted:
        raw = raw_by_label.get(label)
        if not raw:
            print(f'[{label}] skipped — its WASABI_* env var is not set')
            continue
        bucket, base_prefix = wasabi._parse_bucket(raw)
        if bucket in seen_buckets:
            print(f'[{label}] skipped — same bucket ({bucket}) already targeted')
            continue
        seen_buckets.add(bucket)
        out.append((label, bucket, base_prefix))
    return out


def list_incomplete_uploads(client, bucket):
    """
    Every in-progress multipart upload in `bucket`, as boto3 dicts
    (Key, UploadId, Initiated, ...). Paginated — endpoints cap each
    ListMultipartUploads page at 1000 entries.

    Listed bucket-wide (no prefix filter) on purpose: abandoned parts
    are junk wherever they sit, and a prefix filter would hide debris
    from older layouts.
    """
    uploads = []
    paginator = client.get_paginator('list_multipart_uploads')
    for page in paginator.paginate(Bucket=bucket):
        uploads.extend(page.get('Uploads', []))
    return uploads


def split_stale(uploads, cutoff):
    """
    Partition uploads into (stale, fresh) by their Initiated timestamp
    vs `cutoff` (tz-aware datetime). An upload without an Initiated
    value (defensive; not observed in practice) counts as FRESH — never
    abort what can't be dated.
    """
    stale, fresh = [], []
    for u in uploads:
        initiated = u.get('Initiated')
        if initiated is not None and initiated < cutoff:
            stale.append(u)
        else:
            fresh.append(u)
    return stale, fresh


def abort_uploads(client, bucket, uploads):
    """
    Abort each upload; returns (aborted_count, failed_count). A
    NoSuchUpload error counts as success — someone (or a lifecycle
    rule) got there first, which is the outcome we wanted anyway.
    """
    aborted = failed = 0
    for u in uploads:
        key, upload_id = u.get('Key'), u.get('UploadId')
        try:
            client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id
            )
            aborted += 1
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code == 'NoSuchUpload':
                aborted += 1
                continue
            failed += 1
            print(f'    ABORT FAILED key={key} upload_id={upload_id}: {e}')
    return aborted, failed


def ensure_lifecycle(client, bucket, days):
    """
    Ensure `bucket` carries an enabled AbortIncompleteMultipartUpload
    lifecycle rule.

    Returns one of:
      'installed'        rule appended (existing rules preserved)
      'already_present'  an enabled abort rule exists — no write issued
      'unsupported: …'   the endpoint rejected the configuration; use
                         the --apply cron mode instead
    """
    try:
        existing = client.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = existing.get('Rules', [])
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code != 'NoSuchLifecycleConfiguration':
            return f'unsupported: lifecycle read rejected ({code})'
        rules = []

    for rule in rules:
        if (
            rule.get('AbortIncompleteMultipartUpload')
            and rule.get('Status') == 'Enabled'
        ):
            return 'already_present'

    rules.append({
        'ID': LIFECYCLE_RULE_ID,
        'Status': 'Enabled',
        # Empty Filter = whole bucket. Incomplete-upload debris is junk
        # under every prefix, so no scoping.
        'Filter': {},
        'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': days},
    })
    try:
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration={'Rules': rules}
        )
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        if code in _LIFECYCLE_UNSUPPORTED_CODES:
            return f'unsupported: lifecycle write rejected ({code})'
        raise
    return 'installed'


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='List/abort stale incomplete multipart uploads, or '
        'install a lifecycle rule that does it automatically.'
    )
    parser.add_argument(
        '--target', choices=['aip', 'batch', 'both'], default='aip',
        help='which bucket(s) to process (default: aip)',
    )
    parser.add_argument(
        '--days', type=int, default=3,
        help='age threshold in days; uploads initiated more recently '
        'are left alone (default: 3)',
    )
    parser.add_argument(
        '--apply', action='store_true',
        help='actually abort stale uploads (default: report only)',
    )
    parser.add_argument(
        '--set-lifecycle', action='store_true',
        help='install an AbortIncompleteMultipartUpload lifecycle rule '
        'on the target bucket(s) instead of listing/aborting',
    )
    args = parser.parse_args(argv)
    if args.days < 1:
        parser.error('--days must be >= 1')

    targets = resolve_targets(args.target)
    if not targets:
        print('No target bucket resolvable — check WASABI_BUCKET / '
              'WASABI_AIP_BUCKET in the service .env')
        return 1

    client = wasabi._make_client()
    exit_code = 0

    if args.set_lifecycle:
        for label, bucket, _prefix in targets:
            outcome = ensure_lifecycle(client, bucket, args.days)
            print(f'[{label}] bucket={bucket} lifecycle: {outcome}')
            if outcome.startswith('unsupported'):
                print(f'[{label}] fall back to cron: --target {label} --apply')
                exit_code = 2
        return exit_code

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    for label, bucket, _prefix in targets:
        uploads = list_incomplete_uploads(client, bucket)
        stale, fresh = split_stale(uploads, cutoff)
        print(
            f'[{label}] bucket={bucket} incomplete uploads: '
            f'{len(uploads)} total, {len(stale)} older than {args.days}d, '
            f'{len(fresh)} in-window (left alone)'
        )
        for u in stale:
            initiated = u.get('Initiated')
            print(f'    stale key={u.get("Key")} initiated={initiated}')
        if not args.apply or not stale:
            continue
        aborted, failed = abort_uploads(client, bucket, stale)
        print(f'[{label}] aborted {aborted}, failed {failed}')
        if failed:
            exit_code = 2
    if not args.apply:
        print('(report only — re-run with --apply to abort the stale uploads)')
    return exit_code


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
