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
Upload MISSING 003-ingested files to Wasabi (remediation companion to
scripts/reconcile_ingested_wasabi.py).

Closes the gaps that reconciliation reports — batches whose Wasabi copy is
absent or incomplete — using the service's own venv, boto3 client, and
credential resolution. No AWS CLI required on the host.

Safety model:
  * DRY-RUN BY DEFAULT. Without --execute it only prints what it would
    upload. Nothing is ever deleted in any mode (uploads only).
  * Uploads ONLY files that are absent from the bucket. Files that exist
    remotely at a DIFFERENT size are skipped and reported unless
    --overwrite-mismatch is passed — a remote object that differs from
    local needs human review, not a blind overwrite.
  * Every executed upload is immediately verified with head_object; a size
    disagreement counts the file as FAILED.
  * Dot-prefixed files are excluded, matching wasabi.upload_directory.

Batch selection (first match wins):
  --batch NAME [...]        explicit batch folder name(s)
  --from-summary CSV        every batch a reconcile summary CSV marked
                            MISSING (add MISMATCH rows too when
                            --overwrite-mismatch is set)
  (neither)                 every folder under INGESTED_PATH — safe,
                            because already-VERIFIED batches plan zero
                            uploads

Usage (on the curation host, service env loaded):

    set -a; source /etc/curation-api/env; set +a
    # See the plan first:
    .venv/bin/python scripts/sync_missing_to_wasabi.py \
        --from-summary /root/reconcile/reconcile-summary-<ts>.csv
    # Then do it:
    .venv/bin/python scripts/sync_missing_to_wasabi.py \
        --from-summary /root/reconcile/reconcile-summary-<ts>.csv \
        --execute

Exit codes: 0 = nothing left to do / all uploads verified;
2 = some uploads failed or mismatches were skipped; 1 = config error.
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402
from lib import wasabi  # noqa: E402
from scripts import reconcile_ingested_wasabi as recon  # noqa: E402

logger = logging.getLogger('sync_missing')


# --- planning (pure; unit-tested) -------------------------------------------


def plan_uploads(local, remote):
    """
    Decide what a sync would upload. Pure function — no I/O.

    Returns (missing, mismatched):
        missing     — [(rel, local_size)] for files absent remotely
        mismatched  — [(rel, local_size, remote_size)] for files present
                      remotely at a different size (uploaded only with
                      --overwrite-mismatch)
    """
    missing = []
    mismatched = []
    for rel in sorted(local):
        size = local[rel]
        if rel not in remote:
            missing.append((rel, size))
        elif remote[rel] != size:
            mismatched.append((rel, size, remote[rel]))
    return missing, mismatched


def batches_from_summary(csv_path, include_mismatch):
    """
    Batch names from a reconcile summary CSV: every MISSING row, plus
    MISMATCH rows when include_mismatch is set.
    """
    wanted = {'MISSING'} | ({'MISMATCH'} if include_mismatch else set())
    names = []
    with open(csv_path, newline='') as fh:
        for row in csv.DictReader(fh):
            if row.get('verdict') in wanted:
                names.append(row['batch'])
    return names


# --- per-batch sync ---------------------------------------------------------


def sync_batch(client, bucket, base_prefix, ingested_path, batch,
               execute, overwrite_mismatch):
    """
    Sync one batch. Returns a report dict:
        batch, planned, uploaded, verified_failed, skipped_mismatch,
        bytes_planned, bytes_uploaded, error
    """
    batch_path = Path(ingested_path) / batch
    prefix = base_prefix + batch + '/'
    report = {
        'batch': batch,
        'planned': 0,
        'uploaded': 0,
        'verify_failed': 0,
        'skipped_mismatch': 0,
        'bytes_planned': 0,
        'bytes_uploaded': 0,
        'error': None,
    }
    try:
        local, _symlinks = recon.build_local_manifest(batch_path)
        remote = recon.build_remote_manifest(client, bucket, prefix)
    except Exception as e:  # noqa: BLE001 - per-batch isolation
        logger.error('batch %s: %s', batch, e)
        report['error'] = str(e)
        return report

    missing, mismatched = plan_uploads(local, remote)

    uploads = list(missing)
    if overwrite_mismatch:
        uploads.extend((rel, local_size) for rel, local_size, _r in mismatched)
    else:
        report['skipped_mismatch'] = len(mismatched)
        for rel, local_size, remote_size in mismatched:
            logger.warning(
                'batch %s: SKIPPING size-mismatched file %s '
                '(local=%d remote=%d) — review manually or pass '
                '--overwrite-mismatch', batch, rel, local_size, remote_size,
            )

    report['planned'] = len(uploads)
    report['bytes_planned'] = sum(size for _rel, size in uploads)

    for rel, size in uploads:
        key = prefix + rel
        if not execute:
            logger.info('DRY-RUN would upload %s (%d bytes) -> s3://%s/%s',
                        rel, size, bucket, key)
            continue
        local_file = str(batch_path / rel)
        try:
            logger.info('uploading %s (%d bytes) -> s3://%s/%s',
                        rel, size, bucket, key)
            client.upload_file(local_file, bucket, key)
            head = client.head_object(Bucket=bucket, Key=key)
            if head.get('ContentLength') != size:
                logger.error(
                    'VERIFY FAILED %s: uploaded but remote size %s != local %d',
                    key, head.get('ContentLength'), size,
                )
                report['verify_failed'] += 1
                continue
            report['uploaded'] += 1
            report['bytes_uploaded'] += size
        except Exception as e:  # noqa: BLE001 - keep going; totals tell the story
            logger.error('upload FAILED %s: %s', key, e)
            report['verify_failed'] += 1

    return report


# --- CLI --------------------------------------------------------------------


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Upload missing 003-ingested files to Wasabi '
                    '(dry-run by default; never deletes).'
    )
    parser.add_argument('--batch', action='append', default=None,
                        help='Sync only this batch folder name (repeatable).')
    parser.add_argument('--from-summary', default=None,
                        help='Reconcile summary CSV; sync its MISSING batches '
                             '(+ MISMATCH with --overwrite-mismatch).')
    parser.add_argument('--execute', action='store_true',
                        help='Actually upload. Without this flag, dry-run only.')
    parser.add_argument('--overwrite-mismatch', action='store_true',
                        help='Also re-upload files whose remote size differs '
                             'from local (DANGER: overwrites the remote '
                             'version — review first).')
    parser.add_argument('--limit', type=int, default=None,
                        help='Stop after N batches.')
    parser.add_argument('--ingested-path', default=None,
                        help='Override INGESTED_PATH from the environment.')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    ingested_path = args.ingested_path or config.INGESTED_PATH
    if not ingested_path:
        logger.error('INGESTED_PATH is not configured (env or --ingested-path)')
        return 1

    try:
        bucket, base_prefix = wasabi._parse_bucket(config.WASABI_BUCKET)
        client = wasabi._make_client()
    except RuntimeError as e:
        logger.error('Wasabi configuration error: %s', e)
        return 1

    if args.batch:
        batches = args.batch
    elif args.from_summary:
        batches = batches_from_summary(args.from_summary, args.overwrite_mismatch)
    else:
        batches = recon.list_local_batches(ingested_path)
    if args.limit:
        batches = batches[:args.limit]

    mode = 'EXECUTE' if args.execute else 'DRY-RUN'
    logger.info('[%s] syncing %d batch(es) from %s to s3://%s/%s',
                mode, len(batches), ingested_path, bucket,
                base_prefix or '(root)')

    totals = {'planned': 0, 'uploaded': 0, 'verify_failed': 0,
              'skipped_mismatch': 0, 'bytes_planned': 0, 'bytes_uploaded': 0}
    errors = 0
    for i, batch in enumerate(batches, 1):
        report = sync_batch(client, bucket, base_prefix, ingested_path,
                            batch, args.execute, args.overwrite_mismatch)
        if report['error']:
            errors += 1
        for k in totals:
            totals[k] += report[k]
        logger.info('[%d/%d] %s: planned=%d uploaded=%d failed=%d '
                    'skipped_mismatch=%d', i, len(batches), batch,
                    report['planned'], report['uploaded'],
                    report['verify_failed'], report['skipped_mismatch'])

    logger.info(
        '%s complete: planned=%d (%.1f GB) uploaded=%d (%.1f GB) '
        'failed=%d skipped_mismatch=%d batch_errors=%d',
        mode, totals['planned'], totals['bytes_planned'] / 1024 ** 3,
        totals['uploaded'], totals['bytes_uploaded'] / 1024 ** 3,
        totals['verify_failed'], totals['skipped_mismatch'], errors,
    )
    if not args.execute:
        logger.info('dry-run only — re-run with --execute to upload, then '
                    're-run scripts/reconcile_ingested_wasabi.py to confirm '
                    'VERIFIED')

    clean = totals['verify_failed'] == 0 and totals['skipped_mismatch'] == 0 and errors == 0
    return 0 if clean else 2


if __name__ == '__main__':
    sys.exit(main())
