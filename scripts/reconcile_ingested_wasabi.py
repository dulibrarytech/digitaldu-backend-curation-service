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
Reconcile 003-ingested batch folders against their Wasabi S3 copies.

READ-ONLY: this script only lists directories locally and issues
ListObjectsV2 calls against Wasabi. It never uploads, deletes, or
modifies anything on either side. It is the verification gate for the
003-ingested retirement plan (see repo/INGESTED_RETIREMENT_PLAN.md) —
nothing local may be deleted until its batch reports VERIFIED here.

Mapping under test (established by archivematica_ops.move_to_ingested →
wasabi.upload_directory):

    local   <INGESTED_PATH>/<batch>/<rel-path>
    remote  s3://<bucket>/<base_prefix><batch>/<rel-path>

where <batch> is the collection folder name with `new_` already
stripped (that happens before the 003-ingested copy is written), and
bucket/base_prefix come from WASABI_BUCKET exactly as the upload path
parses them. Dot-prefixed files are excluded on BOTH sides because
upload_directory never sends them.

Comparison is existence + byte size per file. Sizes catch truncated or
partial uploads; they do not catch bit-flips (multipart ETags are not
content MD5s, so a cheap checksum compare is not possible for large
files). The optional --checksum flag additionally compares MD5 → ETag
for files at or below the multipart threshold (8 MB), where the ETag
IS the content MD5.

Verdicts per batch:
    VERIFIED     every local file exists remotely at the same size
                 (extra remote objects do not block VERIFIED — they are
                 reported, but more-in-S3 is not a deletion risk)
    MISSING      one or more local files have no remote object
    MISMATCH     one or more sizes (or opted-in checksums) differ
    EMPTY_LOCAL  local batch folder contains no files (nothing to
                 verify; flagged for human review, NOT safe-by-default)
    ERROR        the batch could not be scanned or listed

Usage (run on the curation host with the service .env loaded):

    python scripts/reconcile_ingested_wasabi.py                # all batches
    python scripts/reconcile_ingested_wasabi.py --limit 5      # first 5
    python scripts/reconcile_ingested_wasabi.py --batch codu_100
    python scripts/reconcile_ingested_wasabi.py --checksum
    python scripts/reconcile_ingested_wasabi.py --output-dir /tmp/recon

Outputs:
    <output-dir>/reconcile-summary-<timestamp>.csv    one row per batch
    <output-dir>/reconcile-detail-<timestamp>.jsonl   one JSON object per
                                                      batch incl. capped
                                                      problem lists
Exit codes: 0 = every scanned batch VERIFIED; 2 = at least one batch
not VERIFIED; 1 = configuration/usage error.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Allow running as `python scripts/reconcile_ingested_wasabi.py` from the
# repo root: put the repo root on sys.path so `config` / `lib` import.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402
from lib import wasabi  # noqa: E402

logger = logging.getLogger('reconcile')

# boto3's default multipart threshold — at or below this size a single-part
# upload was used and the S3 ETag is the object's content MD5.
MULTIPART_THRESHOLD = 8 * 1024 * 1024

# Cap per-problem lists in the JSONL detail (full counts always reported).
DETAIL_CAP = 50


# --- manifest builders ------------------------------------------------------


def build_local_manifest(batch_path):
    """
    Walk one local batch folder into {rel_path: size_bytes}.

    Dot-prefixed files are skipped to mirror upload_directory's filter —
    they were never uploaded, so counting them would produce phantom
    MISSING entries. Symlinks are not followed (upload_directory walks
    them via os.walk defaults, but a dangling link would die at stat —
    skip + count separately so the report surfaces them).

    Returns (manifest_dict, skipped_symlinks_count).
    """
    manifest = {}
    symlinks = 0
    batch_path = Path(batch_path)
    for root, _dirs, files in os.walk(batch_path):
        for fname in files:
            if fname.startswith('.'):
                continue
            full = Path(root) / fname
            if full.is_symlink():
                symlinks += 1
                continue
            rel = full.relative_to(batch_path).as_posix()
            manifest[rel] = full.stat().st_size
    return manifest, symlinks


def build_remote_manifest(client, bucket, prefix):
    """
    List every object under `prefix` into {rel_path: size_bytes}.

    `rel_path` is the key with `prefix` stripped, so it is directly
    comparable with the local manifest. Paginated — Wasabi caps each
    ListObjectsV2 page at 1000 keys.
    """
    manifest = {}
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            rel = obj['Key'][len(prefix):]
            if not rel:
                continue
            manifest[rel] = obj['Size']
    return manifest


# --- comparison (pure; unit-tested) ----------------------------------------


def compare_manifests(local, remote):
    """
    Compare {rel: size} manifests. Pure function — no I/O.

    Returns dict:
        missing        — rel paths present locally, absent remotely
        size_mismatch  — [(rel, local_size, remote_size), ...]
        extra          — rel paths present remotely, absent locally
        matched        — count of files present on both sides at equal size
    """
    missing = []
    size_mismatch = []
    matched = 0
    for rel, size in local.items():
        if rel not in remote:
            missing.append(rel)
        elif remote[rel] != size:
            size_mismatch.append((rel, size, remote[rel]))
        else:
            matched += 1
    extra = [rel for rel in remote if rel not in local]
    return {
        'missing': sorted(missing),
        'size_mismatch': sorted(size_mismatch),
        'extra': sorted(extra),
        'matched': matched,
    }


def verdict_for(local_count, comparison):
    """Map a comparison result to the batch verdict string."""
    if local_count == 0:
        return 'EMPTY_LOCAL'
    if comparison['missing']:
        return 'MISSING'
    if comparison['size_mismatch']:
        return 'MISMATCH'
    return 'VERIFIED'


# --- optional checksum pass -------------------------------------------------


def checksum_small_files(client, bucket, prefix, batch_path, local_manifest):
    """
    For files at or below MULTIPART_THRESHOLD, compare local MD5 with the
    remote ETag (single-part uploads: ETag == content MD5). Multipart
    ETags contain a '-'; those are skipped — size comparison is the only
    cheap integrity signal for large files.

    Returns [(rel, local_md5, remote_etag), ...] for mismatches only.
    """
    mismatches = []
    for rel, size in local_manifest.items():
        if size > MULTIPART_THRESHOLD:
            continue
        try:
            head = client.head_object(Bucket=bucket, Key=prefix + rel)
        except Exception as e:  # noqa: BLE001 - missing keys already reported by compare
            logger.debug('checksum head failed %s: %s', rel, e)
            continue
        etag = head.get('ETag', '').strip('"')
        if '-' in etag:
            continue
        md5 = hashlib.md5()
        with open(Path(batch_path) / rel, 'rb') as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b''):
                md5.update(chunk)
        local_md5 = md5.hexdigest()
        if local_md5 != etag:
            mismatches.append((rel, local_md5, etag))
    return mismatches


# --- batch iteration --------------------------------------------------------


def list_local_batches(ingested_path):
    """Sorted non-hidden batch directory names under INGESTED_PATH."""
    root = Path(ingested_path)
    if not root.is_dir():
        raise FileNotFoundError(f'INGESTED_PATH does not exist: {root}')
    return sorted(
        entry.name for entry in root.iterdir()
        if entry.is_dir() and not entry.name.startswith('.')
    )


def reconcile_batch(client, bucket, base_prefix, ingested_path, batch, do_checksum):
    """Reconcile one batch. Returns the per-batch report dict."""
    batch_path = Path(ingested_path) / batch
    prefix = base_prefix + batch + '/'
    report = {
        'batch': batch,
        'prefix': prefix,
        'verdict': 'ERROR',
        'local_files': 0,
        'local_bytes': 0,
        'remote_objects': 0,
        'remote_bytes': 0,
        'matched': 0,
        'missing_count': 0,
        'mismatch_count': 0,
        'extra_count': 0,
        'skipped_symlinks': 0,
        'checksum_mismatch_count': None,
        'missing': [],
        'size_mismatch': [],
        'extra': [],
        'checksum_mismatch': [],
        'error': None,
    }
    try:
        local, symlinks = build_local_manifest(batch_path)
        remote = build_remote_manifest(client, bucket, prefix)
    except Exception as e:  # noqa: BLE001 - per-batch isolation; verdict stays ERROR
        logger.error('batch %s: %s', batch, e)
        report['error'] = str(e)
        return report

    comparison = compare_manifests(local, remote)
    report.update({
        'verdict': verdict_for(len(local), comparison),
        'local_files': len(local),
        'local_bytes': sum(local.values()),
        'remote_objects': len(remote),
        'remote_bytes': sum(remote.values()),
        'matched': comparison['matched'],
        'missing_count': len(comparison['missing']),
        'mismatch_count': len(comparison['size_mismatch']),
        'extra_count': len(comparison['extra']),
        'skipped_symlinks': symlinks,
        'missing': comparison['missing'][:DETAIL_CAP],
        'size_mismatch': comparison['size_mismatch'][:DETAIL_CAP],
        'extra': comparison['extra'][:DETAIL_CAP],
    })

    if do_checksum and report['verdict'] == 'VERIFIED':
        mismatches = checksum_small_files(client, bucket, prefix, batch_path, local)
        report['checksum_mismatch_count'] = len(mismatches)
        report['checksum_mismatch'] = mismatches[:DETAIL_CAP]
        if mismatches:
            report['verdict'] = 'MISMATCH'
            report['mismatch_count'] += len(mismatches)

    return report


# --- output -----------------------------------------------------------------

SUMMARY_FIELDS = [
    'batch', 'verdict', 'local_files', 'local_bytes', 'remote_objects',
    'remote_bytes', 'matched', 'missing_count', 'mismatch_count',
    'extra_count', 'skipped_symlinks', 'checksum_mismatch_count', 'error',
]


def _human_bytes(n):
    for unit in ('B', 'K', 'M', 'G', 'T'):
        if n < 1024 or unit == 'T':
            return '%d%s' % (n, unit) if unit == 'B' else '%.1f%s' % (n, unit)
        n /= 1024.0
    return str(n)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Read-only reconciliation of 003-ingested vs Wasabi.'
    )
    parser.add_argument(
        '--batch', action='append', default=None,
        help='Reconcile only this batch folder name (repeatable). '
             'Default: every folder under INGESTED_PATH.'
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='Stop after N batches (useful for a first sample run).'
    )
    parser.add_argument(
        '--checksum', action='store_true',
        help='Additionally compare MD5→ETag for files <= 8 MB '
             '(slower; reads those files from disk).'
    )
    parser.add_argument(
        '--ingested-path', default=None,
        help='Override INGESTED_PATH from the environment.'
    )
    parser.add_argument(
        '--output-dir', default='.',
        help='Where to write the CSV summary + JSONL detail (default: cwd).'
    )
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

    try:
        batches = args.batch or list_local_batches(ingested_path)
    except FileNotFoundError as e:
        logger.error('%s', e)
        return 1
    if args.limit:
        batches = batches[:args.limit]

    logger.info(
        'reconciling %d batch(es) from %s against s3://%s/%s',
        len(batches), ingested_path, bucket, base_prefix or '(root)',
    )

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f'reconcile-summary-{timestamp}.csv'
    jsonl_path = out_dir / f'reconcile-detail-{timestamp}.jsonl'

    counts = {}
    with open(csv_path, 'w', newline='') as csv_fh, open(jsonl_path, 'w') as jsonl_fh:
        writer = csv.DictWriter(csv_fh, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for i, batch in enumerate(batches, 1):
            report = reconcile_batch(
                client, bucket, base_prefix, ingested_path, batch, args.checksum
            )
            counts[report['verdict']] = counts.get(report['verdict'], 0) + 1
            writer.writerow({k: report[k] for k in SUMMARY_FIELDS})
            csv_fh.flush()
            jsonl_fh.write(json.dumps(report) + '\n')
            jsonl_fh.flush()
            logger.info(
                '[%d/%d] %-12s %s (local %d files / %s; remote %d / %s)',
                i, len(batches), report['verdict'], batch,
                report['local_files'], _human_bytes(report['local_bytes']),
                report['remote_objects'], _human_bytes(report['remote_bytes']),
            )

    logger.info('summary: %s', ', '.join(f'{k}={v}' for k, v in sorted(counts.items())))
    logger.info('wrote %s and %s', csv_path, jsonl_path)

    all_verified = set(counts) <= {'VERIFIED'}
    return 0 if all_verified else 2


if __name__ == '__main__':
    sys.exit(main())
