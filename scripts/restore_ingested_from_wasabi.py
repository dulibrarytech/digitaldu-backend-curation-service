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
Restore (download) packages or single files from the Wasabi archive
(WASABI_BUCKET) into the local 003-ingested folder — the download-direction
companion to scripts/sync_missing_to_wasabi.py (upload) and
scripts/reconcile_ingested_wasabi.py (read-only verification).

Purpose: under the 003-ingested retirement plan the local batch copies are
deleted once a batch reports VERIFIED, leaving Wasabi as the only copy.
This script lets developers pull selected content back down using the
service's own venv, boto3 client, and credential resolution — no AWS CLI
required on the host.

Mapping (same as the upload/reconcile scripts):

    remote  s3://<bucket>/<base_prefix><batch>/<rel-path>
    local   <INGESTED_PATH>/<batch>/<rel-path>

Selection is deliberately package- or file-scoped — there is NO whole-batch
mode, because batches can run to terabytes. Every download run names a
--batch plus at least one --package and/or --file selector; use --list to
discover what is in the bucket first:

    # What batches exist in the archive?
    .venv/bin/python scripts/restore_ingested_from_wasabi.py --list
    # What packages does one batch contain (with sizes)?
    .venv/bin/python scripts/restore_ingested_from_wasabi.py --list --batch codu_100
    # Plan a restore (dry-run is the default):
    .venv/bin/python scripts/restore_ingested_from_wasabi.py \
        --batch codu_100 --package pkg_a --file pkg_b/uri.txt
    # Do it:
    .venv/bin/python scripts/restore_ingested_from_wasabi.py \
        --batch codu_100 --package pkg_a --file pkg_b/uri.txt --execute

Safety model:
  * DRY-RUN BY DEFAULT. Without --execute it only prints what it would
    download. Nothing is ever deleted or uploaded in any mode.
  * Downloads ONLY files that are absent locally. Files already present at
    the remote size are skipped (re-running an interrupted restore resumes
    where it left off). Files present at a DIFFERENT size are skipped and
    reported unless --overwrite-mismatch is passed — a local file that
    disagrees with the archive needs human review, not a blind overwrite.
  * No incomplete file is ever left in place: each download goes to a
    `<name>.part` temp file, is size-verified against the remote manifest,
    and only then atomically renamed into place. Failed/oversized parts
    are removed.
  * Free disk space on the destination is checked against the planned
    byte total (plus margin) before any download starts.
  * A selector that matches nothing in the bucket is an error, not a
    silent no-op.

Exit codes: 0 = nothing left to do / all downloads verified;
2 = failures, skipped mismatches, or unmatched selectors; 1 = config or
usage error.
"""

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402
from lib import wasabi  # noqa: E402
from scripts import reconcile_ingested_wasabi as recon  # noqa: E402

logger = logging.getLogger('restore_ingested')

# Refuse to start downloads that would leave less than this much free
# space on the destination filesystem.
FREE_SPACE_MARGIN = 1024 ** 3  # 1 GB

PART_SUFFIX = '.part'


# --- planning (pure; unit-tested) -------------------------------------------


def select_remote(remote, packages, files):
    """
    Filter a remote {rel: size} manifest down to the requested packages
    (prefix match on `<package>/`) and exact file paths. Pure function.

    Returns (selected, unmatched):
        selected   — {rel: size} union of all selector hits
        unmatched  — [(kind, selector)] for selectors that hit nothing
    """
    selected = {}
    unmatched = []
    for pkg in packages or []:
        prefix = pkg.strip('/') + '/'
        hits = {rel: size for rel, size in remote.items()
                if rel.startswith(prefix)}
        if hits:
            selected.update(hits)
        else:
            unmatched.append(('package', pkg))
    for f in files or []:
        rel = f.lstrip('/')
        if rel in remote:
            selected[rel] = remote[rel]
        else:
            unmatched.append(('file', f))
    return selected, unmatched


def plan_downloads(selected, local):
    """
    Decide what a restore would download. Pure function — no I/O.
    The inverse of sync_missing_to_wasabi.plan_uploads: iterates the
    REMOTE selection and compares against local.

    Returns (missing, mismatched, present):
        missing     — [(rel, remote_size)] for files absent locally
        mismatched  — [(rel, remote_size, local_size)] for files present
                      locally at a different size (downloaded only with
                      --overwrite-mismatch)
        present     — count of files already local at the remote size
    """
    missing = []
    mismatched = []
    present = 0
    for rel in sorted(selected):
        size = selected[rel]
        if rel not in local:
            missing.append((rel, size))
        elif local[rel] != size:
            mismatched.append((rel, size, local[rel]))
        else:
            present += 1
    return missing, mismatched, present


def group_by_package(remote):
    """
    Group a remote {rel: size} manifest by top-level path segment for the
    --list --batch view. Files sitting directly under the batch root are
    grouped under '(batch root)'. Pure function.

    Returns {package: (file_count, byte_total)} .
    """
    groups = {}
    for rel, size in remote.items():
        package = rel.split('/', 1)[0] if '/' in rel else '(batch root)'
        count, total = groups.get(package, (0, 0))
        groups[package] = (count + 1, total + size)
    return groups


# --- remote listing ---------------------------------------------------------


def list_remote_batches(client, bucket, base_prefix):
    """Sorted batch prefixes (folder names) directly under base_prefix."""
    names = []
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix,
                                   Delimiter='/'):
        for cp in page.get('CommonPrefixes', []):
            name = cp['Prefix'][len(base_prefix):].rstrip('/')
            if name:
                names.append(name)
    return sorted(names)


# --- download ---------------------------------------------------------------


def download_one(client, bucket, key, dest, expected_size):
    """
    Download one object to `dest` via a `.part` temp file, verifying the
    byte size before the atomic rename into place. Returns None on
    success, an error string on failure. The `.part` file never survives
    a failure.
    """
    part = Path(str(dest) + PART_SUFFIX)
    part.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.download_file(bucket, key, str(part))
        actual = part.stat().st_size
        if actual != expected_size:
            part.unlink(missing_ok=True)
            return f'size {actual} != remote {expected_size} (removed .part)'
        os.replace(part, dest)
        return None
    except Exception as e:  # noqa: BLE001 - per-file isolation; totals tell the story
        part.unlink(missing_ok=True)
        return str(e)


def restore(client, bucket, prefix, batch_path, downloads, execute):
    """
    Download the planned [(rel, size)] list into batch_path. Returns a
    report dict: downloaded, failed, bytes_downloaded.
    """
    report = {'downloaded': 0, 'failed': 0, 'bytes_downloaded': 0}
    for rel, size in downloads:
        key = prefix + rel
        dest = batch_path / rel
        if not execute:
            logger.info('DRY-RUN would download s3://%s/%s (%d bytes) -> %s',
                        bucket, key, size, dest)
            continue
        logger.info('downloading s3://%s/%s (%d bytes) -> %s',
                    bucket, key, size, dest)
        error = download_one(client, bucket, key, dest, size)
        if error:
            logger.error('download FAILED %s: %s', key, error)
            report['failed'] += 1
        else:
            report['downloaded'] += 1
            report['bytes_downloaded'] += size
    return report


# --- CLI --------------------------------------------------------------------


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Restore packages or single files from the Wasabi '
                    'archive into 003-ingested (dry-run by default; '
                    'never deletes; no whole-batch mode).'
    )
    parser.add_argument('--list', action='store_true',
                        help='List remote batches (or, with --batch, that '
                             'batch\'s packages with sizes) and exit.')
    parser.add_argument('--batch', default=None,
                        help='Batch folder name in the archive (required '
                             'for downloads).')
    parser.add_argument('--package', action='append', default=None,
                        help='Restore every file under this package folder '
                             'inside the batch (repeatable).')
    parser.add_argument('--file', action='append', default=None,
                        help='Restore this exact file path relative to the '
                             'batch, e.g. pkg_a/uri.txt (repeatable).')
    parser.add_argument('--execute', action='store_true',
                        help='Actually download. Without this flag, dry-run '
                             'only.')
    parser.add_argument('--overwrite-mismatch', action='store_true',
                        help='Also re-download files whose local size '
                             'differs from the archive (DANGER: replaces '
                             'the local version — review first).')
    parser.add_argument('--ingested-path', default=None,
                        help='Override INGESTED_PATH from the environment.')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    try:
        bucket, base_prefix = wasabi._parse_bucket(config.WASABI_BUCKET)
        client = wasabi._make_client()
    except RuntimeError as e:
        logger.error('Wasabi configuration error: %s', e)
        return 1

    # --list without a batch: enumerate the archive's batch folders.
    if args.list and not args.batch:
        for name in list_remote_batches(client, bucket, base_prefix):
            print(name)
        return 0

    if not args.batch:
        parser.error('--batch is required (use --list to see what exists)')

    prefix = base_prefix + args.batch + '/'
    remote = recon.build_remote_manifest(client, bucket, prefix)
    if not remote:
        logger.error('batch %s has no objects under s3://%s/%s',
                     args.batch, bucket, prefix)
        return 1

    # --list --batch: show the batch's packages so selectors can be built.
    if args.list:
        groups = group_by_package(remote)
        for package in sorted(groups):
            count, total = groups[package]
            print('%-60s %6d files  %s' %
                  (package, count, recon._human_bytes(total)))
        return 0

    if not args.package and not args.file:
        parser.error('name at least one --package or --file to restore '
                     '(no whole-batch mode — batches can be terabytes; '
                     'use --list --batch %s to see packages)' % args.batch)

    selected, unmatched = select_remote(remote, args.package, args.file)
    for kind, sel in unmatched:
        logger.error('%s selector %r matched nothing under s3://%s/%s',
                     kind, sel, bucket, prefix)

    ingested_path = args.ingested_path or config.INGESTED_PATH
    if not ingested_path:
        logger.error('INGESTED_PATH is not configured (env or --ingested-path)')
        return 1
    batch_path = Path(ingested_path) / args.batch

    if batch_path.is_dir():
        local, _symlinks = recon.build_local_manifest(batch_path)
    else:
        local = {}

    missing, mismatched, present = plan_downloads(selected, local)

    downloads = list(missing)
    skipped_mismatch = 0
    if args.overwrite_mismatch:
        downloads.extend((rel, remote_size)
                         for rel, remote_size, _local in mismatched)
    else:
        skipped_mismatch = len(mismatched)
        for rel, remote_size, local_size in mismatched:
            logger.warning(
                'SKIPPING size-mismatched file %s (local=%d remote=%d) — '
                'review manually or pass --overwrite-mismatch',
                rel, local_size, remote_size,
            )

    bytes_planned = sum(size for _rel, size in downloads)
    mode = 'EXECUTE' if args.execute else 'DRY-RUN'
    logger.info('[%s] batch %s: %d file(s) to download (%s), %d already '
                'present, %d mismatch skipped',
                mode, args.batch, len(downloads),
                recon._human_bytes(bytes_planned), present, skipped_mismatch)

    if args.execute and downloads:
        os.makedirs(ingested_path, exist_ok=True)
        free = shutil.disk_usage(ingested_path).free
        if bytes_planned + FREE_SPACE_MARGIN > free:
            logger.error(
                'not enough disk space on %s: need %s (+1 GB margin), '
                'have %s free', ingested_path,
                recon._human_bytes(bytes_planned), recon._human_bytes(free),
            )
            return 1

    report = restore(client, bucket, prefix, batch_path, downloads,
                     args.execute)

    logger.info(
        '%s complete: planned=%d (%s) downloaded=%d (%s) failed=%d '
        'skipped_mismatch=%d unmatched_selectors=%d',
        mode, len(downloads), recon._human_bytes(bytes_planned),
        report['downloaded'], recon._human_bytes(report['bytes_downloaded']),
        report['failed'], skipped_mismatch, len(unmatched),
    )
    if not args.execute:
        logger.info('dry-run only — re-run with --execute to download, then '
                    'run scripts/reconcile_ingested_wasabi.py --batch %s to '
                    'verify', args.batch)

    clean = (report['failed'] == 0 and skipped_mismatch == 0
             and not unmatched)
    return 0 if clean else 2


if __name__ == '__main__':
    sys.exit(main())
