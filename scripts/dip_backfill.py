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
Copy the served DIP files from DuraCloud Preserve to Wasabi: the one-off
backfill of plan P2.

Lists dip-store/ in DCP (DCP_DIP_LOCATION) and copies every served file
(thumbnails, METS, JPG and PDF access files; see lib/dip_ops.py) to
WASABI_DIP_BUCKET. It needs only the DCP and Wasabi keys the service
already has: no database and no input file.

Safety model:
  * DRY-RUN BY DEFAULT. Without --execute it only reports what it would
    copy (it still checks Wasabi for what is already there).
  * Never deletes anything.
  * Every copied file is verified: the MD5 of the bytes read from DCP
    against DCP's ETag (part by part for a multipart object), and
    Content-MD5 on the upload.
  * A file Wasabi already holds with the same size and ETag is skipped,
    so an interrupted run just starts again.
  * Ctrl-C stops taking new files, lets the copies in flight finish (a
    copy can't be cut short), records them, and writes the summary with
    "interrupted": true. A second Ctrl-C abandons the copies in flight;
    the re-run skips whatever landed.

Disk: a file over 64 MiB is spooled to the temporary directory while it
is verified, up to --workers x 2.1 GB at once (the largest PDFs). Point
TMPDIR at a disk with that much room.

Usage (on libsftp01, service env loaded):

    set -a; source /etc/curation-api/env; set +a
    # Pilot: one folder, plan first, then copy.
    .venv/bin/python scripts/dip_backfill.py --prefix <dip folder>/
    .venv/bin/python scripts/dip_backfill.py --prefix <dip folder>/ --execute
    # Full run, with a log of every file:
    .venv/bin/python scripts/dip_backfill.py --execute --workers 8 \
        --log /root/dip-backfill/run-<ts>.jsonl

Output: one JSON line per served file ({"key", "outcome", "bytes",
"error", "verified"}; outcome is copied, skipped, would_copy or failed;
verified is md5, parts or size, see lib/dip_ops.copy_file), then a
summary line, to --log or stdout. Progress and the summary also go to
stderr.

Exit codes: 0 = every served file is copied or already there;
2 = some files failed; 3 = nothing was listed (a wrong --prefix, or a
prefix that already includes dip-store/); 130 = interrupted;
1 = config error.
"""

import argparse
import json
import logging
import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import dcp, dip_ops, wasabi  # noqa: E402

logger = logging.getLogger('dip_backfill')

OUTCOMES = ('copied', 'skipped', 'would_copy', 'failed')


def served_items(dcp_client, locs, prefixes):
    """Every served file under the DIP location, or under each prefix."""
    bucket, base = locs[0]
    for p in prefixes or ['']:
        start = base + p.lstrip('/')
        listing = dcp.iter_objects(bucket, start, client=dcp_client)
        yield from dip_ops.served_keys(listing, base)


def run(dcp_client, wasabi_client, locs, prefixes=None, limit=None,
        workers=4, execute=False, out=sys.stdout):
    """
    Copy (or, without execute, plan) every served file. Writes one JSON
    line per file and a summary line to `out`; returns the summary.
    Files are handed to the pool a few at a time, so memory stays flat
    over a 108,000-file run.
    """
    workers = max(1, workers)
    summary = {outcome: 0 for outcome in OUTCOMES}
    summary.update({
        'files': 0, 'bytes_copied': 0, 'execute': execute, 'interrupted': False,
    })

    def record(result):
        out.write(json.dumps(result) + '\n')
        summary['files'] += 1
        summary[result['outcome']] += 1
        if result['outcome'] == 'copied':
            summary['bytes_copied'] += result['bytes'] or 0
        if result['outcome'] == 'failed':
            logger.warning('failed %s: %s', result['key'], result['error'])
        if summary['files'] % 1000 == 0:
            logger.info('%d files: %s', summary['files'], {
                k: summary[k] for k in OUTCOMES
            })

    pending = set()
    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        for n, item in enumerate(served_items(dcp_client, locs, prefixes), 1):
            if limit and n > limit:
                break
            pending.add(pool.submit(
                dip_ops.copy_file, item, dcp_client, wasabi_client, locs, execute,
            ))
            if len(pending) >= workers * 4:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    record(future.result())
        for future in pending:
            record(future.result())
    except KeyboardInterrupt:
        # Take no more files; the copies in flight finish (a thread can't
        # be cut short), the queued ones are dropped for the re-run.
        summary['interrupted'] = True
        logger.warning('interrupted: finishing the copies in flight, dropping the queue')
        pool.shutdown(wait=True, cancel_futures=True)
        for future in pending:
            if future.done() and not future.cancelled():
                record(future.result())
    finally:
        pool.shutdown(wait=True)

    out.write(json.dumps({'summary': summary}) + '\n')
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Copy the served DIP files from DuraCloud Preserve to '
                    'Wasabi (dry-run by default; never deletes).'
    )
    parser.add_argument('--execute', action='store_true',
                        help='Actually copy. Without this flag, dry-run only.')
    parser.add_argument('--prefix', action='append', default=None,
                        help='Only this DIP folder (or any key prefix under '
                             'the DIP location; repeatable). For the pilot.')
    parser.add_argument('--limit', type=int, default=None,
                        help='Stop after N served files.')
    parser.add_argument('--workers', type=int, default=4,
                        help='Files copied at once (default 4).')
    parser.add_argument('--log', default=None,
                        help='Write the JSON lines here instead of stdout.')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        stream=sys.stderr,
    )

    try:
        locs = dip_ops.locations()
        # One client each for the whole run: shared by every worker.
        dcp_client = dcp.make_client()
        wasabi_client = wasabi._make_client()
    except RuntimeError as e:
        logger.error('configuration error: %s', e)
        return 1

    (dcp_bucket, dcp_prefix), (wasabi_bucket, wasabi_prefix) = locs
    mode = 'EXECUTE' if args.execute else 'DRY-RUN'
    logger.info('[%s] s3://%s/%s -> s3://%s/%s (workers=%d%s)', mode,
                dcp_bucket, dcp_prefix, wasabi_bucket, wasabi_prefix,
                args.workers, f', prefixes={args.prefix}' if args.prefix else '')

    out = open(args.log, 'a') if args.log else sys.stdout
    try:
        summary = run(dcp_client, wasabi_client, locs, prefixes=args.prefix,
                      limit=args.limit, workers=args.workers,
                      execute=args.execute, out=out)
    finally:
        if args.log:
            out.close()

    logger.info(
        '%s %s: files=%d copied=%d (%.2f GB) skipped=%d '
        'would_copy=%d failed=%d', mode,
        'interrupted' if summary['interrupted'] else 'complete',
        summary['files'], summary['copied'],
        summary['bytes_copied'] / 1024 ** 3, summary['skipped'],
        summary['would_copy'], summary['failed'],
    )
    if summary['files'] == 0:
        logger.warning('nothing listed under s3://%s/%s%s — check --prefix (it is '
                       'relative to the DIP location, without dip-store/)',
                       dcp_bucket, dcp_prefix,
                       f' for prefixes {args.prefix}' if args.prefix else '')
        return 3
    if not args.execute:
        logger.info('dry-run only — re-run with --execute to copy, then run '
                    'dcp_inventory.py check-served')
    if summary['interrupted']:
        logger.warning('interrupted: re-run to continue; what landed is skipped')
        return 130
    return 0 if summary['failed'] == 0 else 2


if __name__ == '__main__':
    sys.exit(main())
