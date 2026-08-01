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
Batch structure QA scan.

Staff assemble ingest batches by hand:

    WORKSPACE/
        new_<collection>-resources_<N>/     <- collection (batch) folder
            <package_a>/                    <- one folder per archival object
                file1.tif
            <package_b>/
                file1.pdf

This module is the single source of truth for structure classification —
files dropped directly into the collection folder, empty package folders,
extra nesting:

  * scan_batch(batch_path)      — one pass over a batch, returns package
                                  names, processed (uri.txt) names, and a
                                  list of structure-error flags.
  * get_workspace_batches(root) — the batch list behind the /workspace
                                  endpoint. Malformed batches are INCLUDED
                                  and flagged, never skipped.

Design constraints (large batches: ~100 packages x hundreds of files each):

  * Exactly one os.scandir() sweep per directory. Every flag is computed
    from that sweep — adding a check must not add a traversal.
  * Directory listings only; no file is ever opened and no per-file stat
    is issued (scandir dirents carry the type bit).
  * items lists are capped at ITEMS_CAP entries; `total` always carries
    the true count so the UI can say "and N more".

Flag codes (severity in parentheses):

  no_packages          (error) batch folder has no package subfolders
  loose_files          (error) files sit directly in the batch folder
  empty_package        (error) package folder has no content files
  nested_dirs          (error) package folder contains subfolders
  bad_folder_name      (error) batch name breaks the naming convention;
                               items are subcodes: missing_new_prefix,
                               missing_resources_id_tail
  unreadable           (error) permission denied scanning the batch
  partially_processed  (info)  some but not all packages have uri.txt;
                               items = packages still lacking uri.txt
  name_hygiene         (warn)  spaces in package or file names

The server reports codes + raw items only. Staff-facing wording lives in
the dashboard views (repo-backend-v2), which own tone and language.

Design history and rationale: repo/notes/CURATION_API_CODE_NOTES.md
"""

import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Cap per-flag item lists so a package with hundreds of offending files
# cannot bloat the JSON response or the rendered table row.
ITEMS_CAP = 20

# Ignored everywhere: OS droppings staff did not create on purpose.
JUNK_FILES = {'Thumbs.db', 'thumbs.db', 'desktop.ini'}

# Batch folder naming convention, matching the parser the ingest service
# applies at submit time (repo-backend-v2 workspace.js _parse_resource_uri):
# the last '-' segment must be resources_<digits> or archival_objects_<digits>.
FOLDER_TAIL_RE = re.compile(r'^(resources|archival_objects)_(\d+)$')

SEVERITY_ERROR = 'error'
SEVERITY_WARN = 'warn'
SEVERITY_INFO = 'info'


def _flag(code, severity, items):
    """Builds one structure-error entry with a capped items list."""
    return {
        'code': code,
        'severity': severity,
        'items': items[:ITEMS_CAP],
        'total': len(items),
    }


def _is_junk(name):
    """True for hidden files/folders and known OS junk files."""
    return name.startswith('.') or name in JUNK_FILES


def check_batch_folder_name(name):
    """
    Validates the batch folder naming convention.

    @param name: Batch folder name
    @return: List of subcode strings (empty when the name is valid)
    """
    problems = []
    if not name.startswith('new_'):
        problems.append('missing_new_prefix')
    tail = name.split('-')[-1]
    if not FOLDER_TAIL_RE.match(tail):
        problems.append('missing_resources_id_tail')
    return problems


def scan_batch(batch_path):
    """
    Scans one batch folder and classifies its structure.

    One scandir sweep of the batch folder plus one sweep per package —
    no file opens, no recursion into nested directories (their existence
    is flagged; their contents are irrelevant to the verdict).

    @param batch_path: Path (or str) of the batch folder
    @return: Dictionary:
        name              — batch folder name
        packages          — sorted package (subfolder) names
        processed         — sorted package names that contain uri.txt
        structure_errors  — list of flag dicts (see module docstring)
        total_bytes       — sum of regular-file sizes across the batch
                            root and every package (from the same
                            scandir entries; stat only, still no file
                            opens). Contents of nested directories are
                            NOT counted — nesting is a structure error
                            and their contents never ingest as-is.
                            None when the batch folder is unreadable.
    """
    batch_path = Path(batch_path)
    name = batch_path.name

    packages = []
    loose_files = []
    structure_errors = []
    total_bytes = 0

    def _entry_size(entry):
        """st_size of a scandir entry; 0 if it vanished mid-scan."""
        try:
            return entry.stat(follow_symlinks=False).st_size
        except OSError:
            return 0

    try:
        with os.scandir(batch_path) as entries:
            for entry in entries:
                if _is_junk(entry.name):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    packages.append(entry.name)
                elif entry.is_file(follow_symlinks=False):
                    loose_files.append(entry.name)
                    total_bytes += _entry_size(entry)
    except PermissionError as e:
        logger.warning('scan_batch: permission denied for %s: %s', batch_path, e)
        return {
            'name': name,
            'packages': [],
            'processed': [],
            'structure_errors': [_flag('unreadable', SEVERITY_ERROR, [])],
            'total_bytes': None,
        }

    packages.sort()
    loose_files.sort()

    bad_name = check_batch_folder_name(name)
    if bad_name:
        structure_errors.append(_flag('bad_folder_name', SEVERITY_ERROR, bad_name))

    if not packages:
        structure_errors.append(_flag('no_packages', SEVERITY_ERROR, []))
    if loose_files:
        structure_errors.append(_flag('loose_files', SEVERITY_ERROR, loose_files))

    processed = []
    empty_packages = []
    nested_dirs = []
    hygiene = []

    for package in packages:
        if ' ' in package:
            hygiene.append(package)

        content_files = 0
        has_uri = False
        subdirs = []
        try:
            with os.scandir(batch_path / package) as entries:
                for entry in entries:
                    if _is_junk(entry.name):
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        subdirs.append(entry.name)
                    elif entry.is_file(follow_symlinks=False):
                        total_bytes += _entry_size(entry)
                        if entry.name == 'uri.txt':
                            has_uri = True
                        else:
                            content_files += 1
                            if ' ' in entry.name:
                                hygiene.append(package + '/' + entry.name)
        except PermissionError as e:
            logger.warning(
                'scan_batch: permission denied for package %s/%s: %s',
                name, package, e,
            )
            nested_dirs.append(package + '/<permission denied>')
            continue

        if has_uri:
            processed.append(package)
        for subdir in sorted(subdirs):
            nested_dirs.append(package + '/' + subdir)
        if content_files == 0 and not subdirs:
            empty_packages.append(package)

    if empty_packages:
        structure_errors.append(_flag('empty_package', SEVERITY_ERROR, empty_packages))
    if nested_dirs:
        structure_errors.append(_flag('nested_dirs', SEVERITY_ERROR, nested_dirs))

    if processed and len(processed) < len(packages):
        unprocessed = [p for p in packages if p not in set(processed)]
        structure_errors.append(
            _flag('partially_processed', SEVERITY_INFO, unprocessed)
        )

    if hygiene:
        structure_errors.append(_flag('name_hygiene', SEVERITY_WARN, hygiene))

    return {
        'name': name,
        'packages': packages,
        'processed': processed,
        'structure_errors': structure_errors,
        'total_bytes': total_bytes,
    }


def has_blocking_errors(scan):
    """True when any flag in a scan_batch result is error severity."""
    return any(
        flag.get('severity') == SEVERITY_ERROR
        for flag in scan.get('structure_errors', [])
    )


def get_workspace_batches(root_dir):
    """
    Lists batches for the Make Digital Objects view, with structure flags.

    Inclusion rules:

      * all packages missing uri.txt      -> included (the normal case)
      * loose files (with or without      -> INCLUDED with error flags
        packages)
      * COMPLETELY EMPTY folder           -> hidden. Staff create the
        collection folder first and fill it afterwards, so an empty one
        is not yet a mistake; it appears, with its structure QA, as soon
        as it holds a package folder or a loose file. Unreadable folders
        stay VISIBLE — a permission problem is not "empty".
      * partially processed               -> included with an info flag
      * fully processed                   -> excluded; those batches
        belong to the ASpace QA / Packaging views, which surface the
        same flags via the /workspace/packages response.

    Skips hidden folders and folders named 'ready' (case-insensitive).

    @param root_dir: WORKSPACE directory path
    @return: List of scan_batch dictionaries, sorted by batch name
    @raises FileNotFoundError / NotADirectoryError / PermissionError:
        when root_dir itself is unusable (route maps these to 500)
    """
    root_path = Path(root_dir).resolve()

    if not root_path.exists():
        raise FileNotFoundError(f'Root directory does not exist: {root_path}')
    if not root_path.is_dir():
        raise NotADirectoryError(f'Root path is not a directory: {root_path}')
    if not os.access(root_path, os.R_OK):
        raise PermissionError(f'Insufficient permissions to read directory: {root_path}')

    batches = []
    for entry in sorted(root_path.iterdir(), key=lambda p: p.name):
        if not entry.is_dir() or entry.name.startswith('.'):
            continue
        if entry.name.lower() == 'ready':
            continue

        scan = scan_batch(entry)
        total = len(scan['packages'])
        processed = len(scan['processed'])

        if total > 0 and processed == total:
            # Fully processed — belongs to the QA / Packaging views.
            continue

        # Completely empty folder — hidden until it has content (see
        # the inclusion rules above). "Content" = at least one package
        # subfolder OR at least one loose file; an unreadable folder is
        # kept visible because we can't know it's empty.
        codes = {flag['code'] for flag in scan['structure_errors']}
        has_content = total > 0 or 'loose_files' in codes
        if not has_content and 'unreadable' not in codes:
            continue

        batches.append(scan)

    logger.info(
        'get_workspace_batches: %d batch(es), %d flagged',
        len(batches),
        sum(1 for b in batches if b['structure_errors']),
    )
    return batches
