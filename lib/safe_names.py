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
Path-segment validation for QA route parameters.

Every `folder` / `uuid` / `package` query param the qa routes accept is a
SINGLE directory name that gets joined onto READY_PATH / INGEST_PATH /
INGESTED_PATH and then handed to filesystem operations (and, historically,
to `os.system('cp -R ' + folder ...)`). Two distinct risks follow from
trusting those values:

  1. Path traversal — `folder=../../etc` escaping the base directory.
  2. Command injection — only relevant while a shell was involved.

The command-injection vector is closed at the sink by switching the ops
layer from `os.system(<string>)` to `subprocess.run([...], shell=False)`
with a `--` end-of-options marker (see lib/archivematica_ops.py:_run). With
no shell, metacharacters in a name (`;`, `|`, `$(...)`, spaces, ...) are
inert — they become literal filename bytes. So this validator deliberately
does NOT try to blocklist shell metacharacters (that would reject legal
filenames and give a false sense that the *string* is the boundary). Its
job is the remaining structural risk: keep each value a single, in-base
path segment.

A value is rejected if it is empty/None, longer than 255 chars, exactly
`.`/`..`, begins with `-` (so it can never be read as a CLI flag even if a
future caller drops the `--`), contains a path separator (`/` or `\\`), or
contains a control character (NUL, newline, ...). Everything else — letters,
digits, dots, dashes, underscores, spaces, unicode — is allowed, matching
the range of names staff actually create.

Note: routes/astools.py has its own, looser validator that intentionally
permits `/` for nested workspace paths and pairs it with a resolved-path
`relative_to()` containment check. This module is the strict, flat-segment
variant for the qa routes, whose params are never nested.
"""

_MAX_LEN = 255


def is_safe_segment(value):
    """True if `value` is a single, traversal-free path segment."""
    if not isinstance(value, str):
        return False
    if not value or len(value) > _MAX_LEN:
        return False
    if value in ('.', '..'):
        return False
    if value[0] == '-':                      # never parseable as a CLI option
        return False
    if '/' in value or '\\' in value:        # single segment only — no traversal
        return False
    if any(ord(c) < 0x20 for c in value):    # NUL / newline / other control chars
        return False
    return True


def validate_segment(value, field='value'):
    """Return None if `value` is a safe segment, else an error string.

    Designed for `err = validate_segment(a) or validate_segment(b)` chaining
    at the route boundary.
    """
    if value is None:
        return 'missing required parameter: ' + field
    if not is_safe_segment(value):
        return (
            'invalid ' + field + ': must be a single path segment '
            '(no "/" or "\\", not "." or "..", no leading "-", '
            'no control characters, max 255 chars)'
        )
    return None
