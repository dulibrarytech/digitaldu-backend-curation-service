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
Archivematica staging operations.

Renamed from `digitaldu-backend-qa/qa_lib.py`. SFTP backend changed from the
abandoned pysftp library to paramiko (well-maintained). Logging changed from
logger.info() to a module logger.

2026-05-23 cancel-flow safety patch (curration-api-modified):
  - Per-uuid lock file added (`_lock_uuid` / `_unlock_uuid` / `_is_locked`)
    so concurrent move operations on the same uuid can't corrupt state.
  - `move_to_ingest` and `move_from_ingest_to_ready` now take/release the
    lock around their disk work. Lock contention returns
    result='move_in_progress' with no disk side effects.
  - `move_from_ingest_to_ready` now best-effort calls clean_up_sftp BEFORE
    the local move (closes the orphaned-SFTP-copy gap for post-cancel
    rollback when Stage 2's move_to_sftp had already run). SFTP failure
    is non-fatal — outcome recorded in `sftp_clean` field of the return.
  - `move_from_ingest_to_ready` now accepts an optional `actor` kwarg
    (the authenticated staff identifier, threaded in from the route)
    and logs it at INFO for cross-reference with the ingest service's
    tbl_ingest_events audit trail.
"""

import logging
import os
import posixpath
import shutil
import stat
import subprocess
import threading

import paramiko

import config
from lib import wasabi

logger = logging.getLogger(__name__)

# Re-export config values under their legacy names so existing function bodies
# continue to work without per-line edits.
ready_path = config.READY_PATH
ingest_path = config.INGEST_PATH
# NOTE: config.INGESTED_PATH still exists for the retirement tooling
# (scripts/reconcile_ingested_wasabi.py, scripts/sync_missing_to_wasabi.py)
# but the ops layer no longer reads it — the 003-ingested local archive
# copy was retired 2026-07-26 (see move_to_ingested's docstring).
sftp_host = config.SFTP_HOST
sftp_username = config.SFTP_ID
sftp_password = config.SFTP_PWD
sftp_path = config.SFTP_REMOTE_PATH
wasabi_endpoint = config.WASABI_ENDPOINT
wasabi_bucket = config.WASABI_BUCKET
wasabi_profile = config.WASABI_PROFILE
uid = config.UID
gid = config.GID
errors_file = config.ERRORS_FILE


# --- subprocess helper -------------------------------------------------------

def _run(argv):
    """Run an external command with NO shell.

    Replaces the prior `os.system('cp -R ' + folder + ...)` form. Passing an
    explicit argv list with shell=False means folder/uuid values reach
    cp/rm/chown as single literal arguments — shell metacharacters in a
    folder name (`;`, `|`, `$(...)`, spaces, ...) can no longer break out
    into command execution. Callers pass a `--` end-of-options marker before
    the operands so a value beginning with `-` can't be read as a flag.

    The exit status is logged but not raised on, matching the legacy
    os.system behavior (which discarded the status — callers that care, like
    move_to_ingested's S3 gate, track success through other means). Returns
    the CompletedProcess so a caller can inspect returncode if needed.
    """
    logger.info('exec: %s', ' '.join(argv))
    result = subprocess.run(argv, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        logger.warning(
            'command exited non-zero (rc=%s): %s | stderr=%s',
            result.returncode, ' '.join(argv), (result.stderr or '').strip(),
        )
    return result


# --- paramiko helpers (replacing pysftp's put_r / walktree / cd / execute) --

def _open_sftp():
    """Open a paramiko SSH+SFTP session. Caller must close both."""
    client = paramiko.SSHClient()
    # Match legacy pysftp behavior: cnopts.hostkeys = None — i.e. accept any host key.
    # NOTE: this is insecure against MITM. Prior service ran with the same posture.
    # Migrate to known_hosts verification in a follow-up ticket.
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=sftp_host,
        username=sftp_username,
        password=sftp_password,
        look_for_keys=False,
        allow_agent=False,
    )
    sftp = client.open_sftp()
    return client, sftp


def _sftp_mkdir_p(sftp, remote_dir):
    """Recursive mkdir over SFTP (paramiko has no built-in equivalent)."""
    if remote_dir in ('', '/'):
        return
    try:
        sftp.stat(remote_dir)
    except IOError:
        parent = posixpath.dirname(remote_dir)
        if parent and parent != remote_dir:
            _sftp_mkdir_p(sftp, parent)
        sftp.mkdir(remote_dir)


def _sftp_put_r(sftp, local_dir, remote_dir, preserve_mtime=True):
    """Recursive directory upload (replaces pysftp.Connection.put_r)."""
    _sftp_mkdir_p(sftp, remote_dir)
    for root, dirs, files in os.walk(local_dir):
        rel = os.path.relpath(root, local_dir)
        rel_remote = remote_dir if rel == '.' else posixpath.join(remote_dir, rel.replace(os.sep, '/'))
        _sftp_mkdir_p(sftp, rel_remote)
        for fname in files:
            local_file = os.path.join(root, fname)
            remote_file = posixpath.join(rel_remote, fname)
            sftp.put(local_file, remote_file)
            if preserve_mtime:
                st = os.stat(local_file)
                sftp.utime(remote_file, (st.st_atime, st.st_mtime))


def _sftp_walk(sftp, remote_dir, on_file=None, on_dir=None, on_other=None):
    """Recursive walk over a remote directory (replaces pysftp.walktree)."""
    for entry in sftp.listdir_attr(remote_dir):
        full_path = posixpath.join(remote_dir, entry.filename)
        if stat.S_ISDIR(entry.st_mode):
            if on_dir:
                on_dir(full_path)
            _sftp_walk(sftp, full_path, on_file, on_dir, on_other)
        elif stat.S_ISREG(entry.st_mode):
            if on_file:
                on_file(full_path)
        else:
            if on_other:
                on_other(full_path)


def _ssh_exec(client, command):
    """Run a shell command over SSH and return decoded stdout (replaces pysftp.execute).

    DEPRECATED (2026-05-23): the production Archivematica SFTP host
    runs OpenSSH `internal-sftp` Subsystem — a restricted SFTP-only
    sandbox with NO shell. paramiko's exec_command opens the channel,
    the server immediately closes it, and stdout.read() returns b''
    with NO exception raised. The result: callers think the command
    succeeded; nothing actually ran.

    Two callers used to depend on this: clean_up_sftp (rm -rf) and
    check_sftp (du -h -s). Both have been rewritten to use pure SFTP
    operations (sftp.remove, sftp.rmdir, sftp.stat). Helper retained
    only for one-off diagnostic scripts that target a shell-enabled
    host. Do NOT add new callers — see _sftp_rmtree below for the
    correct pattern.
    """
    stdin, stdout, stderr = client.exec_command(command)
    return stdout.read().decode('utf-8', errors='replace')


def _sftp_rmtree(sftp, remote_dir):
    """Pure-SFTP recursive remove. Analogue to shutil.rmtree.

    Why pure SFTP and not exec_command('rm -rf'): the production AM
    SFTP host runs `internal-sftp` (no shell). See _ssh_exec docstring.
    sftp.remove (SSH_FXP_REMOVE) and sftp.rmdir (SSH_FXP_RMDIR) are
    part of the SFTP protocol itself and work against internal-sftp.

    Algorithm:
      1. Walk the tree once, recording every file + directory path
         under `remote_dir` (uses the existing _sftp_walk helper).
      2. Remove all files first (rmdir refuses non-empty dirs).
      3. Remove all directories deepest-first (sorted by path depth)
         so children are gone before their parents.
      4. Finally remove `remote_dir` itself.

    Every individual remove is best-effort — an IOError on one entry
    (permission / vanished / etc.) is logged at INFO and the walk
    continues. The caller's contract is "leave the tree maximally
    cleaned"; a single stuck file doesn't block the rest. The final
    sftp.rmdir of `remote_dir` is also best-effort — if a leftover
    entry remains, the parent stays and ops gets a chance to inspect.

    Non-recursive entry types (symlinks, devices, FIFOs) get the
    `on_other` branch from _sftp_walk and are treated as files: we
    try sftp.remove on them. Pure SFTP doesn't traverse symlink
    targets, which is safer than `rm -rf`'s default follow-symlinks
    behavior on broken trees.

    No return value — best-effort throughout. Caller can sftp.stat
    after to confirm the tree is gone if they care.
    """
    # 1. Walk + collect.
    files = []
    dirs = []
    try:
        _sftp_walk(
            sftp,
            remote_dir,
            on_file=files.append,
            on_dir=dirs.append,
            on_other=files.append,  # symlinks / devices: try sftp.remove
        )
    except IOError as e:
        # The top-level directory doesn't exist, or perms denied the
        # walk. Either way nothing to remove. Surface as INFO so ops
        # can see why we bailed.
        logger.info('_sftp_rmtree: walk failed for %s: %s', remote_dir, e)
        return

    # 2. Files first.
    for f in files:
        try:
            sftp.remove(f)
        except IOError as e:
            logger.info('_sftp_rmtree: remove(%s) failed: %s', f, e)

    # 3. Directories deepest-first. Counting `/` is a cheap proxy
    #    for path depth in the walked tree (all paths share the
    #    `remote_dir` prefix, so deeper paths have more slashes).
    dirs.sort(key=lambda d: d.count('/'), reverse=True)
    for d in dirs:
        try:
            sftp.rmdir(d)
        except IOError as e:
            logger.info('_sftp_rmtree: rmdir(%s) failed: %s', d, e)

    # 4. The top-level dir itself.
    try:
        sftp.rmdir(remote_dir)
    except IOError as e:
        logger.info('_sftp_rmtree: rmdir(%s) [root] failed: %s', remote_dir, e)


def _sftp_dir_size(sftp, remote_dir):
    """Pure-SFTP recursive byte-sum analogue to `du -s`.

    Walks the tree once via _sftp_walk and sums each regular file's
    st_size (already returned by listdir_attr — no extra round trip).
    Returns the sum in bytes. Returns 0 on any walk failure (caller
    treats as "size unknown"; the field isn't surfaced to staff
    today, just logged).
    """
    total = [0]

    def add_size(_path, entry):
        # entry is the paramiko SFTPAttributes from listdir_attr.
        # st_size is None for some non-regular entries; treat as 0.
        if entry.st_size:
            total[0] += entry.st_size

    try:
        _sftp_walk_with_attrs(sftp, remote_dir, on_file=add_size)
    except IOError as e:
        logger.info('_sftp_dir_size: walk failed for %s: %s', remote_dir, e)
        return 0
    return total[0]


def _sftp_walk_with_attrs(sftp, remote_dir, on_file=None, on_dir=None):
    """Variant of _sftp_walk that passes the SFTPAttributes object to
    the callbacks alongside the full path. Used by _sftp_dir_size so
    we can sum sizes without an extra round trip per file.

    Kept separate from _sftp_walk so the original signature (callback
    takes only the path) stays stable for existing callers.
    """
    for entry in sftp.listdir_attr(remote_dir):
        full_path = posixpath.join(remote_dir, entry.filename)
        if stat.S_ISDIR(entry.st_mode):
            if on_dir:
                on_dir(full_path, entry)
            _sftp_walk_with_attrs(sftp, full_path, on_file, on_dir)
        elif stat.S_ISREG(entry.st_mode):
            if on_file:
                on_file(full_path, entry)
        # Skip non-file non-dir entries (symlinks, devices) — not
        # counted in `du`-equivalent size either.


# --- per-uuid lockfile (added 2026-05-23 for cancel-flow safety) -----------

# Module-level threading lock guarding access to the disk-side lockfile
# create/remove below. The lockfile itself is the durable cross-process
# guard; this threading.Lock is just defensive against an in-process race
# between the stat() and the open() inside _lock_uuid.
_lock_table_lock = threading.Lock()


def _lockfile_path(uuid):
    """Where the per-uuid .lock file lives on disk.

    Lives inside 002-ingest/<uuid>/.lock so it travels with the directory:
    if the directory is removed (e.g. last package moved out by
    move_from_ingest_to_ready's cleanup step), the lock evaporates with it.
    """
    return os.path.join(ingest_path + uuid, '.lock')


def _lock_uuid(uuid, owner):
    """Acquire the per-uuid lock.

    Returns True if the lock was acquired (caller MUST release in finally).
    Returns False if another operation already holds it — caller should
    surface 'move_in_progress' to the client.

    The lockfile contains the owner name (e.g. 'move_to_ingest') so logs
    can tell you what stole the lock if a release was missed.
    """
    with _lock_table_lock:
        # The 002-ingest/<uuid>/ directory may not exist yet (move_to_ingest
        # creates it). Create it before the lockfile.
        try:
            os.makedirs(ingest_path + uuid, mode=0o777, exist_ok=True)
        except OSError as e:
            logger.warning('_lock_uuid: makedirs failed uuid=%s err=%s', uuid, e)
            return False

        path = _lockfile_path(uuid)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    held_by = f.read().strip() or '<unknown>'
            except OSError:
                held_by = '<unreadable>'
            logger.info(
                '_lock_uuid: BUSY uuid=%s requested_by=%s held_by=%s',
                uuid, owner, held_by,
            )
            return False

        try:
            # O_CREAT | O_EXCL — atomic create-if-absent.
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            try:
                os.write(fd, owner.encode('utf-8'))
            finally:
                os.close(fd)
            logger.info('_lock_uuid: ACQUIRED uuid=%s owner=%s', uuid, owner)
            return True
        except FileExistsError:
            # Race between the exists() check above and the open() — another
            # caller won. Treat as busy.
            logger.info(
                '_lock_uuid: race-lost uuid=%s owner=%s', uuid, owner,
            )
            return False


def _unlock_uuid(uuid, owner):
    """Release the per-uuid lock. Best-effort, never throws.

    Always called in a `finally` so a partial move doesn't leave a stale
    lock that blocks future operations forever. If the lockfile is missing
    (already removed by a cleanup step), that's not an error.
    """
    path = _lockfile_path(uuid)
    try:
        if os.path.exists(path):
            os.remove(path)
            logger.info('_unlock_uuid: RELEASED uuid=%s owner=%s', uuid, owner)
    except OSError as e:
        # Most common cause: the 002-ingest/<uuid>/ dir was already cleaned
        # up by the move's cleanup branch. Harmless.
        logger.info(
            '_unlock_uuid: cleanup-only uuid=%s owner=%s err=%s',
            uuid, owner, e,
        )


def _is_locked(uuid):
    """Caller-friendly check (read-only). Used by tests and diagnostics."""
    return os.path.exists(_lockfile_path(uuid))


# ---------------------------------------------------------------------------


def _list_packages(folder):
    """Non-hidden package DIRECTORIES inside 001-ready/<folder>.

    2026-07-24 (feature-batch-packaging-qa, finding F3): the legacy
    listers used bare os.listdir, so a stray file dropped directly in
    the batch folder was treated as a package name — os.listdir(<file>)
    then raised NotADirectoryError and the route returned a raw 500
    (and check_package_names_threads would even RENAME the stray file).
    Every ready-stage QA function now lists through this helper, which
    keeps only real directories; loose files are reported separately
    by _list_loose_files so staff get an actionable message instead of
    a server error.
    """
    base = ready_path + folder
    return [
        f for f in os.listdir(base)
        if not f.startswith('.') and os.path.isdir(os.path.join(base, f))
    ]


def _list_loose_files(folder):
    """Non-hidden loose FILES directly inside 001-ready/<folder> (see
    _list_packages — these are structure mistakes, not packages)."""
    base = ready_path + folder
    return [
        f for f in os.listdir(base)
        if not f.startswith('.') and not os.path.isdir(os.path.join(base, f))
    ]


def get_ready_folders():
    """
    Gets ready folders
    @returns Dictionary
    """

    ready_list = {}
    folders = [f for f in os.listdir(ready_path) if not f.startswith('.')]

    for folder in folders:

        package_count = len([name for name in os.listdir(ready_path + folder) if
                             os.path.isdir(os.path.join(ready_path + folder, name))])

        if package_count > 0:
            ready_list[folder] = package_count

    return dict(result=ready_list, errors=[])


def set_collection_folder_name(folder):
    """
    Creates collection folder file
    @param: folder
    @returns: void
    """

    try:
        file = open('collection', 'w+')
        file.write(folder)
        return True
    except Exception as e:
        logger.info(e)
        logger.info('ERROR: Unable to create collection folder file - ' + folder)
        return False


def get_collection_folder_name():
    """
    Gets collection folder file
    @param: folder
    @returns: string
    """

    try:
        with open('collection') as collection_file:
            folder = collection_file.read()
    except Exception as e:
        logger.info(e)
        logger.info('ERROR: Unable to open collection file')

    return folder


def check_folder_name(folder):
    """
    Checks if folder name conforms to naming standard
    @param: folder
    @returns: Dictionary
    """

    errors = []

    if folder.find('new_') == -1:
        errors.append('Collection folder name is missing "new_" part.')

    if folder.find('-resources') == -1:
        errors.append('Collection folder name is missing "-resources" part.')

    if folder.find('resources_') == -1:
        errors.append('Collection folder name is missing "resources_" part')

    tmp = folder.split('_')
    is_id = tmp[-1].isdigit()

    if is_id == False:
        errors.append('Collection folder is missing "URI" part')

    return dict(result='collection_folder_name_checked', errors=errors)


def get_package_names(folder):
    """
    Gets package names
    :param folder:
    :return: packages
    """

    [os.remove(ready_path + folder + '/' + f) for f in os.listdir(ready_path + folder)
     if f.startswith('.') and os.path.isfile(ready_path + folder + '/' + f)]
    return _list_packages(folder)


def check_package_names(folder):
    """
    Checks package names and fixes case issues and removes spaces
    @param: folder
    @returns: Dictionary
    """

    threads = []
    packages = _list_packages(folder)
    [os.remove(ready_path + folder + '/' + f) for f in os.listdir(ready_path + folder)
     if f.startswith('.') and os.path.isfile(ready_path + folder + '/' + f)]
    errors = []

    # Loose files are structure mistakes, not packages — report them
    # instead of renaming them (the thread helper used to lowercase a
    # stray extensionless file as if it were a package folder).
    for loose in _list_loose_files(folder):
        errors.append(loose + ' is a file, not a package folder. Move it into a package folder.')

    if len(packages) == 0:
        errors.append(['No packages found'])

    for i in packages:

        thread = threading.Thread(target=check_package_names_threads, args=(folder, i))
        threads.append(thread)
        thread.start()

        for thread in threads:
            thread.join()

    return dict(result='package_names_checked.', errors=errors)


def check_package_names_threads(folder, i):
    """
    Processes packages (thread function for check_package_names)
    @param: folder
    @param: i
    @returns: Dictionary
    """

    package = ready_path + folder + '/'

    if i.upper():
        call_number = i.find('.')

        if call_number == -1:
            os.rename(package + i, package + i.lower().replace(' ', ''))


def check_file_names(folder):
    """
    Checks file names and fixes case issues and removes spaces
    @param: folder
    @returns: Dictionary
    """

    packages = _list_packages(folder)
    threads = []
    files_arr = []
    errors = []
    # Defined before the loop: with zero packages (e.g. a folder holding
    # only loose files) the loop never runs and the count must be 0, not
    # an unbound name.
    local_file_count = 0

    try:
        if os.path.exists(errors_file):
            os.remove(errors_file)
    except Exception as e:
        logger.info(e)
        logger.info('Unable to delete errors_file')

    # Report loose files up front. Before the _list_packages fix (F3)
    # these crashed the whole check with NotADirectoryError → HTTP 500.
    loose_files = _list_loose_files(folder)
    for loose in loose_files:
        errors.append(loose + ' is a file, not a package folder. Move it into a package folder.')

    for i in packages:

        thread = threading.Thread(target=check_file_names_threads, args=(folder, i))
        threads.append(thread)
        thread.start()

        # Get total file count from packages
        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]
        [os.remove(package + f) for f in os.listdir(package)
         if f.startswith('.') and os.path.isfile(package + f)]

        if len(files) < 2:
            errors.append(i + '  is missing files.')

        for j in files:
            files_arr.append(j)

        local_file_count = len(files_arr)

    for thread in threads:
        thread.join()

    try:
        # Append (not replace) so structural errors collected above are
        # not lost when the image-check error file exists.
        with open(errors_file) as file_errors:
            errors.extend(file_errors.readlines())
    except Exception as e:
        logger.info(e)
        logger.info('ERROR: Unable to open error file - ' + errors_file)

    return dict(result=local_file_count, errors=errors)


def check_file_names_threads(folder, i):
    """
    Processes packages (thread function for check_file_names)
    @param: folder
    @param: i
    @returns: void
    """

    package = ready_path + folder + '/' + i + '/'
    files = [f for f in os.listdir(package) if not f.startswith('.')]
    [os.remove(package + f) for f in os.listdir(package) if f.startswith('.')]

    for j in files:

        if j.upper():

            call_number = j.find('.')

            if call_number == -1:
                os.rename(package + j, package + j.lower().replace(' ', ''))
            elif call_number != -1:
                os.rename(package + j, package + j.replace(' ', ''))

            # check images here
            file = package + j
            # if file.endswith('.tiff') or file.endswith('.tif') or file.endswith('.jpg') or file.endswith('.png'):
                # validates image
                # check_image_file(file, j)

            # TODO: check pdf size here
            # if file.endswith('.pdf'):
                # check_pdf_file(file, j)


def check_image_file(full_path, file_name):
    """
    Checks image files to determine if they are broken/corrupt
    @param: full_path
    @param: file_name
    @returns: Dictionary
    """

    try:
        img = Image.open(full_path)
        img.verify()  # confirm that file is an image
        img.close()
        img = Image.open(full_path)
        img.transpose(Image.FLIP_LEFT_RIGHT)  # attempt to manipulate file to determine if it's broken
        img.close()
    except OSError as error:
        try:
            errors = open(errors_file, 'a+')
            errors.write(file_name + ' - ' + str(error) + '\n')
        except Exception as e:
            logger.info(e)
            logger.info('ERROR: Unable to create error file - ' + errors_file)


def check_uri_txt(folder):
    """
    Checks for missing uri.txt files
    @param: ready_path
    @param: folder
    @returns: Dictionary
    """

    errors = []
    packages = _list_packages(folder)

    if len(packages) == 0:
        return errors.append(-1)

    for i in packages:

        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]

        if 'uri.txt' not in files:
            errors.append(i + ' is missing a uri.txt file')

    return dict(result='URI txt files checked', errors=errors)


def get_uri_txt(folder, package):
    """
    Gets ArchivesSpace URIs
    @param: ready_path
    @param: folder
    @returns: Dictionary
    """

    uris = []
    errors = []
    # packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

    # if len(packages) == 0:
    #    return errors.append(-1)
    logger.info(folder)
    logger.info(package)
    package_path = ready_path + folder + '/' + package + '/'
    files = [f for f in os.listdir(package_path) if not f.startswith('.')]

    if 'uri.txt' in files:
        uri_txt = ready_path + folder + '/' + package + '/uri.txt'
        with open(f'{uri_txt}', 'r') as uri:
            uri_text = uri.read()
            uris.append(uri_text)

    """
    for i in packages:

        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]

        if 'uri.txt' in files:
            uri_txt = ready_path + folder + '/' + i + '/uri.txt'
            with open(f'{uri_txt}', 'r') as uri:
                uri_text = uri.read()
                uris.append(uri_text)
    """

    return dict(result=uris, errors=errors)


def get_total_batch_size(folder):
    """
    Checks package file size (bytes)
    @param: folder
    @returns: Dictionary
    https://stackoverflow.com/questions/1392413/calculating-a-directorys-size-using-python
    """

    package = ready_path + folder
    total_size = 0
    errors = []

    try:
        for dirpath, dirnames, filenames in os.walk(package):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except Exception as e:
        logger.info(e)
        errors.append('Unable to get total batch size')
    logger.info(total_size)
    return dict(result=total_size, errors=errors)


def get_package_file_count(collection_folder, package):
    """
    Gets package count in batch
    @param: collection_folder
    @param: package
    @returns: count
    """

    try:
        dir_path = ready_path + collection_folder + '/' + package
        count = 0
        # Iterate directory
        for path in os.listdir(dir_path):
           # check if current path is a directory
           if os.path.isfile(os.path.join(dir_path, path)):
               count += 1
        logger.info('Package count:', count)

        return count
    except Exception as e:
        logger.info(e)


def move_to_ingest(uuid, folder, package):
    '''
    Moves folder from ready to ingest folder and renames it using pid.

    Protected by the per-uuid lock (added 2026-05-23) — if another op
    (e.g. an in-flight move_from_ingest_to_ready cancel rollback)
    holds the lock, returns `move_in_progress` without touching disk.

    @param: uuid
    @param: folder
    @param: package
    @returns: Dictionary
    '''

    errors = []
    mode = 0o777

    if not _lock_uuid(uuid, owner='move_to_ingest'):
        errors.append('move_in_progress: another operation holds the lock')
        return dict(result='move_in_progress', errors=errors)

    try:
        # create collection uuid folder in 002-ingest folder
        # (the lock helper already created the dir if needed, but the
        # legacy behavior is to create with mode=0o777 explicitly —
        # preserve it.)
        try:
            if not os.path.exists(ingest_path + uuid):
                os.mkdir(ingest_path + uuid, mode)
        except Exception as e:
            logger.info(e)
            errors.append('ERROR: Unable to create folder (move_to_ingest)')

        # move package to new uuid folder in 002-ingest
        try:
            shutil.move(ready_path + folder + '/' + package, ingest_path + uuid)
        except Exception as e:
            logger.info(e)
            errors.append('ERROR: Unable to move folder (move_to_ingest)')

        if len(errors) == 0:
            result = 'packages_moved_to_ingested_folder.'
        else:
            result = 'packages_not_moved_to_ingested_folder.'

        return dict(result=result, errors=errors)
    finally:
        _unlock_uuid(uuid, owner='move_to_ingest')


def move_from_ingest_to_ready(uuid, folder, package, actor=None):
    '''
    Inverse of move_to_ingest. Moves a single package from
    002-ingest/<uuid>/<package> back to 001-ready/<folder>/<package>.
    Used by the ingest service's pre-ingest rollback AND post-cancel
    return-to-packaging endpoints to undo a Stage 2 move.

    Behavior:
      * Acquires a per-uuid lock to prevent concurrent moves (e.g. a
        cancel that races against Stage 2 move_to_sftp still copying).
        On contention, returns 'move_in_progress' without touching disk.
      * Best-effort cleans up the Archivematica SFTP staging copy
        BEFORE the local move. The SFTP copy may or may not exist
        (depends on which sub-state Stage 2 was in when staff
        cancelled); failures are logged + recorded in `sftp_clean`
        but DON'T block the local move.
      * If the package directory exists in 002-ingest, it is moved back.
      * If the destination batch folder in 001-ready does not exist, it
        is created.
      * If the 002-ingest/<uuid>/ directory becomes empty after the
        move, it is removed (matches move_to_ingest's per-uuid pattern).
      * Idempotent: if the package is already in 001-ready (already
        rolled back) and not in 002-ingest, this returns success.
      * uri.txt files travel with the package — they are preserved
        through shutil.move and reappear in 001-ready, which means
        the folder reappears in /processed (Packaging and Ingesting
        view) for re-submit.

    Audit:
      * `actor` (optional) is logged at INFO. Pass the authenticated
        staff identifier so this entry can be correlated with the
        ingest service's tbl_ingest_events audit trail. The query
        param is consumed by the route handler and threaded through
        here; never trust it for authz (the X-API-Key header is
        the gate).

    @param: uuid
    @param: folder
    @param: package
    @param: actor (optional)
    @returns: Dictionary with keys:
        result    — one of: packages_moved_back_to_ready.,
                            already_in_ready,
                            move_in_progress,
                            source_not_found,
                            destination_create_failed,
                            move_failed
        errors    — list of error strings (empty on success)
        sftp_clean — dict(attempted: bool, ok: bool, err: str|None)
    '''

    logger.info(
        'move_from_ingest_to_ready: BEGIN uuid=%s folder=%s package=%s actor=%s',
        uuid, folder, package, actor or '<unset>',
    )

    errors = []
    sftp_clean = {'attempted': False, 'ok': False, 'err': None}

    if not _lock_uuid(uuid, owner='move_from_ingest_to_ready'):
        errors.append('move_in_progress: another operation holds the lock')
        logger.info(
            'move_from_ingest_to_ready: BUSY uuid=%s actor=%s',
            uuid, actor or '<unset>',
        )
        return dict(result='move_in_progress', errors=errors, sftp_clean=sftp_clean)

    try:
        src_dir = ingest_path + uuid
        src_pkg = src_dir + '/' + package
        dst_batch = ready_path + folder
        dst_pkg = dst_batch + '/' + package

        # ---- Idempotency check ----
        # If the source package isn't in 002-ingest, either:
        #   (a) the destination already has it — already rolled back, success.
        #   (b) neither has it — something is wrong, surface an error.
        if not os.path.exists(src_pkg):
            if os.path.exists(dst_pkg):
                logger.info(
                    'move_from_ingest_to_ready: ALREADY_IN_READY uuid=%s package=%s actor=%s',
                    uuid, package, actor or '<unset>',
                )
                return dict(result='already_in_ready', errors=[], sftp_clean=sftp_clean)
            errors.append(
                'ERROR: Source package not found in 002-ingest (move_from_ingest_to_ready)'
            )
            logger.info(
                'move_from_ingest_to_ready: SOURCE_NOT_FOUND uuid=%s package=%s actor=%s',
                uuid, package, actor or '<unset>',
            )
            return dict(result='source_not_found', errors=errors, sftp_clean=sftp_clean)

        # ---- Best-effort SFTP cleanup ----
        # Clean up the Archivematica SFTP staging copy first. If the worker
        # had progressed past Stage 2's move_to_sftp, a partial (or
        # complete) copy exists on the AM side; if we don't clean it up
        # here, staff have to delete it manually. The cleanup is
        # best-effort — failure is logged + recorded in the result but
        # never blocks the local move.
        try:
            sftp_clean['attempted'] = True
            clean_up_sftp(uuid, package)
            sftp_clean['ok'] = True
            logger.info(
                'move_from_ingest_to_ready: SFTP_CLEAN_OK uuid=%s package=%s',
                uuid, package,
            )
        except Exception as e:
            sftp_clean['err'] = str(e)
            logger.warning(
                'move_from_ingest_to_ready: SFTP_CLEAN_FAILED (non-fatal) '
                'uuid=%s package=%s err=%s',
                uuid, package, e,
            )

        # ---- Ensure destination batch folder exists ----
        if not os.path.exists(dst_batch):
            try:
                os.mkdir(dst_batch, 0o777)
            except Exception as e:
                logger.info(e)
                errors.append(
                    'ERROR: Unable to create destination batch folder '
                    '(move_from_ingest_to_ready)'
                )
                return dict(
                    result='destination_create_failed',
                    errors=errors,
                    sftp_clean=sftp_clean,
                )

        # ---- Move the package back ----
        try:
            shutil.move(src_pkg, dst_batch + '/')
        except Exception as e:
            logger.info(e)
            errors.append('ERROR: Unable to move folder (move_from_ingest_to_ready)')
            return dict(result='move_failed', errors=errors, sftp_clean=sftp_clean)

        # ---- Clean up empty 002-ingest/<uuid>/ ----
        # The lockfile lives inside this directory; release it BEFORE the
        # rmdir so the directory is genuinely empty. The finally below
        # will be a no-op (path missing → branch in _unlock_uuid skips).
        _unlock_uuid(uuid, owner='move_from_ingest_to_ready')
        try:
            if os.path.exists(src_dir) and not os.listdir(src_dir):
                os.rmdir(src_dir)
                logger.info(
                    'move_from_ingest_to_ready: CLEANED uuid_dir uuid=%s', uuid,
                )
        except Exception as e:
            # Non-fatal; the package is back, just log and move on.
            logger.info(
                'move_from_ingest_to_ready: cleanup failed (non-fatal) '
                'uuid=%s err=%s',
                uuid, e,
            )

        logger.info(
            'move_from_ingest_to_ready: SUCCESS uuid=%s folder=%s package=%s actor=%s',
            uuid, folder, package, actor or '<unset>',
        )
        return dict(
            result='packages_moved_back_to_ready.',
            errors=[],
            sftp_clean=sftp_clean,
        )
    finally:
        # Defensive — if we returned early without releasing (e.g. exception
        # in an unexpected place), make sure the lock is gone. _unlock_uuid
        # is a no-op when the file is already removed.
        _unlock_uuid(uuid, owner='move_from_ingest_to_ready')


# Sentinel dropped into the local 002-ingest/<uuid> staging dir by the
# background upload worker when the SFTP put fails. check_sftp surfaces it
# as message='upload_failed' so the Node poller can halt the row at once
# instead of waiting out the upload timeout. A dotfile in this dir, like
# the existing .lock — it travels with the directory and AM ignores it.
UPLOAD_ERROR_MARKER = '.upload_error'


def _upload_error_marker_path(pid):
    """Path of the per-package upload-error sentinel (see UPLOAD_ERROR_MARKER)."""
    return os.path.join(ingest_path + pid, UPLOAD_ERROR_MARKER)


def _do_sftp_put(pid):
    """Background worker: the actual recursive SFTP put.

    Runs in the daemon thread started by move_to_sftp so the HTTP request
    returns immediately and the Node side can poll check_sftp for live byte
    progress while the transfer runs. Opens its own SFTP connection (the
    request's connection is long gone by the time this executes). On failure
    it writes the .upload_error marker into the local staging dir; check_sftp
    reports that to the poller, which halts the row.
    """
    marker = _upload_error_marker_path(pid)
    # Clear any stale marker from a previous attempt so a retry starts clean.
    try:
        os.remove(marker)
    except OSError:
        pass

    try:
        client, sftp = _open_sftp()
        try:
            _sftp_put_r(sftp, ingest_path, sftp_path, preserve_mtime=True)
            packages = sftp.listdir(sftp_path)
            if pid not in packages:
                raise RuntimeError('package %s not found on sftp after put' % pid)
        finally:
            sftp.close()
            client.close()
    except Exception as e:  # noqa: BLE001
        logger.error('move_to_sftp background put failed for %s: %s', pid, e)
        try:
            with open(marker, 'w') as fh:
                fh.write(str(e)[:1000])
        except OSError as marker_err:
            logger.error('could not write upload-error marker for %s: %s', pid, marker_err)


def move_to_sftp(pid):
    """Kick off the recursive SFTP put in a BACKGROUND thread; return at once.

    The put is long-running (minutes to ~30 min for big batches). The legacy
    implementation did it synchronously, so the Node upload stage — which
    awaits move_to_sftp and only THEN polls check_sftp for byte progress —
    never saw the upload in flight: the whole transfer happened inside this
    call, the poll ran after it had already finished, and the dashboard sat
    on "UPLOADING" with no progress bar the entire time. Backgrounding the
    put lets the poller observe the remote dir growing
    (remote_package_size_bytes) against the local total
    (local_package_size_bytes) and render a live %.

    Mirrors move_to_ingest's fire-and-don't-wait contract. Completion is
    observed by the poller via check_sftp (remote file count reaching the
    local count); failure via the .upload_error marker check_sftp surfaces.

    @param: pid
    @returns: Dictionary (message='upload_started')
    """
    thread = threading.Thread(target=_do_sftp_put, args=(pid,), daemon=True)
    thread.start()
    return dict(message='upload_started')


def check_sftp(uuid, local_file_count):
    """
    checks upload status on archivematica sftp
    @param: pid
    @param: local_file_count
    @returns: Dictionary
    """

    # If the background SFTP put (move_to_sftp) died, surface it at once so
    # the Node poller halts the row instead of waiting out the upload
    # timeout. Cheap local check before any SFTP round-trip.
    marker = _upload_error_marker_path(uuid)
    if os.path.exists(marker):
        try:
            with open(marker) as fh:
                err_text = fh.read()[:1000]
        except OSError:
            err_text = ''
        return dict(message='upload_failed', error=err_text or 'sftp upload failed')

    file_names = []
    dir_names = []
    un_name = []

    def store_files_name(fname):
        file_names.append(fname)

    def store_dir_name(dirname):
        dir_names.append(dirname)

    def store_other_file_types(name):
        un_name.append(name)

    client, sftp = _open_sftp()
    try:
        remote_package = sftp_path + '/' + uuid
        try:
            _sftp_walk(sftp, remote_package, on_file=store_files_name, on_dir=store_dir_name, on_other=store_other_file_types)
        except IOError:
            # The remote package dir doesn't exist yet — the background put
            # (move_to_sftp) is just starting and hasn't created it. Report
            # 0 files arrived (in_progress) so the poller keeps waiting
            # instead of seeing a 500. _sftp_dir_size below already tolerates
            # the missing dir (returns 0).
            del file_names[:]
        remote_file_count = len(file_names)

        # Sum file sizes via pure SFTP. The legacy `du -h -s` call
        # was broken on the production AM SFTP host (internal-sftp
        # rejects exec_command — see _ssh_exec docstring), so the
        # `remote_package_size` field was silently empty. The Node
        # side doesn't surface this to staff today, but the
        # accurate byte sum is useful for logs + future use.
        remote_package_size_bytes = _sftp_dir_size(sftp, remote_package)
        remote_package_size = _human_size(remote_package_size_bytes)

        # Local total for the same uuid staging dir (002-ingest/<uuid> —
        # the source move_to_sftp pushes to sftp_path/<uuid>). The Node
        # side divides remote_package_size_bytes by this to render a byte-
        # accurate upload %, which advances smoothly even for a single
        # large file (the remote file grows during the put). 0 when the
        # local dir is gone/unreadable — the caller treats that as
        # "unknown total" and falls back to the file-count readout.
        local_package_size_bytes = _local_dir_size(ingest_path + uuid)

        if int(local_file_count) == remote_file_count:
            return dict(message='upload_complete', data=[file_names, remote_file_count])

        return dict(message='in_progress', file_names=file_names, remote_file_count=remote_file_count,
                    local_file_count=local_file_count,
                    remote_package_size=remote_package_size,
                    remote_package_size_bytes=remote_package_size_bytes,
                    local_package_size_bytes=local_package_size_bytes)
    finally:
        sftp.close()
        client.close()


def _local_dir_size(path):
    """Sum of regular-file sizes under a local directory, in bytes.

    Used by check_sftp to report the upload *total* for the byte-accurate
    progress %. Symlinks are skipped (match get_total_batch_size). Returns
    0 if the path is missing or unreadable — the Node side treats a 0
    total as "unknown" and the dashboard falls back to the file count.
    """
    total = 0
    try:
        for dirpath, _dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    try:
                        total += os.path.getsize(fp)
                    except OSError:
                        pass
    except Exception as e:  # noqa: BLE001
        logger.info('local dir size failed for %s: %s', path, e)
    return total


def _human_size(num_bytes):
    """Format a byte count to a `du -h`-style short string.

    Matches the legacy contract: short ASCII (e.g. "12K", "3.4M",
    "1.2G", "8B"). Single decimal place for >= 1 KiB to match GNU
    du's default. Returns "0" for 0 (du's output for empty).
    """
    if not num_bytes:
        return '0'
    units = ('B', 'K', 'M', 'G', 'T', 'P')
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == 'B':
                return '%d%s' % (int(size), unit)
            return '%.1f%s' % (size, unit)
        size /= 1024
    return '%.1f%s' % (size, units[-1])


def move_to_ingested(uuid, folder):
    """
    Archives a completed batch's packages to Wasabi S3 and, on VERIFIED
    success, removes the local 002-ingest staging copy.

    2026-07-26 (003-ingested retirement, phase 3 — see
    repo/INGESTED_RETIREMENT_PLAN.md): the local `003-ingested/` archive
    copy is NO LONGER WRITTEN. The `cp -R` that produced it was never
    verified — the 2026-07-26 reconciliation of the 7.2 TB backlog found
    its one corrupt file was truncated by that very copy path — while the
    Wasabi upload below IS verified per file (head_object size check in
    wasabi.upload_directory) and gates the source cleanup. The Wasabi
    batch archive is the sole batch-snapshot custodian from here on;
    preservation-grade redundancy lives in the AIP chain (Wasabi
    aip-store + DuraCloud + AM storage).

    This also retires the old two-branch layout (per-file cp when the
    ingested folder existed / whole-folder rename when it didn't). Both
    branches produced the same S3 keys — `<folder-without-new_>/<rel>` —
    which is what this single path uploads. The rename variant also
    uploaded ALL of 002-ingest with an empty prefix, which would have
    swept any concurrent ingest's staging dir into the wrong keys; the
    unified path uploads only 002-ingest/<uuid>/.

    On S3 failure the source is left in place (2026-05-24 data-loss fix
    invariant: cleanup ONLY on verified success) and the caller — repov2
    Stage 5 — records a FAILED archive_to_wasabi job so staff see it in
    Job History. The remedy is re-running the archive after the cause is
    fixed; nothing is lost.

    @param: uuid    directory name in 002-ingest (the collection pid)
    @param: folder  batch folder name (used, minus `new_`, as the S3 prefix)
    @returns: Dictionary {result, errors} — same contract as before:
        result 'packages_moved_to_ingested_folder' on success,
               'packages_not_moved_to_ingested_folder' otherwise.
    """

    errors = []
    result = 'packages_not_moved_to_ingested_folder'
    source = ingest_path + uuid + '/'

    if not os.path.isdir(source):
        logger.warning('move_to_ingested: source not found: %s', source)
        errors.append('ERROR: Source not found in 002-ingest (move_to_ingested)')
        return dict(result=result, errors=errors)

    # Legacy side effect preserved: re-open the 001-ready batch folder's
    # permissions so staff can keep adding packages to an in-progress
    # collection. Best-effort — the folder may already be gone when the
    # last package of a batch completes (chown failure is logged by _run
    # and ignored, matching the previous behavior).
    reset_permissions(folder)

    try:
        move_result = move_to_s3(source, folder.replace('new_', ''))
        if move_result != 0:
            errors.append('ERROR: Unable to move packages to wasabi s3')
        else:
            # Verified upload (per-file head_object check inside
            # wasabi.upload_directory) — only now is the staging copy
            # safe to remove.
            shutil.rmtree(source)
            result = 'packages_moved_to_ingested_folder'
    except Exception as e:
        logger.error('move_to_ingested: %s', e)
        errors.append('ERROR: Unable to archive packages to wasabi s3 (move_to_ingested)')

    return dict(result=result, errors=errors)


def reset_permissions(folder):
    """
    Resets ready folder permissions so that staff is able to add more packages
    @param: folder
    :returns: void
    """

    message = 'Permissions changed'

    try:
        _run(['chown', '-R', '--', uid + ':' + gid, ready_path + folder])
    except Exception as e:
        logger.info(e)
        message = 'Unable to reset permissions'

    return message


def move_to_s3(source, folder):
    """
    Upload a local directory tree to the configured Wasabi S3 bucket.

    Replaces the prior `os.system('aws s3 cp ...')` shellout. The new
    implementation lives in `lib/wasabi.py` and uses boto3 directly
    against the same WASABI_PROFILE / WASABI_ENDPOINT / WASABI_BUCKET
    env vars — auth is unchanged from the CLI version.

    Two improvements over the shellout:
      - Every upload logs start, per-file size + key, per-file 25/50/
        75/100% milestones for large files, and an END summary with
        elapsed_ms and byte total. Staff can audit any upload via
        `journalctl -u curation-service | grep wasabi`.
      - The return code is a clean 0/1: 0 on full success, 1 on any
        per-file failure or transport error. Fixes the prior data-loss
        bug where os.system's encoded shell status was misinterpreted
        and the caller deleted the local source even on a failed upload.

    @param  source — local directory to upload (recursive walk).
    @param  folder — S3 key prefix segment for this batch.
    @returns int   — 0 success, 1 failure. Maintains the legacy 0/1
                     contract the caller (`move_to_ingested`) expects.
    """
    try:
        result = wasabi.upload_directory(source, folder)
    except RuntimeError as e:
        # Config-level error (missing WASABI_PROFILE etc). Surface
        # cleanly without crashing the route handler — the caller's
        # `if move_result != 0` branch records it in the errors[]
        # response.
        logger.error('move_to_s3: configuration error: %s', e)
        return 1
    if result['ok']:
        return 0
    logger.error(
        'move_to_s3: upload not OK uploaded=%d failed=%d errors=%s',
        result['uploaded'], result['failed'], result['errors'],
    )
    return 1


def clean_up_sftp(pid, archival_package):
    """
    Remove a package's files AND its parent directories from the
    Archivematica SFTP staging area.

    Layout cleaned up:
        <sftp_path>/<pid>/<archival_package>/<files>   ← files removed
        <sftp_path>/<pid>/<archival_package>/          ← directory removed
        <sftp_path>/<pid>/                              ← removed IF empty
                                                        (rmdir silently
                                                         no-ops if other
                                                         sibling packages
                                                         still live here,
                                                         e.g. concurrent
                                                         batch ingest)

    2026-05-23 patch: previously only the FILES inside the package
    directory were deleted (legacy `cd <target> && rm <pkg>/*`),
    leaving empty `<pid>/<package>/` and `<pid>/` directories behind.
    Staff saw orphaned "0" / "q-N" folders on the AM SFTP host after
    every cancel + return-to-packaging cycle. The new chained command
    removes the package tree entirely and best-effort prunes the
    `<pid>` parent.

    :param pid              curation-API per-ingest identifier (the
                            qa_uuid the worker passes to move_to_sftp
                            earlier; appears as the top-level directory
                            under sftp_path)
    :param archival_package package directory inside <pid>
    """

    logger.info('clean_up_sftp pid=%s archival_package=%s', pid, archival_package)

    client, sftp = _open_sftp()
    try:
        target = sftp_path + '/' + pid
        pkg_path = target + '/' + archival_package

        # Idempotency: if the package path doesn't exist on the
        # server (already cleaned up, or never uploaded), nothing
        # to do. sftp.stat raises IOError when the path is missing.
        try:
            sftp.stat(pkg_path)
        except IOError:
            logger.info(
                'clean_up_sftp: nothing to remove pid=%s package=%s',
                pid, archival_package,
            )
            return

        # Recursive remove of the package tree (files + subdirs +
        # the package dir itself). Pure SFTP — see _sftp_rmtree
        # for why we don't use shell exec.
        _sftp_rmtree(sftp, pkg_path)

        # Best-effort prune of the empty <pid> parent. sftp.rmdir
        # raises IOError if the directory still has sibling packages
        # (e.g. a concurrent batch ingest under the same collection
        # uuid). That's the right behavior — leave the parent for
        # the active siblings to use. The legacy shell version
        # achieved this with `rmdir; 2>/dev/null`; same semantics,
        # just plumbed through the SFTP error code.
        try:
            sftp.rmdir(target)
            logger.info(
                'clean_up_sftp: pruned empty parent uuid=%s', pid,
            )
        except IOError as e:
            logger.info(
                'clean_up_sftp: parent has siblings, kept uuid=%s err=%s',
                pid, e,
            )
    finally:
        sftp.close()
        client.close()
