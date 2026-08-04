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

Ready-stage QA checks, the local staging moves (001-ready -> 002-ingest
-> Wasabi), the paramiko SFTP transfer to the Archivematica staging
host, and the per-uuid lock that serializes moves for one collection.

Design history and rationale: repo/notes/CURATION_API_CODE_NOTES.md
"""

import fcntl
import logging
import os
import posixpath
import random
import shutil
import stat
import subprocess
import threading
import time

import paramiko

import config
from lib import wasabi

logger = logging.getLogger(__name__)

# Config values re-exported under the names the function bodies use.
# config.INGESTED_PATH is deliberately absent: this layer no longer writes
# the 003-ingested copy; only the retirement scripts read that path.
ready_path = config.READY_PATH
ingest_path = config.INGEST_PATH
sftp_host = config.SFTP_HOST
sftp_username = config.SFTP_ID
sftp_password = config.SFTP_PWD
sftp_path = config.SFTP_REMOTE_PATH
wasabi_endpoint = config.WASABI_ENDPOINT
wasabi_bucket = config.WASABI_BUCKET
wasabi_profile = config.WASABI_PROFILE
gid = config.GID
errors_file = config.ERRORS_FILE


# --- subprocess helper -------------------------------------------------------

def _run(argv):
    """Run an external command with NO shell.

    Callers must pass an argv list, and a `--` end-of-options marker before
    any operand that could begin with `-`.

    A non-zero exit is logged, not raised. Returns the CompletedProcess so
    a caller can inspect returncode.
    """
    logger.info('exec: %s', ' '.join(argv))
    result = subprocess.run(argv, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        logger.warning(
            'command exited non-zero (rc=%s): %s | stderr=%s',
            result.returncode, ' '.join(argv), (result.stderr or '').strip(),
        )
    return result


# --- paramiko helpers --------------------------------------------------------

# Connection-SETUP retry budget. Setup failures retry with exponential
# backoff + jitter; failures mid-transfer are NOT retried here (callers
# handle those via the .upload_error marker / route error envelopes).
SFTP_CONNECT_ATTEMPTS = 4
SFTP_CONNECT_BASE_DELAY_S = 2.0
SFTP_BANNER_TIMEOUT_S = 60
SFTP_TCP_TIMEOUT_S = 30
SFTP_AUTH_TIMEOUT_S = 30

# Throttles on connections to the vendor-managed AM SFTP host:
#   SFTP_MAX_SESSIONS           — hard cap on CONCURRENT SSH sessions.
#                                 Enforced HOST-wide (flock slot files in
#                                 SFTP_LOCK_DIR), not per gunicorn worker.
#   SFTP_CONNECT_MIN_INTERVAL_S — minimum spacing between connection
#                                 ATTEMPTS, per process.
# A slot is held for the lifetime of the SSH session and releases when the
# caller closes the client, or when the process dies (the kernel drops the
# flock), so slots cannot leak.
SFTP_MAX_SESSIONS = int(os.getenv('SFTP_MAX_SESSIONS', '5'))
SFTP_CONNECT_MIN_INTERVAL_S = float(os.getenv('SFTP_CONNECT_MIN_INTERVAL_S', '1.0'))
SFTP_SLOT_ACQUIRE_TIMEOUT_S = 600  # > longest realistic poll pile-up
SFTP_LOCK_DIR = os.getenv('SFTP_LOCK_DIR', '/tmp/curation-sftp-slots')
_sftp_pace_lock = threading.Lock()
# None (not 0.0) = no connect yet — time.monotonic()'s epoch is platform-
# defined, so a 0.0 seed would stall the first connect.
_sftp_last_connect_at = [None]


def _pace_connect():
    """Sleep (under lock) so connection attempts stay >= the minimum
    interval apart, process-wide. The first connect never waits —
    pacing starts the clock, it doesn't delay an idle service."""
    if SFTP_CONNECT_MIN_INTERVAL_S <= 0:
        return
    with _sftp_pace_lock:
        last = _sftp_last_connect_at[0]
        if last is not None:
            wait = last + SFTP_CONNECT_MIN_INTERVAL_S - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        _sftp_last_connect_at[0] = time.monotonic()


def _acquire_global_slot():
    """Claim one of SFTP_MAX_SESSIONS flock-backed slot files, HOST-wide
    (all gunicorn workers share the same lock dir). Returns the held
    fd. Raises RuntimeError when no slot frees within the timeout."""
    os.makedirs(SFTP_LOCK_DIR, exist_ok=True)
    deadline = time.monotonic() + SFTP_SLOT_ACQUIRE_TIMEOUT_S
    while True:
        for i in range(SFTP_MAX_SESSIONS):
            path = os.path.join(SFTP_LOCK_DIR, 'slot-%d.lock' % i)
            fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except OSError:
                os.close(fd)
        if time.monotonic() >= deadline:
            raise RuntimeError(
                'sftp session slots exhausted (%d in use host-wide) — '
                'refusing to open another connection' % SFTP_MAX_SESSIONS
            )
        # Jittered nap so waiters from different processes de-align.
        time.sleep(0.5 + random.random() * 0.5)


def _open_sftp():
    """Open a paramiko SSH+SFTP session with bounded connect retries.

    Caller must close both returned handles — closing the CLIENT
    releases the host-wide session slot (see the close wrapper below).
    Raises the last error after SFTP_CONNECT_ATTEMPTS failed setups,
    or RuntimeError if no session slot frees up in time.
    """
    slot_fd = _acquire_global_slot()
    try:
        client, sftp = _open_sftp_unslotted()
    except BaseException:
        os.close(slot_fd)  # closing the fd drops the flock
        raise
    _release_slot_on_close(client, slot_fd)
    return client, sftp


def _release_slot_on_close(client, slot_fd):
    """Arrange for the session slot to release exactly once when the
    caller closes the client (every _open_sftp caller closes in a
    finally block). Closing the fd drops the flock; if the process
    dies first, the kernel drops it — slots can't leak."""
    orig_close = client.close
    released = [False]

    def close_and_release():
        try:
            orig_close()
        finally:
            if not released[0]:
                released[0] = True
                try:
                    os.close(slot_fd)
                except OSError:
                    pass

    client.close = close_and_release


def _open_sftp_unslotted():
    last_err = None
    for attempt in range(1, SFTP_CONNECT_ATTEMPTS + 1):
        _pace_connect()
        client = paramiko.SSHClient()
        # Accepts any host key — insecure against MITM. Open item: move to
        # known_hosts verification.
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=sftp_host,
                username=sftp_username,
                password=sftp_password,
                look_for_keys=False,
                allow_agent=False,
                timeout=SFTP_TCP_TIMEOUT_S,
                banner_timeout=SFTP_BANNER_TIMEOUT_S,
                auth_timeout=SFTP_AUTH_TIMEOUT_S,
            )
            sftp = client.open_sftp()
            return client, sftp
        except (paramiko.SSHException, OSError) as e:
            last_err = e
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
            if attempt < SFTP_CONNECT_ATTEMPTS:
                delay = (
                    SFTP_CONNECT_BASE_DELAY_S
                    * (2 ** (attempt - 1))
                    * (0.5 + random.random())
                )
                logger.warning(
                    'sftp connect attempt %d/%d failed (%s); retrying in %.1fs',
                    attempt, SFTP_CONNECT_ATTEMPTS, e, delay,
                )
                time.sleep(delay)
    logger.error(
        'sftp connect failed after %d attempts: %s', SFTP_CONNECT_ATTEMPTS, last_err,
    )
    raise last_err


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


class UploadCancelled(Exception):
    """Raised inside a put to abort it — see request_upload_cancel."""


def _sftp_put_r(sftp, local_dir, remote_dir, preserve_mtime=True, should_abort=None):
    """Recursive directory upload.

    `should_abort` (optional callable) is checked before every file AND
    every ~8 MB inside a file (via paramiko's put callback); when it
    returns True the transfer raises UploadCancelled, so a multi-GB file
    aborts mid-way rather than at the next file boundary.
    """
    check_every = 256  # put callback fires per 32 KB chunk → ~8 MB

    def _chunk_cb(_transferred, _total, _counter=[0]):
        _counter[0] += 1
        if _counter[0] % check_every == 0 and should_abort and should_abort():
            raise UploadCancelled()

    _sftp_mkdir_p(sftp, remote_dir)
    for root, dirs, files in os.walk(local_dir):
        rel = os.path.relpath(root, local_dir)
        rel_remote = remote_dir if rel == '.' else posixpath.join(remote_dir, rel.replace(os.sep, '/'))
        _sftp_mkdir_p(sftp, rel_remote)
        for fname in files:
            if should_abort and should_abort():
                raise UploadCancelled()
            local_file = os.path.join(root, fname)
            remote_file = posixpath.join(rel_remote, fname)
            sftp.put(local_file, remote_file, callback=_chunk_cb if should_abort else None)
            if preserve_mtime:
                st = os.stat(local_file)
                sftp.utime(remote_file, (st.st_atime, st.st_mtime))


def _sftp_walk(sftp, remote_dir, on_file=None, on_dir=None, on_other=None):
    """Recursive walk over a remote directory."""
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
    """Run a shell command over SSH and return decoded stdout.

    DEPRECATED — unusable against the production Archivematica host: it
    runs an `internal-sftp` Subsystem with no shell, and the command
    silently returns b'' instead of failing. Retained only for one-off
    diagnostics against a shell-enabled host. Do NOT add new callers;
    use the pure-SFTP helpers (_sftp_rmtree, _sftp_dir_size) instead.
    """
    stdin, stdout, stderr = client.exec_command(command)
    return stdout.read().decode('utf-8', errors='replace')


def _sftp_rmtree(sftp, remote_dir):
    """Pure-SFTP recursive remove. Analogue to shutil.rmtree.

    Walks the tree once, removes all files, then all directories
    deepest-first, then `remote_dir` itself. Symlinks and other
    non-directory entries are removed, never traversed.

    Best-effort throughout and no return value: a failed remove is
    logged at INFO and the walk continues, so one stuck entry leaves
    the rest of the tree cleaned. Caller can sftp.stat afterwards to
    confirm the tree is gone.
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
    """Variant of _sftp_walk whose callbacks take (path, SFTPAttributes).

    Separate from _sftp_walk, whose callbacks take the path only.
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


# --- per-uuid lockfile -------------------------------------------------------

# The on-disk lockfile is the cross-process guard; this threading.Lock only
# closes the in-process race between the stat() and open() in _lock_uuid.
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

    Every ready-stage QA function lists through this helper so that a
    loose file in the batch folder is never mistaken for a package.
    Loose files are reported separately by _list_loose_files.
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

    # Loose files are structure mistakes, not packages — report them,
    # never rename them.
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
    # Defined before the loop: with zero packages the loop never runs and
    # the count must be 0, not an unbound name.
    local_file_count = 0

    try:
        if os.path.exists(errors_file):
            os.remove(errors_file)
    except Exception as e:
        logger.info(e)
        logger.info('Unable to delete errors_file')

    # Report loose files up front.
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
        # Append, never replace — the structural errors collected above
        # must survive.
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

    Protected by the per-uuid lock: if another op holds it, returns
    `move_in_progress` without touching disk.

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
        # (the lock helper may already have created it; the explicit
        # mode=0o777 create is part of the contract)
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
      * `actor` (optional) is logged at INFO for correlation with the
        ingest service's tbl_ingest_events trail. It is a client-supplied
        label — never an authz input (the X-API-Key header is the gate).

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
        # Remove the Archivematica SFTP staging copy (partial or complete)
        # BEFORE the local move. Best-effort: failure is logged and recorded
        # in sftp_clean but never blocks the local move.
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
# as message='upload_failed' so the Node poller can halt the row at once.
UPLOAD_ERROR_MARKER = '.upload_error'


def _upload_error_marker_path(pid):
    """Path of the per-package upload-error sentinel (see UPLOAD_ERROR_MARKER)."""
    return os.path.join(ingest_path + pid, UPLOAD_ERROR_MARKER)


UPLOAD_CANCEL_MARKER = '.upload_cancel'


def _upload_cancel_marker_path(pid):
    """Path of the cancel-request sentinel. A FILE, not memory: the put
    thread and the cancel request can land on different gunicorn
    workers, so the filesystem is the cross-process signal."""
    return os.path.join(ingest_path + pid, UPLOAD_CANCEL_MARKER)


def request_upload_cancel(pid):
    """Ask an in-flight put for `pid` to stop.

    Best-effort and idempotent: writes the sentinel; the put checks it
    per file and every ~8 MB within a file. Returns message='no_staging_dir'
    when there is no staging dir (nothing can be uploading from it)."""
    staging = ingest_path + pid
    if not os.path.isdir(staging):
        return dict(message='no_staging_dir')
    try:
        with open(_upload_cancel_marker_path(pid), 'w') as fh:
            fh.write('cancel requested')
        logger.info('request_upload_cancel: flag written for %s', pid)
        return dict(message='cancel_requested')
    except OSError as e:
        logger.error('request_upload_cancel failed for %s: %s', pid, e)
        return dict(message='cancel_flag_write_failed', error=str(e))


def _put_lock_path(pid):
    return os.path.join(SFTP_LOCK_DIR, 'put-%s.lock' % pid)


# Per-package single-flight registry for THIS process: a second
# move_to_sftp for a pid already uploading here is a no-op
# ('upload_already_running'). Host-wide single-flight is the put lock
# in _do_sftp_put.
_sftp_puts_in_flight = set()
_sftp_puts_lock = threading.Lock()


def _do_sftp_put(pid):
    """Background worker: the actual recursive SFTP put.

    Runs in the daemon thread started by move_to_sftp, and opens its own
    SFTP connection. On failure it writes the .upload_error marker into
    the local staging dir; check_sftp reports that to the poller, which
    halts the row.
    """
    marker = _upload_error_marker_path(pid)

    # HOST-wide single-flight: if another process already holds the put
    # lock for this pid, bow out and let that put finish. Concurrent puts
    # of one tree corrupt each other (paramiko's post-put verify reports
    # "size mismatch in put!").
    os.makedirs(SFTP_LOCK_DIR, exist_ok=True)
    put_lock_fd = os.open(_put_lock_path(pid), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(put_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(put_lock_fd)
        logger.info(
            '_do_sftp_put: another process is already uploading %s — bowing out', pid,
        )
        return

    cancel_flag = _upload_cancel_marker_path(pid)

    def _cancelled():
        return os.path.exists(cancel_flag)

    # Clear stale markers from a previous attempt so a retry starts clean.
    for stale in (marker, cancel_flag):
        try:
            os.remove(stale)
        except OSError:
            pass

    try:
        client, sftp = _open_sftp()
        try:
            _sftp_put_r(
                sftp, ingest_path, sftp_path,
                preserve_mtime=True, should_abort=_cancelled,
            )
            packages = sftp.listdir(sftp_path)
            if pid not in packages:
                raise RuntimeError('package %s not found on sftp after put' % pid)
        finally:
            sftp.close()
            client.close()
    except UploadCancelled:
        # Staff-requested stop — not an error. No .upload_error marker
        # (the queue row is already CANCELLED_BY_USER and nobody polls);
        # the partial remote tree is cleaned up by the rollback's
        # clean_up_sftp step.
        logger.info('move_to_sftp: upload for %s cancelled by staff request', pid)
        try:
            os.remove(cancel_flag)
        except OSError:
            pass
    except Exception as e:  # noqa: BLE001
        logger.error('move_to_sftp background put failed for %s: %s', pid, e)
        try:
            with open(marker, 'w') as fh:
                fh.write(str(e)[:1000])
        except OSError as marker_err:
            logger.error('could not write upload-error marker for %s: %s', pid, marker_err)
    finally:
        with _sftp_puts_lock:
            _sftp_puts_in_flight.discard(pid)
        os.close(put_lock_fd)  # drops the flock


def move_to_sftp(pid):
    """Kick off the recursive SFTP put in a BACKGROUND thread; return at once.

    Fire-and-don't-wait: the caller gets a response immediately and the
    put runs for minutes to ~30 min. Progress and completion are observed
    only through check_sftp — remote file count reaching the local count,
    or remote_package_size_bytes against local_package_size_bytes for a
    byte-accurate %. Failure surfaces through the .upload_error marker.

    Single-flight per package: if this process already has a put running
    for `pid`, no second put is started.

    @param: pid
    @returns: Dictionary (message='upload_started'|'upload_already_running')
    """
    with _sftp_puts_lock:
        if pid in _sftp_puts_in_flight:
            logger.info('move_to_sftp: put already in flight for %s — not starting another', pid)
            return dict(message='upload_already_running')
        _sftp_puts_in_flight.add(pid)
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
            # The remote package dir doesn't exist yet (the background put
            # hasn't created it). Report 0 files arrived — in_progress — so
            # the poller keeps waiting instead of seeing a 500.
            del file_names[:]
        remote_file_count = len(file_names)

        # Byte sums for the upload %: the Node side divides remote by local.
        remote_package_size_bytes = _sftp_dir_size(sftp, remote_package)
        remote_package_size = _human_size(remote_package_size_bytes)

        # Local total for the same uuid staging dir. 0 when the dir is
        # gone or unreadable — the caller reads that as "unknown total"
        # and falls back to the file-count readout.
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

    Symlinks are skipped. Returns 0 if the path is missing or unreadable;
    callers treat a 0 total as "unknown".
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

    Short ASCII, e.g. "8B", "12K", "3.4M", "1.2G"; one decimal place at
    >= 1 KiB; "0" for zero.
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

    Uploads only 002-ingest/<uuid>/, to keys `<folder-without-new_>/<rel>`.
    No local `003-ingested/` copy is written — the Wasabi batch archive is
    the sole batch-snapshot custodian (preservation redundancy lives in the
    AIP chain: Wasabi aip-store + DuraCloud + AM storage).

    INVARIANT: the staging copy is removed ONLY after a per-file verified
    upload (head_object size check in wasabi.upload_directory). On any S3
    failure the source is left in place and the caller records a FAILED
    archive_to_wasabi job; re-running the archive is the remedy.

    @param: uuid    directory name in 002-ingest (the collection pid)
    @param: folder  batch folder name (used, minus `new_`, as the S3 prefix)
    @returns: Dictionary {result, errors}:
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

    # Re-open the 001-ready batch folder's permissions so staff can keep
    # adding packages to an in-progress collection. Best-effort: the folder
    # may already be gone, and a chgrp/chmod failure is logged and ignored.
    reset_permissions(folder)

    try:
        move_result = move_to_s3(source, folder.replace('new_', ''))
        if move_result != 0:
            errors.append('ERROR: Unable to move packages to wasabi s3')
        else:
            # Upload verified per file — only now is the staging copy
            # safe to remove.
            shutil.rmtree(source)
            result = 'packages_moved_to_ingested_folder'
    except Exception as e:
        logger.error('move_to_ingested: %s', e)
        errors.append('ERROR: Unable to archive packages to wasabi s3 (move_to_ingested)')

    return dict(result=result, errors=errors)


def reset_permissions(folder):
    """
    Restores staff group access on a 001-ready batch folder so staff can
    keep adding packages to an in-progress collection.

    Recursively sets the group to GID (the shared staff group, e.g.
    `domain users`) and grants group rwX. The owner is deliberately left
    untouched: batches are created by different individual staff accounts,
    so the pre-2026-08 `chown -R UID:GID` here flattened every batch to
    one fixed owner (and with the template placeholder 1001:1001, to an
    id that maps to no account at all). UID is no longer read.

    Privilege note: chgrp on files the service user does not own needs
    CAP_CHOWN; on its own files it needs membership in GID (e.g.
    `SupplementaryGroups=` in the systemd unit).

    Best-effort: the folder may already be gone, and any failure is
    logged and ignored — _run logs non-zero exits at WARNING.

    @param: folder
    :returns: String status message
    """

    message = 'Permissions changed'

    if not gid:
        logger.warning('reset_permissions: GID not configured; skipping')
        return 'Unable to reset permissions'

    try:
        target = ready_path + folder
        _run(['chgrp', '-R', '--', gid, target])
        _run(['chmod', '-R', '--', 'g+rwX', target])
    except Exception as e:
        logger.info(e)
        message = 'Unable to reset permissions'

    return message


def move_to_s3(source, folder):
    """
    Upload a local directory tree to the configured Wasabi S3 bucket
    (lib/wasabi.py, using WASABI_PROFILE / WASABI_ENDPOINT / WASABI_BUCKET).

    Each upload logs start, per-file size + key, 25/50/75/100% milestones
    for large files, and an END summary with elapsed_ms and byte total:
    `journalctl -u curation-service | grep wasabi`.

    @param  source — local directory to upload (recursive walk).
    @param  folder — S3 key prefix segment for this batch.
    @returns int   — 0 on full success, 1 on any per-file failure or
                     transport error. Callers gate cleanup on this.
    """
    try:
        result = wasabi.upload_directory(source, folder)
    except RuntimeError as e:
        # Config-level error (missing WASABI_PROFILE etc) — return 1 rather
        # than raising, so the route handler reports it in errors[].
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

    Idempotent: a package path that is already gone is a no-op.

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

        # Already cleaned up, or never uploaded — nothing to do.
        try:
            sftp.stat(pkg_path)
        except IOError:
            logger.info(
                'clean_up_sftp: nothing to remove pid=%s package=%s',
                pid, archival_package,
            )
            return

        # Recursive remove of the package tree (files + subdirs +
        # the package dir itself).
        _sftp_rmtree(sftp, pkg_path)

        # Best-effort prune of the empty <pid> parent. sftp.rmdir raises
        # IOError while sibling packages remain — leave the parent for
        # them.
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
