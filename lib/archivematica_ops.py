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
"""

import logging
import os
import posixpath
import shutil
import stat
import threading

import paramiko

import config

logger = logging.getLogger(__name__)

# Re-export config values under their legacy names so existing function bodies
# continue to work without per-line edits.
ready_path = config.READY_PATH
ingest_path = config.INGEST_PATH
ingested_path = config.INGESTED_PATH
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


# --- paramiko helpers --

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
    """Run a shell command over SSH and return decoded stdout."""
    stdin, stdout, stderr = client.exec_command(command)
    return stdout.read().decode('utf-8', errors='replace')

# ---------------------------------------------------------------------------


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

    [os.remove(ready_path + folder + '/' + f) for f in os.listdir(ready_path + folder) if f.startswith('.')]
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]
    return packages


def check_package_names(folder):
    """
    Checks package names and fixes case issues and removes spaces
    @param: folder
    @returns: Dictionary
    """

    threads = []
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]
    [os.remove(ready_path + folder + '/' + f) for f in os.listdir(ready_path + folder) if f.startswith('.')]
    errors = []

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

    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]
    threads = []
    files_arr = []
    errors = []

    try:
        if os.path.exists(errors_file):
            os.remove(errors_file)
    except Exception as e:
        logger.info(e)
        logger.info('Unable to delete errors_file')

    for i in packages:

        thread = threading.Thread(target=check_file_names_threads, args=(folder, i))
        threads.append(thread)
        thread.start()

        # Get total file count from packages
        package = ready_path + folder + '/' + i + '/'
        files = [f for f in os.listdir(package) if not f.startswith('.')]
        [os.remove(package + f) for f in os.listdir(package) if f.startswith('.')]

        if len(files) < 2:
            errors.append(i + '  is missing files.')

        for j in files:
            files_arr.append(j)

        local_file_count = len(files_arr)

    for thread in threads:
        thread.join()

    try:
        with open(errors_file) as file_errors:
            errors = file_errors.readlines()
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
    packages = [f for f in os.listdir(ready_path + folder) if not f.startswith('.')]

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
    Moves folder from ready to ingest folder and renames it using pid
    @param: pid
    @param: folder
    @returns: Dictionary
    '''

    errors = []
    mode = 0o777

    # create collection uuid folder in 002-ingest folder
    try:
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


def move_to_sftp(pid):
    """"
    Moves folder to Archivematica sftp via ssh
    @param: pid
    @returns: void
    """

    errors = []

    client, sftp = _open_sftp()
    try:
        _sftp_put_r(sftp, ingest_path, sftp_path, preserve_mtime=True)
        packages = sftp.listdir(sftp_path)

        if pid not in packages:
            errors.append(-1)
    finally:
        sftp.close()
        client.close()


def check_sftp(uuid, local_file_count):
    """
    checks upload status on archivematica sftp
    @param: pid
    @param: local_file_count
    @returns: Dictionary
    """

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
        _sftp_walk(sftp, remote_package, on_file=store_files_name, on_dir=store_dir_name, on_other=store_other_file_types)
        remote_file_count = len(file_names)

        # `du` runs in the SSH session's default cwd; cd into the package first.
        # NOTE: uuid is interpolated into a shell command as in the legacy code.
        # Caller (ingest service) sends sanitized UUIDs; harden in a follow-up if needed.
        du_output = _ssh_exec(client, 'cd ' + remote_package + ' && du -h -s')
        remote_package_size = du_output.strip().replace('\t', '')

        if int(local_file_count) == remote_file_count:
            return dict(message='upload_complete', data=[file_names, remote_file_count])

        return dict(message='in_progress', file_names=file_names, remote_file_count=remote_file_count,
                    local_file_count=local_file_count,
                    remote_package_size=remote_package_size)
    finally:
        sftp.close()
        client.close()


def move_to_ingested(uuid, folder):
    """
    Moves packages to ingested folder and Wasabi S3 bucket
    @param: pid
    @param: folder
    @returns: Dictionary
    """

    errors = []
    ingested = ingested_path + folder.replace('new_', '')
    exists = os.path.isdir(ingested)
    result = 'packages_not_moved_to_ingested_folder'

    if exists:

        reset_permissions(folder)

        try:  # move only files because collection folder already exists
            file_names = [f for f in os.listdir(ingest_path + uuid) if not f.startswith('.')]

            for file_name in file_names:
                os.system('cp -R ' + os.path.join(ingest_path + uuid, file_name) + ' ' + ingested)

            source = ingest_path + uuid + '/'
            move_result = move_to_s3(source, folder.replace('new_', ''))
            if move_result == 1:
                errors.append('ERROR: Unable to move packages to wasabi s3')
            else:
                shutil.rmtree(source)
        except Exception as e:
            logger.info(e)
            return errors.append('ERROR: Unable to move files to ingested folder (move_to_ingested)')

    else:  # move entire folder

        try:
            shutil.move(ingest_path + uuid, ingest_path + folder.replace('new_', ''))
            os.system('cp -R ' + ingest_path + folder.replace('new_', '') + ' ' + ingested)
            source = ingest_path
            move_result = move_to_s3(source, '')
            if move_result == 1:
                errors.append('ERROR: Unable to move packages to wasabi s3')
            else:
                shutil.rmtree(ingest_path + folder.replace('new_', ''))
            
            os.system('rm -R ' + ingest_path + folder)
        except Exception as e:
            logger.info(e)
            return errors.append('ERROR: Unable to move folder (move_to_ingested)')

    if len(errors) == 0:
        try:
            logger.info('delete collection file after batch is complete')
            # deletes file
            # os.remove('collection')
        except Exception as e:
            logger.info(e)
            logger.info('collection file not found')

        try:
            #clean_up_sftp(uuid)
            result = 'packages_moved_to_ingested_folder'
        except Exception as e:
            logger.info(e)
            logger.info('unable to run clean up sftp function')

    return dict(result=result, errors=errors)


def reset_permissions(folder):
    """
    Resets ready folder permissions so that staff is able to add more packages
    @param: folder
    :returns: void
    """

    message = 'Permissions changed'

    try:
        cmd = 'chown -R ' + uid + ':' + gid + ' ' + ready_path + folder
        os.system(cmd)
    except Exception as e:
        logger.info(e)
        message = 'Unable to reset permissions'

    return message


def move_to_s3(source, folder):
    """
    Moves packages to Wasabi S3 bucket
    @param: source
    @param: folder
    @returns: void
    """

    errors = []
    aws_exec = '/usr/local/bin/aws s3 cp'
    aws_endpoint = '--endpoint-url=' + wasabi_endpoint
    aws_bucket = wasabi_bucket
    aws_args = '--recursive --profile ' + wasabi_profile
    result = 1

    if folder != '':
        try:
            aws_cmd = aws_exec + ' ' + source + ' ' + aws_endpoint + ' ' + aws_bucket + folder + ' ' + aws_args
            result = os.system(aws_cmd)
        except Exception as e:
            logger.info(e)
            errors.append('error')
    else:
        try:
            aws_cmd = aws_exec + ' ' + source + ' ' + aws_endpoint + ' ' + aws_bucket + ' ' + aws_args
            result = os.system(aws_cmd)
        except Exception as e:
            logger.info(e)
            errors.append('error')

    return result


def clean_up_sftp(pid, archival_package):
    """
    Deletes collection folder from ingest folder and sftp server.
    :param pid
    :param archival_package
    :return void
    """

    logger.info('clean_up_sftp pid=%s archival_package=%s', pid, archival_package)

    client, sftp = _open_sftp()
    try:
        # paramiko.exec_command runs each call in its own shell, so the
        # legacy 3-call sequence (cd path; cd pid; rm pkg/*) needs to be one chained command.
        target = sftp_path + '/' + pid
        _ssh_exec(client, 'cd ' + target + ' && rm ' + archival_package + '/*')
    finally:
        sftp.close()
        client.close()

