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

import os
import logging
from pathlib import Path
from typing import List, Set, Optional

# Configure logging
logger = logging.getLogger(__name__)


def get_workspace_ready_folders(root_dir, target_filename='uri.txt'):
    """
    Gets batch folders where ALL packages are missing uri.txt files.

    Scans the workspace directory (root_dir) and returns names of batch folders
    where ALL package subdirectories are missing the target file.

    IMPORTANT: Only returns batches where NONE of the packages have been processed yet.
    If even one package in a batch has uri.txt, that batch is excluded from results.

    Only checks immediate subdirectories (packages) within each batch folder.
    Does not check nested subdirectories within packages.

    Directory Structure:
        root_dir/                    ← Pass this as root_dir (WORKSPACE)
            batch1/                  ← RETURNED (all packages missing uri.txt)
                package1/            ← Checked for uri.txt
                    file1.tif        ← No uri.txt ✗
                package2/            ← Checked for uri.txt
                    file2.tif        ← No uri.txt ✗
            batch2/                  ← NOT RETURNED (partially processed)
                package3/            ← Checked for uri.txt
                    uri.txt          ← Has uri.txt ✓
                package4/            ← Checked for uri.txt
                    file4.tif        ← No uri.txt ✗
            batch3/                  ← NOT RETURNED (all packages processed)
                package5/            ← Checked for uri.txt
                    uri.txt          ← Has uri.txt ✓

    Example:
        root_dir = '/storage/001-ready/'
        Returns: ['batch1'] (only batch1 has ALL packages without uri.txt)

    Exclusions:
        - Empty folders (no packages)
        - Folders with only files (no packages)
        - Folders named 'ready' (case-insensitive)
        - Hidden folders (starting with '.')
        - Batch folders where ANY package has uri.txt (partially/fully processed)

    @param root_dir: Root directory path to scan (should be WORKSPACE env var value)
    @param target_filename: Target file to check for in packages (default: 'uri.txt')
    @return: Sorted list of batch folder names where ALL packages are missing target file
    @raises ValueError: If root_dir is invalid
    @raises PermissionError: If insufficient permissions to access directory
    """

    # Input validation
    if not root_dir:
        logger.error('root_dir parameter is required')
        raise ValueError('root_dir cannot be empty or None')

    if not isinstance(root_dir, (str, Path)):
        logger.error(f'root_dir must be string or Path, got {type(root_dir)}')
        raise TypeError(f'root_dir must be string or Path, got {type(root_dir)}')

    if not target_filename or not isinstance(target_filename, str):
        logger.error('target_filename must be a non-empty string')
        raise ValueError('target_filename must be a non-empty string')

    # Validate filename doesn't contain path separators (security)
    if os.sep in target_filename or '/' in target_filename or '\\' in target_filename:
        logger.error(f'target_filename contains invalid path separators: {target_filename}')
        raise ValueError('target_filename cannot contain path separators')

    # Convert to Path object for better cross-platform handling
    root_path = Path(root_dir).resolve()

    logger.debug(f'Scanning root directory: {root_path}')

    # Validate root directory exists and is accessible
    if not root_path.exists():
        logger.error(f'Root directory does not exist: {root_path}')
        raise FileNotFoundError(f'Root directory does not exist: {root_path}')

    if not root_path.is_dir():
        logger.error(f'Root path is not a directory: {root_path}')
        raise NotADirectoryError(f'Root path is not a directory: {root_path}')

    # Check read permissions
    if not os.access(root_path, os.R_OK):
        logger.error(f'Insufficient permissions to read directory: {root_path}')
        raise PermissionError(f'Insufficient permissions to read directory: {root_path}')

    try:
        missing_parents = _scan_for_missing_files(root_path, target_filename)
        result = sorted(missing_parents)

        logger.info(f'Found {len(result)} parent folders with missing {target_filename}: {result}')
        return result

    except PermissionError as e:
        logger.error(f'Permission denied while scanning directory: {e}')
        raise
    except OSError as e:
        logger.error(f'OS error while scanning directory: {e}', exc_info=True)
        raise
    except Exception as e:
        logger.error(f'Unexpected error in get_workspace_ready_folders: {e}', exc_info=True)
        raise


def _scan_for_missing_files(root_path: Path, target_filename: str) -> Set[str]:
    """
    Internal function to scan batch folders for packages missing uri.txt files.

    Returns batch folder names where ALL packages are missing the target file.
    If even one package has the target file, that batch folder is excluded.

    Structure expected:
        root_path/                  ← WORKSPACE directory
            batch1/                 ← RETURNED (all packages missing uri.txt)
                package1/           ← Package (checked for uri.txt)
                    file1.tif       ← No uri.txt ✗
                package2/           ← Package (checked for uri.txt)
                    file2.tif       ← No uri.txt ✗
            batch2/                 ← NOT RETURNED (has mixed processing)
                package3/           ← Package (checked for uri.txt)
                    uri.txt         ← Has uri.txt ✓
                package4/           ← Package (checked for uri.txt)
                    file4.tif       ← No uri.txt ✗
            batch3/                 ← NOT RETURNED (all packages have uri.txt)
                package5/
                    uri.txt         ← Has uri.txt ✓
                package6/
                    uri.txt         ← Has uri.txt ✓

    @param root_path: Root Path object to scan (WORKSPACE directory)
    @param target_filename: Target filename to check for (default: 'uri.txt')
    @return: Set of batch folder names where ALL packages are missing target file
    """
    batches_all_missing = set()
    root_path_resolved = root_path.resolve()

    logger.debug(f'Starting scan from: {root_path_resolved}')

    try:
        # Iterate through batch folders (first-level subdirectories in workspace)
        for batch_folder in root_path_resolved.iterdir():
            # Skip if not a directory or is hidden
            if not batch_folder.is_dir() or batch_folder.name.startswith('.'):
                logger.debug(f'Skipping non-directory or hidden: {batch_folder.name}')
                continue

            # Skip folders named 'ready' (case-insensitive)
            if batch_folder.name.lower() == 'ready':
                logger.debug(f'Skipping "ready" folder: {batch_folder.name}')
                continue

            logger.debug(f'Checking batch folder: {batch_folder.name}')

            # Get all package directories (second-level subdirectories)
            try:
                packages = [
                    item for item in batch_folder.iterdir()
                    if item.is_dir() and not item.name.startswith('.')
                ]
            except PermissionError:
                logger.warning(f'Permission denied accessing batch folder: {batch_folder.name}')
                continue

            # Skip if no packages found
            if not packages:
                logger.debug(f'No packages found in batch folder: {batch_folder.name}')
                continue

            logger.debug(f'Found {len(packages)} package(s) in {batch_folder.name}')

            # Check each package for uri.txt
            packages_with_uri = 0
            packages_without_uri = 0
            all_packages_missing_uri = True

            for package in packages:
                uri_file_path = package / target_filename

                if uri_file_path.exists():
                    packages_with_uri += 1
                    all_packages_missing_uri = False
                    logger.debug(f'  - {package.name}: has {target_filename}')
                else:
                    packages_without_uri += 1
                    logger.debug(f'  - {package.name}: MISSING {target_filename}')

            # Only add batch if ALL packages are missing uri.txt
            if all_packages_missing_uri:
                batches_all_missing.add(batch_folder.name)
                logger.info(
                    f'Batch folder "{batch_folder.name}": ALL {len(packages)} packages missing {target_filename} ✓')
            elif packages_without_uri > 0:
                logger.info(
                    f'Batch folder "{batch_folder.name}": Partially processed ({packages_with_uri}/{len(packages)} have {target_filename}) - SKIPPED')
            else:
                logger.debug(
                    f'Batch folder "{batch_folder.name}": All {len(packages)} packages have {target_filename} - SKIPPED')

        logger.info(
            f'Scan complete. Found {len(batches_all_missing)} batch folder(s) where ALL packages are missing {target_filename}')
        return batches_all_missing

    except PermissionError as e:
        logger.warning(f'Permission denied accessing directory: {e}')
        return batches_all_missing
    except Exception as e:
        logger.error(f'Error during scan: {str(e)}', exc_info=True)
        return batches_all_missing


def get_workspace_ready_folders_safe(root_dir, target_filename='uri.txt'):
    """
    Safe wrapper for get_workspace_ready_folders that returns empty list on errors.
    Use this version if you want to handle errors gracefully without exceptions.

    @param root_dir: Root directory path to scan
    @param target_filename: Target file to check for (default: 'uri.txt')
    @return: Sorted list of parent folder names, or empty list on error
    """
    try:
        return get_workspace_ready_folders(root_dir, target_filename)
    except Exception as e:
        logger.error(f'Error in get_workspace_ready_folders_safe: {e}')
        return []


def get_metadata_ready_folders(root_dir, target_filename='uri.txt'):
    """
    Gets batch folders that HAVE been processed (i.e., have uri.txt file).
    This is the opposite of get_workspace_ready_folders.

    The scan is bounded to exactly the expected layout —
    <root>/<batch>/<package>/uri.txt — so a uri.txt at any other depth
    cannot report a phantom batch name.

    @param root_dir: Root directory path to scan
    @param target_filename: Target file to check for (default: 'uri.txt')
    @return: Sorted list of batch folder names
    """
    processed = set()
    root_path = Path(root_dir)

    for batch in root_path.iterdir():
        if not batch.is_dir() or batch.name.startswith('.'):
            continue
        if batch.name.lower() == 'ready':
            continue
        try:
            for package in batch.iterdir():
                if not package.is_dir() or package.name.startswith('.'):
                    continue
                if (package / target_filename).is_file():
                    processed.add(batch.name)
                    break
        except PermissionError:
            logger.warning(f'Permission denied scanning batch folder: {batch}')
            continue

    return sorted(processed)


def check_uri_txt(folder):
    """
    Checks for missing uri.txt files in packages within a specified folder.
    Scans all subdirectories (packages) in the given folder and identifies
    which ones are missing the required uri.txt file.

    IMPORTANT: The 'folder' parameter should be RELATIVE to WORKSPACE.
    The function will automatically prepend WORKSPACE environment variable.

    Example:
        WORKSPACE = '/jetbrains/PycharmProjects/storage/001-ready/'
        folder = 'batch1'  ← Just the folder name, NOT the full path
        Result: Checks /jetbrains/PycharmProjects/storage/001-ready/batch1/

    @param folder: Folder name relative to WORKSPACE environment variable
    @return: Dictionary with 'result' message and 'errors' list
    @raises ValueError: If folder parameter is invalid
    @raises FileNotFoundError: If workspace or folder doesn't exist
    @raises PermissionError: If insufficient permissions to access paths
    """

    try:
        # Validate and get workspace path
        workspace_path = os.getenv('WORKSPACE')
        if not workspace_path:
            logger.error('WORKSPACE environment variable not configured')
            raise ValueError('WORKSPACE environment variable not set')

        # Validate folder parameter
        if not folder or not isinstance(folder, str):
            logger.error('Invalid folder parameter')
            raise ValueError('folder parameter must be a non-empty string')

        # Sanitize and validate folder path
        folder_clean = folder.strip()

        # Remove leading/trailing slashes to prevent path issues
        folder_clean = folder_clean.strip('/').strip('\\')

        logger.debug(f'WORKSPACE: {workspace_path}')
        logger.debug(f'Folder (cleaned): {folder_clean}')

        validation_error = _validate_folder_path_parameter(folder_clean)
        if validation_error:
            logger.error(f'Invalid folder path: {validation_error}')
            raise ValueError(f'Invalid folder path: {validation_error}')

        # Build and validate complete folder path
        workspace_root = Path(workspace_path).resolve()
        target_folder_path = (workspace_root / folder_clean).resolve()

        logger.debug(f'Resolved workspace: {workspace_root}')
        logger.debug(f'Resolved target folder: {target_folder_path}')

        # Security: Prevent directory traversal
        try:
            target_folder_path.relative_to(workspace_root)
        except ValueError:
            logger.error(f'Directory traversal attempt detected: {folder}')
            raise ValueError('Access denied: Path traversal detected')

        # Validate folder exists and is accessible
        if not target_folder_path.exists():
            logger.error(f'Folder does not exist: {target_folder_path}')
            raise FileNotFoundError(f'Folder does not exist: {folder}')

        if not target_folder_path.is_dir():
            logger.error(f'Path is not a directory: {target_folder_path}')
            raise NotADirectoryError(f'Path is not a directory: {folder}')

        if not os.access(target_folder_path, os.R_OK):
            logger.error(f'Insufficient permissions to read folder: {target_folder_path}')
            raise PermissionError(f'Insufficient permissions to read folder: {folder}')

        # Check for uri.txt files
        errors = _scan_packages_for_uri_txt(target_folder_path)

        # Prepare result
        if errors:
            result_message = f'URI txt check completed. Found {len(errors)} package(s) with missing uri.txt files'
            logger.warning(result_message)
        else:
            result_message = 'URI txt files checked. All packages have uri.txt files'
            logger.info(result_message)

        return {
            'result': result_message,
            'errors': errors
        }

    except (ValueError, FileNotFoundError, NotADirectoryError, PermissionError) as e:
        # Re-raise validation and permission errors for caller to handle
        raise

    except OSError as e:
        logger.error(f'OS error in check_uri_txt: {str(e)}', exc_info=True)
        return {
            'result': 'Error checking URI txt files',
            'errors': [f'System error: Unable to access folder']
        }

    except Exception as e:
        logger.error(f'Unexpected error in check_uri_txt: {str(e)}', exc_info=True)
        return {
            'result': 'Error checking URI txt files',
            'errors': [f'Unexpected error occurred']
        }


def _validate_folder_path_parameter(folder: str) -> Optional[str]:
    """
    Validates the folder parameter for security issues.

    @param folder: Folder path to validate
    @return: Error message string if invalid, None if valid
    """
    # Check for empty or whitespace-only
    if not folder or folder.isspace():
        return 'Folder path cannot be empty'

    # Check length limits
    if len(folder) > 512:
        return 'Folder path exceeds maximum length (512 characters)'

    # Check for null bytes (security)
    if '\x00' in folder:
        return 'Folder path contains invalid null bytes'

    # Check for directory traversal patterns
    dangerous_patterns = ['..', '~']
    for pattern in dangerous_patterns:
        if pattern in folder:
            return f'Folder path contains forbidden pattern: {pattern}'

    # Check for absolute paths (after stripping slashes, this shouldn't happen)
    if folder.startswith('/') or folder.startswith('\\'):
        return 'Absolute paths are not allowed'

    # Check for Windows drive letters
    if len(folder) >= 2 and folder[1] == ':':
        return 'Drive letters are not allowed'

    return None


def _scan_packages_for_uri_txt(target_folder_path: Path) -> List[str]:
    """
    Scans all packages (subdirectories) in the target folder for uri.txt files.

    @param target_folder_path: Path object pointing to folder to scan
    @return: List of error messages for packages missing uri.txt
    """
    errors = []

    try:
        # Get all items in the target folder
        try:
            all_items = list(target_folder_path.iterdir())
        except PermissionError as e:
            logger.error(f'Permission denied accessing folder: {target_folder_path}')
            errors.append('Permission denied accessing folder')
            return errors

        # Filter to get only directories (packages), excluding hidden files/folders
        packages = [
            item for item in all_items
            if item.is_dir() and not item.name.startswith('.')
        ]

        # Handle empty packages list
        if not packages:
            logger.info(f'No packages found in folder: {target_folder_path}')
            errors.append('No packages found in folder')
            return errors

        logger.info(f'Scanning {len(packages)} package(s) for uri.txt files')

        # Check each package for uri.txt
        for package_path in packages:
            try:
                # Get files in the package directory
                package_files = list(package_path.iterdir())
                file_names = [
                    f.name for f in package_files
                    if f.is_file() and not f.name.startswith('.')
                ]

                # Check if uri.txt exists
                if 'uri.txt' not in file_names:
                    error_msg = f'{package_path.name} is missing a uri.txt file'
                    errors.append(error_msg)
                    logger.debug(error_msg)

            except PermissionError:
                error_msg = f'{package_path.name} - Permission denied'
                errors.append(error_msg)
                logger.warning(f'Permission denied accessing package: {package_path}')
                continue

            except OSError as e:
                error_msg = f'{package_path.name} - Error accessing package'
                errors.append(error_msg)
                logger.warning(f'OS error accessing package {package_path}: {str(e)}')
                continue

        return errors

    except Exception as e:
        logger.error(f'Error scanning packages: {str(e)}', exc_info=True)
        errors.append('Error scanning packages for uri.txt files')
        return errors


def check_uri_txt_safe(folder):
    """
    Safe wrapper for check_uri_txt that returns a result even on exceptions.
    Use this version if you want to handle errors gracefully without raising exceptions.

    @param folder: Folder path relative to WORKSPACE environment variable
    @return: Dictionary with 'result' message and 'errors' list
    """
    try:
        return check_uri_txt(folder)
    except ValueError as e:
        logger.error(f'Validation error in check_uri_txt_safe: {str(e)}')
        return {
            'result': 'Validation error',
            'errors': [str(e)]
        }
    except (FileNotFoundError, NotADirectoryError) as e:
        logger.error(f'Path error in check_uri_txt_safe: {str(e)}')
        return {
            'result': 'Path error',
            'errors': [str(e)]
        }
    except PermissionError as e:
        logger.error(f'Permission error in check_uri_txt_safe: {str(e)}')
        return {
            'result': 'Permission error',
            'errors': ['Insufficient permissions to access folder']
        }
    except Exception as e:
        logger.error(f'Unexpected error in check_uri_txt_safe: {str(e)}', exc_info=True)
        return {
            'result': 'Error checking URI txt files',
            'errors': ['Unexpected error occurred']
        }