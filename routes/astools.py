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
ArchivesSpace metadata routes (legacy: astools-web_v2).

URL prefix preserved as /api/v1/astools/ so the ingest service does not need
code changes during cutover. Helpers (`_validate_*`, `_build_cli_arguments`,
`_write_serialized_files`) stay co-located with the make_digital_objects
endpoint that owns them.
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request

from auth import require_api_key_astools
from lib import archivesspace_ops as qa_lib

logger = logging.getLogger(__name__)

astools_bp = Blueprint('astools', __name__)


@astools_bp.route('/api/v1/astools/workspace', methods=['GET'])
@require_api_key_astools
def get_workspace_packages():
    """
    Gets batch folders that have not been processed yet (no uri.txt files).

    @return: JSON response with list of folder names and errors
    """
    try:
        logger.info('GET /api/v1/astools/workspace - Retrieving unprocessed packages')

        workspace = os.getenv('WORKSPACE')

        if not workspace:
            logger.error('WORKSPACE environment variable not configured')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500

        # Get workspace packages (folders needing processing)
        make_digital_objects_results = qa_lib.get_workspace_ready_folders(workspace)

        logger.info(f'Found {len(make_digital_objects_results)} folders needing processing')
        return jsonify({
            'result': make_digital_objects_results,
            'errors': []
        }), 200

    except AttributeError as e:
        logger.error(f'Attribute error in get_workspace_packages: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500

    except IOError as e:
        logger.error(f'IO error in get_workspace_packages: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500

    except Exception as e:
        logger.error(f'Unexpected error in get_workspace_packages: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500


@astools_bp.route('/api/v1/astools/workspace/packages', methods=['GET'])
@require_api_key_astools
def get_packages():
    """
    Gets package list in a specific batch folder.

    @param batch: Batch folder name (relative to WORKSPACE) - preferred parameter
    @return: JSON response with package list
    """
    try:
        logger.info('GET /api/v1/astools/workspace/packages - Retrieving packages')

        # Support 'batch' parameter
        batch = request.args.get('batch', '').strip()

        workspace = os.getenv('WORKSPACE')

        if not workspace:
            logger.error('WORKSPACE environment variable not configured')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500

        # Validate batch parameter
        if not batch:
            logger.warning('Missing batch parameter')
            return jsonify({
                'result': None,
                'errors': ['Bad request: Missing batch parameter']
            }), 400

        # Validate and resolve batch path
        batch_path, path_error = _validate_and_resolve_path(batch, workspace)
        if path_error:
            logger.warning(f'Invalid batch path: {path_error}')
            return jsonify({
                'result': None,
                'errors': [path_error]
            }), 403

        # Check if batch folder exists
        if not batch_path.exists():
            logger.warning(f'Batch folder not found: {batch}')
            return jsonify({
                'result': None,
                'errors': [f'Batch folder not found: {batch}']
            }), 404

        if not batch_path.is_dir():
            logger.warning(f'Batch path is not a directory: {batch}')
            return jsonify({
                'result': None,
                'errors': [f'Batch path is not a directory: {batch}']
            }), 400

        # Get packages (subdirectories) in batch folder
        packages = [
            item.name for item in batch_path.iterdir()
            if item.is_dir() and not item.name.startswith('.')
        ]

        logger.info(f'Found {len(packages)} packages in batch: {batch}')
        return jsonify({
            'result': sorted(packages),
            'errors': []
        }), 200

    except PermissionError as e:
        logger.error(f'Permission error: {str(e)}')
        return jsonify({
            'result': None,
            'errors': ['Permission denied accessing folder']
        }), 500

    except Exception as e:
        logger.error(f'Unexpected error in get_packages: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500


@astools_bp.route('/api/v1/astools/workspace/packages/files', methods=['GET'])
@require_api_key_astools
def get_package_files():
    """
    Gets file list for all packages in a batch folder.

    @param batch: Batch folder name (relative to WORKSPACE) - preferred parameter
    @param package_name: Legacy parameter name (backward compatibility) - deprecated
    @return: JSON response with batch info and file lists
    """
    try:
        logger.info('GET /api/v1/astools/workspace/packages/files - Retrieving package files')

        # Support both 'batch' (new) and 'package_name' (old) for backward compatibility
        batch = request.args.get('batch', '').strip()
        if not batch:
            batch = request.args.get('package_name', '').strip()
            if batch:
                logger.info(f'Using legacy parameter "package_name". Please update to use "batch" parameter.')

        workspace = os.getenv('WORKSPACE')

        if not workspace:
            logger.error('WORKSPACE environment variable not configured')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500

        # Validate batch parameter
        if not batch:
            logger.warning('Missing batch parameter (tried both "batch" and "package_name")')
            return jsonify({
                'result': None,
                'errors': ['Bad request: Missing batch or package_name parameter']
            }), 400

        # Validate and resolve batch path
        batch_path, path_error = _validate_and_resolve_path(batch, workspace)
        if path_error:
            logger.warning(f'Invalid batch path: {path_error}')
            return jsonify({
                'result': None,
                'errors': [path_error]
            }), 403

        # Check if batch folder exists
        if not batch_path.exists():
            logger.warning(f'Batch folder not found: {batch}')
            return jsonify({
                'result': None,
                'errors': [f'Batch folder not found: {batch}']
            }), 404

        if not batch_path.is_dir():
            logger.warning(f'Batch path is not a directory: {batch}')
            return jsonify({
                'result': None,
                'errors': [f'Batch path is not a directory: {batch}']
            }), 400

        # Build response with batch info and files
        package_files = {
            'batch': batch,
            'packages': []
        }

        # Get all package directories
        packages = [
            item for item in batch_path.iterdir()
            if item.is_dir() and not item.name.startswith('.')
        ]

        logger.info(f'Processing {len(packages)} packages in batch: {batch}')

        # Get files for each package
        for package_dir in sorted(packages, key=lambda x: x.name):
            try:
                # Get files in package (exclude hidden and .txt files)
                files = [
                    f.name for f in package_dir.iterdir()
                    if f.is_file()
                       and not f.name.startswith('.')
                       and not f.name.endswith('.txt')
                ]

                package_files['packages'].append({
                    'package': package_dir.name,
                    'files': sorted(files)
                })

            except PermissionError:
                logger.warning(f'Permission denied accessing package: {package_dir.name}')
                package_files['packages'].append({
                    'package': package_dir.name,
                    'files': [],
                    'error': 'Permission denied'
                })
                continue

        logger.info(f'Retrieved files for {len(package_files["packages"])} packages')
        return jsonify({
            'result': package_files,
            'errors': []
        }), 200

    except PermissionError as e:
        logger.error(f'Permission error: {str(e)}')
        return jsonify({
            'result': None,
            'errors': ['Permission denied accessing folder']
        }), 500

    except Exception as e:
        logger.error(f'Unexpected error in get_package_files: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500


@astools_bp.route('/api/v1/astools/processed', methods=['GET'])
@require_api_key_astools
def get_processed_packages():
    """
    Gets batch folders that have been processed (contain uri.txt files).

    @return: JSON response with list of processed folder names
    """
    try:
        logger.info('GET /api/v1/astools/processed - Retrieving processed packages')

        workspace = os.getenv('WORKSPACE')

        if not workspace:
            logger.error('WORKSPACE environment variable not configured')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500

        # Get processed packages (folders with uri.txt)
        made_digital_objects_results = qa_lib.get_metadata_ready_folders(workspace)

        logger.info(f'Found {len(made_digital_objects_results)} processed folders')
        return jsonify({
            'result': made_digital_objects_results,
            'errors': []
        }), 200

    except Exception as e:
        logger.error(f'Unexpected error in get_processed_packages: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500


@astools_bp.route('/api/v1/astools/workspace/uri', methods=['GET'])
@require_api_key_astools
def get_uri_txt():
    """
    Gets ArchivesSpace URIs from uri.txt files in a package.

    @param folder: Batch folder name
    @param package: Package name within batch
    @return: JSON response with URIs
    """
    try:
        logger.info('GET /api/v1/astools/workspace/uri - Retrieving URIs')

        folder = request.args.get('folder', '').strip()
        package = request.args.get('package', '').strip()
        workspace = os.getenv('WORKSPACE')

        if not workspace:
            logger.error('WORKSPACE environment variable not configured')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500

        # Validate parameters
        if not folder or not package:
            logger.warning('Missing folder or package parameter')
            return jsonify({
                'result': None,
                'errors': ['Bad request: Missing folder or package parameter']
            }), 400

        # Build and validate path
        workspace_path = Path(workspace).resolve()
        package_path = (workspace_path / folder / package).resolve()

        # Security: Prevent directory traversal
        try:
            package_path.relative_to(workspace_path)
        except ValueError:
            logger.warning(f'Directory traversal attempt: {folder}/{package}')
            return jsonify({
                'result': None,
                'errors': ['Access denied: Path traversal detected']
            }), 403

        # Check if package exists
        if not package_path.exists() or not package_path.is_dir():
            logger.warning(f'Package not found: {folder}/{package}')
            return jsonify({
                'result': None,
                'errors': [f'Package not found: {folder}/{package}']
            }), 404

        # Get files and look for uri.txt
        uris = []
        files = [
            f.name for f in package_path.iterdir()
            if f.is_file() and not f.name.startswith('.')
        ]

        if 'uri.txt' in files:
            uri_file_path = package_path / 'uri.txt'
            try:
                with open(uri_file_path, 'r') as f:
                    uri_content = f.read().strip()
                    if uri_content:
                        uris.append(uri_content)
                        logger.info(f'Found URI in {folder}/{package}')
            except Exception as e:
                logger.error(f'Error reading uri.txt: {str(e)}')

        return jsonify({
            'result': {
                'uris': uris,
                'files': files
            },
            'errors': []
        }), 200

    except Exception as e:
        logger.error(f'Unexpected error in get_uri_txt: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500


@astools_bp.route('/api/v1/astools/check-uri-txt', methods=['GET'])
@require_api_key_astools
def check_uri_txt():
    """
    Runs QA process to check uri.txt files in a folder.

    @param folder: Folder name relative to WORKSPACE (query parameter)
    @return: JSON response with uri_results and errors

    Status Codes:
        200: Success
        400: Bad request (missing/invalid parameters)
        401: Unauthorized (invalid API key)
        403: Forbidden (path traversal attempt)
        404: Not found (folder doesn't exist)
        500: Internal server error
    """
    try:
        logger.info('GET /api/v1/astools/check-uri-txt - Running URI check')

        folder = request.args.get('folder', '').strip()
        workspace_root = os.getenv('WORKSPACE')

        if not workspace_root:
            logger.error('WORKSPACE environment variable not configured')
            return jsonify({
                'uri_results': None,
                'errors': ['Server configuration error']
            }), 500

        # Validate folder parameter
        if not folder:
            logger.warning('API request missing folder parameter')
            return jsonify({
                'uri_results': None,
                'errors': ['Bad request: Missing folder parameter']
            }), 400

        # Validate folder parameter (security checks)
        validation_error = _validate_folder_parameter(folder)
        if validation_error:
            logger.warning(f'Invalid folder parameter: {validation_error}')
            return jsonify({
                'uri_results': None,
                'errors': [validation_error]
            }), 400

        # Execute QA check using qa_lib
        # NOTE: qa_lib.check_uri_txt expects just the folder name,
        # it will prepend WORKSPACE automatically
        logger.info(f'Running uri.txt check for folder: {folder}')
        uri_results = qa_lib.check_uri_txt(folder)

        logger.info(f'Successfully completed uri.txt check for folder: {folder}')
        return jsonify({
            'uri_results': uri_results,
            'errors': []
        }), 200

    except ValueError as e:
        logger.error(f'Validation error: {str(e)}')
        return jsonify({
            'uri_results': None,
            'errors': [str(e)]
        }), 400

    except FileNotFoundError as e:
        logger.error(f'File not found: {str(e)}')
        return jsonify({
            'uri_results': None,
            'errors': [str(e)]
        }), 404

    except AttributeError as e:
        logger.error(f'Attribute error in check_uri_txt: {str(e)}', exc_info=True)
        return jsonify({
            'uri_results': None,
            'errors': ['Internal server error']
        }), 500

    except IOError as e:
        logger.error(f'IO error in check_uri_txt: {str(e)}', exc_info=True)
        return jsonify({
            'uri_results': None,
            'errors': ['Internal server error']
        }), 500

    except Exception as e:
        logger.error(f'Unexpected error in check_uri_txt: {str(e)}', exc_info=True)
        return jsonify({
            'uri_results': None,
            'errors': ['Internal server error']
        }), 500


@astools_bp.route('/api/v1/astools/move-to-ready', methods=['GET'])
@require_api_key_astools
def move_to_ready_():
    """
    Moves packages to ready folder.

    @param folder: Folder name to move
    @return: JSON response with result
    """
    try:
        logger.info('GET /api/v1/astools/move-to-ready - Moving folder')

        folder = request.args.get('folder', '').strip()

        # Validate folder parameter
        if not folder:
            logger.warning('Missing folder parameter')
            return jsonify({
                'result': None,
                'errors': ['Bad request: Missing folder parameter']
            }), 400

        # Move folder using qa_lib
        results = qa_lib.move_to_ready(folder)

        logger.info(f'Move operation completed: {results["result"]}')
        return jsonify(results), 200

    except Exception as e:
        logger.error(f'Unexpected error in move_to_ready: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500


# Helper Functions

def _validate_folder_parameter(folder: str) -> Optional[str]:
    """
    Validates the folder parameter for security issues.

    @param folder: Folder name to validate
    @return: Error message string if invalid, None if valid
    """
    # Check length limits
    if len(folder) > 255:
        return 'Folder name exceeds maximum length (255 characters)'

    # Check for null bytes (security)
    if '\x00' in folder:
        return 'Folder name contains invalid characters'

    # Check for directory traversal patterns
    dangerous_patterns = ['..', '~', '$', '`', '|', ';', '&', '>', '<', '*', '?']
    for pattern in dangerous_patterns:
        if pattern in folder:
            return f'Folder name contains forbidden pattern: {pattern}'

    # Check for absolute paths
    if folder.startswith('/') or folder.startswith('\\'):
        return 'Absolute paths are not allowed'

    # Check for Windows drive letters
    if len(folder) >= 2 and folder[1] == ':':
        return 'Drive letters are not allowed'

    # Validate allowed characters
    if not re.match(r'^[a-zA-Z0-9_\-\s\.\/]+$', folder):
        return 'Folder name contains invalid characters'

    return None


def _validate_and_resolve_path(folder: str, workspace_root: str) -> Tuple[Optional[Path], Optional[str]]:
    """
    Validates and resolves the folder path to prevent directory traversal attacks.

    @param folder: Folder name from user input
    @param workspace_root: Root workspace directory
    @return: Tuple of (resolved_path, error_message)
             Returns (Path object, None) if valid
             Returns (None, error_string) if invalid
    """
    try:
        # Validate folder parameter first
        validation_error = _validate_folder_parameter(folder)
        if validation_error:
            return None, validation_error

        # Convert to Path objects and resolve to absolute paths
        workspace_path = Path(workspace_root).resolve()
        folder_path = (workspace_path / folder).resolve()

        # Security: Ensure the resolved path is within workspace_root
        try:
            folder_path.relative_to(workspace_path)
        except ValueError:
            logger.warning(f'Directory traversal attempt detected: {folder}')
            return None, 'Access denied: Path traversal detected'

        return folder_path, None

    except Exception as e:
        logger.error(f'Error validating folder path: {str(e)}')
        return None, 'Invalid folder path'


def _validate_make_digital_objects_request(data: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Validates the request data for make_digital_objects endpoint.

    @param data: Request JSON data
    @return: Tuple of (validated_data, error_message)
             Returns (validated_dict, None) if valid
             Returns (None, error_string) if invalid
    """
    if not data:
        return None, 'Request body is required'

    # Extract nested data object
    request_data = data.get('data')
    if not request_data or not isinstance(request_data, dict):
        return None, 'Invalid request format: missing or invalid data object'

    # Extract and validate required fields
    folder = request_data.get('folder')
    if not folder or not isinstance(folder, str):
        return None, 'Invalid request: folder is required'

    folder = folder.strip()
    if not folder:
        return None, 'Invalid request: folder cannot be empty'

    # Validate is_kaltura (optional, defaults to 0)
    is_kaltura = request_data.get('is_kaltura', 0)
    if not isinstance(is_kaltura, (int, bool)):
        return None, 'Invalid request: is_kaltura must be a boolean or integer'
    is_kaltura = int(is_kaltura)

    # Validate packages (optional)
    packages = request_data.get('packages', [])
    if packages and not isinstance(packages, list):
        return None, 'Invalid request: packages must be an array'

    # Validate files (optional, required if is_kaltura is true)
    files = request_data.get('files', [])
    if files and not isinstance(files, list):
        return None, 'Invalid request: files must be an array'

    if is_kaltura and not files:
        return None, 'Invalid request: files are required when is_kaltura is enabled'

    # Note: File entry validation is handled by the CLI script
    # We pass through the files array as-is to maintain compatibility

    # Validate optional boolean flags
    no_kaltura = request_data.get('no_kaltura', False)
    no_caption = request_data.get('no_caption', True)
    no_publish = request_data.get('no_publish', False)
    use_test_server = request_data.get('test', False)
    verbose = request_data.get('verbose', False)

    return {
        'folder': folder,
        'is_kaltura': is_kaltura,
        'packages': packages,
        'files': files,
        'no_kaltura': bool(no_kaltura) if not is_kaltura else False,
        'no_caption': bool(no_caption),
        'no_publish': bool(no_publish),
        'use_test_server': bool(use_test_server),
        'verbose': bool(verbose)
    }, None


def _write_serialized_files(files: List[Dict], temp_dir: Path) -> Path:
    """
    Securely writes serialized files JSON to a temporary location.

    @param files: List of file dictionaries with Kaltura mappings
    @param temp_dir: Temporary directory path
    @return: Path to the written JSON file
    """
    serialized_path = temp_dir / 'serialized_files.json'

    with open(serialized_path, 'w', encoding='utf-8') as f:
        json.dump(files, f, indent=2)

    return serialized_path


def _build_cli_arguments(
    script_path: str,
    username: str,
    password: str,
    batch_path: str,
    validated_data: Dict,
    serialized_file_path: Optional[Path] = None
) -> List[str]:
    """
    Builds the CLI argument list for the make_digital_object.py script.

    @param script_path: Full path to the CLI script
    @param username: ArchivesSpace username
    @param password: ArchivesSpace password
    @param batch_path: Full path to the batch folder
    @param validated_data: Validated request data
    @param serialized_file_path: Path to serialized files JSON (optional)
    @return: List of command arguments
    """
    args = [
        sys.executable,  # use the venv's interpreter, not whatever 'python3' resolves to on PATH
        script_path,
        '-u', username,
        '-p', password,
        '--batch'  # Always run in batch mode for web API
    ]

    # Add optional flags based on validated data
    if validated_data.get('no_kaltura'):
        args.append('--no_kaltura_id')

    if validated_data.get('no_caption'):
        args.append('--no_caption')

    if validated_data.get('no_publish'):
        args.append('--no_publish')

    if validated_data.get('use_test_server'):
        args.append('--test')

    if validated_data.get('verbose'):
        args.append('--verbose')

    # Add the path as the final positional argument
    args.append(batch_path)

    return args


@astools_bp.route('/api/v1/astools/make-digital-objects', methods=['POST'])
@require_api_key_astools
def make_digital_objects():
    """
    Runs the make_digital_object CLI script to create digital objects in ArchivesSpace.

    Accepts a JSON payload with batch processing configuration and executes
    the CLI script with proper security measures.

    @body data: JSON object containing:
        - folder: Batch folder name (relative to WORKSPACE)
        - is_kaltura: Whether to process Kaltura IDs (0 or 1)
        - packages: List of package names (optional)
        - files: List of file objects with Kaltura mappings (optional)
        - no_caption: Skip caption processing (default: true)
        - no_publish: Do not publish components (default: false)
        - test: Use test ArchivesSpace server (default: false)
        - verbose: Enable verbose logging (default: false)
    @return: JSON response with processing results
    """
    temp_dir = None

    try:
        logger.info('POST /api/v1/astools/make-digital-objects - Processing request')

        # Validate environment configuration
        required_env_vars = {
            'ASPACE_USERNAME': os.getenv('ASPACE_USERNAME'),
            'ASPACE_PASSWORD': os.getenv('ASPACE_PASSWORD'),
            'WORKSPACE': os.getenv('WORKSPACE'),
            'SCRIPT_PATH': os.getenv('SCRIPT_PATH'),
            'SCRIPT_NAME_PY': os.getenv('SCRIPT_NAME_PY'),
            'LOG_PATH': os.getenv('LOG_PATH'),
        }

        missing_vars = [key for key, value in required_env_vars.items() if not value]
        if missing_vars:
            logger.error(f'Missing environment variables: {", ".join(missing_vars)}')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500

        # Extract validated environment variables
        username = required_env_vars['ASPACE_USERNAME']
        password = required_env_vars['ASPACE_PASSWORD']
        workspace = required_env_vars['WORKSPACE']
        script_path = required_env_vars['SCRIPT_PATH']
        script_name = required_env_vars['SCRIPT_NAME_PY']
        log_path = required_env_vars['LOG_PATH']

        # Parse and validate request JSON
        try:
            data = request.get_json(force=False, silent=False)
        except Exception as e:
            logger.warning(f'Invalid JSON in request: {str(e)}')
            return jsonify({
                'result': None,
                'errors': ['Invalid JSON in request body']
            }), 400

        validated_data, validation_error = _validate_make_digital_objects_request(data)
        if validation_error:
            logger.warning(f'Request validation failed: {validation_error}')
            return jsonify({
                'result': None,
                'errors': [validation_error]
            }), 400

        # Validate and resolve folder path
        folder = validated_data['folder']
        batch_path, path_error = _validate_and_resolve_path(folder, workspace)
        if path_error:
            logger.warning(f'Invalid folder path: {path_error}')
            return jsonify({
                'result': None,
                'errors': [path_error]
            }), 403

        # Verify folder exists
        if not batch_path.exists():
            logger.warning(f'Batch folder not found: {folder}')
            return jsonify({
                'result': None,
                'errors': [f'Batch folder not found: {folder}']
            }), 404

        if not batch_path.is_dir():
            logger.warning(f'Batch path is not a directory: {folder}')
            return jsonify({
                'result': None,
                'errors': [f'Path is not a directory: {folder}']
            }), 400

        # Construct and validate script path
        full_script_path = Path(script_path) / script_name
        if not full_script_path.exists():
            logger.error(f'Script not found: {full_script_path}')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error: script not found']
            }), 500

        # Validate log path exists
        log_path_obj = Path(log_path)
        if not log_path_obj.exists():
            try:
                log_path_obj.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                logger.error(f'Failed to create log directory: {str(e)}')
                return jsonify({
                    'result': None,
                    'errors': ['Server configuration error: log path inaccessible']
                }), 500

        # Handle Kaltura file serialization securely
        serialized_file_path = None
        if validated_data['is_kaltura'] and validated_data['files']:
            # Create temporary directory for serialized files
            temp_dir = Path(tempfile.mkdtemp(prefix='astools_'))
            serialized_file_path = _write_serialized_files(
                validated_data['files'],
                temp_dir
            )
            logger.info(f'Created serialized files at: {serialized_file_path}')

            # Copy serialized files to the batch directory for the CLI script
            # (CLI script expects serialized_files.json in working directory)
            import shutil
            # target_serialized_path = batch_path / 'serialized_files.json'
            target_serialized_path = 'serialized_files.json'
            shutil.copy2(serialized_file_path, target_serialized_path)
            logger.info(f'Copied serialized files to batch directory')

        # Build CLI arguments (secure list-based execution)
        cli_args = _build_cli_arguments(
            str(full_script_path),
            username,
            password,
            str(batch_path),
            validated_data,
            serialized_file_path
        )

        # Log command execution (mask sensitive credentials)
        masked_args = cli_args.copy()
        for i, arg in enumerate(masked_args):
            if arg in ['-u', '-p'] and i + 1 < len(masked_args):
                masked_args[i + 1] = '****'
        logger.info(f'Executing: {" ".join(masked_args)}')

        # Execute the CLI script securely (no shell=True)
        # Using stdout/stderr=PIPE and universal_newlines for Python 3.6 compatibility
        result = subprocess.run(
            cli_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=3600,  # 1 hour timeout for large batches
            cwd=str(batch_path),  # Set working directory to batch path
            env={
                **os.environ,
                'PYTHONUNBUFFERED': '1'  # Ensure real-time output
            }
        )

        # Generate log filename with sanitized folder name
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        sanitized_folder = re.sub(r'[^\w\-]', '_', folder)
        log_suffix = '' if result.returncode == 0 else '-error'
        log_filename = f'{sanitized_folder}-{timestamp}{log_suffix}.log'
        log_file_path = log_path_obj / log_filename

        # Determine output content
        output_content = result.stdout if result.stdout else result.stderr

        # Write log file
        try:
            with open(log_file_path, 'w', encoding='utf-8') as f:
                f.write(f'Command: {" ".join(masked_args)}\n')
                f.write(f'Return Code: {result.returncode}\n')
                f.write(f'Timestamp: {timestamp}\n')
                f.write('-' * 80 + '\n')
                if result.stdout:
                    f.write('STDOUT:\n')
                    f.write(result.stdout)
                    f.write('\n')
                if result.stderr:
                    f.write('STDERR:\n')
                    f.write(result.stderr)
            logger.info(f'Log written to: {log_file_path}')
        except IOError as e:
            logger.error(f'Failed to write log file: {str(e)}')

        # Determine response based on execution result
        if result.returncode == 0:
            logger.info(f'Successfully processed batch: {folder}')
            return jsonify({
                'result': {
                    'success': True,
                    'folder': folder,
                    'output': result.stdout,
                    'log_file': log_filename
                },
                'errors': []
            }), 200
        else:
            logger.error(f'Script execution failed for batch: {folder}')
            return jsonify({
                'result': {
                    'success': False,
                    'folder': folder,
                    'output': output_content,
                    'log_file': log_filename
                },
                'errors': [f'Script execution failed with return code {result.returncode}']
            }), 500

    except subprocess.TimeoutExpired:
        logger.error('Script execution timed out')
        return jsonify({
            'result': None,
            'errors': ['Script execution timed out after 1 hour']
        }), 504

    except json.JSONDecodeError as e:
        logger.error(f'JSON parsing error: {str(e)}')
        return jsonify({
            'result': None,
            'errors': ['Invalid JSON in request body']
        }), 400

    except PermissionError as e:
        logger.error(f'Permission error: {str(e)}')
        return jsonify({
            'result': None,
            'errors': ['Permission denied accessing resources']
        }), 500

    except FileNotFoundError as e:
        logger.error(f'File not found: {str(e)}')
        return jsonify({
            'result': None,
            'errors': ['Required file or directory not found']
        }), 500

    except Exception as e:
        logger.error(f'Unexpected error in make_digital_objects: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500

    finally:
        # Clean up temporary directory
        if temp_dir and temp_dir.exists():
            try:
                import shutil
                shutil.rmtree(temp_dir)
                logger.debug(f'Cleaned up temporary directory: {temp_dir}')
            except Exception as e:
                logger.warning(f'Failed to clean up temporary directory: {str(e)}')


@astools_bp.route('/api/v1/astools/revert-to-make-digital-objects', methods=['POST'])
@require_api_key_astools
def revert_to_make_digital_objects():
    """
    Reverts a batch folder back to the "Make Digital Objects" state by
    removing the uri.txt file from each package directory.

    Used by the dashboard's ASpace QA and Packaging and Ingesting views
    when staff need to rebuild a package after MDO has already run.
    Idempotent — packages without uri.txt are reported as not_present
    rather than treated as errors.

    Only removes uri.txt. The original source files (TIFFs etc.) are
    untouched, so re-running Make Digital Objects on the same folder
    will regenerate uri.txt against the same packages.

    @body data: {folder: "<batch_folder_name>"}
    @return: JSON {result: {folder, removed: [...], not_present: [...], failed: [{package, error}]}, errors: []}
    """
    try:
        logger.info('POST /api/v1/astools/revert-to-make-digital-objects - Processing request')

        workspace = os.getenv('WORKSPACE')
        if not workspace:
            logger.error('WORKSPACE environment variable not configured')
            return jsonify({
                'result': None,
                'errors': ['Server configuration error']
            }), 500

        try:
            data = request.get_json(force=False, silent=False)
        except Exception as e:
            logger.warning(f'Invalid JSON in request: {str(e)}')
            return jsonify({
                'result': None,
                'errors': ['Invalid JSON in request body']
            }), 400

        if not isinstance(data, dict):
            return jsonify({
                'result': None,
                'errors': ['Request body must be a JSON object']
            }), 400

        folder = (data.get('folder') or '').strip()
        if not folder:
            return jsonify({
                'result': None,
                'errors': ['Bad request: Missing folder parameter']
            }), 400

        batch_path, path_error = _validate_and_resolve_path(folder, workspace)
        if path_error:
            logger.warning(f'Invalid folder path: {path_error}')
            return jsonify({
                'result': None,
                'errors': [path_error]
            }), 403

        if not batch_path.exists():
            logger.warning(f'Batch folder not found: {folder}')
            return jsonify({
                'result': None,
                'errors': [f'Batch folder not found: {folder}']
            }), 404

        if not batch_path.is_dir():
            return jsonify({
                'result': None,
                'errors': [f'Path is not a directory: {folder}']
            }), 400

        removed = []
        not_present = []
        failed = []

        for package_dir in sorted(batch_path.iterdir(), key=lambda p: p.name):
            if not package_dir.is_dir() or package_dir.name.startswith('.'):
                continue
            uri_txt = package_dir / 'uri.txt'
            try:
                if uri_txt.exists():
                    uri_txt.unlink()
                    removed.append(package_dir.name)
                else:
                    not_present.append(package_dir.name)
            except (OSError, PermissionError) as e:
                logger.error(f'Failed to remove {uri_txt}: {str(e)}')
                failed.append({'package': package_dir.name, 'error': str(e)})

        logger.info(
            f'Revert complete for {folder}: removed={len(removed)} '
            f'not_present={len(not_present)} failed={len(failed)}'
        )

        # 207 (Multi-Status) when some succeeded and some failed; 200 otherwise.
        # The Node side keys off the body's `failed` list rather than HTTP status.
        status_code = 207 if failed and (removed or not_present) else 200
        if failed and not removed and not not_present:
            status_code = 500

        return jsonify({
            'result': {
                'folder': folder,
                'removed': removed,
                'not_present': not_present,
                'failed': failed
            },
            'errors': [f'{len(failed)} package(s) failed'] if failed else []
        }), status_code

    except PermissionError as e:
        logger.error(f'Permission error: {str(e)}')
        return jsonify({
            'result': None,
            'errors': ['Permission denied accessing folder']
        }), 500

    except Exception as e:
        logger.error(f'Unexpected error in revert_to_make_digital_objects: {str(e)}', exc_info=True)
        return jsonify({
            'result': None,
            'errors': ['Internal server error']
        }), 500
