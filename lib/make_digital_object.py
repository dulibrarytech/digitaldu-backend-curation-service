#!/usr/bin/env python3
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
ArchivesSpace Digital Object Creation Script

This script automates the creation of digital objects in ArchivesSpace:
1. Logs into ArchivesSpace backend via ArchivesSnake
2. Processes digital object packages according to Digital Repository ingest specification
3. Checks for uri.txt existence and generates if missing
4. Creates digital object if not already attached to item record
5. Processes each file and attaches digital object components with file metadata
"""

import argparse
import json
import logging
import magic
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from os.path import join, dirname

from dotenv import load_dotenv
from asnake.aspace import ASpace

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Global configuration
AS = None
VERBOSE_LOG = False


class DigitalObjectException(Exception):
    """Custom exception for digital object processing errors."""

    def __init__(self, message):
        super(DigitalObjectException, self).__init__(message)
        self.message = message


def get_json(uri: str) -> Dict:
    """
    Fetches JSON data from ArchivesSpace API.

    @param uri: API endpoint URI
    @return: JSON response as dictionary
    @raises: HTTPError on non-200 status code
    """
    try:
        r = AS.client.get(uri)
        if r.status_code == 200:
            return json.loads(r.text)
        else:
            r.raise_for_status()
    except Exception as e:
        logger.error(f'Error fetching JSON from {uri}: {str(e)}')
        raise


def post_json(uri: str, data: Dict) -> Dict:
    """
    Posts JSON data to ArchivesSpace API.

    @param uri: API endpoint URI
    @param data: Data to post as dictionary
    @return: JSON response message
    """
    try:
        r = AS.client.post(uri, json=data)
        message = json.loads(r.text)

        if r.status_code == 200:
            if VERBOSE_LOG:
                if 'uri' in message:
                    as_log(f"{message['status']}: {message['uri']}")
                else:
                    as_log(f"{message['status']}: {uri}")
        else:
            as_log(f"Error: {message.get('error', 'Unknown error')}")

        return message

    except Exception as e:
        logger.error(f'Error posting JSON to {uri}: {str(e)}')
        raise


def magic_to_as(file_format_name: str) -> str:
    """
    Translates libmagic file format labels to ArchivesSpace format names.

    @param file_format_name: Format name from libmagic
    @return: ArchivesSpace-compatible format name
    """
    format_mapping = {
        'x-wav': 'wav',
        'quicktime': 'mov',
        'mpeg': 'mp3',
        'x-font-gdos': 'mp3',
        'vnd.adobe.photoshop': 'tiff'
    }

    return format_mapping.get(file_format_name, file_format_name)


def get_kaltura_id_from_file(filename: str, serialized_file_path: str = 'serialized_files.json') -> Optional[str]:
    """
    Retrieves Kaltura ID for a specific file from serialized JSON.

    @param filename: Name of file to look up
    @param serialized_file_path: Path to JSON file with Kaltura mappings
    @return: Kaltura entry ID or None if not found
    """
    try:
        logger.debug(f'Looking for Kaltura ID mapping in: {serialized_file_path}')

        if not os.path.exists(serialized_file_path):
            logger.warning(f'Serialized files not found: {serialized_file_path}')
            return None

        with open(serialized_file_path, 'r') as json_file:
            json_data = json.load(json_file)

            logger.debug(f'Loaded {len(json_data)} entries from serialized_files.json')

            for item in json_data:
                item_file = item.get('file')
                if filename == item_file:
                    kaltura_id = item.get('entry_id')
                    if kaltura_id:
                        logger.info(f'Found Kaltura ID for {filename}: {kaltura_id}')
                        return kaltura_id
                    else:
                        logger.warning(f'File {filename} found in mapping but entry_id is empty')

        logger.warning(f'No Kaltura ID found for {filename} in {serialized_file_path}')
        return None

    except json.JSONDecodeError as e:
        logger.error(f'Error parsing serialized files JSON: {str(e)}')
        return None
    except Exception as e:
        logger.error(f'Error reading Kaltura ID for {filename}: {str(e)}')
        return None


def process_files(ref: str, path: str, no_kaltura_id: bool, no_caption: bool, no_publish: bool):
    """
    Processes files in directory and creates/updates digital object components.

    @param ref: Digital object reference URI
    @param path: Directory path containing files to process
    @param no_kaltura_id: Skip Kaltura ID attachment
    @param no_caption: Skip caption prompts
    @param no_publish: Do not publish components
    """
    try:
        logger.info('Fetching digital object tree...')
        tree = get_json(f'{ref}/tree/root')

        logger.info('Checking files...')

        # Get all files, excluding system files
        excluded_files = {'uri.txt', 'Thumbs.db', '.DS_Store', 'serialized_files.json'}
        path_obj = Path(path)

        files = [
            f.name for f in sorted(path_obj.iterdir(), key=lambda x: x.name)
            if f.is_file() and f.name not in excluded_files and not f.name.startswith('.')
        ]

        if not files:
            logger.info('No files found to process.')
            return

        logger.info(f'Found {len(files)} file(s) to process')

        for file in files:
            _process_single_file(
                file, path_obj, ref, tree,
                no_kaltura_id, no_caption, no_publish
            )

    except Exception as e:
        logger.error(f'Error processing files: {str(e)}', exc_info=True)
        raise


def _process_single_file(
        filename: str,
        path_obj: Path,
        ref: str,
        tree: Dict,
        no_kaltura_id: bool,
        no_caption: bool,
        no_publish: bool
):
    """
    Processes a single file to create or update digital object component.

    @param filename: Name of file to process
    @param path_obj: Path object for directory
    @param ref: Digital object reference URI
    @param tree: Digital object tree structure
    @param no_kaltura_id: Skip Kaltura ID attachment
    @param no_caption: Skip caption prompts
    @param no_publish: Do not publish component
    """
    try:
        path_to_file = path_obj / filename

        # Get file metadata
        file_format_name = magic.from_file(str(path_to_file), mime=True).split('/')[-1]
        file_format_name = magic_to_as(file_format_name)

        # Get file size (limit to MySQL int(11) constraint)
        file_size = path_to_file.stat().st_size
        file_size_bytes = file_size if file_size < 2147483647 else ''

        # Get Kaltura ID if applicable
        # serialized_files.json is located in the same directory as the script (written by web.py)
        kaltura_id = None
        if not no_kaltura_id:
            script_dir = Path(__file__).parent.resolve()
            serialized_path = str(script_dir / 'serialized_files.json')
            logger.debug(f'Looking for serialized_files.json at: {serialized_path}')
            kaltura_id = get_kaltura_id_from_file(filename, serialized_path)

        # Check if file already exists in tree
        make_new = True

        if tree.get('child_count', 0) > 0:
            tree_files = [
                child for child in tree.get('precomputed_waypoints', {}).get('', {}).get('0', [])
                if child.get('title') == filename
            ]

            if tree_files:
                logger.info(f'Checking for file-level metadata updates: {filename}')
                for child in tree_files:
                    make_new = False
                    _update_existing_component(
                        child, filename, file_format_name, file_size_bytes,
                        kaltura_id, no_caption, no_publish, ref
                    )

        if make_new:
            _create_new_component(
                filename, file_format_name, file_size_bytes,
                kaltura_id, no_caption, no_publish, ref
            )

    except Exception as e:
        logger.error(f'Error processing file {filename}: {str(e)}')
        raise


def _update_existing_component(
        child: Dict,
        filename: str,
        file_format_name: str,
        file_size_bytes: int,
        kaltura_id: Optional[str],
        no_caption: bool,
        no_publish: bool,
        ref: str
):
    """
    Updates an existing digital object component with new metadata.

    @param child: Child component from tree
    @param filename: File name
    @param file_format_name: File format
    @param file_size_bytes: File size in bytes
    @param kaltura_id: Kaltura entry ID
    @param no_caption: Skip caption prompt
    @param no_publish: Do not publish
    @param ref: Digital object reference
    """
    try:
        record = get_json(child['uri'])
        updates = False

        # Add Kaltura ID if missing
        if kaltura_id:
            if 'component_id' not in record or not record.get('component_id'):
                logger.info(f'Adding Kaltura ID {kaltura_id} to existing component for {filename}')
                record['component_id'] = kaltura_id
                updates = True
            else:
                logger.debug(f'Component {filename} already has component_id: {record.get("component_id")}')
        else:
            logger.debug(f'No Kaltura ID provided for {filename}')

        # Update or create file version
        if record.get('file_versions'):
            version = record['file_versions'][0]

            if version.get('file_uri') != filename:
                record['file_versions'][0]['file_uri'] = filename
                updates = True

            if version.get('file_format_name') != file_format_name:
                record['file_versions'][0]['file_format_name'] = file_format_name
                updates = True

            if version.get('file_size_bytes') != file_size_bytes:
                record['file_versions'][0]['file_size_bytes'] = file_size_bytes
                updates = True
        else:
            record['file_versions'] = [{
                'jsonmodel_type': 'file_version',
                'file_uri': filename,
                'file_format_name': file_format_name,
                'file_size_bytes': file_size_bytes,
                'is_representative': True,
            }]
            updates = True

        # Handle caption (only in interactive mode)
        if not no_caption:
            caption = input('> Caption (leave blank for none): ')
            if caption:
                if record['file_versions'][0].get('caption') != caption:
                    record['file_versions'][0]['caption'] = caption
                    updates = True

        # Handle publish flag
        if no_publish:
            record['publish'] = False
            record['file_versions'][0]['publish'] = False
            updates = True

        # Post updates if any were made
        if updates:
            logger.info(f'Updating {filename}...')
            record['digital_object'] = {'ref': ref}
            post_json(child['uri'], record)
        else:
            logger.info('No updates necessary')

    except Exception as e:
        logger.error(f'Error updating component for {filename}: {str(e)}')
        raise


def _create_new_component(
        filename: str,
        file_format_name: str,
        file_size_bytes: int,
        kaltura_id: Optional[str],
        no_caption: bool,
        no_publish: bool,
        ref: str
):
    """
    Creates a new digital object component for a file.

    @param filename: File name
    @param file_format_name: File format
    @param file_size_bytes: File size in bytes
    @param kaltura_id: Kaltura entry ID
    @param no_caption: Skip caption prompt
    @param no_publish: Do not publish
    @param ref: Digital object reference
    """
    try:
        logger.info(f'Creating new component for {filename}')

        data = {
            'jsonmodel_type': 'digital_record_children',
            'children': [{
                'title': filename,
                'publish': not no_publish,
                'file_versions': [{
                    'jsonmodel_type': 'file_version',
                    'file_uri': filename,
                    'publish': not no_publish,
                    'file_format_name': file_format_name,
                    'file_size_bytes': file_size_bytes,
                    'is_representative': True
                }],
                'digital_object': {'ref': ref}
            }]
        }

        # Add Kaltura ID if available
        if kaltura_id:
            logger.info(f'Adding Kaltura ID {kaltura_id} to component for {filename}')
            data['children'][0]['component_id'] = kaltura_id
        else:
            logger.debug(f'No Kaltura ID to add for {filename}')

        # Add caption if in interactive mode
        if not no_caption:
            caption = input('> Caption (leave blank for none): ')
            if caption:
                data['children'][0]['file_versions'][0]['caption'] = caption

        logger.info(f'Adding file-level metadata to ArchivesSpace for {filename}')
        post_json(f'{ref}/children', data)

    except Exception as e:
        logger.error(f'Error creating component for {filename}: {str(e)}')
        raise


def write_digital_object(item: Dict) -> str:
    """
    Creates a digital object and attaches it to the provided item record.

    @param item: Item record dictionary
    @return: Digital object reference URI
    """
    try:
        as_log('Creating digital object...')

        obj = {
            'title': item['display_string'],
            'jsonmodel_type': 'digital_object'
        }

        # Determine digital object ID
        links = [
            d for d in item.get('external_documents', [])
            if d.get('title') == 'Special Collections @ DU'
        ]

        if links:
            if len(links) > 1:
                raise DigitalObjectException(
                    'There should not be more than one repository link on an item record.'
                )
            obj['digital_object_id'] = links[0]['location']
        else:
            obj['digital_object_id'] = item['component_id']

        # Get repository ID from environment or default to 2
        repo_id = os.getenv('ASPACE_REPOSITORY_ID', '2')

        # Post the digital object
        r = AS.client.post(f'/repositories/{repo_id}/digital_objects', json=obj)

        if r.status_code == 200:
            ref = json.loads(r.text)['uri']

            # Link digital object to item record
            item['instances'].append({
                'instance_type': 'digital_object',
                'jsonmodel_type': 'instance',
                'is_representative': True,
                'digital_object': {'ref': ref}
            })
            post_json(item['uri'], item)

            logger.info(f'Created digital object: {ref}')
            return ref
        else:
            error_msg = json.loads(r.text).get('error', 'Unknown error')
            raise DigitalObjectException(f'Error creating digital object: {error_msg}')

    except Exception as e:
        logger.error(f'Error writing digital object: {str(e)}')
        raise


def check_digital_object(uri: str) -> str:
    """
    Checks if digital object exists for item, creates if missing.

    @param uri: Item record URI
    @return: Digital object reference URI
    """
    try:
        as_log('Downloading item record...')
        item = get_json(uri)

        as_log('Checking for digital object...')
        instances = [
            i for i in item.get('instances', [])
            if i.get('instance_type') == 'digital_object'
        ]

        if instances:
            if len(instances) > 1:
                raise DigitalObjectException(
                    'An item cannot have more than one digital object. '
                    'Please check ArchivesSpace to confirm proper cataloging.'
                )

            ref = instances[0]['digital_object']['ref']

            # Ensure digital object is representative
            objects = [i for i in instances if i.get('is_representative')]
            if not objects:
                as_log('Making the digital object representative...')
                for instance in item['instances']:
                    if instance['digital_object']['ref'] == ref:
                        instance['is_representative'] = True
                post_json(item['uri'], item)
        else:
            ref = write_digital_object(item)

        return ref

    except Exception as e:
        logger.error(f'Error checking digital object: {str(e)}')
        raise


def write_uri_txt(component_id: str, path: Path) -> str:
    """
    Writes uri.txt by searching for component ID and fetching its URI.

    @param component_id: Component ID to search for
    @param path: Path to write uri.txt
    @return: Item record URI
    """
    try:
        repo_id = os.getenv('ASPACE_REPOSITORY_ID', '2')

        resp = AS.client.get(
            f'/repositories/{repo_id}/search',
            params={
                'q': component_id,
                'type[]': 'archival_object',
                'page': '1'
            }
        )

        if resp.status_code != 200:
            resp.raise_for_status()

        results = json.loads(resp.text).get('results', [])
        uris = [
            r for r in results
            if r.get('component_id') == component_id and 'pui' not in r.get('types', [])
        ]

        if not uris:
            raise DigitalObjectException(
                'Could not find this item in ArchivesSpace. '
                'Has it been cataloged? Is the component ID accurate?'
            )

        if len(uris) > 1:
            raise DigitalObjectException(
                f'Multiple objects with component ID "{component_id}" found. '
                'Check ArchivesSpace for more information.'
            )

        uri = uris[0]['uri']

        # Write uri.txt
        as_log('Writing uri.txt...')
        with open(path, 'w') as f:
            f.write(uri)

        logger.info(f'Wrote URI to {path}')
        return uri

    except Exception as e:
        logger.error(f'Error writing uri.txt: {str(e)}')
        raise


def check_uri_txt(path: Path) -> str:
    """
    Confirms uri.txt exists and URI matches the item component ID.

    @param path: Directory path containing uri.txt
    @return: Item record URI
    """
    try:
        component_id = path.name
        uri_txt_path = path / 'uri.txt'

        as_log('Checking for uri.txt...')

        if uri_txt_path.exists():
            as_log('Checking if URI matches item record...')

            with open(uri_txt_path, 'r') as f:
                uri = f.read().replace('\n', '').strip()

            # Validate URI matches component ID
            item = get_json(uri)
            if item.get('component_id') != component_id:
                logger.warning('URI does not match component ID, regenerating...')
                uri = write_uri_txt(component_id, uri_txt_path)
        else:
            uri = write_uri_txt(component_id, uri_txt_path)

        return uri

    except Exception as e:
        logger.error(f'Error checking uri.txt: {str(e)}')
        raise


def get_path(path_arg: Optional[str], workspace: str) -> Path:
    """
    Validates and returns the full path to process.

    @param path_arg: Path argument from command line
    @param workspace: Workspace root directory
    @return: Validated Path object
    """
    if not path_arg:
        raise ValueError('Path argument is required')

    # Convert to Path and resolve
    full_path = Path(workspace) / path_arg if not Path(path_arg).is_absolute() else Path(path_arg)
    full_path = full_path.resolve()

    # Validate path exists
    if not full_path.exists():
        raise FileNotFoundError(f'{full_path} does not exist')

    if not full_path.is_dir():
        raise NotADirectoryError(f'{full_path} is not a directory')

    return full_path


def as_log(message: str):
    """
    Logs a message (wrapper for future extensibility).

    @param message: Message to log
    """
    print(message)


def process_batch(base_path: Path, no_kaltura_id: bool, no_caption: bool,
                  no_publish: bool):
    """
    Processes every subdirectory of base_path, continuing past
    per-package failures.

    Per-package failures (duplicate component IDs, uncataloged items,
    ...) are COLLECTED and returned, not just logged: main() exits
    non-zero when any are returned, which the curation route and the
    dashboard treat as a failed run.

    @return: (failures, total) where failures is [(subdir_name, error
             message), ...] and total is the number of subdirectories
             attempted.
    """
    subdirs = sorted([d for d in base_path.iterdir() if d.is_dir()], key=lambda x: x.name)

    if not subdirs:
        logger.warning(f'No subdirectories found in {base_path}')
        return [], 0

    logger.info(f'Processing {len(subdirs)} director(ies)')

    failures = []
    for subdir in subdirs:
        try:
            process(subdir, no_kaltura_id, no_caption, no_publish)
        except Exception as e:
            logger.error(f'Failed to process {subdir.name}: {str(e)}')
            failures.append((subdir.name, str(e)))
            # Continue with next directory
            continue
    return failures, len(subdirs)


def report_batch_failures(failures, total):
    """
    Prints the end-of-run failure summary to STDOUT (as_log), where the
    web route's captured output — and therefore the dashboard's result
    card — will carry it. Lines are prefixed `FAILED ` so the route can
    lift them into its errors[] array for the card's headline.
    """
    if not failures:
        return
    as_log('')
    as_log(f'==== MAKE DIGITAL OBJECTS: {len(failures)} of {total} package(s) FAILED ====')
    for name, error in failures:
        as_log(f'FAILED {name}: {error}')
    as_log('Fix the issues above in ArchivesSpace, then run Make Digital Objects again.')
    as_log('Packages that already succeeded are unaffected and are re-checked on the next run.')


def process(path: Path, no_kaltura_id: bool, no_caption: bool, no_publish: bool):
    """
    Main processing function for a single directory.

    @param path: Directory path to process
    @param no_kaltura_id: Skip Kaltura ID attachment
    @param no_caption: Skip caption prompts
    @param no_publish: Do not publish components
    """
    try:
        logger.info(f'Processing: {path.name}')

        uri = check_uri_txt(path)
        ref = check_digital_object(uri)
        process_files(ref, str(path), no_kaltura_id, no_caption, no_publish)

        logger.info(f'Successfully processed: {path.name}')

    except DigitalObjectException as e:
        logger.error(f'Digital object error for {path.name}: {e.message}')
        raise
    except Exception as e:
        logger.error(f'Error processing {path.name}: {str(e)}', exc_info=True)
        raise


def validate_environment() -> Dict:
    """
    Validates required environment variables are set.

    @return: Dictionary of configuration values
    @raises: ValueError if required variables are missing
    """
    required_vars = ['WORKSPACE', 'DEFAULT_URL']
    config = {}

    for var in required_vars:
        value = os.getenv(var)
        if not value:
            raise ValueError(f'Required environment variable not set: {var}')
        config[var] = value

    # Optional variables with defaults
    config['TESTING_URL'] = os.getenv('TESTING_URL', config['DEFAULT_URL'])
    config['ASPACE_REPOSITORY_ID'] = os.getenv('ASPACE_REPOSITORY_ID', '2')

    return config


def main():
    """Main entry point for the script."""
    global AS, VERBOSE_LOG

    try:
        # Validate environment
        config = validate_environment()

        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description='Make a digital object based on the contents of a directory',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )

        parser.add_argument('-u', '--user', required=True, help='ArchivesSpace username')
        parser.add_argument('-p', '--password', required=True, help='ArchivesSpace password')
        parser.add_argument('path', help='The directory to process')
        parser.add_argument(
            '-b', '--batch',
            action='store_true',
            help='Process all subdirectories in the specified path as a batch'
        )
        parser.add_argument(
            '--no_kaltura_id',
            action='store_true',
            help='Do not attach Kaltura IDs to digital object components'
        )
        parser.add_argument(
            '--no_caption',
            action='store_true',
            help='Do not prompt for captions (non-interactive mode)'
        )
        parser.add_argument(
            '--no_publish',
            action='store_true',
            help='Do not publish digital object components'
        )
        parser.add_argument(
            '--test',
            action='store_true',
            help='Run on test ArchivesSpace server'
        )
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            help='Enable verbose logging'
        )

        args = parser.parse_args()

        # Set verbose logging
        VERBOSE_LOG = args.verbose
        if VERBOSE_LOG:
            logger.setLevel(logging.DEBUG)

        # Connect to ArchivesSpace
        baseurl = config['TESTING_URL'] if args.test else config['DEFAULT_URL']
        logger.info(f'Connecting to ArchivesSpace at {baseurl}')

        AS = ASpace(baseurl=baseurl, username=args.user, password=args.password)
        logger.info('Successfully connected to ArchivesSpace')

        # Process path(s)
        if args.batch:
            logger.info('Running in batch mode')
            base_path = get_path(args.path, config['WORKSPACE'])

            failures, total = process_batch(
                base_path, args.no_kaltura_id, args.no_caption, args.no_publish
            )
            if failures:
                # Per-package failures (duplicate component IDs,
                # uncataloged items, ...) must FAIL the run: exit 2 so
                # the curation route returns its failure envelope and
                # the dashboard records a FAILED job with the summary
                # visible on the result card. Exit 2 (not 1)
                # distinguishes "some packages failed" from the
                # config/connection failures below.
                report_batch_failures(failures, total)
                logger.error(
                    f'Script completed with {len(failures)} failed package(s)'
                )
                sys.exit(2)
        else:
            path = get_path(args.path, config['WORKSPACE'])
            process(path, args.no_kaltura_id, args.no_caption, args.no_publish)

        logger.info('Script completed successfully')

    except DigitalObjectException as e:
        logger.error(f'Digital object error: {e.message}')
        sys.exit(1)
    except ValueError as e:
        logger.error(f'Configuration error: {str(e)}')
        sys.exit(1)
    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()