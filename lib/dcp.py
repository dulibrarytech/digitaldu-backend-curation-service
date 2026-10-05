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
DuraCloud Preserve (DCP) S3 client.

DuraCloud Preserve is plain AWS S3; there is no DuraCloud API anymore.
This module builds the boto3 client for it and wraps the calls the
curation service needs: probe a location, look at one key, list a
prefix, and open a (ranged) read of one object.

It is separate from lib/wasabi.py on purpose: a different provider, with
different credentials and region rules. Both modules pass explicit keys
and a region and never fall back to boto3's default credential chain, so
AWS and Wasabi credentials can share this process without either being
picked up for the other.

Locations use the WASABI_AIP_BUCKET form, `s3://<bucket>/<prefix>/`:

    DCP_DIP_LOCATION=s3://dcp-archivesdu-dip-store/dip-store/
    DCP_AIP_LOCATION=s3://dcp-archivesdu-aip-store/aip-store/
"""

import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

import config
from lib import wasabi

# Data reads: retry transient failures. read_timeout bounds each socket
# read, not the whole transfer, so long streams are fine. Endpoint URLs
# from the environment or ~/.aws/config are ignored: a stray
# AWS_ENDPOINT_URL_S3 meant for another provider must not redirect DCP.
_DATA_CONFIG = Config(
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    connect_timeout=30,
    read_timeout=180,
    ignore_configured_endpoint_urls=True,
)

# Health probes answer fast or report failure: one attempt, short timeouts.
_PROBE_CONFIG = Config(
    retries={'max_attempts': 1},
    connect_timeout=10,
    read_timeout=15,
    ignore_configured_endpoint_urls=True,
)

# The location settings, by short name, in report order.
LOCATION_SETTINGS = (
    ('dip', 'DCP_DIP_LOCATION'),
    ('aip', 'DCP_AIP_LOCATION'),
)


class RangeError(Exception):
    """A ranged GET came back without the byte range that was asked for."""


def is_configured():
    """True when the DCP key pair and region are all set."""
    return bool(
        config.DCP_ACCESS_KEY_ID
        and config.DCP_SECRET_ACCESS_KEY
        and config.DCP_REGION
    )


def location(setting):
    """
    Parse a location setting ('DCP_DIP_LOCATION' or 'DCP_AIP_LOCATION')
    into (bucket, prefix). The prefix is '' or ends in '/'. Raises
    RuntimeError naming the setting when it is unset.
    """
    raw = getattr(config, setting, None)
    if not raw:
        raise RuntimeError(f'{setting} is not configured')
    return wasabi._parse_bucket(raw)


def make_client(probe=False):
    """
    Build a boto3 S3 client for DCP from the explicit DCP_* settings.

    probe=True uses the single-attempt, short-timeout config for health
    checks. AWS_PROFILE / AWS_DEFAULT_PROFILE are popped while the
    session and client are built, the same guard as
    wasabi._client_from_keys: set, they make boto3 load a named profile
    even when keys are passed.
    """
    if not is_configured():
        raise RuntimeError(
            'DuraCloud Preserve is not configured. Set DCP_ACCESS_KEY_ID, '
            'DCP_SECRET_ACCESS_KEY and DCP_REGION in the service env.'
        )
    client_kwargs = {'config': _PROBE_CONFIG if probe else _DATA_CONFIG}
    if config.DCP_ENDPOINT_URL:
        client_kwargs['endpoint_url'] = config.DCP_ENDPOINT_URL

    saved = {
        k: os.environ.pop(k) for k in wasabi._PROFILE_ENV_VARS if k in os.environ
    }
    try:
        session = boto3.Session(
            aws_access_key_id=config.DCP_ACCESS_KEY_ID,
            aws_secret_access_key=config.DCP_SECRET_ACCESS_KEY,
            region_name=config.DCP_REGION,
        )
        return session.client('s3', **client_kwargs)
    finally:
        os.environ.update(saved)


def _status(error):
    """HTTP status of a botocore ClientError, or None."""
    return error.response.get('ResponseMetadata', {}).get('HTTPStatusCode')


def head(bucket, key, client=None):
    """
    HeadObject for one key.

    Returns {'exists': False} for a 404. Otherwise returns exists=True
    plus content_length, etag (unquoted), content_type and
    last_modified. Any other error raises (ClientError / BotoCoreError):
    a 403 is a policy problem, never "missing".
    """
    client = client or make_client()
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if _status(e) == 404:
            return {'exists': False}
        raise
    return {
        'exists': True,
        'content_length': resp.get('ContentLength'),
        'etag': (resp.get('ETag') or '').strip('"'),
        'content_type': resp.get('ContentType'),
        'last_modified': resp.get('LastModified'),
    }


def iter_objects(bucket, prefix, client=None):
    """
    Yield every object under `prefix`, following continuation tokens.

    Each item is {'key', 'size', 'etag' (unquoted), 'last_modified'}.
    """
    client = client or make_client()
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get('Contents', []):
            yield {
                'key': obj['Key'],
                'size': obj.get('Size'),
                'etag': (obj.get('ETag') or '').strip('"'),
                'last_modified': obj.get('LastModified'),
            }
        if not resp.get('IsTruncated'):
            return
        kwargs['ContinuationToken'] = resp['NextContinuationToken']


def open_range(bucket, key, offset=0, client=None):
    """
    Open a streamed read of one object, from byte `offset` to the end.

    Returns the get_object response dict; read the bytes from its
    'Body'. A ranged read (offset > 0) must come back as HTTP 206 with a
    Content-Range starting at `offset`. Anything else raises RangeError,
    because splicing a whole body onto a partial read would corrupt the
    copy.
    """
    if offset < 0:
        raise ValueError(f'offset must be >= 0 (got {offset})')
    client = client or make_client()
    if not offset:
        return client.get_object(Bucket=bucket, Key=key)

    resp = client.get_object(Bucket=bucket, Key=key, Range=f'bytes={offset}-')
    status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode')
    content_range = resp.get('ContentRange') or ''
    if status != 206 or not content_range.startswith(f'bytes {offset}-'):
        body = resp.get('Body')
        if body is not None:
            body.close()
        raise RangeError(
            f'ranged GET of {key} from byte {offset} returned HTTP {status} '
            f'({content_range or "no Content-Range"})'
        )
    return resp


def health_check():
    """
    Probe each configured DCP location with the DCP credential by listing
    one key under its prefix. Returns a dict and never raises, the same
    contract as wasabi.health_check, so /health/dcp can always answer 200:

        {'ok': bool, 'configured': bool, 'error': str or None,
         'locations': [{'name', 'setting', 'bucket', 'prefix', 'ok',
                        'error'}],
         'elapsed_ms': int}

    The probe is a prefix listing, not head_bucket, because it needs only
    ListBucket on the prefix: the narrowest grant DCP may issue.
    """
    started = time.monotonic()
    out = {
        'ok': False,
        'configured': is_configured(),
        'error': None,
        'locations': [],
    }

    def done():
        out['elapsed_ms'] = int((time.monotonic() - started) * 1000)
        return out

    if not out['configured']:
        out['error'] = (
            'not configured (DCP_ACCESS_KEY_ID, DCP_SECRET_ACCESS_KEY, '
            'DCP_REGION)'
        )
        return done()
    try:
        client = make_client(probe=True)
    except (BotoCoreError, RuntimeError) as e:
        out['error'] = str(e)
        return done()

    for name, setting in LOCATION_SETTINGS:
        if not getattr(config, setting, None):
            continue
        loc = {
            'name': name,
            'setting': setting,
            'bucket': None,
            'prefix': None,
            'ok': False,
            'error': None,
        }
        try:
            loc['bucket'], loc['prefix'] = location(setting)
            client.list_objects_v2(
                Bucket=loc['bucket'], Prefix=loc['prefix'], MaxKeys=1,
            )
            loc['ok'] = True
        except ClientError as e:
            err = e.response.get('Error', {})
            loc['error'] = (
                f"list_objects_v2 failed ({err.get('Code', 'unknown')}): "
                f"{err.get('Message', '')}"
            ).rstrip(': ')
        except (BotoCoreError, RuntimeError) as e:
            loc['error'] = str(e)
        out['locations'].append(loc)

    if not out['locations']:
        out['error'] = (
            'no location configured (DCP_DIP_LOCATION, DCP_AIP_LOCATION)'
        )
    else:
        out['ok'] = all(loc['ok'] for loc in out['locations'])
    return done()
