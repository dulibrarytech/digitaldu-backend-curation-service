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
Unit tests for lib/dcp.py, the DuraCloud Preserve (AWS S3) client.

S3 is stubbed with botocore's Stubber on a real boto3 client, so no
network or real credentials are needed. config values are patched per
test, and conftest.py blanks DCP_* in the environment, so nothing here
depends on the host's settings.
"""

import io
import os
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, patch

import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber

import config
from lib import dcp, wasabi

DIP = 's3://dcp-test-dip-store/dip-store/'
AIP = 's3://dcp-test-aip-store/aip-store/'


def configured(**overrides):
    """patch.multiple() for a fully configured DCP, with overrides."""
    values = {
        'DCP_ACCESS_KEY_ID': 'AKIATESTKEY',
        'DCP_SECRET_ACCESS_KEY': 'test-secret',
        'DCP_REGION': 'us-east-1',
        'DCP_ENDPOINT_URL': None,
        'DCP_DIP_LOCATION': DIP,
        'DCP_AIP_LOCATION': AIP,
    }
    values.update(overrides)
    return patch.multiple(config, **values)


def body(data):
    return StreamingBody(io.BytesIO(data), len(data))


def one_key_listing(key):
    """A list_objects_v2 answer holding one key: what a live location returns."""
    return {'KeyCount': 1, 'Contents': [{'Key': key, 'Size': 1}]}


class IsConfiguredTest(unittest.TestCase):
    def test_true_with_keys_and_region(self):
        with configured():
            self.assertTrue(dcp.is_configured())

    def test_false_without_region(self):
        # The region must be explicit; DCP won't let us discover it.
        with configured(DCP_REGION=''):
            self.assertFalse(dcp.is_configured())

    def test_false_with_half_a_key_pair(self):
        with configured(DCP_SECRET_ACCESS_KEY=None):
            self.assertFalse(dcp.is_configured())


class LocationTest(unittest.TestCase):
    def test_parses_bucket_and_prefix(self):
        with configured():
            self.assertEqual(
                dcp.location('DCP_DIP_LOCATION'),
                ('dcp-test-dip-store', 'dip-store/'),
            )

    def test_unset_names_the_setting(self):
        with configured(DCP_AIP_LOCATION=''):
            with self.assertRaises(RuntimeError) as ctx:
                dcp.location('DCP_AIP_LOCATION')
        self.assertIn('DCP_AIP_LOCATION', str(ctx.exception))


class MakeClientTest(unittest.TestCase):
    """The call shape: explicit DCP_* settings, an isolated session."""

    def setUp(self):
        patcher = patch.object(wasabi, '_session_from_keys')
        self.session_from_keys = patcher.start()
        self.addCleanup(patcher.stop)
        self.session = self.session_from_keys.return_value

    def test_passes_explicit_keys_and_region_with_aws_config_off(self):
        with configured():
            dcp.make_client()
        self.session_from_keys.assert_called_once_with(
            'AKIATESTKEY', 'test-secret', 'us-east-1', read_aws_config=False,
        )
        client_call = self.session.client.call_args
        self.assertEqual(client_call.args, ('s3',))
        self.assertIs(client_call.kwargs['config'], dcp._DATA_CONFIG)
        # Blank DCP_ENDPOINT_URL means AWS's own endpoint: none passed.
        self.assertNotIn('endpoint_url', client_call.kwargs)

    def test_probe_client_uses_the_short_single_attempt_config(self):
        with configured():
            dcp.make_client(probe=True)
        client_call = self.session.client.call_args
        self.assertIs(client_call.kwargs['config'], dcp._PROBE_CONFIG)

    def test_endpoint_override_is_passed_through(self):
        with configured(DCP_ENDPOINT_URL='http://127.0.0.1:9000'):
            dcp.make_client()
        client_call = self.session.client.call_args
        self.assertEqual(client_call.kwargs['endpoint_url'], 'http://127.0.0.1:9000')

    def test_raises_when_not_configured(self):
        with configured(DCP_ACCESS_KEY_ID=''):
            with self.assertRaises(RuntimeError) as ctx:
                dcp.make_client()
        self.assertIn('DCP_ACCESS_KEY_ID', str(ctx.exception))
        self.session_from_keys.assert_not_called()

    def test_configs_ignore_endpoint_urls_from_the_environment(self):
        # A stray AWS_ENDPOINT_URL_S3 meant for Wasabi must not redirect DCP.
        self.assertTrue(dcp._DATA_CONFIG.ignore_configured_endpoint_urls)
        self.assertTrue(dcp._PROBE_CONFIG.ignore_configured_endpoint_urls)


class SessionIsolationTest(unittest.TestCase):
    """
    Real boto3 sessions (no requests): nothing from the environment or
    the AWS config files reaches the DCP client, and building one never
    touches os.environ (popping the profile variables around the build
    raced across threads).
    """

    def test_profile_variables_are_neither_read_nor_touched(self):
        seen_at_session = {}
        real_session = boto3.Session

        def spy(**kwargs):
            # The variables are still in place while boto3 builds the
            # session: nothing popped them.
            seen_at_session['AWS_PROFILE'] = os.environ.get('AWS_PROFILE')
            seen_at_session['AWS_DEFAULT_PROFILE'] = os.environ.get('AWS_DEFAULT_PROFILE')
            return real_session(**kwargs)

        env = {'AWS_PROFILE': 'no-such-profile', 'AWS_DEFAULT_PROFILE': ''}
        with configured(), patch.dict(os.environ, env), \
                patch.object(wasabi.boto3, 'Session', side_effect=spy):
            client = dcp.make_client()
            self.assertEqual(os.environ['AWS_PROFILE'], 'no-such-profile')
            self.assertEqual(os.environ['AWS_DEFAULT_PROFILE'], '')
        # A missing profile name in the environment is simply not consulted.
        self.assertEqual(seen_at_session['AWS_PROFILE'], 'no-such-profile')
        self.assertEqual(seen_at_session['AWS_DEFAULT_PROFILE'], '')
        self.assertEqual(client.meta.region_name, 'us-east-1')
        self.assertEqual(client.meta.endpoint_url, 'https://s3.amazonaws.com')

    def test_aws_config_file_settings_do_not_reach_the_client(self):
        # A [default] profile with Wasabi-style S3 settings, pointed at by
        # AWS_CONFIG_FILE as a host's ~/.aws/config would be.
        with tempfile.NamedTemporaryFile('w', suffix='.ini', delete=False) as f:
            f.write(
                '[default]\n'
                'region = us-west-1\n'
                'aws_access_key_id = DEFAULTPROFILEKEY\n'
                'aws_secret_access_key = defaultprofilesecret\n'
                's3 =\n'
                '    addressing_style = path\n'
                '    signature_version = s3\n'
            )
        self.addCleanup(os.unlink, f.name)
        with configured(), patch.dict(os.environ, {'AWS_CONFIG_FILE': f.name}):
            client = dcp.make_client()
        self.assertEqual(client.meta.region_name, 'us-east-1')
        self.assertEqual(client.meta.config.signature_version, 's3v4')
        self.assertIsNone(client.meta.config.s3)
        creds = client._request_signer._credentials
        self.assertEqual(creds.access_key, 'AKIATESTKEY')

    def test_builds_from_many_threads_at_once(self):
        errors = []

        def build():
            try:
                dcp.make_client(probe=True)
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))

        env = {'AWS_PROFILE': 'no-such-profile', 'AWS_DEFAULT_PROFILE': ''}
        with configured(), patch.dict(os.environ, env):
            threads = [threading.Thread(target=build) for _ in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        self.assertEqual(errors, [])


class RetryConfigTest(unittest.TestCase):
    """botocore's `max_attempts` counts retries; the probe must make ONE attempt."""

    def test_probe_makes_exactly_one_attempt(self):
        with configured():
            client = dcp.make_client(probe=True)
        self.assertEqual(client.meta.config.retries['total_max_attempts'], 1)

    def test_data_client_makes_six_attempts_like_wasabi(self):
        with configured():
            client = dcp.make_client()
        self.assertEqual(client.meta.config.retries['total_max_attempts'], 6)


class StubbedClientTest(unittest.TestCase):
    """Builds a real boto3 client (no network) and stubs its responses."""

    def setUp(self):
        patcher = configured()
        patcher.start()
        self.addCleanup(patcher.stop)
        self.client = dcp.make_client()
        self.stub = Stubber(self.client)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)


class IterObjectsTest(StubbedClientTest):
    def test_follows_continuation_tokens(self):
        self.stub.add_response(
            'list_objects_v2',
            {
                'Contents': [{'Key': 'dip-store/a', 'Size': 1, 'ETag': '"e1"'}],
                'IsTruncated': True,
                'NextContinuationToken': 'tok',
            },
            {'Bucket': 'b', 'Prefix': 'dip-store/'},
        )
        self.stub.add_response(
            'list_objects_v2',
            {
                'Contents': [{'Key': 'dip-store/b', 'Size': 2, 'ETag': '"e2"'}],
                'IsTruncated': False,
            },
            {'Bucket': 'b', 'Prefix': 'dip-store/', 'ContinuationToken': 'tok'},
        )
        out = list(dcp.iter_objects('b', 'dip-store/', client=self.client))
        self.assertEqual([o['key'] for o in out], ['dip-store/a', 'dip-store/b'])
        self.assertEqual([o['etag'] for o in out], ['e1', 'e2'])
        self.assertEqual([o['size'] for o in out], [1, 2])
        self.stub.assert_no_pending_responses()

    def test_empty_prefix_yields_nothing(self):
        self.stub.add_response(
            'list_objects_v2', {'IsTruncated': False},
            {'Bucket': 'b', 'Prefix': 'dip-store/none/'},
        )
        self.assertEqual(
            list(dcp.iter_objects('b', 'dip-store/none/', client=self.client)), [],
        )


class OpenRangeTest(StubbedClientTest):
    def test_offset_zero_is_a_plain_get(self):
        self.stub.add_response(
            'get_object', {'Body': body(b'0123456789'), 'ContentLength': 10},
            {'Bucket': 'b', 'Key': 'k'},
        )
        resp = dcp.open_range('b', 'k', client=self.client)
        self.assertEqual(resp['Body'].read(), b'0123456789')

    def test_ranged_read_requires_206_and_matching_content_range(self):
        self.stub.add_response(
            'get_object',
            {
                'Body': body(b'56789'),
                'ContentLength': 5,
                'ContentRange': 'bytes 5-9/10',
                'ResponseMetadata': {'HTTPStatusCode': 206},
            },
            {'Bucket': 'b', 'Key': 'k', 'Range': 'bytes=5-'},
        )
        resp = dcp.open_range('b', 'k', offset=5, client=self.client)
        self.assertEqual(resp['Body'].read(), b'56789')

    def test_resume_pins_the_object_with_if_match(self):
        # The resumed read must be of the same object the copy started
        # with; S3 answers 412 (a ClientError) if the ETag changed.
        self.stub.add_response(
            'get_object',
            {
                'Body': body(b'56789'),
                'ContentLength': 5,
                'ContentRange': 'bytes 5-9/10',
                'ResponseMetadata': {'HTTPStatusCode': 206},
            },
            {'Bucket': 'b', 'Key': 'k', 'Range': 'bytes=5-', 'IfMatch': '"etag-1"'},
        )
        resp = dcp.open_range('b', 'k', offset=5, client=self.client, if_match='"etag-1"')
        self.assertEqual(resp['Body'].read(), b'56789')
        self.stub.add_client_error(
            'get_object', 'PreconditionFailed', 'At least one of the pre-conditions '
            'you specified did not hold', http_status_code=412,
        )
        with self.assertRaises(dcp.ClientError):
            dcp.open_range('b', 'k', offset=5, client=self.client, if_match='"etag-1"')

    def test_whole_body_on_a_ranged_request_is_refused(self):
        # A 200 here would splice the full object onto a partial read.
        self.stub.add_response(
            'get_object',
            {
                'Body': body(b'0123456789'),
                'ContentLength': 10,
                'ResponseMetadata': {'HTTPStatusCode': 200},
            },
            {'Bucket': 'b', 'Key': 'k', 'Range': 'bytes=5-'},
        )
        with self.assertRaises(dcp.RangeError) as ctx:
            dcp.open_range('b', 'k', offset=5, client=self.client)
        self.assertIn('HTTP 200', str(ctx.exception))

    def test_wrong_content_range_is_refused(self):
        self.stub.add_response(
            'get_object',
            {
                'Body': body(b'0123456789'),
                'ContentRange': 'bytes 0-9/10',
                'ResponseMetadata': {'HTTPStatusCode': 206},
            },
            {'Bucket': 'b', 'Key': 'k', 'Range': 'bytes=5-'},
        )
        with self.assertRaises(dcp.RangeError):
            dcp.open_range('b', 'k', offset=5, client=self.client)

    def test_negative_offset_is_rejected(self):
        with self.assertRaises(ValueError):
            dcp.open_range('b', 'k', offset=-1, client=self.client)


class HealthCheckTest(unittest.TestCase):
    def _stubbed_probe_client(self):
        with configured():
            client = dcp.make_client(probe=True)
        stub = Stubber(client)
        stub.activate()
        self.addCleanup(stub.deactivate)
        return client, stub

    def test_not_configured(self):
        with configured(DCP_ACCESS_KEY_ID=None):
            out = dcp.health_check()
        self.assertFalse(out['ok'])
        self.assertFalse(out['configured'])
        self.assertIn('DCP_ACCESS_KEY_ID', out['error'])
        self.assertIn('elapsed_ms', out)

    def test_both_locations_reachable(self):
        client, stub = self._stubbed_probe_client()
        stub.add_response(
            'list_objects_v2', one_key_listing('dip-store/a/METS.x.xml'),
            {'Bucket': 'dcp-test-dip-store', 'Prefix': 'dip-store/', 'MaxKeys': 1},
        )
        stub.add_response(
            'list_objects_v2', one_key_listing('aip-store/a.7z'),
            {'Bucket': 'dcp-test-aip-store', 'Prefix': 'aip-store/', 'MaxKeys': 1},
        )
        with configured(), patch.object(dcp, 'make_client', return_value=client):
            out = dcp.health_check()
        self.assertTrue(out['ok'])
        self.assertIsNone(out['error'])
        self.assertEqual([loc['name'] for loc in out['locations']], ['dip', 'aip'])
        self.assertTrue(all(loc['ok'] for loc in out['locations']))
        self.assertEqual([loc['keys'] for loc in out['locations']], [1, 1])

    def test_access_denied_on_one_location_fails_the_check(self):
        client, stub = self._stubbed_probe_client()
        stub.add_response('list_objects_v2', one_key_listing('dip-store/a'))
        stub.add_client_error(
            'list_objects_v2', 'AccessDenied', 'Access Denied', http_status_code=403,
        )
        with configured(), patch.object(dcp, 'make_client', return_value=client):
            out = dcp.health_check()
        self.assertFalse(out['ok'])
        dip, aip = out['locations']
        self.assertTrue(dip['ok'])
        self.assertFalse(aip['ok'])
        self.assertIn('AccessDenied', aip['error'])

    def test_empty_location_is_not_ok(self):
        # A mistyped prefix lists fine and returns nothing. Both DCP
        # locations hold thousands of objects, so empty means wrong.
        client, stub = self._stubbed_probe_client()
        stub.add_response('list_objects_v2', {'KeyCount': 0})
        with configured(DCP_AIP_LOCATION=None), \
                patch.object(dcp, 'make_client', return_value=client):
            out = dcp.health_check()
        self.assertFalse(out['ok'])
        (dip,) = out['locations']
        self.assertFalse(dip['ok'])
        self.assertEqual(dip['keys'], 0)
        self.assertIn('DCP_DIP_LOCATION', dip['error'])

    def test_only_configured_locations_are_probed(self):
        client, stub = self._stubbed_probe_client()
        stub.add_response('list_objects_v2', one_key_listing('dip-store/a'))
        with configured(DCP_AIP_LOCATION=None), \
                patch.object(dcp, 'make_client', return_value=client):
            out = dcp.health_check()
        self.assertTrue(out['ok'])
        self.assertEqual([loc['name'] for loc in out['locations']], ['dip'])
        stub.assert_no_pending_responses()

    def test_no_locations_is_not_ok(self):
        with configured(DCP_DIP_LOCATION=None, DCP_AIP_LOCATION=None), \
                patch.object(dcp, 'make_client', return_value=MagicMock()):
            out = dcp.health_check()
        self.assertFalse(out['ok'])
        self.assertIn('no location configured', out['error'])

    def test_malformed_endpoint_is_reported_not_raised(self):
        # botocore raises ValueError for an endpoint without a scheme; the
        # probe's contract is "never raises", so /health/dcp stays 200.
        with configured(DCP_ENDPOINT_URL='localhost:9000'):
            out = dcp.health_check()
        self.assertFalse(out['ok'])
        self.assertIn('ValueError', out['error'])
        self.assertIn('endpoint', out['error'].lower())


class StartupProbeTest(unittest.TestCase):
    """The log line each outcome of the boot-time probe produces."""

    def setUp(self):
        from app import _probe_dcp_at_startup
        self.probe = _probe_dcp_at_startup

    def test_skipped_when_not_configured(self):
        with configured(DCP_ACCESS_KEY_ID=None), self.assertLogs('app', level='INFO') as logs:
            self.probe()
        self.assertIn('dcp probe skipped', '\n'.join(logs.output))

    def test_ok_names_each_location(self):
        result = {
            'ok': True, 'elapsed_ms': 12,
            'locations': [
                {'name': 'dip', 'ok': True, 'error': None},
                {'name': 'aip', 'ok': True, 'error': None},
            ],
        }
        with configured(), patch.object(dcp, 'health_check', return_value=result), \
                self.assertLogs('app', level='INFO') as logs:
            self.probe()
        self.assertIn('dcp probe OK dip=ok, aip=ok', '\n'.join(logs.output))

    def test_failure_names_the_error(self):
        result = {
            'ok': False, 'elapsed_ms': 12,
            'locations': [
                {'name': 'dip', 'ok': True, 'error': None},
                {'name': 'aip', 'ok': False, 'error': 'list_objects_v2 failed (AccessDenied)'},
            ],
        }
        with configured(), patch.object(dcp, 'health_check', return_value=result), \
                self.assertLogs('app', level='WARNING') as logs:
            self.probe()
        self.assertIn('dcp probe FAILED dip=ok, aip=list_objects_v2 failed', '\n'.join(logs.output))

    def test_a_probe_that_throws_is_logged_not_raised(self):
        with configured(), patch.object(dcp, 'health_check', side_effect=RuntimeError('boom')), \
                self.assertLogs('app', level='WARNING') as logs:
            self.probe()
        self.assertIn('dcp probe threw unexpectedly: boom', '\n'.join(logs.output))


class HealthRouteTest(unittest.TestCase):
    def test_route_answers_200_with_the_probe_result(self):
        from app import create_app
        with configured(DCP_ACCESS_KEY_ID=None):
            app = create_app()
            app.testing = True
            resp = app.test_client().get('/health/dcp')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertFalse(data['ok'])
        self.assertFalse(data['configured'])

    def test_malformed_endpoint_still_answers_200(self):
        from app import create_app
        with configured(DCP_ENDPOINT_URL='localhost:9000'):
            app = create_app()
            app.testing = True
            resp = app.test_client().get('/health/dcp')
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.get_json()['ok'])

    def test_create_app_starts_no_probe_thread(self):
        # The probe thread starts once per process, at import, never from
        # create_app(): tests build apps under patched config.
        from app import create_app

        def probe_threads():
            return [t for t in threading.enumerate() if t.name == 'dcp-startup-probe']

        before = len(probe_threads())
        with configured():
            create_app()
        self.assertLessEqual(len(probe_threads()), before)


if __name__ == '__main__':
    unittest.main()
