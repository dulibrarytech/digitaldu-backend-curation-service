# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Unit tests for lib/dcp.py, the DuraCloud Preserve (AWS S3) client.

S3 is stubbed with botocore's Stubber on a real boto3 client, so no
network or real credentials are needed. config values are patched per
test; nothing reads them from the environment.
"""

import io
import os
import unittest
from unittest.mock import MagicMock, patch

from botocore.response import StreamingBody
from botocore.stub import Stubber

import config
from lib import dcp

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
    def setUp(self):
        patcher = patch.object(dcp, 'boto3')
        self.boto3_mock = patcher.start()
        self.addCleanup(patcher.stop)

    def test_passes_explicit_keys_and_region_only(self):
        with configured():
            dcp.make_client()
        session_kwargs = self.boto3_mock.Session.call_args.kwargs
        self.assertEqual(session_kwargs, {
            'aws_access_key_id': 'AKIATESTKEY',
            'aws_secret_access_key': 'test-secret',
            'region_name': 'us-east-1',
        })
        client_call = self.boto3_mock.Session.return_value.client.call_args
        self.assertEqual(client_call.args, ('s3',))
        self.assertIs(client_call.kwargs['config'], dcp._DATA_CONFIG)
        # Blank DCP_ENDPOINT_URL means AWS's own endpoint: none passed.
        self.assertNotIn('endpoint_url', client_call.kwargs)

    def test_probe_client_uses_the_short_single_attempt_config(self):
        with configured():
            dcp.make_client(probe=True)
        client_call = self.boto3_mock.Session.return_value.client.call_args
        self.assertIs(client_call.kwargs['config'], dcp._PROBE_CONFIG)

    def test_endpoint_override_is_passed_through(self):
        with configured(DCP_ENDPOINT_URL='http://127.0.0.1:9000'):
            dcp.make_client()
        client_call = self.boto3_mock.Session.return_value.client.call_args
        self.assertEqual(client_call.kwargs['endpoint_url'], 'http://127.0.0.1:9000')

    def test_raises_when_not_configured(self):
        with configured(DCP_ACCESS_KEY_ID=''):
            with self.assertRaises(RuntimeError) as ctx:
                dcp.make_client()
        self.assertIn('DCP_ACCESS_KEY_ID', str(ctx.exception))
        self.boto3_mock.Session.assert_not_called()

    def test_profile_env_vars_are_hidden_while_building_then_restored(self):
        seen = {}

        def fake_session(**kwargs):
            seen['profile'] = os.environ.get('AWS_PROFILE')
            return MagicMock()

        self.boto3_mock.Session.side_effect = fake_session
        with configured(), patch.dict(os.environ, {'AWS_PROFILE': 'wasabi'}):
            dcp.make_client()
            self.assertEqual(os.environ.get('AWS_PROFILE'), 'wasabi')
        self.assertIsNone(seen['profile'])

    def test_configs_ignore_endpoint_urls_from_the_environment(self):
        # A stray AWS_ENDPOINT_URL_S3 meant for Wasabi must not redirect DCP.
        self.assertTrue(dcp._DATA_CONFIG.ignore_configured_endpoint_urls)
        self.assertTrue(dcp._PROBE_CONFIG.ignore_configured_endpoint_urls)


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
            'list_objects_v2', {'KeyCount': 1},
            {'Bucket': 'dcp-test-dip-store', 'Prefix': 'dip-store/', 'MaxKeys': 1},
        )
        stub.add_response(
            'list_objects_v2', {'KeyCount': 1},
            {'Bucket': 'dcp-test-aip-store', 'Prefix': 'aip-store/', 'MaxKeys': 1},
        )
        with configured(), patch.object(dcp, 'make_client', return_value=client):
            out = dcp.health_check()
        self.assertTrue(out['ok'])
        self.assertIsNone(out['error'])
        self.assertEqual([loc['name'] for loc in out['locations']], ['dip', 'aip'])
        self.assertTrue(all(loc['ok'] for loc in out['locations']))

    def test_access_denied_on_one_location_fails_the_check(self):
        client, stub = self._stubbed_probe_client()
        stub.add_response('list_objects_v2', {'KeyCount': 0})
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

    def test_only_configured_locations_are_probed(self):
        client, stub = self._stubbed_probe_client()
        stub.add_response('list_objects_v2', {'KeyCount': 0})
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


if __name__ == '__main__':
    unittest.main()
