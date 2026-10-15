# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Tests for lib/storage_usage.py — the cached storage-utilization readout.
The S3 walk is faked at the wasabi._make_client and dcp.make_client
boundaries; the background thread is run inline via the injectable _spawn.

Run:
    python -m pytest tests/test_storage_usage.py -v
"""

import os
import tempfile
import unittest
from unittest.mock import patch

import config
from lib import dcp
from lib import storage_usage
from lib import wasabi


class FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, Bucket=None, Prefix=None):  # noqa: N803 - boto3 casing
        self.bucket = Bucket
        self.prefix = Prefix
        for page in self._pages:
            yield page


class FakeClient:
    """Returns canned pages keyed by (bucket, prefix)."""

    def __init__(self, listings):
        self._listings = listings
        self.calls = []

    def get_paginator(self, _name):
        outer = self

        class P:
            def paginate(self, Bucket=None, Prefix=None):  # noqa: N803
                outer.calls.append((Bucket, Prefix))
                for page in outer._listings.get((Bucket, Prefix), []):
                    yield page
        return P()


def _pages(sizes):
    return [{'Contents': [{'Key': f'k-{i}', 'Size': s} for i, s in enumerate(sizes)]}]


class StorageUsageTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        patches = [
            patch.object(storage_usage, '_CACHE_PATH',
                         os.path.join(self.tmp.name, 'usage.json')),
            patch.object(storage_usage, '_COMPUTING_MARKER',
                         os.path.join(self.tmp.name, 'usage.computing')),
            patch.object(config, 'WASABI_BUCKET', 's3://test-bucket/'),
            patch.object(config, 'WASABI_AIP_BUCKET', 's3://aip-bucket/aip-store/'),
            # No DCP key unless a test sets one, whatever this machine's env.
            patch.object(dcp, 'is_configured', return_value=False),
            # Run "background" recomputes inline for determinism.
            patch.object(storage_usage, '_spawn', lambda fn: fn()),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self.tmp.cleanup)

    def _client(self):
        return FakeClient({
            ('test-bucket', ''): _pages([100, 200, 300]),
            ('aip-bucket', 'aip-store/'): _pages([1_000_000, 2_000_000]),
        })

    def test_compute_sums_both_buckets_with_prefixes(self):
        client = self._client()
        with patch.object(wasabi, '_make_client', return_value=client):
            cache = storage_usage.compute_usage()
        self.assertEqual(cache['buckets']['batch_backups']['objects'], 3)
        self.assertEqual(cache['buckets']['batch_backups']['bytes'], 600)
        self.assertEqual(cache['buckets']['aip_store']['objects'], 2)
        self.assertEqual(cache['buckets']['aip_store']['bytes'], 3_000_000)
        # The AIP walk is scoped to its base prefix.
        self.assertIn(('aip-bucket', 'aip-store/'), client.calls)

    def test_one_broken_bucket_does_not_hide_the_other(self):
        client = FakeClient({
            ('aip-bucket', 'aip-store/'): _pages([5]),
        })

        real_get_paginator = client.get_paginator

        def selective(_name):
            p = real_get_paginator(_name)
            orig = p.paginate

            def paginate(Bucket=None, Prefix=None):  # noqa: N803
                if Bucket == 'test-bucket':
                    raise RuntimeError('listing exploded')
                return orig(Bucket=Bucket, Prefix=Prefix)
            p.paginate = paginate
            return p

        client.get_paginator = selective
        with patch.object(wasabi, '_make_client', return_value=client):
            cache = storage_usage.compute_usage()
        self.assertIn('error', cache['buckets']['batch_backups'])
        self.assertEqual(cache['buckets']['aip_store']['bytes'], 5)

    def _dcp(self, dip='s3://dcp-dip/dip-store/', aip='s3://dcp-aip/aip-store/'):
        # The DCP key is set, with these locations.
        return [
            patch.object(dcp, 'is_configured', return_value=True),
            patch.object(config, 'DCP_DIP_LOCATION', dip),
            patch.object(config, 'DCP_AIP_LOCATION', aip),
        ]

    def test_dcp_locations_are_measured_with_the_dcp_client(self):
        wasabi_client = self._client()
        dcp_client = FakeClient({
            ('dcp-dip', 'dip-store/'): _pages([10, 20]),
            ('dcp-aip', 'aip-store/'): _pages([3_000]),
        })
        for p in self._dcp():
            p.start()
            self.addCleanup(p.stop)
        with patch.object(wasabi, '_make_client', return_value=wasabi_client), \
                patch.object(dcp, 'make_client', return_value=dcp_client):
            cache = storage_usage.compute_usage()
        self.assertEqual(cache['buckets']['dcp_dip_store']['objects'], 2)
        self.assertEqual(cache['buckets']['dcp_dip_store']['bytes'], 30)
        self.assertEqual(cache['buckets']['dcp_aip_store']['bytes'], 3_000)
        # Each store is listed with its own client, on its own prefix.
        self.assertEqual(
            sorted(dcp_client.calls),
            [('dcp-aip', 'aip-store/'), ('dcp-dip', 'dip-store/')],
        )
        self.assertEqual(
            sorted(wasabi_client.calls),
            [('aip-bucket', 'aip-store/'), ('test-bucket', '')],
        )
        self.assertEqual(cache['buckets']['aip_store']['bytes'], 3_000_000)

    def test_dcp_is_left_out_until_curation_has_its_key(self):
        with patch.object(config, 'DCP_DIP_LOCATION', 's3://dcp-dip/dip-store/'), \
                patch.object(wasabi, '_make_client', return_value=self._client()):
            cache = storage_usage.compute_usage()
        self.assertNotIn('dcp_dip_store', cache['buckets'])
        self.assertNotIn('dcp_aip_store', cache['buckets'])

    def test_a_dcp_failure_does_not_hide_the_wasabi_numbers(self):
        for p in self._dcp(aip=None):
            p.start()
            self.addCleanup(p.stop)
        with patch.object(wasabi, '_make_client', return_value=self._client()), \
                patch.object(dcp, 'make_client', side_effect=RuntimeError('AccessDenied')):
            cache = storage_usage.compute_usage()
        self.assertIn('AccessDenied', cache['buckets']['dcp_dip_store']['error'])
        # An unset location is an error naming it, once the key is set.
        self.assertEqual(
            cache['buckets']['dcp_aip_store'], {'error': 'DCP_AIP_LOCATION is not configured'}
        )
        self.assertEqual(cache['buckets']['aip_store']['bytes'], 3_000_000)

    def test_get_usage_triggers_recompute_when_cache_missing(self):
        with patch.object(wasabi, '_make_client', return_value=self._client()):
            result = storage_usage.get_usage()
        # Inline _spawn means the recompute already ran; the cache file
        # now exists for the NEXT read even though this read was stale.
        self.assertTrue(result['stale'])
        followup = storage_usage.get_usage()
        self.assertFalse(followup['stale'])
        self.assertEqual(
            followup['usage']['buckets']['aip_store']['bytes'], 3_000_000
        )

    def test_fresh_cache_is_served_without_recompute(self):
        with patch.object(wasabi, '_make_client', return_value=self._client()):
            storage_usage.compute_usage()
        calls = []
        with patch.object(storage_usage, 'trigger_recompute',
                          side_effect=lambda **kw: calls.append(1)):
            result = storage_usage.get_usage()
        self.assertFalse(result['stale'])
        self.assertEqual(calls, [])

    def test_a_fresh_cache_without_the_dcp_locations_is_stale_once_the_key_is_set(self):
        # The cache predates the key: it must rebuild itself, not wait a day.
        with patch.object(wasabi, '_make_client', return_value=self._client()):
            storage_usage.compute_usage()  # the Wasabi buckets only
        for p in self._dcp():
            p.start()
            self.addCleanup(p.stop)
        dcp_client = FakeClient({
            ('dcp-dip', 'dip-store/'): _pages([10]),
            ('dcp-aip', 'aip-store/'): _pages([20]),
        })
        with patch.object(wasabi, '_make_client', return_value=self._client()), \
                patch.object(dcp, 'make_client', return_value=dcp_client):
            result = storage_usage.get_usage()
        self.assertTrue(result['stale'])
        # Inline _spawn: the walk ran, and the next read has the DCP numbers.
        followup = storage_usage.get_usage()
        self.assertFalse(followup['stale'])
        self.assertEqual(followup['usage']['buckets']['dcp_dip_store']['bytes'], 10)
        self.assertEqual(followup['usage']['buckets']['dcp_aip_store']['bytes'], 20)

    def test_a_failed_dcp_location_does_not_keep_the_cache_stale(self):
        # Its error entry counts as an entry: no walk after walk.
        for p in self._dcp():
            p.start()
            self.addCleanup(p.stop)
        with patch.object(wasabi, '_make_client', return_value=self._client()), \
                patch.object(dcp, 'make_client', side_effect=RuntimeError('AccessDenied')):
            storage_usage.compute_usage()
        calls = []
        with patch.object(storage_usage, 'trigger_recompute',
                          side_effect=lambda **kw: calls.append(1)):
            result = storage_usage.get_usage()
        self.assertFalse(result['stale'])
        self.assertEqual(calls, [])

    def test_unset_dcp_locations_are_recorded_as_errors_and_then_count_as_measured(self):
        for p in self._dcp(dip=None, aip=None):
            p.start()
            self.addCleanup(p.stop)
        with patch.object(wasabi, '_make_client', return_value=self._client()):
            cache = storage_usage.compute_usage()
        self.assertEqual(
            cache['buckets']['dcp_dip_store'], {'error': 'DCP_DIP_LOCATION is not configured'}
        )
        self.assertEqual(
            cache['buckets']['dcp_aip_store'], {'error': 'DCP_AIP_LOCATION is not configured'}
        )
        # Those entries count: no walk after walk.
        calls = []
        with patch.object(storage_usage, 'trigger_recompute',
                          side_effect=lambda **kw: calls.append(1)):
            result = storage_usage.get_usage()
        self.assertFalse(result['stale'])
        self.assertEqual(calls, [])

    def test_marker_debounces_concurrent_recomputes(self):
        with open(storage_usage._COMPUTING_MARKER, 'w') as f:
            f.write('now')
        spawned = []
        with patch.object(storage_usage, '_spawn',
                          side_effect=lambda fn: spawned.append(fn)):
            storage_usage.trigger_recompute()
        self.assertEqual(spawned, [])  # already running — not spawned again
        with patch.object(storage_usage, '_spawn',
                          side_effect=lambda fn: spawned.append(fn)):
            storage_usage.trigger_recompute(force=True)
        self.assertEqual(len(spawned), 1)  # force overrides


class BucketUsageRouteTests(unittest.TestCase):
    def setUp(self):
        from flask import Flask
        from routes.aip import aip_bp
        app = Flask(__name__)
        app.register_blueprint(aip_bp)
        app.testing = True
        self.client = app.test_client()
        self.env = patch.dict(os.environ, {'API_KEY': 'test-key-123'})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_requires_api_key(self):
        self.assertEqual(
            self.client.get('/api/v2/aip/bucket-usage').status_code, 403
        )

    def test_get_returns_cache_shape(self):
        fake = {'usage': {'computed_at': 1, 'buckets': {}},
                'computing': False, 'stale': False}
        with patch.object(storage_usage, 'get_usage', return_value=fake):
            res = self.client.get(
                '/api/v2/aip/bucket-usage',
                headers={'X-API-Key': 'test-key-123'},
            )
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertTrue(body['ok'])
        self.assertFalse(body['computing'])

    def test_refresh_forces_recompute(self):
        forced = []
        fake = {'usage': None, 'computing': True, 'stale': True}
        with patch.object(storage_usage, 'trigger_recompute',
                          side_effect=lambda force=False: forced.append(force)), \
                patch.object(storage_usage, 'get_usage', return_value=fake):
            res = self.client.post(
                '/api/v2/aip/bucket-usage/refresh',
                headers={'X-API-Key': 'test-key-123'},
            )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(forced, [True])


if __name__ == '__main__':
    unittest.main()
