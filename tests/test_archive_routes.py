# Copyright 2026 University of Denver
# Licensed under the Apache License, Version 2.0.
"""
Route tests for the read-only batch-archive browser
(routes/archive.py — repo/WASABI_ARCHIVE_BROWSER_PLAN.md).

The wasabi layer is faked at the module boundary (list_all_prefixes /
list_prefixes / list_objects / generate_presigned_url), so no AWS
credentials or network access are needed.

Run:
    python -m pytest tests/test_archive_routes.py -v
"""

import os
import unittest
from unittest.mock import patch

from flask import Flask

from routes.archive import archive_bp
from lib import wasabi


API_KEY = 'test-key-123'


def _make_client():
    app = Flask(__name__)
    app.register_blueprint(archive_bp)
    app.testing = True
    return app.test_client()


class ArchiveRoutesTests(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()
        self.env = patch.dict(os.environ, {'API_KEY': API_KEY})
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def _get(self, url):
        return self.client.get(url, headers={'X-API-Key': API_KEY})

    def _post(self, url, json_body):
        return self.client.post(url, json=json_body, headers={'X-API-Key': API_KEY})

    # --- auth ---------------------------------------------------------

    def test_requires_api_key(self):
        res = self.client.get('/api/v2/archive/collections')
        self.assertEqual(res.status_code, 403)

    # --- collections --------------------------------------------------

    def test_collections_listing(self):
        with patch.object(wasabi, 'list_all_prefixes', return_value=['codu_a', 'codu_b']):
            res = self._get('/api/v2/archive/collections')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json(), {
            'result': {'collections': ['codu_a', 'codu_b']},
            'errors': [],
        })

    def test_collections_wasabi_failure_is_502(self):
        with patch.object(wasabi, 'list_all_prefixes', side_effect=RuntimeError('no creds')):
            res = self._get('/api/v2/archive/collections')
        self.assertEqual(res.status_code, 502)

    # --- packages -----------------------------------------------------

    def test_packages_listing_with_token_passthrough(self):
        with patch.object(
            wasabi, 'list_prefixes',
            return_value={'prefixes': ['pkg_a', 'pkg_b'], 'next_token': 'tok2'},
        ) as m:
            res = self._get('/api/v2/archive/collections/codu_a/packages?token=tok1')
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body['result']['packages'], ['pkg_a', 'pkg_b'])
        self.assertEqual(body['result']['next_token'], 'tok2')
        m.assert_called_once_with('codu_a/', continuation_token='tok1')

    def test_packages_rejects_traversal_collection(self):
        res = self._get('/api/v2/archive/collections/../packages')
        # Flask may resolve `..` at the URL layer (404) or our validator
        # rejects it (400) — either way it must not reach the wasabi layer.
        self.assertIn(res.status_code, (400, 404))

    # --- files --------------------------------------------------------

    def test_files_listing_includes_folders_and_metadata(self):
        objects_page = {
            'objects': [
                {'name': 'scan1.tif', 'key': 'codu_a/pkg_a/scan1.tif',
                 'size': 5, 'last_modified': '2026-07-26T00:00:00+00:00'},
            ],
            'next_token': None,
        }
        with patch.object(wasabi, 'list_objects', return_value=objects_page), \
                patch.object(wasabi, 'list_prefixes', return_value={'prefixes': ['nested'], 'next_token': None}):
            res = self._get('/api/v2/archive/collections/codu_a/packages/pkg_a/files')
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body['result']['files'], objects_page['objects'])
        self.assertEqual(body['result']['folders'], ['nested'])
        self.assertIsNone(body['result']['next_token'])

    def test_files_continuation_skips_folder_relisting(self):
        with patch.object(
            wasabi, 'list_objects',
            return_value={'objects': [], 'next_token': None},
        ), patch.object(wasabi, 'list_prefixes') as m_prefixes:
            res = self._get(
                '/api/v2/archive/collections/codu_a/packages/pkg_a/files?token=t2'
            )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json()['result']['folders'], [])
        m_prefixes.assert_not_called()

    # --- download-url -------------------------------------------------

    def test_download_url_happy_path(self):
        with patch.object(
            wasabi, 'generate_presigned_url',
            return_value='https://wasabi.example/signed',
        ) as m:
            res = self._post('/api/v2/archive/download-url',
                             {'key': 'codu_a/pkg_a/scan1.tif'})
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertTrue(body['ok'])
        self.assertEqual(body['url'], 'https://wasabi.example/signed')
        self.assertIn('expires_at', body)
        m.assert_called_once_with('codu_a/pkg_a/scan1.tif', ttl_seconds=900)

    def test_download_url_clamps_ttl(self):
        with patch.object(
            wasabi, 'generate_presigned_url', return_value='u'
        ) as m:
            self._post('/api/v2/archive/download-url',
                       {'key': 'c/p/f.tif', 'ttl_seconds': 999999})
        m.assert_called_once_with('c/p/f.tif', ttl_seconds=3600)

    def test_download_url_rejects_bad_keys(self):
        bad_keys = [
            None,
            '',
            '/absolute/path.tif',
            'trailing/slash/',
            'single-segment',
            'a/../b.tif',
            'a/b\\c.tif',
            'a/-b/c.tif',
            'a/b\x00c.tif',
        ]
        for key in bad_keys:
            res = self._post('/api/v2/archive/download-url', {'key': key})
            self.assertEqual(res.status_code, 400, f'key {key!r} should be rejected')
            self.assertFalse(res.get_json()['ok'])

    def test_download_url_wasabi_failure_is_ok_false(self):
        with patch.object(
            wasabi, 'generate_presigned_url', side_effect=RuntimeError('no creds')
        ):
            res = self._post('/api/v2/archive/download-url', {'key': 'c/p/f.tif'})
        self.assertEqual(res.status_code, 200)
        self.assertFalse(res.get_json()['ok'])


class WasabiListingHelperTests(unittest.TestCase):
    """Unit tests for the new listing helpers with a fake boto3 client."""

    def _fake_client(self, pages):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def list_objects_v2(self, **kwargs):
                self.calls.append(kwargs)
                return pages.pop(0)

        return FakeClient()

    def test_list_prefixes_strips_parent_and_slash(self):
        client = self._fake_client([{
            'CommonPrefixes': [{'Prefix': 'codu_a/pkg_1/'}, {'Prefix': 'codu_a/pkg_2/'}],
            'IsTruncated': True,
            'NextContinuationToken': 'tok',
        }])
        with patch.object(wasabi, '_make_client', return_value=client), \
                patch.object(wasabi, '_resolve_bucket', return_value=('bucket', '')):
            page = wasabi.list_prefixes('codu_a/')
        self.assertEqual(page['prefixes'], ['pkg_1', 'pkg_2'])
        self.assertEqual(page['next_token'], 'tok')
        self.assertEqual(client.calls[0]['Delimiter'], '/')
        self.assertEqual(client.calls[0]['Prefix'], 'codu_a/')

    def test_list_all_prefixes_follows_pagination(self):
        client = self._fake_client([
            {'CommonPrefixes': [{'Prefix': 'a/'}], 'IsTruncated': True,
             'NextContinuationToken': 't2'},
            {'CommonPrefixes': [{'Prefix': 'b/'}], 'IsTruncated': False},
        ])
        with patch.object(wasabi, '_make_client', return_value=client), \
                patch.object(wasabi, '_resolve_bucket', return_value=('bucket', '')):
            names = wasabi.list_all_prefixes('')
        self.assertEqual(names, ['a', 'b'])
        self.assertEqual(client.calls[1].get('ContinuationToken'), 't2')

    def test_list_objects_builds_entries_and_skips_marker(self):
        from datetime import datetime, timezone
        client = self._fake_client([{
            'Contents': [
                {'Key': 'base/c/p/', 'Size': 0},  # directory marker
                {'Key': 'base/c/p/f.tif', 'Size': 5,
                 'LastModified': datetime(2026, 7, 26, tzinfo=timezone.utc)},
            ],
            'IsTruncated': False,
        }])
        with patch.object(wasabi, '_make_client', return_value=client), \
                patch.object(wasabi, '_resolve_bucket', return_value=('bucket', 'base/')):
            page = wasabi.list_objects('c/p/')
        self.assertEqual(len(page['objects']), 1)
        entry = page['objects'][0]
        self.assertEqual(entry['name'], 'f.tif')
        self.assertEqual(entry['key'], 'c/p/f.tif')
        self.assertEqual(entry['size'], 5)
        self.assertTrue(entry['last_modified'].startswith('2026-07-26'))


if __name__ == '__main__':
    unittest.main()


class PackageSearchTests(unittest.TestCase):
    """Server-side package prefix search (2026-07-30): ?q= on the
    packages level queries S3 by name prefix instead of filtering the
    loaded page — a fresh backup in a thousands-of-packages migrated
    collection was invisible behind pagination."""

    def setUp(self):
        self.client = _make_client()
        self.env = patch.dict(os.environ, {'API_KEY': API_KEY})
        self.env.start()

    def tearDown(self):
        self.env.stop()

    def _get(self, url):
        return self.client.get(url, headers={'X-API-Key': API_KEY})

    def test_q_routes_to_search_prefixes(self):
        fake = {'prefixes': ['B002.01.0103.0160'], 'next_token': None}
        with patch.object(wasabi, 'search_prefixes', return_value=fake) as m:
            res = self._get('/api/v2/archive/collections/coll_a/packages?q=B002.01.0103')
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body['result']['packages'], ['B002.01.0103.0160'])
        args, kwargs = m.call_args
        self.assertEqual(args[0], 'coll_a/')
        self.assertEqual(args[1], 'B002.01.0103')

    def test_without_q_uses_plain_listing(self):
        fake = {'prefixes': ['p1'], 'next_token': 'tok'}
        with patch.object(wasabi, 'list_prefixes', return_value=fake) as m:
            res = self._get('/api/v2/archive/collections/coll_a/packages')
        self.assertEqual(res.status_code, 200)
        self.assertTrue(m.called)

    def test_unsafe_q_is_rejected(self):
        res = self._get('/api/v2/archive/collections/coll_a/packages?q=..%2Fetc')
        self.assertEqual(res.status_code, 400)
