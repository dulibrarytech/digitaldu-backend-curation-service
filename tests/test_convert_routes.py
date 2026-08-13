# Copyright 2026 University of Denver
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""
Tests for the derivative conversion + serving routes and convert_ops.

The contract shapes matter more than usual here: repo-backend-v2's
convert worker classifies GET /image responses (200/206 real,
400 'File is empty', 404 JSON missing) and treats POST /convert/tiff
statuses (200/4xx/507) as the truthful conversion outcome. These tests
pin every shape. DuraCloud fetches are monkeypatched; storage is a real
tmp dir so the atomic-write behavior is exercised for real.
"""

import io
import json
import os

import pytest
from PIL import Image

import config
from lib import convert_ops


AUTH = {'X-API-Key': 'test-key'}


def tiny_tiff_bytes(mode='RGB', size=(8, 8)):
    buffer = io.BytesIO()
    Image.new(mode, size, color=128).save(buffer, format='TIFF')
    return buffer.getvalue()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'DERIVATIVE_STORAGE_PATH', str(tmp_path))
    monkeypatch.setattr(config, 'DERIVATIVE_MIN_FREE_BYTES', 0)
    monkeypatch.setattr(config, 'DURACLOUD_API', 'dc.example.org/durastore/dip-store/')
    monkeypatch.setattr(config, 'DURACLOUD_USER', 'user')
    monkeypatch.setattr(config, 'DURACLOUD_PWD', 'pwd')
    from app import create_app
    app = create_app()
    app.testing = True
    return app.test_client()


class FakeResponse:
    def __init__(self, status_code=200, data=b'', headers=None):
        self.status_code = status_code
        self._data = data
        self.headers = headers if headers is not None else {
            'content-length': str(len(data)),
        }

    def iter_content(self, chunk_size):
        for i in range(0, len(self._data), chunk_size):
            yield self._data[i:i + chunk_size]

    def close(self):
        pass


PAYLOAD = {
    'sip_uuid': '5d771949-70c2-454d-b402-2d457dd22112',
    'full_path': 'f823/dips/objects/8a5cee8f-B463.00001.tif',
    'object_name': '8a5cee8f-B463.00001.tif',
    'mime_type': 'image/tiff',
}


# ---- POST /api/v1/convert/tiff ---------------------------------------------

def test_convert_requires_api_key(client):
    assert client.post('/api/v1/convert/tiff', json=PAYLOAD).status_code == 401


def test_convert_happy_path_writes_verified_jpg(client, tmp_path, monkeypatch):
    monkeypatch.setattr(
        convert_ops.requests, 'get',
        lambda *a, **k: FakeResponse(data=tiny_tiff_bytes()),
    )
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 200
    body = response.get_json()
    assert body['error'] is False
    assert body['data']['file_name'] == '8a5cee8f-B463.00001.jpg'
    assert body['data']['bytes'] > 0

    on_disk = tmp_path / '8a5cee8f-B463.00001.jpg'
    assert on_disk.stat().st_size == body['data']['bytes']
    # Output decodes as a real JPEG; no temp file left behind.
    assert Image.open(on_disk).format == 'JPEG'
    assert not (tmp_path / '8a5cee8f-B463.00001.jpg.tmp').exists()


def test_convert_normalizes_cmyk_sources(client, tmp_path, monkeypatch):
    monkeypatch.setattr(
        convert_ops.requests, 'get',
        lambda *a, **k: FakeResponse(data=tiny_tiff_bytes(mode='CMYK')),
    )
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 200


def test_convert_validation_shapes(client):
    response = client.post('/api/v1/convert/tiff', json={'sip_uuid': ''}, headers=AUTH)
    assert response.status_code == 400
    body = response.get_json()
    assert body['error'] is True
    assert any('full_path' in e for e in body['errors'])


def test_convert_missing_source_is_404(client, monkeypatch):
    monkeypatch.setattr(
        convert_ops.requests, 'get',
        lambda *a, **k: FakeResponse(status_code=404),
    )
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 404
    assert response.get_json()['error'] is True


def test_convert_refuses_when_storage_low(client, monkeypatch):
    monkeypatch.setattr(config, 'DERIVATIVE_MIN_FREE_BYTES', 2 ** 62)
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 507
    assert 'Insufficient storage' in response.get_json()['message']


def test_convert_source_over_cap_is_400(client, monkeypatch):
    monkeypatch.setattr(config, 'DERIVATIVE_MAX_SOURCE_BYTES', 4)
    monkeypatch.setattr(
        convert_ops.requests, 'get',
        lambda *a, **k: FakeResponse(data=b'0123456789'),
    )
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 400


def test_convert_corrupt_tiff_is_422_and_writes_nothing(client, tmp_path, monkeypatch):
    monkeypatch.setattr(
        convert_ops.requests, 'get',
        lambda *a, **k: FakeResponse(data=b'this is not a tiff'),
    )
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 422
    assert 'undecodable' in response.get_json()['message']
    assert list(tmp_path.iterdir()) == []


def test_convert_truncated_tiff_is_422_naming_corruption(client, tmp_path, monkeypatch):
    # The 2026-08-12 incident shape: a valid TIFF stored as a prefix of
    # itself. The magic bytes survive, the directory does not.
    truncated = tiny_tiff_bytes()[:20]
    monkeypatch.setattr(
        convert_ops.requests, 'get',
        lambda *a, **k: FakeResponse(data=truncated),
    )
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 422
    body = response.get_json()
    assert body['error'] is True
    assert 'corrupt or truncated' in body['message']
    assert list(tmp_path.iterdir()) == []


def test_convert_short_fetch_is_500_naming_transit_truncation(client, tmp_path, monkeypatch):
    # Body shorter than the declared content-length: in-transit loss,
    # distinct from corruption at rest (worth a retry, so not 422).
    data = tiny_tiff_bytes()
    monkeypatch.setattr(
        convert_ops.requests, 'get',
        lambda *a, **k: FakeResponse(
            data=data, headers={'content-length': str(len(data) + 10)},
        ),
    )
    response = client.post('/api/v1/convert/tiff', json=PAYLOAD, headers=AUTH)
    assert response.status_code == 500
    assert 'truncated in transit' in response.get_json()['message']
    assert list(tmp_path.iterdir()) == []


# ---- GET /api/v1/image ------------------------------------------------------

def test_image_missing_is_json_404(client):
    response = client.get('/api/v1/image?filename=nope.jpg', headers=AUTH)
    assert response.status_code == 404
    assert response.get_json()['error'] is True


def test_image_empty_file_is_the_verifier_400_shape(client, tmp_path):
    (tmp_path / 'empty.jpg').write_bytes(b'')
    response = client.get('/api/v1/image?filename=empty.jpg', headers=AUTH)
    assert response.status_code == 400
    assert response.get_json()['errors'] == ['File is empty']


def test_image_serves_real_file_with_range_support(client, tmp_path):
    (tmp_path / 'real.jpg').write_bytes(b'JFIF-REAL-BYTES-HERE')
    full = client.get('/api/v1/image?filename=real.jpg', headers=AUTH)
    assert full.status_code == 200
    assert full.data == b'JFIF-REAL-BYTES-HERE'
    assert full.headers['Content-Type'] == 'image/jpeg'

    ranged = client.get(
        '/api/v1/image?filename=real.jpg',
        headers={**AUTH, 'Range': 'bytes=0-0'},
    )
    assert ranged.status_code == 206
    assert ranged.headers['Content-Range'].endswith('/20')


def test_image_rejects_traversal_and_non_jpg(client):
    for name in ('../secret.jpg', 'a/b.jpg', 'thing.tif', ''):
        response = client.get('/api/v1/image?filename=' + name, headers=AUTH)
        assert response.status_code == 400, name


# ---- GET /api/v1/convert/status --------------------------------------------

def test_status_reports_free_space(client):
    response = client.get('/api/v1/convert/status', headers=AUTH)
    assert response.status_code == 200
    assert json.loads(response.data)['ok'] is True


# ---- convert_ops unit details ----------------------------------------------

def test_derivative_name_normalizes():
    assert convert_ops.derivative_name('a/b/thing.tif') == 'thing.jpg'
    assert convert_ops.derivative_name('thing.TIFF') == 'thing.jpg'
    with pytest.raises(convert_ops.InvalidRequest):
        convert_ops.derivative_name('')


def test_write_derivative_cleans_up_temp_on_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'DERIVATIVE_STORAGE_PATH', str(tmp_path))

    real_stat = os.stat

    def lying_stat(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if str(path).endswith('.tmp'):
            # Simulate ENOSPC's short-write signature.
            class Fake:
                st_size = result.st_size - 1
            return Fake()
        return result

    monkeypatch.setattr(convert_ops.os, 'stat', lying_stat)
    with pytest.raises(convert_ops.ConvertError, match='short write'):
        convert_ops.write_derivative(b'JPEGBYTES', 'x.tif')
    assert list(tmp_path.iterdir()) == []
