#!/usr/bin/env python3
#
# Test: Swift SLO ?multipart-manifest=get returns manifest JSON.
#
# This test verifies that GET with ?multipart-manifest=get on a Static Large
# Object (SLO) returns the manifest JSON body (raw format) instead of
# assembling the segments.
#
# Requires a running RGW with Swift TempAuth.
# Setup:
#   radosgw-admin user create --subuser=test:tester \
#     --display-name=Tester-Subuser --key-type=swift \
#     --secret=testing --access=full
#
# Usage:
#   RGW_PORT=8000 python3 test_rgw_slo_manifest.py
#

import hashlib
import json
import os
import sys
import requests

RGW_HOST = os.environ.get('RGW_HOST', 'localhost')
RGW_PORT = os.environ.get('RGW_PORT', '8000')
AUTH_URL = f'http://{RGW_HOST}:{RGW_PORT}/auth/1.0'
ST_USER = 'test:tester'
ST_KEY = 'testing'

CONTAINER = 'slo-test-container'
SEGMENT_PREFIX = 'slo-segment-'
SLO_NAME = 'slo-manifest-obj'


def swift_auth():
    """Authenticate via TempAuth, return (storage_url, token)."""
    r = requests.get(AUTH_URL, headers={
        'X-Auth-User': ST_USER,
        'X-Auth-Key': ST_KEY,
    })
    if r.status_code not in (200, 204):
        print(f'FAILED: Auth returned {r.status_code}')
        sys.exit(1)
    return r.headers['X-Storage-Url'], r.headers['X-Auth-Token']


def cleanup(storage_url, token):
    """Best-effort cleanup of test objects."""
    headers = {'X-Auth-Token': token}
    # Delete SLO with segments
    requests.delete(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=delete',
        headers=headers,
    )
    # Delete container
    requests.delete(f'{storage_url}/{CONTAINER}', headers=headers)


def test_single_segment():
    """Test ?multipart-manifest=get with a single segment SLO."""
    print('TEST: single segment SLO manifest GET')

    storage_url, token = swift_auth()
    headers = {'X-Auth-Token': token}

    # Cleanup from any prior failed run
    cleanup(storage_url, token)

    # 1. Create container
    r = requests.put(f'{storage_url}/{CONTAINER}', headers=headers)
    assert r.status_code in (201, 202, 204), \
        f'Create container failed: {r.status_code}'

    # 2. Upload segment object
    segment_data = b'A' * 1024
    segment_name = f'{SEGMENT_PREFIX}001'
    segment_etag = hashlib.md5(segment_data).hexdigest()

    r = requests.put(
        f'{storage_url}/{CONTAINER}/{segment_name}',
        headers={**headers, 'Content-Type': 'application/octet-stream'},
        data=segment_data,
    )
    assert r.status_code == 201, f'Upload segment failed: {r.status_code}'

    # 3. Upload SLO manifest
    manifest = [
        {
            'path': f'{CONTAINER}/{segment_name}',
            'etag': segment_etag,
            'size_bytes': len(segment_data),
        }
    ]
    manifest_json = json.dumps(manifest)

    r = requests.put(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=put',
        headers={**headers, 'Content-Type': 'application/json'},
        data=manifest_json,
    )
    assert r.status_code == 201, \
        f'Upload SLO manifest failed: {r.status_code} {r.text}'

    # 4. Verify object is SLO (HEAD)
    r = requests.head(f'{storage_url}/{CONTAINER}/{SLO_NAME}', headers=headers)
    assert r.status_code == 200, f'HEAD SLO failed: {r.status_code}'
    slo_header = r.headers.get('X-Static-Large-Object', '')
    assert slo_header.lower() == 'true', \
        f'HEAD missing X-Static-Large-Object: {dict(r.headers)}'

    # 5. GET ?multipart-manifest=get — the core test
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers=headers,
    )
    assert r.status_code == 200, \
        f'GET ?multipart-manifest=get failed: {r.status_code}'
    assert int(r.headers.get('Content-Length', 0)) > 0, \
        f'Content-Length is 0! Headers: {dict(r.headers)}'
    assert 'application/json' in r.headers.get('Content-Type', ''), \
        f'Content-Type not JSON: {r.headers.get("Content-Type")}'
    assert r.headers.get('X-Static-Large-Object', '').lower() == 'true', \
        f'Missing X-Static-Large-Object on manifest GET'

    # 6. Verify JSON body content (raw format: path, etag, size_bytes)
    returned_manifest = r.json()
    assert isinstance(returned_manifest, list), \
        f'Expected JSON array, got: {type(returned_manifest)}'
    assert len(returned_manifest) == 1, \
        f'Expected 1 entry, got {len(returned_manifest)}'

    entry = returned_manifest[0]
    assert 'path' in entry, f'Missing "path" field in entry: {entry}'
    assert 'etag' in entry, f'Missing "etag" field in entry: {entry}'
    assert 'size_bytes' in entry, f'Missing "size_bytes" field in entry: {entry}'

    assert entry['path'] == f'{CONTAINER}/{segment_name}', \
        f'path mismatch: expected "{CONTAINER}/{segment_name}", got "{entry["path"]}"'
    assert entry['etag'] == segment_etag, \
        f'etag mismatch: expected "{segment_etag}", got "{entry["etag"]}"'
    assert entry['size_bytes'] == len(segment_data), \
        f'size_bytes mismatch: expected {len(segment_data)}, got {entry["size_bytes"]}'

    # 7. Verify normal GET still assembles segments
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}',
        headers=headers,
    )
    assert r.status_code == 200, f'Normal GET failed: {r.status_code}'
    assert r.content == segment_data, \
        f'Normal GET content mismatch: expected {len(segment_data)} bytes, ' \
        f'got {len(r.content)} bytes'

    # 8. Cleanup
    cleanup(storage_url, token)

    print('PASSED: single segment SLO manifest GET')


def test_multi_segment():
    """Test ?multipart-manifest=get with multiple segments."""
    print('TEST: multi-segment SLO manifest GET')

    storage_url, token = swift_auth()
    headers = {'X-Auth-Token': token}

    # Cleanup from any prior failed run
    cleanup(storage_url, token)

    # 1. Create container
    r = requests.put(f'{storage_url}/{CONTAINER}', headers=headers)
    assert r.status_code in (201, 202, 204), \
        f'Create container failed: {r.status_code}'

    # 2. Upload 3 segment objects
    segments = []
    for i in range(3):
        seg_data = bytes([0x41 + i]) * (512 * (i + 1))
        seg_name = f'{SEGMENT_PREFIX}{i:03d}'
        seg_etag = hashlib.md5(seg_data).hexdigest()

        r = requests.put(
            f'{storage_url}/{CONTAINER}/{seg_name}',
            headers={**headers, 'Content-Type': 'application/octet-stream'},
            data=seg_data,
        )
        assert r.status_code == 201, \
            f'Upload segment {i} failed: {r.status_code}'

        segments.append({
            'name': seg_name,
            'data': seg_data,
            'etag': seg_etag,
            'size': len(seg_data),
        })

    # 3. Upload SLO manifest
    manifest = [
        {
            'path': f'{CONTAINER}/{seg["name"]}',
            'etag': seg['etag'],
            'size_bytes': seg['size'],
        }
        for seg in segments
    ]
    manifest_json = json.dumps(manifest)

    r = requests.put(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=put',
        headers={**headers, 'Content-Type': 'application/json'},
        data=manifest_json,
    )
    assert r.status_code == 201, \
        f'Upload SLO manifest failed: {r.status_code} {r.text}'

    # 4. GET ?multipart-manifest=get
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers=headers,
    )
    assert r.status_code == 200, \
        f'GET ?multipart-manifest=get failed: {r.status_code}'

    returned_manifest = r.json()
    assert len(returned_manifest) == 3, \
        f'Expected 3 entries, got {len(returned_manifest)}'

    # Verify each entry matches
    for i, (entry, seg) in enumerate(zip(returned_manifest, segments)):
        assert entry['path'] == f'{CONTAINER}/{seg["name"]}', \
            f'Entry {i} path mismatch'
        assert entry['etag'] == seg['etag'], \
            f'Entry {i} etag mismatch'
        assert entry['size_bytes'] == seg['size'], \
            f'Entry {i} size_bytes mismatch'

    # 5. Verify normal GET assembles all segments in order
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}',
        headers=headers,
    )
    assert r.status_code == 200, f'Normal GET failed: {r.status_code}'
    expected_data = b''.join(seg['data'] for seg in segments)
    assert r.content == expected_data, \
        f'Normal GET content mismatch: expected {len(expected_data)} bytes, ' \
        f'got {len(r.content)} bytes'

    # 6. Cleanup
    cleanup(storage_url, token)

    print('PASSED: multi-segment SLO manifest GET')


if __name__ == '__main__':
    failed = False
    for test_fn in [test_single_segment, test_multi_segment]:
        try:
            test_fn()
        except AssertionError as e:
            print(f'FAILED: {e}')
            failed = True
        except Exception as e:
            print(f'ERROR: {type(e).__name__}: {e}')
            failed = True

    if failed:
        sys.exit(1)
    print('\nAll tests passed.')
