#!/usr/bin/env python3
#
# Test: Swift SLO ?multipart-manifest=get returns manifest JSON.
#
# This test verifies that GET with ?multipart-manifest=get on a Static Large
# Object (SLO) returns the manifest JSON body instead of assembling the
# segments, in both of swift's formats:
#  - plain multipart-manifest=get: user format with name/hash/bytes keys
#    and Content-Type: application/json; charset=utf-8
#  - &format=raw: put format with path/etag/size_bytes keys and the
#    manifest object's own content type
# and that Range headers apply to the manifest body (206/416).
#
# Requires a running RGW with Swift TempAuth.
#
# In teuthology this is driven by test_rgw_slo_manifest.sh, which creates the
# Swift subuser, installs 'requests' into a venv, and exports RGW_HOST/RGW_PORT.
#
# To run manually against a local vstart cluster:
#   radosgw-admin user create --subuser=test:tester \
#     --display-name=Tester-Subuser --key-type=swift \
#     --secret=testing --access=full
#   RGW_PORT=8000 python3 test_rgw_slo_manifest.py
#

import hashlib
import json
import os
import sys
import requests

RGW_HOST = os.environ.get('RGW_HOST', 'localhost')
RGW_PORT = os.environ.get('RGW_PORT', '80')
AUTH_URL = f'http://{RGW_HOST}:{RGW_PORT}/auth/1.0'
ST_USER = 'test:tester'
ST_KEY = 'testing'

CONTAINER = 'slo-test-container'
SEGMENT_PREFIX = 'slo-segment-'
SLO_NAME = 'slo-manifest-obj'

# content-type used when PUTting the SLO manifest. deliberately not the
# content-type that plain ?multipart-manifest=get forces back
# (application/json; charset=utf-8), so that the raw response (which echoes
# the stored type) and the plain response (which overrides it) are provably
# distinct rather than coincidentally equal.
MANIFEST_PUT_CONTENT_TYPE = 'application/x-slo-manifest+json'
PLAIN_GET_CONTENT_TYPE = 'application/json; charset=utf-8'


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
    segment_etag = hashlib.md5(segment_data, usedforsecurity=False).hexdigest()

    r = requests.put(
        f'{storage_url}/{CONTAINER}/{segment_name}',
        headers={**headers, 'Content-Type': 'application/octet-stream'},
        data=segment_data,
    )
    assert r.status_code == 201, f'Upload segment failed: {r.status_code}'

    # 3. Upload SLO manifest (leading-slash paths, like real swift clients)
    manifest = [
        {
            'path': f'/{CONTAINER}/{segment_name}',
            'etag': segment_etag,
            'size_bytes': len(segment_data),
        }
    ]
    manifest_json = json.dumps(manifest)

    r = requests.put(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=put',
        headers={**headers, 'Content-Type': MANIFEST_PUT_CONTENT_TYPE},
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

    # 5. GET ?multipart-manifest=get — the core test (user format:
    #    name/hash/bytes, the keys python-swiftclient depends on)
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers=headers,
    )
    assert r.status_code == 200, \
        f'GET ?multipart-manifest=get failed: {r.status_code}'
    assert int(r.headers.get('Content-Length', 0)) > 0, \
        f'Content-Length is 0! Headers: {dict(r.headers)}'
    assert r.headers.get('Content-Type') == PLAIN_GET_CONTENT_TYPE, \
        f'Content-Type mismatch: {r.headers.get("Content-Type")}'
    assert r.headers.get('X-Static-Large-Object', '').lower() == 'true', \
        'Missing X-Static-Large-Object on manifest GET'

    manifest_len = len(r.content)
    full_body = r.content
    plain_content_type = r.headers.get('Content-Type')

    returned_manifest = r.json()
    assert isinstance(returned_manifest, list), \
        f'Expected JSON array, got: {type(returned_manifest)}'
    assert len(returned_manifest) == 1, \
        f'Expected 1 entry, got {len(returned_manifest)}'

    entry = returned_manifest[0]
    assert set(entry.keys()) == {'name', 'hash', 'bytes'}, \
        f'Expected name/hash/bytes keys, got: {entry}'
    assert entry['name'] == f'/{CONTAINER}/{segment_name}', \
        f'name mismatch: expected "/{CONTAINER}/{segment_name}", ' \
        f'got "{entry["name"]}"'
    assert entry['hash'] == segment_etag, \
        f'hash mismatch: expected "{segment_etag}", got "{entry["hash"]}"'
    assert entry['bytes'] == len(segment_data), \
        f'bytes mismatch: expected {len(segment_data)}, got {entry["bytes"]}'

    # 5b. HEAD ?multipart-manifest=get — headers only, no body
    r = requests.head(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers=headers,
    )
    assert r.status_code == 200, \
        f'HEAD ?multipart-manifest=get failed: {r.status_code}'
    assert r.headers.get('Content-Type') == 'application/json; charset=utf-8', \
        f'HEAD Content-Type mismatch: {r.headers.get("Content-Type")}'
    assert r.headers.get('X-Static-Large-Object', '').lower() == 'true', \
        'HEAD manifest-get missing X-Static-Large-Object'
    assert int(r.headers.get('Content-Length', -1)) == manifest_len, \
        f'HEAD Content-Length {r.headers.get("Content-Length")} != ' \
        f'manifest length {manifest_len}'
    assert not r.content, 'HEAD must not return a body'

    # 6. GET ?multipart-manifest=get&format=raw — the put format
    #    (path/etag/size_bytes, and the manifest object's own content type)
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}'
        f'?multipart-manifest=get&format=raw',
        headers=headers,
    )
    assert r.status_code == 200, \
        f'GET ?multipart-manifest=get&format=raw failed: {r.status_code}'
    # raw echoes the manifest object's stored content type (set on PUT),
    # whereas plain forces application/json; charset=utf-8. assert both the
    # exact stored value and that the two formats genuinely differ, so the
    # override logic is proven to run rather than coincidentally matching.
    raw_content_type = r.headers.get('Content-Type')
    assert raw_content_type == MANIFEST_PUT_CONTENT_TYPE, \
        f'raw Content-Type should echo the stored type ' \
        f'{MANIFEST_PUT_CONTENT_TYPE!r}, got {raw_content_type!r}'
    assert raw_content_type != plain_content_type, \
        f'raw and plain Content-Type must differ, both were ' \
        f'{raw_content_type!r}'

    raw_manifest = r.json()
    assert isinstance(raw_manifest, list), \
        f'Expected JSON array, got: {type(raw_manifest)}'
    assert len(raw_manifest) == 1, \
        f'Expected 1 entry, got {len(raw_manifest)}'

    entry = raw_manifest[0]
    assert set(entry.keys()) == {'path', 'etag', 'size_bytes'}, \
        f'Expected path/etag/size_bytes keys, got: {entry}'
    assert entry['path'] == f'/{CONTAINER}/{segment_name}', \
        f'path mismatch: expected "/{CONTAINER}/{segment_name}", ' \
        f'got "{entry["path"]}"'
    assert entry['etag'] == segment_etag, \
        f'etag mismatch: expected "{segment_etag}", got "{entry["etag"]}"'
    assert entry['size_bytes'] == len(segment_data), \
        f'size_bytes mismatch: expected {len(segment_data)}, ' \
        f'got {entry["size_bytes"]}'

    # 6b. Range applies to the manifest body, like a regular object read
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers={**headers, 'Range': 'bytes=0-9'},
    )
    assert r.status_code == 206, \
        f'Range GET expected 206, got {r.status_code}'
    assert r.headers.get('Content-Range') == f'bytes 0-9/{manifest_len}', \
        f'Content-Range mismatch: {r.headers.get("Content-Range")}'
    assert r.content == full_body[0:10], \
        f'Range body mismatch: expected first 10 bytes of the manifest'

    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers={**headers, 'Range': 'bytes=-5'},
    )
    assert r.status_code == 206, \
        f'Suffix range GET expected 206, got {r.status_code}'
    assert r.content == full_body[-5:], \
        f'Suffix range body mismatch: expected last 5 bytes of the manifest'

    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers={**headers, 'Range': 'bytes=99999999-'},
    )
    assert r.status_code == 416, \
        f'Unsatisfiable range GET expected 416, got {r.status_code}'

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
        seg_etag = hashlib.md5(seg_data, usedforsecurity=False).hexdigest()

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

    # 3. Upload SLO manifest (leading-slash paths, like real swift clients)
    manifest = [
        {
            'path': f'/{CONTAINER}/{seg["name"]}',
            'etag': seg['etag'],
            'size_bytes': seg['size'],
        }
        for seg in segments
    ]
    manifest_json = json.dumps(manifest)

    r = requests.put(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=put',
        headers={**headers, 'Content-Type': MANIFEST_PUT_CONTENT_TYPE},
        data=manifest_json,
    )
    assert r.status_code == 201, \
        f'Upload SLO manifest failed: {r.status_code} {r.text}'

    # 4. GET ?multipart-manifest=get (user format: name/hash/bytes)
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}?multipart-manifest=get',
        headers=headers,
    )
    assert r.status_code == 200, \
        f'GET ?multipart-manifest=get failed: {r.status_code}'
    assert r.headers.get('Content-Type') == PLAIN_GET_CONTENT_TYPE, \
        f'Content-Type mismatch: {r.headers.get("Content-Type")}'

    returned_manifest = r.json()
    assert len(returned_manifest) == 3, \
        f'Expected 3 entries, got {len(returned_manifest)}'

    # Verify each entry matches
    for i, (entry, seg) in enumerate(zip(returned_manifest, segments)):
        assert set(entry.keys()) == {'name', 'hash', 'bytes'}, \
            f'Entry {i} expected name/hash/bytes keys, got: {entry}'
        assert entry['name'] == f'/{CONTAINER}/{seg["name"]}', \
            f'Entry {i} name mismatch'
        assert entry['hash'] == seg['etag'], \
            f'Entry {i} hash mismatch'
        assert entry['bytes'] == seg['size'], \
            f'Entry {i} bytes mismatch'

    # 4b. GET ?multipart-manifest=get&format=raw (put format)
    r = requests.get(
        f'{storage_url}/{CONTAINER}/{SLO_NAME}'
        f'?multipart-manifest=get&format=raw',
        headers=headers,
    )
    assert r.status_code == 200, \
        f'GET ?multipart-manifest=get&format=raw failed: {r.status_code}'
    assert r.headers.get('Content-Type') == MANIFEST_PUT_CONTENT_TYPE, \
        f'raw Content-Type should echo the stored type ' \
        f'{MANIFEST_PUT_CONTENT_TYPE!r}, got {r.headers.get("Content-Type")!r}'
    assert r.headers.get('Content-Type') != PLAIN_GET_CONTENT_TYPE, \
        'raw and plain Content-Type must differ'

    raw_manifest = r.json()
    assert len(raw_manifest) == 3, \
        f'Expected 3 entries, got {len(raw_manifest)}'

    for i, (entry, seg) in enumerate(zip(raw_manifest, segments)):
        assert set(entry.keys()) == {'path', 'etag', 'size_bytes'}, \
            f'Entry {i} expected path/etag/size_bytes keys, got: {entry}'
        assert entry['path'] == f'/{CONTAINER}/{seg["name"]}', \
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
