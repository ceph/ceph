#!/usr/bin/env python3
"""
Compression tests that change the zone's placement config while the gateway
is running.

The rgw task must create a realm, so that a period commit makes the running
gateway reload the zone's placement config. The tests set the compression
type of the STANDARD storage class themselves, and restore it at the end.

Covers:
- a multipart upload whose placement compression type changes between its
  parts, uploaded with UploadPart or copied with UploadPartCopy: the upload
  keeps the type configured when it was initiated.
- the storage class's compression_min_size: objects smaller than it are
  stored uncompressed, objects at or above it are compressed. The test
  creates these storage classes, and removes them if all tests pass:
    COMPRESSED - zstd compression
    MINSIZE    - zstd compression with --compression-min-size 64K
  It covers PUT (below, at and above the threshold), ranged copy-source PUTs,
  CopyObject and LC transition into MINSIZE from uncompressed and compressed
  sources, transition of noncurrent null versions below a delete marker, and
  small multipart uploads (including a small part copied with UploadPartCopy)
  and a chunked upload of unknown size, which ignore the minimum size.
"""

import http.client
import json
import logging as log
import ssl
import time
from urllib.parse import urlparse

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import BotoCoreError, ClientError
from common import exec_cmd, create_user, boto_connect

USER = 'compression-tester'
DISPLAY_NAME = 'Compression Testing'
ACCESS_KEY = 'COMPRESSION0123456789'
SECRET_KEY = 'compressionsecretkey0123456789abcdefghij'
BUCKET_NAME = 'compression-bucket'
PLACEMENT_ID = 'default-placement'
PART_SIZE = 5 * 1024 * 1024
CANARY_SIZE = 64 * 1024
RELOAD_TIMEOUT = 120
# under 'random' each write picks its own algorithm, possibly none, so a
# canary can't show that the gateway reloaded the config; wait this long
RANDOM_RELOAD_WAIT = 30
MIN_SIZE = 64 * 1024
COMPRESSED_CLASS = 'COMPRESSED'
MINSIZE_CLASS = 'MINSIZE'
STORAGE_CLASSES = {
    COMPRESSED_CLASS: '--compression zstd',
    MINSIZE_CLASS: '--compression zstd --compression-min-size 64K',
}

LC_POLL_INTERVAL = 10
LC_TIMEOUT = 120


def make_compressible_body(size_bytes):
    pattern = b'The quick brown fox jumps over the lazy dog. '
    return (pattern * (size_bytes // len(pattern) + 1))[:size_bytes]


def admin(cmd):
    out = exec_cmd(f'radosgw-admin {cmd}')
    if isinstance(out, bytes):
        out = out.decode('utf-8', errors='replace')
    return out


def object_stat(bucket_name, object_key, version_id=None):
    cmd = f'object stat --bucket={bucket_name} --object={object_key}'
    if version_id:
        cmd += f' --object-version={version_id}'
    return json.loads(admin(cmd))


def get_compression_type(stat):
    """ None if the object is not compressed """
    ct = stat.get('compression', {}).get('compression_type', 'none')
    return None if ct.lower() == 'none' else ct.lower()


def get_storage_class(stat):
    sc = stat.get('attrs', {}).get('user.rgw.storage_class', '').strip().strip('\x00')
    return sc or 'STANDARD'


def verify(bucket, key, body, expected_compression):
    """ expected_compression is None if the object should be uncompressed,
    or 'random' to accept any compression type """
    ct = get_compression_type(object_stat(bucket.name, key))
    log.info(f'  {key}: compression_type={ct}, expected={expected_compression}')
    if expected_compression != 'random':
        assert ct == expected_compression, \
            f'{key}: expected compression {expected_compression}, got {ct}'
    assert bucket.Object(key).get()['Body'].read() == body, f'{key}: data mismatch'


def recreate_bucket(conn, name):
    b = conn.Bucket(name)
    try:
        b.object_versions.all().delete()
        b.delete()
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            raise
    return conn.create_bucket(Bucket=name)


def commit_period(stale):
    """ commit the period, then poll stale() until the gateway has reloaded
    the zone's placement config. stale() returns None once the new config
    is in effect, or a description of what still looks stale """
    admin('period update --commit')
    deadline = time.time() + RELOAD_TIMEOUT
    while True:
        try:
            state = stale()
            if state is None:
                return
        except (ClientError, BotoCoreError) as e:
            # requests may fail while the gateway reloads
            state = f'error: {e}'
        assert time.time() < deadline, \
            f'placement config not applied within {RELOAD_TIMEOUT}s (last state: {state})'
        time.sleep(1)


def get_placement_compression(storage_class):
    zone = json.loads(admin('zone get'))
    for p in zone['placement_pools']:
        if p['key'] == PLACEMENT_ID:
            sc = p['val']['storage_classes'].get(storage_class, {})
            return sc.get('compression_type') or 'none'
    return 'none'


def set_placement_compression(bucket, storage_class, compression):
    """ set the storage class's compression type, and wait until the gateway
    has reloaded it. canaries are written to the given bucket. when the old
    or new type is 'random', a canary can't show the reload, so this sleeps
    for RANDOM_RELOAD_WAIT instead """
    previous = get_placement_compression(storage_class)
    if previous == compression:
        # a period commit makes the gateway reload even if nothing changed
        return
    admin(f'zone placement modify --placement-id {PLACEMENT_ID} '
          f'--storage-class {storage_class} --compression {compression}')
    log.info(f'set {storage_class} compression={compression}')
    if 'random' in (previous, compression):
        admin('period update --commit')
        time.sleep(RANDOM_RELOAD_WAIT)
        return

    def stale():
        # write a canary until it's stored with the new compression type
        key = 'compression-canary'
        bucket.put_object(Key=key, Body=make_compressible_body(CANARY_SIZE),
                          StorageClass=storage_class)
        ct = get_compression_type(object_stat(bucket.name, key))
        if ct == (None if compression == 'none' else compression):
            return None
        return f'canary compression={ct}'
    commit_period(stale)


def test_multipart_compression_change(bucket, initial, changed, operation):
    """ change the compression type between the parts of a multipart upload,
    which are written with the given operation, UploadPart or UploadPartCopy,
    and verify that the upload completes with the type configured when it
    was initiated. UploadPartCopy copies the parts from an object stored with
    the changed type, so parts that kept the source's compression would fail
    the check, except when initial is 'random', which accepts any type """
    if operation not in ('UploadPart', 'UploadPartCopy'):
        raise ValueError(f'unknown operation {operation}')
    log.info(f'--- multipart upload ({operation}), compression {initial} -> {changed} ---')
    client = bucket.meta.client
    key = f'multipart-{operation}-{initial}-to-{changed}'
    part_data = make_compressible_body(PART_SIZE)
    if operation == 'UploadPartCopy':
        source = f'{key}-source'
        set_placement_compression(bucket, 'STANDARD', changed)
        bucket.put_object(Key=source, Body=part_data)
        src_ct = get_compression_type(object_stat(bucket.name, source))
        if changed != 'random':
            assert src_ct == (None if changed == 'none' else changed), \
                f'{source}: unexpected source compression {src_ct}'

    set_placement_compression(bucket, 'STANDARD', initial)
    upload_id = client.create_multipart_upload(Bucket=bucket.name, Key=key)['UploadId']
    try:
        parts = []
        for part_num in [1, 2]:
            if part_num == 2:
                set_placement_compression(bucket, 'STANDARD', changed)
            if operation == 'UploadPartCopy':
                etag = client.upload_part_copy(
                    Bucket=bucket.name, Key=key, PartNumber=part_num, UploadId=upload_id,
                    CopySource={'Bucket': bucket.name, 'Key': source})['CopyPartResult']['ETag']
            else:
                etag = client.upload_part(Bucket=bucket.name, Key=key, PartNumber=part_num,
                                          UploadId=upload_id, Body=part_data)['ETag']
            parts.append({'PartNumber': part_num, 'ETag': etag})
        client.complete_multipart_upload(Bucket=bucket.name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})
        upload_id = None
    finally:
        if upload_id:
            client.abort_multipart_upload(Bucket=bucket.name, Key=key, UploadId=upload_id)

    # parts uploaded under 'random' all pick the same algorithm, possibly none
    verify(bucket, key, part_data * 2, None if initial == 'none' else initial)


def create_storage_classes(bucket):
    """ add the storage classes to the zonegroup's and zone's default
    placement, and wait until the gateway has reloaded them. canaries are
    written to the given bucket """
    for name, args in STORAGE_CLASSES.items():
        admin(f'zonegroup placement add --placement-id {PLACEMENT_ID} '
              f'--storage-class {name}')
        admin(f'zone placement add --placement-id {PLACEMENT_ID} '
              f'--storage-class {name} --data-pool default.rgw.buckets.data.{name.lower()} '
              f'{args}')
    log.info(f'created storage classes {list(STORAGE_CLASSES)}')

    key = 'storage-class-canary'
    # every storage class compresses a canary at the min size, and MINSIZE
    # doesn't compress one below it
    canaries = [(sc, MIN_SIZE, 'zstd') for sc in STORAGE_CLASSES] + \
               [(MINSIZE_CLASS, MIN_SIZE - 1, None)]

    def stale():
        for sc, size, expected in canaries:
            bucket.put_object(Key=key, Body=make_compressible_body(size), StorageClass=sc)
            ct = get_compression_type(object_stat(bucket.name, key))
            if ct != expected:
                return f'{sc} canary of {size} bytes: compression={ct}'
        return None
    commit_period(stale)
    bucket.Object(key).delete()


def remove_storage_classes(bucket):
    """ remove the storage classes, which must no longer hold any objects,
    and wait until the gateway has reloaded the placement config """
    for name in STORAGE_CLASSES:
        admin(f'zone placement rm --placement-id {PLACEMENT_ID} --storage-class {name}')
        admin(f'zonegroup placement rm --placement-id {PLACEMENT_ID} --storage-class {name}')
    log.info(f'removed storage classes {list(STORAGE_CLASSES)}')

    def stale():
        # write a canary until the gateway rejects the storage class
        key = 'storage-class-canary'
        try:
            bucket.put_object(Key=key, Body=b'canary', StorageClass=MINSIZE_CLASS)
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidArgument':
                return None
            raise
        bucket.Object(key).delete()
        return f'{MINSIZE_CLASS} still accepted'
    commit_period(stale)


def test_put(bucket):
    log.info('--- PUT into MINSIZE ---')
    for size, expected in [(4 * 1024, None), (MIN_SIZE, 'zstd'), (256 * 1024, 'zstd')]:
        key = f'put-{size}'
        body = make_compressible_body(size)
        bucket.put_object(Key=key, Body=body, StorageClass=MINSIZE_CLASS)
        verify(bucket, key, body, expected)


def test_copy(bucket):
    log.info('--- CopyObject into MINSIZE ---')
    # sources in COMPRESSED are already compressed with zstd
    for size, src_class, expected in [(4 * 1024, 'STANDARD', None),
                                      (256 * 1024, 'STANDARD', 'zstd'),
                                      (4 * 1024, COMPRESSED_CLASS, None),
                                      (256 * 1024, COMPRESSED_CLASS, 'zstd')]:
        src = f'copy-src-{src_class}-{size}'
        dst = f'copy-dst-{src_class}-{size}'
        body = make_compressible_body(size)
        bucket.put_object(Key=src, Body=body, StorageClass=src_class)
        src_ct = get_compression_type(object_stat(bucket.name, src))
        assert src_ct == ('zstd' if src_class == COMPRESSED_CLASS else None), \
            f'{src}: unexpected source compression {src_ct}'
        bucket.meta.client.copy_object(Bucket=bucket.name, Key=dst,
                                       CopySource={'Bucket': bucket.name, 'Key': src},
                                       StorageClass=MINSIZE_CLASS)
        verify(bucket, dst, body, expected)


def test_ranged_copy_put(bucket):
    log.info('--- ranged copy-source PUTs into MINSIZE ---')
    client = bucket.meta.client
    # the copied range's size decides, not the source object's size
    cases = [(4 * 1024, 0, 4 * 1024 - 1, None),
             (256 * 1024, 0, 256 * 1024 - 1, 'zstd'),
             (256 * 1024, 0, 4095, None),
             (256 * 1024, 0, MIN_SIZE - 2, None),
             (256 * 1024, 4096, 4096 + MIN_SIZE - 1, 'zstd')]
    for size, first, last, expected in cases:
        src = f'range-src-{size}'
        dst = f'range-dst-{size}-{first}-{last}'
        source_body = make_compressible_body(size)
        bucket.put_object(Key=src, Body=source_body)

        # a PutObject with x-amz-copy-source-range and no body, which SDKs
        # don't expose, so add the headers before the request is signed
        def add_copy_headers(request, **kwargs):
            request.headers['x-amz-copy-source'] = f'{bucket.name}/{src}'
            request.headers['x-amz-copy-source-range'] = f'bytes={first}-{last}'
            # drop checksums of the empty request body, which don't match the
            # copied data
            for name in list(request.headers.keys()):
                if name.lower() == 'content-md5' or name.lower().startswith(
                        ('x-amz-checksum-', 'x-amz-sdk-checksum-')):
                    del request.headers[name]
        client.meta.events.register_first('before-sign.s3.PutObject', add_copy_headers,
                                          unique_id='ranged-copy')
        try:
            client.put_object(Bucket=bucket.name, Key=dst, StorageClass=MINSIZE_CLASS)
        finally:
            client.meta.events.unregister('before-sign.s3.PutObject', unique_id='ranged-copy')
        verify(bucket, dst, source_body[first:last + 1], expected)


def test_chunked_put(bucket):
    log.info('--- chunked PUT of unknown size into MINSIZE ---')
    client = bucket.meta.client
    key = 'chunked-small'
    body = make_compressible_body(4 * 1024)
    url = f'{client.meta.endpoint_url}/{bucket.name}/{key}'
    # send neither Content-Length nor x-amz-decoded-content-length, so the
    # size isn't known up front
    request = AWSRequest(method='PUT', url=url, headers={
        'x-amz-storage-class': MINSIZE_CLASS,
        'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
        'Transfer-Encoding': 'chunked',
    })
    SigV4Auth(Credentials(ACCESS_KEY, SECRET_KEY), 's3',
              client.meta.region_name or 'us-east-1').add_auth(request)

    parsed = urlparse(url)
    if parsed.scheme == 'https':
        conn = http.client.HTTPSConnection(parsed.hostname, parsed.port,
                                           context=ssl._create_unverified_context())
    else:
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port)
    conn.request('PUT', parsed.path, body=iter([body[:2048], body[2048:]]),
                 headers=dict(request.headers), encode_chunked=True)
    response = conn.getresponse()
    assert response.status == 200, \
        f'{key}: chunked PUT failed: {response.status} {response.read()}'
    conn.close()
    # an upload of unknown size ignores the min size, so MINSIZE compresses it
    verify(bucket, key, body, 'zstd')


def test_multipart(bucket):
    log.info('--- small multipart uploads into MINSIZE ---')
    key = 'multipart-small'
    body = make_compressible_body(4 * 1024)
    client = bucket.meta.client
    upload_id = client.create_multipart_upload(Bucket=bucket.name, Key=key,
                                               StorageClass=MINSIZE_CLASS)['UploadId']
    etag = client.upload_part(Bucket=bucket.name, Key=key, PartNumber=1,
                              UploadId=upload_id, Body=body)['ETag']
    client.complete_multipart_upload(
        Bucket=bucket.name, Key=key, UploadId=upload_id,
        MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag}]})
    # multipart uploads ignore the min size, so MINSIZE compresses them
    verify(bucket, key, body, 'zstd')

    # a small last part copied with UploadPartCopy ignores it too, so it's
    # compressed like the uploaded part before it
    key = 'multipart-small-copied-part'
    source = f'{key}-source'
    bucket.put_object(Key=source, Body=body)
    part_data = make_compressible_body(PART_SIZE)
    upload_id = client.create_multipart_upload(Bucket=bucket.name, Key=key,
                                               StorageClass=MINSIZE_CLASS)['UploadId']
    etag1 = client.upload_part(Bucket=bucket.name, Key=key, PartNumber=1,
                               UploadId=upload_id, Body=part_data)['ETag']
    etag2 = client.upload_part_copy(
        Bucket=bucket.name, Key=key, PartNumber=2, UploadId=upload_id,
        CopySource={'Bucket': bucket.name, 'Key': source})['CopyPartResult']['ETag']
    client.complete_multipart_upload(
        Bucket=bucket.name, Key=key, UploadId=upload_id,
        MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag1},
                                   {'PartNumber': 2, 'ETag': etag2}]})
    verify(bucket, key, part_data + body, 'zstd')


def wait_for_storage_class(bucket_name, key, version_id=None):
    deadline = time.time() + LC_TIMEOUT
    while True:
        sc = get_storage_class(object_stat(bucket_name, key, version_id))
        if sc == MINSIZE_CLASS:
            return
        assert time.time() < deadline, \
            f'{key} (version {version_id}): transition timed out in storage class {sc}'
        admin(f'lc process --bucket={bucket_name} --rgw-lc-debug-interval=10')
        time.sleep(LC_POLL_INTERVAL)


def test_transition(conn):
    log.info('--- LC transition into MINSIZE ---')
    bucket = recreate_bucket(conn, BUCKET_NAME + '-lc')
    # sources in COMPRESSED are already compressed with zstd
    objects = [(f'lc-{src_class}-{size}', make_compressible_body(size), src_class, expected)
               for size, src_class, expected in [(4 * 1024, 'STANDARD', None),
                                                 (256 * 1024, 'STANDARD', 'zstd'),
                                                 (4 * 1024, COMPRESSED_CLASS, None),
                                                 (256 * 1024, COMPRESSED_CLASS, 'zstd')]]
    for key, body, src_class, _ in objects:
        bucket.put_object(Key=key, Body=body, StorageClass=src_class)
        src_ct = get_compression_type(object_stat(bucket.name, key))
        assert src_ct == ('zstd' if src_class == COMPRESSED_CLASS else None), \
            f'{key}: unexpected source compression {src_ct}'
    bucket.meta.client.put_bucket_lifecycle_configuration(
        Bucket=bucket.name,
        LifecycleConfiguration={'Rules': [{
            'ID': 'to-minsize', 'Filter': {'Prefix': ''}, 'Status': 'Enabled',
            'Transitions': [{'Days': 0, 'StorageClass': MINSIZE_CLASS}],
        }]})

    for key, body, _, expected in objects:
        wait_for_storage_class(bucket.name, key)
        verify(bucket, key, body, expected)

    bucket.objects.all().delete()
    bucket.delete()


def test_noncurrent_null_version_transition(conn):
    log.info('--- LC transition of noncurrent null versions into MINSIZE ---')
    bucket = recreate_bucket(conn, BUCKET_NAME + '-nv')
    objects = [(f'nv-null-{size}', make_compressible_body(size), expected)
               for size, expected in [(4 * 1024, None), (256 * 1024, 'zstd')]]
    # the null versions are written before versioning is enabled, then
    # delete markers make them noncurrent
    for key, body, _ in objects:
        bucket.put_object(Key=key, Body=body)
    bucket.Versioning().enable()
    for key, _, _ in objects:
        bucket.Object(key).delete()
    bucket.meta.client.put_bucket_lifecycle_configuration(
        Bucket=bucket.name,
        LifecycleConfiguration={'Rules': [{
            'ID': 'noncurrent-to-minsize', 'Filter': {'Prefix': ''}, 'Status': 'Enabled',
            'NoncurrentVersionTransitions': [{'NoncurrentDays': 1,
                                              'StorageClass': MINSIZE_CLASS}],
        }]})

    for key, body, expected in objects:
        wait_for_storage_class(bucket.name, key, 'null')
        ct = get_compression_type(object_stat(bucket.name, key, 'null'))
        log.info(f'  {key} (null version): compression_type={ct}, expected={expected}')
        assert ct == expected, f'{key}: expected compression {expected}, got {ct}'
        data = bucket.Object(key).get(VersionId='null')['Body'].read()
        assert data == body, f'{key}: data mismatch'

    bucket.object_versions.all().delete()
    bucket.delete()


def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    bucket = recreate_bucket(conn, BUCKET_NAME)
    original = get_placement_compression('STANDARD')
    failures = []

    def run(test, *args):
        # keep going, so one failure doesn't hide the other results
        name = f'{test.__name__}{args[1:]}'
        try:
            test(*args)
        except Exception:
            log.exception(f'{name} failed')
            failures.append(name)

    try:
        for operation in ['UploadPart', 'UploadPartCopy']:
            for initial, changed in [('none', 'zstd'), ('zstd', 'none'), ('random', 'none')]:
                run(test_multipart_compression_change, bucket, initial, changed, operation)

        # the min size tests expect STANDARD to be uncompressed
        set_placement_compression(bucket, 'STANDARD', 'none')
        create_storage_classes(bucket)
        minsize_bucket = recreate_bucket(conn, BUCKET_NAME + '-minsize')
        for test in [test_put, test_ranged_copy_put, test_copy, test_multipart,
                     test_chunked_put]:
            run(test, minsize_bucket)
        run(test_transition, conn)
        run(test_noncurrent_null_version_transition, conn)
        if not failures:
            # on failure, keep the objects and their storage classes around
            minsize_bucket.objects.all().delete()
            minsize_bucket.delete()
            remove_storage_classes(bucket)
    finally:
        run(set_placement_compression, bucket, 'STANDARD', original)

    assert not failures, f'failed tests: {failures}'
    bucket.objects.all().delete()
    bucket.delete()
    log.info('All compression tests passed')


main()
