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
"""

import json
import logging as log
import time

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


def make_compressible_body(size_bytes):
    pattern = b'The quick brown fox jumps over the lazy dog. '
    return (pattern * (size_bytes // len(pattern) + 1))[:size_bytes]


def admin(cmd):
    out = exec_cmd(f'radosgw-admin {cmd}')
    if isinstance(out, bytes):
        out = out.decode('utf-8', errors='replace')
    return out


def object_stat(bucket_name, object_key):
    return json.loads(admin(f'object stat --bucket={bucket_name} --object={object_key}'))


def get_compression_type(stat):
    """ None if the object is not compressed """
    ct = stat.get('compression', {}).get('compression_type', 'none')
    return None if ct.lower() == 'none' else ct.lower()


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
    finally:
        run(set_placement_compression, bucket, 'STANDARD', original)

    assert not failures, f'failed tests: {failures}'
    bucket.objects.all().delete()
    bucket.delete()
    log.info('All compression tests passed')


main()
