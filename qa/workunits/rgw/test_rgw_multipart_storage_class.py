#!/usr/bin/env python3

import logging as log
from common import create_user, boto_connect

"""
Tests multipart uploads made directly into a non-default storage class.

The qa suite already configures the LUKEWARM storage class with its own data pool.
"""

USER = 'mpart-sc-tester'
DISPLAY_NAME = 'Multipart Storage Class Testing'
ACCESS_KEY = 'MPARTSC0123456789ABC'
SECRET_KEY = 'mpartscsecretkey0123456789abcdefghijklmn'
BUCKET_NAME = 'mpart-sc-bucket'
STORAGE_CLASS = 'LUKEWARM'


def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    connection = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = connection.meta.client

    bucket = connection.create_bucket(Bucket=BUCKET_NAME)

    # TESTCASE 'verify that partNumber reads work on a multipart upload in a non-default storage class'
    log.debug('TEST: verify that partNumber reads work on a multipart upload in a non-default storage class\n')
    key = 'mpu'
    part_sizes = [5 * 1024 * 1024, 5 * 1024 * 1024, 1 * 1024 * 1024]
    up = client.create_multipart_upload(Bucket=BUCKET_NAME, Key=key, StorageClass=STORAGE_CLASS)
    parts = []
    bodies = []
    for i, size in enumerate(part_sizes, 1):
        data = bytes([i]) * size
        bodies.append(data)
        r = client.upload_part(Bucket=BUCKET_NAME, Key=key, UploadId=up['UploadId'], PartNumber=i, Body=data)
        parts.append({'PartNumber': i, 'ETag': r['ETag']})
    client.complete_multipart_upload(Bucket=BUCKET_NAME, Key=key, UploadId=up['UploadId'], MultipartUpload={'Parts': parts})

    r = client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert r.get('StorageClass') == STORAGE_CLASS, \
        'object storage class {} != {}'.format(r.get('StorageClass'), STORAGE_CLASS)

    # without the fix, the part head was looked up in the bucket's default
    # (STANDARD) pool instead of the storage class pool, so every partNumber
    # read returned 400 InvalidPart
    for i, data in enumerate(bodies, 1):
        r = client.head_object(Bucket=BUCKET_NAME, Key=key, PartNumber=i)
        assert r['ContentLength'] == len(data), \
            'head part {} length {} != {}'.format(i, r['ContentLength'], len(data))
        assert r.get('PartsCount') == len(part_sizes), \
            'head part {} PartsCount {} != {}'.format(i, r.get('PartsCount'), len(part_sizes))

        r = client.get_object(Bucket=BUCKET_NAME, Key=key, PartNumber=i)
        assert r['ContentLength'] == len(data), \
            'get part {} length {} != {}'.format(i, r['ContentLength'], len(data))
        assert r['Body'].read() == data, 'get part {} content mismatch'.format(i)
        assert r.get('PartsCount') == len(part_sizes), \
            'get part {} PartsCount {} != {}'.format(i, r.get('PartsCount'), len(part_sizes))

    # Clean up
    log.debug("Deleting bucket {}".format(BUCKET_NAME))
    bucket.objects.all().delete()
    bucket.delete()

main()
log.info("Completed multipart storage class tests")
