#!/usr/bin/env python3

import json
import logging as log
import time
from common import exec_cmd, create_user, boto_connect

"""
Tests that a permanent restore from a cloud tier recompresses the object
with the compression type of the restore storage class.

The qa job compresses STANDARD with zstd and configures the
CLOUDTIER-CLIENT0 cloud storage class with retain_head_object=true.
"""

USER = 'cloud-restore-recompress-tester'
DISPLAY_NAME = 'Cloud Restore Recompress Testing'
ACCESS_KEY = 'CLOUDRESTORE01234567'
SECRET_KEY = 'cloudrestoresecretkey0123456789abcdefghij'
BUCKET_NAME = 'cloud-restore-recompress-bucket'
OBJECT_KEY = 'compressible'
CLOUD_STORAGE_CLASS = 'CLOUDTIER-CLIENT0'
TIMEOUT = 300


def object_stat():
    out = exec_cmd(f'radosgw-admin object stat --bucket={BUCKET_NAME} --object={OBJECT_KEY}')
    return json.loads(out.decode('utf-8', errors='replace'))


def wait_for_storage_class(storage_class):
    deadline = time.time() + TIMEOUT
    while True:
        stat = object_stat()
        sc = stat.get('attrs', {}).get('user.rgw.storage_class', '').strip('\x00')
        if (sc or 'STANDARD') == storage_class:
            return stat
        assert time.time() < deadline, f'timed out waiting for {storage_class}'
        time.sleep(10)


def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    client = boto_connect(ACCESS_KEY, SECRET_KEY).meta.client
    client.create_bucket(Bucket=BUCKET_NAME)
    body = b'The quick brown fox jumps over the lazy dog. ' * 20000
    client.put_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY, Body=body)

    client.put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration={'Rules': [{
            'ID': 'cloud', 'Filter': {'Prefix': ''}, 'Status': 'Enabled',
            'Transitions': [{'Days': 1, 'StorageClass': CLOUD_STORAGE_CLASS}]}]})
    stat = wait_for_storage_class(CLOUD_STORAGE_CLASS)
    # the cloud copy is uncompressed, so the stub must not claim otherwise
    assert 'compression' not in stat, 'cloud-tiered stub kept its compression attr'

    client.delete_bucket_lifecycle(Bucket=BUCKET_NAME)
    client.restore_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY, RestoreRequest={})
    stat = wait_for_storage_class('STANDARD')
    comp = stat.get('compression', {})
    assert comp.get('compression_type') == 'zstd', 'permanent restore did not recompress'
    assert comp.get('orig_size') == len(body)
    assert client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)['Body'].read() == body

    client.delete_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    client.delete_bucket(Bucket=BUCKET_NAME)
    log.info('cloud restore recompression test passed')


main()
