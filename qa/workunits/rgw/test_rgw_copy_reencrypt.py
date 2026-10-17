#!/usr/bin/env python3

import logging as log
import os
import sys
import time
import botocore.exceptions
from common import create_user, boto_connect, make_compressible_body, \
    upload_object, object_stat, get_compression_type, get_storage_class, \
    get_crypt_mode, get_crypt_attr_raw

"""
Tests that CopyObject re-encrypts an object with the encryption algorithm
the gateway is configured with now, rather than the one the object was
written with, and recompresses it for the destination storage class.

The test runs in two phases around a cipher change. The suite pins
aes-256-cbc before the gateway starts, runs the put phase, sets
aes-256-gcm, restarts the gateway, then runs the copy phase. Objects
written in the put phase therefore predate the cipher change, which is
what an operator changing the cipher on an existing cluster has.

The phase is selected by COPY_REENCRYPT_PHASE.

The qa suite configures:
  STANDARD  - no compression
  LUKEWARM  - zstd compression
"""

USER = 'copy-reencrypt-tester'
DISPLAY_NAME = 'CopyObject Reencrypt Testing'
ACCESS_KEY = 'COPYREENC0123456789A'
SECRET_KEY = 'copyreencsecretkey0123456789abcdefghijklm'
BUCKET_NAME = 'copy-reencrypt-bucket'
KMS_KEY_ID = 'testkey-1'
NEW_KMS_KEY_ID = 'testkey-2'

SMALL_KEY = 'small'
LARGE_KEY = 'large'
INPLACE_KEY = 'multipart-inplace'
SSEC_KEY = 'ssec'
PROBE_KEY = 'gcm-probe'

SMALL_SIZE = 4 * 1024
LARGE_SIZE = 32 * 1024 * 1024
INPLACE_SIZE = 9 * 1024 * 1024

SSEC_ARGS = {
    'SSECustomerAlgorithm': 'AES256',
    'SSECustomerKey': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
    'SSECustomerKeyMD5': 'DWygnHRtgiJ77HCm+1rvHw==',
}
SSEC_COPY_SOURCE_ARGS = {f'CopySource{k}': v for k, v in SSEC_ARGS.items()}
NEW_SSEC_ARGS = {
    'SSECustomerAlgorithm': 'AES256',
    'SSECustomerKey': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
    'SSECustomerKeyMD5': 'arxBvwY2V4SiOne6yppVPQ==',
}
KMS_ARGS = {
    'ServerSideEncryption': 'aws:kms',
    'SSEKMSKeyId': KMS_KEY_ID,
}
NEW_KMS_ARGS = {
    'ServerSideEncryption': 'aws:kms',
    'SSEKMSKeyId': NEW_KMS_KEY_ID,
}


def connect_with_retry():
    """
    Connect to the gateway, retrying while it starts up.

    ceph.restart waits for cluster health, not for radosgw to accept
    connections, so use the same backoff the rgw task uses at startup.
    """
    num_retries = 8
    for seconds in range(num_retries):
        try:
            return boto_connect(ACCESS_KEY, SECRET_KEY)
        except botocore.exceptions.ConnectionError:
            log.info(f'radosgw not accepting connections, retry in {2**seconds}s')
            time.sleep(2**seconds)
    raise AssertionError('radosgw did not come back up after restart')


def verify_encrypted(key, expected_mode):
    """Check the stored encryption mode of an object."""
    stat = object_stat(BUCKET_NAME, key)

    mode = get_crypt_mode(stat)
    log.info(f'{key}: crypt_mode={mode} storage_class={get_storage_class(stat)} '
             f'compression={get_compression_type(stat)}')
    assert mode == expected_mode, \
        f'{key} crypt mode is {mode}, expected {expected_mode}'

    return stat


def reencrypt(client, key, size, mode, get_args=None, **copy_args):
    """Copy an object onto itself, then check its mode and its data."""
    client.copy_object(Bucket=BUCKET_NAME, Key=key,
                       CopySource={'Bucket': BUCKET_NAME, 'Key': key}, **copy_args)
    stat = verify_encrypted(key, mode)
    response = client.get_object(Bucket=BUCKET_NAME, Key=key, **(get_args or {}))
    assert response['Body'].read() == make_compressible_body(size), \
        f'{key} data mismatch after re-encrypting'
    return stat


def run_put_phase():
    """Write objects under aes-256-cbc."""
    log.info('=== put phase ===')
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client

    try:
        bucket = conn.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError:
        pass

    conn.create_bucket(Bucket=BUCKET_NAME)

    for key, size, args, mode in ((SMALL_KEY, SMALL_SIZE, KMS_ARGS, 'SSE-KMS'),
                                  (LARGE_KEY, LARGE_SIZE, KMS_ARGS, 'SSE-KMS'),
                                  (INPLACE_KEY, INPLACE_SIZE, KMS_ARGS, 'SSE-KMS'),
                                  (SSEC_KEY, SMALL_SIZE, SSEC_ARGS, 'SSE-C-AES256')):
        log.info(f'uploading {key}, {size} bytes')
        upload_object(client, BUCKET_NAME, key, make_compressible_body(size), args)
        verify_encrypted(key, mode)

    log.info('put phase passed')


def run_copy_phase():
    """Re-encrypt each object by copying it onto itself."""
    log.info('=== copy phase ===')
    conn = connect_with_retry()
    client = conn.meta.client
    bucket = conn.Bucket(BUCKET_NAME)

    # a fresh upload proves the cipher change took effect, so that a
    # failure here is not mistaken for a copy bug
    log.info('probing the configured cipher')
    bucket.put_object(Key=PROBE_KEY, Body=make_compressible_body(SMALL_SIZE), **KMS_ARGS)
    verify_encrypted(PROBE_KEY, 'SSE-KMS-GCM')

    log.info('--- re-encrypt sse-kms object in place ---')
    reencrypt(client, SMALL_KEY, SMALL_SIZE, 'SSE-KMS-GCM', **KMS_ARGS)
    first_salt = get_crypt_attr_raw(BUCKET_NAME, SMALL_KEY, 'salt')

    # a second re-encryption has a salt to rotate away from, which the
    # first does not: the object was written under CBC, which stores none
    log.info('--- re-encrypt the same object again, salt must rotate ---')
    reencrypt(client, SMALL_KEY, SMALL_SIZE, 'SSE-KMS-GCM', **KMS_ARGS)
    second_salt = get_crypt_attr_raw(BUCKET_NAME, SMALL_KEY, 'salt')
    assert second_salt != first_salt, \
        'crypt.salt did not rotate across re-encryption'

    # a multipart source with no storage class change, so the copy is legal
    # only because the request names an encryption
    log.info('--- re-encrypt multipart object in place under a new kms key ---')
    before = client.head_object(Bucket=BUCKET_NAME, Key=INPLACE_KEY)['ETag']
    assert '-' in before, \
        f'{INPLACE_KEY} was expected to carry a multipart ETag, got {before}'
    reencrypt(client, INPLACE_KEY, INPLACE_SIZE, 'SSE-KMS-GCM', **NEW_KMS_ARGS)
    head = client.head_object(Bucket=BUCKET_NAME, Key=INPLACE_KEY)
    assert head['ETag'] == before, \
        f'{INPLACE_KEY} ETag changed across re-encryption, {before} -> {head["ETag"]}'
    assert head['SSEKMSKeyId'] == NEW_KMS_KEY_ID, \
        f'{INPLACE_KEY} kms key is {head["SSEKMSKeyId"]}, expected {NEW_KMS_KEY_ID}'

    log.info('--- re-encrypt and recompress multipart object ---')
    stat = reencrypt(client, LARGE_KEY, LARGE_SIZE, 'SSE-KMS-GCM',
                     StorageClass='LUKEWARM', **KMS_ARGS)
    sc = get_storage_class(stat)
    assert sc == 'LUKEWARM', \
        f'{LARGE_KEY} storage class is {sc}, expected LUKEWARM'
    ct = get_compression_type(stat)
    assert ct == 'zstd', f'{LARGE_KEY} compression is {ct}, expected zstd'
    orig_size = stat['compression']['orig_size']
    assert orig_size == LARGE_SIZE, \
        f'{LARGE_KEY} orig_size is {orig_size}, expected {LARGE_SIZE}'

    log.info('--- re-encrypt sse-c object in place under a new key ---')
    reencrypt(client, SSEC_KEY, SMALL_SIZE, 'SSE-C-AES256-GCM', get_args=NEW_SSEC_ARGS,
              **NEW_SSEC_ARGS, **SSEC_COPY_SOURCE_ARGS)

    bucket.objects.all().delete()
    bucket.delete()
    log.info('copy phase passed')


def main():
    phase = os.environ.get('COPY_REENCRYPT_PHASE')
    if phase == 'put':
        run_put_phase()
    elif phase == 'copy':
        run_copy_phase()
    else:
        sys.exit(f'COPY_REENCRYPT_PHASE must be put or copy, got {phase!r}')


if __name__ == '__main__':
    main()
