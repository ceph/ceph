#!/usr/bin/env python3

import logging as log
import json
import time
from common import exec_cmd, create_user, boto_connect

"""
Tests that RGW lifecycle transitions correctly recompress objects
when moving between storage classes with different compression configs.

The qa suite configures:
  STANDARD  - no compression
  LUKEWARM  - zstd compression
  FROZEN    - zlib compression

Test plan:
  1. Upload object to STANDARD (uncompressed)
  2. Transition STANDARD -> LUKEWARM (compress with zstd)
  3. Transition LUKEWARM -> FROZEN (recompress with zlib)
  4. Transition FROZEN -> STANDARD (decompress)
  5. GET object and verify data integrity
"""

USER = 'lc-recompress-tester'
DISPLAY_NAME = 'LC Recompress Testing'
ACCESS_KEY = 'LCRECOMP0123456789AB'
SECRET_KEY = 'lcrecompsecretkey0123456789abcdefghijklmn'
BUCKET_NAME = 'lc-recompress-bucket'
OBJECT_KEY = 'compressible-object'

# ~450KB of highly compressible data
OBJECT_BODY = (b'The quick brown fox jumps over the lazy dog. ' * 10000)

LC_POLL_INTERVAL = 10
LC_TIMEOUT = 120


def object_stat(bucket_name, object_key):
    """Run radosgw-admin object stat and return parsed JSON."""
    out = exec_cmd(
        f'radosgw-admin object stat --bucket={bucket_name} --object={object_key}'
    )
    return json.loads(out)


def get_compression_type(stat):
    """
    Extract the compression_type from object stat output.
    Returns None if the object is not compressed.
    """
    compression = stat.get('compression')
    if compression is None:
        return None
    ct = compression.get('compression_type', 'none')
    if ct.lower() == 'none':
        return None
    return ct.lower()


def get_storage_class(stat):
    """
    Extract the storage class from object stat output.
    The storage class attr lives in attrs['user.rgw.storage_class'].
    If absent, the object is in the STANDARD storage class.
    """
    attrs = stat.get('attrs', {})
    sc = attrs.get('user.rgw.storage_class', '')
    # The value may be a raw string possibly with trailing null bytes
    sc = sc.strip().strip('\x00')
    if not sc:
        return 'STANDARD'
    return sc


def put_lifecycle_rule(client, bucket_name, rule_id, target_class):
    """
    Set a lifecycle configuration with a single transition rule.
    Uses Days=0 for immediate transition.
    """
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            'Rules': [
                {
                    'ID': rule_id,
                    'Filter': {'Prefix': ''},
                    'Status': 'Enabled',
                    'Transitions': [
                        {
                            'Days': 0,
                            'StorageClass': target_class,
                        }
                    ],
                }
            ]
        }
    )
    log.info(f'Set lifecycle rule {rule_id}: transition to {target_class}')


def wait_for_transition(bucket_name, object_key, expected_class, timeout=LC_TIMEOUT):
    """
    Poll radosgw-admin lc process + object stat until the object
    reaches the expected storage class, or timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        exec_cmd('radosgw-admin lc process')
        time.sleep(LC_POLL_INTERVAL)

        stat = object_stat(bucket_name, object_key)
        sc = get_storage_class(stat)
        log.info(f'  current storage class: {sc} (waiting for {expected_class})')
        if sc == expected_class:
            return stat

    raise AssertionError(
        f'Timed out waiting for object to transition to {expected_class}'
    )


def verify_transition(stat, expected_class, expected_compression):
    """
    Verify that the object stat matches the expected storage class
    and compression type after a transition.
    """
    sc = get_storage_class(stat)
    ct = get_compression_type(stat)

    log.info(f'  storage_class={sc}, compression_type={ct}')
    assert sc == expected_class, \
        f'Expected storage class {expected_class}, got {sc}'
    assert ct == expected_compression, \
        f'Expected compression type {expected_compression}, got {ct}'


def main():
    log.info('Creating user and connecting')
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client

    # Clean up any previous run
    try:
        bucket = conn.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.delete()
    except Exception:
        pass

    bucket = conn.create_bucket(Bucket=BUCKET_NAME)
    log.info(f'Created bucket {BUCKET_NAME}')

    # Upload compressible object to STANDARD (no compression)
    log.info(f'Uploading {len(OBJECT_BODY)} byte object as {OBJECT_KEY}')
    bucket.put_object(Key=OBJECT_KEY, Body=OBJECT_BODY)

    stat = object_stat(BUCKET_NAME, OBJECT_KEY)
    verify_transition(stat, 'STANDARD', None)
    log.info('Initial upload verified: STANDARD, no compression')

    # Transition 1: STANDARD (none) -> LUKEWARM (zstd)
    log.info('=== Transition 1: STANDARD -> LUKEWARM (zstd) ===')
    put_lifecycle_rule(client, BUCKET_NAME, 'to-lukewarm', 'LUKEWARM')
    stat = wait_for_transition(BUCKET_NAME, OBJECT_KEY, 'LUKEWARM')
    verify_transition(stat, 'LUKEWARM', 'zstd')
    orig_size = stat['compression']['orig_size']
    assert orig_size == len(OBJECT_BODY), \
        f'Expected orig_size={len(OBJECT_BODY)}, got {orig_size}'
    body = bucket.Object(OBJECT_KEY).get()['Body'].read()
    assert body == OBJECT_BODY, 'Data mismatch after transition 1'
    log.info('Transition 1 verified: LUKEWARM with zstd compression')

    # Transition 2: LUKEWARM (zstd) -> FROZEN (zlib)
    log.info('=== Transition 2: LUKEWARM -> FROZEN (zlib) ===')
    put_lifecycle_rule(client, BUCKET_NAME, 'to-frozen', 'FROZEN')
    stat = wait_for_transition(BUCKET_NAME, OBJECT_KEY, 'FROZEN')
    verify_transition(stat, 'FROZEN', 'zlib')
    orig_size = stat['compression']['orig_size']
    assert orig_size == len(OBJECT_BODY), \
        f'Expected orig_size={len(OBJECT_BODY)}, got {orig_size}'
    body = bucket.Object(OBJECT_KEY).get()['Body'].read()
    assert body == OBJECT_BODY, 'Data mismatch after transition 2'
    log.info('Transition 2 verified: FROZEN with zlib compression')

    # Transition 3: FROZEN (zlib) -> STANDARD (none)
    log.info('=== Transition 3: FROZEN -> STANDARD (none) ===')
    put_lifecycle_rule(client, BUCKET_NAME, 'to-standard', 'STANDARD')
    stat = wait_for_transition(BUCKET_NAME, OBJECT_KEY, 'STANDARD')
    verify_transition(stat, 'STANDARD', None)
    body = bucket.Object(OBJECT_KEY).get()['Body'].read()
    assert body == OBJECT_BODY, 'Data mismatch after transition 3'
    log.info('Transition 3 verified: STANDARD with no compression')

    # Clean up
    log.info('Cleaning up')
    bucket.objects.all().delete()
    bucket.delete()

    log.info('All lifecycle recompression tests passed')


main()
