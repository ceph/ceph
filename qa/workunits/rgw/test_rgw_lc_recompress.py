#!/usr/bin/env python3

import logging as log
import sys
import time
from common import exec_cmd, create_user, boto_connect, object_stat, \
    get_compression_type, get_storage_class, get_crypt_mode, \
    get_crypt_attr_raw, make_compressible_body, upload_object, \
    MULTIPART_THRESHOLD

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

With --encrypt, objects are uploaded with SSE-KMS and the test
verifies encryption is preserved across each transition. This
requires the compress-encrypted zonegroup feature.

Accepts --size-kb <N> to set the object size in KB (default: runs
both 4KB and 32MB). Sizes above 8MB use multipart upload to
exercise the multipart compressed+encrypted transition path.
"""

USER = 'lc-recompress-tester'
DISPLAY_NAME = 'LC Recompress Testing'
ACCESS_KEY = 'LCRECOMP0123456789AB'
SECRET_KEY = 'lcrecompsecretkey0123456789abcdefghijklmn'
BUCKET_NAME = 'lc-recompress-bucket'
KMS_KEY_ID = 'testkey-1'

LC_POLL_INTERVAL = 10
LC_TIMEOUT = 120


def is_aead_crypt_mode(mode):
    """
    True for GCM-family crypt modes that derive per-object keys from
    a stored salt. CBC modes don't write crypt.salt at all.
    """
    return mode is not None and mode.endswith('-GCM')


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
        exec_cmd(f'radosgw-admin lc process --bucket={bucket_name}'
                 ' --rgw-lc-debug-interval=10')
        time.sleep(LC_POLL_INTERVAL)

        stat = object_stat(bucket_name, object_key)
        sc = get_storage_class(stat)
        log.info(f'  current storage class: {sc} (waiting for {expected_class})')
        if sc == expected_class:
            return stat

    raise AssertionError(
        f'Timed out waiting for object to transition to {expected_class}'
    )


def verify_transition(stat, expected_class, expected_compression, encrypt=False):
    """
    Verify that the object stat matches the expected storage class
    and compression type after a transition. When encrypt=True,
    also verify that encryption mode is still present.
    """
    sc = get_storage_class(stat)
    ct = get_compression_type(stat)
    on_disk = stat.get('size', 0)
    comp = stat.get('compression', {})
    orig = comp.get('orig_size', on_disk)

    log.info(f'  storage_class={sc}, compression_type={ct},'
             f' on_disk={on_disk}, orig_size={orig}')
    assert sc == expected_class, \
        f'Expected storage class {expected_class}, got {sc}'
    assert ct == expected_compression, \
        f'Expected compression type {expected_compression}, got {ct}'

    if encrypt:
        mode = get_crypt_mode(stat)
        log.info(f'  crypt_mode={mode}')
        assert mode is not None, \
            'Expected object to be encrypted, but crypt mode is missing'


def run_test(size_kb, encrypt):
    """Run the full transition test with the given object size."""
    size_bytes = size_kb * 1024
    object_key = f'test-object-{size_kb}kb'
    object_body = make_compressible_body(size_bytes)
    label = f'{size_kb}KB'
    if size_kb >= 1024:
        label = f'{size_kb // 1024}MB'

    log.info(f'=== Testing {label} object (encrypt={encrypt}) ===')

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

    # Upload object — use multipart for sizes above 8MB to exercise
    # the multipart compressed+encrypted transition path
    extra_args = {}
    if encrypt:
        extra_args['ServerSideEncryption'] = 'aws:kms'
        extra_args['SSEKMSKeyId'] = KMS_KEY_ID

    multipart = ' (multipart)' if size_bytes > MULTIPART_THRESHOLD else ''
    log.info(f'Uploading {len(object_body)} byte object as {object_key}{multipart}')
    upload_object(client, BUCKET_NAME, object_key, object_body, extra_args)

    stat = object_stat(BUCKET_NAME, object_key)
    verify_transition(stat, 'STANDARD', None, encrypt)
    log.info('Initial upload verified: STANDARD, no compression')
    # Salt rotation only applies to AEAD modes (CBC has no salt attr).
    prev_salt = None
    if encrypt and is_aead_crypt_mode(get_crypt_mode(stat)):
        prev_salt = get_crypt_attr_raw(BUCKET_NAME, object_key, 'salt')

    def assert_salt_rotated(new_stat, prev):
        if not encrypt or not is_aead_crypt_mode(get_crypt_mode(new_stat)):
            return None
        new_salt = get_crypt_attr_raw(BUCKET_NAME, object_key, 'salt')
        assert new_salt != prev, \
            'crypt.salt did not rotate across re-encryption'
        return new_salt

    # Transition 1: STANDARD (none) -> LUKEWARM (zstd)
    log.info(f'--- Transition 1: STANDARD -> LUKEWARM (zstd) [{label}] ---')
    put_lifecycle_rule(client, BUCKET_NAME, 'to-lukewarm', 'LUKEWARM')
    stat = wait_for_transition(BUCKET_NAME, object_key, 'LUKEWARM')
    verify_transition(stat, 'LUKEWARM', 'zstd', encrypt)
    orig_size = stat['compression']['orig_size']
    assert orig_size == len(object_body), \
        f'Expected orig_size={len(object_body)}, got {orig_size}'
    body = bucket.Object(object_key).get()['Body'].read()
    assert body == object_body, 'Data mismatch after transition 1'
    prev_salt = assert_salt_rotated(stat, prev_salt)
    log.info('Transition 1 verified')

    # Transition 2: LUKEWARM (zstd) -> FROZEN (zlib)
    log.info(f'--- Transition 2: LUKEWARM -> FROZEN (zlib) [{label}] ---')
    put_lifecycle_rule(client, BUCKET_NAME, 'to-frozen', 'FROZEN')
    stat = wait_for_transition(BUCKET_NAME, object_key, 'FROZEN')
    verify_transition(stat, 'FROZEN', 'zlib', encrypt)
    orig_size = stat['compression']['orig_size']
    assert orig_size == len(object_body), \
        f'Expected orig_size={len(object_body)}, got {orig_size}'
    body = bucket.Object(object_key).get()['Body'].read()
    assert body == object_body, 'Data mismatch after transition 2'
    prev_salt = assert_salt_rotated(stat, prev_salt)
    log.info('Transition 2 verified')

    # Same-codec CopyObject (compressed+encrypted only) — exercises the
    # passthrough path where set_writer skips decompression. Verifies
    # that CRYPT_ORIGINAL_SIZE is the plaintext size (not the post-
    # decrypt compressed bytes), which drives bucket-index quota.
    if encrypt:
        copy_key = f'{object_key}-same-codec-copy'
        log.info(f'--- Same-codec CopyObject passthrough [{label}] ---')
        client.copy_object(
            Bucket=BUCKET_NAME, Key=copy_key,
            CopySource={'Bucket': BUCKET_NAME, 'Key': object_key},
            StorageClass='FROZEN',
            ServerSideEncryption='aws:kms', SSEKMSKeyId=KMS_KEY_ID,
            MetadataDirective='COPY',
        )
        copy_stat = object_stat(BUCKET_NAME, copy_key)
        verify_transition(copy_stat, 'FROZEN', 'zlib', encrypt)
        copy_orig = copy_stat['compression']['orig_size']
        assert copy_orig == len(object_body), \
            f'Same-codec copy orig_size {copy_orig} != plaintext {len(object_body)}'
        if is_aead_crypt_mode(get_crypt_mode(copy_stat)):
            copy_salt = get_crypt_attr_raw(BUCKET_NAME, copy_key, 'salt')
            assert copy_salt != prev_salt, \
                'Copy salt did not rotate vs source'
        body = bucket.Object(copy_key).get()['Body'].read()
        assert body == object_body, 'Data mismatch on same-codec copy'
        bucket.Object(copy_key).delete()
        log.info('Same-codec CopyObject passthrough verified')

    # Transition 3: FROZEN (zlib) -> STANDARD (none)
    log.info(f'--- Transition 3: FROZEN -> STANDARD (none) [{label}] ---')
    put_lifecycle_rule(client, BUCKET_NAME, 'to-standard', 'STANDARD')
    stat = wait_for_transition(BUCKET_NAME, object_key, 'STANDARD')
    verify_transition(stat, 'STANDARD', None, encrypt)
    body = bucket.Object(object_key).get()['Body'].read()
    assert body == object_body, 'Data mismatch after transition 3'
    prev_salt = assert_salt_rotated(stat, prev_salt)
    log.info('Transition 3 verified')

    # Clean up
    bucket.objects.all().delete()
    bucket.delete()
    log.info(f'{label} test passed')


def main():
    encrypt = '--encrypt' in sys.argv

    # parse --size-kb <N> for a single size, otherwise run both 4KB and 32MB
    sizes_kb = [4, 32 * 1024]
    for i, arg in enumerate(sys.argv):
        if arg == '--size-kb' and i + 1 < len(sys.argv):
            sizes_kb = [int(sys.argv[i + 1])]
            break

    if encrypt:
        log.info('Running with encryption enabled (SSE-KMS)')

    log.info('Creating test user')
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    for size_kb in sizes_kb:
        run_test(size_kb, encrypt)

    suffix = ' (with encryption)' if encrypt else ''
    log.info(f'All lifecycle recompression tests passed{suffix}')


main()
