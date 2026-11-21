#!/usr/bin/env python3

import logging as log
import subprocess
import sys
import tempfile
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

Also checks that a transition canceled by a concurrent overwrite is
not reported as transitioned, and leaves no data behind.

With --encrypt, objects are uploaded with SSE-KMS and the test
verifies encryption is preserved across each transition. This
requires the compress-encrypted zonegroup feature.

Accepts --size-kb <N> to set the object size in KB (default: runs
both 4KB and 30MB). Sizes above 8MB use multipart upload to
exercise the multipart compressed+encrypted transition path. Without
--encrypt, a multipart object keeps its parts across transitions, and
object rewrite, a versioned bucket's transitions, a canceled multipart
transition and an appendable object's transition are checked too.
"""

USER = 'lc-recompress-tester'
DISPLAY_NAME = 'LC Recompress Testing'
ACCESS_KEY = 'LCRECOMP0123456789AB'
SECRET_KEY = 'lcrecompsecretkey0123456789abcdefghijklmn'
BUCKET_NAME = 'lc-recompress-bucket'
KMS_KEY_ID = 'testkey-1'

LC_POLL_INTERVAL = 10
LC_TIMEOUT = 120
MULTIPART_THRESHOLD = 8 * 1024 * 1024


def is_aead_crypt_mode(mode):
    """
    True for GCM-family crypt modes that derive per-object keys from
    a stored salt. CBC modes don't write crypt.salt at all.
    """
    return mode is not None and mode.endswith('-GCM')


def put_lifecycle_rule(client, bucket_name, rule_id, target_class, days=0):
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
                            'Days': days,
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


def verify_parts(client, bucket_name, object_key, object_body, part_size,
                 version_id=None, etag=None):
    """
    Verify that a multipart object still has its parts: each part reads
    back with GET ?partNumber, and the parts count and sizes are reported.
    """
    v = {'VersionId': version_id} if version_id else {}
    parts_count = -(-len(object_body) // part_size)
    head = client.head_object(Bucket=bucket_name, Key=object_key, PartNumber=1, **v)
    assert head.get('PartsCount') == parts_count, \
        f'Expected {parts_count} parts, HEAD reports {head.get("PartsCount")}'
    if etag:
        assert head['ETag'] == etag, f'ETag changed from {etag} to {head["ETag"]}'
    sizes = []
    for n in range(1, parts_count + 1):
        res = client.get_object(Bucket=bucket_name, Key=object_key, PartNumber=n, **v)
        assert res.get('PartsCount') == parts_count, \
            f'part {n}: expected {parts_count} parts, got {res.get("PartsCount")}'
        expected = object_body[(n - 1) * part_size:n * part_size]
        assert res['ContentLength'] == len(expected), \
            f'part {n}: expected {len(expected)} bytes, got {res["ContentLength"]}'
        assert res['Body'].read() == expected, f'Data mismatch in part {n}'
        sizes.append(len(expected))
    attrs = client.get_object_attributes(Bucket=bucket_name, Key=object_key,
                                         ObjectAttributes=['ObjectParts'], **v)
    parts = attrs.get('ObjectParts', {})
    assert parts.get('TotalPartsCount') == parts_count, \
        f'Expected {parts_count} parts in ObjectParts, got {parts.get("TotalPartsCount")}'
    assert [p['Size'] for p in parts.get('Parts', [])] == sizes, \
        f'Expected part sizes {sizes}, got {parts.get("Parts")}'
    # ranged reads across the part boundaries, which use the compression
    # info of the whole object
    for n in range(1, parts_count):
        start = n * part_size - 4096
        end = n * part_size + 4095
        res = client.get_object(Bucket=bucket_name, Key=object_key,
                                Range=f'bytes={start}-{end}', **v)
        assert res['Body'].read() == object_body[start:end + 1], \
            f'Data mismatch in range {start}-{end}'
    log.info(f'  {parts_count} parts verified')


def upload_multipart(client, bucket_name, object_key, object_body):
    """Upload an object in MULTIPART_THRESHOLD parts and return the response."""
    mpu = client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
    parts = []
    for n, ofs in enumerate(range(0, len(object_body), MULTIPART_THRESHOLD), 1):
        res = client.upload_part(Bucket=bucket_name, Key=object_key,
                                 UploadId=mpu['UploadId'], PartNumber=n,
                                 Body=object_body[ofs:ofs + MULTIPART_THRESHOLD])
        parts.append({'PartNumber': n, 'ETag': res['ETag']})
    return client.complete_multipart_upload(
        Bucket=bucket_name, Key=object_key, UploadId=mpu['UploadId'],
        MultipartUpload={'Parts': parts})


def data_pools():
    """The data pools of the zone's storage classes."""
    zone = json.loads(exec_cmd('radosgw-admin zone get'))
    pools = set()
    for placement in zone['placement_pools']:
        for sc in placement['val']['storage_classes'].values():
            if sc.get('data_pool'):
                pools.add(sc['data_pool'])
    return pools


def bucket_rados_objects(marker):
    """The rados objects in the data pools that belong to a bucket."""
    objs = []
    for pool in data_pools():
        out, ret = exec_cmd(f'rados -p {pool} ls --all', check_retcode=False)
        if ret != 0:
            continue
        for line in out.decode().splitlines():
            oid = line.split('\t')[-1]
            if oid.startswith(marker + '_'):
                objs.append(f'{pool}/{oid}')
    return objs


def delete_bucket_and_check_leaks(conn, bucket_name):
    """Delete all versions and the bucket, run gc, check no data is left."""
    stats = json.loads(exec_cmd(f'radosgw-admin bucket stats --bucket={bucket_name}'))
    bucket = conn.Bucket(bucket_name)
    bucket.object_versions.all().delete()
    bucket.delete()
    exec_cmd('radosgw-admin gc process --include-all')
    left = bucket_rados_objects(stats['marker'])
    assert not left, f'rados objects left after delete and gc: {left}'


def run_happy_path_transition_tests(size_kb, encrypt):
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

    # re-encryption writes a single stream, so only unencrypted objects
    # keep their parts
    etag = client.head_object(Bucket=BUCKET_NAME, Key=object_key)['ETag']
    def check_parts():
        if size_bytes > MULTIPART_THRESHOLD and not encrypt:
            verify_parts(client, BUCKET_NAME, object_key, object_body,
                         MULTIPART_THRESHOLD, etag=etag)
    check_parts()
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
    check_parts()
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
    check_parts()
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
    check_parts()
    prev_salt = assert_salt_rotated(stat, prev_salt)
    log.info('Transition 3 verified')

    # Clean up
    delete_bucket_and_check_leaks(conn, BUCKET_NAME)
    log.info(f'{label} test passed')


def run_multipart_rewrite_test():
    """
    radosgw-admin object rewrite keeps the parts of a multipart object, and
    of a version of one.
    """
    log.info('=== Testing object rewrite of multipart objects ===')
    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    body = make_compressible_body(30 * 1024 * 1024)

    for versioned in (False, True):
        bucket_name = BUCKET_NAME + ('-ver' if versioned else '-rewrite')
        conn.create_bucket(Bucket=bucket_name)
        if versioned:
            client.put_bucket_versioning(Bucket=bucket_name,
                                         VersioningConfiguration={'Status': 'Enabled'})
        key = 'test-object-rewrite'
        res = upload_multipart(client, bucket_name, key, body)
        version_id = res.get('VersionId') if versioned else None
        etag = res['ETag']

        cmd = f'radosgw-admin object rewrite --bucket={bucket_name} --object={key}'
        if version_id:
            cmd += f' --object-version={version_id}'
        exec_cmd(cmd)

        v = {'VersionId': version_id} if version_id else {}
        got = client.get_object(Bucket=bucket_name, Key=key, **v)['Body'].read()
        assert got == body, f'Data mismatch after rewrite (versioned={versioned})'
        verify_parts(client, bucket_name, key, body, MULTIPART_THRESHOLD,
                     version_id=version_id, etag=etag)
        delete_bucket_and_check_leaks(conn, bucket_name)
    log.info('Multipart rewrite test passed')


def run_multipart_versioned_transition_test():
    """
    Lifecycle transitions keep the parts of the current and noncurrent
    versions of a multipart object.
    """
    log.info('=== Testing transitions of multipart versions ===')
    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    bucket_name = BUCKET_NAME + '-ver'
    conn.create_bucket(Bucket=bucket_name)
    client.put_bucket_versioning(Bucket=bucket_name,
                                 VersioningConfiguration={'Status': 'Enabled'})
    key = 'test-object-versions'
    old_body = make_compressible_body(30 * 1024 * 1024)
    new_body = old_body[::-1]
    old = upload_multipart(client, bucket_name, key, old_body)
    new = upload_multipart(client, bucket_name, key, new_body)

    client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={'Rules': [{
            'ID': 'to-lukewarm', 'Filter': {'Prefix': ''}, 'Status': 'Enabled',
            'Transitions': [{'Days': 0, 'StorageClass': 'LUKEWARM'}],
            'NoncurrentVersionTransitions': [
                {'NoncurrentDays': 1, 'StorageClass': 'LUKEWARM'}],
        }]})

    versions = [(old['VersionId'], old['ETag'], old_body),
                (new['VersionId'], new['ETag'], new_body)]
    deadline = time.time() + LC_TIMEOUT
    while True:
        exec_cmd(f'radosgw-admin lc process --bucket={bucket_name}'
                 ' --rgw-lc-debug-interval=10')
        classes = [client.head_object(Bucket=bucket_name, Key=key, VersionId=vid)
                   .get('StorageClass', 'STANDARD') for vid, _, _ in versions]
        log.info(f'  storage classes: {classes}')
        if classes == ['LUKEWARM', 'LUKEWARM']:
            break
        assert time.time() < deadline, f'versions not transitioned: {classes}'
        time.sleep(LC_POLL_INTERVAL)

    for vid, etag, body in versions:
        got = client.get_object(Bucket=bucket_name, Key=key, VersionId=vid)['Body'].read()
        assert got == body, f'Data mismatch in version {vid}'
        verify_parts(client, bucket_name, key, body, MULTIPART_THRESHOLD,
                     version_id=vid, etag=etag)
    delete_bucket_and_check_leaks(conn, bucket_name)
    log.info('Multipart versioned transition test passed')


def run_failure_test_canceled_transition(multipart=False):
    """
    Overwrite an object while lifecycle is transitioning it.
    Lifecycle should not report it as transitioned, and should remove
    what it copied, including the parts of a multipart object.
    """
    log.info(f'=== Testing transition canceled by a concurrent overwrite'
             f' (multipart={multipart}) ===')
    object_key = 'test-object-canceled'
    new_body = b'new data'
    bucket_name = BUCKET_NAME + '-cancel'

    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    bucket = conn.create_bucket(Bucket=bucket_name)
    if multipart:
        upload_multipart(client, bucket_name, object_key,
                         make_compressible_body(30 * 1024 * 1024))
    else:
        bucket.put_object(Key=object_key, Body=b'old data')
    # due in 5s for radosgw-admin (1s days) but in 50s for rgw's own LC thread (10s days)
    transition_days = 5
    put_lifecycle_rule(client, bucket_name, 'to-lukewarm', 'LUKEWARM', days=transition_days)
    time.sleep(transition_days + 1)

    # hold the transition after it reads the object and before it rewrites it
    with tempfile.NamedTemporaryFile(suffix='.log') as lc_log:
        def read_lc_log():
            with open(lc_log.name, 'rb') as f:
                return f.read()

        lc = None
        deadline = time.time() + LC_TIMEOUT
        while b'injecting delay' not in read_lc_log():
            if lc is None or lc.poll() is not None:
                lc = subprocess.Popen(
                    f'radosgw-admin lc process --bucket={bucket_name} --rgw-lc-debug-interval=1'
                    ' --rgw-inject-delay-sec=5 --rgw-inject-delay-pattern=delay_copy_obj_data_complete'
                    f' --debug-rgw-lifecycle=2 --log-file={lc_log.name}', shell=True)
            assert time.time() < deadline, 'timed out waiting for the transition to start'
            time.sleep(0.5)
        bucket.put_object(Key=object_key, Body=new_body)
        assert lc.wait(timeout=LC_TIMEOUT) == 0
        out = read_lc_log()

    assert b'TRANSITIONED:' not in out, 'canceled transition was reported as transitioned'
    assert b'object changed concurrently' in out, 'canceled transition was not logged'
    stat = object_stat(bucket_name, object_key)
    assert get_storage_class(stat) == 'STANDARD'
    assert bucket.Object(object_key).get()['Body'].read() == new_body

    # the canceled copy removed its data, and gc removes the replaced object's
    delete_bucket_and_check_leaks(conn, bucket_name)
    log.info('Canceled transition test passed')


def append_object(client, bucket_name, key, data, position):
    """Append data to an object with RGW's AppendObject extension."""
    def add_params(request, **kwargs):
        request.url += ('&' if '?' in request.url else '?') + \
            f'append&position={position}'
    client.meta.events.register_first('before-sign.s3.PutObject', add_params)
    try:
        client.put_object(Bucket=bucket_name, Key=key, Body=data)
    finally:
        client.meta.events.unregister('before-sign.s3.PutObject', add_params)


def run_appendable_transition_test():
    """
    Each append to an appendable object is a part, but there are no parts
    to keep. A transition copies it as a single stream.
    """
    log.info('=== Testing transition of an appendable object ===')
    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    bucket_name = BUCKET_NAME + '-append'
    conn.create_bucket(Bucket=bucket_name)
    key = 'test-object-appendable'
    body = make_compressible_body(3 * 1024 * 1024)
    chunk = 1024 * 1024
    for ofs in range(0, len(body), chunk):
        append_object(client, bucket_name, key, body[ofs:ofs + chunk], ofs)
    stat = object_stat(bucket_name, key)
    rules = [r.get('val', r) for r in stat['manifest']['rules']]
    assert any(r['start_part_num'] > 0 for r in rules), \
        f'expected the appends as parts: {rules}'

    put_lifecycle_rule(client, bucket_name, 'to-lukewarm', 'LUKEWARM')
    stat = wait_for_transition(bucket_name, key, 'LUKEWARM')
    verify_transition(stat, 'LUKEWARM', 'zstd')
    rules = [r.get('val', r) for r in stat['manifest']['rules']]
    assert all(r['start_part_num'] == 0 for r in rules), \
        f'expected a single stream: {rules}'
    got = client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
    assert got == body, 'Data mismatch after transition'
    delete_bucket_and_check_leaks(conn, bucket_name)
    log.info('Appendable transition test passed')


def main():
    encrypt = '--encrypt' in sys.argv

    # parse --size-kb <N> for a single size, otherwise run both 4KB and 30MB
    # (multipart, with a shorter last part)
    sizes_kb = [4, 30 * 1024]
    for i, arg in enumerate(sys.argv):
        if arg == '--size-kb' and i + 1 < len(sys.argv):
            sizes_kb = [int(sys.argv[i + 1])]
            break

    if encrypt:
        log.info('Running with encryption enabled (SSE-KMS)')

    log.info('Creating test user')
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    for size_kb in sizes_kb:
        run_happy_path_transition_tests(size_kb, encrypt)

    if not encrypt:
        run_multipart_rewrite_test()
        run_multipart_versioned_transition_test()
        run_appendable_transition_test()
        run_failure_test_canceled_transition(multipart=True)

    run_failure_test_canceled_transition()

    suffix = ' (with encryption)' if encrypt else ''
    log.info(f'All lifecycle recompression tests passed{suffix}')


main()
