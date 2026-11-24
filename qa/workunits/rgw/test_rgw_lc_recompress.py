#!/usr/bin/env python3

import argparse
import contextlib
import logging as log
import os
import subprocess
import tempfile
import threading
import time
from botocore.config import Config
from botocore.exceptions import ClientError
from common import exec_cmd, create_user, boto_connect, object_stat, \
    get_compression_type, get_storage_class, get_crypt_mode, \
    get_crypt_attr_raw, make_compressible_body, upload_object, \
    has_pending_write, wait_for_pending_write, MULTIPART_THRESHOLD

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
not reported as transitioned, that a write racing a transition, an
object rewrite or a tagging update retries instead of being lost, and
that a write that runs out of retries fails instead of being lost.

With --encrypt, objects are uploaded with SSE-KMS and the test
verifies encryption is preserved across each transition. This
requires the compress-encrypted zonegroup feature.

Accepts --size-kb <N> to set the object size in KB (default: runs
both 4KB and 32MB). Sizes above 8MB use multipart upload to
exercise the multipart compressed+encrypted transition path.

For local runs: --race-only runs only the write race cases, and
--case <name>[,<name>...] only the named ones (see RACE_CASES). The
RGW_TEST_INJECT_DELAY environment variable sets the seconds rgw holds
a raced write (default 20, minimum 5).
"""

USER = 'lc-recompress-tester'
DISPLAY_NAME = 'LC Recompress Testing'
ACCESS_KEY = 'LCRECOMP0123456789AB'
SECRET_KEY = 'lcrecompsecretkey0123456789abcdefghijklmn'
BUCKET_NAME = 'lc-recompress-bucket'
KMS_KEY_ID = 'testkey-1'

LC_POLL_INTERVAL = 10
LC_TIMEOUT = 120
MIN_INJECT_DELAY = 5


def get_inject_delay():
    """
    Seconds rgw holds each guarded attempt of a write before its head
    write: RGW_TEST_INJECT_DELAY, or 20. The hold must cover the radosgw-admin
    calls that land the concurrent change, hence the minimum.
    """
    value = os.environ.get('RGW_TEST_INJECT_DELAY')
    if value is None:
        return 20
    delay = float(value)
    if delay < MIN_INJECT_DELAY:
        raise SystemExit(f'RGW_TEST_INJECT_DELAY={value} is below the minimum'
                         f' of {MIN_INJECT_DELAY}s')
    return int(delay) if delay.is_integer() else delay


INJECT_DELAY = get_inject_delay()
WRITE_MAX_ATTEMPTS = 4  # guarded attempts of a write: the first, plus rgw's 3 retries
CEPH_CFG_PROPAGATION_WAIT = 5  # seconds for 'ceph config set/rm' to reach rgw
TAIL_BODY_SIZE = 8 * 1024 * 1024  # above rgw_max_chunk_size, so the object has a tail
# a raced write is held for at most WRITE_MAX_ATTEMPTS * INJECT_DELAY. Slack
# on top of that, at least a fixed amount so that a short delay still leaves
# room for the requests themselves. The join outlasts the client's read
# timeout, so that a timed out write fails with an error
WRITE_READ_TIMEOUT = WRITE_MAX_ATTEMPTS * INJECT_DELAY + max(2 * INJECT_DELAY, 30)
WRITE_JOIN_TIMEOUT = WRITE_MAX_ATTEMPTS * INJECT_DELAY + max(3 * INJECT_DELAY, 40)


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


def run_failure_test_canceled_transition():
    """
    Overwrite an object while lifecycle is transitioning it.
    Lifecycle should not report it as transitioned.
    """
    log.info('=== Testing transition canceled by a concurrent overwrite ===')
    object_key = 'test-object-canceled'
    new_body = b'new data'

    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    bucket = conn.create_bucket(Bucket=BUCKET_NAME)
    bucket.put_object(Key=object_key, Body=b'old data')
    # due in 5s for radosgw-admin (1s days) but in 50s for rgw's own LC thread (10s days)
    transition_days = 5
    put_lifecycle_rule(client, BUCKET_NAME, 'to-lukewarm', 'LUKEWARM', days=transition_days)
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
                    f'radosgw-admin lc process --bucket={BUCKET_NAME} --rgw-lc-debug-interval=1'
                    ' --rgw-inject-delay-sec=5 --rgw-inject-delay-pattern=delay_copy_obj_data_complete'
                    f' --debug-rgw-lifecycle=2 --log-file={lc_log.name}', shell=True)
            assert time.time() < deadline, 'timed out waiting for the transition to start'
            time.sleep(0.5)
        bucket.put_object(Key=object_key, Body=new_body)
        assert lc.wait(timeout=LC_TIMEOUT) == 0
        out = read_lc_log()

    assert b'TRANSITIONED:' not in out, 'canceled transition was reported as transitioned'
    assert b'object changed concurrently' in out, 'canceled transition was not logged'
    stat = object_stat(BUCKET_NAME, object_key)
    assert get_storage_class(stat) == 'STANDARD'
    assert bucket.Object(object_key).get()['Body'].read() == new_body

    bucket.objects.all().delete()
    bucket.delete()
    log.info('Canceled transition test passed')


def cleanup_bucket(conn, bucket_name):
    """Delete a bucket with all its versions and multipart uploads."""
    client = conn.meta.client
    try:
        uploads = client.list_multipart_uploads(Bucket=bucket_name).get('Uploads', [])
        for u in uploads:
            client.abort_multipart_upload(Bucket=bucket_name, Key=u['Key'],
                                          UploadId=u['UploadId'])
        bucket = conn.Bucket(bucket_name)
        bucket.object_versions.all().delete()
        bucket.delete()
    except Exception as e:
        log.warning(f'failed to clean up bucket {bucket_name}: {e}')


def write_client():
    """
    A client for a held write: no retries, so that a resent request can't
    hide a lost write, and a read timeout that outlasts every hold of it.
    """
    return boto_connect(ACCESS_KEY, SECRET_KEY, config=Config(
        retries={'total_max_attempts': 1},
        read_timeout=WRITE_READ_TIMEOUT)).meta.client


def start_write(do_write):
    """
    Run do_write in a thread. Returns the thread and a dict that gets the
    'elapsed' seconds and the 'error' it raised, if any.
    """
    result = {}
    def write():
        start = time.monotonic()
        try:
            do_write()
        except Exception as e:
            result['error'] = e
        result['elapsed'] = time.monotonic() - start
    t = threading.Thread(target=write, daemon=True)
    t.start()
    return t, result


@contextlib.contextmanager
def hold_guarded_writes():
    """
    Hold each guarded attempt of a write after it reads the head and before
    it writes it. radosgw-admin must pass --rgw-inject-delay-sec=0 so that
    it isn't held.
    """
    try:
        exec_cmd(f'ceph config set client rgw_inject_delay_sec {INJECT_DELAY}')
        exec_cmd('ceph config set client rgw_inject_delay_pattern'
                 ' delay_write_meta_guarded_head_write')
        time.sleep(CEPH_CFG_PROPAGATION_WAIT)
        yield
    finally:
        exec_cmd('ceph config rm client rgw_inject_delay_sec')
        exec_cmd('ceph config rm client rgw_inject_delay_pattern')
        time.sleep(CEPH_CFG_PROPAGATION_WAIT)


def run_test_write_racing_head_change(name, changer, writer='put',
                                      versioning=None, body=b'new data',
                                      if_match=True):
    """
    Hold a write between reading the object's head and writing it, and
    replace the head meanwhile with one stamped before the write: by an lc
    transition or an object rewrite, which keep the old mtime, or by a
    tagging update, which adds 1ns to it. The held write is still the
    newest, so it must retry and win, not be reported as a success and
    dropped. writer is 'put' (PutObject) or 'multipart'
    (CompleteMultipartUpload).

    With if_match, the write sends If-Match with the ETag of the head, which
    the change keeps. Its first attempt is then guarded: it reads the head,
    prepares its pending bucket index entry and is held, so that entry shows
    that the head was read. Without it, the write first tries an exclusive
    create that prepares the entry and fails, unheld, and its guarded retry
    reads the head two osd round trips later. The change must then be one
    that radosgw-admin, a new process, makes, to start well after that.
    """
    assert if_match or changer != 'tagging', 'a tagging update may beat the head read'
    label = f'{writer} racing {changer} ({name}{", If-Match" if if_match else ""})'
    log.info(f'=== Testing a {label} ===')
    bucket_name = f'{BUCKET_NAME}-race-{name}'
    object_key = 'test-object-write-race'

    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    put_client = write_client()

    try:
        bucket = conn.create_bucket(Bucket=bucket_name)
        if versioning == 'Suspended':
            # leave a version behind, so that the head is an olh, and the
            # writes below are null versions over it
            client.put_bucket_versioning(Bucket=bucket_name,
                                         VersioningConfiguration={'Status': 'Enabled'})
            bucket.put_object(Key=object_key, Body=b'versioned data')
            client.put_bucket_versioning(Bucket=bucket_name,
                                         VersioningConfiguration={'Status': 'Suspended'})
        bucket.put_object(Key=object_key, Body=b'old data')
        if changer == 'transition':
            # due in 5s for radosgw-admin (1s days) but in 50s for rgw's own LC thread (10s days)
            transition_days = 5
            put_lifecycle_rule(client, bucket_name, 'to-lukewarm', 'LUKEWARM', days=transition_days)
            time.sleep(transition_days + 1)
        stat_before = object_stat(bucket_name, object_key)
        cond = {}
        if if_match:
            cond['IfMatch'] = client.head_object(Bucket=bucket_name, Key=object_key)['ETag']
        if writer == 'multipart':
            upload_id = client.create_multipart_upload(Bucket=bucket_name,
                                                       Key=object_key)['UploadId']
            part_etag = client.upload_part(Bucket=bucket_name, Key=object_key,
                                           PartNumber=1, UploadId=upload_id,
                                           Body=body)['ETag']

        def write():
            if writer == 'multipart':
                put_client.complete_multipart_upload(
                    Bucket=bucket_name, Key=object_key, UploadId=upload_id,
                    MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part_etag}]},
                    **cond)
            else:
                put_client.put_object(Bucket=bucket_name, Key=object_key, Body=body,
                                      **cond)

        with hold_guarded_writes():
            # the first hold ends INJECT_DELAY after its attempt starts, so
            # after this
            hold_end = time.monotonic() + INJECT_DELAY
            t, result = start_write(write)
            wait_for_pending_write(bucket_name, object_key, writer=t)
            time.sleep(1)
            assert t.is_alive(), f'{writer} was not held'

            if changer == 'transition':
                while True:
                    exec_cmd(f'radosgw-admin lc process --bucket={bucket_name}'
                             ' --rgw-lc-debug-interval=1 --rgw-inject-delay-sec=0')
                    if get_storage_class(object_stat(bucket_name, object_key)) == 'LUKEWARM':
                        break
                    assert time.monotonic() < hold_end, 'timed out waiting for the transition'
                # keep rgw's own LC thread off the write once it lands
                client.delete_bucket_lifecycle(Bucket=bucket_name)
            elif changer == 'rewrite':
                exec_cmd(f'radosgw-admin object rewrite --bucket={bucket_name}'
                         f' --object={object_key} --rgw-inject-delay-sec=0')
                stat = object_stat(bucket_name, object_key)
                assert stat['manifest']['prefix'] != stat_before['manifest']['prefix'], \
                    'object was not rewritten'
            else:
                client.put_object_tagging(Bucket=bucket_name, Key=object_key,
                                          Tagging={'TagSet': [{'Key': 'race', 'Value': 'lost'}]})
            assert t.is_alive(), f'{writer} was not held during the {changer}'
            t.join(timeout=WRITE_JOIN_TIMEOUT)
            assert not t.is_alive(), f'{writer} did not finish'

        assert 'error' not in result, f'{writer} failed: {result.get("error")}'
        # held once per guarded attempt: once more than INJECT_DELAY means
        # that it retried
        assert result['elapsed'] >= 1.5 * INJECT_DELAY, \
            f'{writer} took {result["elapsed"]:.1f}s, so it did not retry'
        got = bucket.Object(object_key).get()['Body'].read()
        assert got == body, f'{writer} was acknowledged but its data was lost'
        assert get_storage_class(object_stat(bucket_name, object_key)) == 'STANDARD'
        if changer == 'tagging':
            tags = client.get_object_tagging(Bucket=bucket_name, Key=object_key)['TagSet']
            assert tags == [], f'tags of the replaced head survived: {tags}'
    finally:
        cleanup_bucket(conn, bucket_name)
    log.info(f'A {label} test passed')


def run_test_write_retries_exhausted():
    """
    Replace the head with an older one, by a tagging update, during every
    guarded attempt of a plain PUT. Out of retries, the PUT must fail with
    503, not be reported as a success and dropped, leave the object as the
    last tagging update left it, and leave no pending bucket index entry.
    """
    label = 'put out of retries'
    log.info(f'=== Testing a {label} ===')
    bucket_name = f'{BUCKET_NAME}-race-exhausted'
    object_key = 'test-object-write-race'
    old_body = b'old data'

    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    put_client = write_client()

    def tag_set(n):
        return [{'Key': 'race', 'Value': str(n)}]

    try:
        bucket = conn.create_bucket(Bucket=bucket_name)
        bucket.put_object(Key=object_key, Body=old_body)

        with hold_guarded_writes():
            start = time.monotonic()
            t, result = start_write(lambda: put_client.put_object(
                Bucket=bucket_name, Key=object_key, Body=b'new data'))
            wait_for_pending_write(bucket_name, object_key, writer=t)
            seen = time.monotonic()
            # the PUT's exclusive create prepared that entry, and its first
            # guarded attempt is held two osd round trips later, so between
            # start and seen. Attempt n (from 0) is held for INJECT_DELAY from
            # n * INJECT_DELAY after that, plus a few osd round trips per
            # retry. Update the tags where every hold start allows: in the
            # middle of [seen, start + INJECT_DELAY], shifted by n holds
            margin = (INJECT_DELAY - (seen - start)) / 2
            assert margin >= 1, \
                f'{seen - start:.1f}s to see the held PUT, raise RGW_TEST_INJECT_DELAY'
            for n in range(WRITE_MAX_ATTEMPTS):
                time.sleep(max(seen + margin + n * INJECT_DELAY - time.monotonic(), 0))
                assert t.is_alive(), f'put finished before tagging update {n}'
                client.put_object_tagging(Bucket=bucket_name, Key=object_key,
                                          Tagging={'TagSet': tag_set(n)})
            t.join(timeout=WRITE_JOIN_TIMEOUT)
            assert not t.is_alive(), 'put did not finish'

        error = result.get('error')
        assert isinstance(error, ClientError) and \
            error.response['ResponseMetadata']['HTTPStatusCode'] == 503, \
            f'put out of retries returned {error!r}, not a 503'
        assert result['elapsed'] >= (WRITE_MAX_ATTEMPTS - 0.5) * INJECT_DELAY, \
            f'put took {result["elapsed"]:.1f}s, so it was not held {WRITE_MAX_ATTEMPTS} times'
        assert bucket.Object(object_key).get()['Body'].read() == old_body, \
            'put failed but its data was written'
        tags = client.get_object_tagging(Bucket=bucket_name, Key=object_key)['TagSet']
        assert tags == tag_set(WRITE_MAX_ATTEMPTS - 1), f'unexpected tags {tags}'
        assert not has_pending_write(bucket_name, object_key), \
            'the failed put left its bucket index entry pending'
    finally:
        cleanup_bucket(conn, bucket_name)
    log.info(f'A {label} test passed')


RACE_CASES = {
    'transition': lambda: run_test_write_racing_head_change('transition', 'transition'),
    # plain PUTs without If-Match, the writes that were acknowledged and
    # lost: in an unversioned bucket, and as a null version over an olh
    'plain-rewrite': lambda: run_test_write_racing_head_change(
        'plain-rewrite', 'rewrite', if_match=False),
    'suspended': lambda: run_test_write_racing_head_change(
        'suspended', 'transition', versioning='Suspended', if_match=False),
    'tail': lambda: run_test_write_racing_head_change(
        'tail', 'rewrite', body=make_compressible_body(TAIL_BODY_SIZE)),
    'multipart': lambda: run_test_write_racing_head_change(
        'multipart', 'rewrite', writer='multipart'),
    'tagging': lambda: run_test_write_racing_head_change('tagging', 'tagging'),
    # a plain PUT that runs out of retries
    'exhausted': run_test_write_retries_exhausted,
}


def parse_cases(value):
    cases = [c for c in value.split(',') if c]
    unknown = [c for c in cases if c not in RACE_CASES]
    if not cases or unknown:
        raise argparse.ArgumentTypeError(
            f'unknown case(s) {unknown}, choose from {", ".join(RACE_CASES)}')
    return cases


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--encrypt', action='store_true',
                        help='upload with SSE-KMS (happy paths only)')
    parser.add_argument('--size-kb', type=int,
                        help='object size of the happy path (default: 4KB and 32MB)')
    parser.add_argument('--race-only', action='store_true',
                        help='skip the happy paths and the canceled transition test')
    parser.add_argument('--case', type=parse_cases,
                        help='run only these write race cases, comma separated:'
                        f' {",".join(RACE_CASES)}. Implies --race-only')
    args = parser.parse_args()
    encrypt = args.encrypt
    race_only = args.race_only or args.case is not None
    cases = args.case or list(RACE_CASES)

    # a single size if --size-kb, otherwise both 4KB and 32MB
    sizes_kb = [args.size_kb] if args.size_kb is not None else [4, 32 * 1024]

    if encrypt:
        log.info('Running with encryption enabled (SSE-KMS)')
    log.info(f'Holding raced writes for {INJECT_DELAY}s')

    log.info('Creating test user')
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    if not race_only:
        for size_kb in sizes_kb:
            run_happy_path_transition_tests(size_kb, encrypt)

        run_failure_test_canceled_transition()
    for case in cases:
        RACE_CASES[case]()

    suffix = ' (with encryption)' if encrypt else ''
    log.info(f'All lifecycle recompression tests passed{suffix}')


main()
