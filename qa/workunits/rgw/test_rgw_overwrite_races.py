#!/usr/bin/env python3

import json
import logging as log
import random
import string
import sys
import threading
import time
from contextlib import contextmanager

import botocore.exceptions
from botocore.config import Config
from common import exec_cmd, create_user, boto_connect

"""
Races between writers of one key that leak RADOS objects or lose data.

Each case holds one request at an rgw_inject_delay_pattern point while
another request changes the key. Afterwards the case deletes every key and
drains GC: no RADOS object named for the bucket may remain in the data
pool, and an object that is still listed must read back whole.

The cases need a radosgw built with the injection points they name. A case
whose race did not play out as arranged is reported as inconclusive, not
as passed.
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

""" Constants """
USER = 'race-tester'
DISPLAY_NAME = 'Overwrite Race Testing'
ACCESS_KEY = 'RACE3NQ7M1ZLXK0T5YBW'
SECRET_KEY = 'k2Jd8fQz0PmX4vRt7LwN1sYc6HbGe9UaTo3iVn5Z'
MB = 1024 * 1024
OBJ_SIZE = 8 * MB   # past the 4 MiB head, so the object has a tail
DELAY = 6           # seconds a request waits at an injection point


class Inconclusive(Exception):
    """the race did not play out as the case arranged it"""


class Request(threading.Thread):
    """one S3 request, run in the background"""
    def __init__(self, name, fn):
        super().__init__(name=name, daemon=True)
        self.fn = fn
        self.result = None
        self.error = None
        self.elapsed = None

    def run(self):
        start = time.monotonic()
        try:
            self.result = self.fn()
        except Exception as e:
            self.error = e
        self.elapsed = time.monotonic() - start

    def outcome(self):
        self.join()
        log.debug(f'{self.name} took {self.elapsed:.1f}s')
        if self.error:
            raise self.error
        return self.result

    def check_held(self, point, delay):
        if self.elapsed < delay:
            raise Inconclusive(f'{self.name} took {self.elapsed:.1f}s, so {point} '
                               'did not hold it: does this radosgw have that point?')


@contextmanager
def inject_delay(point, delay):
    """hold every request that reaches the named point for delay seconds"""
    try:
        exec_cmd(f'ceph config set client rgw_inject_delay_sec {delay}')
        exec_cmd(f'ceph config set client rgw_inject_delay_pattern {point}')
        time.sleep(2)  # let the radosgw daemons see the change
        yield
    finally:
        exec_cmd('ceph config rm client rgw_inject_delay_pattern')
        exec_cmd('ceph config rm client rgw_inject_delay_sec')
        time.sleep(2)


def data_pool():
    zone = json.loads(exec_cmd('radosgw-admin zone get'))
    for placement in zone['placement_pools']:
        if placement['key'] == 'default-placement':
            return placement['val']['storage_classes']['STANDARD']['data_pool']
    raise Exception('the zone has no default-placement')


def rados_objects(marker):
    out = exec_cmd(f'rados -p {data_pool()} ls')
    return sorted(n for n in out.decode().split() if n.startswith(marker + '_'))


def new_bucket(client, case):
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    name = f'race-{case}-{suffix}'
    client.create_bucket(Bucket=name)
    stats = json.loads(exec_cmd(f'radosgw-admin bucket stats --bucket {name}'))
    return name, stats['marker']


def body(fill):
    return fill.encode() * OBJ_SIZE


def put(client, bucket, key, data):
    return client.put_object(Bucket=bucket, Key=key, Body=data)['ETag']


def etag(client, bucket, key):
    try:
        return client.head_object(Bucket=bucket, Key=key)['ETag']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
            return None
        raise


def check_readable(reader, bucket, key, data):
    # nothing holds a read, so a stalled GET means radosgw sent the head's
    # bytes and then failed on a missing tail object
    try:
        got = reader.get_object(Bucket=bucket, Key=key)['Body'].read()
    except botocore.exceptions.ClientError as e:
        raise AssertionError(f'GET {key} failed: {e.response["Error"]}')
    except botocore.exceptions.BotoCoreError as e:
        raise AssertionError(f'GET {key} stopped mid-body, a tail object is missing: {e}')
    assert got == data, f'GET {key} returned {len(got)} bytes that differ from the write'


def check_no_leaks(client, bucket, marker):
    """delete every key, drain GC, and expect no RADOS object of the bucket"""
    uploads = client.list_multipart_uploads(Bucket=bucket).get('Uploads', [])
    for upload in uploads:
        client.abort_multipart_upload(Bucket=bucket, Key=upload['Key'],
                                      UploadId=upload['UploadId'])
    for obj in client.list_objects_v2(Bucket=bucket).get('Contents', []):
        client.delete_object(Bucket=bucket, Key=obj['Key'])
    exec_cmd('radosgw-admin gc process --include-all')
    left = rados_objects(marker)
    client.delete_bucket(Bucket=bucket)
    assert not left, f'{len(left)} RADOS objects outlived every key and GC: {left}'


def dedup_counter(stats, name):
    """a dedup stats counter, wherever its section nests it"""
    if isinstance(stats, dict):
        for key, value in stats.items():
            if key == name:
                return value
            found = dedup_counter(value, name)
            if found is not None:
                return found
    return None


def run_dedup(timeout):
    """run a full dedup pass and wait for it; returns its stats and duration"""
    exec_cmd('radosgw-admin dedup exec --yes-i-really-mean-it')
    start = time.monotonic()
    while True:
        time.sleep(3)
        stats = json.loads(exec_cmd('radosgw-admin dedup stats'))
        elapsed = time.monotonic() - start
        if stats.get('completed'):
            log.debug(f'dedup completed in {elapsed:.0f}s')
            return stats, elapsed
        if elapsed > timeout:
            raise Inconclusive(f'dedup did not complete within {timeout}s')


def shared_source(client, bucket, data):
    """
    two copies of data, deduped, so that a later copy is always the target:
    dedup never replaces a source whose manifest is already shared
    """
    put(client, bucket, 'src1', data)
    put(client, bucket, 'src2', data)
    stats, elapsed = run_dedup(timeout=600)
    if not dedup_counter(stats, 'Deduped Obj (this cycle)'):
        raise Inconclusive('dedup did not share the two copies')
    # generous, so the next pass finishes while a request is held
    return max(30, int(4 * elapsed) + 10)


def test_losing_complete(client, reader):
    # TESTCASE 'losing CompleteMultipartUpload','multipart','complete','leaks its parts'
    """
    A PUT and a CompleteMultipartUpload of an existing key both wait at the
    head write. The PUT arrives first and wins; the completion's ID-tag
    guard fails, it is answered as success, and nothing frees its parts.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'complete')
    key = 'obj'
    put(client, bucket, key, body('a'))
    upload = client.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
    parts = []
    for num, fill in ((1, 'p'), (2, 'q')):
        res = client.upload_part(Bucket=bucket, Key=key, UploadId=upload,
                                 PartNumber=num, Body=fill.encode() * (5 * MB))
        parts.append({'PartNumber': num, 'ETag': res['ETag']})

    with inject_delay(point, DELAY):
        writer = Request('put', lambda: put(client, bucket, key, body('b')))
        writer.start()
        time.sleep(DELAY / 3)
        complete = Request('complete', lambda: client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={'Parts': parts}))
        complete.start()
        put_etag = writer.outcome()
        complete.outcome()
    writer.check_held(point, DELAY)
    if etag(client, bucket, key) != put_etag:
        raise Inconclusive('the completion won the head write')

    check_no_leaks(client, bucket, marker)


def test_delete_racing_put(client, reader):
    # TESTCASE 'DeleteObject racing PutObject','object','delete','leaks the new tail'
    """
    A DeleteObject reads the head, then waits. A PUT replaces the object.
    The delete removes the new head without an ID-tag guard and sends the
    old manifest to GC, so the new tail is never freed.
    """
    point = 'delete_obj_before_head_delete'
    bucket, marker = new_bucket(client, 'delete')
    key = 'obj'
    put(client, bucket, key, body('a'))

    with inject_delay(point, DELAY):
        delete = Request('delete', lambda: client.delete_object(Bucket=bucket, Key=key))
        delete.start()
        time.sleep(DELAY / 3)
        put(client, bucket, key, body('b'))
        delete.outcome()
    delete.check_held(point, DELAY)
    log.debug(f'after the race the key is {"present" if etag(client, bucket, key) else "gone"}')

    check_no_leaks(client, bucket, marker)


def test_losing_copy(client, reader):
    # TESTCASE 'losing CopyObject','object','copy','leaks the source tail references'
    """
    A PUT and a CopyObject to an existing key both wait at the head write.
    The PUT arrives first and wins. The copy had already taken references
    on the source's tail; answered as success, it never drops them, so the
    source's tail outlives the source.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'copy')
    put(client, bucket, 'src', body('s'))
    put(client, bucket, 'dst', body('a'))

    with inject_delay(point, DELAY):
        writer = Request('put', lambda: put(client, bucket, 'dst', body('b')))
        writer.start()
        time.sleep(DELAY / 3)
        copy = Request('copy', lambda: client.copy_object(
            Bucket=bucket, Key='dst', CopySource={'Bucket': bucket, 'Key': 'src'}))
        copy.start()
        put_etag = writer.outcome()
        copy.outcome()
    writer.check_held(point, DELAY)
    if etag(client, bucket, 'dst') != put_etag:
        raise Inconclusive('the copy won the head write')

    check_no_leaks(client, bucket, marker)


def test_delete_racing_dedup(client, reader):
    # TESTCASE 'DeleteObject racing dedup','dedup','delete','leaks the source tail'
    """
    A DeleteObject reads the target's head, then waits while dedup points
    the target at the source's tail and frees the target's own. The delete
    sends the old manifest to GC, so the reference dedup took on the
    source's tail for the target is never dropped.
    """
    point = 'delete_obj_before_head_delete'
    bucket, marker = new_bucket(client, 'dedupdel')
    data = body('d')
    hold = shared_source(client, bucket, data)
    put(client, bucket, 'tgt', data)

    with inject_delay(point, hold):
        delete = Request('delete', lambda: client.delete_object(Bucket=bucket, Key='tgt'))
        delete.start()
        time.sleep(2)
        stats, _ = run_dedup(timeout=hold - 10)
        if not delete.is_alive():
            raise Inconclusive('the delete finished before dedup did')
        delete.outcome()
    delete.check_held(point, hold)
    if not dedup_counter(stats, 'Deduped Obj (this cycle)'):
        raise Inconclusive('dedup did not dedup the target')

    check_no_leaks(client, bucket, marker)


def test_copy_to_itself_racing_dedup(client, reader):
    # TESTCASE 'CopyObject to itself racing dedup','dedup','copy','loses the tail'
    """
    A copy of the target onto itself reads its manifest, then waits while
    dedup points the target at the source's tail and frees the target's
    own. The copy keeps its tail and writes the old manifest back, over
    tail objects that no longer exist.
    """
    point = 'copy_obj_before_write_meta'
    bucket, marker = new_bucket(client, 'dedupcopy')
    data = body('e')
    hold = shared_source(client, bucket, data)
    put(client, bucket, 'tgt', data)

    with inject_delay(point, hold):
        copy = Request('copy', lambda: client.copy_object(
            Bucket=bucket, Key='tgt', CopySource={'Bucket': bucket, 'Key': 'tgt'},
            MetadataDirective='REPLACE', Metadata={'copied': 'yes'}))
        copy.start()
        time.sleep(2)
        stats, _ = run_dedup(timeout=hold - 10)
        if not copy.is_alive():
            raise Inconclusive('the copy finished before dedup did')
        copy.outcome()
    copy.check_held(point, hold)
    if not dedup_counter(stats, 'Deduped Obj (this cycle)'):
        raise Inconclusive('dedup did not dedup the target')

    check_readable(reader, bucket, 'tgt', data)
    check_no_leaks(client, bucket, marker)


CASES = [
    test_losing_complete,
    test_delete_racing_put,
    test_losing_copy,
    test_delete_racing_dedup,
    test_copy_to_itself_racing_dedup,
]


def main():
    """
    run the named cases, or all of them; exit nonzero unless every case passed
    """
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    # a held request can take several minutes to answer; never retry one
    client = boto_connect(ACCESS_KEY, SECRET_KEY, Config(
        read_timeout=900, retries={'total_max_attempts': 1})).meta.client
    reader = boto_connect(ACCESS_KEY, SECRET_KEY, Config(
        read_timeout=15, retries={'total_max_attempts': 1})).meta.client

    names = sys.argv[1:]
    cases = [c for c in CASES if not names or c.__name__ in names]
    results = []
    for case in cases:
        log.info(f'TEST: {case.__name__}')
        try:
            case(client, reader)
            results.append((case.__name__, 'PASS', ''))
        except Inconclusive as e:
            results.append((case.__name__, 'INCONCLUSIVE', str(e)))
        except AssertionError as e:
            results.append((case.__name__, 'FAIL', str(e)))
    for name, verdict, reason in results:
        log.info(f'{verdict:12} {name} {reason}')
    if any(verdict != 'PASS' for _, verdict, _ in results):
        sys.exit(1)


main()
log.info("Completed overwrite race tests")
