#!/usr/bin/env python3

import json
import logging as log
import random
import string
import sys
import threading
import time
from contextlib import contextmanager
from functools import partial, update_wrapper

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

Most cases also run in a bucket whose versioning is suspended, over a key
that has an olh: its null version shares the key's head object with the
olh. Other cases run in a bucket whose versioning is enabled, where each
write has a head of its own and the races are at the olh link.
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
def config(**opts):
    """
    set options for the duration, globally: the OSDs read some of them in
    cls_rgw, as the pending-op expiry
    """
    try:
        for name, value in opts.items():
            exec_cmd(f'ceph config set global {name} {value}')
        time.sleep(2)  # let the daemons see the change
        yield
    finally:
        for name in opts:
            exec_cmd(f'ceph config rm global {name}')
        time.sleep(2)


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


def set_versioning(client, bucket, status):
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={'Status': status})


def new_bucket(client, case, versioning=None, keys=()):
    """
    a bucket for a case. With versioning 'Suspended', each of keys gets an
    olh first: a version, then a delete marker, written while versioning
    was enabled. The key then has no current version, and its null
    version will share its head object with the olh.
    """
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    name = f'race-{case}-{suffix}'
    client.create_bucket(Bucket=name)
    if versioning:
        set_versioning(client, name, 'Enabled')
    if versioning == 'Suspended':
        for key in keys:
            client.put_object(Bucket=name, Key=key, Body=b'v')
            client.delete_object(Bucket=name, Key=key)
        set_versioning(client, name, 'Suspended')
    stats = json.loads(exec_cmd(f'radosgw-admin bucket stats --bucket {name}'))
    return name, stats['marker']


def delete_point(versioning):
    """where a DeleteObject without a version id waits, having read the key"""
    # in a versioned bucket it links a delete marker instead of removing
    # the head
    return 'delete_obj_before_olh_link' if versioning else 'delete_obj_before_head_delete'


def body(fill):
    return fill.encode() * OBJ_SIZE


def put(client, bucket, key, data):
    return client.put_object(Bucket=bucket, Key=key, Body=data)['ETag']


def etag(client, bucket, key):
    try:
        # a head that lost its object's attributes answers without one
        return client.head_object(Bucket=bucket, Key=key).get('ETag')
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
    # every version and delete marker; in a bucket that is not versioned
    # each object is listed once, as version null
    for page in client.get_paginator('list_object_versions').paginate(Bucket=bucket):
        for v in page.get('Versions', []) + page.get('DeleteMarkers', []):
            client.delete_object(Bucket=bucket, Key=v['Key'], VersionId=v['VersionId'])
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


def test_losing_complete(client, reader, versioning=None):
    # TESTCASE 'losing CompleteMultipartUpload','multipart','complete','leaks its parts'
    """
    A PUT and a CompleteMultipartUpload of an existing key both wait at the
    head write. The PUT arrives first and wins; the completion's ID-tag
    guard fails, it is answered as success, and nothing frees its parts.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'complete', versioning, ('obj',))
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


def test_delete_racing_put(client, reader, versioning=None):
    # TESTCASE 'DeleteObject racing PutObject','object','delete','leaks the new tail'
    """
    A DeleteObject reads the head, then waits. A PUT replaces the object.
    The delete removes the new head without an ID-tag guard and sends the
    old manifest to GC, so the new tail is never freed.
    """
    point = delete_point(versioning)
    bucket, marker = new_bucket(client, 'delete', versioning, ('obj',))
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


def test_losing_copy(client, reader, versioning=None):
    # TESTCASE 'losing CopyObject','object','copy','leaks the source tail references'
    """
    A PUT and a CopyObject to an existing key both wait at the head write.
    The PUT arrives first and wins. The copy had already taken references
    on the source's tail; answered as success, it never drops them, so the
    source's tail outlives the source.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'copy', versioning, ('dst',))
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


def status(request):
    """the HTTP status a finished Request was answered with"""
    if request.error is None:
        return 200
    if isinstance(request.error, botocore.exceptions.ClientError):
        return request.error.response['ResponseMetadata']['HTTPStatusCode']
    raise request.error


def part_entries(bucket, upload):
    """the bucket index entries of an upload's parts"""
    out = exec_cmd(f'radosgw-admin bi list --bucket {bucket}')
    entries = json.loads(out.replace(b'\x80', b'0x80'))
    return sorted(e['entry']['name'] for e in entries
                  if e.get('type') == 'plain' and upload in e['entry'].get('name', '')
                  and not e['entry']['name'].endswith('.meta'))


def test_cond_delete_racing_put(client, reader, versioning=None):
    # TESTCASE 'conditional DeleteObject racing PutObject','object','delete','deletes an object that fails its If-Match'
    """
    A DeleteObject with If-Match on the object's ETag checks it against the
    head it read, then waits. A PUT replaces the object. The delete must not
    remove the new object, whose ETag does not match.
    """
    point = delete_point(versioning)
    bucket, marker = new_bucket(client, 'conddel', versioning, ('obj',))
    key = 'obj'
    old = put(client, bucket, key, body('a'))

    with inject_delay(point, DELAY):
        delete = Request('delete', lambda: client.delete_object(Bucket=bucket, Key=key, IfMatch=old))
        delete.start()
        time.sleep(DELAY / 3)
        new = put(client, bucket, key, body('b'))
        delete.join()
    delete.check_held(point, DELAY)
    log.debug(f'the conditional delete was answered {status(delete)}')
    assert etag(client, bucket, key) == new, \
        'the If-Match delete removed an object whose ETag does not match'

    check_no_leaks(client, bucket, marker)


def test_conditional_puts_race(client, reader, versioning=None):
    # TESTCASE 'two conditional PutObjects','object','put','both answered success'
    """
    A PutObject with If-Match on the object's ETag and one with If-Match: *
    both wait at the head write, and the first wins. If both are answered
    success, the second must have come after the first, so the head must
    hold the second's object; with the first's there, no order of the two
    explains both answers.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'condputs', versioning, ('obj',))
    key = 'obj'
    old = put(client, bucket, key, body('a'))

    with inject_delay(point, DELAY):
        first = Request('if-match', lambda: client.put_object(
            Bucket=bucket, Key=key, Body=body('b'), IfMatch=old)['ETag'])
        first.start()
        time.sleep(DELAY / 3)
        second = Request('if-match-any', lambda: client.put_object(
            Bucket=bucket, Key=key, Body=body('c'), IfMatch='*')['ETag'])
        second.start()
        first.join()
        second.join()
    first.check_held(point, DELAY)
    head = etag(client, bucket, key)
    if status(first) != 200 or head != first.result:
        raise Inconclusive('the If-Match PUT did not win the race')
    assert status(second) != 200, \
        'both conditional PUTs were answered success, and the head holds the first'

    check_no_leaks(client, bucket, marker)


def test_if_match_losing_race(client, reader, versioning=None):
    # TESTCASE 'PutObject with If-Match losing a race','object','put','answered 500'
    """
    A PutObject, then a PutObject with If-Match on the object's ETag, both
    wait at the head write. The unconditional one waits twice, for its
    exclusive create and then its guarded write, so it reads the head
    before the conditional one and writes before it. The conditional one
    loses its guard: it must be answered 412, not 500.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'ifmatch', versioning, ('obj',))
    key = 'obj'
    old = put(client, bucket, key, body('a'))

    with inject_delay(point, DELAY):
        writer = Request('put', lambda: put(client, bucket, key, body('b')))
        writer.start()
        time.sleep(DELAY * 1.3)
        cond = Request('if-match', lambda: client.put_object(
            Bucket=bucket, Key=key, Body=body('c'), IfMatch=old)['ETag'])
        cond.start()
        writer.join()
        cond.join()
    writer.check_held(point, DELAY)
    if etag(client, bucket, key) != writer.result:
        raise Inconclusive('the If-Match PUT did not lose the race')
    assert status(cond) != 500, 'the If-Match PUT that lost the race was answered 500'

    check_no_leaks(client, bucket, marker)


def test_refused_complete_keeps_parts(client, reader, versioning=None):
    # TESTCASE 'CompleteMultipartUpload with If-None-Match refused','multipart','complete','drops its parts index entries'
    """
    A PutObject and a completion with If-None-Match: * both create a new
    key and wait at the head write; the PutObject wins. The completion is
    refused with 412 and its upload stays, so its parts must stay in the
    bucket index.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'refused', versioning, ('obj',))
    key = 'obj'
    upload = client.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
    parts = []
    for num, fill in ((1, 'p'), (2, 'q')):
        res = client.upload_part(Bucket=bucket, Key=key, UploadId=upload,
                                 PartNumber=num, Body=fill.encode() * (5 * MB))
        parts.append({'PartNumber': num, 'ETag': res['ETag']})
    before = part_entries(bucket, upload)

    with inject_delay(point, DELAY):
        writer = Request('put', lambda: put(client, bucket, key, body('b')))
        writer.start()
        # over a key with an olh, the PutObject's exclusive create fails and
        # it waits a second time, for its guarded write: start the
        # completion after it has read the head again
        time.sleep(DELAY * 1.3 if versioning else DELAY / 3)
        complete = Request('complete', lambda: client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={'Parts': parts},
            IfNoneMatch='*'))
        complete.start()
        writer.join()
        complete.join()
    writer.check_held(point, DELAY)
    if status(complete) != 412:
        raise Inconclusive(f'the completion was answered {status(complete)}, not refused')
    after = part_entries(bucket, upload)
    assert after == before, \
        f'the refused completion dropped its parts from the index ({len(before)} -> {len(after)}), while its upload stays'

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


def test_versioned_if_match_racing_put(client, reader):
    # TESTCASE 'versioned PutObject with If-Match racing PutObject','versioning','put','links over a version it did not check'
    """
    In a bucket with versioning enabled, a PutObject with If-Match on the
    current version's ETag writes its version, then waits before linking
    it. A PutObject links a newer version meanwhile. The conditional one
    must not become current over a version it did not check.
    """
    point = 'write_meta_before_olh_link'
    bucket, marker = new_bucket(client, 'vifmatch', 'Enabled')
    key = 'obj'
    old = put(client, bucket, key, body('a'))

    with inject_delay(point, DELAY):
        cond = Request('if-match', lambda: client.put_object(
            Bucket=bucket, Key=key, Body=body('b'), IfMatch=old)['ETag'])
        cond.start()
        time.sleep(DELAY / 3)
        # this PUT waits at the same point, and links after the first
        writer = Request('put', lambda: put(client, bucket, key, body('c')))
        writer.start()
        cond.join()
        writer.join()
    cond.check_held(point, DELAY)
    if status(writer) != 200:
        raise Inconclusive(f'the PutObject was answered {status(writer)}')
    if status(cond) == 200:
        assert etag(client, bucket, key) == writer.result, \
            'the If-Match PUT linked its version over one it did not check'
    else:
        # its condition held when checked: S3 answers a conflicting
        # operation during a conditional write 409 ConditionalRequestConflict
        assert status(cond) == 409, f'the If-Match PUT that lost the race was answered {status(cond)}, not 409'
    check_versions_readable(client, reader, bucket, key)
    check_no_leaks(client, bucket, marker)


def test_versioned_cond_delete_racing_put(client, reader):
    # TESTCASE 'versioned DeleteObject with If-Match racing PutObject','versioning','delete','hides a version that fails its If-Match'
    """
    In a bucket with versioning enabled, a DeleteObject with If-Match on
    the current version's ETag checks it, then waits before linking its
    delete marker. A PutObject links a newer version meanwhile. The delete
    marker must not hide the new version, whose ETag does not match.
    """
    point = 'delete_obj_before_olh_link'
    bucket, marker = new_bucket(client, 'vconddel', 'Enabled')
    key = 'obj'
    old = put(client, bucket, key, body('a'))

    with inject_delay(point, DELAY):
        delete = Request('delete', lambda: client.delete_object(Bucket=bucket, Key=key, IfMatch=old))
        delete.start()
        time.sleep(DELAY / 3)
        new = put(client, bucket, key, body('b'))
        delete.join()
    delete.check_held(point, DELAY)
    log.debug(f'the conditional delete was answered {status(delete)}')
    assert etag(client, bucket, key) == new, \
        'the If-Match delete marker hid a version whose ETag does not match'
    # the condition held when checked; a concurrent PUT won: S3 answers 409
    assert status(delete) == 409, f'the refused conditional delete was answered {status(delete)}, not 409'

    check_no_leaks(client, bucket, marker)


def test_suspended_if_none_match_puts_race(client, reader):
    # TESTCASE 'two PutObjects with If-None-Match racing, versioning suspended','versioning','put','the refused one replaces the head of the one answered success'
    """
    Versioning is suspended, and the key's current version is a delete
    marker. Two PutObjects with If-None-Match: * both pass their check and
    wait at the head write; they share the key's head object. The one that
    links first is answered success, and the key must read back as its
    object: the other must not have replaced its head.
    """
    point = 'write_meta_before_head_write'
    bucket, marker = new_bucket(client, 'nullinm', 'Suspended', ('obj',))
    key = 'obj'
    bodies = {'first': body('b'), 'second': body('c')}

    with inject_delay(point, DELAY):
        first = Request('first', lambda: client.put_object(
            Bucket=bucket, Key=key, Body=bodies['first'], IfNoneMatch='*')['ETag'])
        first.start()
        time.sleep(DELAY / 3)
        second = Request('second', lambda: client.put_object(
            Bucket=bucket, Key=key, Body=bodies['second'], IfNoneMatch='*')['ETag'])
        second.start()
        first.join()
        second.join()
    first.check_held(point, DELAY)
    winners = [r for r in (first, second) if status(r) == 200]
    log.debug(f'answered {status(first)} and {status(second)}')
    assert len(winners) == 1, \
        f'{len(winners)} of the two If-None-Match PUTs were answered success'
    loser = second if winners[0] is first else first
    # both passed their check before either wrote: the loser met a
    # conflicting operation, which S3 answers 409
    assert status(loser) == 409, f'the If-None-Match PUT that lost was answered {status(loser)}, not 409'
    check_readable(reader, bucket, key, bodies[winners[0].name])

    check_no_leaks(client, bucket, marker)


def test_suspended_delete_null_racing_put(client, reader):
    # TESTCASE 'DeleteObject of the null version racing PutObject, versioning suspended','versioning','delete','removes the head of the new null version'
    """
    Versioning is suspended. A PutObject of a new null version reads the
    key, then waits before its head write. A DeleteObject of the null
    version unlinks it meanwhile, which logs the removal of the null
    version's head, and waits before it applies the log. The PutObject then
    writes the key's head object and links its version after that removal.
    Whoever applies the log must not remove the new version's head: it is
    listed, and must read back whole.
    """
    points = 'write_meta_before_head_write,unlink_instance_before_olh_update'
    bucket, marker = new_bucket(client, 'nulldel', 'Suspended', ('obj',))
    key = 'obj'
    put(client, bucket, key, body('a'))

    with inject_delay(points, DELAY):
        # over a key with an olh, the PutObject's exclusive create fails, and
        # it reads the key again and waits a second time
        new = Request('put', lambda: put(client, bucket, key, body('b')))
        new.start()
        time.sleep(DELAY * 1.3)
        delete = Request('delete', lambda: client.delete_object(
            Bucket=bucket, Key=key, VersionId='null'))
        delete.start()
        new.join()
        delete.join()
    new.check_held(points, 2 * DELAY)
    delete.check_held(points, DELAY)
    if status(new) != 200:
        raise Inconclusive(f'the PutObject was answered {status(new)}')
    assert etag(client, bucket, key) == new.result, \
        'the PutObject linked last, but is not the current version'
    check_readable(reader, bucket, key, body('b'))

    check_no_leaks(client, bucket, marker)

def copy_to_itself_racing_attrs_case(client, reader, update):
    """
    A copy of an object onto itself with new metadata reads the head, then
    waits. A tagging or ACL update meanwhile gives the head a new ID tag,
    and keeps the object. If the copy is answered success, the object must
    carry its metadata, and the update's tag set: whichever came first, the
    other keeps it.
    """
    point = 'copy_obj_before_write_meta'
    bucket, marker = new_bucket(client, f'copyself{update}')
    key = 'obj'
    data = body('a')
    put(client, bucket, key, data)

    with inject_delay(point, DELAY):
        copy = Request('copy', lambda: client.copy_object(
            Bucket=bucket, Key=key, CopySource={'Bucket': bucket, 'Key': key},
            MetadataDirective='REPLACE', Metadata={'copied': 'yes'}))
        copy.start()
        time.sleep(DELAY / 3)
        if update == 'tagging':
            client.put_object_tagging(Bucket=bucket, Key=key,
                                      Tagging={'TagSet': [{'Key': 'k', 'Value': 'v'}]})
        else:
            client.put_object_acl(Bucket=bucket, Key=key, ACL='public-read')
        copy.join()
    copy.check_held(point, DELAY)
    log.debug(f'the copy was answered {status(copy)}')
    if status(copy) == 200:
        meta = client.head_object(Bucket=bucket, Key=key).get('Metadata', {})
        assert meta.get('copied') == 'yes', \
            f'the copy onto itself was answered success, but the object has metadata {meta}'
    if update == 'tagging':
        tags = client.get_object_tagging(Bucket=bucket, Key=key)['TagSet']
        assert tags == [{'Key': 'k', 'Value': 'v'}], \
            f'the tagging update was answered success, but the object has tags {tags}'
    check_readable(reader, bucket, key, data)
    check_no_leaks(client, bucket, marker)


def test_copy_to_itself_racing_tagging(client, reader):
    # TESTCASE 'CopyObject to itself racing PutObjectTagging','copy','copy','answered success without its metadata'
    copy_to_itself_racing_attrs_case(client, reader, 'tagging')


def test_copy_to_itself_racing_acl(client, reader):
    # TESTCASE 'CopyObject to itself racing PutObjectAcl','copy','copy','answered success without its metadata'
    copy_to_itself_racing_attrs_case(client, reader, 'acl')


def test_suspended_copy_null_to_itself(client, reader):
    # TESTCASE 'CopyObject of the null version onto itself, versioning suspended','versioning','copy','answered success without its metadata'
    """
    Versioning is suspended, and the key's current version is its null
    version, whose head is the olh's head object. A copy of the key onto
    itself with new metadata must apply it.
    """
    bucket, marker = new_bucket(client, 'nullcopyself', 'Suspended', ('obj',))
    key = 'obj'
    data = body('a')
    put(client, bucket, key, data)
    client.copy_object(Bucket=bucket, Key=key, CopySource={'Bucket': bucket, 'Key': key},
                       MetadataDirective='REPLACE', Metadata={'copied': 'yes'})
    meta = client.head_object(Bucket=bucket, Key=key).get('Metadata', {})
    assert meta.get('copied') == 'yes', \
        f'the copy onto itself was answered success, but the object has metadata {meta}'
    check_readable(reader, bucket, key, data)
    check_no_leaks(client, bucket, marker)


def check_versions_readable(client, reader, bucket, key):
    """every version of the key reads back whole"""
    for page in client.get_paginator('list_object_versions').paginate(Bucket=bucket, Prefix=key):
        for v in page.get('Versions', []):
            if v['Key'] != key:
                continue
            try:
                got = reader.get_object(Bucket=bucket, Key=key, VersionId=v['VersionId'])['Body'].read()
            except botocore.exceptions.ClientError as e:
                raise AssertionError(f'GET {key} version {v["VersionId"]} failed: {e.response["Error"]}')
            except botocore.exceptions.BotoCoreError as e:
                raise AssertionError(f'GET {key} version {v["VersionId"]} stopped mid-body: {e}')
            assert len(got) == v['Size'], \
                f'GET {key} version {v["VersionId"]} returned {len(got)} of {v["Size"]} bytes'


def link_olh_error_case(client, reader, versioning, request):
    """
    the olh link of a write to key 'linkolh' fails after the bucket index
    has linked the version. Whatever the write is answered, what the key
    lists must read back whole once GC has run.
    """
    key = 'linkolh'
    bucket, marker = new_bucket(client, f'linkolh-{request}', versioning, (key,))
    if request == 'complete':
        upload = client.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
        parts = []
        for num, fill in ((1, 'p'), (2, 'q')):
            res = client.upload_part(Bucket=bucket, Key=key, UploadId=upload,
                                     PartNumber=num, Body=fill.encode() * (5 * MB))
            parts.append({'PartNumber': num, 'ETag': res['ETag']})
    with config(rgw_debug_inject_link_olh_log_err_key=key):
        if request == 'put':
            write = Request('put', lambda: put(client, bucket, key, body('a')))
        else:
            write = Request('complete', lambda: client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={'Parts': parts}))
        write.start()
        write.join()
    log.debug(f'the write was answered {status(write)}')
    if request == 'complete':
        # an upload that is still listed is aborted, as a client would
        # after an error
        for upload in client.list_multipart_uploads(Bucket=bucket).get('Uploads', []):
            client.abort_multipart_upload(Bucket=bucket, Key=upload['Key'],
                                          UploadId=upload['UploadId'])
    exec_cmd('radosgw-admin gc process --include-all')
    check_versions_readable(client, reader, bucket, key)
    assert status(write) == 200, \
        f'the write was answered {status(write)}, though the index links its version'
    check_no_leaks(client, bucket, marker)


def test_versioned_put_link_olh_error(client, reader):
    # TESTCASE 'versioned PutObject whose olh link fails after the index links it','versioning','put','deletes the tail of the current version'
    link_olh_error_case(client, reader, 'Enabled', 'put')


def test_suspended_put_link_olh_error(client, reader):
    # TESTCASE 'null version PutObject whose olh link fails after the index links it','versioning','put','deletes the tail of the current version'
    link_olh_error_case(client, reader, 'Suspended', 'put')


def test_versioned_complete_link_olh_error(client, reader):
    # TESTCASE 'versioned CompleteMultipartUpload whose olh link fails after the index links it','versioning','complete','leaves the upload, whose abort frees the parts'
    link_olh_error_case(client, reader, 'Enabled', 'complete')


def test_suspended_copy_to_itself(client, reader):
    # TESTCASE 'CopyObject onto itself while a version is current, versioning suspended','versioning','copy','shares the version tail without a reference'
    """
    Versioning is suspended, and the key's current version is a version
    written while it was enabled. A copy of the key onto itself writes a
    null version from the current version's manifest. Deleting that
    version afterwards must leave the null version readable.
    """
    bucket, marker = new_bucket(client, 'nullcopy', 'Enabled')
    key = 'obj'
    data = body('a')
    version = client.put_object(Bucket=bucket, Key=key, Body=data)['VersionId']
    set_versioning(client, bucket, 'Suspended')
    client.copy_object(Bucket=bucket, Key=key, CopySource={'Bucket': bucket, 'Key': key},
                       MetadataDirective='REPLACE', Metadata={'copied': 'yes'})
    client.delete_object(Bucket=bucket, Key=key, VersionId=version)
    exec_cmd('radosgw-admin gc process --include-all')
    check_readable(reader, bucket, key, data)
    check_no_leaks(client, bucket, marker)


def listed(client, bucket, key):
    """the ETags ListObjects shows for key"""
    return [o['ETag'] for o in client.list_objects_v2(Bucket=bucket, Prefix=key).get('Contents', [])
            if o['Key'] == key]


def stalled_put_case(client, reader, versioning):
    """
    A PutObject prepares its index op, then stalls past the pending-op
    expiry. A listing meanwhile drops the op and repairs the entry from
    the head, which is still the old object. Once the PUT finishes, the
    listing must show the key once, as the new object.
    """
    point = 'write_meta_before_head_write'
    key = 'obj'
    bucket, marker = new_bucket(client, 'stalled', versioning, (key,))
    put(client, bucket, key, body('a'))
    expiry = 3
    with config(rgw_pending_bucket_index_op_expiration=expiry):
        with inject_delay(point, 3 * expiry + DELAY):
            writer = Request('put', lambda: put(client, bucket, key, body('b')))
            writer.start()
            time.sleep(2 * expiry)
            listed(client, bucket, key)
            client.list_object_versions(Bucket=bucket, Prefix=key)
            writer.join()
    writer.check_held(point, 3 * expiry + DELAY)
    new = writer.outcome()
    got = listed(client, bucket, key)
    assert got == [new], f'ListObjects shows {key} as {got}, but the PUT wrote {new}'
    if versioning:
        versions = [(v['ETag'], v['IsLatest']) for v in
                    client.list_object_versions(Bucket=bucket, Prefix=key).get('Versions', [])
                    if v['Key'] == key]
        latest = [e for e, is_latest in versions if is_latest]
        assert latest == [new], f'ListObjectVersions shows {versions}, but the PUT wrote {new}'
        assert len(set(e for e, _ in versions)) == len(versions), f'a version is listed twice: {versions}'
    check_readable(reader, bucket, key, body('b'))
    check_no_leaks(client, bucket, marker)


def test_versioned_listing_during_stalled_put(client, reader):
    # TESTCASE 'listing during a stalled PutObject, versioning enabled','versioning','put','the index keeps the old object'
    stalled_put_case(client, reader, 'Enabled')


def test_suspended_listing_during_stalled_put(client, reader):
    # TESTCASE 'listing during a stalled null version PutObject, versioning suspended','versioning','put','the index keeps the old object'
    stalled_put_case(client, reader, 'Suspended')


def suspended(case):
    """the case, in a bucket with versioning suspended"""
    variant = update_wrapper(partial(case, versioning='Suspended'), case)
    variant.__name__ = f'{case.__name__}_suspended'
    return variant


CASES = [
    test_losing_complete,
    test_delete_racing_put,
    test_losing_copy,
    test_cond_delete_racing_put,
    test_conditional_puts_race,
    test_if_match_losing_race,
    test_refused_complete_keeps_parts,
    test_delete_racing_dedup,
    test_copy_to_itself_racing_dedup,
    suspended(test_losing_complete),
    suspended(test_delete_racing_put),
    suspended(test_losing_copy),
    suspended(test_cond_delete_racing_put),
    suspended(test_conditional_puts_race),
    suspended(test_if_match_losing_race),
    suspended(test_refused_complete_keeps_parts),
    test_versioned_if_match_racing_put,
    test_versioned_cond_delete_racing_put,
    test_suspended_if_none_match_puts_race,
    test_suspended_delete_null_racing_put,
    test_versioned_put_link_olh_error,
    test_suspended_put_link_olh_error,
    test_versioned_complete_link_olh_error,
    test_suspended_copy_to_itself,
    test_copy_to_itself_racing_tagging,
    test_copy_to_itself_racing_acl,
    test_suspended_copy_null_to_itself,
    test_versioned_listing_during_stalled_put,
    test_suspended_listing_during_stalled_put,
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
