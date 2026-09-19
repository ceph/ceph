#!/usr/bin/env python3

import logging as log
import json
import uuid
import botocore
import time
import threading
from common import exec_cmd, create_user, boto_connect
from botocore.config import Config

"""
Tests behavior of bucket versioning.
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

""" Constants """
USER = 'versioning-tester'
DISPLAY_NAME = 'Versioning Testing'
ACCESS_KEY = 'LTA662PVVDTDWX6M2AB0'
SECRET_KEY = 'pvtchqajgzqx5581t6qbddbkj0bgf3a69qdkjcea'
BUCKET_NAME = 'versioning-bucket'
DATA_POOL = 'default.rgw.buckets.data'

def main():
    """
    execute versioning tests
    """
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries = {
        'total_max_attempts': 1,
    }))

    # pre-test cleanup
    try:
        bucket = connection.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.object_versions.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == 'NoSuchBucket':
            raise

    bucket = connection.create_bucket(Bucket=BUCKET_NAME)
    connection.BucketVersioning(BUCKET_NAME).enable()

    # reproducer for bug from https://tracker.ceph.com/issues/59663
    # TESTCASE 'verify that index entries and OLH objects are cleaned up after redundant deletes'
    log.debug('TEST: verify that index entries and OLH objects are cleaned up after redundant deletes\n')
    key = str(uuid.uuid4())
    resp = bucket.Object(key).delete()
    assert 'DeleteMarker' in resp, 'DeleteMarker key not present in response'
    assert resp['DeleteMarker'], 'DeleteMarker value not True in response'
    assert 'VersionId' in resp, 'VersionId key not present in response'
    version_id_1 = resp['VersionId']
    log.info(f"first delete marker has version id {version_id_1}")

    resp = bucket.Object(key).delete()
    assert 'DeleteMarker' in resp, 'DeleteMarker key not present in response'
    assert resp['DeleteMarker'], 'DeleteMarker value not True in response'
    assert 'VersionId' in resp, 'VersionId key not present in response'
    version_id_2 = resp['VersionId']
    log.info(f"second delete marker has version id {version_id_2}")

    connection.ObjectVersion(bucket.name, key, version_id_2).delete()
    # bucket index should only contain entries for one version of one object
    out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME}')
    json_out = json.loads(out.replace(b'\x80', b'0x80'))
    log.info(f"json_out has length of { len(json_out) }")
    idx_list = list(map(lambda v: v['idx'], json_out))
    log.info(f"json_out has idx's of { idx_list }")
    assert len(json_out) == 4, "bucket index did not only contain entries for one version of one object; expecting two plain, one instance, and one OLH entry"

    connection.ObjectVersion(bucket.name, key, version_id_1).delete()
    # bucket index should now be empty
    out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME}')
    json_out = json.loads(out.replace(b'\x80', b'0x80'))
    assert len(json_out) == 0, 'bucket index was not empty after all objects were deleted'

    (_out, ret) = exec_cmd(f'rados -p {DATA_POOL} ls | grep {key}', check_retcode=False)
    assert ret != 0, 'olh object was not cleaned up'

    # TESTCASE 'verify that index entries and OLH objects are cleaned up after index linking error'
    log.debug('TEST: verify that index entries and OLH objects are cleaned up after index linking error\n')
    key = str(uuid.uuid4())
    try:
        exec_cmd('ceph config set client rgw_debug_inject_set_olh_err 2')
        time.sleep(1)
        bucket.Object(key).delete()
    finally:
        exec_cmd('ceph config rm client rgw_debug_inject_set_olh_err')
    out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME}')
    json_out = json.loads(out.replace(b'\x80', b'0x80'))
    assert len(json_out) == 0, 'bucket index was not empty after op failed'
    (_out, ret) = exec_cmd(f'rados -p {DATA_POOL} ls | grep {key}', check_retcode=False)
    assert ret != 0, 'olh object was not cleaned up'

    # TESTCASE 'verify that original null object version is intact after failed olh upgrade'
    log.debug('TEST: verify that original null object version is intact after failed olh upgrade\n')
    connection.BucketVersioning(BUCKET_NAME).suspend()
    key = str(uuid.uuid4())
    put_resp = bucket.put_object(Key=key, Body=b"data")
    connection.BucketVersioning(BUCKET_NAME).enable()
    try:
        exec_cmd('ceph config set client rgw_debug_inject_set_olh_err 2')
        time.sleep(1)
        # expected to fail due to the above error injection
        bucket.put_object(Key=key, Body=b"new data")
    except Exception as e:
        log.debug(e)
    finally:
        exec_cmd('ceph config rm client rgw_debug_inject_set_olh_err')
    get_resp = bucket.Object(key).get()
    assert put_resp.e_tag == get_resp['ETag'], 'get did not return null version with correct etag'

    # TESTCASE 'verify that concurrent delete requests do not leave behind olh entries'
    log.debug('TEST: verify that concurrent delete requests do not leave behind olh entries\n')
    bucket.object_versions.all().delete()
    
    key = 'concurrent-delete'
    # create a delete marker
    resp = bucket.Object(key).delete()
    version_id = resp['ResponseMetadata']['HTTPHeaders']['x-amz-version-id']
    try:
        exec_cmd('ceph config set client rgw_debug_inject_latency_bi_unlink 2')
        time.sleep(1)

        def do_delete():
            connection.ObjectVersion(bucket.name, key, version_id).delete()
            
        t2 = threading.Thread(target=do_delete)
        t2.start()
        do_delete()
        t2.join()
    finally:
        exec_cmd('ceph config rm client rgw_debug_inject_latency_bi_unlink')
    out = exec_cmd(f'radosgw-admin bucket check olh --bucket {bucket.name} --dump-keys')
    num_leftover_olh_entries = len(json.loads(out))
    assert num_leftover_olh_entries == 0, \
      'Found leftover olh entries after concurrent deletes'

    # TESTCASE 'verify that reads recover after a canceled olh replay'
    log.debug('TEST: verify that reads recover after a canceled olh replay\n')
    key = 'olh-replay-race'
    current_version = bucket.Object(key).put(Body=b'b')['VersionId']
    marker = json.loads(exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}'))['marker']
    olh_oid = f'{marker}_{key}'
    olh_ver = exec_cmd(f'rados -p {DATA_POOL} getxattr {olh_oid} user.rgw.olh.ver').decode()
    try:
        # force the replay's version check to fail
        exec_cmd(f'rados -p {DATA_POOL} setxattr {olh_oid} user.rgw.olh.ver 9223372036854775807')
        connection.ObjectVersion(bucket.name, key, current_version).delete()
        assert 'Errors' not in bucket.delete_objects(Delete={'Objects': [{'Key': key}]})
        new_version = bucket.Object(key).put(Body=b'c')['VersionId']
        bucket.Object(key).get()
        assert False, 'expected 503 while the olh replay is canceled'
    except botocore.exceptions.ClientError as e:
        assert e.operation_name == 'GetObject'
        assert e.response['ResponseMetadata']['HTTPStatusCode'] == 503
    finally:
        exec_cmd(f'rados -p {DATA_POOL} setxattr {olh_oid} user.rgw.olh.ver {olh_ver}')
    assert bucket.Object(key).get()['VersionId'] == new_version

    # TESTCASE 'verify that a delete writes a delete marker after its olh replay removes the head'
    log.debug('TEST: verify that a delete writes a delete marker after its olh replay removes the head\n')
    key = 'olh-replay-remove'
    version = bucket.Object(key).put(Body=b'a')['VersionId']
    olh_oid = f'{marker}_{key}'
    olh_ver = exec_cmd(f'rados -p {DATA_POOL} getxattr {olh_oid} user.rgw.olh.ver').decode()
    try:
        # leave the unlink pending in the olh log
        exec_cmd(f'rados -p {DATA_POOL} setxattr {olh_oid} user.rgw.olh.ver 9223372036854775807')
        connection.ObjectVersion(bucket.name, key, version).delete()
    finally:
        exec_cmd(f'rados -p {DATA_POOL} setxattr {olh_oid} user.rgw.olh.ver {olh_ver}')
    assert bucket.Object(key).delete().get('DeleteMarker'), 'expected a delete marker'

    # Clean up
    log.debug("Deleting bucket {}".format(BUCKET_NAME))
    bucket.object_versions.all().delete()
    bucket.delete()

main()
log.info("Completed bucket versioning tests")
