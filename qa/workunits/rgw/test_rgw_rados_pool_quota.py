#!/usr/bin/env python3

import logging as log
import uuid
import botocore
import time
from common import exec_cmd, create_user, boto_connect
from botocore.config import Config

"""
Tests rgw behavior when backing rados pool is at quota
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

""" Constants """
USER = 'quota-tester'
DISPLAY_NAME = 'Quota Tester'
ACCESS_KEY = 'LTA662PVVDTDWX6M2AB0'
SECRET_KEY = 'pvtchqajgzqx5581t6qbddbkj0bgf3a69qdkjcea'
BUCKET_NAME = 'quota-tester-bucket'
DATA_POOL = 'default.rgw.buckets.data'

def main():
    """
    execute quota tests
    """
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries = {
        'total_max_attempts': 1,
    }))

    # pre-test cleanup
    try:
        exec_cmd(f"ceph osd pool set-quota {DATA_POOL} max_objects 0")
    except:
        pass

    try:
        bucket = connection.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == 'NoSuchBucket':
            raise

    bucket = connection.create_bucket(Bucket=BUCKET_NAME)

    # reproducer for bug from https://tracker.ceph.com/issues/69723
    # TESTCASE 'add objects to pool, set quota to small value, verify that we can delete objects'
    log.debug('TEST: verify that objects can be deleted with rados pool at quota')
    key = str(uuid.uuid4())

    objects = [f"{key}.{i}" for i in range(10)]
    for obj in objects:
        bucket.put_object(Key=obj, Body=b"new data")

    exec_cmd(f"ceph osd pool set-quota {DATA_POOL} max_objects 1")

    log.debug('forcing quota to propagate')
    # We need the monitor to notice the pool stats and set the pool flags
    time.sleep(10)
    # And then we need to make sure the map with the newly set pool flags
    # has propagated to the OSDs.  rados ls should force every OSD with a
    # pg for this pool to have the most recent map
    exec_cmd(f'rados -p {DATA_POOL} ls')
    log.debug('forced quota to propagate')

    for obj in objects:
        try:
            bucket.Object(obj).delete()
        except Exception as e:
            log.debug(f"Got error {e} on delete of obj {obj}")
            assert False, f'Failure to delete obj {obj} with error {e}'

    (_out, ret) = exec_cmd(f'rados -p {DATA_POOL} ls | grep {key}', check_retcode=False)
    assert ret != 0, f'some objects were not cleaned up: {_out.decode("utf-8")}'

    # reset quota
    exec_cmd(f"ceph osd pool set-quota {DATA_POOL} max_objects 0")

main()
log.info("Completed rados pool quota tests")
