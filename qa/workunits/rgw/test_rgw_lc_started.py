#!/usr/bin/env python3

import logging as log
import json
import time
import botocore
from email.utils import parsedate_to_datetime
from common import exec_cmd, create_user, boto_connect
from botocore.config import Config

"""
Tests the 'started' timestamp reported by 'radosgw-admin lc list'.

The single-bucket lifecycle path used by 'radosgw-admin lc process
--bucket' transitions the LC pool entry to PROCESSING. Historically it
left start_time at its default-initialized value of 0, so 'lc list'
reported "Thu, 01 Jan 1970 00:00:00 GMT" for buckets that had never
been processed by the periodic LC worker.

start_time is now initialized on that path when, and only when, it has
never been set. An ad-hoc run must not overwrite a timestamp already
recorded for the entry, because already_run_today() and
expired_session() in the periodic-shard path read that value.

For a bucket that has never been processed, the first ad-hoc run
records start_time, so a scheduled pass in the same interval skips the
bucket (already_run_today()), and a scheduled pass in a later interval
processes it.
"""

""" Constants """
USER = 'lc-started-tester'
DISPLAY_NAME = 'LC Started Testing'
ACCESS_KEY = 'LCSTARTED0123456789A'
SECRET_KEY = 'lcstartedsecretkey0123456789abcdefghijklm'
BUCKET_NAME = 'lc-started-bucket'
RULE_ID = 'lc-started-expire'

# rgw_lc_debug_interval: each INTERVAL seconds is treated as one LC day
INTERVAL = 60


def get_lc_entry(bucket_name):
    """
    Return the 'lc list' entry for bucket_name, or None. Entries are
    keyed as '<tenant>:<bucket>:<marker>'.
    """
    out = exec_cmd('radosgw-admin lc list')
    for entry in json.loads(out):
        if f':{bucket_name}:' in entry['bucket']:
            return entry
    return None


def get_started(bucket_name):
    """
    Return the 'started' value of the 'lc list' entry for bucket_name
    as a unix timestamp.
    """
    entry = get_lc_entry(bucket_name)
    assert entry is not None, f'no lc list entry for {bucket_name}'
    return parsedate_to_datetime(entry['started']).timestamp()


def lc_process(bucket_name=None):
    """
    Run 'radosgw-admin lc process', either ad-hoc for a single bucket
    or, without a bucket, through the scheduled (shard-walking) path.
    """
    cmd = 'radosgw-admin lc process'
    if bucket_name:
        cmd += f' --bucket={bucket_name}'
    cmd += f' --rgw-lc-debug-interval={INTERVAL}'
    exec_cmd(cmd)


def wait_for_interval_start(interval):
    """
    Sleep until just after the next interval boundary, so that the
    commands which follow land in the same LC "day".
    """
    time.sleep(interval - (time.time() % interval) + 1)


def set_lifecycle(connection, bucket_name):
    connection.meta.client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            'Rules': [
                {
                    'ID': RULE_ID,
                    'Filter': {'Prefix': ''},
                    'Status': 'Enabled',
                    'Expiration': {'Days': 1},
                }
            ]
        }
    )
    log.info(f'Set lifecycle rule {RULE_ID} on {bucket_name}')


def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries={
        'total_max_attempts': 1,
    }))

    # pre-test cleanup
    try:
        bucket = connection.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == 'NoSuchBucket':
            raise

    bucket = connection.create_bucket(Bucket=BUCKET_NAME)
    bucket.put_object(Key='obj', Body=b'some_data')

    set_lifecycle(connection, BUCKET_NAME)

    # TESTCASE 'an entry that has never been processed is UNINITIAL'
    log.debug('TEST: an entry that has never been processed is UNINITIAL\n')
    entry = get_lc_entry(BUCKET_NAME)
    assert entry is not None, f'no lc list entry for {BUCKET_NAME}'
    assert entry['status'] == 'UNINITIAL', \
        f"expected UNINITIAL, got {entry['status']}"

    # align to an interval boundary so the ad-hoc run and the scheduled
    # pass below fall into the same LC "day"
    wait_for_interval_start(INTERVAL)

    # TESTCASE 'an ad-hoc run initializes started'
    log.debug('TEST: an ad-hoc run initializes started\n')
    lc_process(BUCKET_NAME)
    first_started = get_started(BUCKET_NAME)
    assert abs(first_started - time.time()) < 60, \
        f"'started' was not initialized, got {first_started}"
    log.info(f'started initialized to {first_started}')

    # 'started' has one-second resolution; sleep so that a later run
    # would produce a visibly different timestamp if it overwrote it
    time.sleep(2)

    # TESTCASE 'a second ad-hoc run does not overwrite started'
    log.debug('TEST: a second ad-hoc run does not overwrite started\n')
    lc_process(BUCKET_NAME)
    started = get_started(BUCKET_NAME)
    assert started == first_started, \
        f"ad-hoc run overwrote 'started': {first_started} -> {started}"

    # TESTCASE 'a scheduled pass in the same interval skips the bucket'
    log.debug('TEST: a scheduled pass in the same interval skips the bucket\n')
    lc_process()
    started = get_started(BUCKET_NAME)
    assert started == first_started, \
        f"scheduled pass in the same interval processed the bucket: " \
        f"{first_started} -> {started}"

    # TESTCASE 'a scheduled pass in a later interval processes the bucket'
    log.debug('TEST: a scheduled pass in a later interval processes the bucket\n')
    wait_for_interval_start(INTERVAL)
    lc_process()
    started = get_started(BUCKET_NAME)
    assert started > first_started, \
        f"scheduled pass in a later interval skipped the bucket: " \
        f"'started' still {first_started}"

    # post-test cleanup
    bucket.objects.all().delete()
    bucket.delete()

    log.info('OK')


main()
