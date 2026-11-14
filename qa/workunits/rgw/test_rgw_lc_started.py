#!/usr/bin/env python3

import logging as log
import json
import time
import botocore
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
"""

""" Constants """
USER = 'lc-started-tester'
DISPLAY_NAME = 'LC Started Testing'
ACCESS_KEY = 'LCSTARTED0123456789A'
SECRET_KEY = 'lcstartedsecretkey0123456789abcdefghijklm'
BUCKET_NAME = 'lc-started-bucket'
RULE_ID = 'lc-started-expire'

EPOCH_PREFIX = 'Thu, 01 Jan 1970'


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

    # TESTCASE 'an entry that has never been processed reports the epoch'
    log.debug('TEST: an entry that has never been processed reports the epoch\n')
    entry = get_lc_entry(BUCKET_NAME)
    assert entry is not None, f'no lc list entry for {BUCKET_NAME}'
    assert entry['started'].startswith(EPOCH_PREFIX), \
        f"expected an unset 'started', got {entry['started']}"

    # TESTCASE 'an ad-hoc run initializes started'
    log.debug('TEST: an ad-hoc run initializes started\n')
    exec_cmd(f'radosgw-admin lc process --bucket={BUCKET_NAME}'
             ' --rgw-lc-debug-interval=10')
    entry = get_lc_entry(BUCKET_NAME)
    assert entry is not None, f'no lc list entry for {BUCKET_NAME}'
    assert not entry['started'].startswith(EPOCH_PREFIX), \
        f"'started' was not initialized, got {entry['started']}"
    first_started = entry['started']
    log.info(f'started initialized to {first_started}')

    # 'started' has one-second resolution; sleep so that a second ad-hoc
    # run would produce a visibly different timestamp if it overwrote it
    time.sleep(2)

    # TESTCASE 'a second ad-hoc run does not overwrite started'
    log.debug('TEST: a second ad-hoc run does not overwrite started\n')
    exec_cmd(f'radosgw-admin lc process --bucket={BUCKET_NAME}'
             ' --rgw-lc-debug-interval=10')
    entry = get_lc_entry(BUCKET_NAME)
    assert entry is not None, f'no lc list entry for {BUCKET_NAME}'
    assert entry['started'] == first_started, \
        f"ad-hoc run overwrote 'started': {first_started} -> {entry['started']}"

    # post-test cleanup
    bucket.objects.all().delete()
    bucket.delete()

    log.info('OK')


main()

