#!/usr/bin/env python3

import logging as log
import json
import botocore
from common import exec_cmd, create_user, boto_connect, put_objects
from botocore.config import Config

"""
Tests radosgw-admin bucket suspend/unsuspend commands.
"""

USER = 'suspend-tester'
DISPLAY_NAME = 'Bucket Suspend Testing'
ACCESS_KEY = 'OJODXSLNX4LUNHQG99PB'
SECRET_KEY = '3l6ffld34qaymfomuh832j94738aie2x4p2o8h6n'
BUCKET_NAME = 'suspend-bucket'

def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries={
        'total_max_attempts': 1,
    }))

    try:
        bucket = connection.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            raise

    bucket = connection.create_bucket(Bucket=BUCKET_NAME)
    put_objects(bucket, ['obj1'])

    # TESTCASE 'bucket stats reports suspended=false by default'
    log.debug('TEST: bucket stats reports suspended=false by default\n')
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}')
    stats = json.loads(out)
    assert stats['suspended'] is False

    # TESTCASE 'list objects succeeds before suspend'
    log.debug('TEST: list objects succeeds before suspend\n')
    assert list(bucket.objects.all())

    # TESTCASE 'bucket suspend sets suspended=true and blocks S3 access'
    log.debug('TEST: bucket suspend sets suspended=true and blocks S3 access\n')
    exec_cmd(f'radosgw-admin bucket suspend --bucket {BUCKET_NAME}')
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}')
    stats = json.loads(out)
    assert stats['suspended'] is True

    try:
        list(bucket.objects.all())
        raise AssertionError('expected BucketSuspended error')
    except botocore.exceptions.ClientError as e:
        code = e.response['Error']['Code']
        assert code == 'BucketSuspended', (
            'expected BucketSuspended, got %r: %s' % (
                code, e.response.get('Error', {})))

    # TESTCASE 'bucket unsuspend restores access'
    log.debug('TEST: bucket unsuspend restores access\n')
    exec_cmd(f'radosgw-admin bucket unsuspend --bucket {BUCKET_NAME}')
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}')
    stats = json.loads(out)
    assert stats['suspended'] is False
    assert list(bucket.objects.all())

    bucket.objects.all().delete()
    bucket.delete()

if __name__ == '__main__':
    main()
    log.info('Completed bucket suspend tests')
