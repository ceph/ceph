#!/usr/bin/env python3

import logging as log
import json
from common import exec_cmd, create_user, boto_connect
from botocore.config import Config
from botocore.exceptions import ClientError

"""
Tests that multipart upload part heads are tracked under the
rgw.multipart category in bucket stats, separate from completed
objects (rgw.main), and that quota enforcement still counts them.
"""

USER = 'mpart-cat-tester'
DISPLAY_NAME = 'Multipart Category Testing'
ACCESS_KEY = 'MPARTCAT1TESTKEY00001'
SECRET_KEY = 'mpartcat1testsecretkey000000001'

PART_SIZE = 5 * 1024 * 1024  # 5 MiB (S3 minimum part size)


def get_bucket_stats(bucket_name):
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {bucket_name}')
    return json.loads(out)


def get_category(stats, category):
    return stats['usage'].get(category, {})


def assert_category(stats, category, num_objects, label=''):
    cat = get_category(stats, category)
    actual = cat.get('num_objects', 0)
    assert actual == num_objects, \
        f'{label}: expected {category}.num_objects={num_objects}, got {actual}'


def cleanup_bucket(connection, bucket_name):
    try:
        s3client = connection.meta.client
        response = s3client.list_multipart_uploads(Bucket=bucket_name)
        for upload in response.get('Uploads', []):
            s3client.abort_multipart_upload(
                Bucket=bucket_name, Key=upload['Key'],
                UploadId=upload['UploadId'])
        connection.Bucket(bucket_name).objects.all().delete()
        connection.Bucket(bucket_name).delete()
    except Exception:
        pass


def test_category_tracking(connection):
    """Verify multipart parts appear under rgw.multipart, not rgw.main."""
    bucket_name = 'mpart-cat-bucket'
    cleanup_bucket(connection, bucket_name)
    connection.create_bucket(Bucket=bucket_name)
    s3client = connection.meta.client

    # TESTCASE 'empty bucket has no stats'
    log.debug('TEST: empty bucket has no stats\n')
    stats = get_bucket_stats(bucket_name)
    assert_category(stats, 'rgw.main', 0, 'empty bucket')

    # TESTCASE 'incomplete multipart parts appear under rgw.multipart'
    log.debug('TEST: incomplete multipart parts appear under rgw.multipart\n')
    part_body = b'x' * PART_SIZE

    response = s3client.create_multipart_upload(Bucket=bucket_name, Key='incomplete-obj')
    upload_id = response['UploadId']

    for part_num in (1, 2):
        s3client.upload_part(
            Bucket=bucket_name, Key='incomplete-obj',
            UploadId=upload_id, PartNumber=part_num, Body=part_body)

    stats = get_bucket_stats(bucket_name)
    assert_category(stats, 'rgw.main', 0, 'after upload parts')
    assert_category(stats, 'rgw.multipart', 2, 'after upload parts')
    assert_category(stats, 'rgw.multimeta', 1, 'after upload parts')

    response = s3client.list_objects_v2(Bucket=bucket_name)
    assert response['KeyCount'] == 0, f'expected 0 listed objects, got {response["KeyCount"]}'

    # TESTCASE 'complete multipart moves parts to rgw.main'
    log.debug('TEST: complete multipart moves parts to rgw.main\n')
    parts_list = s3client.list_parts(
        Bucket=bucket_name, Key='incomplete-obj', UploadId=upload_id)
    parts = [{'ETag': p['ETag'], 'PartNumber': p['PartNumber']}
             for p in parts_list['Parts']]

    s3client.complete_multipart_upload(
        Bucket=bucket_name, Key='incomplete-obj',
        UploadId=upload_id,
        MultipartUpload={'Parts': parts})

    stats = get_bucket_stats(bucket_name)
    assert_category(stats, 'rgw.main', 1, 'after complete')
    assert_category(stats, 'rgw.multipart', 0, 'after complete')
    assert_category(stats, 'rgw.multimeta', 0, 'after complete')

    # TESTCASE 'abort multipart cleans up rgw.multipart entries'
    log.debug('TEST: abort multipart cleans up rgw.multipart entries\n')
    response = s3client.create_multipart_upload(
        Bucket=bucket_name, Key='aborted-obj')
    upload_id2 = response['UploadId']

    s3client.upload_part(
        Bucket=bucket_name, Key='aborted-obj',
        UploadId=upload_id2, PartNumber=1, Body=part_body)

    stats = get_bucket_stats(bucket_name)
    assert_category(stats, 'rgw.main', 1, 'before abort')
    assert_category(stats, 'rgw.multipart', 1, 'before abort')
    assert_category(stats, 'rgw.multimeta', 1, 'before abort')

    s3client.abort_multipart_upload(Bucket=bucket_name, Key='aborted-obj', UploadId=upload_id2)

    stats = get_bucket_stats(bucket_name)
    assert_category(stats, 'rgw.main', 1, 'after abort')
    assert_category(stats, 'rgw.multipart', 0, 'after abort')
    assert_category(stats, 'rgw.multimeta', 0, 'after abort')

    cleanup_bucket(connection, bucket_name)
    log.debug('test_category_tracking PASSED\n')


def test_quota_enforcement(connection):
    """Verify multipart parts count against bucket and user quotas."""
    bucket1 = 'mpart-quota-1'
    bucket2 = 'mpart-quota-2'
    s3client = connection.meta.client
    part_body = b'x' * PART_SIZE

    cleanup_bucket(connection, bucket1)
    cleanup_bucket(connection, bucket2)

    # bucket quota: 15 MB (3 x 5MB parts), user quota: 25 MB (5 x 5MB parts)
    exec_cmd(f'radosgw-admin quota set --quota-scope=bucket --uid={USER} --max-size=15728640')
    exec_cmd(f'radosgw-admin quota set --quota-scope=user --uid={USER} --max-size=26214400')
    exec_cmd(f'radosgw-admin quota enable --quota-scope=bucket --uid={USER}')
    exec_cmd(f'radosgw-admin quota enable --quota-scope=user --uid={USER}')

    try:
        connection.create_bucket(Bucket=bucket1)
        connection.create_bucket(Bucket=bucket2)

        # TESTCASE 'bucket quota: 4th part exceeds 15 MB bucket limit'
        log.debug('TEST: bucket quota rejects part that exceeds limit\n')
        mpu1 = s3client.create_multipart_upload(Bucket=bucket1, Key='obj1')
        uid1 = mpu1['UploadId']

        for i in range(1, 4):
            s3client.upload_part(Bucket=bucket1, Key='obj1', UploadId=uid1,
                                 PartNumber=i, Body=part_body)

        try:
            s3client.upload_part(Bucket=bucket1, Key='obj1', UploadId=uid1,
                                 PartNumber=4, Body=part_body)
            assert False, 'part 4 should have been rejected by bucket quota'
        except ClientError as e:
            assert e.response['Error']['Code'] == 'QuotaExceeded', \
                f'expected QuotaExceeded, got {e.response["Error"]["Code"]}'

        response = s3client.list_objects_v2(Bucket=bucket1)
        assert response['KeyCount'] == 0, 'bucket1 should have no completed objects'

        stats = get_bucket_stats(bucket1)
        assert_category(stats, 'rgw.main', 0, 'bucket1 quota')
        assert_category(stats, 'rgw.multipart', 3, 'bucket1 quota')

        # TESTCASE 'user quota: 3rd part in bucket2 exceeds 25 MB user limit'
        log.debug('TEST: user quota rejects part that exceeds limit\n')
        mpu2 = s3client.create_multipart_upload(Bucket=bucket2, Key='obj2')
        uid2 = mpu2['UploadId']

        # 15 MB already used in bucket1; user quota is 25 MB
        for i in range(1, 3):
            s3client.upload_part(Bucket=bucket2, Key='obj2', UploadId=uid2,
                                 PartNumber=i, Body=part_body)

        try:
            s3client.upload_part(Bucket=bucket2, Key='obj2', UploadId=uid2,
                                 PartNumber=3, Body=part_body)
            assert False, 'part 3 should have been rejected by user quota'
        except ClientError as e:
            assert e.response['Error']['Code'] == 'QuotaExceeded', \
                f'expected QuotaExceeded, got {e.response["Error"]["Code"]}'

        response = s3client.list_objects_v2(Bucket=bucket2)
        assert response['KeyCount'] == 0, 'bucket2 should have no completed objects'

        stats = get_bucket_stats(bucket2)
        assert_category(stats, 'rgw.main', 0, 'bucket2 quota')
        assert_category(stats, 'rgw.multipart', 2, 'bucket2 quota')

    finally:
        cleanup_bucket(connection, bucket1)
        cleanup_bucket(connection, bucket2)
        exec_cmd(f'radosgw-admin quota disable --quota-scope=bucket --uid={USER}')
        exec_cmd(f'radosgw-admin quota disable --quota-scope=user --uid={USER}')

    log.debug('test_quota_enforcement PASSED\n')


def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    # ensure quotas are disabled before category tracking tests
    exec_cmd(f'radosgw-admin quota disable --quota-scope=bucket --uid={USER}')
    exec_cmd(f'radosgw-admin quota disable --quota-scope=user --uid={USER}')

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries={
        'total_max_attempts': 1,
    }))

    test_category_tracking(connection)
    test_quota_enforcement(connection)

    log.debug('All multipart category tests passed\n')


if __name__ == '__main__':
    main()
