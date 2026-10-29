#!/usr/bin/env python3

import logging as log
import time
import datetime
import botocore
from common import exec_cmd, create_user, boto_connect
from botocore.config import Config
from swiftclient import client as swift
from swiftclient.exceptions import ClientException

"""
Tests that Swift object expiration (X-Delete-At/X-Delete-After) can't remove
object versions protected by S3 object lock retention or legal hold.
"""

USER = 'swift-lock-tester'
DISPLAY_NAME = 'Swift Object Lock Testing'
ACCESS_KEY = 'SWIFTLOCK0123456789A'
SECRET_KEY = 'swiftlocksecret0123456789abcdefghijklmn'
SWIFT_USER = USER + ':swift'
SWIFT_KEY = 'swiftlockswiftkey0123456789abcdefghijkl'
BUCKET_NAME = 'swift-lock-bucket'
DEFAULT_RETENTION_BUCKET = 'swift-lock-default-bucket'


def swift_connect(endpoint):
    # TempAuth with the swift subuser; no retries, so that errors surface
    return swift.Connection(authurl=endpoint + '/auth/1.0', user=SWIFT_USER,
                            key=SWIFT_KEY, auth_version='1', retries=0)


def swift_status(fn, *args, **kwargs):
    # swiftclient raises on non-2xx responses; return the HTTP status either way
    resp = {}
    try:
        fn(*args, response_dict=resp, **kwargs)
    except ClientException as e:
        return e.http_status
    return resp['status']


def swift_post(conn, bucket, key, headers):
    return swift_status(conn.post_object, bucket, key, headers=headers)


def swift_put(conn, bucket, key, headers):
    return swift_status(conn.put_object, bucket, key, b'data', headers=headers)


def version_exists(client, bucket, key, version_id):
    try:
        client.head_object(Bucket=bucket, Key=key, VersionId=version_id)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return False
        raise


def expire_objects(delay):
    # wait until the expiry hints are due, then run the object expirer
    time.sleep(delay)
    exec_cmd('radosgw-admin objects expire')


def cleanup(client, bucket):
    try:
        versions = client.list_object_versions(Bucket=bucket).get('Versions', [])
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            return
        raise
    for v in versions:
        try:
            client.put_object_legal_hold(Bucket=bucket, Key=v['Key'], VersionId=v['VersionId'],
                                         LegalHold={'Status': 'OFF'})
        except botocore.exceptions.ClientError:
            pass
        client.delete_object(Bucket=bucket, Key=v['Key'], VersionId=v['VersionId'],
                             BypassGovernanceRetention=True)
    client.delete_bucket(Bucket=bucket)


def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    exec_cmd(f'radosgw-admin subuser create --uid {USER} --subuser {SWIFT_USER} '
             f'--access full --key-type swift --secret {SWIFT_KEY}')

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries={
        'total_max_attempts': 1,
    }))
    client = connection.meta.client
    swift_conn = swift_connect(client.meta.endpoint_url)

    for b in (BUCKET_NAME, DEFAULT_RETENTION_BUCKET):
        cleanup(client, b)
    client.create_bucket(Bucket=BUCKET_NAME, ObjectLockEnabledForBucket=True)
    retain_until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)

    # TESTCASE 'swift X-Delete-After is rejected for a version under retention'
    log.debug('TEST: swift X-Delete-After is rejected for a version under retention\n')
    v = client.put_object(Bucket=BUCKET_NAME, Key='retained', Body=b'data',
                          ObjectLockMode='GOVERNANCE',
                          ObjectLockRetainUntilDate=retain_until)['VersionId']
    status = swift_post(swift_conn, BUCKET_NAME, 'retained',
                        {'X-Object-Version-Id': v, 'X-Delete-After': '1'})
    assert status == 403, status

    # TESTCASE 'swift X-Delete-At after the retain-until date is accepted'
    log.debug('TEST: swift X-Delete-At after the retain-until date is accepted\n')
    delete_at = int(retain_until.timestamp()) + 3600
    status = swift_post(swift_conn, BUCKET_NAME, 'retained',
                        {'X-Object-Version-Id': v, 'X-Delete-At': str(delete_at)})
    assert status == 202, status

    # TESTCASE 'swift X-Delete-After is rejected for a version under legal hold'
    log.debug('TEST: swift X-Delete-After is rejected for a version under legal hold\n')
    v = client.put_object(Bucket=BUCKET_NAME, Key='held', Body=b'data',
                          ObjectLockLegalHoldStatus='ON')['VersionId']
    status = swift_post(swift_conn, BUCKET_NAME, 'held',
                        {'X-Object-Version-Id': v, 'X-Delete-After': '1'})
    assert status == 403, status

    # TESTCASE 'swift PUT with X-Delete-After is rejected under default retention'
    log.debug('TEST: swift PUT with X-Delete-After is rejected under default retention\n')
    client.create_bucket(Bucket=DEFAULT_RETENTION_BUCKET, ObjectLockEnabledForBucket=True)
    client.put_object_lock_configuration(
        Bucket=DEFAULT_RETENTION_BUCKET,
        ObjectLockConfiguration={'ObjectLockEnabled': 'Enabled',
                                 'Rule': {'DefaultRetention': {'Mode': 'GOVERNANCE', 'Days': 1}}})
    status = swift_put(swift_conn, DEFAULT_RETENTION_BUCKET, 'obj', {'X-Delete-After': '1'})
    assert status == 403, status
    assert 'Versions' not in client.list_object_versions(Bucket=DEFAULT_RETENTION_BUCKET)

    # TESTCASE 'object expirer doesn't delete a version locked after X-Delete-At was set'
    log.debug("TEST: object expirer doesn't delete a version locked after X-Delete-At was set\n")
    short_retain_until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=20)
    held = client.put_object(Bucket=BUCKET_NAME, Key='late-held', Body=b'data')['VersionId']
    retained = client.put_object(Bucket=BUCKET_NAME, Key='late-retained', Body=b'data')['VersionId']
    for key, ver in (('late-held', held), ('late-retained', retained)):
        status = swift_post(swift_conn, BUCKET_NAME, key,
                            {'X-Object-Version-Id': ver, 'X-Delete-After': '2'})
        assert status == 202, status
    client.put_object_legal_hold(Bucket=BUCKET_NAME, Key='late-held', VersionId=held,
                                 LegalHold={'Status': 'ON'})
    client.put_object_retention(Bucket=BUCKET_NAME, Key='late-retained', VersionId=retained,
                                Retention={'Mode': 'GOVERNANCE',
                                           'RetainUntilDate': short_retain_until})
    expire_objects(5)
    assert version_exists(client, BUCKET_NAME, 'late-held', held)
    assert version_exists(client, BUCKET_NAME, 'late-retained', retained)

    # TESTCASE 'releasing the legal hold queues the expired version for deletion'
    log.debug('TEST: releasing the legal hold queues the expired version for deletion\n')
    client.put_object_legal_hold(Bucket=BUCKET_NAME, Key='late-held', VersionId=held,
                                 LegalHold={'Status': 'OFF'})
    expire_objects(1)
    assert not version_exists(client, BUCKET_NAME, 'late-held', held)
    assert version_exists(client, BUCKET_NAME, 'late-retained', retained)

    # TESTCASE 'object expirer deletes the version once its retention ends'
    log.debug('TEST: object expirer deletes the version once its retention ends\n')
    wait = (short_retain_until - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    expire_objects(max(wait, 0) + 1)
    assert not version_exists(client, BUCKET_NAME, 'late-retained', retained)

    # TESTCASE 'object expirer still deletes an unlocked version'
    log.debug('TEST: object expirer still deletes an unlocked version\n')
    v = client.put_object(Bucket=BUCKET_NAME, Key='unlocked', Body=b'data')['VersionId']
    status = swift_post(swift_conn, BUCKET_NAME, 'unlocked',
                        {'X-Object-Version-Id': v, 'X-Delete-After': '2'})
    assert status == 202, status
    expire_objects(5)
    assert not version_exists(client, BUCKET_NAME, 'unlocked', v)

    for b in (BUCKET_NAME, DEFAULT_RETENTION_BUCKET):
        cleanup(client, b)
    exec_cmd(f'radosgw-admin user rm --uid {USER} --purge-keys')


if __name__ == '__main__':
    main()
    log.info('Completed swift object lock tests')
