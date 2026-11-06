#!/usr/bin/env python3

import logging as log
import json
import uuid
import urllib.parse
from datetime import datetime, timezone
import botocore
from botocore.config import Config
from common import exec_cmd, create_user, boto_connect

"""
Tests the rgwx-migration-* request params that let users with the
migration=write cap preserve object version ids and mtimes when copying
objects into RGW from other S3 vendors.
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

""" Constants """
MIGRATION_USER = 'migration-tester'
MIGRATION_DISPLAY_NAME = 'Migration Params Testing'
MIGRATION_ACCESS_KEY = 'XZ0SL162HONHIEPYRWOJ'
MIGRATION_SECRET_KEY = 'yedgknyi7lxqjeqrvvf3gy5pv4eq6dr17k02o64t'
BUCKET_NAME = 'migration-bucket'
LOCK_BUCKET_NAME = 'migration-lock-bucket'

USER = 'migration-nocap-tester'
DISPLAY_NAME = 'Migration Params Testing (no cap)'
ACCESS_KEY = '19S0JETUHYPMVNYFGONW'
SECRET_KEY = 'ijbhbkm2dfnu7vwqt6db35hgkrqxjgsssp1e06ci'
NOCAP_BUCKET_NAME = 'migration-nocap-bucket'

SYSTEM_USER = 'migration-system-tester'
SYSTEM_DISPLAY_NAME = 'Migration Params Testing (system, no cap)'
SYSTEM_ACCESS_KEY = 'GI40HQK0T6HW9K9I45IU'
SYSTEM_SECRET_KEY = 'q83eyhgee020zhs5cabdy4jvjvdz3ral2kmsm5gt'
SYSTEM_BUCKET_NAME = 'migration-system-bucket'

MTIME_SECS = 1600000000
MTIME = datetime.fromtimestamp(MTIME_SECS, tz=timezone.utc)
MTIME_PARAM = f'{MTIME_SECS}.500000000'
MTIME_STAT = '2020-09-13T12:26:40.500000Z'

# past the 32-bit signed time_t limit (2038), within utime_t's 32-bit unsigned seconds
FAR_MTIME_SECS = 4102444800
FAR_MTIME = datetime.fromtimestamp(FAR_MTIME_SECS, tz=timezone.utc)

# minimum size of a non-final multipart part
PART_SIZE = 5 * 1024 * 1024


class QueryParams:
    """
    Appends the given params to the query string of the given operations
    (PutObject and CompleteMultipartUpload by default) sent through the client.
    """
    def __init__(self, client, ops=('PutObject', 'CompleteMultipartUpload')):
        self.params = {}
        for op in ops:
            client.meta.events.register(f'before-sign.s3.{op}', self.add_params)

    def set(self, **params):
        self.params = {'rgwx-' + k.replace('_', '-'): v for k, v in params.items()}

    def add_params(self, request, **kwargs):
        if not self.params:
            return
        sep = '&' if '?' in request.url else '?'
        request.url += sep + urllib.parse.urlencode(self.params)


def new_version_id():
    return 'migrated-' + uuid.uuid4().hex


def reset_bucket(connection, bucket_name, object_lock=False):
    try:
        bucket = connection.Bucket(bucket_name)
        bucket.object_versions.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == 'NoSuchBucket':
            raise
    # object lock implies versioning
    connection.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=object_lock)
    connection.BucketVersioning(bucket_name).enable()


def delete_bucket(connection, bucket_name):
    bucket = connection.Bucket(bucket_name)
    bucket.object_versions.all().delete()
    bucket.delete()


def object_stat_mtime(bucket_name, key, version_id):
    out = exec_cmd(f'radosgw-admin object stat --bucket {bucket_name} --object {key} --object-version {version_id}')
    return json.loads(out)['mtime']


def multipart_upload(client, bucket_name, key):
    upload = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    parts = []
    for num, body in enumerate([b'a' * PART_SIZE, b'b' * 1024], start=1):
        part = client.upload_part(Bucket=bucket_name, Key=key, UploadId=upload['UploadId'],
                                  PartNumber=num, Body=body)
        parts.append({'PartNumber': num, 'ETag': part['ETag']})
    try:
        return client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload['UploadId'],
                                                MultipartUpload={'Parts': parts})
    except botocore.exceptions.ClientError:
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload['UploadId'])
        raise


def assert_error(fn, status, code):
    try:
        fn()
    except botocore.exceptions.ClientError as e:
        got_status = e.response['ResponseMetadata']['HTTPStatusCode']
        got_code = e.response['Error']['Code']
        assert (got_status, got_code) == (status, code), f'expected {status} {code}, got {got_status} {got_code}'
        return
    assert False, f'expected {status} {code}, request succeeded'


def assert_ignored(client, params, bucket_name):
    key = str(uuid.uuid4())
    version_id = new_version_id()
    params.set(migration_version_id=version_id, migration_mtime=MTIME_PARAM)
    before = datetime.now(tz=timezone.utc)
    resp = client.put_object(Bucket=bucket_name, Key=key, Body=b'some_data')
    assert resp['VersionId'] != version_id, 'version id must not be set without the migration cap'
    head = client.head_object(Bucket=bucket_name, Key=key)
    assert abs((head['LastModified'] - before).total_seconds()) < 300, \
        f'mtime must not be set without the migration cap, got Last-Modified {head["LastModified"]}'


def main():
    """
    execute rgwx-migration-* param tests
    """
    create_user(MIGRATION_USER, MIGRATION_DISPLAY_NAME, MIGRATION_ACCESS_KEY, MIGRATION_SECRET_KEY)
    exec_cmd(f'radosgw-admin caps add --uid {MIGRATION_USER} --caps "migration=write"')
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    create_user(SYSTEM_USER, SYSTEM_DISPLAY_NAME, SYSTEM_ACCESS_KEY, SYSTEM_SECRET_KEY)
    # as a system user, CreateBucket returns a JSON body that botocore treats
    # as an error, so set --system only after creating the bucket
    exec_cmd(f'radosgw-admin user modify --uid {SYSTEM_USER} --system=false')

    config = Config(retries = {'total_max_attempts': 1})
    migration_connection = boto_connect(MIGRATION_ACCESS_KEY, MIGRATION_SECRET_KEY, config)
    connection = boto_connect(ACCESS_KEY, SECRET_KEY, config)
    system_connection = boto_connect(SYSTEM_ACCESS_KEY, SYSTEM_SECRET_KEY, config)

    reset_bucket(migration_connection, BUCKET_NAME)
    reset_bucket(migration_connection, LOCK_BUCKET_NAME, object_lock=True)
    reset_bucket(connection, NOCAP_BUCKET_NAME)
    reset_bucket(system_connection, SYSTEM_BUCKET_NAME)
    exec_cmd(f'radosgw-admin user modify --uid {SYSTEM_USER} --system')

    client = migration_connection.meta.client
    params = QueryParams(client)

    # TESTCASE 'migration user preserves version id and mtime on PutObject'
    log.debug('TEST: migration user preserves version id and mtime on PutObject\n')
    key = str(uuid.uuid4())
    version_id = new_version_id()
    params.set(migration_version_id=version_id, migration_mtime=MTIME_PARAM)
    resp = client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b'some_data')
    assert resp['VersionId'] == version_id, f'PutObject returned version {resp["VersionId"]}, expected {version_id}'
    head = client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head['VersionId'] == version_id, f'HEAD returned version {head["VersionId"]}, expected {version_id}'
    assert head['LastModified'] == MTIME, f'HEAD returned Last-Modified {head["LastModified"]}, expected {MTIME}'
    mtime = object_stat_mtime(BUCKET_NAME, key, version_id)
    assert mtime == MTIME_STAT, f'object stat returned mtime {mtime}, expected {MTIME_STAT}'

    # TESTCASE 'migration mtime past 2038 with short fraction is preserved'
    log.debug('TEST: migration mtime past 2038 with short fraction is preserved\n')
    key = str(uuid.uuid4())
    params.set(migration_mtime=f'{FAR_MTIME_SECS}.25')
    resp = client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b'some_data')
    head = client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head['LastModified'] == FAR_MTIME, f'HEAD returned Last-Modified {head["LastModified"]}, expected {FAR_MTIME}'
    mtime = object_stat_mtime(BUCKET_NAME, key, resp['VersionId'])
    assert mtime == '2100-01-01T00:00:00.250000Z', f'object stat returned mtime {mtime}'

    # TESTCASE 'migration user preserves version id and mtime on multipart upload'
    log.debug('TEST: migration user preserves version id and mtime on multipart upload\n')
    key = str(uuid.uuid4())
    version_id = new_version_id()
    params.set(migration_version_id=version_id, migration_mtime=MTIME_PARAM)
    resp = multipart_upload(client, BUCKET_NAME, key)
    assert resp['VersionId'] == version_id, f'CompleteMultipartUpload returned version {resp["VersionId"]}, expected {version_id}'
    head = client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head['VersionId'] == version_id, f'HEAD returned version {head["VersionId"]}, expected {version_id}'
    assert head['LastModified'] == MTIME, f'HEAD returned Last-Modified {head["LastModified"]}, expected {MTIME}'
    assert head['ContentLength'] == PART_SIZE + 1024
    mtime = object_stat_mtime(BUCKET_NAME, key, version_id)
    assert mtime == MTIME_STAT, f'object stat returned mtime {mtime}, expected {MTIME_STAT}'

    # TESTCASE 'migration params are ignored without the migration cap'
    log.debug('TEST: migration params are ignored without the migration cap\n')
    assert_ignored(connection.meta.client, QueryParams(connection.meta.client), NOCAP_BUCKET_NAME)

    # TESTCASE 'migration params are ignored for system users without the migration cap'
    log.debug('TEST: migration params are ignored for system users without the migration cap\n')
    system_client = system_connection.meta.client
    system_params = QueryParams(system_client)
    assert_ignored(system_client, system_params, SYSTEM_BUCKET_NAME)

    # TESTCASE 'system user still preserves version id with rgwx-version-id'
    log.debug('TEST: system user still preserves version id with rgwx-version-id\n')
    version_id = new_version_id()
    system_params.set(version_id=version_id)
    resp = system_client.put_object(Bucket=SYSTEM_BUCKET_NAME, Key=str(uuid.uuid4()), Body=b'some_data')
    assert resp['VersionId'] == version_id, f'PutObject returned version {resp["VersionId"]}, expected {version_id}'

    # TESTCASE 'system rgwx-version-id takes precedence over migration version id'
    log.debug('TEST: system rgwx-version-id takes precedence over migration version id\n')
    exec_cmd(f'radosgw-admin caps add --uid {SYSTEM_USER} --caps "migration=write"')
    version_id = new_version_id()
    system_params.set(version_id=version_id, migration_version_id=new_version_id())
    resp = system_client.put_object(Bucket=SYSTEM_BUCKET_NAME, Key=str(uuid.uuid4()), Body=b'some_data')
    exec_cmd(f'radosgw-admin caps rm --uid {SYSTEM_USER} --caps "migration=write"')
    assert resp['VersionId'] == version_id, f'PutObject returned version {resp["VersionId"]}, expected {version_id}'

    # TESTCASE 'migration mtime is rejected on object lock buckets'
    log.debug('TEST: migration mtime is rejected on object lock buckets\n')
    params.set(migration_version_id=new_version_id(), migration_mtime=MTIME_PARAM)
    assert_error(lambda: client.put_object(Bucket=LOCK_BUCKET_NAME, Key=str(uuid.uuid4()), Body=b'some_data'),
                 409, 'InvalidBucketState')
    assert_error(lambda: multipart_upload(client, LOCK_BUCKET_NAME, str(uuid.uuid4())),
                 409, 'InvalidBucketState')

    # TESTCASE 'migration params are ignored on UploadPart'
    log.debug('TEST: migration params are ignored on UploadPart\n')
    part_params = QueryParams(client, ops=('UploadPart',))
    part_params.set(migration_version_id=new_version_id(), migration_mtime=MTIME_PARAM)
    params.set()
    resp = multipart_upload(client, LOCK_BUCKET_NAME, str(uuid.uuid4()))
    part_params.set()
    head = client.head_object(Bucket=LOCK_BUCKET_NAME, Key=resp['Key'], VersionId=resp['VersionId'])
    assert head['LastModified'] != MTIME, 'mtime must not be set by UploadPart'

    # TESTCASE 'migration version id is preserved on object lock buckets'
    log.debug('TEST: migration version id is preserved on object lock buckets\n')
    version_id = new_version_id()
    params.set(migration_version_id=version_id)
    resp = client.put_object(Bucket=LOCK_BUCKET_NAME, Key=str(uuid.uuid4()), Body=b'some_data')
    assert resp['VersionId'] == version_id, f'PutObject returned version {resp["VersionId"]}, expected {version_id}'

    # TESTCASE 'malformed migration mtime is rejected'
    log.debug('TEST: malformed migration mtime is rejected\n')
    for bad in ['not-a-timestamp', '.5', f'{MTIME_SECS}.', f'{MTIME_SECS}.1.2', f'{MTIME_SECS}.1234567890', f'-{MTIME_SECS}',
                f'+{MTIME_SECS}', f'{MTIME_SECS}.+5', f'{MTIME_SECS}.-0', f'{2**32}']:
        params.set(migration_version_id=new_version_id(), migration_mtime=bad)
        assert_error(lambda: client.put_object(Bucket=BUCKET_NAME, Key=str(uuid.uuid4()), Body=b'some_data'),
                     400, 'InvalidArgument')

    log.debug('Deleting buckets')
    params.set()
    delete_bucket(migration_connection, BUCKET_NAME)
    delete_bucket(migration_connection, LOCK_BUCKET_NAME)
    delete_bucket(connection, NOCAP_BUCKET_NAME)
    delete_bucket(system_connection, SYSTEM_BUCKET_NAME)


main()
log.info("Completed rgwx-migration param tests")
