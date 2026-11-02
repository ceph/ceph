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
Tests the rgwx-* system request params that let system users preserve
object version ids and mtimes when copying objects into RGW. These are
relied on by external replication and migration tools, so this guards
the params from being dropped or broken by refactoring.
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

""" Constants """
SYSTEM_USER = 'sysparams-system-tester'
SYSTEM_DISPLAY_NAME = 'System Params Testing (system)'
SYSTEM_ACCESS_KEY = 'KMQ8R5BRZ6NTVDDZ9WCB'
SYSTEM_SECRET_KEY = 'gv5p0vhrmsyfmuvw6bqa8hvvi4zd7jrrxtb2h4lp'
SYSTEM_BUCKET_NAME = 'sysparams-system-bucket'

USER = 'sysparams-tester'
DISPLAY_NAME = 'System Params Testing'
ACCESS_KEY = 'QZBMHZY0EV6Z7DJPP3JT'
SECRET_KEY = '1s6utnc2fyjo8p8xxgpygjgm2t8gbg6uxb72hqq0'
BUCKET_NAME = 'sysparams-bucket'

MTIME_SECS = 1600000000
MTIME = datetime.fromtimestamp(MTIME_SECS, tz=timezone.utc)
MTIME_PARAM = f'{MTIME_SECS}.500000000'
MTIME_STAT = '2020-09-13T12:26:40.500000Z'

# minimum size of a non-final multipart part
PART_SIZE = 5 * 1024 * 1024


class SysParams:
    """
    Appends rgwx-* params to the query string of every PutObject and
    CompleteMultipartUpload request sent through the given client.
    """
    def __init__(self, client):
        self.params = {}
        events = client.meta.events
        events.register('before-sign.s3.PutObject', self.add_params)
        events.register('before-sign.s3.CompleteMultipartUpload', self.add_params)

    def set(self, version_id, mtime):
        self.params = {'rgwx-version-id': version_id, 'rgwx-mtime': mtime}

    def add_params(self, request, **kwargs):
        if not self.params:
            return
        sep = '&' if '?' in request.url else '?'
        request.url += sep + urllib.parse.urlencode(self.params)


def new_version_id():
    return 'migrated-' + uuid.uuid4().hex


def reset_bucket(connection, bucket_name):
    try:
        bucket = connection.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.object_versions.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == 'NoSuchBucket':
            raise
    connection.create_bucket(Bucket=bucket_name)
    connection.BucketVersioning(bucket_name).enable()


def object_stat_mtime(bucket_name, key, version_id):
    out = exec_cmd(f'radosgw-admin object stat --bucket {bucket_name} --object {key} --object-version {version_id}')
    return json.loads(out)['mtime']


def main():
    """
    execute rgwx-* system param tests
    """
    create_user(SYSTEM_USER, SYSTEM_DISPLAY_NAME, SYSTEM_ACCESS_KEY, SYSTEM_SECRET_KEY)
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    config = Config(retries = {'total_max_attempts': 1})
    system_connection = boto_connect(SYSTEM_ACCESS_KEY, SYSTEM_SECRET_KEY, config)
    connection = boto_connect(ACCESS_KEY, SECRET_KEY, config)

    reset_bucket(system_connection, SYSTEM_BUCKET_NAME)
    reset_bucket(connection, BUCKET_NAME)
    # set --system only after creating buckets: as a system user, CreateBucket
    # returns a JSON body that botocore treats as an error
    exec_cmd(f'radosgw-admin user modify --uid {SYSTEM_USER} --system')

    system_client = system_connection.meta.client
    system_params = SysParams(system_client)
    client = connection.meta.client
    params = SysParams(client)

    # TESTCASE 'system user preserves version id and mtime on PutObject'
    log.debug('TEST: system user preserves version id and mtime on PutObject\n')
    key = str(uuid.uuid4())
    version_id = new_version_id()
    system_params.set(version_id, MTIME_PARAM)
    resp = system_client.put_object(Bucket=SYSTEM_BUCKET_NAME, Key=key, Body=b'some_data')
    assert resp['VersionId'] == version_id, f'PutObject returned version {resp["VersionId"]}, expected {version_id}'
    head = system_client.head_object(Bucket=SYSTEM_BUCKET_NAME, Key=key)
    assert head['VersionId'] == version_id, f'HEAD returned version {head["VersionId"]}, expected {version_id}'
    assert head['LastModified'] == MTIME, f'HEAD returned Last-Modified {head["LastModified"]}, expected {MTIME}'
    mtime = object_stat_mtime(SYSTEM_BUCKET_NAME, key, version_id)
    assert mtime == MTIME_STAT, f'object stat returned mtime {mtime}, expected {MTIME_STAT}'

    # TESTCASE 'system user preserves version id and mtime on multipart upload'
    log.debug('TEST: system user preserves version id and mtime on multipart upload\n')
    key = str(uuid.uuid4())
    version_id = new_version_id()
    system_params.set(version_id, MTIME_PARAM)
    upload = system_client.create_multipart_upload(Bucket=SYSTEM_BUCKET_NAME, Key=key)
    parts = []
    for num, body in enumerate([b'a' * PART_SIZE, b'b' * 1024], start=1):
        part = system_client.upload_part(Bucket=SYSTEM_BUCKET_NAME, Key=key, UploadId=upload['UploadId'],
                                         PartNumber=num, Body=body)
        parts.append({'PartNumber': num, 'ETag': part['ETag']})
    resp = system_client.complete_multipart_upload(Bucket=SYSTEM_BUCKET_NAME, Key=key, UploadId=upload['UploadId'],
                                                   MultipartUpload={'Parts': parts})
    assert resp['VersionId'] == version_id, f'CompleteMultipartUpload returned version {resp["VersionId"]}, expected {version_id}'
    head = system_client.head_object(Bucket=SYSTEM_BUCKET_NAME, Key=key)
    assert head['VersionId'] == version_id, f'HEAD returned version {head["VersionId"]}, expected {version_id}'
    assert head['LastModified'] == MTIME, f'HEAD returned Last-Modified {head["LastModified"]}, expected {MTIME}'
    assert head['ContentLength'] == PART_SIZE + 1024
    mtime = object_stat_mtime(SYSTEM_BUCKET_NAME, key, version_id)
    assert mtime == MTIME_STAT, f'object stat returned mtime {mtime}, expected {MTIME_STAT}'

    # TESTCASE 'short fractional rgwx-mtime is padded to nanoseconds'
    log.debug('TEST: short fractional rgwx-mtime is padded to nanoseconds\n')
    key = str(uuid.uuid4())
    version_id = new_version_id()
    system_params.set(version_id, f'{MTIME_SECS}.25')
    system_client.put_object(Bucket=SYSTEM_BUCKET_NAME, Key=key, Body=b'some_data')
    head = system_client.head_object(Bucket=SYSTEM_BUCKET_NAME, Key=key)
    assert head['LastModified'] == MTIME, f'HEAD returned Last-Modified {head["LastModified"]}, expected {MTIME}'
    mtime = object_stat_mtime(SYSTEM_BUCKET_NAME, key, version_id)
    assert mtime == '2020-09-13T12:26:40.250000Z', f'object stat returned mtime {mtime}'

    # TESTCASE 'rgwx params are ignored for non-system users'
    log.debug('TEST: rgwx params are ignored for non-system users\n')
    key = str(uuid.uuid4())
    version_id = new_version_id()
    params.set(version_id, MTIME_PARAM)
    before = datetime.now(tz=timezone.utc)
    resp = client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b'some_data')
    assert resp['VersionId'] != version_id, 'non-system user must not set the version id'
    head = client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head['LastModified'] != MTIME, 'non-system user must not set the mtime'
    assert abs((head['LastModified'] - before).total_seconds()) < 300, f'unexpected Last-Modified {head["LastModified"]}'

    # TESTCASE 'malformed rgwx-mtime is rejected for system users'
    log.debug('TEST: malformed rgwx-mtime is rejected for system users\n')
    for bad in ['not-a-timestamp', '.5', f'{MTIME_SECS}.', f'{MTIME_SECS}.1.2', f'{MTIME_SECS}.1234567890', f'-{MTIME_SECS}',
                f'+{MTIME_SECS}', f'{MTIME_SECS}.+5', f'{MTIME_SECS}.-0']:
        key = str(uuid.uuid4())
        system_params.set(new_version_id(), bad)
        try:
            system_client.put_object(Bucket=SYSTEM_BUCKET_NAME, Key=key, Body=b'some_data')
            assert False, f'PutObject with rgwx-mtime={bad} must fail'
        except botocore.exceptions.ClientError as e:
            assert e.response['Error']['Code'] == 'InvalidArgument', f'rgwx-mtime={bad}: unexpected error {e.response["Error"]["Code"]}'

    log.debug('Deleting buckets')
    for conn, name in ((system_connection, SYSTEM_BUCKET_NAME), (connection, BUCKET_NAME)):
        bucket = conn.Bucket(name)
        bucket.object_versions.all().delete()
        bucket.delete()


main()
log.info("Completed rgwx system param tests")
