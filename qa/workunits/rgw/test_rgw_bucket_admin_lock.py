#!/usr/bin/env python3

import logging as log
import json
import datetime
import botocore
from common import exec_cmd, create_user, boto_connect, put_objects
from botocore.config import Config

"""
Tests radosgw-admin bucket admin-lock/admin-unlock commands.
"""

USER = 'admin-lock-tester'
DISPLAY_NAME = 'Bucket Admin Lock Testing'
ACCESS_KEY = '0555b35654ad1656d804'
SECRET_KEY = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
BUCKET_NAME = 'admin-lock-bucket'

POLICY = json.dumps({
    'Version': '2012-10-17',
    'Statement': [{
        'Effect': 'Allow',
        'Principal': {'AWS': ['*']},
        'Action': 's3:GetObject',
        'Resource': f'arn:aws:s3:::{BUCKET_NAME}/*',
    }],
})
LIFECYCLE = {
    'Rules': [{
        'ID': 'expire',
        'Status': 'Enabled',
        'Filter': {'Prefix': ''},
        'Expiration': {'Days': 1},
    }],
}
OBJECT_LOCK = {
    'ObjectLockEnabled': 'Enabled',
    'Rule': {'DefaultRetention': {'Mode': 'GOVERNANCE', 'Days': 1}},
}
SHORTER_OBJECT_LOCK = {
    'ObjectLockEnabled': 'Enabled',
    'Rule': {'DefaultRetention': {'Mode': 'GOVERNANCE', 'Days': 0}},
}
TAGGING = {'TagSet': [{'Key': 'owner', 'Value': 'tenant'}]}
PUBLIC_ACCESS_BLOCK = {
    'BlockPublicAcls': False,
    'IgnorePublicAcls': False,
    'BlockPublicPolicy': False,
    'RestrictPublicBuckets': False,
}
OWNERSHIP = {'Rules': [{'ObjectOwnership': 'ObjectWriter'}]}
CORS = {'CORSRules': [{'AllowedMethods': ['GET'], 'AllowedOrigins': ['*']}]}
ENCRYPTION = {'Rules': [{'ApplyServerSideEncryptionByDefault':
                         {'SSEAlgorithm': 'AES256'}}]}
WEBSITE = {'IndexDocument': {'Suffix': 'index.html'}}
LOGGING = {'LoggingEnabled': {'TargetBucket': BUCKET_NAME,
                              'TargetPrefix': 'log/'}}
REPLICATION = {
    'Role': 'arn:aws:iam::123456789012:role/replication',
    'Rules': [{'ID': 'r', 'Status': 'Enabled', 'Priority': 1,
               'Filter': {'Prefix': ''},
               'DeleteMarkerReplication': {'Status': 'Disabled'},
               'Destination': {'Bucket': 'arn:aws:s3:::other'}}],
}

def assert_denied(fn, also=(), **kwargs):
    try:
        fn(**kwargs)
    except botocore.exceptions.ClientError as e:
        code = e.response['Error']['Code']
        assert code == 'AccessDenied' or code in also, (
            '%s: expected AccessDenied, got %r' % (fn.__name__, code))
        return
    raise AssertionError('%s: expected AccessDenied' % fn.__name__)

# website needs rgw_enable_static_website and replication needs a sync
# policy, without them rgw answers MethodNotAllowed before the lock check
NOT_ENABLED = ('MethodNotAllowed',)

def admin_locked():
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}')
    return json.loads(out)['admin_locked']

def cleanup(client):
    try:
        versions = client.list_object_versions(Bucket=BUCKET_NAME)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            return
        raise
    for v in versions.get('Versions', []) + versions.get('DeleteMarkers', []):
        client.put_object_legal_hold(Bucket=BUCKET_NAME, Key=v['Key'],
                                     VersionId=v['VersionId'],
                                     LegalHold={'Status': 'OFF'})
        client.delete_object(Bucket=BUCKET_NAME, Key=v['Key'],
                             VersionId=v['VersionId'],
                             BypassGovernanceRetention=True)
    client.delete_bucket(Bucket=BUCKET_NAME)

def main():
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries={
        'total_max_attempts': 1,
    }))
    client = connection.meta.client
    cleanup(client)

    bucket = connection.create_bucket(Bucket=BUCKET_NAME,
                                      ObjectLockEnabledForBucket=True)
    client.put_object_lock_configuration(Bucket=BUCKET_NAME,
                                         ObjectLockConfiguration=OBJECT_LOCK)
    put_objects(bucket, ['obj1'])
    obj1_version = client.head_object(Bucket=BUCKET_NAME,
                                      Key='obj1')['VersionId']

    # TESTCASE 'bucket stats reports admin_locked=false by default'
    log.debug('TEST: bucket stats reports admin_locked=false by default\n')
    assert admin_locked() is False

    # TESTCASE 'admin-lock blocks every change to the bucket for the owner'
    log.debug('TEST: admin-lock blocks every change to the bucket for the owner\n')
    exec_cmd(f'radosgw-admin bucket admin-lock --bucket {BUCKET_NAME}')
    assert admin_locked() is True

    b = {'Bucket': BUCKET_NAME}
    assert_denied(client.put_bucket_policy, Policy=POLICY, **b)
    assert_denied(client.delete_bucket_policy, **b)
    assert_denied(client.put_bucket_acl, ACL='public-read', **b)
    assert_denied(client.put_bucket_versioning,
                  VersioningConfiguration={'Status': 'Suspended'}, **b)
    assert_denied(client.put_bucket_lifecycle_configuration,
                  LifecycleConfiguration=LIFECYCLE, **b)
    assert_denied(client.delete_bucket_lifecycle, **b)
    assert_denied(client.put_object_lock_configuration,
                  ObjectLockConfiguration=SHORTER_OBJECT_LOCK, **b)
    assert_denied(client.put_bucket_tagging, Tagging=TAGGING, **b)
    assert_denied(client.delete_bucket_tagging, **b)
    assert_denied(client.put_public_access_block,
                  PublicAccessBlockConfiguration=PUBLIC_ACCESS_BLOCK, **b)
    assert_denied(client.delete_public_access_block, **b)
    assert_denied(client.put_bucket_ownership_controls,
                  OwnershipControls=OWNERSHIP, **b)
    assert_denied(client.delete_bucket_ownership_controls, **b)
    assert_denied(client.put_bucket_cors, CORSConfiguration=CORS, **b)
    assert_denied(client.delete_bucket_cors, **b)
    assert_denied(client.put_bucket_encryption,
                  ServerSideEncryptionConfiguration=ENCRYPTION, **b)
    assert_denied(client.delete_bucket_encryption, **b)
    assert_denied(client.put_bucket_website, also=NOT_ENABLED,
                  WebsiteConfiguration=WEBSITE, **b)
    assert_denied(client.delete_bucket_website, also=NOT_ENABLED, **b)
    assert_denied(client.put_bucket_request_payment,
                  RequestPaymentConfiguration={'Payer': 'Requester'}, **b)
    assert_denied(client.put_bucket_logging,
                  BucketLoggingStatus=LOGGING, **b)
    assert_denied(client.put_bucket_notification_configuration,
                  NotificationConfiguration={}, **b)
    assert_denied(client.put_bucket_replication, also=NOT_ENABLED,
                  ReplicationConfiguration=REPLICATION, **b)
    assert_denied(client.delete_bucket_replication, also=NOT_ENABLED, **b)
    assert_denied(client.delete_bucket, **b)

    # TESTCASE 'admin-lock blocks changes to object retention, legal hold and acl'
    log.debug('TEST: admin-lock blocks changes to object retention, legal hold and acl\n')
    o = {'Bucket': BUCKET_NAME, 'Key': 'obj1', 'VersionId': obj1_version}
    soon = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
    assert_denied(client.put_object_retention,
                  Retention={'Mode': 'GOVERNANCE', 'RetainUntilDate': soon},
                  BypassGovernanceRetention=True, **o)
    assert_denied(client.put_object_legal_hold,
                  LegalHold={'Status': 'ON'}, **o)
    assert_denied(client.put_object_acl, ACL='public-read', **o)
    assert_denied(client.delete_object, BypassGovernanceRetention=True, **o)
    out = client.delete_objects(Bucket=BUCKET_NAME, BypassGovernanceRetention=True,
                                Delete={'Objects': [{'Key': 'obj1',
                                                     'VersionId': obj1_version}]})
    errors = [e['Code'] for e in out.get('Errors', [])]
    assert errors == ['AccessDenied'] and not out.get('Deleted'), out

    # TESTCASE 'admin-lock does not affect object reads and writes'
    log.debug('TEST: admin-lock does not affect object reads and writes\n')
    put_objects(bucket, ['obj2'])
    client.put_object(Bucket=BUCKET_NAME, Key='obj3', Body=b'x',
                      ObjectLockMode='GOVERNANCE',
                      ObjectLockRetainUntilDate=soon)
    keys = sorted(o.key for o in bucket.objects.all())
    assert keys == ['obj1', 'obj2', 'obj3'], keys
    client.get_object(Bucket=BUCKET_NAME, Key='obj1')['Body'].read()

    # TESTCASE 'admin-unlock lets the owner change the bucket again'
    log.debug('TEST: admin-unlock lets the owner change the bucket again\n')
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {BUCKET_NAME}')
    assert admin_locked() is False
    client.put_bucket_policy(Policy=POLICY, **b)
    client.delete_bucket_policy(**b)
    client.put_bucket_lifecycle_configuration(LifecycleConfiguration=LIFECYCLE, **b)
    client.delete_bucket_lifecycle(**b)
    client.put_bucket_tagging(Tagging=TAGGING, **b)
    client.delete_bucket_tagging(**b)
    client.put_bucket_cors(CORSConfiguration=CORS, **b)
    client.delete_bucket_cors(**b)
    client.put_object_legal_hold(LegalHold={'Status': 'ON'}, **o)
    client.put_object_legal_hold(LegalHold={'Status': 'OFF'}, **o)
    client.delete_object(BypassGovernanceRetention=True, **o)

    cleanup(client)

if __name__ == '__main__':
    main()
    log.info('Completed bucket admin-lock tests')
