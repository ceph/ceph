#!/usr/bin/env python3

import logging as log
import json
import datetime
import os
import threading
import time
import urllib.parse
import botocore
import requests
import urllib3
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from common import exec_cmd, create_user, boto_connect, put_objects
from botocore.config import Config

"""
Tests radosgw-admin bucket admin-lock/admin-unlock commands.
"""

USER = 'admin-lock-tester'
DISPLAY_NAME = 'Bucket Admin Lock Testing'
ACCESS_KEY = '0555b35654ad1656d804'
SECRET_KEY = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
ADMIN_USER = 'admin-lock-admin'
ADMIN_ACCESS_KEY = 'ADMINLOCKADMIN000001'
ADMIN_SECRET_KEY = 'adminlockadminsecret00000000000000000001'
SYSTEM_USER = 'admin-lock-system'
SYSTEM_ACCESS_KEY = 'ADMINLOCKSYSTEM00001'
SYSTEM_SECRET_KEY = 'adminlocksystemsecret0000000000000000001'
CAPS_USER = 'admin-lock-caps'
CAPS_ACCESS_KEY = 'ADMINLOCKCAPS0000001'
CAPS_SECRET_KEY = 'adminlockcapssecret000000000000000000001'
RATE_USER = 'admin-lock-ratelimit'
RATE_ACCESS_KEY = 'ADMINLOCKRATE0000001'
RATE_SECRET_KEY = 'adminlockratesecret000000000000000000001'
USERS_CAPS_USER = 'admin-lock-users-caps'
USERS_CAPS_ACCESS_KEY = 'ADMINLOCKUSERS000001'
USERS_CAPS_SECRET_KEY = 'adminlockuserssecret00000000000000000001'
PURGE_USER = 'admin-lock-purge'
PURGE_ACCESS_KEY = 'ADMINLOCKPURGE000001'
PURGE_SECRET_KEY = 'adminlockpurgesecret00000000000000000001'
PURGE_BUCKET = 'admin-lock-purge-bucket'
TENANT = 'adminlocktenant'
TENANT_USER = 'admin-lock-tenant-user'
TENANT_UID = f'{TENANT}${TENANT_USER}'
TENANT_ACCESS_KEY = 'ADMINLOCKTENANT00001'
TENANT_SECRET_KEY = 'adminlocktenantsecret0000000000000000001'
XT_BUCKET = 'admin-lock-xt'
SRC_TENANT = 'adminlocksrc'
SRC_TENANT_UID = f'{SRC_TENANT}$admin-lock-src-user'
SRC_TENANT_ACCESS_KEY = 'ADMINLOCKSRCTENANT01'
SRC_TENANT_SECRET_KEY = 'adminlocksrctenantsecret0000000000000001'
SWIFT_USER = 'admin-lock-swift'
SWIFT_SUBUSER = f'{SWIFT_USER}:swift'
SWIFT_KEY = 'adminlockswiftsecret000000000000000001'
SWIFT_CONTAINER = 'admin-lock-container'
BUCKET_NAME = 'admin-lock-bucket'
EMPTY_BUCKET = 'admin-lock-empty'
LOG_BUCKET = 'admin-lock-logs'
SRC_BUCKET = 'admin-lock-src'
RENAMED_BUCKET = 'admin-lock-renamed'
MDSEARCH = {'X-Amz-Meta-Search': 'x-amz-meta-foo;string'}
# the rgw/website job sets this: website and replication are configured
# there, so a skip would hide a missing lock check
REQUIRE_FEATURES = os.environ.get('ADMIN_LOCK_REQUIRE_FEATURES') == '1'
# rgw_inject_delay_sec set for this task in the qa yaml
DELAY = 10

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
LOGGING = {'LoggingEnabled': {'TargetBucket': LOG_BUCKET,
                              'TargetPrefix': 'log/'}}
REPLICATION = {
    'Role': 'arn:aws:iam::123456789012:role/replication',
    'Rules': [{'ID': 'r', 'Status': 'Enabled', 'Priority': 1,
               'Filter': {'Prefix': ''},
               'DeleteMarkerReplication': {'Status': 'Disabled'},
               'Destination': {'Bucket': 'arn:aws:s3:::other'}}],
}

def error_code(fn, **kwargs):
    try:
        fn(**kwargs)
    except botocore.exceptions.ClientError as e:
        return e.response['Error']['Code']
    return None

def assert_denied(fn, **kwargs):
    code = error_code(fn, **kwargs)
    assert code == 'AccessDenied', (
        '%s: expected AccessDenied, got %r' % (fn.__name__, code))

def supported(fn, **kwargs):
    # website needs rgw_enable_static_website and replication a zonegroup
    # sync policy. without them rgw answers MethodNotAllowed before any
    # permission or lock check, so there is nothing to test.
    code = error_code(fn, **kwargs)
    if code == 'MethodNotAllowed':
        assert not REQUIRE_FEATURES, f'{fn.__name__}: not enabled in this cluster'
        log.info('SKIP %s: not enabled in this cluster', fn.__name__)
        return False
    # refused while unlocked would make the locked AccessDenied meaningless
    assert code != 'AccessDenied', fn.__name__
    return True

def wait_locked(client, bucket):
    # object checks read the bucket info rgw has cached; wait until rgw has
    # the lock (deleting tags a bucket doesn't have changes nothing)
    for _ in range(30):
        if error_code(client.delete_bucket_tagging, Bucket=bucket) == 'AccessDenied':
            return
        time.sleep(1)
    raise AssertionError(f'{bucket}: lock not seen by rgw')

def bucket_stats():
    return json.loads(exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}'))

def admin_locked():
    return bucket_stats()['admin_locked']

def signed_request(client, method, access_key, secret_key, path, params, headers=None):
    # sign the Host header we send, as is: libraries disagree on whether a
    # default port (:80, :443) belongs in it. https without verification,
    # like boto_connect
    url = client.meta.endpoint_url + path
    if params:
        url += '?' + urllib.parse.urlencode(params)
    headers = dict(headers or {}, Host=urllib.parse.urlsplit(url).netloc)
    req = AWSRequest(method=method, url=url, headers=headers)
    S3SigV4Auth(Credentials(access_key, secret_key), 's3',
                client.meta.region_name or 'us-east-1').add_auth(req)
    return requests.request(method, url, headers=dict(req.headers.items()),
                            verify=False)

def admin_api(client, method, access_key, secret_key, path='/admin/bucket', **params):
    # signed request to the admin ops api, returns the http status
    return signed_request(client, method, access_key, secret_key, path, params).status_code

def forwarded_client(uid, config):
    # what another zone sends to the metadata master: the zone's system key,
    # and the user it acts for in rgwx-uid
    client = boto_connect(SYSTEM_ACCESS_KEY, SYSTEM_SECRET_KEY, config).meta.client
    if uid:
        def add_uid(request, **kwargs):
            sep = '&' if '?' in request.url else '?'
            request.url += sep + urllib.parse.urlencode({'rgwx-uid': uid})
        client.meta.events.register('before-sign.s3', add_uid)
    return client

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

def race(pattern, bucket, setup, request):
    # Holds request() at rgw's delay point `pattern` and locks the bucket
    # meanwhile, so the result shows what the lock check after that point
    # does. An attempt only counts if the request was held there and the lock
    # was in place before it resumed. setup() restores the starting state
    # before every attempt, a failed attempt may have changed it.
    for attempt in range(5):
        # the bucket may not exist yet, setup() creates it
        exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {bucket}', check_retcode=False)
        setup()
        exec_cmd(f'ceph config set client rgw_inject_delay_pattern {pattern}')
        time.sleep(1) # let the config change reach rgw
        result = {}
        def run():
            result['start'] = time.monotonic()
            try:
                result['code'] = request()
            except Exception as e:
                result['error'] = e
            result['end'] = time.monotonic()
        try:
            t = threading.Thread(target=run)
            t.start()
            # time to reach the delay point, also under valgrind
            time.sleep(DELAY / 3)
            exec_cmd(f'radosgw-admin bucket admin-lock --bucket {bucket}')
            locked_at = time.monotonic()
            t.join()
        finally:
            exec_cmd('ceph config rm client rgw_inject_delay_pattern')
        if 'error' in result:
            raise result['error']
        held = result['end'] - result['start'] >= DELAY * 0.9
        if held and locked_at < result['start'] + DELAY:
            return result['code']
        log.info('race %s attempt %d: held=%s, lock %.1fs after start, again',
                 pattern, attempt, held, locked_at - result['start'])
    raise AssertionError(f'race {pattern}: could not set up the race, is rgw_inject_delay_sec set?')

def test_races(client):
    def no_policy():
        error_code(client.delete_bucket_policy, Bucket=BUCKET_NAME)
    def logging_on():
        client.put_bucket_logging(Bucket=BUCKET_NAME, BucketLoggingStatus=LOGGING)
    def empty_bucket():
        if error_code(client.head_bucket, Bucket=EMPTY_BUCKET) is not None:
            client.create_bucket(Bucket=EMPTY_BUCKET)

    # a config write racing with the lock: the lock lands after the request
    # loaded the bucket, the write collides and is checked again
    code = race('delay_raced_bucket_write', BUCKET_NAME, no_policy,
                lambda: error_code(client.put_bucket_policy, Bucket=BUCKET_NAME, Policy=POLICY))
    assert code == 'AccessDenied', code
    assert error_code(client.get_bucket_policy, Bucket=BUCKET_NAME) == 'NoSuchBucketPolicy'

    # turning logging off racing with the lock: the lock lands before the op
    # loads the bucket itself, so only the check before the write sees it
    code = race('delay_bucket_logging_load', BUCKET_NAME, logging_on,
                lambda: error_code(client.put_bucket_logging, Bucket=BUCKET_NAME,
                                   BucketLoggingStatus={}))
    assert code == 'AccessDenied', code
    assert 'LoggingEnabled' in client.get_bucket_logging(Bucket=BUCKET_NAME)
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {BUCKET_NAME}')
    client.put_bucket_logging(Bucket=BUCKET_NAME, BucketLoggingStatus={})

    # deleting an empty bucket racing with the lock
    code = race('delay_delete_bucket', EMPTY_BUCKET, empty_bucket,
                lambda: error_code(client.delete_bucket, Bucket=EMPTY_BUCKET))
    assert code == 'AccessDenied', code
    client.head_bucket(Bucket=EMPTY_BUCKET)
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {EMPTY_BUCKET}')
    client.delete_bucket(Bucket=EMPTY_BUCKET)

def test_user_purge(client, config):
    # removing the owner with purge-data would delete a locked bucket. a
    # caller with only users caps may not; unlocked, it may.
    users_caps = (USERS_CAPS_ACCESS_KEY, USERS_CAPS_SECRET_KEY)
    def purge():
        return admin_api(client, 'DELETE', *users_caps, path='/admin/user',
                         uid=PURGE_USER, **{'purge-data': 'true'})
    create_user(PURGE_USER, 'admin lock purge', PURGE_ACCESS_KEY, PURGE_SECRET_KEY)
    owner = boto_connect(PURGE_ACCESS_KEY, PURGE_SECRET_KEY, config).meta.client
    owner.create_bucket(Bucket=PURGE_BUCKET)
    owner.put_object(Bucket=PURGE_BUCKET, Key='obj', Body=b'data')

    exec_cmd(f'radosgw-admin bucket admin-lock --bucket {PURGE_BUCKET}')
    assert purge() == 403
    owner.head_object(Bucket=PURGE_BUCKET, Key='obj')
    exec_cmd(f'radosgw-admin user info --uid {PURGE_USER}')

    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {PURGE_BUCKET}')
    assert purge() == 200
    assert error_code(client.head_bucket, Bucket=PURGE_BUCKET) == '404'

TEST_USERS = (ADMIN_USER, SYSTEM_USER, CAPS_USER, RATE_USER, USERS_CAPS_USER)

def test_swift(client):
    # container metadata and acls are locked; a PUT of the container that
    # changes nothing still works, swift clients do that before uploading
    exec_cmd(f'radosgw-admin user create --uid {SWIFT_USER} --display-name swift')
    exec_cmd(f'radosgw-admin subuser create --uid {SWIFT_USER} --subuser {SWIFT_SUBUSER} '
             f'--access=full --key-type=swift --secret {SWIFT_KEY}')
    auth = requests.get(client.meta.endpoint_url + '/auth/1.0', verify=False,
                        headers={'X-Auth-User': SWIFT_SUBUSER, 'X-Auth-Key': SWIFT_KEY})
    assert auth.status_code in (200, 204), auth.status_code
    url = auth.headers['X-Storage-Url'] + '/' + SWIFT_CONTAINER
    token = {'X-Auth-Token': auth.headers['X-Auth-Token']}
    def swift(method, **headers):
        return requests.request(method, url, headers=dict(token, **headers),
                                verify=False).status_code
    meta = {'X-Container-Meta-A': 'b', 'Content-Type': 'text/plain'}
    assert swift('PUT', **meta) in (201, 202)
    exec_cmd(f'radosgw-admin bucket admin-lock --bucket {SWIFT_CONTAINER}')
    for _ in range(30): # wait until rgw has the lock
        if swift('POST', **{'X-Container-Meta-A': 'c'}) == 403:
            break
        time.sleep(1)
    assert swift('POST', **{'X-Container-Meta-A': 'c'}) == 403
    assert swift('POST', **{'X-Container-Read': '.r:*'}) == 403
    assert swift('PUT', **{'X-Container-Meta-A': 'c'}) == 403
    assert swift('PUT', **{'X-Remove-Container-Meta-A': 'x'}) == 403
    assert swift('PUT', **meta) in (201, 202)
    assert swift('PUT') in (201, 202)
    assert swift('DELETE') == 403
    head = requests.head(url, headers=token, verify=False)
    assert head.headers.get('X-Container-Meta-A') == 'b', head.headers
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {SWIFT_CONTAINER}')
    assert swift('POST', **{'X-Container-Meta-A': 'c'}) == 204
    assert swift('DELETE') == 204
    exec_cmd(f'radosgw-admin user rm --uid {SWIFT_USER} --purge-data')

def tenant_user(uid, access_key, secret_key, config):
    # quoted: common.create_user would let the shell expand the '$'
    exec_cmd(f"radosgw-admin user create --uid '{uid}' --display-name tenant "
             f"--access-key {access_key} --secret {secret_key}")
    return boto_connect(access_key, secret_key, config).meta.client

def test_cross_tenant_link(client, config, caps):
    # linking a bucket from one tenant for a user in another replaces that
    # tenant's bucket of the same name; refused when that one is locked
    tenant_client = tenant_user(TENANT_UID, TENANT_ACCESS_KEY, TENANT_SECRET_KEY, config)
    src_client = tenant_user(SRC_TENANT_UID, SRC_TENANT_ACCESS_KEY, SRC_TENANT_SECRET_KEY, config)
    tenant_client.create_bucket(Bucket=XT_BUCKET)
    tenant_client.put_object(Bucket=XT_BUCKET, Key='audit', Body=b'audit')
    src_client.create_bucket(Bucket=XT_BUCKET)
    exec_cmd(f'radosgw-admin bucket admin-lock --bucket {TENANT}/{XT_BUCKET}')
    before = json.loads(exec_cmd(f'radosgw-admin bucket stats --bucket {TENANT}/{XT_BUCKET}'))
    assert admin_api(client, 'PUT', *caps, bucket=f'{SRC_TENANT}/{XT_BUCKET}',
                     uid=TENANT_UID) == 403
    after = json.loads(exec_cmd(f'radosgw-admin bucket stats --bucket {TENANT}/{XT_BUCKET}'))
    assert after['id'] == before['id'] and after['admin_locked'] is True, after
    tenant_client.head_object(Bucket=XT_BUCKET, Key='audit')
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {TENANT}/{XT_BUCKET}')
    for uid in (TENANT_UID, SRC_TENANT_UID):
        exec_cmd(f"radosgw-admin user rm --uid '{uid}' --purge-data")

def main():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)
    create_user(ADMIN_USER, 'admin lock admin', ADMIN_ACCESS_KEY, ADMIN_SECRET_KEY)
    exec_cmd(f'radosgw-admin user modify --uid {ADMIN_USER} --admin')
    create_user(SYSTEM_USER, 'admin lock system', SYSTEM_ACCESS_KEY, SYSTEM_SECRET_KEY)
    exec_cmd(f'radosgw-admin user modify --uid {SYSTEM_USER} --system')
    create_user(CAPS_USER, 'admin lock caps', CAPS_ACCESS_KEY, CAPS_SECRET_KEY)
    exec_cmd(f'radosgw-admin caps add --uid {CAPS_USER} --caps "buckets=*"')
    create_user(RATE_USER, 'admin lock ratelimit', RATE_ACCESS_KEY, RATE_SECRET_KEY)
    exec_cmd(f'radosgw-admin caps add --uid {RATE_USER} --caps "ratelimit=*"')
    create_user(USERS_CAPS_USER, 'admin lock users caps', USERS_CAPS_ACCESS_KEY,
                USERS_CAPS_SECRET_KEY)
    exec_cmd(f'radosgw-admin caps add --uid {USERS_CAPS_USER} --caps "users=*"')

    config = Config(retries={'total_max_attempts': 1})
    connection = boto_connect(ACCESS_KEY, SECRET_KEY, config)
    client = connection.meta.client
    # a failed earlier run may have left things locked
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {BUCKET_NAME}', check_retcode=False)
    cleanup(client)
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {PURGE_BUCKET}', check_retcode=False)
    exec_cmd(f'radosgw-admin user rm --uid {PURGE_USER} --purge-data', check_retcode=False)
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {SWIFT_CONTAINER}', check_retcode=False)
    exec_cmd(f'radosgw-admin user rm --uid {SWIFT_USER} --purge-data', check_retcode=False)
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {TENANT}/{XT_BUCKET}', check_retcode=False)
    for uid in (TENANT_UID, SRC_TENANT_UID):
        exec_cmd(f"radosgw-admin user rm --uid '{uid}' --purge-data", check_retcode=False)
    for name in (EMPTY_BUCKET, LOG_BUCKET, SRC_BUCKET, RENAMED_BUCKET, XT_BUCKET):
        exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {name}', check_retcode=False)
        exec_cmd(f'radosgw-admin bucket rm --bucket {name} --purge-objects',
                 check_retcode=False)
    client.create_bucket(Bucket=LOG_BUCKET)
    client.create_bucket(Bucket=SRC_BUCKET)
    # the logging target has to let the logging service write to it
    client.put_bucket_policy(Bucket=LOG_BUCKET, Policy=json.dumps({
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'Service': 'logging.s3.amazonaws.com'},
            'Action': 's3:PutObject',
            'Resource': f'arn:aws:s3:::{LOG_BUCKET}/*',
        }],
    }))

    bucket = connection.create_bucket(Bucket=BUCKET_NAME,
                                      ObjectLockEnabledForBucket=True)
    client.put_object_lock_configuration(Bucket=BUCKET_NAME,
                                         ObjectLockConfiguration=OBJECT_LOCK)
    put_objects(bucket, ['obj1'])
    obj1_version = client.head_object(Bucket=BUCKET_NAME,
                                      Key='obj1')['VersionId']

    # ACCESS_KEY is the standard vstart key, so the owner may be an existing
    # user and not USER. forwarded requests and the admin api need the real uid.
    owner = bucket_stats()['owner']

    b = {'Bucket': BUCKET_NAME}
    # probe each website/replication call while unlocked, undo what a
    # successful probe set, and only assert the lock for supported calls
    probes = [(client.put_bucket_website, {'WebsiteConfiguration': WEBSITE}),
              (client.delete_bucket_website, {}),
              (client.put_bucket_replication, {'ReplicationConfiguration': REPLICATION}),
              (client.delete_bucket_replication, {})]
    enabled = {fn.__name__ for fn, kw in probes if supported(fn, **kw, **b)}
    error_code(client.delete_bucket_website, **b)
    error_code(client.delete_bucket_replication, **b)

    # TESTCASE 'unlocked: forwarded owner requests and caps admin calls work'
    log.debug('TEST: unlocked: forwarded owner requests and caps admin calls work\n')
    caps = (CAPS_ACCESS_KEY, CAPS_SECRET_KEY)
    forwarded_client(owner, config).put_bucket_policy(Policy=POLICY, **b)
    client.delete_bucket_policy(**b)
    assert admin_api(client, 'PUT', *caps, bucket=BUCKET_NAME, sync='false') == 200
    assert admin_api(client, 'PUT', *caps, bucket=BUCKET_NAME, sync='true') == 200
    assert admin_api(client, 'POST', *caps, bucket=BUCKET_NAME, uid=owner) == 200
    assert admin_api(client, 'PUT', *caps, bucket=BUCKET_NAME, uid=owner) == 200
    assert admin_api(client, 'PUT', *caps, quota='', uid=owner, bucket=BUCKET_NAME,
                     **{'max-size': '-1', 'enabled': 'false'}) == 200
    rate = (RATE_ACCESS_KEY, RATE_SECRET_KEY)
    assert admin_api(client, 'POST', *rate, path='/admin/ratelimit', bucket=BUCKET_NAME,
                     **{'ratelimit-scope': 'bucket', 'enabled': 'false'}) == 200
    client.head_bucket(**b)
    # renaming a bucket by linking it under a new name works for buckets caps
    assert admin_api(client, 'PUT', *caps, bucket=SRC_BUCKET, uid=owner,
                     **{'new-bucket-name': RENAMED_BUCKET}) == 200
    client.head_bucket(Bucket=RENAMED_BUCKET)
    # metadata search can be configured and removed
    own = (ACCESS_KEY, SECRET_KEY)
    assert signed_request(client, 'POST', *own, f'/{BUCKET_NAME}', {'mdsearch': ''},
                          MDSEARCH).status_code == 200
    assert signed_request(client, 'DELETE', *own, f'/{BUCKET_NAME}',
                          {'mdsearch': ''}).status_code in (200, 204)
    assert signed_request(client, 'POST', *own, f'/{BUCKET_NAME}', {'mdsearch': ''},
                          MDSEARCH).status_code == 200

    # TESTCASE 'bucket stats reports admin_locked=false by default'
    log.debug('TEST: bucket stats reports admin_locked=false by default\n')
    assert admin_locked() is False

    # TESTCASE 'writes racing with admin-lock are not applied'
    log.debug('TEST: writes racing with admin-lock are not applied\n')
    test_races(client)

    # TESTCASE 'admin-lock blocks every change to the bucket for the owner'
    log.debug('TEST: admin-lock blocks every change to the bucket for the owner\n')
    exec_cmd(f'radosgw-admin bucket admin-lock --bucket {BUCKET_NAME}')
    assert admin_locked() is True
    wait_locked(client, BUCKET_NAME)

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
    for fn, kw in probes:
        if fn.__name__ in enabled:
            assert_denied(fn, **kw, **b)
    assert_denied(client.put_bucket_request_payment,
                  RequestPaymentConfiguration={'Payer': 'Requester'}, **b)
    assert_denied(client.put_bucket_logging,
                  BucketLoggingStatus=LOGGING, **b)
    assert_denied(client.put_bucket_notification_configuration,
                  NotificationConfiguration={}, **b)
    assert_denied(client.delete_bucket, **b)
    assert signed_request(client, 'POST', *own, f'/{BUCKET_NAME}', {'mdsearch': ''},
                          {'X-Amz-Meta-Search': 'x-amz-meta-bar;string'}).status_code == 403
    assert signed_request(client, 'DELETE', *own, f'/{BUCKET_NAME}',
                          {'mdsearch': ''}).status_code == 403
    mdsearch = signed_request(client, 'GET', *own, f'/{BUCKET_NAME}', {'mdsearch': ''})
    assert 'x-amz-meta-foo' in mdsearch.text, mdsearch.text

    # TESTCASE 'admin-lock and admin-unlock only take the current bucket id'
    log.debug('TEST: admin-lock and admin-unlock only take the current bucket id\n')
    _, ret = exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {BUCKET_NAME} '
                      '--bucket-id not-the-current-id', check_retcode=False)
    assert ret != 0
    assert admin_locked() is True
    bucket_id = bucket_stats()['id']
    exec_cmd(f'radosgw-admin bucket admin-unlock --bucket {BUCKET_NAME} --bucket-id {bucket_id}')
    assert admin_locked() is False
    exec_cmd(f'radosgw-admin bucket admin-lock --bucket {BUCKET_NAME} --bucket-id {bucket_id}')
    assert admin_locked() is True

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

    # TESTCASE 'a request forwarded for the owner is judged as the owner'
    log.debug('TEST: a request forwarded for the owner is judged as the owner\n')
    assert_denied(forwarded_client(owner, config).put_bucket_policy,
                  Policy=POLICY, **b)
    forwarded_client(ADMIN_USER, config).put_bucket_policy(Policy=POLICY, **b)
    forwarded_client(None, config).delete_bucket_policy(**b)

    # TESTCASE 'admin ops api: buckets caps alone do not get past the lock'
    log.debug('TEST: admin ops api: buckets caps alone do not get past the lock\n')
    assert admin_api(client, 'DELETE', *caps, bucket=BUCKET_NAME) == 403
    assert admin_api(client, 'DELETE', *caps, bucket=BUCKET_NAME,
                     object='obj1') == 403
    assert admin_api(client, 'POST', *caps, bucket=BUCKET_NAME, uid=owner) == 403
    assert admin_api(client, 'PUT', *caps, bucket=BUCKET_NAME, uid=CAPS_USER) == 403
    assert admin_api(client, 'PUT', *caps, quota='', uid=owner, bucket=BUCKET_NAME,
                     **{'max-size': '1', 'enabled': 'true'}) == 403
    assert admin_api(client, 'POST', *rate, path='/admin/ratelimit', bucket=BUCKET_NAME,
                     **{'ratelimit-scope': 'bucket', 'max-write-ops': '1',
                        'enabled': 'true'}) == 403
    assert not bucket_stats().get('bucket_quota', {}).get('enabled')
    assert admin_api(client, 'PUT', *caps, bucket=BUCKET_NAME,
                     sync='false') == 403
    # linking another bucket under the locked bucket's name would replace it
    assert admin_api(client, 'PUT', *caps, bucket=RENAMED_BUCKET, uid=owner,
                     **{'new-bucket-name': BUCKET_NAME}) == 403
    # regression guard: naming the locked bucket's own instance is refused
    assert admin_api(client, 'PUT', *caps, bucket=BUCKET_NAME, uid=owner,
                     **{'bucket-id': bucket_stats()['id']}) == 403
    client.head_bucket(Bucket=RENAMED_BUCKET)
    assert admin_locked() is True
    client.head_object(Bucket=BUCKET_NAME, Key='obj1')
    test_cross_tenant_link(client, config, caps)

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
    forwarded_client(owner, config).put_bucket_policy(Policy=POLICY, **b)
    client.delete_bucket_policy(**b)
    client.delete_object(BypassGovernanceRetention=True, **o)
    assert signed_request(client, 'DELETE', *own, f'/{BUCKET_NAME}',
                          {'mdsearch': ''}).status_code in (200, 204)

    # TESTCASE 'swift container metadata and acls are locked'
    log.debug('TEST: swift container metadata and acls are locked\n')
    test_swift(client)

    # TESTCASE 'users caps can't delete a locked bucket by purging its owner'
    log.debug("TEST: users caps can't delete a locked bucket by purging its owner\n")
    test_user_purge(client, config)

    cleanup(client)
    connection.Bucket(LOG_BUCKET).objects.all().delete()
    client.delete_bucket(Bucket=LOG_BUCKET)
    client.delete_bucket(Bucket=RENAMED_BUCKET)
    for uid in TEST_USERS:
        exec_cmd(f'radosgw-admin user rm --uid {uid}', check_retcode=False)

if __name__ == '__main__':
    main()
    log.info('Completed bucket admin-lock tests')
