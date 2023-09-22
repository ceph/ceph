#!/usr/bin/env python3

import logging as log
import json
import botocore
from common import exec_cmd, create_user, boto_connect, put_objects, create_unlinked_objects
from botocore.config import Config

"""
Tests behavior of radosgw-admin bucket check commands. 
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

""" Constants """
USER = 'check-tester'
DISPLAY_NAME = 'Check Testing'
ACCESS_KEY = 'OJODXSLNX4LUNHQG99PA'
SECRET_KEY = '3l6ffld34qaymfomuh832j94738aie2x4p2o8h6n'
BUCKET_NAME = 'check-bucket'

def main():
    """
    execute bucket check commands
    """
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    connection = boto_connect(ACCESS_KEY, SECRET_KEY, Config(retries = {
        'total_max_attempts': 1,
    }))

    # pre-test cleanup
    try:
        bucket = connection.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.object_versions.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == 'NoSuchBucket':
            raise

    bucket = connection.create_bucket(Bucket=BUCKET_NAME)

    null_version_keys = ['a', 'z']
    null_version_objs = put_objects(bucket, null_version_keys)

    connection.BucketVersioning(BUCKET_NAME).enable()

    ok_keys = ['a', 'b', 'c', 'd']
    unlinked_keys = ['c', 'd', 'e', 'f']
    ok_objs = put_objects(bucket, ok_keys)
    
    # TESTCASE 'recalculated bucket check stats are correct'
    log.debug('TEST: recalculated bucket check stats are correct\n')
    exec_cmd(f'radosgw-admin bucket check --fix --bucket {BUCKET_NAME}')
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}')
    json_out = json.loads(out)
    log.debug(json_out['usage'])
    assert json_out['usage']['rgw.main']['num_objects'] == 6
    
    # TESTCASE 'bucket check unlinked does not report normal entries'
    log.debug('TEST: bucket check unlinked does not report normal entries\n')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --min-age-hours 0 --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == 0

    unlinked_objs = create_unlinked_objects(connection, bucket, unlinked_keys)
    
    # TESTCASE 'bucket check unlinked finds unlistable entries'
    log.debug('TEST: bucket check unlinked finds unlistable entries\n')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --min-age-hours 0 --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == len(unlinked_keys)

    # TESTCASE 'unlinked entries are not listable'
    log.debug('TEST: unlinked entries are not listable\n')
    for ov in bucket.object_versions.all():
        assert (ov.key, ov.version_id) not in unlinked_objs, f'object "{ov.key}:{ov.version_id}" was found in bucket listing'

    # TESTCASE 'GET returns 404 for unlinked entry keys that have no other versions'
    log.debug('TEST: GET returns 404 for unlinked entry keys that have no other versions\n')
    noent_keys = set(unlinked_keys) - set(ok_keys)
    for key in noent_keys:
        try:
            bucket.Object(key).get()
            assert False, 'GET did not return 404 for key={key} with no prior successful PUT'
        except botocore.exceptions.ClientError as e:
            assert e.response['ResponseMetadata']['HTTPStatusCode'] == 404
            
    # TESTCASE 'bucket check unlinked fixes unlistable entries'
    log.debug('TEST: bucket check unlinked fixes unlistable entries\n')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --fix --min-age-hours 0 --rgw-olh-pending-timeout-sec 0 --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == len(unlinked_keys)
    for o in unlinked_objs:
        try:
            connection.ObjectVersion(bucket.name, o[0], o[1]).head()
            assert False, f'head for unlistable object {o[0]}:{o[1]} succeeded after fix'
        except botocore.exceptions.ClientError as e:
            assert e.response['ResponseMetadata']['HTTPStatusCode'] == 404

    # TESTCASE 'bucket check unlinked fix does not affect normal entries'
    log.debug('TEST: bucket check unlinked does not affect normal entries\n')
    all_listable = list(bucket.object_versions.all())
    assert len(all_listable) == len(ok_keys) + len(null_version_keys), 'some normal objects were not accounted for in object listing after unlinked fix'
    for o in ok_objs:
        assert o in map(lambda x: (x.key, x.version_id), all_listable), "normal object not listable after fix"
        connection.ObjectVersion(bucket.name, o[0], o[1]).head()

    # TESTCASE 'bucket check unlinked does not find new unlistable entries after fix'
    log.debug('TEST: bucket check unlinked does not find new unlistable entries after fix\n')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --min-age-hours 0 --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == 0
    
    # for this set of keys we can produce leftover OLH object/entries by
    # deleting the normal object instance since we should already have a leftover
    # pending xattr on the OLH object due to the errors associated with the 
    # prior unlinked entries that were created for the same keys 
    leftover_pending_xattr_keys = set(ok_keys).intersection(unlinked_keys)
    objs_to_delete = filter(lambda x: x[0] in leftover_pending_xattr_keys, ok_objs)
        
    for o in objs_to_delete:
        connection.ObjectVersion(bucket.name, o[0], o[1]).delete()

    for key in leftover_pending_xattr_keys:
        out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME} --object {key}')
        idx_entries = json.loads(out.replace(b'\x80', b'0x80'))
        assert len(idx_entries) > 0, 'failed to create leftover OLH entries for key {key}'
        
    # TESTCASE 'bucket check olh finds leftover OLH entries'
    log.debug('TEST: bucket check olh finds leftover OLH entries\n')
    out = exec_cmd(f'radosgw-admin bucket check olh --bucket {BUCKET_NAME} --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == len(leftover_pending_xattr_keys)

    # TESTCASE 'bucket check olh fixes leftover OLH entries'
    log.debug('TEST: bucket check olh fixes leftover OLH entries\n')
    out = exec_cmd(f'radosgw-admin bucket check olh --bucket {BUCKET_NAME} --fix --rgw-olh-pending-timeout-sec 0 --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == len(leftover_pending_xattr_keys)
    
    for key in leftover_pending_xattr_keys:
        out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME} --object {key}')
        idx_entries = json.loads(out.replace(b'\x80', b'0x80'))
        assert len(idx_entries) == 0, 'index entries still exist for key={key} after olh fix'

    # TESTCASE 'bucket check olh does not find new leftover OLH entries after fix'
    log.debug('TEST: bucket check olh does not find new leftover OLH entries after fix\n')
    out = exec_cmd(f'radosgw-admin bucket check olh --bucket {BUCKET_NAME} --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == 0

    # TESTCASE 'bucket check fixes do not affect null version objects'
    log.debug('TEST: verify that bucket check fixes do not affect null version objects\n')
    for o in null_version_objs:
        connection.ObjectVersion(bucket.name, o[0], 'null').head()
        
    all_versions = list(map(lambda x: (x.key, x.version_id), bucket.object_versions.all()))
    for key in null_version_keys:
        assert (key, 'null') in all_versions

    # TESTCASE 'bucket check stats are correct in the presence of unlinked entries'
    log.debug('TEST: bucket check stats are correct in the presence of unlinked entries\n')
    bucket.object_versions.all().delete()
    null_version_objs = put_objects(bucket, null_version_keys)
    ok_objs = put_objects(bucket, ok_keys)
    unlinked_objs = create_unlinked_objects(connection, bucket, unlinked_keys)
    exec_cmd(f'radosgw-admin bucket check --fix --bucket {BUCKET_NAME}')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --fix --min-age-hours 0 --rgw-olh-pending-timeout-sec 0 --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == len(unlinked_keys)
    bucket.object_versions.all().delete()
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}')
    json_out = json.loads(out)
    log.debug(json_out['usage'])
    assert json_out['usage']['rgw.main']['size'] == 0
    assert json_out['usage']['rgw.main']['num_objects'] == 0
    assert json_out['usage']['rgw.main']['size_actual'] == 0
    assert json_out['usage']['rgw.main']['size_kb'] == 0
    assert json_out['usage']['rgw.main']['size_kb_actual'] == 0
    assert json_out['usage']['rgw.main']['size_kb_utilized'] == 0

    # Clean up
    log.debug("Deleting bucket {}".format(BUCKET_NAME))
    bucket.object_versions.all().delete()
    bucket.delete()

main()
log.info("Completed bucket check tests")
