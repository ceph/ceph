#!/usr/bin/env python3

import logging as log
import json
import botocore
from common import exec_cmd, create_user, boto_connect, put_objects, create_unlinked_objects, create_orphaned_list_entries
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

def verify_orphaned_entries_exist(orphaned_objs, context_msg):
    """
    Verify that orphaned list entries still exist in the bucket index.
    Used to confirm that various bucket check commands do NOT fix orphaned entries.
    """
    for key, instance in orphaned_objs:
        out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME} --object {key}')
        entries = json.loads(out.replace(b'\x80', b'0x80'))
        entry_types = [e['type'] for e in entries]
        assert 'plain' in entry_types, \
            f'orphaned entry {key} should still have plain entry {context_msg}'
        assert 'instance' not in entry_types, \
            f'orphaned entry {key} should still be missing instance entry {context_msg}'


def test_bucket_check_stats(connection, bucket):
    """Test that bucket check --fix correctly recalculates bucket stats."""
    log.debug('TEST: recalculated bucket check stats are correct\n')

    null_version_keys = ['a', 'z']
    null_version_objs = put_objects(bucket, null_version_keys)

    connection.BucketVersioning(BUCKET_NAME).enable()

    ok_keys = ['a', 'b', 'c', 'd']
    ok_objs = put_objects(bucket, ok_keys)

    # TESTCASE 'recalculated bucket check stats are correct'
    exec_cmd(f'radosgw-admin bucket check --fix --bucket {BUCKET_NAME}')
    out = exec_cmd(f'radosgw-admin bucket stats --bucket {BUCKET_NAME}')
    json_out = json.loads(out)
    log.debug(json_out['usage'])
    assert json_out['usage']['rgw.main']['num_objects'] == 6

    return null_version_keys, null_version_objs, ok_keys, ok_objs


def test_bucket_check_unlinked(connection, bucket, ok_keys, ok_objs, null_version_keys, null_version_objs):
    """Test bucket check unlinked - finds instance entries without list entries."""

    unlinked_keys = ['c', 'd', 'e', 'f']

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

    return unlinked_keys, unlinked_objs


def test_bucket_check_olh(connection, bucket, ok_keys, ok_objs, unlinked_keys):
    """Test bucket check olh - finds leftover OLH entries."""

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


def test_null_versions_preserved(connection, bucket, null_version_keys, null_version_objs):
    """Test that bucket check fixes do not affect null version objects."""

    # TESTCASE 'bucket check fixes do not affect null version objects'
    log.debug('TEST: verify that bucket check fixes do not affect null version objects\n')
    for o in null_version_objs:
        connection.ObjectVersion(bucket.name, o[0], 'null').head()
        
    all_versions = list(map(lambda x: (x.key, x.version_id), bucket.object_versions.all()))
    for key in null_version_keys:
        assert (key, 'null') in all_versions


def test_stats_with_unlinked(connection, bucket):
    """Test bucket check stats are correct in the presence of unlinked entries."""

    null_version_keys = ['a', 'z']
    ok_keys = ['a', 'b', 'c', 'd']
    unlinked_keys = ['c', 'd', 'e', 'f']

    # TESTCASE 'bucket check stats are correct in the presence of unlinked entries'
    log.debug('TEST: bucket check stats are correct in the presence of unlinked entries\n')
    bucket.object_versions.all().delete()
    null_version_objs = put_objects(bucket, null_version_keys)

    connection.BucketVersioning(BUCKET_NAME).enable()

    ok_objs = put_objects(bucket, ok_keys)
    unlinked_objs = create_unlinked_objects(connection, bucket, unlinked_keys)
    exec_cmd(f'radosgw-admin bucket check --fix --bucket {BUCKET_NAME}')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --fix --min-age-hours 0 --rgw-olh-pending-timeout-sec 0 --dump-keys')
    json_out = json.loads(out)
    log.info(f'"bucket check unlinked" returned {json_out}, expecting {unlinked_keys}')
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


def test_orphaned_list_entries(connection, bucket):
    """
    Test orphaned list entries (list entry WITHOUT instance entry).

    This is the OPPOSITE of "unlinked" entries:
    - Unlinked: instance entry exists, but list entry is missing
    - Orphaned: list entry exists, but instance entry is missing

    Bug scenario: LC fails with ENOENT when trying to delete a delete marker
    that has a list entry but is missing its corresponding instance entry.

    This test:
    1. Creates orphaned list entries (simulating the corruption)
    2. Verifies existing bucket check commands do NOT fix the issue
    3. Then, verifies that the new bucket check orphan command finds and
       fixes the orphaned delete marker entries, while skipping data objects
       to avoid orphaning RADOS data.
    """
    log.debug('TEST SUITE: Orphaned List Entries (list entry WITHOUT instance entry)')

    # Clean up any existing objects first
    bucket.object_versions.all().delete()

    # Ensure versioning is enabled
    connection.BucketVersioning(BUCKET_NAME).enable()

    # =========================================================================
    # STEP 1: Verify normal objects are not affected by bucket check orphan
    # =========================================================================
    # Create a "sane" versioned object: upload twice, then delete.
    # This creates a non-current data version + current delete marker.
    # bucket check orphan should NOT report or touch these normal entries.
    log.debug('TEST: bucket check orphan does not report normal versioned objects\n')
    sane_key = 'sane-object'
    # Upload twice to create non-current version
    bucket.put_object(Key=sane_key, Body=b'version1')
    bucket.put_object(Key=sane_key, Body=b'version2')
    # Delete to create current delete marker
    bucket.Object(sane_key).delete()

    # Verify the object has proper index entries (plain + instance for each version)
    out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME} --object {sane_key}')
    sane_entries = json.loads(out.replace(b'\x80', b'0x80'))
    sane_entry_types = [e['type'] for e in sane_entries]
    assert 'plain' in sane_entry_types, 'sane object should have plain entries'
    assert 'instance' in sane_entry_types, 'sane object should have instance entries'
    log.info(f'  Created sane object {sane_key} with entry types: {sane_entry_types}')

    # bucket check orphan should not report this sane object
    out = exec_cmd(f'radosgw-admin bucket check orphan --bucket {BUCKET_NAME} --dump-keys')
    json_out = json.loads(out)
    sane_in_output = [e for e in json_out if e.get('name') == sane_key]
    assert len(sane_in_output) == 0, \
        f'bucket check orphan should not report sane object {sane_key}, but found: {sane_in_output}'
    log.info(f'  bucket check orphan: Did not report sane object (as expected)')

    # bucket check orphan --fix should not touch sane object
    log.debug('TEST: bucket check orphan --fix does not touch normal versioned objects\n')
    out = exec_cmd(f'radosgw-admin bucket check orphan --bucket {BUCKET_NAME} --fix --dump-keys')
    json_out = json.loads(out)
    sane_in_output = [e for e in json_out if e.get('name') == sane_key]
    assert len(sane_in_output) == 0, \
        f'bucket check orphan --fix should not report sane object {sane_key}, but found: {sane_in_output}'

    # Verify index entries are still intact after --fix
    out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME} --object {sane_key}')
    sane_entries_after = json.loads(out.replace(b'\x80', b'0x80'))
    assert len(sane_entries_after) == len(sane_entries), \
        f'sane object index entries changed after --fix: before={len(sane_entries)}, after={len(sane_entries_after)}'
    log.info(f'  bucket check orphan --fix: Did not touch sane object (as expected)')

    # Clean up sane object
    for e in sane_entries:
        if e['type'] == 'plain':
            continue  # Will be cleaned up with instance
        instance = e.get('entry', {}).get('instance', '')
        if instance:
            connection.ObjectVersion(bucket.name, sane_key, instance).delete()

    orphaned_keys = ['orphan1', 'orphan2']

    # =========================================================================
    # STEP 2: Create orphaned list entries
    # =========================================================================
    log.debug('TEST: orphaned list entries can be created\n')
    orphaned_objs = create_orphaned_list_entries(connection, bucket, orphaned_keys)
    assert len(orphaned_objs) == len(orphaned_keys), \
        f'Expected {len(orphaned_keys)} orphaned entries, got {len(orphaned_objs)}'
    log.info(f'Successfully created {len(orphaned_objs)} orphaned list entries')

    # =========================================================================
    # STEP 3: Verify the orphaned state
    # =========================================================================
    log.debug('TEST: orphaned list entries appear in bucket listing\n')
    listed_versions = list(bucket.object_versions.all())
    listed_keys = set(ov.key for ov in listed_versions)
    for key in orphaned_keys:
        assert key in listed_keys, f'orphaned key {key} should appear in bucket listing'
    log.info(f'Orphaned keys visible in listing: {orphaned_keys}')

    log.debug('TEST: bi list shows only PLAIN entry for orphaned objects\n')
    for key, instance in orphaned_objs:
        out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME} --object {key}')
        entries = json.loads(out.replace(b'\x80', b'0x80'))
        entry_types = [e['type'] for e in entries]
        assert 'plain' in entry_types, f'orphaned entry {key} should have plain entry'
        assert 'instance' not in entry_types, f'orphaned entry {key} should NOT have instance entry'
        assert 'olh' not in entry_types, f'orphaned entry {key} should NOT have olh entry'
        log.info(f'  {key}: entry types = {entry_types} (expected: only plain)')

    # =========================================================================
    # STEP 4: Verify existing bucket check commands do NOT fix the issue
    # =========================================================================
    log.debug('Verifying existing bucket check commands do NOT fix orphaned entries')

    log.debug('TEST: bucket check --fix does not fix orphaned list entries\n')
    exec_cmd(f'radosgw-admin bucket check --fix --bucket {BUCKET_NAME}')
    verify_orphaned_entries_exist(orphaned_objs, "after bucket check --fix")
    log.info('  bucket check --fix: Did NOT fix orphaned entries (as expected)')

    log.debug('TEST: bucket check --check-objects does not fix orphaned list entries\n')
    out, ret = exec_cmd(f'radosgw-admin bucket check --check-objects --fix --bucket {BUCKET_NAME}', check_retcode=False)
    verify_orphaned_entries_exist(orphaned_objs, "after bucket check --check-objects --fix")
    log.info('  bucket check --check-objects --fix: Did NOT fix orphaned entries (as expected)')

    log.debug('TEST: bucket check unlinked does not find orphaned list entries\n')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --min-age-hours 0 --dump-keys')
    json_out = json.loads(out)
    assert len(json_out) == 0, \
        'bucket check unlinked should not find orphaned list entries (it finds the OPPOSITE problem)'
    log.info('  bucket check unlinked: Found 0 entries (expected - it finds opposite problem)')

    log.debug('TEST: bucket check unlinked --fix does not fix orphaned list entries\n')
    out = exec_cmd(f'radosgw-admin bucket check unlinked --bucket {BUCKET_NAME} --fix --min-age-hours 0 --dump-keys')
    json_out = json.loads(out)
    verify_orphaned_entries_exist(orphaned_objs, "after bucket check unlinked --fix")
    log.info('  bucket check unlinked --fix: Did NOT fix orphaned entries (as expected)')

    log.debug('TEST: bucket check olh does not find orphaned list entries\n')
    out = exec_cmd(f'radosgw-admin bucket check olh --bucket {BUCKET_NAME} --dump-keys')
    json_out = json.loads(out)
    log.info(f'  bucket check olh: Found {len(json_out)} entries')

    log.debug('TEST: bucket check olh --fix does not fix orphaned list entries\n')
    out = exec_cmd(f'radosgw-admin bucket check olh --bucket {BUCKET_NAME} --fix --dump-keys')
    verify_orphaned_entries_exist(orphaned_objs, "after bucket check olh --fix")
    log.info('  bucket check olh --fix: Did NOT fix orphaned entries (as expected)')

    # =========================================================================
    # STEP 5: Test bucket check orphan command with orphaned entries
    # =========================================================================
    log.debug('TEST: bucket check orphan finds orphaned list entries\n')
    out = exec_cmd(f'radosgw-admin bucket check orphan --bucket {BUCKET_NAME} --dump-keys')
    json_out = json.loads(out)
    # bucket check orphan reports delete markers (delete_marker=true) and skipped
    # data objects (action=skipped). Only delete markers are fixed.
    delete_markers = [e for e in json_out if e.get('delete_marker', False) and e.get('action') != 'skipped']
    skipped_data_objs = [e for e in json_out if e.get('action') == 'skipped']
    log.info(f'  bucket check orphan: Found {len(delete_markers)} delete markers, {len(skipped_data_objs)} skipped data objects')

    assert len(delete_markers) == len(orphaned_keys), \
        f'bucket check orphan should find {len(orphaned_keys)} delete marker orphans, found {len(delete_markers)}'

    # Verify delete markers match our orphaned objects
    found_keys = set((e['name'], e['instance']) for e in delete_markers)
    for key, instance in orphaned_objs:
        assert (key, instance) in found_keys, f'orphaned entry {key}:{instance} not found by bucket check orphan'

    log.debug('TEST: bucket check orphan --fix removes orphaned delete markers\n')
    out = exec_cmd(f'radosgw-admin bucket check orphan --bucket {BUCKET_NAME} --fix --dump-keys')
    json_out = json.loads(out)
    fixed_delete_markers = [e for e in json_out if e.get('delete_marker', False) and e.get('action') != 'skipped']
    assert len(fixed_delete_markers) == len(orphaned_keys), \
        f'bucket check orphan --fix should report {len(orphaned_keys)} fixed delete markers, reported {len(fixed_delete_markers)}'
    log.info(f'  bucket check orphan --fix: Removed {len(fixed_delete_markers)} orphaned delete markers')

    # Verify orphaned delete marker entries are removed after fix.
    # Data objects (exists=true) are intentionally skipped to avoid orphaning RADOS data.
    # OLH markers (flags=8, empty instance) may also remain.
    log.debug('TEST: orphaned delete marker entries are removed after fix\n')
    for key, instance in orphaned_objs:
        out = exec_cmd(f'radosgw-admin bi list --bucket {BUCKET_NAME} --object {key}')
        entries = json.loads(out.replace(b'\x80', b'0x80'))
        # Filter to delete marker entries (exists=false, non-empty instance)
        delete_marker_entries = [e for e in entries
                                 if e.get('entry', {}).get('instance', '')
                                 and not e.get('entry', {}).get('exists', True)]
        assert len(delete_marker_entries) == 0, \
            f'orphaned delete marker {key}:{instance} should be removed after bucket check orphan --fix'
    log.info('  Verified: All orphaned delete marker entries removed')

    # Verify bucket check orphan only finds skipped data objects (no delete markers)
    log.debug('TEST: bucket check orphan finds no delete marker orphans after fix\n')
    out = exec_cmd(f'radosgw-admin bucket check orphan --bucket {BUCKET_NAME} --dump-keys')
    json_out = json.loads(out)
    delete_markers_remaining = [e for e in json_out if e.get('delete_marker', False) and e.get('action') != 'skipped']
    assert len(delete_markers_remaining) == 0, \
        f'bucket check orphan should find 0 delete marker orphans after fix, found {len(delete_markers_remaining)}'
    skipped_entries = [e for e in json_out if e.get('action') == 'skipped']
    log.info(f'  bucket check orphan: Found 0 delete markers, {len(skipped_entries)} skipped data objects (expected)')


def main():
    """
    Execute bucket check command tests.
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

    #
    # Run test suites
    #
    null_version_keys, null_version_objs, ok_keys, ok_objs = test_bucket_check_stats(connection, bucket)

    unlinked_keys, unlinked_objs = test_bucket_check_unlinked(
        connection, bucket, ok_keys, ok_objs, null_version_keys, null_version_objs)

    test_bucket_check_olh(connection, bucket, ok_keys, ok_objs, unlinked_keys)

    test_null_versions_preserved(connection, bucket, null_version_keys, null_version_objs)

    test_stats_with_unlinked(connection, bucket)

    # Test orphaned list entries (the opposite of unlinked entries)
    test_orphaned_list_entries(connection, bucket)

    # Final cleanup - use bucket rm to force-delete even corrupted entries
    log.debug("Deleting bucket {}".format(BUCKET_NAME))
    exec_cmd(f'radosgw-admin bucket rm --bucket {BUCKET_NAME} --purge-objects --yes-i-really-mean-it', check_retcode=False)


main()
log.info("Completed bucket check tests")
