"""Read-after-write / state-transition tests for the S3 Files API.

These tests exercise multi-op scenarios — sequences of mutations
followed by reads — to catch persistence failures that single-op
shape conformance alone misses (e.g., an op that returns 200 but
doesn't actually persist, or one whose updates are observable on
some accessors but not others).

Single-op round-trip assertions live in the per-op conformance
files alongside the negative cases. This file focuses on
sequences across ops.
"""

import json

import pytest

from . import errors


# ---------------------------------------------------------------- FileSystem


@pytest.mark.read_after_write
def test_file_system_create_get_list_consistency(
    s3files_client, bucket_arn, shared_test_role
):
    """A newly-created FS appears in both Get and List with the
    same fields."""
    created = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
        tags=[{"key": "Name", "value": "raw-test-fs"}],
    )
    fs_id = created['fileSystemId']
    try:
        got = s3files_client.get_file_system(fileSystemId=fs_id)
        listed_entry = next(
            (fs for fs in s3files_client.list_file_systems()['fileSystems']
             if fs['fileSystemId'] == fs_id),
            None,
        )
        assert listed_entry is not None
        for field in ('fileSystemArn', 'bucket', 'roleArn', 'ownerId'):
            assert got[field] == listed_entry[field], field
        assert got.get('name') == listed_entry.get('name') == 'raw-test-fs'
    finally:
        s3files_client.delete_file_system(fileSystemId=fs_id)


@pytest.mark.read_after_write
def test_file_system_delete_invisible_to_list(
    s3files_client, bucket_arn, shared_test_role
):
    """After delete, the FS is gone from both Get and List."""
    fs_id = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
    )['fileSystemId']
    s3files_client.delete_file_system(fileSystemId=fs_id)
    listed_ids = {
        fs['fileSystemId']
        for fs in s3files_client.list_file_systems()['fileSystems']
    }
    assert fs_id not in listed_ids
    with pytest.raises(s3files_client.exceptions.ResourceNotFoundException):
        s3files_client.get_file_system(fileSystemId=fs_id)


# ---------------------------------------------------------------- AccessPoint


@pytest.mark.read_after_write
def test_access_point_under_file_system_visible_via_filtered_list(
    s3files_client, test_file_system
):
    """An AP created on a FS shows up in ListAccessPoints(fs_id)."""
    fs_id = test_file_system['fileSystemId']
    ap = s3files_client.create_access_point(fileSystemId=fs_id)
    try:
        ids = {
            entry['accessPointId']
            for entry in s3files_client.list_access_points(
                fileSystemId=fs_id,
            )['accessPoints']
        }
        assert ap['accessPointId'] in ids
    finally:
        s3files_client.delete_access_point(accessPointId=ap['accessPointId'])


@pytest.mark.read_after_write
def test_access_point_persists_posix_user_and_root_directory(
    s3files_client, test_file_system
):
    fs_id = test_file_system['fileSystemId']
    ap = s3files_client.create_access_point(
        fileSystemId=fs_id,
        posixUser={"uid": 2000, "gid": 2000, "secondaryGids": [3001]},
        rootDirectory={"path": "/scoped"},
    )
    try:
        got = s3files_client.get_access_point(
            accessPointId=ap['accessPointId'],
        )
        assert got['posixUser']['uid'] == 2000
        assert got['posixUser']['gid'] == 2000
        assert got['posixUser']['secondaryGids'] == [3001]
        assert got['rootDirectory']['path'] == "/scoped"
    finally:
        s3files_client.delete_access_point(accessPointId=ap['accessPointId'])


# ---------------------------------------------------------------- MountTarget


@pytest.mark.read_after_write
def test_mount_target_update_visible_to_get(
    s3files_client, test_mount_target
):
    """UpdateMountTarget(securityGroups=...) is visible to a
    subsequent GetMountTarget."""
    mt_id = test_mount_target['mountTargetId']
    # Smithy: ^sg-[0-9a-f]{8,40}$
    sgs = ["sg-0000aaa1", "sg-0000aaa2", "sg-0000aaa3"]
    s3files_client.update_mount_target(
        mountTargetId=mt_id,
        securityGroups=sgs,
    )
    got = s3files_client.get_mount_target(mountTargetId=mt_id)
    assert set(got.get('securityGroups', [])) == set(sgs)


# ---------------------------------------------------------------- Policy


@pytest.mark.read_after_write
def test_policy_replace_observable(s3files_client, test_file_system):
    """Two PutFileSystemPolicy calls; the second replaces (not
    merges with) the first."""
    fs_id = test_file_system['fileSystemId']
    p1 = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Sid": "A", "Effect": "Allow", "Principal": "*",
         "Action": "*", "Resource": "*"}]})
    p2 = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Sid": "B", "Effect": "Deny", "Principal": "*",
         "Action": "*", "Resource": "*"}]})
    s3files_client.put_file_system_policy(fileSystemId=fs_id, policy=p1)
    s3files_client.put_file_system_policy(fileSystemId=fs_id, policy=p2)
    got = s3files_client.get_file_system_policy(fileSystemId=fs_id)
    got_doc = json.loads(got['policy'])
    assert got_doc == json.loads(p2)
    sids = {st.get('Sid') for st in got_doc.get('Statement', [])}
    assert "A" not in sids  # first statement gone
    assert "B" in sids


# ---------------------------------------------------------------- Sync


@pytest.mark.read_after_write
def test_sync_configuration_version_increments_on_put(
    s3files_client, test_file_system
):
    """Successive Puts increment latestVersionNumber."""
    fs_id = test_file_system['fileSystemId']
    rules = [{"prefix": "", "trigger": "ON_FILE_ACCESS",
              "sizeLessThan": 1024 * 1024}]
    expirations = [{"daysAfterLastAccess": 30}]
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        importDataRules=rules,
        expirationDataRules=expirations,
    )
    v1 = s3files_client.get_synchronization_configuration(
        fileSystemId=fs_id,
    )['latestVersionNumber']
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        latestVersionNumber=v1,
        importDataRules=rules,
        expirationDataRules=[{"daysAfterLastAccess": 60}],
    )
    v2 = s3files_client.get_synchronization_configuration(
        fileSystemId=fs_id,
    )['latestVersionNumber']
    assert v2 > v1


# ---------------------------------------------------------------- Tagging


@pytest.fixture(params=["test_file_system", "test_access_point"])
def taggable_arn(request):
    resource = request.getfixturevalue(request.param)
    # Use bare ids rather than ARNs: AWS Smithy ResourceId
    # accepts both, and ARNs in the URL contain `/` characters
    # (encoded as %2F by boto3) that RGW's canonical-URI
    # handler decodes back to `/`, breaking the sigv4
    # signature match.
    return resource.get('fileSystemId') or resource['accessPointId']


@pytest.mark.read_after_write
def test_tag_replace_then_untag_visible_to_list(
    s3files_client, taggable_arn
):
    """Tagging with the same key replaces; untag removes."""
    s3files_client.tag_resource(
        resourceId=taggable_arn,
        tags=[{"key": "phase", "value": "v1"}],
    )
    s3files_client.tag_resource(
        resourceId=taggable_arn,
        tags=[{"key": "phase", "value": "v2"}],
    )
    after_replace = {
        t['key']: t['value']
        for t in s3files_client.list_tags_for_resource(
            resourceId=taggable_arn,
        )['tags']
    }
    assert after_replace.get('phase') == 'v2'

    s3files_client.untag_resource(resourceId=taggable_arn, tagKeys=["phase"])
    after_untag = {
        t['key']
        for t in s3files_client.list_tags_for_resource(
            resourceId=taggable_arn,
        )['tags']
    }
    assert "phase" not in after_untag


# -------------------------------------------------------- cross-resource ----


@pytest.mark.read_after_write
def test_delete_file_system_cleans_dependent_visibility(
    s3files_client, bucket_arn, shared_test_role
):
    """After all dependent APs are removed and FS is deleted, no
    artifacts of the FS remain visible via any list endpoint
    using the FS id."""
    fs_id = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
    )['fileSystemId']

    # Create-then-delete one AP underneath, in case ordering matters.
    ap = s3files_client.create_access_point(fileSystemId=fs_id)
    s3files_client.delete_access_point(accessPointId=ap['accessPointId'])
    s3files_client.delete_file_system(fileSystemId=fs_id)

    # ListAccessPoints with a now-deleted FS should raise NotFound.
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.list_access_points(fileSystemId=fs_id)
    err = exc.value.response
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err
