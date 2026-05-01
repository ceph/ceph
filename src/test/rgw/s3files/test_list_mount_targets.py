"""Conformance tests for ListMountTargets.

Smithy reference: com.amazonaws.s3files#ListMountTargets.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.

ListMountTargets accepts optional fileSystemId / accessPointId
query filters. With neither, it returns all mount targets in
the calling account.
"""

import pytest

from . import errors, assert_errorcode, validation_excs, NONEXISTENT_FS_ID


@pytest.mark.conformance
def test_list_returns_list_field(s3files_client):
    resp = s3files_client.list_mount_targets()
    assert 'mountTargets' in resp
    assert isinstance(resp['mountTargets'], list)


@pytest.mark.conformance
@pytest.mark.divergence  # uses test_mount_target (subnet-{zone_hex})
def test_list_filtered_by_file_system(s3files_client, test_mount_target):
    fs_id = test_mount_target['fileSystemId']
    mt_id = test_mount_target['mountTargetId']
    resp = s3files_client.list_mount_targets(fileSystemId=fs_id)
    ids = {mt['mountTargetId'] for mt in resp['mountTargets']}
    assert ids == {mt_id}


@pytest.mark.conformance
def test_list_filtered_by_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.list_mount_targets(
            fileSystemId=NONEXISTENT_FS_ID,
        )
    assert_errorcode(exc.value, errors.FILE_SYSTEM_NOT_FOUND)


@pytest.mark.conformance
def test_list_max_results_out_of_range(s3files_client):
    """Smithy `@range` is 1..100. Either side may catch."""
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.list_mount_targets(maxResults=10000)
