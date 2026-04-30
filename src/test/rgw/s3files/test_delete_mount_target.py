"""Conformance tests for DeleteMountTarget.

Smithy reference: com.amazonaws.s3files#DeleteMountTarget.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ValidationException.
"""

import pytest

from . import errors


@pytest.mark.conformance
def test_delete_existing(s3files_client, test_file_system, test_zone_id):
    """Create + delete + assert subsequent get returns NotFound."""
    mt = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_zone_id,
    )
    mt_id = mt['mountTargetId']
    s3files_client.delete_mount_target(mountTargetId=mt_id)
    with pytest.raises(s3files_client.exceptions.ResourceNotFoundException):
        s3files_client.get_mount_target(mountTargetId=mt_id)


@pytest.mark.conformance
def test_delete_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.delete_mount_target(mountTargetId="fsmt-no-such-9z9z9z")
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.MOUNT_TARGET_NOT_FOUND, err
