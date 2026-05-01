"""Conformance tests for DeleteMountTarget.

Smithy reference: com.amazonaws.s3files#DeleteMountTarget.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ValidationException.
"""

import pytest

from . import errors, assert_errorcode, NONEXISTENT_MT_ID


@pytest.mark.conformance
@pytest.mark.divergence  # depends on subnet-{zone_hex}
def test_delete_existing(s3files_client, test_file_system, test_subnet_id):
    """Create + delete + assert subsequent get returns NotFound."""
    mt = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_subnet_id,
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
        s3files_client.delete_mount_target(mountTargetId=NONEXISTENT_MT_ID)
    assert_errorcode(exc.value, errors.MOUNT_TARGET_NOT_FOUND)
