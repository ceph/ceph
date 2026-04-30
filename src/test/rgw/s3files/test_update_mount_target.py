"""Conformance tests for UpdateMountTarget.

Smithy reference: com.amazonaws.s3files#UpdateMountTarget.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ValidationException.

Update accepts only `securityGroups` (required). RGW v1 stores
the spec change but treats SGs as no-ops at the data plane.
"""

import pytest

from . import errors


@pytest.mark.conformance
def test_update_security_groups(s3files_client, test_mount_target):
    """Update succeeds; the request shape is exercised. RGW stores
    the new SG list on the spec but does not enforce it."""
    mt_id = test_mount_target['mountTargetId']
    s3files_client.update_mount_target(
        mountTargetId=mt_id,
        securityGroups=["sg-test-1", "sg-test-2"],
    )
    got = s3files_client.get_mount_target(mountTargetId=mt_id)
    assert got['status'] in ('AVAILABLE', 'UPDATING')


@pytest.mark.conformance
def test_update_missing_security_groups(s3files_client, test_mount_target):
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.update_mount_target(
            mountTargetId=test_mount_target['mountTargetId'],
        )


@pytest.mark.conformance
def test_update_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.update_mount_target(
            mountTargetId="fsmt-no-such-9z9z9z",
            securityGroups=["sg-test"],
        )
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.MOUNT_TARGET_NOT_FOUND, err
