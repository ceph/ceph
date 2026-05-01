"""Conformance tests for UpdateMountTarget.

Smithy reference: com.amazonaws.s3files#UpdateMountTarget.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ValidationException.

Update accepts only `securityGroups` (required). RGW v1 stores
the spec change but treats SGs as no-ops at the data plane.
"""

import pytest

from . import errors, assert_errorcode, validation_excs, NONEXISTENT_MT_ID


# SecurityGroup smithy constraint: ^sg-[0-9a-f]{8,40}$ (length 11..43).
_SG_A = "sg-0000aaaa"
_SG_B = "sg-0000bbbb"


@pytest.mark.conformance
@pytest.mark.divergence  # uses test_mount_target (subnet-{zone_hex})
def test_update_security_groups(s3files_client, test_mount_target):
    """Update succeeds; the request shape is exercised. RGW stores
    the new SG list on the spec but does not enforce it."""
    mt_id = test_mount_target['mountTargetId']
    s3files_client.update_mount_target(
        mountTargetId=mt_id,
        securityGroups=[_SG_A, _SG_B],
    )
    got = s3files_client.get_mount_target(mountTargetId=mt_id)
    assert got['status'] in ('AVAILABLE', 'UPDATING')


@pytest.mark.conformance
@pytest.mark.divergence  # uses test_mount_target (subnet-{zone_hex})
def test_update_missing_security_groups(s3files_client, test_mount_target):
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.update_mount_target(
            mountTargetId=test_mount_target['mountTargetId'],
        )


@pytest.mark.conformance
def test_update_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.update_mount_target(
            mountTargetId=NONEXISTENT_MT_ID,
            securityGroups=[_SG_A],
        )
    assert_errorcode(exc.value, errors.MOUNT_TARGET_NOT_FOUND)
