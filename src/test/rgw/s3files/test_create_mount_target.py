"""Conformance tests for CreateMountTarget.

Smithy reference: com.amazonaws.s3files#CreateMountTarget.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ServiceQuotaExceededException,
ValidationException.

Note: subnetId carries a Ceph zone-id in this implementation
(see design doc and test_ceph_divergences.py).
"""

import pytest

from . import errors, assert_errorcode, validation_excs, NONEXISTENT_FS_ID

# Tests that *send* a `subnet-{zone_hex}` value depend on the Ceph
# divergence and are double-marked `divergence` (see
# test_ceph_divergences.py). Tests that only assert error responses
# without needing a real subnet stay pure-conformance.


# ---------------------------------------------------------------- positive


@pytest.mark.conformance
@pytest.mark.divergence  # asserts subnetId round-trip and availabilityZoneId
def test_create_minimum(
    s3files_client, test_file_system, test_subnet_id, test_zone_id
):
    """Minimum valid request: fileSystemId + subnetId."""
    resp = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_subnet_id,
    )
    try:
        assert 'mountTargetId' in resp
        assert resp['fileSystemId'] == test_file_system['fileSystemId']
        # subnetId round-trips in its `subnet-{zone}` request form
        assert resp['subnetId'] == test_subnet_id
        # availabilityZoneId carries the bare zone-id (no `subnet-` prefix)
        assert resp.get('availabilityZoneId') == test_zone_id
        assert resp['status'] in ('CREATING', 'AVAILABLE')
    finally:
        s3files_client.delete_mount_target(mountTargetId=resp['mountTargetId'])


# ---------------------------------------------------------------- validation


@pytest.mark.conformance
def test_create_missing_file_system_id(s3files_client, test_subnet_id):
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.create_mount_target(subnetId=test_subnet_id)


@pytest.mark.conformance
def test_create_missing_subnet_id(s3files_client, test_file_system):
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.create_mount_target(
            fileSystemId=test_file_system['fileSystemId'],
        )


# ---------------------------------------------------------------- not-found


@pytest.mark.conformance
@pytest.mark.divergence  # passes a `subnet-{zone_hex}` value
def test_create_on_nonexistent_file_system(s3files_client, test_subnet_id):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.create_mount_target(
            fileSystemId=NONEXISTENT_FS_ID,
            subnetId=test_subnet_id,
        )
    assert_errorcode(exc.value, errors.FILE_SYSTEM_NOT_FOUND)


# ---------------------------------------------------------------- conflict


@pytest.mark.conformance
@pytest.mark.divergence  # asserts MOUNT_TARGET_ALREADY_IN_ZONE on subnet-{zone}
def test_only_one_mount_target_per_file_system_per_zone(
    s3files_client, test_file_system, test_subnet_id
):
    """AWS-shape rule: one mount target per AZ per filesystem.

    Maps to MOUNT_TARGET_ALREADY_EXISTS_IN_ZONE for our zone-id
    interpretation of subnetId.
    """
    first = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_subnet_id,
    )
    try:
        with pytest.raises(
            s3files_client.exceptions.ConflictException
        ) as exc:
            s3files_client.create_mount_target(
                fileSystemId=test_file_system['fileSystemId'],
                subnetId=test_subnet_id,
            )
        assert_errorcode(exc.value, errors.MOUNT_TARGET_ALREADY_IN_ZONE)
    finally:
        s3files_client.delete_mount_target(
            mountTargetId=first['mountTargetId'],
        )
