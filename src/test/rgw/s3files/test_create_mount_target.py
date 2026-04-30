"""Conformance tests for CreateMountTarget.

Smithy reference: com.amazonaws.s3files#CreateMountTarget.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ServiceQuotaExceededException,
ValidationException.

Note: subnetId carries a Ceph zone-id in this implementation
(see design doc and test_ceph_divergences.py).
"""

import pytest

from . import errors


# ---------------------------------------------------------------- positive


@pytest.mark.conformance
def test_create_minimum(s3files_client, test_file_system, test_zone_id):
    """Minimum valid request: fileSystemId + subnetId(=zone-id)."""
    resp = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_zone_id,
    )
    try:
        assert 'mountTargetId' in resp
        assert resp['fileSystemId'] == test_file_system['fileSystemId']
        assert resp['subnetId'] == test_zone_id
        assert resp['status'] in ('CREATING', 'AVAILABLE')
        # availabilityZoneId is response-populated with the same zone
        assert resp.get('availabilityZoneId') == test_zone_id
    finally:
        s3files_client.delete_mount_target(mountTargetId=resp['mountTargetId'])


# ---------------------------------------------------------------- validation


@pytest.mark.conformance
def test_create_missing_file_system_id(s3files_client, test_zone_id):
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.create_mount_target(subnetId=test_zone_id)


@pytest.mark.conformance
def test_create_missing_subnet_id(s3files_client, test_file_system):
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.create_mount_target(
            fileSystemId=test_file_system['fileSystemId'],
        )


# ---------------------------------------------------------------- not-found


@pytest.mark.conformance
def test_create_on_nonexistent_file_system(s3files_client, test_zone_id):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.create_mount_target(
            fileSystemId="fs-no-such-thing-9z9z9z",
            subnetId=test_zone_id,
        )
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err


# ---------------------------------------------------------------- conflict


@pytest.mark.conformance
def test_only_one_mount_target_per_file_system_per_zone(
    s3files_client, test_file_system, test_zone_id
):
    """AWS-shape rule: one mount target per AZ per filesystem.

    Maps to MOUNT_TARGET_ALREADY_EXISTS_IN_ZONE for our zone-id
    interpretation of subnetId.
    """
    first = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_zone_id,
    )
    try:
        with pytest.raises(
            s3files_client.exceptions.ConflictException
        ) as exc:
            s3files_client.create_mount_target(
                fileSystemId=test_file_system['fileSystemId'],
                subnetId=test_zone_id,
            )
        err = exc.value.response.get('Error', {})
        assert err.get('errorCode') == errors.MOUNT_TARGET_ALREADY_IN_ZONE, err
    finally:
        s3files_client.delete_mount_target(
            mountTargetId=first['mountTargetId'],
        )
