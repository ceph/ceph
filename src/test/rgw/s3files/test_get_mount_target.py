"""Conformance tests for GetMountTarget.

Smithy reference: com.amazonaws.s3files#GetMountTarget.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.
"""

import pytest

from . import errors


@pytest.mark.conformance
def test_get_existing(s3files_client, test_mount_target, test_zone_id):
    mt_id = test_mount_target['mountTargetId']
    resp = s3files_client.get_mount_target(mountTargetId=mt_id)
    assert resp['mountTargetId'] == mt_id
    assert resp['fileSystemId'] == test_mount_target['fileSystemId']
    assert resp['subnetId'] == test_zone_id
    assert resp.get('availabilityZoneId') == test_zone_id
    assert resp['status'] in ('CREATING', 'AVAILABLE')


@pytest.mark.conformance
def test_get_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.get_mount_target(mountTargetId="fsmt-no-such-9z9z9z")
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.MOUNT_TARGET_NOT_FOUND, err
