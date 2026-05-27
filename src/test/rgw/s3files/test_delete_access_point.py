"""Conformance tests for DeleteAccessPoint.

Smithy reference: com.amazonaws.s3files#DeleteAccessPoint.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ValidationException.
"""

import pytest

from . import errors, assert_errorcode, NONEXISTENT_AP_ID


@pytest.mark.conformance
def test_delete_existing(s3files_client, test_file_system):
    """Create + delete + assert subsequent get returns NotFound."""
    ap = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
    )
    ap_id = ap['accessPointId']
    s3files_client.delete_access_point(accessPointId=ap_id)
    with pytest.raises(s3files_client.exceptions.ResourceNotFoundException):
        s3files_client.get_access_point(accessPointId=ap_id)


@pytest.mark.conformance
def test_delete_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.delete_access_point(accessPointId=NONEXISTENT_AP_ID)
    assert_errorcode(exc.value, errors.ACCESS_POINT_NOT_FOUND)
