"""Conformance tests for GetAccessPoint.

Smithy reference: com.amazonaws.s3files#GetAccessPoint.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.
"""

import pytest

from . import errors, NONEXISTENT_AP_ID


@pytest.mark.conformance
def test_get_existing(s3files_client, test_access_point):
    ap_id = test_access_point['accessPointId']
    resp = s3files_client.get_access_point(accessPointId=ap_id)
    assert resp['accessPointId'] == ap_id
    assert resp['fileSystemId'] == test_access_point['fileSystemId']
    assert resp['status'] in ('CREATING', 'AVAILABLE')
    assert 'accessPointArn' in resp


@pytest.mark.conformance
def test_get_by_arn(s3files_client, test_access_point):
    """Smithy declares accessPointId accepts either the bare id or the ARN."""
    arn = test_access_point['accessPointArn']
    resp = s3files_client.get_access_point(accessPointId=arn)
    assert resp['accessPointArn'] == arn


@pytest.mark.conformance
def test_get_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.get_access_point(accessPointId=NONEXISTENT_AP_ID)
    err = exc.value.response
    assert err.get('errorCode') == errors.ACCESS_POINT_NOT_FOUND, err
