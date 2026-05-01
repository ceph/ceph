"""Conformance tests for ListAccessPoints.

Smithy reference: com.amazonaws.s3files#ListAccessPoints.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.
"""

import pytest

from . import errors, missing_required_exc, NONEXISTENT_FS_ID


@pytest.mark.conformance
def test_list_empty_for_new_file_system(s3files_client, test_file_system):
    resp = s3files_client.list_access_points(
        fileSystemId=test_file_system['fileSystemId'],
    )
    assert 'accessPoints' in resp
    assert isinstance(resp['accessPoints'], list)
    assert resp['accessPoints'] == []


@pytest.mark.conformance
def test_list_includes_created(s3files_client, test_access_point):
    fs_id = test_access_point['fileSystemId']
    ap_id = test_access_point['accessPointId']
    resp = s3files_client.list_access_points(fileSystemId=fs_id)
    ids = {ap['accessPointId'] for ap in resp['accessPoints']}
    assert ap_id in ids


@pytest.mark.conformance
def test_list_missing_file_system_id(s3files_client):
    """fileSystemId is required (httpQuery=fileSystemId)."""
    with pytest.raises(missing_required_exc(s3files_client)):
        s3files_client.list_access_points()


@pytest.mark.conformance
def test_list_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.list_access_points(
            fileSystemId=NONEXISTENT_FS_ID,
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err


@pytest.mark.conformance
def test_list_max_results_out_of_range(s3files_client, test_file_system):
    """Smithy range is 1..1000."""
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.list_access_points(
            fileSystemId=test_file_system['fileSystemId'],
            maxResults=10000,
        )
