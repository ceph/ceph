"""Conformance tests for ListAccessPoints.

Smithy reference: com.amazonaws.s3files#ListAccessPoints.
Errors: InternalServerException, ResourceNotFoundException,
ValidationException.
"""

import pytest

from . import errors, assert_errorcode, validation_excs, NONEXISTENT_FS_ID


@pytest.mark.conformance
def test_list_empty_for_new_file_system(s3files_client, test_file_system):
    resp = s3files_client.list_access_points(
        fileSystemId=test_file_system['fileSystemId'],
    )
    assert 'accessPoints' in resp
    assert isinstance(resp['accessPoints'], list)
    assert resp['accessPoints'] == []


@pytest.mark.conformance
def test_list_entries_match_get(s3files_client, test_access_point):
    """Each list entry carries the same `AccessPointDescription`
    fields as the per-id Get response, with matching values."""
    fs_id = test_access_point['fileSystemId']
    ap_id = test_access_point['accessPointId']
    got = s3files_client.get_access_point(accessPointId=ap_id)
    listed = next(
        (ap for ap in s3files_client.list_access_points(
            fileSystemId=fs_id,
        )['accessPoints'] if ap['accessPointId'] == ap_id),
        None,
    )
    assert listed is not None
    for field in ('accessPointArn', 'fileSystemId',
                  'status', 'ownerId'):
        assert listed.get(field) == got.get(field), field


@pytest.mark.conformance
def test_list_missing_file_system_id(s3files_client):
    """fileSystemId is required (httpQuery=fileSystemId)."""
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.list_access_points()


@pytest.mark.conformance
def test_list_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.list_access_points(
            fileSystemId=NONEXISTENT_FS_ID,
        )
    assert_errorcode(exc.value, errors.FILE_SYSTEM_NOT_FOUND)


@pytest.mark.conformance
def test_list_max_results_out_of_range(s3files_client, test_file_system):
    """Smithy `@range` is 1..1000. Either side may catch the
    violation: boto3 enforces the trait client-side, RGW also
    enforces server-side."""
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.list_access_points(
            fileSystemId=test_file_system['fileSystemId'],
            maxResults=10000,
        )
