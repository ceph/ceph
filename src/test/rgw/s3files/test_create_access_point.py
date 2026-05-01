"""Conformance tests for CreateAccessPoint.

Smithy reference: com.amazonaws.s3files#CreateAccessPoint.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ServiceQuotaExceededException,
ValidationException.
"""

import pytest

from . import errors, NONEXISTENT_FS_ID


# ---------------------------------------------------------------- positive


@pytest.mark.conformance
def test_create_minimum(s3files_client, test_file_system):
    """Minimum valid request: only fileSystemId."""
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
    )
    try:
        assert 'accessPointId' in resp
        assert 'accessPointArn' in resp
        assert resp['fileSystemId'] == test_file_system['fileSystemId']
        assert resp['status'] in ('CREATING', 'AVAILABLE')
    finally:
        s3files_client.delete_access_point(accessPointId=resp['accessPointId'])


@pytest.mark.conformance
def test_create_with_posix_user(s3files_client, test_file_system):
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
        posixUser={"uid": 1000, "gid": 1000, "secondaryGids": [1001, 1002]},
    )
    try:
        assert resp['posixUser']['uid'] == 1000
        assert resp['posixUser']['gid'] == 1000
        assert resp['posixUser']['secondaryGids'] == [1001, 1002]
    finally:
        s3files_client.delete_access_point(accessPointId=resp['accessPointId'])


@pytest.mark.conformance
def test_create_with_root_directory(s3files_client, test_file_system):
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
        rootDirectory={
            "path": "/scoped",
            "creationPermissions": {
                "ownerUid": 1000,
                "ownerGid": 1000,
                "permissions": "0755",
            },
        },
    )
    try:
        assert resp['rootDirectory']['path'] == "/scoped"
    finally:
        s3files_client.delete_access_point(accessPointId=resp['accessPointId'])


@pytest.mark.conformance
def test_create_with_tags(s3files_client, test_file_system):
    """The `Name` tag drives the response `name` field."""
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
        tags=[
            {"key": "Name", "value": "ap-test"},
            {"key": "tier", "value": "ci"},
        ],
    )
    ap_id = resp['accessPointId']
    try:
        got = s3files_client.get_access_point(accessPointId=ap_id)
        assert got.get('name') == 'ap-test'
    finally:
        s3files_client.delete_access_point(accessPointId=ap_id)


@pytest.mark.conformance
def test_create_idempotent_with_client_token(s3files_client, test_file_system):
    token = 'idempotency-token-ap'
    a = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
        clientToken=token,
    )
    try:
        b = s3files_client.create_access_point(
            fileSystemId=test_file_system['fileSystemId'],
            clientToken=token,
        )
        assert a['accessPointId'] == b['accessPointId']
    finally:
        s3files_client.delete_access_point(accessPointId=a['accessPointId'])


# ---------------------------------------------------------------- validation


@pytest.mark.conformance
def test_create_missing_file_system_id(s3files_client):
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.create_access_point()


@pytest.mark.conformance
def test_create_invalid_posix_user(s3files_client, test_file_system):
    """posixUser must include both uid and gid."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.create_access_point(
            fileSystemId=test_file_system['fileSystemId'],
            posixUser={"uid": 1000},  # missing gid
        )
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.INVALID_POSIX_USER, err


# ---------------------------------------------------------------- not-found


@pytest.mark.conformance
def test_create_on_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.create_access_point(
            fileSystemId=NONEXISTENT_FS_ID,
        )
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err
