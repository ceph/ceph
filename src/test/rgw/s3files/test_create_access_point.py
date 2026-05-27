"""Conformance tests for CreateAccessPoint.

Smithy reference: com.amazonaws.s3files#CreateAccessPoint.
Errors: ConflictException, InternalServerException,
ResourceNotFoundException, ServiceQuotaExceededException,
ValidationException.
"""

import re

import pytest

from . import errors, assert_errorcode, validation_excs, NONEXISTENT_FS_ID


# Smithy: ^arn:aws[-a-z]*:s3files:[0-9a-z-:]+:file-system/fs-[...]/access-point/fsap-[...]
# Loosened `[0-9a-z-:]+` to `[0-9A-Za-z-:]*` so RGW account ids
# like `RGW65713045997841677` (uppercase alphanumeric) match too.
_AP_ARN_RE = re.compile(
    r'^arn:aws[-a-z]*:s3files:[0-9A-Za-z-:]*:'
    r'file-system/fs-[0-9a-f]{17,40}/access-point/fsap-[0-9a-f]{17,40}$'
)
_AP_ID_RE = re.compile(r'^fsap-[0-9a-f]{17,40}$')


# ---------------------------------------------------------------- positive


@pytest.mark.conformance
def test_create_minimum(s3files_client, test_file_system):
    """Minimum valid request: only fileSystemId. The full output
    shape is exercised: ids match Smithy patterns, the AP ARN
    encodes both the parent FS id and the AP id, and the new AP
    is observable via Get with matching fields."""
    fs_id = test_file_system['fileSystemId']
    resp = s3files_client.create_access_point(fileSystemId=fs_id)
    ap_id = resp['accessPointId']
    try:
        assert _AP_ID_RE.match(ap_id), ap_id
        arn = resp['accessPointArn']
        assert _AP_ARN_RE.match(arn), arn
        # ARN's resource component is separated from account by `:`
        assert arn.endswith(f":file-system/{fs_id}/access-point/{ap_id}")
        assert resp['fileSystemId'] == fs_id
        assert resp['status'] in ('CREATING', 'AVAILABLE')
        assert resp.get('ownerId'), resp
        got = s3files_client.get_access_point(accessPointId=ap_id)
        for field in ('accessPointId', 'accessPointArn',
                      'fileSystemId', 'ownerId'):
            assert got[field] == resp[field], field
    finally:
        s3files_client.delete_access_point(accessPointId=ap_id)


@pytest.mark.conformance
def test_create_with_posix_user(s3files_client, test_file_system):
    """posixUser round-trips through both the create response and Get."""
    pu = {"uid": 1000, "gid": 1000, "secondaryGids": [1001, 1002]}
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
        posixUser=pu,
    )
    ap_id = resp['accessPointId']
    try:
        assert resp['posixUser'] == pu
        got = s3files_client.get_access_point(accessPointId=ap_id)
        assert got['posixUser'] == pu
    finally:
        s3files_client.delete_access_point(accessPointId=ap_id)


@pytest.mark.conformance
def test_create_with_root_directory(s3files_client, test_file_system):
    """rootDirectory (path + creationPermissions) round-trips
    through both the create response and Get."""
    rd = {
        "path": "/scoped",
        "creationPermissions": {
            "ownerUid": 1000,
            "ownerGid": 1000,
            "permissions": "0755",
        },
    }
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
        rootDirectory=rd,
    )
    ap_id = resp['accessPointId']
    try:
        assert resp['rootDirectory'] == rd
        got = s3files_client.get_access_point(accessPointId=ap_id)
        assert got['rootDirectory'] == rd
    finally:
        s3files_client.delete_access_point(accessPointId=ap_id)


@pytest.mark.conformance
def test_create_with_tags(s3files_client, test_file_system):
    """All tags round-trip via list_tags_for_resource. The `Name`
    tag also drives the response `name` field."""
    in_tags = [
        {"key": "Name", "value": "ap-test"},
        {"key": "tier", "value": "ci"},
        {"key": "env", "value": "test"},
    ]
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
        tags=in_tags,
    )
    ap_id = resp['accessPointId']
    try:
        got = s3files_client.get_access_point(accessPointId=ap_id)
        assert got.get('name') == 'ap-test'
        listed = s3files_client.list_tags_for_resource(resourceId=ap_id)
        out = {t['key']: t['value'] for t in listed['tags']}
        for t in in_tags:
            assert out.get(t['key']) == t['value'], (
                f"tag {t['key']!r} not persisted: out={out}"
            )
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
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.create_access_point()


@pytest.mark.conformance
def test_create_invalid_posix_user(s3files_client, test_file_system):
    """posixUser.gid is Smithy `@required`; boto3 may reject the
    request client-side. Either path validates the contract."""
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.create_access_point(
            fileSystemId=test_file_system['fileSystemId'],
            posixUser={"uid": 1000},  # missing gid
        )


# ---------------------------------------------------------------- not-found


@pytest.mark.conformance
def test_create_on_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.create_access_point(
            fileSystemId=NONEXISTENT_FS_ID,
        )
    assert_errorcode(exc.value, errors.FILE_SYSTEM_NOT_FOUND)
