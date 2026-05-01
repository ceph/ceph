"""Conformance tests for PutFileSystemPolicy.

Smithy reference: com.amazonaws.s3files#PutFileSystemPolicy.
Errors: ValidationException, ResourceNotFoundException,
InternalServerException.
"""

import json

import pytest

from . import errors, missing_required_exc, NONEXISTENT_FS_ID


_VALID_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowOwner",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3files:GetFileSystem",
            "Resource": "*",
        }
    ],
})


@pytest.mark.conformance
def test_put_then_get_round_trip(s3files_client, test_file_system):
    fs_id = test_file_system['fileSystemId']
    s3files_client.put_file_system_policy(fileSystemId=fs_id, policy=_VALID_POLICY)
    got = s3files_client.get_file_system_policy(fileSystemId=fs_id)
    assert got['fileSystemId'] == fs_id
    assert json.loads(got['policy']) == json.loads(_VALID_POLICY)


@pytest.mark.conformance
def test_put_replaces_existing(s3files_client, test_file_system):
    fs_id = test_file_system['fileSystemId']
    s3files_client.put_file_system_policy(fileSystemId=fs_id, policy=_VALID_POLICY)
    new_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Deny", "Principal": "*",
                        "Action": "s3files:*", "Resource": "*"}],
    })
    s3files_client.put_file_system_policy(fileSystemId=fs_id, policy=new_policy)
    got = s3files_client.get_file_system_policy(fileSystemId=fs_id)
    assert json.loads(got['policy']) == json.loads(new_policy)


@pytest.mark.conformance
def test_put_missing_policy(s3files_client, test_file_system):
    with pytest.raises(missing_required_exc(s3files_client)):
        s3files_client.put_file_system_policy(
            fileSystemId=test_file_system['fileSystemId'],
        )


@pytest.mark.conformance
def test_put_invalid_json(s3files_client, test_file_system):
    """Policy field must parse as JSON."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.put_file_system_policy(
            fileSystemId=test_file_system['fileSystemId'],
            policy="not-valid-json{",
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.INVALID_POLICY_DOCUMENT, err


@pytest.mark.conformance
def test_put_on_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.put_file_system_policy(
            fileSystemId=NONEXISTENT_FS_ID,
            policy=_VALID_POLICY,
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err
