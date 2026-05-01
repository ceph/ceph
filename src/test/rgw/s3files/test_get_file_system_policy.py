"""Conformance tests for GetFileSystemPolicy.

Smithy reference: com.amazonaws.s3files#GetFileSystemPolicy.
Errors: ValidationException, ResourceNotFoundException,
InternalServerException.
"""

import json

import pytest

from . import errors, NONEXISTENT_FS_ID


@pytest.mark.conformance
def test_get_no_policy_set(s3files_client, test_file_system):
    """Get on a FS with no policy yet → ResourceNotFoundException."""
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.get_file_system_policy(
            fileSystemId=test_file_system['fileSystemId'],
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.POLICY_NOT_FOUND, err


@pytest.mark.conformance
def test_get_after_put(s3files_client, test_file_system):
    fs_id = test_file_system['fileSystemId']
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": "*",
                        "Action": "*", "Resource": "*"}],
    })
    s3files_client.put_file_system_policy(fileSystemId=fs_id, policy=policy)
    got = s3files_client.get_file_system_policy(fileSystemId=fs_id)
    assert got['fileSystemId'] == fs_id
    assert json.loads(got['policy']) == json.loads(policy)


@pytest.mark.conformance
def test_get_on_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.get_file_system_policy(
            fileSystemId=NONEXISTENT_FS_ID,
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err
