"""Conformance tests for DeleteFileSystemPolicy.

Smithy reference: com.amazonaws.s3files#DeleteFileSystemPolicy.
Errors: ValidationException, ResourceNotFoundException,
InternalServerException.
"""

import json

import pytest

from . import errors, NONEXISTENT_FS_ID


_VALID_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": "*",
                    "Action": "*", "Resource": "*"}],
})


@pytest.mark.conformance
def test_put_then_delete(s3files_client, test_file_system):
    """After delete, a subsequent get raises ResourceNotFoundException
    with POLICY_NOT_FOUND."""
    fs_id = test_file_system['fileSystemId']
    s3files_client.put_file_system_policy(fileSystemId=fs_id, policy=_VALID_POLICY)
    s3files_client.delete_file_system_policy(fileSystemId=fs_id)
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.get_file_system_policy(fileSystemId=fs_id)
    err = exc.value.response
    assert err.get('errorCode') == errors.POLICY_NOT_FOUND, err


@pytest.mark.conformance
def test_delete_no_policy_set(s3files_client, test_file_system):
    """Delete on a FS with no policy → ResourceNotFoundException."""
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.delete_file_system_policy(
            fileSystemId=test_file_system['fileSystemId'],
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.POLICY_NOT_FOUND, err


@pytest.mark.conformance
def test_delete_on_nonexistent_file_system(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.delete_file_system_policy(
            fileSystemId=NONEXISTENT_FS_ID,
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err
