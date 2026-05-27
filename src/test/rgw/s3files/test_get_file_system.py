"""Conformance tests for GetFileSystem.

Smithy reference: com.amazonaws.s3files#GetFileSystem.
"""

import pytest

from . import errors, assert_errorcode, NONEXISTENT_FS_ID


@pytest.mark.conformance
def test_get_existing(s3files_client, test_file_system):
    fs_id = test_file_system['fileSystemId']
    resp = s3files_client.get_file_system(fileSystemId=fs_id)
    assert resp['fileSystemId'] == fs_id
    assert resp['bucket'] == test_file_system['bucket']
    assert resp['status'] in ('CREATING', 'AVAILABLE')
    assert 'fileSystemArn' in resp
    assert 'creationTime' in resp
    assert 'ownerId' in resp


@pytest.mark.conformance
def test_get_by_arn(s3files_client, test_file_system):
    """Smithy declares fileSystemId accepts either the bare id or
    the full FileSystem ARN."""
    arn = test_file_system['fileSystemArn']
    resp = s3files_client.get_file_system(fileSystemId=arn)
    assert resp['fileSystemArn'] == arn


@pytest.mark.conformance
def test_get_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.get_file_system(fileSystemId=NONEXISTENT_FS_ID)
    assert_errorcode(exc.value, errors.FILE_SYSTEM_NOT_FOUND)
