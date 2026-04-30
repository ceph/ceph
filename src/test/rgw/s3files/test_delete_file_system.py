"""Conformance tests for DeleteFileSystem.

Smithy reference: com.amazonaws.s3files#DeleteFileSystem.

Note: the test_delete_with_children case lives in the AccessPoint
test file once that handler ships, since it requires
CreateAccessPoint.
"""

import pytest

from . import errors


@pytest.mark.conformance
def test_delete_existing(s3files_client, bucket_arn, shared_test_role):
    """Create + delete + assert subsequent get returns NotFound."""
    fs = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
    )
    fs_id = fs['fileSystemId']
    s3files_client.delete_file_system(fileSystemId=fs_id)
    with pytest.raises(s3files_client.exceptions.ResourceNotFoundException):
        s3files_client.get_file_system(fileSystemId=fs_id)


@pytest.mark.conformance
def test_delete_nonexistent(s3files_client):
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.delete_file_system(
            fileSystemId="fs-no-such-thing-9z9z9z"
        )
    err = exc.value.response.get('Error', {})
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err
