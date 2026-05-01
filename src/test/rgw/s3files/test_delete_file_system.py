"""Conformance tests for DeleteFileSystem.

Smithy reference: com.amazonaws.s3files#DeleteFileSystem.
"""

import pytest

from . import errors, NONEXISTENT_FS_ID


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
            fileSystemId=NONEXISTENT_FS_ID
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.FILE_SYSTEM_NOT_FOUND, err


@pytest.mark.conformance
def test_delete_with_children_rejected(s3files_client, test_file_system):
    """Deleting a FileSystem with active AccessPoints fails with
    ConflictException + FILE_SYSTEM_HAS_CHILDREN. Tracks the
    no-cascade semantics decision in the design doc Open
    questions; flip this test if cascade is later chosen."""
    fs_id = test_file_system['fileSystemId']
    ap = s3files_client.create_access_point(fileSystemId=fs_id)
    try:
        with pytest.raises(s3files_client.exceptions.ConflictException) as exc:
            s3files_client.delete_file_system(fileSystemId=fs_id)
        err = exc.value.response
        assert err.get('errorCode') == errors.FILE_SYSTEM_HAS_CHILDREN, err
    finally:
        s3files_client.delete_access_point(accessPointId=ap['accessPointId'])
