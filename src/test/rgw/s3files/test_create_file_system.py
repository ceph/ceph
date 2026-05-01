"""Conformance tests for CreateFileSystem.

Smithy reference: com.amazonaws.s3files#CreateFileSystem in
src/rgw/spec/aws/s3files/2025-05-05/. Exercises positive shape
plus the declared error set: ConflictException,
InternalServerException, ResourceNotFoundException,
ServiceQuotaExceededException, ValidationException.
"""

import pytest

from . import errors, missing_required_exc


# ---------------------------------------------------------------- positive


@pytest.mark.conformance
def test_create_minimum(s3files_client, bucket_arn, shared_test_role):
    """Minimum valid request: bucket + roleArn."""
    resp = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
    )
    try:
        assert 'fileSystemId' in resp
        assert 'fileSystemArn' in resp
        assert resp['bucket'] == bucket_arn
        assert resp['roleArn'] == shared_test_role
        assert resp['status'] in ('CREATING', 'AVAILABLE')
    finally:
        s3files_client.delete_file_system(fileSystemId=resp['fileSystemId'])


@pytest.mark.conformance
def test_create_with_prefix(s3files_client, bucket_arn, shared_test_role):
    resp = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
        prefix="data/",
    )
    try:
        assert resp['prefix'] == "data/"
    finally:
        s3files_client.delete_file_system(fileSystemId=resp['fileSystemId'])


@pytest.mark.conformance
def test_create_with_tags(s3files_client, bucket_arn, shared_test_role):
    """tags are accepted; the `Name` tag drives the response `name`."""
    resp = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
        tags=[
            {"key": "Name", "value": "test-fs"},
            {"key": "env", "value": "ci"},
        ],
    )
    fs_id = resp['fileSystemId']
    try:
        got = s3files_client.get_file_system(fileSystemId=fs_id)
        assert got.get('name') == 'test-fs'
    finally:
        s3files_client.delete_file_system(fileSystemId=fs_id)


@pytest.mark.conformance
def test_create_idempotent_with_client_token(
    s3files_client, bucket_arn, shared_test_role
):
    """Same clientToken twice → same FileSystem, no duplicate."""
    token = 'idempotency-token-test'
    a = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
        clientToken=token,
    )
    try:
        b = s3files_client.create_file_system(
            bucket=bucket_arn,
            roleArn=shared_test_role,
            clientToken=token,
        )
        assert a['fileSystemId'] == b['fileSystemId']
    finally:
        s3files_client.delete_file_system(fileSystemId=a['fileSystemId'])


# ---------------------------------------------------------------- validation


@pytest.mark.conformance
def test_create_missing_bucket(s3files_client, shared_test_role):
    """Required field `bucket` missing → ValidationException."""
    with pytest.raises(missing_required_exc(s3files_client)):
        s3files_client.create_file_system(roleArn=shared_test_role)


@pytest.mark.conformance
def test_create_missing_role_arn(s3files_client, bucket_arn):
    """Required field `roleArn` missing → ValidationException."""
    with pytest.raises(missing_required_exc(s3files_client)):
        s3files_client.create_file_system(bucket=bucket_arn)


@pytest.mark.conformance
def test_create_invalid_bucket_arn(s3files_client, shared_test_role):
    """bucket field doesn't parse as an S3 ARN."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.create_file_system(
            bucket="not-an-arn",
            roleArn=shared_test_role,
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.INVALID_BUCKET_ARN, err


@pytest.mark.conformance
def test_create_invalid_role_arn(s3files_client, bucket_arn):
    """roleArn field doesn't parse as an IAM role ARN."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.create_file_system(
            bucket=bucket_arn,
            roleArn="not-an-arn",
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.INVALID_ROLE_ARN, err


@pytest.mark.conformance
def test_create_invalid_prefix(s3files_client, bucket_arn, shared_test_role):
    """prefix must match Smithy pattern ^(|.*/)$ — must end in '/'."""
    with pytest.raises(s3files_client.exceptions.ValidationException):
        s3files_client.create_file_system(
            bucket=bucket_arn,
            roleArn=shared_test_role,
            prefix="data",  # missing trailing slash
        )


# ---------------------------------------------------------------- not-found


@pytest.mark.conformance
def test_create_bucket_not_found(s3files_client, shared_test_role):
    """bucket ARN format-valid but bucket does not exist."""
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.create_file_system(
            bucket="arn:aws:s3:::no-such-bucket-here-9z9z9z",
            roleArn=shared_test_role,
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.BUCKET_NOT_FOUND, err


@pytest.mark.conformance
def test_create_role_not_found(s3files_client, bucket_arn):
    """roleArn format-valid but role does not exist."""
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.create_file_system(
            bucket=bucket_arn,
            roleArn="arn:aws:iam::000000000000:role/no-such-role-9z9z9z",
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.ROLE_NOT_FOUND, err


# ---------------------------------------------------------------- conflict


@pytest.mark.conformance
def test_create_bucket_already_in_use(
    s3files_client, bucket_arn, shared_test_role
):
    """A second FileSystem on the same bucket → ConflictException."""
    first = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
    )
    try:
        with pytest.raises(
            s3files_client.exceptions.ConflictException
        ) as exc:
            s3files_client.create_file_system(
                bucket=bucket_arn,
                roleArn=shared_test_role,
            )
        err = exc.value.response
        assert err.get('errorCode') == errors.BUCKET_ALREADY_IN_USE, err
    finally:
        s3files_client.delete_file_system(fileSystemId=first['fileSystemId'])
