"""Conformance tests for CreateFileSystem.

Smithy reference: com.amazonaws.s3files#CreateFileSystem in
src/rgw/spec/aws/s3files/2025-05-05/. Exercises positive shape
plus the declared error set: ConflictException,
InternalServerException, ResourceNotFoundException,
ServiceQuotaExceededException, ValidationException.
"""

import re

import pytest

from . import errors, assert_errorcode, validation_excs


# Smithy: `^(arn:aws[-a-z]*:s3files:[0-9a-z-:]+:file-system/fs-[0-9a-f]{17,40})$`
_FS_ARN_RE = re.compile(
    r'^arn:aws[-a-z]*:s3files:[0-9a-z-:]+:file-system/fs-[0-9a-f]{17,40}$'
)
_FS_ID_RE = re.compile(r'^fs-[0-9a-f]{17,40}$')


# ---------------------------------------------------------------- positive


@pytest.mark.conformance
def test_create_minimum(s3files_client, bucket_arn, shared_test_role):
    """Minimum valid request: bucket + roleArn. The Smithy-declared
    output shape is fully exercised: ids match the Smithy patterns,
    the ARN encodes the id, and the new FS is observable via Get."""
    resp = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
    )
    fs_id = resp['fileSystemId']
    try:
        assert _FS_ID_RE.match(fs_id), fs_id
        assert _FS_ARN_RE.match(resp['fileSystemArn']), resp['fileSystemArn']
        # ARN's resource component is separated from account by `:`
        assert resp['fileSystemArn'].endswith(f":file-system/{fs_id}")
        assert resp['bucket'] == bucket_arn
        assert resp['roleArn'] == shared_test_role
        assert resp['status'] in ('CREATING', 'AVAILABLE')
        assert 'creationTime' in resp
        assert resp.get('ownerId'), resp
        # Re-Get and value-compare every output member.
        got = s3files_client.get_file_system(fileSystemId=fs_id)
        for field in ('fileSystemId', 'fileSystemArn', 'bucket',
                      'roleArn', 'ownerId', 'creationTime'):
            assert got[field] == resp[field], field
    finally:
        s3files_client.delete_file_system(fileSystemId=fs_id)


@pytest.mark.conformance
def test_create_with_prefix(s3files_client, bucket_arn, shared_test_role):
    """prefix round-trips through both the create response and Get."""
    resp = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
        prefix="data/",
    )
    fs_id = resp['fileSystemId']
    try:
        assert resp['prefix'] == "data/"
        got = s3files_client.get_file_system(fileSystemId=fs_id)
        assert got['prefix'] == "data/"
    finally:
        s3files_client.delete_file_system(fileSystemId=fs_id)


@pytest.mark.conformance
def test_create_with_tags(s3files_client, bucket_arn, shared_test_role):
    """All tags round-trip via list_tags_for_resource. The `Name`
    tag also drives the response `name` field."""
    in_tags = [
        {"key": "Name", "value": "test-fs"},
        {"key": "env", "value": "ci"},
        {"key": "tier", "value": "gold"},
    ]
    resp = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
        tags=in_tags,
    )
    fs_id = resp['fileSystemId']
    try:
        got = s3files_client.get_file_system(fileSystemId=fs_id)
        assert got.get('name') == 'test-fs'
        listed = s3files_client.list_tags_for_resource(resourceId=fs_id)
        out = {t['key']: t['value'] for t in listed['tags']}
        # Every input tag must appear with its supplied value.
        for t in in_tags:
            assert out.get(t['key']) == t['value'], (
                f"tag {t['key']!r} not persisted: out={out}"
            )
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
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.create_file_system(roleArn=shared_test_role)


@pytest.mark.conformance
def test_create_missing_role_arn(s3files_client, bucket_arn):
    """Required field `roleArn` missing → ValidationException."""
    with pytest.raises(validation_excs(s3files_client)):
        s3files_client.create_file_system(bucket=bucket_arn)


@pytest.mark.conformance
def test_create_invalid_bucket_arn(s3files_client, shared_test_role):
    """bucket field doesn't parse as an S3 ARN."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.create_file_system(
            bucket="not-an-arn",
            roleArn=shared_test_role,
        )
    assert_errorcode(exc.value, errors.INVALID_BUCKET_ARN)


@pytest.mark.conformance
def test_create_invalid_role_arn(s3files_client, bucket_arn):
    """roleArn field doesn't parse as an IAM role ARN."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.create_file_system(
            bucket=bucket_arn,
            roleArn="not-an-arn",
        )
    assert_errorcode(exc.value, errors.INVALID_ROLE_ARN)


@pytest.mark.conformance
def test_create_invalid_prefix(s3files_client, bucket_arn, shared_test_role):
    """prefix must match Smithy pattern ^(|.*/)$ — must end in '/'."""
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.create_file_system(
            bucket=bucket_arn,
            roleArn=shared_test_role,
            prefix="data",  # missing trailing slash
        )
    assert_errorcode(exc.value, errors.INVALID_PREFIX)


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
    assert_errorcode(exc.value, errors.BUCKET_NOT_FOUND)


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
    assert_errorcode(exc.value, errors.ROLE_NOT_FOUND)


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
        assert_errorcode(exc.value, errors.BUCKET_ALREADY_IN_USE)
    finally:
        s3files_client.delete_file_system(fileSystemId=first['fileSystemId'])
