"""Pytest fixtures for the S3 Files API conformance suite.

Session-scoped: boto3 clients (s3, iam, s3files), and a single
shared IAM role usable as roleArn on CreateFileSystem.

Function-scoped: per-test bucket and FileSystem fixtures, with
best-effort teardown that swallows cleanup errors so a flaky
delete doesn't mask the actual test failure.
"""

import json
import logging
import random
import string

import pytest
from botocore.exceptions import ClientError

from . import setup, make_client, get_user_id, get_zone_id, make_subnet_id

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True, scope="session")
def setup_config():
    setup()


def _rand_suffix(n=8):
    return ''.join(random.choice(string.ascii_lowercase + string.digits)
                   for _ in range(n))


# ---------------------------------------------------------------- clients


@pytest.fixture(scope="session")
def s3_client(setup_config):
    return make_client('s3')


@pytest.fixture(scope="session")
def iam_client(setup_config):
    return make_client('iam')


@pytest.fixture(scope="session")
def s3files_client(setup_config):
    return make_client('s3files')


# ---------------------------------------------------------------- shared role


_ROLE_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "s3files.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}


@pytest.fixture(scope="session")
def shared_test_role(iam_client):
    """A single IAM role used as roleArn on CreateFileSystem.

    Created once per session; removed on teardown. Tests that need
    a missing-role case construct a non-existent ARN inline rather
    than depending on this fixture.
    """
    name = f"s3files-test-role-{_rand_suffix()}"
    resp = iam_client.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=json.dumps(_ROLE_TRUST_POLICY),
    )
    arn = resp['Role']['Arn']
    yield arn
    try:
        iam_client.delete_role(RoleName=name)
    except ClientError:
        log.warning("failed to delete IAM role %s", name, exc_info=True)


# ---------------------------------------------------------------- bucket


@pytest.fixture(scope="function")
def test_bucket(s3_client):
    """A freshly-created S3 bucket for one test.

    The bucket is emptied and removed on teardown; failures are
    logged, not raised.
    """
    name = f"s3files-test-{_rand_suffix()}"
    s3_client.create_bucket(Bucket=name)
    yield name
    try:
        objs = s3_client.list_objects_v2(Bucket=name).get('Contents', [])
        for o in objs:
            s3_client.delete_object(Bucket=name, Key=o['Key'])
        s3_client.delete_bucket(Bucket=name)
    except ClientError:
        log.warning("failed to clean up bucket %s", name, exc_info=True)


def _bucket_arn(bucket_name):
    return f"arn:aws:s3:::{bucket_name}"


@pytest.fixture(scope="function")
def bucket_arn(test_bucket):
    return _bucket_arn(test_bucket)


# ---------------------------------------------------------------- file system


@pytest.fixture(scope="function")
def test_file_system(s3files_client, bucket_arn, shared_test_role):
    """A freshly-created FileSystem on a fresh bucket.

    Best-effort cleanup; tests that have created child resources
    (APs, MTs) under this FS are responsible for tearing those
    down before fixture teardown attempts the FS delete.
    """
    resp = s3files_client.create_file_system(
        bucket=bucket_arn,
        roleArn=shared_test_role,
    )
    fs_id = resp['fileSystemId']
    yield resp
    try:
        s3files_client.delete_file_system(fileSystemId=fs_id)
    except ClientError:
        log.warning("failed to delete FileSystem %s", fs_id, exc_info=True)


# ---------------------------------------------------------------- access point


@pytest.fixture(scope="function")
def test_access_point(s3files_client, test_file_system):
    """A freshly-created AccessPoint on a fresh FileSystem.

    Best-effort cleanup; tests that have created child resources
    (MTs) on the parent FS are responsible for cleaning those up
    first.
    """
    resp = s3files_client.create_access_point(
        fileSystemId=test_file_system['fileSystemId'],
    )
    yield resp
    try:
        s3files_client.delete_access_point(accessPointId=resp['accessPointId'])
    except ClientError:
        log.warning(
            "failed to delete AccessPoint %s",
            resp['accessPointId'],
            exc_info=True,
        )


# ---------------------------------------------------------------- mount target


@pytest.fixture(scope="session")
def test_zone_id(setup_config):
    """The bare zone-id (matches what the response's
    availabilityZoneId carries)."""
    return get_zone_id()


@pytest.fixture(scope="session")
def test_subnet_id(test_zone_id):
    """The Smithy-pattern-valid form passed in CreateMountTarget
    request fields: `subnet-{zone_id}`."""
    return make_subnet_id(test_zone_id)


@pytest.fixture(scope="function")
def test_mount_target(s3files_client, test_file_system, test_subnet_id):
    """A freshly-created MountTarget on a fresh FileSystem in the
    configured zone."""
    resp = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_subnet_id,
    )
    yield resp
    try:
        s3files_client.delete_mount_target(mountTargetId=resp['mountTargetId'])
    except ClientError:
        log.warning(
            "failed to delete MountTarget %s",
            resp['mountTargetId'],
            exc_info=True,
        )
