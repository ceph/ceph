"""Least-privilege / IAM permission tests for the S3 Files API.

The conformance suite runs as the account-root user, which
bypasses policy evaluation — proving the verify_permission()
wiring doesn't break anything but never proving it actually
checks anything. This file fills that gap: each test mints a
fresh non-root IAM user inside the test account, attaches a
specific inline policy, and asserts that the resulting client's
API calls are allowed or denied per the policy.

User and policy bootstrap goes through the IAM API
(`make_scoped_client` factory in conftest.py), not radosgw-admin,
so the same tests should be portable to a real AWS account given
suitable account-root credentials.

Marked `permissions` so this category can be selected or excluded
independently of the per-op `conformance` tests.
"""

import pytest


_ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": "s3files:*", "Resource": "*"},
    ],
}

_EMPTY = {"Version": "2012-10-17", "Statement": []}


@pytest.mark.permissions
def test_no_policy_denies_all(make_scoped_client, test_file_system):
    """A non-root user with an empty policy gets AccessDenied on
    every Files API call. mandatory_policy=True in the handler
    means absence of a matching Allow is itself a Deny."""
    client = make_scoped_client(_EMPTY)
    fs_id = test_file_system['fileSystemId']
    with pytest.raises(client.exceptions.ClientError) as exc:
        client.list_file_systems()
    assert exc.value.response['Error']['Code'] == 'AccessDenied'

    with pytest.raises(client.exceptions.ClientError) as exc:
        client.get_file_system(fileSystemId=fs_id)
    assert exc.value.response['Error']['Code'] == 'AccessDenied'


@pytest.mark.permissions
def test_allow_all_permits_all(make_scoped_client, test_file_system):
    """`Allow s3files:* on *` permits every Files API operation."""
    client = make_scoped_client(_ALLOW_ALL)
    fs_id = test_file_system['fileSystemId']
    # Both reads should succeed.
    client.list_file_systems()
    got = client.get_file_system(fileSystemId=fs_id)
    assert got['fileSystemId'] == fs_id


@pytest.mark.permissions
def test_action_scoped_allow(make_scoped_client, test_file_system):
    """`Allow s3files:GetFileSystem on *` permits Get but denies
    other operations like Delete."""
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3files:GetFileSystem",
            "Resource": "*",
        }],
    }
    client = make_scoped_client(policy)
    fs_id = test_file_system['fileSystemId']

    # Allowed.
    client.get_file_system(fileSystemId=fs_id)

    # Not allowed: ListFileSystems is a different action.
    with pytest.raises(client.exceptions.ClientError) as exc:
        client.list_file_systems()
    assert exc.value.response['Error']['Code'] == 'AccessDenied'

    # Not allowed: DeleteFileSystem is a different action.
    with pytest.raises(client.exceptions.ClientError) as exc:
        client.delete_file_system(fileSystemId=fs_id)
    assert exc.value.response['Error']['Code'] == 'AccessDenied'


@pytest.mark.permissions
def test_resource_scoped_allow(
    make_scoped_client, s3files_client, s3_client, shared_test_role,
):
    """`Allow s3files:GetFileSystem on file-system/<fs-A>` permits
    Get against fs-A but denies Get against fs-B. This is the test
    that catches a `*`-vs-specific-ARN regression: if the handler
    sends the request's resource as `*` instead of the actual id,
    a fine-grained policy never matches and the test fails with
    AccessDenied even on fs-A."""
    import uuid
    bucket_a = f"s3files-perm-a-{uuid.uuid4().hex[:8]}"
    bucket_b = f"s3files-perm-b-{uuid.uuid4().hex[:8]}"
    s3_client.create_bucket(Bucket=bucket_a)
    s3_client.create_bucket(Bucket=bucket_b)

    fs_a = s3files_client.create_file_system(
        bucket=f"arn:aws:s3:::{bucket_a}", roleArn=shared_test_role,
    )['fileSystemId']
    fs_b = s3files_client.create_file_system(
        bucket=f"arn:aws:s3:::{bucket_b}", roleArn=shared_test_role,
    )['fileSystemId']
    try:
        # Wildcard for region+account in the policy ARN — the
        # handler fills the account segment with the actual
        # caller's account-id. We use `*` so the test isn't
        # coupled to a specific account-id format.
        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3files:GetFileSystem",
                "Resource": f"arn:aws:s3files::*:file-system/{fs_a}",
            }],
        }
        client = make_scoped_client(policy)

        # fs_a is permitted.
        got = client.get_file_system(fileSystemId=fs_a)
        assert got['fileSystemId'] == fs_a

        # fs_b is denied — same action, different resource.
        with pytest.raises(client.exceptions.ClientError) as exc:
            client.get_file_system(fileSystemId=fs_b)
        assert exc.value.response['Error']['Code'] == 'AccessDenied'
    finally:
        for fs_id in (fs_a, fs_b):
            try:
                s3files_client.delete_file_system(fileSystemId=fs_id)
            except Exception:
                pass
        for b in (bucket_a, bucket_b):
            try:
                s3_client.delete_bucket(Bucket=b)
            except Exception:
                pass


@pytest.mark.permissions
def test_explicit_deny_overrides_allow(make_scoped_client, test_file_system):
    """An explicit Deny statement overrides a broader Allow.
    `Allow s3files:* on *` + `Deny s3files:DeleteFileSystem on *`
    permits Get but denies Delete."""
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "s3files:*", "Resource": "*"},
            {
                "Effect": "Deny",
                "Action": "s3files:DeleteFileSystem",
                "Resource": "*",
            },
        ],
    }
    client = make_scoped_client(policy)
    fs_id = test_file_system['fileSystemId']

    # Get is allowed by the wildcard Allow.
    client.get_file_system(fileSystemId=fs_id)

    # Delete is explicitly denied.
    with pytest.raises(client.exceptions.ClientError) as exc:
        client.delete_file_system(fileSystemId=fs_id)
    assert exc.value.response['Error']['Code'] == 'AccessDenied'


@pytest.mark.permissions
def test_account_root_bypasses_policy(s3files_client):
    """The account-root user (testid in our test config) has no
    inline policy attached and yet calls succeed — root bypasses
    IAM policy evaluation. This is the symmetric guard for
    test_no_policy_denies_all: prove root works without a policy
    and prove non-root doesn't."""
    # If this raised AccessDenied the conformance suite would have
    # already failed; this test pins the property explicitly.
    s3files_client.list_file_systems()
