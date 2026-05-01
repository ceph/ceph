"""Conformance tests for Ceph deviations from AWS S3 Files.

These tests capture intentional divergences between RGW's
implementation and the AWS S3 Files API spec. They live alongside
the per-op conformance tests but stay clearly distinguishable so
upstream-AWS conformance and Ceph-side intentional drift are
audited separately.

See doc/dev/radosgw/s3_files_api.rst for the prose discussion of
each divergence.
"""

import pytest

from . import errors, make_subnet_id


@pytest.mark.divergence
def test_subnet_id_carries_zone_id(
    s3files_client, test_file_system, test_subnet_id, test_zone_id
):
    """The AWS `subnetId` field carries a Ceph zone-id, encoded as
    `subnet-{zone_id}` to satisfy the Smithy pattern. The
    response's `availabilityZoneId` echoes the bare zone-id (no
    `subnet-` prefix), per AWS's "this MT lives in AZ X"
    convention."""
    resp = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_subnet_id,
    )
    try:
        assert resp['subnetId'] == test_subnet_id
        assert resp.get('availabilityZoneId') == test_zone_id
    finally:
        s3files_client.delete_mount_target(mountTargetId=resp['mountTargetId'])


@pytest.mark.divergence
def test_subnet_id_unknown_zone_rejected(
    s3files_client, test_file_system
):
    """An unknown zone-id (encoded as `subnet-{hex}` for some hex
    that doesn't name any zone in the period) is rejected with
    INVALID_ZONE_ID or ZONE_NOT_IN_ZONEGROUP. AWS would have
    returned `InvalidSubnetId.NotFound`."""
    bogus = make_subnet_id("ffffffffffffffffffffffffffffffff")
    with pytest.raises(s3files_client.exceptions.ValidationException) as exc:
        s3files_client.create_mount_target(
            fileSystemId=test_file_system['fileSystemId'],
            subnetId=bogus,
        )
    err = exc.value.response
    assert err.get('errorCode') in (
        errors.INVALID_ZONE_ID,
        errors.ZONE_NOT_IN_ZONEGROUP,
    ), err


@pytest.mark.divergence
def test_caller_supplied_ipv4_address_ignored(
    s3files_client, test_file_system, test_subnet_id
):
    """Caller-supplied ipv4Address is accepted and silently ignored.

    The actual ipv4Address in the response is determined by the
    files-placement binding (operator-configured per zone), not
    by what the caller passed. AWS would either accept the
    address (placing it within the subnet's CIDR) or reject it.
    """
    bogus = "203.0.113.99"  # documentation-range address (RFC 5737)
    resp = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_subnet_id,
        ipv4Address=bogus,
    )
    try:
        # If the placement resolved to a real VIP, it won't match
        # 203.0.113.99. If the placement isn't bound in this zone,
        # ipv4Address is absent.
        assert resp.get('ipv4Address') != bogus, (
            "RGW must ignore caller-supplied ipv4Address; got "
            f"{resp.get('ipv4Address')}"
        )
    finally:
        s3files_client.delete_mount_target(mountTargetId=resp['mountTargetId'])


@pytest.mark.divergence
def test_security_groups_stored_but_not_enforced(
    s3files_client, test_file_system, test_subnet_id
):
    """Caller-supplied securityGroups are stored on the spec and
    round-trip through GetMountTarget, but are not enforced at
    the data plane in v1 (no VPC SG model in Ceph). A future
    version may begin enforcement."""
    mt = s3files_client.create_mount_target(
        fileSystemId=test_file_system['fileSystemId'],
        subnetId=test_subnet_id,
    )
    mt_id = mt['mountTargetId']
    try:
        s3files_client.update_mount_target(
            mountTargetId=mt_id,
            securityGroups=["sg-divergence-1", "sg-divergence-2"],
        )
        got = s3files_client.get_mount_target(mountTargetId=mt_id)
        assert set(got.get('securityGroups', [])) == {
            "sg-divergence-1", "sg-divergence-2",
        }
    finally:
        s3files_client.delete_mount_target(mountTargetId=mt_id)


@pytest.mark.divergence
def test_role_arn_must_reference_ceph_iam_role(s3files_client, bucket_arn):
    """Unlike AWS, where roleArn references an AWS IAM role, Ceph
    requires the ARN to name a role that exists in the caller's
    Ceph IAM account (RGW already has account/role plumbing).
    Wire-level role assumption / delegation is deferred to a
    later version."""
    fake_arn = "arn:aws:iam::000000000000:role/never-created-role"
    with pytest.raises(
        s3files_client.exceptions.ResourceNotFoundException
    ) as exc:
        s3files_client.create_file_system(
            bucket=bucket_arn,
            roleArn=fake_arn,
        )
    err = exc.value.response
    assert err.get('errorCode') == errors.ROLE_NOT_FOUND, err


@pytest.mark.divergence
def test_synchronization_configuration_stored_not_enforced(
    s3files_client, test_file_system
):
    """RGW v1 stores sync configuration in FDB but the reconciler
    does not act on it; the data path serves the bucket directly.

    This test asserts the stored values round-trip; semantic
    enforcement (e.g., that data is fetched on first access per
    the import rule trigger) is not testable through the API and
    is documented in the design doc as out-of-scope for v1.
    """
    fs_id = test_file_system['fileSystemId']
    rules = [{
        "prefix": "",
        "trigger": "ON_FILE_ACCESS",
        "sizeLessThan": 1024 * 1024,
    }]
    expirations = [{"daysAfterLastAccess": 14}]
    s3files_client.put_synchronization_configuration(
        fileSystemId=fs_id,
        importDataRules=rules,
        expirationDataRules=expirations,
    )
    got = s3files_client.get_synchronization_configuration(fileSystemId=fs_id)
    assert got['importDataRules'] == rules
    assert got['expirationDataRules'] == expirations
