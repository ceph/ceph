"""Smoke tests for the RGW S3 Files API harness.

These tests confirm that the boto3 client is available locally
(validates the boto3 version pin) and that the configured RGW
endpoint accepts a request at the transport layer. They do not
yet exercise any specific operation semantics — those live in
the per-op conformance tests.
"""

import logging

import pytest
from botocore.exceptions import EndpointConnectionError, ClientError

from . import make_client

log = logging.getLogger(__name__)


@pytest.mark.smoke
def test_boto3_has_s3files_client():
    """Confirms the installed boto3 carries the s3files service.

    No network traffic — this validates the requirements pin
    (boto3 >= 1.42.0) and catches environments where an older
    boto3 would silently leave operations missing.
    """
    client = make_client('s3files')
    assert client is not None

    # Spot-check a representative slice of the operation surface so
    # an older boto3 is caught here rather than mid-conformance run.
    expected_ops = [
        'create_file_system',
        'get_file_system',
        'list_file_systems',
        'delete_file_system',
        'put_file_system_policy',
        'put_synchronization_configuration',
        'create_access_point',
        'create_mount_target',
        'update_mount_target',
        'list_tags_for_resource',
        'tag_resource',
    ]
    missing = [op for op in expected_ops if not hasattr(client, op)]
    assert not missing, (
        f"boto3 s3files client is missing operations: {missing}. "
        "Upgrade boto3 to >= 1.42.0."
    )


@pytest.mark.smoke
def test_rgw_endpoint_reachable():
    """Confirms the configured RGW endpoint is reachable.

    Until the `s3files` API is enabled in rgw_enable_apis and
    handlers ship, this call will return an HTTP-level error
    (404, 501, etc.). Any ClientError is acceptable here — it
    proves the transport works. Only a connection-level failure
    fails the test.

    Once handlers exist, this test refines into a positive
    list_file_systems assertion in test_list_file_systems.py.
    """
    client = make_client('s3files')
    try:
        client.list_file_systems()
    except EndpointConnectionError as e:
        pytest.fail(
            f"RGW endpoint not reachable: {e}. "
            "Check the host/port/scheme in the file referenced by "
            "S3FILES_TESTS_CONF."
        )
    except ClientError as e:
        log.info(
            "Endpoint reachable; received expected HTTP-level error "
            "before handlers ship: %s", e
        )
