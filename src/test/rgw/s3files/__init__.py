import configparser
import os

import boto3
from botocore.exceptions import ParamValidationError


def validation_excs(s3files_client):
    """Acceptable exceptions for a Smithy-trait violation that can
    be caught either client-side or server-side.

    boto3's Smithy client validator enforces `@required`,
    `@length`, `@pattern`, and `@range` traits before sending the
    request, raising ParamValidationError. RGW also enforces the
    same traits on the wire, raising ValidationException. Tests
    that exercise one of these traits accept either path — the
    contract is enforced, regardless of which side enforces it.

    A future raw-HTTP test would bypass boto3's validator and
    verify the server-side path independently; that is tracked
    in the coverage-gap task.
    """
    return (
        s3files_client.exceptions.ValidationException,
        ParamValidationError,
    )


# Test mode: which target are we exercising?
#
# - "rgw" (default): asserts both the typed exception AND the
#   RGW-defined errorCode string from errors.py.
# - "aws": asserts only the typed exception. The Smithy spec types
#   `errorCode` as a non-empty string but doesn't fix the value;
#   AWS does not publish theirs. Running against real AWS S3 Files
#   needs the value-level asserts to be skipped.
def _test_mode():
    return os.environ.get('S3FILES_TESTS_MODE', 'rgw').lower()


def assert_errorcode(exc_value, expected):
    """Assert that the exception carries an `errorCode` (or one of
    a set of acceptable values) per the Smithy contract, but only
    when running against RGW.

    `expected` is either a string or an iterable of acceptable
    strings. In AWS mode the assertion is a no-op (the typed
    exception was already enforced by `pytest.raises`).
    """
    if _test_mode() != 'rgw':
        return
    err = exc_value.response
    actual = err.get('errorCode')
    if isinstance(expected, str):
        assert actual == expected, (
            f"expected errorCode={expected!r}, got {actual!r} "
            f"in response: {err}"
        )
    else:
        expected_set = set(expected)
        assert actual in expected_set, (
            f"expected errorCode in {sorted(expected_set)}, "
            f"got {actual!r} in response: {err}"
        )


def setup():
    cfg = configparser.RawConfigParser()
    try:
        path = os.environ['S3FILES_TESTS_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment variable '
            'S3FILES_TESTS_CONF to a config file.',
        )
    cfg.read(path)

    if not cfg.defaults():
        raise RuntimeError('Your config file is missing the DEFAULT section!')
    if not cfg.has_section("s3 main"):
        raise RuntimeError('Your config file is missing the "s3 main" section!')

    defaults = cfg.defaults()

    global default_host
    default_host = defaults.get("host")

    global default_port
    default_port = int(defaults.get("port"))

    global default_scheme
    default_scheme = defaults.get("scheme", "http")

    global main_access_key
    main_access_key = cfg.get('s3 main', "access_key")

    global main_secret_key
    main_secret_key = cfg.get('s3 main', "secret_key")

    global main_user_id
    main_user_id = cfg.get('s3 main', 'user_id')

    global main_zone_id
    main_zone_id = cfg.get('s3 main', 'zone_id', fallback='default')


def get_endpoint_url():
    global default_scheme, default_host, default_port
    return f"{default_scheme}://{default_host}:{default_port}"


def get_access_key():
    global main_access_key
    return main_access_key


def get_secret_key():
    global main_secret_key
    return main_secret_key


def get_user_id():
    global main_user_id
    return main_user_id


def get_zone_id():
    global main_zone_id
    return main_zone_id


def make_subnet_id(zone_id):
    """Encode a Ceph zone-id as a value that satisfies the AWS
    `SubnetId` Smithy pattern `^subnet-[0-9a-f]{8,40}$`.

    The zone-id is required to be hex (8..40 chars) so it can be
    passed through verbatim. RGW strips the `subnet-` prefix at
    the API layer to recover the zone-id.
    """
    return f"subnet-{zone_id}"


# Pattern-valid IDs that are vanishingly unlikely to exist. Used by
# tests that exercise "resource not found" paths without tripping
# the boto3 client-side regex validator (the Smithy patterns are
# `^fs-[0-9a-f]{17,40}$`, `^fsap-...`, `^fsmt-...`).
NONEXISTENT_FS_ID = "fs-" + "0" * 32
NONEXISTENT_AP_ID = "fsap-" + "0" * 32
NONEXISTENT_MT_ID = "fsmt-" + "0" * 32


def make_client(service_name, *, access_key=None, secret_key=None):
    """Construct a boto3 client of the given service against the
    configured RGW endpoint.

    By default uses the credentials from the test config (the
    account-root user). Pass `access_key` and `secret_key` to use
    a different principal — for example a non-root IAM user
    minted within a test to exercise least-privilege policies.

    Examples:
        make_client('s3files')
        make_client('iam')
        make_client('s3files', access_key=ak, secret_key=sk)
    """
    return boto3.client(
        service_name,
        endpoint_url=get_endpoint_url(),
        aws_access_key_id=access_key or get_access_key(),
        aws_secret_access_key=secret_key or get_secret_key(),
        region_name='default',
    )
