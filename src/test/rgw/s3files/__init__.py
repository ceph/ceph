import configparser
import os

import boto3


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


def make_client(service_name):
    """Construct a boto3 client of the given service against the
    configured RGW endpoint, using the configured credentials.

    Examples:
        make_client('s3files')
        make_client('s3')
        make_client('iam')
    """
    return boto3.client(
        service_name,
        endpoint_url=get_endpoint_url(),
        aws_access_key_id=get_access_key(),
        aws_secret_access_key=get_secret_key(),
        region_name='default',
    )
