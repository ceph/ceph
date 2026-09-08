import pytest
from botocore.exceptions import ClientError, ParamValidationError
from . import setup, get_config_host, get_config_port, get_access_key, get_secret_key
import boto3


def _has_bucket_logging_extension():
    """Check if boto3 supports the LoggingType field (sdk-extras.json installed)."""
    hostname = get_config_host()
    port = get_config_port()
    if port in (443, 8443):
        endpoint_url = f'https://{hostname}:{port}'
    else:
        endpoint_url = f'http://{hostname}:{port}'

    client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=get_access_key(),
        aws_secret_access_key=get_secret_key(),
        verify=False
    )
    try:
        client.put_bucket_logging(
            Bucket='nonexistent-probe-bucket',
            BucketLoggingStatus={
                'LoggingEnabled': {
                    'TargetBucket': 'nonexistent-probe-log',
                    'TargetPrefix': 'log/',
                    'LoggingType': 'Journal'
                }
            }
        )
    except ParamValidationError:
        return False
    except ClientError:
        return True
    return True


def pytest_addoption(parser):
    parser.addoption(
        "--logging-type",
        choices=["Standard", "Journal"],
        default="Standard",
        help="Bucket logging type to test: Standard (default) or Journal",
    )


@pytest.fixture(autouse=True, scope="session")
def setup_config(request):
    setup()
    logging_type = request.config.getoption("--logging-type")
    if logging_type == "Journal" and not _has_bucket_logging_extension():
        pytest.exit(
            "Journal mode requested but boto3 LoggingType extension is not available. "
            "Install service-2.sdk-extras.json to ~/.aws/models/s3/2006-03-01/",
            returncode=1,
        )


@pytest.fixture(scope="session")
def logging_type(request):
    return request.config.getoption("--logging-type")
