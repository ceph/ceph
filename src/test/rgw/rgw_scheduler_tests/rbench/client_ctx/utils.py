import platform
import logging
from urllib.parse import urljoin
import boto3

def assert_py_version():
    major, minor, _ = platform.python_version_tuple()
    assert int(major) >= 3 and int(minor) >=5

def normalise_url_path(baseurl, path):
    # TODO validate the end slash for pure hostname endpoints or signature
    # calculations will fail
    return urljoin(baseurl, path)

def create_buffer(sz):
    data = b"a"*sz
    mv = memoryview(data)
    return mv

def str_to_log_level(log_level='INFO'):
    s = log_level.upper()
    if s == 'DEBUG':
        return logging.DEBUG
    elif s == 'INFO':
        return logging.INFO
    elif s == 'WARNING':
        return logging.WARNING
    elif s == 'ERROR':
        return logging.ERRROR
    else:
        raise ValueError('Invalid Log Level')

def create_buckets(buckets, base_url, creds):
    # A simple boto3 create buckets function. While technically just setting
    # PUT bucket with the async client should suffice, we'd need to handle the
    # exception and stop the ev loop, let's stick to simple boto3 call for now
    c = boto3.client('s3','us-east-1',
                     endpoint_url = base_url,
                     aws_access_key_id = creds['access_key'],
                     aws_secret_access_key = creds['secret_key']
    )
    for bucket in buckets:
        c.create_bucket(Bucket=bucket)
