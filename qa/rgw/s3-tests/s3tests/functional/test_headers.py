from io import StringIO
import boto.connection
import boto.exception
import boto.s3.connection
import boto.s3.acl
import boto.utils
import pytest
import operator
import random
import string
import socket
import ssl
import os
import re
from email.utils import formatdate

from urllib.parse import urlparse

from boto.s3.connection import S3Connection

from .utils import assert_raises

from email.header import decode_header

from . import (
    configfile,
    setup_teardown,
    _make_raw_request,
    nuke_prefixed_buckets,
    get_new_bucket,
    s3,
    config,
    get_prefix,
    TargetConnection,
    targets,
    )


_orig_authorize = None
_custom_headers = {}
_remove_headers = []


# HeaderS3Connection and _our_authorize are necessary to be able to arbitrarily
# overwrite headers. Depending on the version of boto, one or the other is
# necessary. We later determine in setup what needs to be used.

def _update_headers(headers):
    """ update a set of headers with additions/removals
    """
    global _custom_headers, _remove_headers

    headers.update(_custom_headers)

    for header in _remove_headers:
        try:
            del headers[header]
        except KeyError:
            pass


# Note: We need to update the headers twice. The first time so the
# authentication signing is done correctly. The second time to overwrite any
# headers modified or created in the authentication step.

class HeaderS3Connection(S3Connection):
    """ establish an authenticated connection w/customized headers
    """
    def fill_in_auth(self, http_request, **kwargs):
        _update_headers(http_request.headers)
        S3Connection.fill_in_auth(self, http_request, **kwargs)
        _update_headers(http_request.headers)

        return http_request


def _our_authorize(self, connection, **kwargs):
    """ perform an authentication w/customized headers
    """
    _update_headers(self.headers)
    _orig_authorize(self, connection, **kwargs)
    _update_headers(self.headers)


@pytest.fixture
def hook_headers(setup_teardown):
    boto_type = None
    _orig_conn = {}

    # we determine what we need to replace by the existence of particular
    # attributes. boto 2.0rc1 as fill_in_auth for S3Connection, while boto 2.0
    # has authorize for HTTPRequest.
    if hasattr(S3Connection, 'fill_in_auth'):
        boto_type = 'S3Connection'
        for conn in s3:
            _orig_conn[conn] = s3[conn]
            header_conn = HeaderS3Connection(
                aws_access_key_id=s3[conn].aws_access_key_id,
                aws_secret_access_key=s3[conn].aws_secret_access_key,
                is_secure=s3[conn].is_secure,
                port=s3[conn].port,
                host=s3[conn].host,
                calling_format=s3[conn].calling_format
                )

            s3[conn] = header_conn
    elif hasattr(boto.connection.HTTPRequest, 'authorize'):
        global _orig_authorize

        boto_type = 'HTTPRequest'

        _orig_authorize = boto.connection.HTTPRequest.authorize
        boto.connection.HTTPRequest.authorize = _our_authorize
    else:
        raise RuntimeError

    yield

    # replace original functionality depending on the boto version
    if boto_type is 'S3Connection':
        for conn in s3:
            s3[conn] = _orig_conn[conn]
        _orig_conn = {}
    elif boto_type is 'HTTPRequest':
        boto.connection.HTTPRequest.authorize = _orig_authorize
        _orig_authorize = None
    else:
        raise RuntimeError


def _clear_custom_headers():
    """ Eliminate any header customizations
    """
    global _custom_headers, _remove_headers
    _custom_headers = {}
    _remove_headers = []

@pytest.fixture(autouse=True)
def clear_custom_headers(setup_teardown, hook_headers):
    yield
    _clear_custom_headers() # clear headers before teardown()

def _add_custom_headers(headers=None, remove=None):
    """ Define header customizations (additions, replacements, removals)
    """
    global _custom_headers, _remove_headers
    if not _custom_headers:
        _custom_headers = {}

    if headers is not None:
        _custom_headers.update(headers)
    if remove is not None:
        _remove_headers.extend(remove)


def _setup_bad_object(headers=None, remove=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket = get_new_bucket()

    _add_custom_headers(headers=headers, remove=remove)
    return bucket.new_key('foo')

#
# common tests
#

@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_object_create_bad_contentlength_none():
    key = _setup_bad_object(remove=('Content-Length',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 411
    assert e.reason == 'Length Required'
    assert e.error_code == 'MissingContentLength'


@pytest.mark.auth_common
@pytest.mark.fails_on_rgw
def test_object_create_bad_contentlength_mismatch_above():
    content = 'bar'
    length = len(content) + 1

    key = _setup_bad_object({'Content-Length': length})

    # Disable retries since key.should_retry will discard the response with
    # PleaseRetryException.
    def no_retry(response, chunked_transfer): return False
    key.should_retry = no_retry

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code == 'RequestTimeout'


@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_object_create_bad_authorization_empty():
    key = _setup_bad_object({'Authorization': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'AccessDenied'

@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_object_create_date_and_amz_date():
    date = formatdate(usegmt=True)
    key = _setup_bad_object({'Date': date, 'X-Amz-Date': date})
    key.set_contents_from_string('bar')

@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_object_create_amz_date_and_no_date():
    date = formatdate(usegmt=True)
    key = _setup_bad_object({'X-Amz-Date': date}, ('Date',))
    key.set_contents_from_string('bar')


# the teardown is really messed up here. check it out
@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_object_create_bad_authorization_none():
    key = _setup_bad_object(remove=('Authorization',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'AccessDenied'


@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_bucket_create_contentlength_none():
    _add_custom_headers(remove=('Content-Length',))
    get_new_bucket()


@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_object_acl_create_contentlength_none():
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    key.set_contents_from_string('blah')

    _add_custom_headers(remove=('Content-Length',))
    key.set_acl('public-read')

def _create_new_connection():
    # We're going to need to manually build a connection using bad authorization info.
    # But to save the day, lets just hijack the settings from s3.main. :)
    main = s3.main
    conn = HeaderS3Connection(
        aws_access_key_id=main.aws_access_key_id,
        aws_secret_access_key=main.aws_secret_access_key,
        is_secure=main.is_secure,
        port=main.port,
        host=main.host,
        calling_format=main.calling_format,
        )
    return TargetConnection(targets.main.default.conf, conn)

@pytest.mark.auth_common
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_contentlength_empty():
    conn = _create_new_connection()
    _add_custom_headers({'Content-Length': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket, conn)
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case


@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_bucket_create_bad_contentlength_none():
    _add_custom_headers(remove=('Content-Length',))
    bucket = get_new_bucket()


@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_bucket_create_bad_authorization_empty():
    _add_custom_headers({'Authorization': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'AccessDenied'


# the teardown is really messed up here. check it out
@pytest.mark.auth_common
@pytest.mark.fails_on_dbstore
def test_bucket_create_bad_authorization_none():
    _add_custom_headers(remove=('Authorization',))
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'AccessDenied'

#
# AWS2 specific tests
#

@pytest.mark.auth_aws2
@pytest.mark.fails_on_dbstore
def test_object_create_bad_contentlength_mismatch_below_aws2():
    check_aws2_support()
    content = 'bar'
    length = len(content) - 1
    key = _setup_bad_object({'Content-Length': length})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code == 'BadDigest'


@pytest.mark.auth_aws2
@pytest.mark.fails_on_dbstore
def test_object_create_bad_authorization_incorrect_aws2():
    check_aws2_support()
    key = _setup_bad_object({'Authorization': 'AWS AKIAIGR7ZNNBHC5BKSUB:FWeDfwojDSdS2Ztmpfeubhd9isU='})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch', 'InvalidAccessKeyId')


@pytest.mark.auth_aws2
@pytest.mark.fails_on_dbstore
def test_object_create_bad_authorization_invalid_aws2():
    check_aws2_support()
    key = _setup_bad_object({'Authorization': 'AWS HAHAHA'})
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code == 'InvalidArgument'

@pytest.mark.auth_aws2
@pytest.mark.fails_on_dbstore
def test_object_create_bad_date_none_aws2():
    check_aws2_support()
    key = _setup_bad_object(remove=('Date',))
    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'AccessDenied'


@pytest.mark.auth_aws2
def test_bucket_create_bad_authorization_invalid_aws2():
    check_aws2_support()
    _add_custom_headers({'Authorization': 'AWS HAHAHA'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code == 'InvalidArgument'

@pytest.mark.auth_aws2
@pytest.mark.fails_on_dbstore
def test_bucket_create_bad_date_none_aws2():
    check_aws2_support()
    _add_custom_headers(remove=('Date',))
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'AccessDenied'

#
# AWS4 specific tests
#

def check_aws4_support():
    if 'S3_USE_SIGV4' not in os.environ:
        pytest.skip('sigv4 tests not enabled by S3_USE_SIGV4')

def check_aws2_support():
    if 'S3_USE_SIGV4' in os.environ:
        pytest.skip('sigv2 tests disabled by S3_USE_SIGV4')


@pytest.mark.auth_aws4
def test_object_create_bad_md5_invalid_garbage_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Content-MD5':'AWS4 HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code == 'InvalidDigest'


@pytest.mark.auth_aws4
def test_object_create_bad_contentlength_mismatch_below_aws4():
    check_aws4_support()
    content = 'bar'
    length = len(content) - 1
    key = _setup_bad_object({'Content-Length': length})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, content)
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code == 'XAmzContentSHA256Mismatch'


@pytest.mark.auth_aws4
def test_object_create_bad_authorization_incorrect_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Authorization': 'AWS4-HMAC-SHA256 Credential=AKIAIGR7ZNNBHC5BKSUB/20150930/us-east-1/s3/aws4_request,SignedHeaders=host;user-agent,Signature=FWeDfwojDSdS2Ztmpfeubhd9isU='})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch', 'InvalidAccessKeyId')


@pytest.mark.auth_aws4
def test_object_create_bad_authorization_invalid_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Authorization': 'AWS4-HMAC-SHA256 Credential=HAHAHA'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code in ('AuthorizationHeaderMalformed', 'InvalidArgument')


@pytest.mark.auth_aws4
def test_object_create_bad_ua_empty_aws4():
    check_aws4_support()
    key = _setup_bad_object({'User-Agent': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'SignatureDoesNotMatch'


@pytest.mark.auth_aws4
def test_object_create_bad_ua_none_aws4():
    check_aws4_support()
    key = _setup_bad_object(remove=('User-Agent',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'SignatureDoesNotMatch'


@pytest.mark.auth_aws4
def test_object_create_bad_date_invalid_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Bad Date'})
    key.set_contents_from_string('bar')


@pytest.mark.auth_aws4
def test_object_create_bad_amz_date_invalid_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': 'Bad Date'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_object_create_bad_date_empty_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': ''})
    key.set_contents_from_string('bar')


@pytest.mark.auth_aws4
def test_object_create_bad_amz_date_empty_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': ''})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_object_create_bad_date_none_aws4():
    check_aws4_support()
    key = _setup_bad_object(remove=('Date',))
    key.set_contents_from_string('bar')


@pytest.mark.auth_aws4
def test_object_create_bad_amz_date_none_aws4():
    check_aws4_support()
    key = _setup_bad_object(remove=('X-Amz-Date',))

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_object_create_bad_date_before_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2010 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@pytest.mark.auth_aws4
def test_object_create_bad_amz_date_before_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '20100707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_object_create_bad_date_after_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 2030 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@pytest.mark.auth_aws4
def test_object_create_bad_amz_date_after_today_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '20300707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_object_create_bad_date_before_epoch_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 1950 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@pytest.mark.auth_aws4
def test_object_create_bad_amz_date_before_epoch_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '19500707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_object_create_bad_date_after_end_aws4():
    check_aws4_support()
    key = _setup_bad_object({'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'})
    key.set_contents_from_string('bar')


@pytest.mark.auth_aws4
def test_object_create_bad_amz_date_after_end_aws4():
    check_aws4_support()
    key = _setup_bad_object({'X-Amz-Date': '99990707T215304Z'})

    e = assert_raises(boto.exception.S3ResponseError, key.set_contents_from_string, 'bar')
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_object_create_missing_signed_custom_header_aws4():
    check_aws4_support()
    method='PUT'
    expires_in='100000'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    body='zoo'

    # compute the signature with 'x-amz-foo=bar' in the headers...
    request_headers = {'x-amz-foo':'bar'}
    url = key.generate_url(expires_in, method=method, headers=request_headers)

    o = urlparse(url)
    path = o.path + '?' + o.query

    # avoid sending 'x-amz-foo=bar' in the headers
    request_headers.pop('x-amz-foo')

    res =_make_raw_request(host=s3.main.host, port=s3.main.port, method=method, path=path,
                           body=body, request_headers=request_headers, secure=s3.main.is_secure)

    assert res.status == 403
    assert res.reason == 'Forbidden'


@pytest.mark.auth_aws4
def test_object_create_missing_signed_header_aws4():
    check_aws4_support()
    method='PUT'
    expires_in='100000'
    bucket = get_new_bucket()
    key = bucket.new_key('foo')
    body='zoo'

    # compute the signature...
    request_headers = {}
    url = key.generate_url(expires_in, method=method, headers=request_headers)

    o = urlparse(url)
    path = o.path + '?' + o.query

    # 'X-Amz-Expires' is missing
    target = r'&X-Amz-Expires=' + expires_in
    path = re.sub(target, '', path)

    res =_make_raw_request(host=s3.main.host, port=s3.main.port, method=method, path=path,
                           body=body, request_headers=request_headers, secure=s3.main.is_secure)

    assert res.status == 403
    assert res.reason == 'Forbidden'


@pytest.mark.auth_aws4
def test_bucket_create_bad_authorization_invalid_aws4():
    check_aws4_support()
    _add_custom_headers({'Authorization': 'AWS4 HAHAHA'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 400
    assert e.reason.lower() == 'bad request' # some proxies vary the case
    assert e.error_code == 'InvalidArgument'


@pytest.mark.auth_aws4
def test_bucket_create_bad_ua_empty_aws4():
    check_aws4_support()
    _add_custom_headers({'User-Agent': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'SignatureDoesNotMatch'

@pytest.mark.auth_aws4
def test_bucket_create_bad_ua_none_aws4():
    check_aws4_support()
    _add_custom_headers(remove=('User-Agent',))

    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)
    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code == 'SignatureDoesNotMatch'


@pytest.mark.auth_aws4
def test_bucket_create_bad_date_invalid_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Bad Date'})
    get_new_bucket()


@pytest.mark.auth_aws4
def test_bucket_create_bad_amz_date_invalid_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': 'Bad Date'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_bucket_create_bad_date_empty_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': ''})
    get_new_bucket()


@pytest.mark.auth_aws4
def test_bucket_create_bad_amz_date_empty_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': ''})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')

@pytest.mark.auth_aws4
def test_bucket_create_bad_date_none_aws4():
    check_aws4_support()
    _add_custom_headers(remove=('Date',))
    get_new_bucket()


@pytest.mark.auth_aws4
def test_bucket_create_bad_amz_date_none_aws4():
    check_aws4_support()
    _add_custom_headers(remove=('X-Amz-Date',))
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_bucket_create_bad_date_before_today_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Tue, 07 Jul 2010 21:53:04 GMT'})
    get_new_bucket()


@pytest.mark.auth_aws4
def test_bucket_create_bad_amz_date_before_today_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': '20100707T215304Z'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_bucket_create_bad_date_after_today_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Tue, 07 Jul 2030 21:53:04 GMT'})
    get_new_bucket()


@pytest.mark.auth_aws4
def test_bucket_create_bad_amz_date_after_today_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': '20300707T215304Z'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('RequestTimeTooSkewed', 'SignatureDoesNotMatch')


@pytest.mark.auth_aws4
def test_bucket_create_bad_date_before_epoch_aws4():
    check_aws4_support()
    _add_custom_headers({'Date': 'Tue, 07 Jul 1950 21:53:04 GMT'})
    get_new_bucket()


@pytest.mark.auth_aws4
def test_bucket_create_bad_amz_date_before_epoch_aws4():
    check_aws4_support()
    _add_custom_headers({'X-Amz-Date': '19500707T215304Z'})
    e = assert_raises(boto.exception.S3ResponseError, get_new_bucket)

    assert e.status == 403
    assert e.reason == 'Forbidden'
    assert e.error_code in ('AccessDenied', 'SignatureDoesNotMatch')
