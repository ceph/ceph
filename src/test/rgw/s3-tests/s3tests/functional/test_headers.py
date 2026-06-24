import boto3
import pytest
import botocore.config
from botocore.exceptions import ClientError
from email.utils import formatdate

from .utils import assert_raises
from .utils import _get_status_and_error_code
from .utils import _get_status

from . import (
    configfile,
    setup_teardown,
    get_client,
    get_v2_client,
    get_new_bucket,
    get_new_bucket_name,
    )

def _add_header_create_object(headers, client=None):
    """ Create a new bucket, add an object w/header customizations
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutObject', add_headers)
    client.put_object(Bucket=bucket_name, Key=key_name)

    return bucket_name, key_name


def _add_header_create_bad_object(headers, client=None):
    """ Create a new bucket, add an object with a header. This should cause a failure 
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutObject', add_headers)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')

    return e


def _remove_header_create_object(remove, client=None):
    """ Create a new bucket, add an object without a header
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.PutObject', remove_header)
    client.put_object(Bucket=bucket_name, Key=key_name)

    return bucket_name, key_name

def _remove_header_create_bad_object(remove, client=None):
    """ Create a new bucket, add an object without a header. This should cause a failure
    """
    bucket_name = get_new_bucket()
    if client == None:
        client = get_client()
    key_name = 'foo'

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.PutObject', remove_header)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')

    return e


def _add_header_create_bucket(headers, client=None):
    """ Create a new bucket, w/header customizations
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.CreateBucket', add_headers)
    client.create_bucket(Bucket=bucket_name)

    return bucket_name


def _add_header_create_bad_bucket(headers=None, client=None):
    """ Create a new bucket, w/header customizations that should cause a failure 
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # pass in custom headers before PutObject call
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.CreateBucket', add_headers)
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)

    return e


def _remove_header_create_bucket(remove, client=None):
    """ Create a new bucket, without a header
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.CreateBucket', remove_header)
    client.create_bucket(Bucket=bucket_name)

    return bucket_name

def _remove_header_create_bad_bucket(remove, client=None):
    """ Create a new bucket, without a header. This should cause a failure
    """
    bucket_name = get_new_bucket_name()
    if client == None:
        client = get_client()

    # remove custom headers before PutObject call
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.CreateBucket', remove_header)
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)

    return e

#
# common tests
#

@pytest.mark.auth_common
def test_object_create_bad_md5_invalid_short():
    e = _add_header_create_bad_object({'Content-MD5':'YWJyYWNhZGFicmE='})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidDigest'

@pytest.mark.auth_common
def test_object_create_bad_md5_bad():
    e = _add_header_create_bad_object({'Content-MD5':'rL0Y20xC+Fzt72VPzMSk2A=='})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'BadDigest'

@pytest.mark.auth_common
def test_object_create_bad_md5_empty():
    e = _add_header_create_bad_object({'Content-MD5':''})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidDigest'

@pytest.mark.auth_common
def test_object_create_bad_md5_none():
    bucket_name, key_name = _remove_header_create_object('Content-MD5')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_common
def test_object_create_bad_expect_mismatch():
    bucket_name, key_name = _add_header_create_object({'Expect': 200})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_common
def test_object_create_bad_expect_empty():
    bucket_name, key_name = _add_header_create_object({'Expect': ''})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_common
def test_object_create_bad_expect_none():
    bucket_name, key_name = _remove_header_create_object('Expect')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@pytest.mark.fails_on_rgw
def test_object_create_bad_contentlength_empty():
    e = _add_header_create_bad_object({'Content-Length':''})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.auth_common
@pytest.mark.fails_on_mod_proxy_fcgi
def test_object_create_bad_contentlength_negative():
    # to test Content-Length=-1, we have to prevent the checksum calculation
    # from switching to STREAMING-UNSIGNED-PAYLOAD-TRAILER
    config = botocore.config.Config(request_checksum_calculation = 'when_required')
    client = get_client(config)
    bucket_name = get_new_bucket()
    key_name = 'foo'
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, ContentLength=-1)
    status = _get_status(e.response)
    assert status == 400

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@pytest.mark.fails_on_rgw
def test_object_create_bad_contentlength_none():
    remove = 'Content-Length'
    e = _remove_header_create_bad_object('Content-Length')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 411
    assert error_code == 'MissingContentLength'

@pytest.mark.auth_common
def test_object_create_bad_contenttype_invalid():
    bucket_name, key_name = _add_header_create_object({'Content-Type': 'text/plain'})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_common
def test_object_create_bad_contenttype_empty():
    client = get_client()
    key_name = 'foo'
    bucket_name = get_new_bucket()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar', ContentType='')

@pytest.mark.auth_common
def test_object_create_bad_contenttype_none():
    bucket_name = get_new_bucket()
    key_name = 'foo'
    client = get_client()
    # as long as ContentType isn't specified in put_object it isn't going into the request
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')


@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the authorization header
@pytest.mark.fails_on_rgw
def test_object_create_bad_authorization_empty():
    e = _add_header_create_bad_object({'Authorization': ''})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to pass both the 'Date' and 'X-Amz-Date' header during signing and not 'X-Amz-Date' before
@pytest.mark.fails_on_rgw
def test_object_create_date_and_amz_date():
    date = formatdate(usegmt=True)
    bucket_name, key_name = _add_header_create_object({'Date': date, 'X-Amz-Date': date})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to pass both the 'Date' and 'X-Amz-Date' header during signing and not 'X-Amz-Date' before
@pytest.mark.fails_on_rgw
def test_object_create_amz_date_and_no_date():
    date = formatdate(usegmt=True)
    bucket_name, key_name = _add_header_create_object({'Date': '', 'X-Amz-Date': date})
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

# the teardown is really messed up here. check it out
@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the authorization header
@pytest.mark.fails_on_rgw
def test_object_create_bad_authorization_none():
    e = _remove_header_create_bad_object('Authorization')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@pytest.mark.fails_on_rgw
def test_bucket_create_contentlength_none():
    remove = 'Content-Length'
    _remove_header_create_bucket(remove)

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@pytest.mark.fails_on_rgw
def test_object_acl_create_contentlength_none():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    remove = 'Content-Length'
    def remove_header(**kwargs):
        if (remove in kwargs['params']['headers']):
            del kwargs['params']['headers'][remove]

    client.meta.events.register('before-call.s3.PutObjectAcl', remove_header)
    client.put_object_acl(Bucket=bucket_name, Key='foo', ACL='public-read')

@pytest.mark.auth_common
def test_bucket_put_bad_canned_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    headers = {'x-amz-acl': 'public-ready'}
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutBucketAcl', add_headers)

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL='public-read')
    status = _get_status(e.response)
    assert status == 400

@pytest.mark.auth_common
def test_bucket_create_bad_expect_mismatch():
    bucket_name = get_new_bucket_name()
    client = get_client()

    headers = {'Expect': 200}
    add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.CreateBucket', add_headers)
    client.create_bucket(Bucket=bucket_name)

@pytest.mark.auth_common
def test_bucket_create_bad_expect_empty():
    headers = {'Expect': ''}
    _add_header_create_bucket(headers)

@pytest.mark.auth_common
# TODO: The request isn't even making it to the RGW past the frontend
# This test had 'fails_on_rgw' before the move to boto3
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_contentlength_empty():
    headers = {'Content-Length': ''}
    e = _add_header_create_bad_bucket(headers)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.auth_common
@pytest.mark.fails_on_mod_proxy_fcgi
def test_bucket_create_bad_contentlength_negative():
    headers = {'Content-Length': '-1'}
    e = _add_header_create_bad_bucket(headers)
    status = _get_status(e.response)
    assert status == 400

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the content-length header
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_contentlength_none():
    remove = 'Content-Length'
    _remove_header_create_bucket(remove)

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_authorization_empty():
    headers = {'Authorization': ''}
    e = _add_header_create_bad_bucket(headers)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_common
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_authorization_none():
    e = _remove_header_create_bad_bucket('Authorization')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
def test_object_create_bad_md5_invalid_garbage_aws2():
    v2_client = get_v2_client()
    headers = {'Content-MD5': 'AWS HAHAHA'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidDigest'

@pytest.mark.auth_aws2
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the Content-Length header
@pytest.mark.fails_on_rgw
def test_object_create_bad_contentlength_mismatch_below_aws2():
    v2_client = get_v2_client()
    content = 'bar'
    length = len(content) - 1
    headers = {'Content-Length': str(length)}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'BadDigest'

@pytest.mark.auth_aws2
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@pytest.mark.fails_on_rgw
def test_object_create_bad_authorization_incorrect_aws2():
    v2_client = get_v2_client()
    headers = {'Authorization': 'AWS AKIAIGR7ZNNBHC5BKSUB:FWeDfwojDSdS2Ztmpfeubhd9isU='}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'InvalidDigest'

@pytest.mark.auth_aws2
# TODO: remove 'fails_on_rgw' and once we have learned how to manipulate the authorization header
@pytest.mark.fails_on_rgw
def test_object_create_bad_authorization_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'Authorization': 'AWS HAHAHA'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.auth_aws2
def test_object_create_bad_ua_empty_aws2():
    v2_client = get_v2_client()
    headers = {'User-Agent': ''}
    bucket_name, key_name = _add_header_create_object(headers, v2_client)
    v2_client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_aws2
def test_object_create_bad_ua_none_aws2():
    v2_client = get_v2_client()
    remove = 'User-Agent'
    bucket_name, key_name = _remove_header_create_object(remove, v2_client)
    v2_client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

@pytest.mark.auth_aws2
def test_object_create_bad_date_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Bad Date'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
def test_object_create_bad_date_empty_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': ''}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the date header
@pytest.mark.fails_on_rgw
def test_object_create_bad_date_none_aws2():
    v2_client = get_v2_client()
    remove = 'x-amz-date'
    e = _remove_header_create_bad_object(remove, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
def test_object_create_bad_date_before_today_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 2010 21:53:04 GMT'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'RequestTimeTooSkewed'

@pytest.mark.auth_aws2
def test_object_create_bad_date_before_epoch_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 1950 21:53:04 GMT'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
def test_object_create_bad_date_after_end_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 9999 21:53:04 GMT'}
    e = _add_header_create_bad_object(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'RequestTimeTooSkewed'

@pytest.mark.auth_aws2
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the date header
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_authorization_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'Authorization': 'AWS HAHAHA'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.auth_aws2
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_ua_empty_aws2():
    v2_client = get_v2_client()
    headers = {'User-Agent': ''}
    _add_header_create_bucket(headers, v2_client)

@pytest.mark.auth_aws2
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_ua_none_aws2():
    v2_client = get_v2_client()
    remove = 'User-Agent'
    _remove_header_create_bucket(remove, v2_client)

@pytest.mark.auth_aws2
def test_bucket_create_bad_date_invalid_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Bad Date'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
def test_bucket_create_bad_date_empty_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': ''}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
# TODO: remove 'fails_on_rgw' and once we have learned how to remove the date header
@pytest.mark.fails_on_rgw
def test_bucket_create_bad_date_none_aws2():
    v2_client = get_v2_client()
    remove = 'x-amz-date'
    e = _remove_header_create_bad_bucket(remove, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.auth_aws2
def test_bucket_create_bad_date_before_today_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 2010 21:53:04 GMT'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'RequestTimeTooSkewed'

@pytest.mark.auth_aws2
def test_bucket_create_bad_date_after_today_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 2030 21:53:04 GMT'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'RequestTimeTooSkewed'

@pytest.mark.auth_aws2
def test_bucket_create_bad_date_before_epoch_aws2():
    v2_client = get_v2_client()
    headers = {'x-amz-date': 'Tue, 07 Jul 1950 21:53:04 GMT'}
    e = _add_header_create_bad_bucket(headers, v2_client)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
