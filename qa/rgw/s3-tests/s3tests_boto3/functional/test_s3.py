import boto3
import botocore.session
import botocore.config
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
from botocore.handlers import validate_bucket_name
import isodate
import email.utils
import datetime
import threading
import re
import pytz
from collections import OrderedDict
import requests
import json
import base64
import hmac
import hashlib
import xml.etree.ElementTree as ET
import time
import operator
import pytest
import os
import string
import random
import socket
import dateutil.parser
import ssl
import pdb
from collections import namedtuple
from collections import defaultdict
from io import BytesIO

from .utils import assert_raises
from .utils import generate_random
from .utils import _get_status_and_error_code
from .utils import _get_status

from .policy import Policy, Statement, make_json_policy

from .iam import iam_root

from . import (
    configfile,
    setup_teardown,
    get_client,
    get_prefix,
    get_unauthenticated_client,
    get_bad_auth_client,
    get_v2_client,
    get_new_bucket,
    get_new_bucket_name,
    get_new_bucket_resource,
    get_config_is_secure,
    get_config_host,
    get_config_port,
    get_config_endpoint,
    get_config_ssl_verify,
    get_main_aws_access_key,
    get_main_aws_secret_key,
    get_main_display_name,
    get_main_user_id,
    get_main_email,
    get_main_api_name,
    get_alt_aws_access_key,
    get_alt_aws_secret_key,
    get_alt_display_name,
    get_alt_user_id,
    get_alt_email,
    get_alt_client,
    get_iam_root_client,
    get_iam_root_s3client,
    get_tenant_client,
    get_v2_tenant_client,
    get_tenant_iam_client,
    get_tenant_name,
    get_tenant_user_id,
    get_buckets_list,
    get_objects_list,
    get_main_kms_keyid,
    get_secondary_kms_keyid,
    get_svc_client,
    get_cloud_storage_class,
    get_cloud_retain_head_object,
    get_allow_read_through,
    get_cloud_regular_storage_class,
    get_cloud_target_path,
    get_cloud_target_storage_class,
    get_cloud_client,
    nuke_prefixed_buckets,
    configured_storage_classes,
    get_lc_debug_interval,
    get_restore_debug_interval,
    get_restore_processor_period,
    get_read_through_days,
    create_iam_user_s3client,
    )


def _bucket_is_empty(bucket):
    is_empty = True
    for obj in bucket.objects.all():
        is_empty = False
        break
    return is_empty

def test_bucket_list_empty():
    bucket = get_new_bucket_resource()
    is_empty = _bucket_is_empty(bucket)
    assert is_empty == True

@pytest.mark.list_objects_v2
def test_bucket_list_distinct():
    bucket1 = get_new_bucket_resource()
    bucket2 = get_new_bucket_resource()
    obj = bucket1.put_object(Body='str', Key='asdf')
    is_empty = _bucket_is_empty(bucket2)
    assert is_empty == True

def _create_objects(bucket=None, bucket_name=None, keys=[]):
    """
    Populate a (specified or new) bucket with objects with
    specified names (and contents identical to their names).
    """
    if bucket_name is None:
        bucket_name = get_new_bucket_name()
    if bucket is None:
        bucket = get_new_bucket_resource(name=bucket_name)

    for key in keys:
        obj = bucket.put_object(Body=key, Key=key)

    return bucket_name

def _get_keys(response):
    """
    return lists of strings that are the keys from a client.list_objects() response
    """
    keys = []
    if 'Contents' in response:
        objects_list = response['Contents']
        keys = [obj['Key'] for obj in objects_list]
    return keys

def _get_prefixes(response):
    """
    return lists of strings that are prefixes from a client.list_objects() response
    """
    prefixes = []
    if 'CommonPrefixes' in response:
        prefix_list = response['CommonPrefixes']
        prefixes = [prefix['Prefix'] for prefix in prefix_list]
    return prefixes

@pytest.mark.fails_on_dbstore
def test_bucket_list_many():
    bucket_name = _create_objects(keys=['foo', 'bar', 'baz'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 2
    assert keys == ['bar', 'baz']
    assert response['IsTruncated'] == True

    response = client.list_objects(Bucket=bucket_name, Marker='baz',MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 1
    assert response['IsTruncated'] == False
    assert keys == ['foo']

@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_many():
    bucket_name = _create_objects(keys=['foo', 'bar', 'baz'])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 2
    assert keys == ['bar', 'baz']
    assert response['IsTruncated'] == True

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter='baz',MaxKeys=2)
    keys = _get_keys(response)
    assert len(keys) == 1
    assert response['IsTruncated'] == False
    assert keys == ['foo']

@pytest.mark.list_objects_v2
def test_basic_key_count():
    client = get_client()
    bucket_names = []
    bucket_name = get_new_bucket_name()
    client.create_bucket(Bucket=bucket_name)
    for j in range(5):
            client.put_object(Bucket=bucket_name, Key=str(j))
    response1 = client.list_objects_v2(Bucket=bucket_name)
    assert response1['KeyCount'] == 5

def test_bucket_list_delimiter_basic():
    bucket_name = _create_objects(keys=['foo/bar', 'foo/bar/xyzzy', 'quux/thud', 'asdf'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/')
    assert response['Delimiter'] == '/'
    keys = _get_keys(response)
    assert keys == ['asdf']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ['foo/', 'quux/']

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_basic():
    bucket_name = _create_objects(keys=['foo/bar', 'foo/bar/xyzzy', 'quux/thud', 'asdf'])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    assert response['Delimiter'] == '/'
    keys = _get_keys(response)
    assert keys == ['asdf']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ['foo/', 'quux/']
    assert response['KeyCount'] == len(prefixes) + len(keys)


@pytest.mark.list_objects_v2
def test_bucket_listv2_encoding_basic():
    bucket_name = _create_objects(keys=['foo+1/bar', 'foo/bar/xyzzy', 'quux ab/thud', 'asdf+b'])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='/', EncodingType='url')
    assert response['Delimiter'] == '/'
    keys = _get_keys(response)
    assert keys == ['asdf%2Bb']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 3
    assert prefixes == ['foo%2B1/', 'foo/', 'quux%20ab/']

def test_bucket_list_encoding_basic():
    bucket_name = _create_objects(keys=['foo+1/bar', 'foo/bar/xyzzy', 'quux ab/thud', 'asdf+b'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/', EncodingType='url')
    assert response['Delimiter'] == '/'
    keys = _get_keys(response)
    assert keys == ['asdf%2Bb']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 3
    assert prefixes == ['foo%2B1/', 'foo/', 'quux%20ab/']


def validate_bucket_list(bucket_name, prefix, delimiter, marker, max_keys,
                         is_truncated, check_objs, check_prefixes, next_marker):
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter=delimiter, Marker=marker, MaxKeys=max_keys, Prefix=prefix)
    assert response['IsTruncated'] == is_truncated
    if 'NextMarker' not in response:
        response['NextMarker'] = None
    assert response['NextMarker'] == next_marker

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)

    assert len(keys) == len(check_objs)
    assert len(prefixes) == len(check_prefixes)
    assert keys == check_objs
    assert prefixes == check_prefixes

    return response['NextMarker']

def validate_bucket_listv2(bucket_name, prefix, delimiter, continuation_token, max_keys,
                         is_truncated, check_objs, check_prefixes, last=False):
    client = get_client()

    params = dict(Bucket=bucket_name, Delimiter=delimiter, MaxKeys=max_keys, Prefix=prefix)
    if continuation_token is not None:
        params['ContinuationToken'] = continuation_token
    else:
        params['StartAfter'] = ''
    response = client.list_objects_v2(**params)
    assert response['IsTruncated'] == is_truncated
    if 'NextContinuationToken' not in response:
        response['NextContinuationToken'] = None
    if last:
        assert response['NextContinuationToken'] == None


    keys = _get_keys(response)
    prefixes = _get_prefixes(response)

    assert len(keys) == len(check_objs)
    assert len(prefixes) == len(check_prefixes)
    assert keys == check_objs
    assert prefixes == check_prefixes

    return response['NextContinuationToken']

@pytest.mark.fails_on_dbstore
def test_bucket_list_delimiter_prefix():
    bucket_name = _create_objects(keys=['asdf', 'boo/bar', 'boo/baz/xyzzy', 'cquux/thud', 'cquux/bla'])

    delim = '/'
    marker = ''
    prefix = ''

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['asdf'], [], 'asdf')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, True, [], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['cquux/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, True, ['asdf'], ['boo/'], 'boo/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 2, False, [], ['cquux/'], None)

    prefix = 'boo/'

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['boo/bar'], [], 'boo/bar')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['boo/baz/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, False, ['boo/bar'], ['boo/baz/'], None)

@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_delimiter_prefix():
    bucket_name = _create_objects(keys=['asdf', 'boo/bar', 'boo/baz/xyzzy', 'cquux/thud', 'cquux/bla'])

    delim = '/'
    continuation_token = ''
    prefix = ''

    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, None, 1, True, ['asdf'], [])
    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token, 1, True, [], ['boo/'])
    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token, 1, False, [], ['cquux/'], last=True)

    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, None, 2, True, ['asdf'], ['boo/'])
    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token, 2, False, [], ['cquux/'], last=True)

    prefix = 'boo/'

    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, None, 1, True, ['boo/bar'], [])
    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token, 1, False, [], ['boo/baz/'], last=True)

    continuation_token = validate_bucket_listv2(bucket_name, prefix, delim, None, 2, False, ['boo/bar'], ['boo/baz/'], last=True)


@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_prefix_ends_with_delimiter():
    bucket_name = _create_objects(keys=['asdf/'])
    validate_bucket_listv2(bucket_name, 'asdf/', '/', None, 1000, False, ['asdf/'], [], last=True)

def test_bucket_list_delimiter_prefix_ends_with_delimiter():
    bucket_name = _create_objects(keys=['asdf/'])
    validate_bucket_list(bucket_name, 'asdf/', '/', '', 1000, False, ['asdf/'], [], None)

def test_bucket_list_delimiter_alt():
    bucket_name = _create_objects(keys=['bar', 'baz', 'cab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='a')
    assert response['Delimiter'] == 'a'

    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ['ba', 'ca']

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_alt():
    bucket_name = _create_objects(keys=['bar', 'baz', 'cab', 'foo'])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='a')
    assert response['Delimiter'] == 'a'

    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    # bar, baz, and cab should be broken up by the 'a' delimiters
    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    assert prefixes == ['ba', 'ca']

@pytest.mark.fails_on_dbstore
def test_bucket_list_delimiter_prefix_underscore():
    bucket_name = _create_objects(keys=['_obj1_','_under1/bar', '_under1/baz/xyzzy', '_under2/thud', '_under2/bla'])

    delim = '/'
    marker = ''
    prefix = ''
    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['_obj1_'], [], '_obj1_')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, True, [], ['_under1/'], '_under1/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['_under2/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, True, ['_obj1_'], ['_under1/'], '_under1/')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 2, False, [], ['_under2/'], None)

    prefix = '_under1/'

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 1, True, ['_under1/bar'], [], '_under1/bar')
    marker = validate_bucket_list(bucket_name, prefix, delim, marker, 1, False, [], ['_under1/baz/'], None)

    marker = validate_bucket_list(bucket_name, prefix, delim, '', 2, False, ['_under1/bar'], ['_under1/baz/'], None)

@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_delimiter_prefix_underscore():
    bucket_name = _create_objects(keys=['_obj1_','_under1/bar', '_under1/baz/xyzzy', '_under2/thud', '_under2/bla'])

    delim = '/'
    continuation_token = ''
    prefix = ''
    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, None, 1, True, ['_obj1_'], [])
    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token , 1, True, [], ['_under1/'])
    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token , 1, False, [], ['_under2/'], last=True)

    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, None, 2, True, ['_obj1_'], ['_under1/'])
    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token , 2, False, [], ['_under2/'], last=True)

    prefix = '_under1/'

    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, None, 1, True, ['_under1/bar'], [])
    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, continuation_token , 1, False, [], ['_under1/baz/'], last=True)

    continuation_token  = validate_bucket_listv2(bucket_name, prefix, delim, None, 2, False, ['_under1/bar'], ['_under1/baz/'], last=True)


def test_bucket_list_delimiter_percentage():
    bucket_name = _create_objects(keys=['b%ar', 'b%az', 'c%ab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='%')
    assert response['Delimiter'] == '%'
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ['b%', 'c%']

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_percentage():
    bucket_name = _create_objects(keys=['b%ar', 'b%az', 'c%ab', 'foo'])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='%')
    assert response['Delimiter'] == '%'
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ['b%', 'c%']

def test_bucket_list_delimiter_whitespace():
    bucket_name = _create_objects(keys=['b ar', 'b az', 'c ab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter=' ')
    assert response['Delimiter'] == ' '
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ['b ', 'c ']

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_whitespace():
    bucket_name = _create_objects(keys=['b ar', 'b az', 'c ab', 'foo'])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter=' ')
    assert response['Delimiter'] == ' '
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ['b ', 'c ']

def test_bucket_list_delimiter_dot():
    bucket_name = _create_objects(keys=['b.ar', 'b.az', 'c.ab', 'foo'])
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='.')
    assert response['Delimiter'] == '.'
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ['b.', 'c.']

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_dot():
    bucket_name = _create_objects(keys=['b.ar', 'b.az', 'c.ab', 'foo'])
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='.')
    assert response['Delimiter'] == '.'
    keys = _get_keys(response)
    # foo contains no 'a' and so is a complete key
    assert keys == ['foo']

    prefixes = _get_prefixes(response)
    assert len(prefixes) == 2
    # bar, baz, and cab should be broken up by the 'a' delimiters
    assert prefixes == ['b.', 'c.']

def test_bucket_list_delimiter_unreadable():
    key_names=['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='\x0a')
    assert response['Delimiter'] == '\x0a'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_unreadable():
    key_names=['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='\x0a')
    assert response['Delimiter'] == '\x0a'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

def test_bucket_list_delimiter_empty():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='')
    # putting an empty value into Delimiter will not return a value in the response
    assert not 'Delimiter' in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_empty():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='')
    # putting an empty value into Delimiter will not return a value in the response
    assert not 'Delimiter' in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

def test_bucket_list_delimiter_none():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    # putting an empty value into Delimiter will not return a value in the response
    assert not 'Delimiter' in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_none():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name)
    # putting an empty value into Delimiter will not return a value in the response
    assert not 'Delimiter' in response

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_fetchowner_notempty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, FetchOwner=True)
    objs_list = response['Contents']
    assert 'Owner' in objs_list[0]

@pytest.mark.list_objects_v2
def test_bucket_listv2_fetchowner_defaultempty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name)
    objs_list = response['Contents']
    assert not 'Owner' in objs_list[0]

@pytest.mark.list_objects_v2
def test_bucket_listv2_fetchowner_empty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, FetchOwner= False)
    objs_list = response['Contents']
    assert not 'Owner' in objs_list[0]

def test_bucket_list_delimiter_not_exist():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/')
    # putting an empty value into Delimiter will not return a value in the response
    assert response['Delimiter'] == '/'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_delimiter_not_exist():
    key_names = ['bar', 'baz', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    # putting an empty value into Delimiter will not return a value in the response
    assert response['Delimiter'] == '/'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []


@pytest.mark.fails_on_dbstore
def test_bucket_list_delimiter_not_skip_special():
    key_names = ['0/'] + ['0/%s' % i for i in range(1000, 1999)]
    key_names2 = ['1999', '1999#', '1999+', '2000']
    key_names += key_names2
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/')
    assert response['Delimiter'] == '/'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names2
    assert prefixes == ['0/']

def test_bucket_list_prefix_basic():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='foo/')
    assert response['Prefix'] == 'foo/'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['foo/bar', 'foo/baz']
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_basic():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix='foo/')
    assert response['Prefix'] == 'foo/'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['foo/bar', 'foo/baz']
    assert prefixes == []

# just testing that we can do the delimeter and prefix logic on non-slashes
def test_bucket_list_prefix_alt():
    key_names = ['bar', 'baz', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='ba')
    assert response['Prefix'] == 'ba'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['bar', 'baz']
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_alt():
    key_names = ['bar', 'baz', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix='ba')
    assert response['Prefix'] == 'ba'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['bar', 'baz']
    assert prefixes == []

def test_bucket_list_prefix_empty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='')
    assert response['Prefix'] == ''

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_empty():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix='')
    assert response['Prefix'] == ''

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

def test_bucket_list_prefix_none():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='')
    assert response['Prefix'] == ''

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_none():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix='')
    assert response['Prefix'] == ''

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == key_names
    assert prefixes == []

def test_bucket_list_prefix_not_exist():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='d')
    assert response['Prefix'] == 'd'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_not_exist():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix='d')
    assert response['Prefix'] == 'd'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

def test_bucket_list_prefix_unreadable():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Prefix='\x0a')
    assert response['Prefix'] == '\x0a'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_unreadable():
    key_names = ['foo/bar', 'foo/baz', 'quux']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Prefix='\x0a')
    assert response['Prefix'] == '\x0a'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

def test_bucket_list_prefix_delimiter_basic():
    key_names = ['foo/bar', 'foo/baz/xyzzy', 'quux/thud', 'asdf']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='/', Prefix='foo/')
    assert response['Prefix'] == 'foo/'
    assert response['Delimiter'] == '/'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['foo/bar']
    assert prefixes == ['foo/baz/']

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_basic():
    key_names = ['foo/bar', 'foo/baz/xyzzy', 'quux/thud', 'asdf']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='/', Prefix='foo/')
    assert response['Prefix'] == 'foo/'
    assert response['Delimiter'] == '/'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['foo/bar']
    assert prefixes == ['foo/baz/']

def test_bucket_list_prefix_delimiter_alt():
    key_names = ['bar', 'bazar', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='a', Prefix='ba')
    assert response['Prefix'] == 'ba'
    assert response['Delimiter'] == 'a'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['bar']
    assert prefixes == ['baza']

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_alt():
    key_names = ['bar', 'bazar', 'cab', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='a', Prefix='ba')
    assert response['Prefix'] == 'ba'
    assert response['Delimiter'] == 'a'

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['bar']
    assert prefixes == ['baza']

def test_bucket_list_prefix_delimiter_prefix_not_exist():
    key_names = ['b/a/r', 'b/a/c', 'b/a/g', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='d', Prefix='/')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_prefix_not_exist():
    key_names = ['b/a/r', 'b/a/c', 'b/a/g', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='d', Prefix='/')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

def test_bucket_list_prefix_delimiter_delimiter_not_exist():
    key_names = ['b/a/c', 'b/a/g', 'b/a/r', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='z', Prefix='b')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['b/a/c', 'b/a/g', 'b/a/r']
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_delimiter_not_exist():
    key_names = ['b/a/c', 'b/a/g', 'b/a/r', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='z', Prefix='b')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == ['b/a/c', 'b/a/g', 'b/a/r']
    assert prefixes == []

def test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist():
    key_names = ['b/a/c', 'b/a/g', 'b/a/r', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Delimiter='z', Prefix='y')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_prefix_delimiter_prefix_delimiter_not_exist():
    key_names = ['b/a/c', 'b/a/g', 'b/a/r', 'g']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, Delimiter='z', Prefix='y')

    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    assert keys == []
    assert prefixes == []

@pytest.mark.fails_on_dbstore
def test_bucket_list_maxkeys_one():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=1)
    assert response['IsTruncated'] == True

    keys = _get_keys(response)
    assert keys == key_names[0:1]

    response = client.list_objects(Bucket=bucket_name, Marker=key_names[0])
    assert response['IsTruncated'] == False

    keys = _get_keys(response)
    assert keys == key_names[1:]

@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_maxkeys_one():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    assert response['IsTruncated'] == True

    keys = _get_keys(response)
    assert keys == key_names[0:1]

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter=key_names[0])
    assert response['IsTruncated'] == False

    keys = _get_keys(response)
    assert keys == key_names[1:]

def test_bucket_list_maxkeys_zero():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, MaxKeys=0)

    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_maxkeys_zero():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=0)

    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == []

def test_bucket_list_maxkeys_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == key_names
    assert response['MaxKeys'] == 1000

@pytest.mark.list_objects_v2
def test_bucket_listv2_maxkeys_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name)
    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == key_names
    assert response['MaxKeys'] == 1000

def get_http_response_body(**kwargs):
    global http_response_body
    http_response_body = kwargs['http_response'].__dict__['_content']

def parseXmlToJson(xml):
  response = {}

  for child in list(xml):
    if len(list(child)) > 0:
      response[child.tag] = parseXmlToJson(child)
    else:
      response[child.tag] = child.text or ''

    # one-liner equivalent
    # response[child.tag] = parseXmlToJson(child) if len(list(child)) > 0 else child.text or ''

  return response

@pytest.mark.fails_on_aws
def test_account_usage():
    # boto3.set_stream_logger(name='botocore')
    client = get_client()
    # adds the unordered query parameter
    def add_usage(**kwargs):
        kwargs['params']['url'] += "?usage"
    client.meta.events.register('before-call.s3.ListBuckets', add_usage)
    client.meta.events.register('after-call.s3.ListBuckets', get_http_response_body)
    client.list_buckets()
    xml    = ET.fromstring(http_response_body.decode('utf-8'))
    parsed = parseXmlToJson(xml)
    summary = parsed['Summary']
    assert summary['QuotaMaxBytes'] == '-1'
    assert summary['QuotaMaxBuckets'] == '1000'
    assert summary['QuotaMaxObjCount'] == '-1'
    assert summary['QuotaMaxBytesPerBucket'] == '-1'
    assert summary['QuotaMaxObjCountPerBucket'] == '-1'

@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_head_bucket_usage():
    client = get_client()
    bucket_name = _create_objects(keys=['foo'])

    def add_read_stats_param(request, **kwargs):
        request.params['read-stats'] = 'true'

    client.meta.events.register('request-created.s3.HeadBucket', add_read_stats_param)
    client.meta.events.register('after-call.s3.HeadBucket', get_http_response)
    client.head_bucket(Bucket=bucket_name)
    hdrs = http_response['headers']
    assert hdrs['X-RGW-Object-Count'] == '1'
    assert hdrs['X-RGW-Bytes-Used'] == '3'
    assert hdrs['X-RGW-Quota-Max-Buckets'] == '1000'

@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_bucket_list_unordered():
    # boto3.set_stream_logger(name='botocore')
    keys_in = ['ado', 'bot', 'cob', 'dog', 'emu', 'fez', 'gnu', 'hex',
               'abc/ink', 'abc/jet', 'abc/kin', 'abc/lax', 'abc/mux',
               'def/nim', 'def/owl', 'def/pie', 'def/qed', 'def/rye',
               'ghi/sew', 'ghi/tor', 'ghi/uke', 'ghi/via', 'ghi/wit',
               'xix', 'yak', 'zoo']
    bucket_name = _create_objects(keys=keys_in)
    client = get_client()

    # adds the unordered query parameter
    def add_unordered(**kwargs):
        kwargs['params']['url'] += "&allow-unordered=true"
    client.meta.events.register('before-call.s3.ListObjects', add_unordered)

    # test simple retrieval
    response = client.list_objects(Bucket=bucket_name, MaxKeys=1000)
    unordered_keys_out = _get_keys(response)
    assert len(keys_in) == len(unordered_keys_out)
    assert keys_in.sort() == unordered_keys_out.sort()

    # test retrieval with prefix
    response = client.list_objects(Bucket=bucket_name,
                                   MaxKeys=1000,
                                   Prefix="abc/")
    unordered_keys_out = _get_keys(response)
    assert 5 == len(unordered_keys_out)

    # test incremental retrieval with marker
    response = client.list_objects(Bucket=bucket_name, MaxKeys=6)
    unordered_keys_out = _get_keys(response)
    assert 6 == len(unordered_keys_out)

    # now get the next bunch
    response = client.list_objects(Bucket=bucket_name,
                                   MaxKeys=6,
                                   Marker=unordered_keys_out[-1])
    unordered_keys_out2 = _get_keys(response)
    assert 6 == len(unordered_keys_out2)

    # make sure there's no overlap between the incremental retrievals
    intersect = set(unordered_keys_out).intersection(unordered_keys_out2)
    assert 0 == len(intersect)

    # verify that unordered used with delimiter results in error
    e = assert_raises(ClientError,
                      client.list_objects, Bucket=bucket_name, Delimiter="/")
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.fails_on_aws
@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_unordered():
    # boto3.set_stream_logger(name='botocore')
    keys_in = ['ado', 'bot', 'cob', 'dog', 'emu', 'fez', 'gnu', 'hex',
               'abc/ink', 'abc/jet', 'abc/kin', 'abc/lax', 'abc/mux',
               'def/nim', 'def/owl', 'def/pie', 'def/qed', 'def/rye',
               'ghi/sew', 'ghi/tor', 'ghi/uke', 'ghi/via', 'ghi/wit',
               'xix', 'yak', 'zoo']
    bucket_name = _create_objects(keys=keys_in)
    client = get_client()

    # adds the unordered query parameter
    def add_unordered(**kwargs):
        kwargs['params']['url'] += "&allow-unordered=true"
    client.meta.events.register('before-call.s3.ListObjects', add_unordered)

    # test simple retrieval
    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000)
    unordered_keys_out = _get_keys(response)
    assert len(keys_in) == len(unordered_keys_out)
    assert keys_in.sort() == unordered_keys_out.sort()

    # test retrieval with prefix
    response = client.list_objects_v2(Bucket=bucket_name,
                                   MaxKeys=1000,
                                   Prefix="abc/")
    unordered_keys_out = _get_keys(response)
    assert 5 == len(unordered_keys_out)

    # test incremental retrieval with marker
    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=6)
    unordered_keys_out = _get_keys(response)
    assert 6 == len(unordered_keys_out)

    # now get the next bunch
    response = client.list_objects_v2(Bucket=bucket_name,
                                   MaxKeys=6,
                                   StartAfter=unordered_keys_out[-1])
    unordered_keys_out2 = _get_keys(response)
    assert 6 == len(unordered_keys_out2)

    # make sure there's no overlap between the incremental retrievals
    intersect = set(unordered_keys_out).intersection(unordered_keys_out2)
    assert 0 == len(intersect)

    #pdb.set_trace()
    # verify that unordered used with delimiter results in error
    e = assert_raises(ClientError,
                      client.list_objects, Bucket=bucket_name, Delimiter="/")
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'


def test_bucket_list_maxkeys_invalid():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    # adds invalid max keys to url
    # before list_objects is called
    def add_invalid_maxkeys(**kwargs):
        kwargs['params']['url'] += "&max-keys=blah"
    client.meta.events.register('before-call.s3.ListObjects', add_invalid_maxkeys)

    e = assert_raises(ClientError, client.list_objects, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'



def test_bucket_list_marker_none():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name)
    assert response['Marker'] == ''


def test_bucket_list_marker_empty():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='')
    assert response['Marker'] == ''
    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == key_names

@pytest.mark.list_objects_v2
def test_bucket_listv2_continuationtoken_empty():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, ContinuationToken='')
    assert response['ContinuationToken'] == ''
    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == key_names

@pytest.mark.list_objects_v2
def test_bucket_listv2_continuationtoken():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response1 = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    next_continuation_token = response1['NextContinuationToken']

    response2 = client.list_objects_v2(Bucket=bucket_name, ContinuationToken=next_continuation_token)
    assert response2['ContinuationToken'] == next_continuation_token
    assert response2['IsTruncated'] == False
    key_names2 = ['baz', 'foo', 'quxx']
    keys = _get_keys(response2)
    assert keys == key_names2

@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_bucket_listv2_both_continuationtoken_startafter():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response1 = client.list_objects_v2(Bucket=bucket_name, StartAfter='bar', MaxKeys=1)
    next_continuation_token = response1['NextContinuationToken']

    response2 = client.list_objects_v2(Bucket=bucket_name, StartAfter='bar', ContinuationToken=next_continuation_token)
    assert response2['ContinuationToken'] == next_continuation_token
    assert response2['StartAfter'] == 'bar'
    assert response2['IsTruncated'] == False
    key_names2 = ['foo', 'quxx']
    keys = _get_keys(response2)
    assert keys == key_names2

def test_bucket_list_marker_unreadable():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='\x0a')
    assert response['Marker'] == '\x0a'
    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == key_names

@pytest.mark.list_objects_v2
def test_bucket_listv2_startafter_unreadable():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter='\x0a')
    assert response['StartAfter'] == '\x0a'
    assert response['IsTruncated'] == False
    keys = _get_keys(response)
    assert keys == key_names

def test_bucket_list_marker_not_in_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='blah')
    assert response['Marker'] == 'blah'
    keys = _get_keys(response)
    assert keys == [ 'foo','quxx']

@pytest.mark.list_objects_v2
def test_bucket_listv2_startafter_not_in_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter='blah')
    assert response['StartAfter'] == 'blah'
    keys = _get_keys(response)
    assert keys == ['foo', 'quxx']

def test_bucket_list_marker_after_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects(Bucket=bucket_name, Marker='zzz')
    assert response['Marker'] == 'zzz'
    keys = _get_keys(response)
    assert response['IsTruncated'] == False
    assert keys == []

@pytest.mark.list_objects_v2
def test_bucket_listv2_startafter_after_list():
    key_names = ['bar', 'baz', 'foo', 'quxx']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    response = client.list_objects_v2(Bucket=bucket_name, StartAfter='zzz')
    assert response['StartAfter'] == 'zzz'
    keys = _get_keys(response)
    assert response['IsTruncated'] == False
    assert keys == []

def _compare_dates(datetime1, datetime2):
    """
    changes ms from datetime1 to 0, compares it to datetime2
    """
    # both times are in datetime format but datetime1 has
    # microseconds and datetime2 does not
    datetime1 = datetime1.replace(microsecond=0)
    assert datetime1 == datetime2

@pytest.mark.fails_on_dbstore
def test_bucket_list_return_data():
    key_names = ['bar', 'baz', 'foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    data = {}
    for key_name in key_names:
        obj_response = client.head_object(Bucket=bucket_name, Key=key_name)
        acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
        data.update({
            key_name: {
                'DisplayName': acl_response['Owner']['DisplayName'],
                'ID': acl_response['Owner']['ID'],
                'ETag': obj_response['ETag'],
                'LastModified': obj_response['LastModified'],
                'ContentLength': obj_response['ContentLength'],
                }
            })

    response  = client.list_objects(Bucket=bucket_name)
    objs_list = response['Contents']
    for obj in objs_list:
        key_name = obj['Key']
        key_data = data[key_name]
        assert obj['ETag'] == key_data['ETag']
        assert obj['Size'] == key_data['ContentLength']
        assert obj['Owner']['DisplayName'] == key_data['DisplayName']
        assert obj['Owner']['ID'] == key_data['ID']
        _compare_dates(obj['LastModified'],key_data['LastModified'])


@pytest.mark.fails_on_dbstore
def test_bucket_list_return_data_versioning():
    bucket_name = get_new_bucket()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    key_names = ['bar', 'baz', 'foo']
    bucket_name = _create_objects(bucket_name=bucket_name,keys=key_names)

    client = get_client()
    data = {}

    for key_name in key_names:
        obj_response = client.head_object(Bucket=bucket_name, Key=key_name)
        acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
        data.update({
            key_name: {
                'ID': acl_response['Owner']['ID'],
                'DisplayName': acl_response['Owner']['DisplayName'],
                'ETag': obj_response['ETag'],
                'LastModified': obj_response['LastModified'],
                'ContentLength': obj_response['ContentLength'],
                'VersionId': obj_response['VersionId']
                }
            })

    response  = client.list_object_versions(Bucket=bucket_name)
    objs_list = response['Versions']

    for obj in objs_list:
        key_name = obj['Key']
        key_data = data[key_name]
        assert obj['Owner']['DisplayName'] == key_data['DisplayName']
        assert obj['ETag'] == key_data['ETag']
        assert obj['Size'] == key_data['ContentLength']
        assert obj['Owner']['ID'] == key_data['ID']
        assert obj['VersionId'] == key_data['VersionId']
        _compare_dates(obj['LastModified'],key_data['LastModified'])

def test_bucket_list_objects_anonymous():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

    unauthenticated_client = get_unauthenticated_client()
    unauthenticated_client.list_objects(Bucket=bucket_name)

@pytest.mark.list_objects_v2
def test_bucket_listv2_objects_anonymous():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

    unauthenticated_client = get_unauthenticated_client()
    unauthenticated_client.list_objects_v2(Bucket=bucket_name)

def test_bucket_list_objects_anonymous_fail():
    bucket_name = get_new_bucket()

    unauthenticated_client = get_unauthenticated_client()
    e = assert_raises(ClientError, unauthenticated_client.list_objects, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.list_objects_v2
def test_bucket_listv2_objects_anonymous_fail():
    bucket_name = get_new_bucket()

    unauthenticated_client = get_unauthenticated_client()
    e = assert_raises(ClientError, unauthenticated_client.list_objects_v2, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

def test_bucket_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.list_objects, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

@pytest.mark.list_objects_v2
def test_bucketv2_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.list_objects_v2, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

def test_bucket_delete_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

def test_bucket_delete_nonempty():
    key_names = ['foo']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'BucketNotEmpty'

def _do_set_bucket_canned_acl(client, bucket_name, canned_acl, i, results):
    try:
        client.put_bucket_acl(ACL=canned_acl, Bucket=bucket_name)
        results[i] = True
    except:
        results[i] = False

def _do_set_bucket_canned_acl_concurrent(client, bucket_name, canned_acl, num, results):
    t = []
    for i in range(num):
        thr = threading.Thread(target = _do_set_bucket_canned_acl, args=(client, bucket_name, canned_acl, i, results))
        thr.start()
        t.append(thr)
    return t

def _do_wait_completion(t):
    for thr in t:
        thr.join()

def test_bucket_concurrent_set_canned_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    num_threads = 50 # boto2 retry defaults to 5 so we need a thread to fail at least 5 times
                     # this seems like a large enough number to get through retry (if bug
                     # exists)
    results = [None] * num_threads

    t = _do_set_bucket_canned_acl_concurrent(client, bucket_name, 'public-read', num_threads, results)
    _do_wait_completion(t)

    for r in results:
        assert r == True

def test_object_write_to_nonexist_bucket():
    key_names = ['foo']
    bucket_name = 'whatchutalkinboutwillis'
    client = get_client()

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='foo')

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'


def _ev_add_te_header(request, **kwargs):
    request.headers.add_header('Transfer-Encoding', 'chunked')

def test_object_write_with_chunked_transfer_encoding():
    bucket_name = get_new_bucket()
    client = get_client()

    client.meta.events.register_first('before-sign.*.*', _ev_add_te_header)
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_bucket_create_delete():
    bucket_name = get_new_bucket()
    client = get_client()
    client.delete_bucket(Bucket=bucket_name)

    e = assert_raises(ClientError, client.delete_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

def test_object_read_not_exist():
    bucket_name = get_new_bucket()
    client = get_client()

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='bar')

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

http_response = None

def get_http_response(**kwargs):
    global http_response
    http_response = kwargs['http_response'].__dict__

@pytest.mark.fails_on_dbstore
def test_object_requestid_matches_header_on_error():
    bucket_name = get_new_bucket()
    client = get_client()

    # get http response after failed request
    client.meta.events.register('after-call.s3.GetObject', get_http_response)
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='bar')

    response_body = http_response['_content']
    resp_body_xml = ET.fromstring(response_body)
    request_id = resp_body_xml.find('.//RequestId').text

    assert request_id is not None
    assert request_id == e.response['ResponseMetadata']['RequestId']

def _make_objs_dict(key_names):
    objs_list = []
    for key in key_names:
        obj_dict = {'Key': key}
        objs_list.append(obj_dict)
    objs_dict = {'Objects': objs_list}
    return objs_dict

def test_versioning_concurrent_multi_object_delete():
    num_objects = 5
    num_versions_per_object = 3
    total_num_objects_in_the_bucket = num_objects * num_versions_per_object
    num_threads = 5
    bucket_name = get_new_bucket()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key_names = ["key_{:d}".format(x) for x in range(num_objects)]
    for _ in range(num_versions_per_object):
        bucket = _create_objects(bucket_name=bucket_name, keys=key_names)
        assert bucket == bucket_name

    client = get_client()
    versions = client.list_object_versions(Bucket=bucket_name)['Versions']
    assert len(versions) == total_num_objects_in_the_bucket
    objs_dict = {'Objects': [dict((k, v[k]) for k in ["Key", "VersionId"]) for v in versions]}
    results = [None] * num_threads

    def do_request(n):
        results[n] = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)

    t = []
    for i in range(num_threads):
        thr = threading.Thread(target = do_request, args=[i])
        thr.start()
        t.append(thr)
    _do_wait_completion(t)

    for response in results:
        assert len(response['Deleted']) == total_num_objects_in_the_bucket
        assert 'Errors' not in response

    response_list_objects = client.list_objects(Bucket=bucket_name)
    assert 'Contents' not in response_list_objects
    response_list_versions = client.list_object_versions(Bucket=bucket_name)
    assert 'Versions' not in response_list_versions and 'DeleteMarkers' not in response_list_versions

def test_multi_object_delete():
    key_names = ['key0', 'key1', 'key2']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()
    response = client.list_objects(Bucket=bucket_name)
    assert len(response['Contents']) == 3

    objs_dict = _make_objs_dict(key_names=key_names)
    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)

    assert len(response['Deleted']) == 3
    assert 'Errors' not in response
    response = client.list_objects(Bucket=bucket_name)
    assert 'Contents' not in response

    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
    assert len(response['Deleted']) == 3
    assert 'Errors' not in response
    response = client.list_objects(Bucket=bucket_name)
    assert 'Contents' not in response

@pytest.mark.list_objects_v2
def test_expected_bucket_owner():
  bucket_name = get_new_bucket()
  client = get_client()
  client.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')
  client.list_objects(Bucket=bucket_name)
  client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

  unauthenticated_client = get_unauthenticated_client()
  incorrect_expected_owner = get_main_user_id() + 'foo'

  e = assert_raises(ClientError, unauthenticated_client.list_objects, Bucket=bucket_name, ExpectedBucketOwner=incorrect_expected_owner)
  status, error_code = _get_status_and_error_code(e.response)
  assert status == 403
  assert error_code == 'AccessDenied'

  e = assert_raises(ClientError, unauthenticated_client.put_object, Bucket=bucket_name, Key='bar', Body='coffee', ExpectedBucketOwner=incorrect_expected_owner)
  status, error_code = _get_status_and_error_code(e.response)
  assert status == 403
  assert error_code == 'AccessDenied'

@pytest.mark.list_objects_v2
def test_multi_objectv2_delete():
    key_names = ['key0', 'key1', 'key2']
    bucket_name = _create_objects(keys=key_names)
    client = get_client()
    response = client.list_objects_v2(Bucket=bucket_name)
    assert len(response['Contents']) == 3

    objs_dict = _make_objs_dict(key_names=key_names)
    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)

    assert len(response['Deleted']) == 3
    assert 'Errors' not in response
    response = client.list_objects_v2(Bucket=bucket_name)
    assert 'Contents' not in response

    response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
    assert len(response['Deleted']) == 3
    assert 'Errors' not in response
    response = client.list_objects_v2(Bucket=bucket_name)
    assert 'Contents' not in response

def test_multi_object_delete_key_limit():
    key_names = [f"key-{i}" for i in range(1001)]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    paginator = client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket_name)
    numKeys = 0
    for page in pages:
        numKeys += len(page['Contents'])
    assert numKeys == 1001

    objs_dict = _make_objs_dict(key_names=key_names)
    e = assert_raises(ClientError,client.delete_objects,Bucket=bucket_name,Delete=objs_dict)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

def test_multi_objectv2_delete_key_limit():
    key_names = [f"key-{i}" for i in range(1001)]
    bucket_name = _create_objects(keys=key_names)
    client = get_client()

    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)
    numKeys = 0
    for page in pages:
        numKeys += len(page['Contents'])
    assert numKeys == 1001

    objs_dict = _make_objs_dict(key_names=key_names)
    e = assert_raises(ClientError,client.delete_objects,Bucket=bucket_name,Delete=objs_dict)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

def test_object_head_zero_bytes():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='')

    response = client.head_object(Bucket=bucket_name, Key='foo')
    assert response['ContentLength'] == 0

def test_object_write_check_etag():
    bucket_name = get_new_bucket()
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['ETag'] == '"37b51d194a7513e45b56f6524f2d51f2"'

def test_object_write_cache_control():
    bucket_name = get_new_bucket()
    client = get_client()
    cache_control = 'public, max-age=14400'
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', CacheControl=cache_control)

    response = client.head_object(Bucket=bucket_name, Key='foo')
    assert response['ResponseMetadata']['HTTPHeaders']['cache-control'] == cache_control

def test_object_write_expires():
    bucket_name = get_new_bucket()
    client = get_client()

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Expires=expires)

    response = client.head_object(Bucket=bucket_name, Key='foo')
    _compare_dates(expires, response['Expires'])

def _get_body(response):
    body = response['Body']
    got = body.read()
    if type(got) is bytes:
        got = got.decode()
    return got

def test_object_write_read_update_read_delete():
    bucket_name = get_new_bucket()
    client = get_client()

    # Write
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    # Read
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'
    # Update
    client.put_object(Bucket=bucket_name, Key='foo', Body='soup')
    # Read
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'soup'
    # Delete
    client.delete_object(Bucket=bucket_name, Key='foo')

def _set_get_metadata(metadata, bucket_name=None):
    """
    create a new bucket new or use an existing
    name to create an object that bucket,
    set the meta1 property to a specified, value,
    and then re-read and return that property
    """
    if bucket_name is None:
        bucket_name = get_new_bucket()

    client = get_client()
    metadata_dict = {'meta1': metadata}
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata=metadata_dict)

    response = client.get_object(Bucket=bucket_name, Key='foo')
    return response['Metadata']['meta1']

def test_object_set_get_metadata_none_to_good():
    got = _set_get_metadata('mymeta')
    assert got == 'mymeta'

def test_object_set_get_metadata_none_to_empty():
    got = _set_get_metadata('')
    assert got == ''

def test_object_set_get_metadata_overwrite_to_empty():
    bucket_name = get_new_bucket()
    got = _set_get_metadata('oldmeta', bucket_name)
    assert got == 'oldmeta'
    got = _set_get_metadata('', bucket_name)
    assert got == ''

# TODO: the decoding of this unicode metadata is not happening properly for unknown reasons
@pytest.mark.fails_on_rgw
def test_object_set_get_unicode_metadata():
    bucket_name = get_new_bucket()
    client = get_client()

    def set_unicode_metadata(**kwargs):
        kwargs['params']['headers']['x-amz-meta-meta1'] = u"Hello World\xe9"

    client.meta.events.register('before-call.s3.PutObject', set_unicode_metadata)
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    got = response['Metadata']['meta1']
    print(got)
    print(u"Hello World\xe9")
    assert got == u"Hello World\xe9"

def _set_get_metadata_unreadable(metadata, bucket_name=None):
    """
    set and then read back a meta-data value (which presumably
    includes some interesting characters), and return a list
    containing the stored value AND the encoding with which it
    was returned.

    This should return a 400 bad request because the webserver
    rejects the request.
    """
    bucket_name = get_new_bucket()
    client = get_client()
    metadata_dict = {'meta1': metadata}
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='bar', Metadata=metadata_dict)
    return e

def test_object_metadata_replaced_on_put():
    bucket_name = get_new_bucket()
    client = get_client()
    metadata_dict = {'meta1': 'bar'}
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata=metadata_dict)

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    got = response['Metadata']
    assert got == {}

def test_object_write_file():
    bucket_name = get_new_bucket()
    client = get_client()
    data_str = 'bar'
    data = bytes(data_str, 'utf-8')
    client.put_object(Bucket=bucket_name, Key='foo', Body=data)
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

def _get_post_url(bucket_name):
    endpoint = get_config_endpoint()
    return '{endpoint}/{bucket_name}'.format(endpoint=endpoint, bucket_name=bucket_name)

def test_post_object_anonymous_request():
    bucket_name = get_new_bucket_name()
    client = get_client()
    url = _get_post_url(bucket_name)
    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)
    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = _get_body(response)
    assert body == 'bar'

def test_post_object_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = _get_body(response)
    assert body == 'bar'

def test_post_object_authenticated_no_content_type():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)


    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key="foo.txt")
    body = _get_body(response)
    assert body == 'bar'

def test_post_object_authenticated_request_bad_access_key():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , 'foo'),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_set_success_code():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("success_action_status" , "201"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 201
    message = ET.fromstring(r.content).find('Key')
    assert message.text == 'foo.txt'

def test_post_object_set_invalid_success_code():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("success_action_status" , "404"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    content = r.content.decode()
    assert content == ''

def test_post_object_upload_larger_than_chunk():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 5*1024*1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    foo_string = 'foo' * 1024*1024

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', foo_string)])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = _get_body(response)
    assert body == foo_string

def test_post_object_set_key_from_filename():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "${filename}"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('foo.txt', 'bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = _get_body(response)
    assert body == 'bar'

def test_post_object_ignored_header():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),("x-ignore-foo" , "bar"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

def test_post_object_case_insensitive_condition_fields():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bUcKeT": bucket_name},\
    ["StArTs-WiTh", "$KeY", "foo"],\
    {"AcL": "private"},\
    ["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    foo_string = 'foo' * 1024*1024

    payload = OrderedDict([ ("kEy" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("aCl" , "private"),("signature" , signature),("pOLICy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

def test_post_object_escaped_field_values():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key='\$foo.txt')
    body = _get_body(response)
    assert body == 'bar'

def test_post_object_success_redirect_action():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    url = _get_post_url(bucket_name)
    redirect_url = _get_post_url(bucket_name)

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["eq", "$success_action_redirect", redirect_url],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),("success_action_redirect" , redirect_url),\
    ('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 200
    url = r.url
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    assert url == '{rurl}?bucket={bucket}&key={key}&etag=%22{etag}%22'.format(\
    rurl = redirect_url, bucket = bucket_name, key = 'foo.txt', etag = response['ETag'].strip('"'))

def test_post_object_invalid_signature():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())[::-1]

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_invalid_access_key():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id[::-1]),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_invalid_date_format():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": str(expires),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "\$foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_no_key_specified():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_missing_signature():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_missing_policy_condition():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    ["starts-with", "$key", "\$foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_user_specified_header():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ["starts-with", "$x-amz-meta-foo",  "bar"]
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    assert response['Metadata']['foo'] == 'barclamp'

def test_post_object_request_missing_policy_specified_field():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ["starts-with", "$x-amz-meta-foo",  "bar"]
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_condition_is_case_sensitive():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "CONDITIONS": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_expires_is_case_sensitive():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"EXPIRATION": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_expired_policy():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=-6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key", "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_wrong_bucket():
    bucket_name = get_new_bucket()
    client = get_client()

    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024]\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "${filename}"),('bucket', bucket_name),\
    ("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('foo.txt', 'bar'))])

    bad_bucket_name = get_new_bucket()
    url = _get_post_url(bad_bucket_name)

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_invalid_request_field_value():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ["eq", "$x-amz-meta-foo",  ""]
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())
    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('x-amz-meta-foo' , 'barclamp'),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 403

def test_post_object_missing_expires_condition():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 1024],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_missing_conditions_list():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ")}

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_upload_size_limit_exceeded():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0, 0],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_missing_content_length_argument():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 0],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_invalid_content_length_argument():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", -1, 0],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_upload_size_below_minimum():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", 512, 1000],\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_post_object_upload_size_rgw_chunk_size_bug():
    # Test for https://tracker.ceph.com/issues/58627
    # TODO: if this value is different in Teuthology runs, this would need tuning
    # https://github.com/ceph/ceph/blob/main/qa/suites/rgw/verify/striping%24/stripe-greater-than-chunk.yaml
    _rgw_max_chunk_size = 4 * 2**20 # 4MiB
    min_size = _rgw_max_chunk_size
    max_size = _rgw_max_chunk_size * 3
    # [(chunk),(small)]
    test_payload_size = _rgw_max_chunk_size + 200 # extra bit to push it over the chunk boundary
    # it should be valid when we run this test!
    assert test_payload_size > min_size
    assert test_payload_size < max_size

    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", min_size, max_size],\
    ]\
    }

    test_payload = 'x' * test_payload_size

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', (test_payload))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

def test_post_object_empty_conditions():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    { }\
    ]\
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400

def test_get_object_ifmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    etag = response['ETag']

    response = client.get_object(Bucket=bucket_name, Key='foo', IfMatch=etag)
    body = _get_body(response)
    assert body == 'bar'

def test_get_object_ifmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfMatch='"ABCORZ"')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

def test_get_object_ifnonematch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    etag = response['ETag']

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfNoneMatch=etag)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 304
    assert e.response['Error']['Message'] == 'Not Modified'
    assert e.response['ResponseMetadata']['HTTPHeaders']['etag'] == etag

def test_get_object_ifnonematch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo', IfNoneMatch='ABCORZ')
    body = _get_body(response)
    assert body == 'bar'

def test_get_object_ifmodifiedsince_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo', IfModifiedSince='Sat, 29 Oct 1994 19:43:31 GMT')
    body = _get_body(response)
    assert body == 'bar'

@pytest.mark.fails_on_dbstore
def test_get_object_ifmodifiedsince_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    etag = response['ETag']
    last_modified = str(response['LastModified'])

    last_modified = last_modified.split('+')[0]
    mtime = datetime.datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S')

    after = mtime + datetime.timedelta(seconds=1)
    after_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", after.timetuple())

    time.sleep(1)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfModifiedSince=after_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 304
    assert e.response['Error']['Message'] == 'Not Modified'
    assert e.response['ResponseMetadata']['HTTPHeaders']['etag'] == etag

@pytest.mark.fails_on_dbstore
def test_get_object_ifunmodifiedsince_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfUnmodifiedSince='Sat, 29 Oct 1994 19:43:31 GMT')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

def test_get_object_ifunmodifiedsince_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo', IfUnmodifiedSince='Sat, 29 Oct 2100 19:43:31 GMT')
    body = _get_body(response)
    assert body == 'bar'


@pytest.mark.fails_on_aws
def test_put_object_ifmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

    etag = response['ETag'].replace('"', '')

    # pass in custom header 'If-Match' before PutObject call
    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': etag}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    response = client.put_object(Bucket=bucket_name,Key='foo', Body='zar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'zar'

@pytest.mark.fails_on_dbstore
def test_put_object_ifmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

    # pass in custom header 'If-Match' before PutObject call
    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '"ABCORZ"'}))
    client.meta.events.register('before-call.s3.PutObject', lf)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

@pytest.mark.fails_on_aws
def test_put_object_ifmatch_overwrite_existed_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    response = client.put_object(Bucket=bucket_name,Key='foo', Body='zar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'zar'

@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_put_object_ifmatch_nonexisted_failed():
    bucket_name = get_new_bucket()
    client = get_client()

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='bar')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

@pytest.mark.fails_on_aws
def test_put_object_ifnonmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': 'ABCORZ'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    response = client.put_object(Bucket=bucket_name,Key='foo', Body='zar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'zar'

@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_put_object_ifnonmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

    etag = response['ETag'].replace('"', '')

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': etag}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

@pytest.mark.fails_on_aws
def test_put_object_ifnonmatch_nonexisted_good():
    bucket_name = get_new_bucket()
    client = get_client()

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_put_object_ifnonmatch_overwrite_existed_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

def _setup_bucket_object_acl(bucket_acl, object_acl, client=None):
    """
    add a foo key, and specified key and bucket acls to
    a (new or existing) bucket.
    """
    if client is None:
        client = get_client()
    bucket_name = get_new_bucket_name()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)
    client.put_object(ACL=object_acl, Bucket=bucket_name, Key='foo')

    return bucket_name

def _setup_bucket_acl(bucket_acl=None):
    """
    set up a new bucket with specified acl
    """
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)

    return bucket_name

def test_object_raw_get():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')

    unauthenticated_client = get_unauthenticated_client()
    response = unauthenticated_client.get_object(Bucket=bucket_name, Key='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_object_raw_get_bucket_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')
    client.delete_bucket(Bucket=bucket_name)

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

def test_object_delete_key_bucket_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')
    client.delete_bucket(Bucket=bucket_name)

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(ClientError, unauthenticated_client.delete_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

def test_object_raw_get_object_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

def test_bucket_head():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.head_bucket(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_bucket_head_notexist():
    bucket_name = get_new_bucket_name()
    client = get_client()

    e = assert_raises(ClientError, client.head_bucket, Bucket=bucket_name)

    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    # n.b., RGW does not send a response document for this operation,
    # which seems consistent with
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
    #assert error_code == 'NoSuchKey'

@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_bucket_head_extended():
    bucket_name = get_new_bucket()
    client = get_client()

    def add_read_stats_param(request, **kwargs):
        request.params['read-stats'] = 'true'
    client.meta.events.register('request-created.s3.HeadBucket', add_read_stats_param)

    response = client.head_bucket(Bucket=bucket_name)
    assert int(response['ResponseMetadata']['HTTPHeaders']['x-rgw-object-count']) == 0
    assert int(response['ResponseMetadata']['HTTPHeaders']['x-rgw-bytes-used']) == 0

    _create_objects(bucket_name=bucket_name, keys=['foo','bar','baz'])
    response = client.head_bucket(Bucket=bucket_name)

    assert int(response['ResponseMetadata']['HTTPHeaders']['x-rgw-object-count']) == 3
    assert int(response['ResponseMetadata']['HTTPHeaders']['x-rgw-bytes-used']) == 9

def test_object_raw_get_bucket_acl():
    bucket_name = _setup_bucket_object_acl('private', 'public-read')

    unauthenticated_client = get_unauthenticated_client()
    response = unauthenticated_client.get_object(Bucket=bucket_name, Key='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_object_raw_get_object_acl():
    bucket_name = _setup_bucket_object_acl('public-read', 'private')

    unauthenticated_client = get_unauthenticated_client()
    e = assert_raises(ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

def test_object_put_acl_mtime():
    key = 'foo'
    bucket_name = get_new_bucket()
    # Enable versioning
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    client = get_client()

    content = 'foooz'
    client.put_object(Bucket=bucket_name, Key=key, Body=content)
    
    obj_response = client.head_object(Bucket=bucket_name, Key=key)
    create_mtime = obj_response['LastModified']

    response  = client.list_objects(Bucket=bucket_name)
    obj_list = response['Contents'][0]
    _compare_dates(obj_list['LastModified'],create_mtime)

    response  = client.list_object_versions(Bucket=bucket_name)
    obj_list = response['Versions'][0]
    _compare_dates(obj_list['LastModified'],create_mtime)

    # set acl
    time.sleep(2)
    client.put_object_acl(ACL='private',Bucket=bucket_name, Key=key)
    
    # mtime should match with create mtime
    obj_response = client.head_object(Bucket=bucket_name, Key=key)
    _compare_dates(create_mtime,obj_response['LastModified'])

    response  = client.list_objects(Bucket=bucket_name)
    obj_list = response['Contents'][0]
    _compare_dates(obj_list['LastModified'],create_mtime)

    response  = client.list_object_versions(Bucket=bucket_name)
    obj_list = response['Versions'][0]
    _compare_dates(obj_list['LastModified'],create_mtime)

def test_object_raw_authenticated():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_object_raw_response_headers():
    bucket_name = _setup_bucket_object_acl('private', 'private')

    client = get_client()

    response = client.get_object(Bucket=bucket_name, Key='foo', ResponseCacheControl='no-cache', ResponseContentDisposition='bla', ResponseContentEncoding='aaa', ResponseContentLanguage='esperanto', ResponseContentType='foo/bar', ResponseExpires='123')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['ResponseMetadata']['HTTPHeaders']['content-type'] == 'foo/bar'
    assert response['ResponseMetadata']['HTTPHeaders']['content-disposition'] == 'bla'
    assert response['ResponseMetadata']['HTTPHeaders']['content-language'] == 'esperanto'
    assert response['ResponseMetadata']['HTTPHeaders']['content-encoding'] == 'aaa'
    assert response['ResponseMetadata']['HTTPHeaders']['cache-control'] == 'no-cache'

def test_object_raw_authenticated_bucket_acl():
    bucket_name = _setup_bucket_object_acl('private', 'public-read')

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_object_raw_authenticated_object_acl():
    bucket_name = _setup_bucket_object_acl('public-read', 'private')

    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_object_raw_authenticated_bucket_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')
    client.delete_bucket(Bucket=bucket_name)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

def test_object_raw_authenticated_object_gone():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()

    client.delete_object(Bucket=bucket_name, Key='foo')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

def _test_object_raw_get_x_amz_expires_not_expired(client):
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read', client=client)
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=100000, HttpMethod='GET')

    res = requests.options(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 400

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 200

def test_object_raw_get_x_amz_expires_not_expired():
    _test_object_raw_get_x_amz_expires_not_expired(client=get_client())

def test_object_raw_get_x_amz_expires_not_expired_tenant():
    _test_object_raw_get_x_amz_expires_not_expired(client=get_tenant_client())

def test_object_raw_get_x_amz_expires_out_range_zero():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=0, HttpMethod='GET')

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 403

def test_object_raw_get_x_amz_expires_out_max_range():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=609901, HttpMethod='GET')

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 403

def test_object_raw_get_x_amz_expires_out_positive_range():
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read')
    client = get_client()
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=-7, HttpMethod='GET')

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 403

def test_object_content_encoding_aws_chunked():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'encoding'

    client.put_object(Bucket=bucket, Key=key, ContentEncoding='gzip')
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentEncoding'] == 'gzip'

    client.put_object(Bucket=bucket, Key=key, ContentEncoding='deflate, gzip')
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentEncoding'] == 'deflate, gzip'

    client.put_object(Bucket=bucket, Key=key, ContentEncoding='gzip, aws-chunked')
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentEncoding'] == 'gzip'

    client.put_object(Bucket=bucket, Key=key, ContentEncoding='aws-chunked, gzip')
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentEncoding'] == 'gzip'

    client.put_object(Bucket=bucket, Key=key, ContentEncoding='aws-chunked')
    response = client.head_object(Bucket=bucket, Key=key)
    assert 'ContentEncoding' not in response

    client.put_object(Bucket=bucket, Key=key, ContentEncoding='aws-chunked, aws-chunked')
    response = client.head_object(Bucket=bucket, Key=key)
    assert 'ContentEncoding' not in response

def test_object_anon_put():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo')

    unauthenticated_client = get_unauthenticated_client()

    e = assert_raises(ClientError, unauthenticated_client.put_object, Bucket=bucket_name, Key='foo', Body='foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

def test_object_anon_put_write_access():
    bucket_name = _setup_bucket_acl('public-read-write')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo')

    unauthenticated_client = get_unauthenticated_client()

    response = unauthenticated_client.put_object(Bucket=bucket_name, Key='foo', Body='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def test_object_put_authenticated():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.put_object(Bucket=bucket_name, Key='foo', Body='foo')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def _test_object_presigned_put_object_with_acl(client=None):
    if client is None:
        client = get_client()

    bucket_name = get_new_bucket(client)
    key = 'foo'

    params = {'Bucket': bucket_name, 'Key': key, 'ACL': 'private'}
    url = client.generate_presigned_url(ClientMethod='put_object', Params=params, HttpMethod='PUT')

    data = b'hello world'
    headers = {'x-amz-acl': 'private'}
    res = requests.put(url, data=data, headers=headers, verify=get_config_ssl_verify())
    assert res.status_code == 200

    params = {'Bucket': bucket_name, 'Key': key}
    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, HttpMethod='GET')

    res = requests.get(url, verify=get_config_ssl_verify())
    assert res.status_code == 200
    assert res.text == 'hello world'

def test_object_presigned_put_object_with_acl():
    _test_object_presigned_put_object_with_acl(
        client=get_client())

def test_object_presigned_put_object_with_acl_tenant():
    _test_object_presigned_put_object_with_acl(
        client=get_tenant_client())

def test_object_raw_put_authenticated_expired():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo')

    params = {'Bucket': bucket_name, 'Key': 'foo'}
    url = client.generate_presigned_url(ClientMethod='put_object', Params=params, ExpiresIn=-1000, HttpMethod='PUT')

    # params wouldn't take a 'Body' parameter so we're passing it in here
    res = requests.put(url, data="foo", verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 403

def check_bad_bucket_name(bucket_name):
    """
    Attempt to create a bucket with a specified name, and confirm
    that the request fails because of an invalid bucket name.
    """
    client = get_client()
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidBucketName'


# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@pytest.mark.fails_on_aws
# Breaks DNS with SubdomainCallingFormat
def test_bucket_create_naming_bad_starts_nonalpha():
    bucket_name = get_new_bucket_name()
    check_bad_bucket_name('_' + bucket_name)

def check_invalid_bucketname(invalid_name):
    """
    Send a create bucket_request with an invalid bucket name
    that will bypass the ParamValidationError that would be raised
    if the invalid bucket name that was passed in normally.
    This function returns the status and error code from the failure
    """
    client = get_client()
    valid_bucket_name = get_new_bucket_name()
    def replace_bucketname_from_url(**kwargs):
        url = kwargs['params']['url']
        new_url = url.replace(valid_bucket_name, invalid_name)
        kwargs['params']['url'] = new_url
    client.meta.events.register('before-call.s3.CreateBucket', replace_bucketname_from_url)
    e = assert_raises(ClientError, client.create_bucket, Bucket=invalid_name)
    status, error_code = _get_status_and_error_code(e.response)
    return (status, error_code)

def test_bucket_create_naming_bad_short_one():
    check_bad_bucket_name('a')

def test_bucket_create_naming_bad_short_two():
    check_bad_bucket_name('aa')

def check_good_bucket_name(name, _prefix=None):
    """
    Attempt to create a bucket with a specified name
    and (specified or default) prefix, returning the
    results of that effort.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    if _prefix is None:
        _prefix = get_prefix()
    bucket_name = '{prefix}{name}'.format(
            prefix=_prefix,
            name=name,
            )
    client = get_client()
    response = client.create_bucket(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def _test_bucket_create_naming_good_long(length):
    """
    Attempt to create a bucket whose name (including the
    prefix) is of a specified length.
    """
    # tests using this with the default prefix must *not* rely on
    # being able to set the initial character, or exceed the max len

    # tests using this with a custom prefix are responsible for doing
    # their own setup/teardown nukes, with their custom prefix; this
    # should be very rare
    prefix = get_new_bucket_name()
    assert len(prefix) < 63
    num = length - len(prefix)
    name=num*'a'

    bucket_name = '{prefix}{name}'.format(
            prefix=prefix,
            name=name,
            )
    client = get_client()
    response = client.create_bucket(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_good_long_60():
    _test_bucket_create_naming_good_long(60)

# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_good_long_61():
    _test_bucket_create_naming_good_long(61)

# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_good_long_62():
    _test_bucket_create_naming_good_long(62)


# Breaks DNS with SubdomainCallingFormat
def test_bucket_create_naming_good_long_63():
    _test_bucket_create_naming_good_long(63)


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_list_long_name():
    prefix = get_new_bucket_name()
    length = 61
    num = length - len(prefix)
    name=num*'a'

    bucket_name = '{prefix}{name}'.format(
            prefix=prefix,
            name=name,
            )
    bucket = get_new_bucket_resource(name=bucket_name)
    is_empty = _bucket_is_empty(bucket)
    assert is_empty == True

# AWS does not enforce all documented bucket restrictions.
# http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/index.html?BucketRestrictions.html
@pytest.mark.fails_on_aws
def test_bucket_create_naming_bad_ip():
    check_bad_bucket_name('192.168.5.123')

# test_bucket_create_naming_dns_* are valid but not recommended
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_underscore():
    invalid_bucketname = 'foo_bar'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == 'InvalidBucketName'

# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
def test_bucket_create_naming_dns_long():
    prefix = get_prefix()
    assert len(prefix) < 50
    num = 63 - len(prefix)
    check_good_bucket_name(num * 'a')

# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dash_at_end():
    invalid_bucketname = 'foo-'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == 'InvalidBucketName'


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dot_dot():
    invalid_bucketname = 'foo..bar'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == 'InvalidBucketName'


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dot_dash():
    invalid_bucketname = 'foo.-bar'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == 'InvalidBucketName'


# Breaks DNS with SubdomainCallingFormat
@pytest.mark.fails_on_aws
# Should now pass on AWS even though it has 'fails_on_aws' attr.
def test_bucket_create_naming_dns_dash_dot():
    invalid_bucketname = 'foo-.bar'
    status, error_code = check_invalid_bucketname(invalid_bucketname)
    assert status == 400
    assert error_code == 'InvalidBucketName'

def test_bucket_create_exists():
    # aws-s3 default region allows recreation of buckets
    # but all other regions fail with BucketAlreadyOwnedByYou.
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name)
    try:
        response = client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        status, error_code = _get_status_and_error_code(e.response)
        assert e.status == 409
        assert e.error_code == 'BucketAlreadyOwnedByYou'

@pytest.mark.fails_on_dbstore
def test_bucket_get_location():
    location_constraint = get_main_api_name()
    if not location_constraint:
        pytest.skip('no api_name configured')
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': location_constraint})

    response = client.get_bucket_location(Bucket=bucket_name)
    if location_constraint == "":
        location_constraint = None
    assert response['LocationConstraint'] == location_constraint

@pytest.mark.fails_on_dbstore
def test_bucket_create_exists_nonowner():
    # Names are shared across a global namespace. As such, no two
    # users can create a bucket with that same name.
    bucket_name = get_new_bucket_name()
    client = get_client()

    alt_client = get_alt_client()

    client.create_bucket(Bucket=bucket_name)
    e = assert_raises(ClientError, alt_client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'BucketAlreadyExists'

@pytest.mark.fails_on_dbstore
def test_bucket_recreate_overwrite_acl():
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name, ACL='public-read')
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'BucketAlreadyExists'

@pytest.mark.fails_on_dbstore
def test_bucket_recreate_new_acl():
    bucket_name = get_new_bucket_name()
    client = get_client()

    client.create_bucket(Bucket=bucket_name)
    e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name, ACL='public-read')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'BucketAlreadyExists'

def check_access_denied(fn, *args, **kwargs):
    e = assert_raises(ClientError, fn, *args, **kwargs)
    status = _get_status(e.response)
    assert status == 403

def check_request_timeout(fn, *args, **kwargs):
    e = assert_raises(ClientError, fn, *args, **kwargs)
    status = _get_status(e.response)
    assert status == 400

def check_grants(got, want):
    """
    Check that grants list in got matches the dictionaries in want,
    in any order.
    """
    assert len(got) == len(want)

    # There are instances when got does not match due the order of item.
    if got[0]["Grantee"].get("DisplayName"):
        got.sort(key=lambda x: x["Grantee"].get("DisplayName"))
        want.sort(key=lambda x: x["DisplayName"])

    for g, w in zip(got, want):
        w = dict(w)
        g = dict(g)
        assert g.pop('Permission', None) == w['Permission']
        assert g['Grantee'].pop('DisplayName', None) == w['DisplayName']
        assert g['Grantee'].pop('ID', None) == w['ID']
        assert g['Grantee'].pop('Type', None) == w['Type']
        assert g['Grantee'].pop('URI', None) == w['URI']
        assert g['Grantee'].pop('EmailAddress', None) == w['EmailAddress']
        assert g == {'Grantee': {}}


def test_bucket_acl_default():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    assert response['Owner']['DisplayName'] == display_name
    assert response['Owner']['ID'] == user_id

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@pytest.mark.fails_on_aws
def test_bucket_acl_canned_during_create():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_bucket_acl_canned():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    client.put_bucket_acl(ACL='private', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_bucket_acl_canned_publicreadwrite():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='WRITE',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_bucket_acl_canned_authenticatedread():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(ACL='authenticated-read', Bucket=bucket_name)
    response = client.get_bucket_acl(Bucket=bucket_name)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_put_bucket_acl_grant_group_read():
    bucket_name = get_new_bucket()
    client = get_client()
    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grant = {'Grantee': {'Type': 'Group', 'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'}, 'Permission': 'READ'}
    policy = add_bucket_user_grant(bucket_name, grant)

    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    response = client.get_bucket_acl(Bucket=bucket_name)

    check_grants(
        response['Grants'],
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_object_acl_default():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()


    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_object_acl_canned_during_create():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(ACL='public-read', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()


    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_object_acl_canned():
    bucket_name = get_new_bucket()
    client = get_client()

    # Since it defaults to private, set it public-read first
    client.put_object(ACL='public-read', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    # Then back to private.
    client.put_object_acl(ACL='private',Bucket=bucket_name, Key='foo')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')
    grants = response['Grants']

    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_object_acl_canned_publicreadwrite():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(ACL='public-read-write', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='WRITE',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_object_acl_canned_authenticatedread():
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(ACL='authenticated-read', Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_object_acl_canned_bucketownerread():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')

    alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
    bucket_owner_id = bucket_acl_response['Grants'][2]['Grantee']['ID']
    bucket_owner_display_name = bucket_acl_response['Grants'][2]['Grantee']['DisplayName']

    alt_client.put_object(ACL='bucket-owner-read', Bucket=bucket_name, Key='foo')
    response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')

    alt_display_name = get_alt_display_name()
    alt_user_id = get_alt_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='READ',
                ID=bucket_owner_id,
                DisplayName=bucket_owner_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def test_object_acl_canned_bucketownerfullcontrol():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')

    alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
    bucket_owner_id = bucket_acl_response['Grants'][2]['Grantee']['ID']
    bucket_owner_display_name = bucket_acl_response['Grants'][2]['Grantee']['DisplayName']

    alt_client.put_object(ACL='bucket-owner-full-control', Bucket=bucket_name, Key='foo')
    response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')

    alt_display_name = get_alt_display_name()
    alt_user_id = get_alt_user_id()

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=bucket_owner_id,
                DisplayName=bucket_owner_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@pytest.mark.fails_on_aws
def test_object_acl_full_control_verify_owner():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')

    main_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    grant = { 'Grants': [{'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'FULL_CONTROL'}], 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    main_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grant)

    grant = { 'Grants': [{'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'READ_ACP'}], 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    alt_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grant)

    response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')
    assert response['Owner']['ID'] == main_user_id

def add_obj_user_grant(bucket_name, key, grant):
    """
    Adds a grant to the existing grants meant to be passed into
    the AccessControlPolicy argument of put_object_acls for an object
    owned by the main user, not the alt user
    A grant is a dictionary in the form of:
    {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}

    """
    client = get_client()
    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    response = client.get_object_acl(Bucket=bucket_name, Key=key)

    grants = response['Grants']
    grants.append(grant)

    grant = {'Grants': grants, 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    return grant

def test_object_acl_full_control_verify_attributes():
    bucket_name = get_new_bucket_name()
    main_client = get_client()
    alt_client = get_alt_client()

    main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')

    header = {'x-amz-foo': 'bar'}
    # lambda to add any header
    add_header = (lambda **kwargs: kwargs['params']['headers'].update(header))

    main_client.meta.events.register('before-call.s3.PutObject', add_header)
    main_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = main_client.get_object(Bucket=bucket_name, Key='foo')
    content_type = response['ContentType']
    etag = response['ETag']

    alt_user_id = get_alt_user_id()

    grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'FULL_CONTROL'}

    grants = add_obj_user_grant(bucket_name, 'foo', grant)

    main_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grants)

    response = main_client.get_object(Bucket=bucket_name, Key='foo')
    assert content_type == response['ContentType']
    assert etag == response['ETag']

def test_bucket_acl_canned_private_to_private():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.put_bucket_acl(Bucket=bucket_name, ACL='private')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def add_bucket_user_grant(bucket_name, grant):
    """
    Adds a grant to the existing grants meant to be passed into
    the AccessControlPolicy argument of put_object_acls for an object
    owned by the main user, not the alt user
    A grant is a dictionary in the form of:
    {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}
    """
    client = get_client()
    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    grants.append(grant)

    grant = {'Grants': grants, 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

    return grant

def _check_object_acl(permission):
    """
    Sets the permission on an object then checks to see
    if it was set
    """
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.get_object_acl(Bucket=bucket_name, Key='foo')

    policy = {}
    policy['Owner'] = response['Owner']
    policy['Grants'] = response['Grants']
    policy['Grants'][0]['Permission'] = permission

    client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=policy)

    response = client.get_object_acl(Bucket=bucket_name, Key='foo')
    grants = response['Grants']

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    check_grants(
        grants,
        [
            dict(
                Permission=permission,
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )


@pytest.mark.fails_on_aws
def test_object_acl():
    _check_object_acl('FULL_CONTROL')

@pytest.mark.fails_on_aws
def test_object_acl_write():
    _check_object_acl('WRITE')

@pytest.mark.fails_on_aws
def test_object_acl_writeacp():
    _check_object_acl('WRITE_ACP')


@pytest.mark.fails_on_aws
def test_object_acl_read():
    _check_object_acl('READ')


@pytest.mark.fails_on_aws
def test_object_acl_readacp():
    _check_object_acl('READ_ACP')


def _bucket_acl_grant_userid(permission):
    """
    create a new bucket, grant a specific user the specified
    permission, read back the acl and verify correct setting
    """
    bucket_name = get_new_bucket()
    client = get_client()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': permission}

    grant = add_bucket_user_grant(bucket_name, grant)

    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission=permission,
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    return bucket_name

def _check_bucket_acl_grant_can_read(bucket_name):
    """
    verify ability to read the specified bucket
    """
    alt_client = get_alt_client()
    response = alt_client.head_bucket(Bucket=bucket_name)

def _check_bucket_acl_grant_cant_read(bucket_name):
    """
    verify inability to read the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.head_bucket, Bucket=bucket_name)

def _check_bucket_acl_grant_can_readacp(bucket_name):
    """
    verify ability to read acls on specified bucket
    """
    alt_client = get_alt_client()
    alt_client.get_bucket_acl(Bucket=bucket_name)

def _check_bucket_acl_grant_cant_readacp(bucket_name):
    """
    verify inability to read acls on specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.get_bucket_acl, Bucket=bucket_name)

def _check_bucket_acl_grant_can_write(bucket_name):
    """
    verify ability to write the specified bucket
    """
    alt_client = get_alt_client()
    alt_client.put_object(Bucket=bucket_name, Key='foo-write', Body='bar')

def _check_bucket_acl_grant_cant_write(bucket_name):

    """
    verify inability to write the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key='foo-write', Body='bar')

def _check_bucket_acl_grant_can_writeacp(bucket_name):
    """
    verify ability to set acls on the specified bucket
    """
    alt_client = get_alt_client()
    alt_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

def _check_bucket_acl_grant_cant_writeacp(bucket_name):
    """
    verify inability to set acls on the specified bucket
    """
    alt_client = get_alt_client()
    check_access_denied(alt_client.put_bucket_acl,Bucket=bucket_name, ACL='public-read')

@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_fullcontrol():
    bucket_name = _bucket_acl_grant_userid('FULL_CONTROL')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket_name)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket_name)
    # can write
    _check_bucket_acl_grant_can_write(bucket_name)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket_name)

    client = get_client()

    bucket_acl_response = client.get_bucket_acl(Bucket=bucket_name)
    owner_id = bucket_acl_response['Owner']['ID']
    owner_display_name = bucket_acl_response['Owner']['DisplayName']

    main_display_name = get_main_display_name()
    main_user_id = get_main_user_id()

    assert owner_id == main_user_id
    assert owner_display_name == main_display_name

@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_read():
    bucket_name = _bucket_acl_grant_userid('READ')

    # alt user can read
    _check_bucket_acl_grant_can_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket_name)

@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_readacp():
    bucket_name = _bucket_acl_grant_userid('READ_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can read acl
    _check_bucket_acl_grant_can_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can't write acp
    #_check_bucket_acl_grant_cant_writeacp_can_readacp(bucket)
    _check_bucket_acl_grant_cant_writeacp(bucket_name)

@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_write():
    bucket_name = _bucket_acl_grant_userid('WRITE')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can write
    _check_bucket_acl_grant_can_write(bucket_name)
    # can't write acl
    _check_bucket_acl_grant_cant_writeacp(bucket_name)

@pytest.mark.fails_on_aws
def test_bucket_acl_grant_userid_writeacp():
    bucket_name = _bucket_acl_grant_userid('WRITE_ACP')

    # alt user can't read
    _check_bucket_acl_grant_cant_read(bucket_name)
    # can't read acl
    _check_bucket_acl_grant_cant_readacp(bucket_name)
    # can't write
    _check_bucket_acl_grant_cant_write(bucket_name)
    # can write acl
    _check_bucket_acl_grant_can_writeacp(bucket_name)

def test_bucket_acl_grant_nonexist_user():
    bucket_name = get_new_bucket()
    client = get_client()

    bad_user_id = '_foo'

    #response = client.get_bucket_acl(Bucket=bucket_name)
    grant = {'Grantee': {'ID': bad_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'FULL_CONTROL'}

    grant = add_bucket_user_grant(bucket_name, grant)

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, AccessControlPolicy=grant)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

def _get_acl_header(user_id=None, perms=None):
    all_headers = ["read", "write", "read-acp", "write-acp", "full-control"]
    headers = []

    if user_id == None:
        user_id = get_alt_user_id()

    if perms != None:
        for perm in perms:
            header = ("x-amz-grant-{perm}".format(perm=perm), "id={uid}".format(uid=user_id))
            headers.append(header)

    else:
        for perm in all_headers:
            header = ("x-amz-grant-{perm}".format(perm=perm), "id={uid}".format(uid=user_id))
            headers.append(header)

    return headers

@pytest.mark.fails_on_dho
@pytest.mark.fails_on_aws
def test_object_header_acl_grants():
    bucket_name = get_new_bucket()
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    headers = _get_acl_header()

    def add_headers_before_sign(**kwargs):
        updated_headers = (kwargs['request'].__dict__['headers'].__dict__['_headers'] + headers)
        kwargs['request'].__dict__['headers'].__dict__['_headers'] = updated_headers

    client.meta.events.register('before-sign.s3.PutObject', add_headers_before_sign)

    client.put_object(Bucket=bucket_name, Key='foo_key', Body='bar')

    response = client.get_object_acl(Bucket=bucket_name, Key='foo_key')

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='READ_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

@pytest.mark.fails_on_dho
@pytest.mark.fails_on_aws
def test_bucket_header_acl_grants():
    headers = _get_acl_header()
    bucket_name = get_new_bucket_name()
    client = get_client()

    headers = _get_acl_header()

    def add_headers_before_sign(**kwargs):
        updated_headers = (kwargs['request'].__dict__['headers'].__dict__['_headers'] + headers)
        kwargs['request'].__dict__['headers'].__dict__['_headers'] = updated_headers

    client.meta.events.register('before-sign.s3.CreateBucket', add_headers_before_sign)

    client.create_bucket(Bucket=bucket_name)

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()

    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='READ_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='WRITE_ACP',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    alt_client = get_alt_client()

    alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    # set bucket acl to public-read-write so that teardown can work
    alt_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')


# This test will fail on DH Objects. DHO allows multiple users with one account, which
# would violate the uniqueness requirement of a user's email. As such, DHO users are
# created without an email.
@pytest.mark.fails_on_aws
def test_bucket_acl_grant_email():
    bucket_name = get_new_bucket()
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()
    alt_email_address = get_alt_email()

    main_user_id = get_main_user_id()
    main_display_name = get_main_display_name()

    grant = {'Grantee': {'EmailAddress': alt_email_address, 'Type': 'AmazonCustomerByEmail' }, 'Permission': 'FULL_CONTROL'}

    grant = add_bucket_user_grant(bucket_name, grant)

    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy = grant)

    response = client.get_bucket_acl(Bucket=bucket_name)

    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='FULL_CONTROL',
                ID=alt_user_id,
                DisplayName=alt_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=main_user_id,
                DisplayName=main_display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
        ]
    )

def test_bucket_acl_grant_email_not_exist():
    # behavior not documented by amazon
    bucket_name = get_new_bucket()
    client = get_client()

    alt_user_id = get_alt_user_id()
    alt_display_name = get_alt_display_name()
    alt_email_address = get_alt_email()

    NONEXISTENT_EMAIL = 'doesnotexist@dreamhost.com.invalid'
    grant = {'Grantee': {'EmailAddress': NONEXISTENT_EMAIL, 'Type': 'AmazonCustomerByEmail'}, 'Permission': 'FULL_CONTROL'}

    grant = add_bucket_user_grant(bucket_name, grant)

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, AccessControlPolicy = grant)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'UnresolvableGrantByEmailAddress'

def test_bucket_acl_revoke_all():
    # revoke all access, including the owner's access
    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    response = client.get_bucket_acl(Bucket=bucket_name)
    old_grants = response['Grants']
    policy = {}
    policy['Owner'] = response['Owner']
    # clear grants
    policy['Grants'] = []

    # remove read/write permission for everyone
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    response = client.get_bucket_acl(Bucket=bucket_name)

    assert len(response['Grants']) == 0

    # set policy back to original so that bucket can be cleaned up
    policy['Grants'] = old_grants
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

def _setup_access(bucket_acl, object_acl):
    """
    Simple test fixture: create a bucket with given ACL, with objects:
    - a: owning user, given ACL
    - a2: same object accessed by some other user
    - b: owning user, default ACL in bucket w/given ACL
    - b2: same object accessed by a some other user
    """
    bucket_name = get_new_bucket()
    client = get_client()

    key1 = 'foo'
    key2 = 'bar'
    newkey = 'new'

    client.put_bucket_acl(Bucket=bucket_name, ACL=bucket_acl)
    client.put_object(Bucket=bucket_name, Key=key1, Body='foocontent')
    client.put_object_acl(Bucket=bucket_name, Key=key1, ACL=object_acl)
    client.put_object(Bucket=bucket_name, Key=key2, Body='barcontent')

    return bucket_name, key1, key2, newkey

def get_bucket_key_names(bucket_name):
    objs_list = get_objects_list(bucket_name)
    return frozenset(obj for obj in objs_list)

def list_bucket_storage_class(client, bucket_name):
    result = defaultdict(list)
    response  = client.list_object_versions(Bucket=bucket_name)
    for k in response['Versions']:
        result[k['StorageClass']].append(k)

    return result

def list_bucket_versions(client, bucket_name):
    result = defaultdict(list)
    response  = client.list_object_versions(Bucket=bucket_name)
    for k in response['Versions']:
        result[response['Name']].append(k)

    return result

def test_access_bucket_private_object_private():
    # all the test_access_* tests follow this template
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='private', object_acl='private')

    alt_client = get_alt_client()
    # acled object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    # default object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    # bucket read fail
    check_access_denied(alt_client.list_objects, Bucket=bucket_name)

    # acled object write fail
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='barcontent')
    # NOTE: The above put's causes the connection to go bad, therefore the client can't be used
    # anymore. This can be solved either by:
    # 1) putting an empty string ('') in the 'Body' field of those put_object calls
    # 2) getting a new client hence the creation of alt_client{2,3} for the tests below
    # TODO: Test it from another host and on AWS, Report this to Amazon, if findings are identical

    alt_client2 = get_alt_client()
    # default object write fail
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')
    # bucket write fail
    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

@pytest.mark.list_objects_v2
def test_access_bucket_private_objectv2_private():
    # all the test_access_* tests follow this template
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='private', object_acl='private')

    alt_client = get_alt_client()
    # acled object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    # default object read fail
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    # bucket read fail
    check_access_denied(alt_client.list_objects_v2, Bucket=bucket_name)

    # acled object write fail
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='barcontent')
    # NOTE: The above put's causes the connection to go bad, therefore the client can't be used
    # anymore. This can be solved either by:
    # 1) putting an empty string ('') in the 'Body' field of those put_object calls
    # 2) getting a new client hence the creation of alt_client{2,3} for the tests below
    # TODO: Test it from another host and on AWS, Report this to Amazon, if findings are identical

    alt_client2 = get_alt_client()
    # default object write fail
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')
    # bucket write fail
    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

def test_access_bucket_private_object_publicread():

    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='private', object_acl='public-read')
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read, b gets default (private)
    assert body == 'foocontent'

    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects, Bucket=bucket_name)
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

@pytest.mark.list_objects_v2
def test_access_bucket_private_objectv2_publicread():

    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='private', object_acl='public-read')
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read, b gets default (private)
    assert body == 'foocontent'

    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects_v2, Bucket=bucket_name)
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

def test_access_bucket_private_object_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='private', object_acl='public-read-write')
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read-only ... because it is in a private bucket
    # b gets default (private)
    assert body == 'foocontent'

    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects, Bucket=bucket_name)
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

@pytest.mark.list_objects_v2
def test_access_bucket_private_objectv2_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='private', object_acl='public-read-write')
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read-only ... because it is in a private bucket
    # b gets default (private)
    assert body == 'foocontent'

    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

    alt_client3 = get_alt_client()
    check_access_denied(alt_client3.list_objects_v2, Bucket=bucket_name)
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

def test_access_bucket_publicread_object_private():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='public-read', object_acl='private')
    alt_client = get_alt_client()

    # a should be private, b gets default (private)
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='barcontent')

    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

    alt_client3 = get_alt_client()

    objs = get_objects_list(bucket=bucket_name, client=alt_client3)

    assert objs == ['bar', 'foo']
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')

def test_access_bucket_publicread_object_publicread():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='public-read', object_acl='public-read')
    alt_client = get_alt_client()

    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    # a should be public-read, b gets default (private)
    body = _get_body(response)
    assert body == 'foocontent'

    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')

    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

    alt_client3 = get_alt_client()

    objs = get_objects_list(bucket=bucket_name, client=alt_client3)

    assert objs == ['bar', 'foo']
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')


def test_access_bucket_publicread_object_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='public-read', object_acl='public-read-write')
    alt_client = get_alt_client()

    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)

    # a should be public-read-only ... because it is in a r/o bucket
    # b gets default (private)
    assert body == 'foocontent'

    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')

    alt_client2 = get_alt_client()
    check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
    check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

    alt_client3 = get_alt_client()

    objs = get_objects_list(bucket=bucket_name, client=alt_client3)

    assert objs == ['bar', 'foo']
    check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=newkey, Body='newcontent')


def test_access_bucket_publicreadwrite_object_private():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='public-read-write', object_acl='private')
    alt_client = get_alt_client()

    # a should be private, b gets default (private)
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
    alt_client.put_object(Bucket=bucket_name, Key=key1, Body='barcontent')

    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    alt_client.put_object(Bucket=bucket_name, Key=key2, Body='baroverwrite')

    objs = get_objects_list(bucket=bucket_name, client=alt_client)
    assert objs == ['bar', 'foo']
    alt_client.put_object(Bucket=bucket_name, Key=newkey, Body='newcontent')

def test_access_bucket_publicreadwrite_object_publicread():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='public-read-write', object_acl='public-read')
    alt_client = get_alt_client()

    # a should be public-read, b gets default (private)
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)

    body = _get_body(response)
    assert body == 'foocontent'
    alt_client.put_object(Bucket=bucket_name, Key=key1, Body='barcontent')

    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    alt_client.put_object(Bucket=bucket_name, Key=key2, Body='baroverwrite')

    objs = get_objects_list(bucket=bucket_name, client=alt_client)
    assert objs == ['bar', 'foo']
    alt_client.put_object(Bucket=bucket_name, Key=newkey, Body='newcontent')

def test_access_bucket_publicreadwrite_object_publicreadwrite():
    bucket_name, key1, key2, newkey = _setup_access(bucket_acl='public-read-write', object_acl='public-read-write')
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key=key1)
    body = _get_body(response)

    # a should be public-read-write, b gets default (private)
    assert body == 'foocontent'
    alt_client.put_object(Bucket=bucket_name, Key=key1, Body='foooverwrite')
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
    alt_client.put_object(Bucket=bucket_name, Key=key2, Body='baroverwrite')
    objs = get_objects_list(bucket=bucket_name, client=alt_client)
    assert objs == ['bar', 'foo']
    alt_client.put_object(Bucket=bucket_name, Key=newkey, Body='newcontent')

def test_buckets_create_then_list():
    client = get_client()
    bucket_names = []
    for i in range(5):
        bucket_name = get_new_bucket_name()
        bucket_names.append(bucket_name)

    for name in bucket_names:
        client.create_bucket(Bucket=name)

    response = client.list_buckets()
    bucket_dicts = response['Buckets']
    buckets_list = []

    buckets_list = get_buckets_list()

    for name in bucket_names:
        if name not in buckets_list:
            raise RuntimeError("S3 implementation's GET on Service did not return bucket we created: %r", bucket.name)

def test_buckets_list_ctime():
    # check that creation times are within a day
    before = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)

    client = get_client()
    buckets = []
    for i in range(5):
        name = get_new_bucket_name()
        client.create_bucket(Bucket=name)
        buckets.append(name)

    response = client.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'] in buckets:
            ctime = bucket['CreationDate']
            assert before <= ctime, '%r > %r' % (before, ctime)

@pytest.mark.fails_on_aws
def test_list_buckets_anonymous():
    # Get a connection with bad authorization, then change it to be our new Anonymous auth mechanism,
    # emulating standard HTTP access.
    #
    # While it may have been possible to use httplib directly, doing it this way takes care of also
    # allowing us to vary the calling format in testing.
    unauthenticated_client = get_unauthenticated_client()
    response = unauthenticated_client.list_buckets()
    assert len(response['Buckets']) == 0

def test_list_buckets_invalid_auth():
    bad_auth_client = get_bad_auth_client()
    e = assert_raises(ClientError, bad_auth_client.list_buckets)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

def test_list_buckets_bad_auth():
    main_access_key = get_main_aws_access_key()
    bad_auth_client = get_bad_auth_client(aws_access_key_id=main_access_key)
    e = assert_raises(ClientError, bad_auth_client.list_buckets)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

@pytest.mark.fails_on_dbstore
def test_list_buckets_paginated():
    client = get_client()

    response = client.list_buckets(MaxBuckets=1)
    assert len(response['Buckets']) == 0
    assert 'ContinuationToken' not in response

    bucket1 = get_new_bucket()

    response = client.list_buckets(MaxBuckets=1)
    assert len(response['Buckets']) == 1
    assert response['Buckets'][0]['Name'] == bucket1
    assert 'ContinuationToken' not in response

    bucket2 = get_new_bucket()

    response = client.list_buckets(MaxBuckets=1)
    assert len(response['Buckets']) == 1
    assert response['Buckets'][0]['Name'] == bucket1
    continuation = response['ContinuationToken']

    response = client.list_buckets(MaxBuckets=1, ContinuationToken=continuation)
    assert len(response['Buckets']) == 1
    assert response['Buckets'][0]['Name'] == bucket2
    assert 'ContinuationToken' not in response

@pytest.fixture
def override_prefix_a():
    nuke_prefixed_buckets(prefix='a'+get_prefix())
    yield
    nuke_prefixed_buckets(prefix='a'+get_prefix())

# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
def test_bucket_create_naming_good_starts_alpha(override_prefix_a):
    check_good_bucket_name('foo', _prefix='a'+get_prefix())

@pytest.fixture
def override_prefix_0():
    nuke_prefixed_buckets(prefix='0'+get_prefix())
    yield
    nuke_prefixed_buckets(prefix='0'+get_prefix())

# this test goes outside the user-configure prefix because it needs to
# control the initial character of the bucket name
def test_bucket_create_naming_good_starts_digit(override_prefix_0):
    check_good_bucket_name('foo', _prefix='0'+get_prefix())

def test_bucket_create_naming_good_contains_period():
    check_good_bucket_name('aaa.111')

def test_bucket_create_naming_good_contains_hyphen():
    check_good_bucket_name('aaa-111')

def test_bucket_recreate_not_overriding():
    key_names = ['mykey1', 'mykey2']
    bucket_name = _create_objects(keys=key_names)

    objs_list = get_objects_list(bucket_name)
    assert key_names == objs_list

    client = get_client()
    client.create_bucket(Bucket=bucket_name)

    objs_list = get_objects_list(bucket_name)
    assert key_names == objs_list

@pytest.mark.fails_on_dbstore
def test_bucket_create_special_key_names():
    key_names = [
        ' ',
        '"',
        '$',
        '%',
        '&',
        '\'',
        '<',
        '>',
        '_',
        '_ ',
        '_ _',
        '__',
    ]

    bucket_name = _create_objects(keys=key_names)

    objs_list = get_objects_list(bucket_name)
    assert key_names == objs_list

    client = get_client()

    for name in key_names:
        assert name in objs_list
        response = client.get_object(Bucket=bucket_name, Key=name)
        body = _get_body(response)
        assert name == body
        client.put_object_acl(Bucket=bucket_name, Key=name, ACL='private')

def test_bucket_list_special_prefix():
    key_names = ['_bla/1', '_bla/2', '_bla/3', '_bla/4', 'abcd']
    bucket_name = _create_objects(keys=key_names)

    objs_list = get_objects_list(bucket_name)

    assert len(objs_list) == 5

    objs_list = get_objects_list(bucket_name, prefix='_bla/')
    assert len(objs_list) == 4

@pytest.mark.fails_on_dbstore
def test_object_copy_zero_size():
    key = 'foo123bar'
    bucket_name = _create_objects(keys=[key])
    fp_a = FakeWriteFile(0, '')
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key, Body=fp_a)

    copy_source = {'Bucket': bucket_name, 'Key': key}

    client.copy(copy_source, bucket_name, 'bar321foo')
    response = client.get_object(Bucket=bucket_name, Key='bar321foo')
    assert response['ContentLength'] == 0

@pytest.mark.fails_on_dbstore
def test_object_copy_16m():
    bucket_name = get_new_bucket()
    key1 = 'obj1'
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=key1, Body=bytearray(16*1024*1024))

    copy_source = {'Bucket': bucket_name, 'Key': key1}
    key2 = 'obj2'
    client.copy_object(Bucket=bucket_name, Key=key2, CopySource=copy_source)
    response = client.get_object(Bucket=bucket_name, Key=key2)
    assert response['ContentLength'] == 16*1024*1024

@pytest.mark.fails_on_dbstore
def test_object_copy_same_bucket():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

    copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}

    client.copy(copy_source, bucket_name, 'bar321foo')

    response = client.get_object(Bucket=bucket_name, Key='bar321foo')
    body = _get_body(response)
    assert 'foo' == body

@pytest.mark.fails_on_dbstore
def test_object_copy_verify_contenttype():
    bucket_name = get_new_bucket()
    client = get_client()

    content_type = 'text/bla'
    client.put_object(Bucket=bucket_name, ContentType=content_type, Key='foo123bar', Body='foo')

    copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}

    client.copy(copy_source, bucket_name, 'bar321foo')

    response = client.get_object(Bucket=bucket_name, Key='bar321foo')
    body = _get_body(response)
    assert 'foo' == body
    response_content_type = response['ContentType']
    assert response_content_type == content_type

def test_object_copy_to_itself():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

    copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}

    e = assert_raises(ClientError, client.copy, copy_source, bucket_name, 'foo123bar')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidRequest'

@pytest.mark.fails_on_dbstore
def test_object_copy_to_itself_with_metadata():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')
    copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
    metadata = {'foo': 'bar'}

    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='foo123bar', Metadata=metadata, MetadataDirective='REPLACE')
    response = client.get_object(Bucket=bucket_name, Key='foo123bar')
    assert response['Metadata'] == metadata

@pytest.mark.fails_on_dbstore
def test_object_copy_diff_bucket():
    bucket_name1 = get_new_bucket()
    bucket_name2 = get_new_bucket()

    client = get_client()
    client.put_object(Bucket=bucket_name1, Key='foo123bar', Body='foo')

    copy_source = {'Bucket': bucket_name1, 'Key': 'foo123bar'}

    client.copy(copy_source, bucket_name2, 'bar321foo')

    response = client.get_object(Bucket=bucket_name2, Key='bar321foo')
    body = _get_body(response)
    assert 'foo' == body

def test_object_copy_not_owned_bucket():
    client = get_client()
    alt_client = get_alt_client()
    bucket_name1 = get_new_bucket_name()
    bucket_name2 = get_new_bucket_name()
    client.create_bucket(Bucket=bucket_name1)
    alt_client.create_bucket(Bucket=bucket_name2)

    client.put_object(Bucket=bucket_name1, Key='foo123bar', Body='foo')

    copy_source = {'Bucket': bucket_name1, 'Key': 'foo123bar'}

    e = assert_raises(ClientError, alt_client.copy, copy_source, bucket_name2, 'bar321foo')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

def test_object_copy_not_owned_object_bucket():
    client = get_client()
    alt_client = get_alt_client()
    bucket_name = get_new_bucket_name()
    client.create_bucket(Bucket=bucket_name)
    client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

    alt_user_id = get_alt_user_id()

    grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser' }, 'Permission': 'FULL_CONTROL'}
    grants = add_obj_user_grant(bucket_name, 'foo123bar', grant)
    client.put_object_acl(Bucket=bucket_name, Key='foo123bar', AccessControlPolicy=grants)

    grant = add_bucket_user_grant(bucket_name, grant)
    client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

    alt_client.get_object(Bucket=bucket_name, Key='foo123bar')

    copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
    alt_client.copy(copy_source, bucket_name, 'bar321foo')

@pytest.mark.fails_on_dbstore
def test_object_copy_canned_acl():
    bucket_name = get_new_bucket()
    client = get_client()
    alt_client = get_alt_client()
    client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

    copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='bar321foo', ACL='public-read')
    # check ACL is applied by doing GET from another user
    alt_client.get_object(Bucket=bucket_name, Key='bar321foo')


    metadata={'abc': 'def'}
    copy_source = {'Bucket': bucket_name, 'Key': 'bar321foo'}
    client.copy_object(ACL='public-read', Bucket=bucket_name, CopySource=copy_source, Key='foo123bar', Metadata=metadata, MetadataDirective='REPLACE')

    # check ACL is applied by doing GET from another user
    alt_client.get_object(Bucket=bucket_name, Key='foo123bar')

@pytest.mark.fails_on_dbstore
def test_object_copy_retaining_metadata():
    for size in [3, 1024 * 1024]:
        bucket_name = get_new_bucket()
        client = get_client()
        content_type = 'audio/ogg'

        metadata = {'key1': 'value1', 'key2': 'value2'}
        client.put_object(Bucket=bucket_name, Key='foo123bar', Metadata=metadata, ContentType=content_type, Body=bytearray(size))

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='bar321foo')

        response = client.get_object(Bucket=bucket_name, Key='bar321foo')
        assert content_type == response['ContentType']
        assert metadata == response['Metadata']
        body = _get_body(response)
        assert size == response['ContentLength']

@pytest.mark.fails_on_dbstore
def test_object_copy_replacing_metadata():
    for size in [3, 1024 * 1024]:
        bucket_name = get_new_bucket()
        client = get_client()
        content_type = 'audio/ogg'

        metadata = {'key1': 'value1', 'key2': 'value2'}
        client.put_object(Bucket=bucket_name, Key='foo123bar', Metadata=metadata, ContentType=content_type, Body=bytearray(size))

        metadata = {'key3': 'value3', 'key2': 'value2'}
        content_type = 'audio/mpeg'

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='bar321foo', Metadata=metadata, MetadataDirective='REPLACE', ContentType=content_type)

        response = client.get_object(Bucket=bucket_name, Key='bar321foo')
        assert content_type == response['ContentType']
        assert metadata == response['Metadata']
        assert size == response['ContentLength']

def test_object_copy_bucket_not_found():
    bucket_name = get_new_bucket()
    client = get_client()

    copy_source = {'Bucket': bucket_name + "-fake", 'Key': 'foo123bar'}
    e = assert_raises(ClientError, client.copy, copy_source, bucket_name, 'bar321foo')
    status = _get_status(e.response)
    assert status == 404

def test_object_copy_key_not_found():
    bucket_name = get_new_bucket()
    client = get_client()

    copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
    e = assert_raises(ClientError, client.copy, copy_source, bucket_name, 'bar321foo')
    status = _get_status(e.response)
    assert status == 404

@pytest.mark.fails_on_dbstore
def test_object_copy_versioned_bucket():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    size = 1*5
    data = bytearray(size)
    data_str = data.decode()
    key1 = 'foo123bar'
    client.put_object(Bucket=bucket_name, Key=key1, Body=data)

    response = client.get_object(Bucket=bucket_name, Key=key1)
    version_id = response['VersionId']

    # copy object in the same bucket
    copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
    key2 = 'bar321foo'
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key2)
    response = client.get_object(Bucket=bucket_name, Key=key2)
    body = _get_body(response)
    assert data_str == body
    assert size == response['ContentLength']


    # second copy
    version_id2 = response['VersionId']
    copy_source = {'Bucket': bucket_name, 'Key': key2, 'VersionId': version_id2}
    key3 = 'bar321foo2'
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key3)
    response = client.get_object(Bucket=bucket_name, Key=key3)
    body = _get_body(response)
    assert data_str == body
    assert size == response['ContentLength']

    # copy to another versioned bucket
    bucket_name2 = get_new_bucket()
    check_configure_versioning_retry(bucket_name2, "Enabled", "Enabled")
    copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
    key4 = 'bar321foo3'
    client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key=key4)
    response = client.get_object(Bucket=bucket_name2, Key=key4)
    body = _get_body(response)
    assert data_str == body
    assert size == response['ContentLength']

    # copy to another non versioned bucket
    bucket_name3 = get_new_bucket()
    copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
    key5 = 'bar321foo4'
    client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key5)
    response = client.get_object(Bucket=bucket_name3, Key=key5)
    body = _get_body(response)
    assert data_str == body
    assert size == response['ContentLength']

    # copy from a non versioned bucket
    copy_source = {'Bucket': bucket_name3, 'Key': key5}
    key6 = 'foo123bar2'
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key6)
    response = client.get_object(Bucket=bucket_name, Key=key6)
    body = _get_body(response)
    assert data_str == body
    assert size == response['ContentLength']

@pytest.mark.fails_on_dbstore
def test_object_copy_versioned_url_encoding():
    bucket = get_new_bucket_resource()
    check_configure_versioning_retry(bucket.name, "Enabled", "Enabled")
    src_key = 'foo?bar'
    src = bucket.put_object(Key=src_key)
    src.load() # HEAD request tests that the key exists

    # copy object in the same bucket
    dst_key = 'bar&foo'
    dst = bucket.Object(dst_key)
    dst.copy_from(CopySource={'Bucket': src.bucket_name, 'Key': src.key, 'VersionId': src.version_id})
    dst.load() # HEAD request tests that the key exists

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        if this_part_size > len(s):
            s = s + strpart[0:this_part_size - len(s)]
        yield s
        if (x == size):
            return

def _multipart_upload(bucket_name, key, size, part_size=5*1024*1024, client=None, content_type=None, metadata=None, resend_parts=[], tagging=None):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    if client == None:
        client = get_client()


    if content_type == None and metadata == None:
      if tagging is not None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Tagging=tagging)
      else:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    else:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata, ContentType=content_type)

    upload_id = response['UploadId']
    s = ''
    parts = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i+1
        s += part
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})
        if i in resend_parts:
            client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)

    return (upload_id, s, parts)

def _multipart_upload_checksum(bucket_name, key, size, part_size=5*1024*1024, client=None, content_type=None, metadata=None, resend_parts=[]):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    if client == None:
        client = get_client()


    if content_type == None and metadata == None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, ChecksumAlgorithm='SHA256')
    else:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata, ContentType=content_type,
                                                  ChecksumAlgorithm='SHA256')

    upload_id = response['UploadId']
    s = ''
    parts = []
    part_checksums = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i+1
        s += part
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part,
                                      ChecksumAlgorithm='SHA256')

        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})

        armored_part_cksum = base64.b64encode(hashlib.sha256(part.encode('utf-8')).digest())
        part_checksums.append(armored_part_cksum.decode())

        if i in resend_parts:
            client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part,
                               ChecksumAlgorithm='SHA256')

    return (upload_id, s, parts, part_checksums)

@pytest.mark.fails_on_dbstore
def test_object_copy_versioning_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key1 = "srcmultipart"
    key1_metadata = {'foo': 'bar'}
    content_type = 'text/bla'
    objlen = 30 * 1024 * 1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key1, size=objlen, content_type=content_type, metadata=key1_metadata)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key1, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.get_object(Bucket=bucket_name, Key=key1)
    key1_size = response['ContentLength']
    version_id = response['VersionId']

    # copy object in the same bucket
    copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
    key2 = 'dstmultipart'
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key2)
    response = client.get_object(Bucket=bucket_name, Key=key2)
    version_id2 = response['VersionId']
    body = _get_body(response)
    assert data == body
    assert key1_size == response['ContentLength']
    assert key1_metadata == response['Metadata']
    assert content_type == response['ContentType']

    # second copy
    copy_source = {'Bucket': bucket_name, 'Key': key2, 'VersionId': version_id2}
    key3 = 'dstmultipart2'
    client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key3)
    response = client.get_object(Bucket=bucket_name, Key=key3)
    body = _get_body(response)
    assert data == body
    assert key1_size == response['ContentLength']
    assert key1_metadata == response['Metadata']
    assert content_type == response['ContentType']

    # copy to another versioned bucket
    bucket_name2 = get_new_bucket()
    check_configure_versioning_retry(bucket_name2, "Enabled", "Enabled")

    copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
    key4 = 'dstmultipart3'
    client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key=key4)
    response = client.get_object(Bucket=bucket_name2, Key=key4)
    body = _get_body(response)
    assert data == body
    assert key1_size == response['ContentLength']
    assert key1_metadata == response['Metadata']
    assert content_type == response['ContentType']

    # copy to another non versioned bucket
    bucket_name3 = get_new_bucket()
    copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
    key5 = 'dstmultipart4'
    client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key5)
    response = client.get_object(Bucket=bucket_name3, Key=key5)
    body = _get_body(response)
    assert data == body
    assert key1_size == response['ContentLength']
    assert key1_metadata == response['Metadata']
    assert content_type == response['ContentType']

    # copy from a non versioned bucket
    copy_source = {'Bucket': bucket_name3, 'Key': key5}
    key6 = 'dstmultipart5'
    client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key6)
    response = client.get_object(Bucket=bucket_name3, Key=key6)
    body = _get_body(response)
    assert data == body
    assert key1_size == response['ContentLength']
    assert key1_metadata == response['Metadata']
    assert content_type == response['ContentType']

def test_multipart_upload_empty():
    bucket_name = get_new_bucket()
    client = get_client()

    key1 = "mymultipart"
    objlen = 0
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key1, size=objlen)
    e = assert_raises(ClientError, client.complete_multipart_upload,Bucket=bucket_name, Key=key1, UploadId=upload_id)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'

@pytest.mark.fails_on_dbstore
def test_multipart_upload_small():
    bucket_name = get_new_bucket()
    client = get_client()

    key1 = "mymultipart"
    objlen = 1
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key1, size=objlen)
    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key1, UploadId=upload_id, MultipartUpload={'Parts': parts})
    response = client.get_object(Bucket=bucket_name, Key=key1)
    assert response['ContentLength'] == objlen
    # check extra client.complete_multipart_upload
    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key1, UploadId=upload_id, MultipartUpload={'Parts': parts})

def _create_key_with_random_content(keyname, size=7*1024*1024, bucket_name=None, client=None):
    if bucket_name is None:
        bucket_name = get_new_bucket()

    if client == None:
        client = get_client()

    data_str = str(next(generate_random(size, size)))
    data = bytes(data_str, 'utf-8')
    client.put_object(Bucket=bucket_name, Key=keyname, Body=data)

    return bucket_name

def _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size, client=None, part_size=5*1024*1024, version_id=None):

    if(client == None):
        client = get_client()

    response = client.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key)
    upload_id = response['UploadId']

    if(version_id == None):
        copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
    else:
        copy_source = {'Bucket': src_bucket_name, 'Key': src_key, 'VersionId': version_id}

    parts = []

    i = 0
    for start_offset in range(0, size, part_size):
        end_offset = min(start_offset + part_size - 1, size - 1)
        part_num = i+1
        copy_source_range = 'bytes={start}-{end}'.format(start=start_offset, end=end_offset)
        response = client.upload_part_copy(Bucket=dest_bucket_name, Key=dest_key, CopySource=copy_source, PartNumber=part_num, UploadId=upload_id, CopySourceRange=copy_source_range)
        parts.append({'ETag': response['CopyPartResult']['ETag'], 'PartNumber': part_num})
        i = i+1

    return (upload_id, parts)

def _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name, version_id=None):
    client = get_client()

    if(version_id == None):
        response = client.get_object(Bucket=src_bucket_name, Key=src_key)
    else:
        response = client.get_object(Bucket=src_bucket_name, Key=src_key, VersionId=version_id)
    src_size = response['ContentLength']

    response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
    dest_size = response['ContentLength']
    dest_data = _get_body(response)
    assert(src_size >= dest_size)

    r = 'bytes={s}-{e}'.format(s=0, e=dest_size-1)
    if(version_id == None):
        response = client.get_object(Bucket=src_bucket_name, Key=src_key, Range=r)
    else:
        response = client.get_object(Bucket=src_bucket_name, Key=src_key, Range=r, VersionId=version_id)
    src_data = _get_body(response)
    assert src_data == dest_data

@pytest.mark.fails_on_dbstore
def test_multipart_copy_small():
    src_key = 'foo'
    src_bucket_name = _create_key_with_random_content(src_key)

    dest_bucket_name = get_new_bucket()
    dest_key = "mymultipart"
    size = 1
    client = get_client()

    (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
    assert size == response['ContentLength']
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

def test_multipart_copy_invalid_range():
    client = get_client()
    src_key = 'source'
    src_bucket_name = _create_key_with_random_content(src_key, size=5)

    response = client.create_multipart_upload(Bucket=src_bucket_name, Key='dest')
    upload_id = response['UploadId']

    copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
    copy_source_range = 'bytes={start}-{end}'.format(start=0, end=21)

    e = assert_raises(ClientError, client.upload_part_copy,Bucket=src_bucket_name, Key='dest', UploadId=upload_id, CopySource=copy_source, CopySourceRange=copy_source_range, PartNumber=1)
    status, error_code = _get_status_and_error_code(e.response)
    valid_status = [400, 416]
    if not status in valid_status:
       raise AssertionError("Invalid response " + str(status))
    assert error_code == 'InvalidRange'


# TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40795 is resolved
@pytest.mark.fails_on_rgw
def test_multipart_copy_improper_range():
    client = get_client()
    src_key = 'source'
    src_bucket_name = _create_key_with_random_content(src_key, size=5)

    response = client.create_multipart_upload(
        Bucket=src_bucket_name, Key='dest')
    upload_id = response['UploadId']

    copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
    test_ranges = ['{start}-{end}'.format(start=0, end=2),
                   'bytes={start}'.format(start=0),
                   'bytes=hello-world',
                   'bytes=0-bar',
                   'bytes=hello-',
                   'bytes=0-2,3-5']

    for test_range in test_ranges:
        e = assert_raises(ClientError, client.upload_part_copy,
                          Bucket=src_bucket_name, Key='dest',
                          UploadId=upload_id,
                          CopySource=copy_source,
                          CopySourceRange=test_range,
                          PartNumber=1)
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 400
        assert error_code == 'InvalidArgument'


def test_multipart_copy_without_range():
    client = get_client()
    src_key = 'source'
    src_bucket_name = _create_key_with_random_content(src_key, size=10)
    dest_bucket_name = get_new_bucket_name()
    get_new_bucket(name=dest_bucket_name)
    dest_key = "mymultipartcopy"

    response = client.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key)
    upload_id = response['UploadId']
    parts = []

    copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
    part_num = 1
    copy_source_range = 'bytes={start}-{end}'.format(start=0, end=9)

    response = client.upload_part_copy(Bucket=dest_bucket_name, Key=dest_key, CopySource=copy_source, PartNumber=part_num, UploadId=upload_id)

    parts.append({'ETag': response['CopyPartResult']['ETag'], 'PartNumber': part_num})
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
    assert response['ContentLength'] == 10
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

@pytest.mark.fails_on_dbstore
def test_multipart_copy_special_names():
    src_bucket_name = get_new_bucket()

    dest_bucket_name = get_new_bucket()

    dest_key = "mymultipart"
    size = 1
    client = get_client()

    for src_key in (' ', '_', '__', '?versionId'):
        _create_key_with_random_content(src_key, bucket_name=src_bucket_name)
        (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        response = client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
        response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
        assert size == response['ContentLength']
        _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

def _check_content_using_range(key, bucket_name, data, step):
    client = get_client()
    response = client.get_object(Bucket=bucket_name, Key=key)
    size = response['ContentLength']

    for ofs in range(0, size, step):
        toread = size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        r = 'bytes={s}-{e}'.format(s=ofs, e=end)
        response = client.get_object(Bucket=bucket_name, Key=key, Range=r)
        assert response['ContentLength'] == toread
        body = _get_body(response)
        assert body == data[ofs:end+1]

@pytest.mark.fails_on_dbstore
def test_multipart_upload():
    bucket_name = get_new_bucket()
    key="mymultipart"
    content_type='text/bla'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    client = get_client()

    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, content_type=content_type, metadata=metadata)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    # check extra client.complete_multipart_upload
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response['Contents']) == 1
    assert response['Contents'][0]['Size'] == objlen

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response['ContentType'] == content_type
    assert response['Metadata'] == metadata
    body = _get_body(response)
    assert len(body) == response['ContentLength']
    assert body == data

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)

def check_versioning(bucket_name, status):
    client = get_client()

    try:
        response = client.get_bucket_versioning(Bucket=bucket_name)
        assert response['Status'] == status
    except KeyError:
        assert status == None

# amazon is eventually consistent, retry a bit if failed
def check_configure_versioning_retry(bucket_name, status, expected_string):
    client = get_client()
    client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': status})

    read_status = None

    for i in range(5):
        try:
            response = client.get_bucket_versioning(Bucket=bucket_name)
            read_status = response['Status']
        except KeyError:
            read_status = None

        if (expected_string == read_status):
            break

        time.sleep(1)

    assert expected_string == read_status

@pytest.mark.fails_on_dbstore
def test_multipart_copy_versioned():
    src_bucket_name = get_new_bucket()
    dest_bucket_name = get_new_bucket()

    dest_key = "mymultipart"
    check_versioning(src_bucket_name, None)

    src_key = 'foo'
    check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")

    size = 15 * 1024 * 1024
    _create_key_with_random_content(src_key, size=size, bucket_name=src_bucket_name)
    _create_key_with_random_content(src_key, size=size, bucket_name=src_bucket_name)
    _create_key_with_random_content(src_key, size=size, bucket_name=src_bucket_name)

    version_id = []
    client = get_client()
    response = client.list_object_versions(Bucket=src_bucket_name)
    for ver in response['Versions']:
        version_id.append(ver['VersionId'])

    for vid in version_id:
        (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size, version_id=vid)
        response = client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
        response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
        assert size == response['ContentLength']
        _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name, version_id=vid)

def _check_upload_multipart_resend(bucket_name, key, objlen, resend_parts):
    content_type = 'text/bla'
    metadata = {'foo': 'bar'}
    client = get_client()
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, content_type=content_type, metadata=metadata, resend_parts=resend_parts)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response['ContentType'] == content_type
    assert response['Metadata'] == metadata
    body = _get_body(response)
    assert len(body) == response['ContentLength']
    assert body == data

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)

@pytest.mark.fails_on_dbstore
def test_multipart_upload_resend_part():
    bucket_name = get_new_bucket()
    key="mymultipart"
    objlen = 30 * 1024 * 1024

    _check_upload_multipart_resend(bucket_name, key, objlen, [0])
    _check_upload_multipart_resend(bucket_name, key, objlen, [1])
    _check_upload_multipart_resend(bucket_name, key, objlen, [2])
    _check_upload_multipart_resend(bucket_name, key, objlen, [1,2])
    _check_upload_multipart_resend(bucket_name, key, objlen, [0,1,2,3,4,5])

def test_multipart_upload_multiple_sizes():
    bucket_name = get_new_bucket()
    key="mymultipart"
    client = get_client()

    objlen = 5*1024*1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    objlen = 5*1024*1024+100*1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    objlen = 5*1024*1024+600*1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    objlen = 10*1024*1024+100*1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    objlen = 10*1024*1024+600*1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    objlen = 10*1024*1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

@pytest.mark.fails_on_dbstore
def test_multipart_copy_multiple_sizes():
    src_key = 'foo'
    src_bucket_name = _create_key_with_random_content(src_key, 12*1024*1024)

    dest_bucket_name = get_new_bucket()
    dest_key="mymultipart"
    client = get_client()

    size = 5*1024*1024
    (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 5*1024*1024+100*1024
    (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 5*1024*1024+600*1024
    (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 10*1024*1024+100*1024
    (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 10*1024*1024+600*1024
    (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

    size = 10*1024*1024
    (upload_id, parts) = _multipart_copy(src_bucket_name, src_key, dest_bucket_name, dest_key, size)
    client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    _check_key_content(src_key, src_bucket_name, dest_key, dest_bucket_name)

def test_multipart_upload_size_too_small():
    bucket_name = get_new_bucket()
    key="mymultipart"
    client = get_client()

    size = 100*1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=size, part_size=10*1024)
    e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'EntityTooSmall'

def gen_rand_string(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def _do_test_multipart_upload_contents(bucket_name, key, num_parts):
    payload=gen_rand_string(5)*1024*1024
    client = get_client()

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response['UploadId']

    parts = []

    for part_num in range(0, num_parts):
        part = bytes(payload, 'utf-8')
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num+1, Body=part)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num+1})

    last_payload = '123'*1024*1024
    last_part = bytes(last_payload, 'utf-8')
    response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=num_parts+1, Body=last_part)
    parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': num_parts+1})

    res = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    assert res['ETag'] != ''

    response = client.get_object(Bucket=bucket_name, Key=key)
    test_string = _get_body(response)

    all_payload = payload*num_parts + last_payload

    assert test_string == all_payload

    return all_payload

@pytest.mark.fails_on_dbstore
def test_multipart_upload_contents():
    bucket_name = get_new_bucket()
    _do_test_multipart_upload_contents(bucket_name, 'mymultipart', 3)

def test_multipart_upload_overwrite_existing_object():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'mymultipart'
    payload='12345'*1024*1024
    num_parts=2
    client.put_object(Bucket=bucket_name, Key=key, Body=payload)


    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response['UploadId']

    parts = []

    for part_num in range(0, num_parts):
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num+1, Body=payload)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num+1})

    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.get_object(Bucket=bucket_name, Key=key)
    test_string = _get_body(response)

    assert test_string == payload*num_parts

def test_abort_multipart_upload():
    bucket_name = get_new_bucket()
    key="mymultipart"
    objlen = 10 * 1024 * 1024
    client = get_client()

    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen)
    client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert 'Contents' not in response

def test_abort_multipart_upload_not_found():
    bucket_name = get_new_bucket()
    client = get_client()
    key="mymultipart"
    client.put_object(Bucket=bucket_name, Key=key)

    e = assert_raises(ClientError, client.abort_multipart_upload, Bucket=bucket_name, Key=key, UploadId='56788')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchUpload'

@pytest.mark.fails_on_dbstore
def test_list_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    key="mymultipart"
    mb = 1024 * 1024

    upload_ids = []
    (upload_id1, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=5*mb)
    upload_ids.append(upload_id1)
    (upload_id2, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=6*mb)
    upload_ids.append(upload_id2)

    key2="mymultipart2"
    (upload_id3, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key2, size=5*mb)
    upload_ids.append(upload_id3)

    response = client.list_multipart_uploads(Bucket=bucket_name)
    uploads = response['Uploads']
    resp_uploadids = []

    for i in range(0, len(uploads)):
        resp_uploadids.append(uploads[i]['UploadId'])

    for i in range(0, len(upload_ids)):
        assert True == (upload_ids[i] in resp_uploadids)

    client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id1)
    client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id2)
    client.abort_multipart_upload(Bucket=bucket_name, Key=key2, UploadId=upload_id3)

@pytest.mark.fails_on_dbstore
def test_list_multipart_upload_owner():
    bucket_name = get_new_bucket()

    client1 = get_client()
    user1 = get_main_user_id()
    name1 = get_main_display_name()

    client2 = get_alt_client()
    user2  = get_alt_user_id()
    name2 = get_alt_display_name()

    # add bucket acl for public read/write access
    client1.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')

    key1 = 'multipart1'
    key2 = 'multipart2'
    upload1 = client1.create_multipart_upload(Bucket=bucket_name, Key=key1)['UploadId']
    try:
        upload2 = client2.create_multipart_upload(Bucket=bucket_name, Key=key2)['UploadId']
        try:
            # match fields of an Upload from ListMultipartUploadsResult
            def match(upload, key, uploadid, userid, username):
                assert upload['Key'] == key
                assert upload['UploadId'] == uploadid
                assert upload['Initiator']['ID'] == userid
                assert upload['Initiator']['DisplayName'] == username
                assert upload['Owner']['ID'] == userid
                assert upload['Owner']['DisplayName'] == username

            # list uploads with client1
            uploads1 = client1.list_multipart_uploads(Bucket=bucket_name)['Uploads']
            assert len(uploads1) == 2
            match(uploads1[0], key1, upload1, user1, name1)
            match(uploads1[1], key2, upload2, user2, name2)

            # list uploads with client2
            uploads2 = client2.list_multipart_uploads(Bucket=bucket_name)['Uploads']
            assert len(uploads2) == 2
            match(uploads2[0], key1, upload1, user1, name1)
            match(uploads2[1], key2, upload2, user2, name2)
        finally:
            client2.abort_multipart_upload(Bucket=bucket_name, Key=key2, UploadId=upload2)
    finally:
        client1.abort_multipart_upload(Bucket=bucket_name, Key=key1, UploadId=upload1)

def test_multipart_upload_missing_part():
    bucket_name = get_new_bucket()
    client = get_client()
    key="mymultipart"
    size = 1

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response['UploadId']

    parts = []
    response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=1, Body=bytes('\x00', 'utf-8'))
    # 'PartNumber should be 1'
    parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': 9999})

    e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidPart'

def test_multipart_upload_incorrect_etag():
    bucket_name = get_new_bucket()
    client = get_client()
    key="mymultipart"
    size = 1

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id = response['UploadId']

    parts = []
    response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=1, Body=bytes('\x00', 'utf-8'))
    # 'ETag' should be "93b885adfe0da089cdf634904fd59f71"
    parts.append({'ETag': "ffffffffffffffffffffffffffffffff", 'PartNumber': 1})

    e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidPart'

@pytest.mark.fails_on_dbstore
def test_multipart_get_part():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"

    part_size = 5*1024*1024
    part_sizes = 3 * [part_size] + [1*1024*1024]
    part_count = len(part_sizes)
    total_size = sum(part_sizes)

    (upload_id, data, parts) = _multipart_upload(bucket_name, key, total_size, part_size, resend_parts=[2])

    # request part before complete
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=1)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    assert len(parts) == part_count

    for part, size in zip(parts, part_sizes):
        response = client.head_object(Bucket=bucket_name, Key=key, PartNumber=part['PartNumber'])
        assert response['PartsCount'] == part_count
        assert response['ETag'] == '"{}"'.format(part['ETag'])

        response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=part['PartNumber'])
        assert response['PartsCount'] == part_count
        assert response['ETag'] == '"{}"'.format(part['ETag'])
        assert response['ContentLength'] == size
        # compare contents
        for chunk in response['Body'].iter_chunks():
            assert chunk.decode() == data[0:len(chunk)]
            data = data[len(chunk):]

    # request PartNumber out of range
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=5)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidPart'

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_multipart_sse_c_get_part():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"

    part_size = 5*1024*1024
    part_sizes = 3 * [part_size] + [1*1024*1024]
    part_count = len(part_sizes)
    total_size = sum(part_sizes)

    enc_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
    }
    get_args = {
        'SSECustomerAlgorithm': 'AES256',
        'SSECustomerKey': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'SSECustomerKeyMD5': 'DWygnHRtgiJ77HCm+1rvHw==',
    }

    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, total_size,
            part_size, init_headers=enc_headers, part_headers=enc_headers, metadata=None, resend_parts=[2])

    # request part before complete
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=1)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    assert len(parts) == part_count

    for part, size in zip(parts, part_sizes):
        response = client.head_object(Bucket=bucket_name, Key=key, PartNumber=part['PartNumber'], **get_args)
        assert response['PartsCount'] == part_count
        assert response['ETag'] == '"{}"'.format(part['ETag'])

        response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=part['PartNumber'], **get_args)
        assert response['PartsCount'] == part_count
        assert response['ETag'] == '"{}"'.format(part['ETag'])
        assert response['ContentLength'] == size
        # compare contents
        for chunk in response['Body'].iter_chunks():
            assert chunk.decode() == data[0:len(chunk)]
            data = data[len(chunk):]

    # request PartNumber out of range
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=5)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidPart'

@pytest.mark.fails_on_dbstore
def test_multipart_single_get_part():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "mymultipart"

    part_size = 5*1024*1024
    part_sizes = [part_size] # just one part
    part_count = len(part_sizes)
    total_size = sum(part_sizes)

    (upload_id, data, parts) = _multipart_upload(bucket_name, key, total_size, part_size)

    # request part before complete
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=1)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    assert len(parts) == part_count

    for part, size in zip(parts, part_sizes):
        response = client.head_object(Bucket=bucket_name, Key=key, PartNumber=part['PartNumber'])
        assert response['PartsCount'] == part_count
        assert response['ETag'] == '"{}"'.format(part['ETag'])

        response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=part['PartNumber'])
        assert response['PartsCount'] == part_count
        assert response['ETag'] == '"{}"'.format(part['ETag'])
        assert response['ContentLength'] == size
        # compare contents
        for chunk in response['Body'].iter_chunks():
            assert chunk.decode() == data[0:len(chunk)]
            data = data[len(chunk):]

    # request PartNumber out of range
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=5)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidPart'

@pytest.mark.fails_on_dbstore
def test_non_multipart_get_part():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "singlepart"

    response = client.put_object(Bucket=bucket_name, Key=key, Body='body')
    etag = response['ETag']

    # request for PartNumber > 1 results in InvalidPart
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=2)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidPart'

    # request for PartNumber = 1 gives back the entire object
    response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=1)
    assert response['ETag'] == etag
    assert _get_body(response) == 'body'

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_non_multipart_sse_c_get_part():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "singlepart"

    sse_args = {
        'SSECustomerAlgorithm': 'AES256',
        'SSECustomerKey': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'SSECustomerKeyMD5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    response = client.put_object(Bucket=bucket_name, Key=key, Body='body', **sse_args)
    etag = response['ETag']

    # request for PartNumber > 1 results in InvalidPart
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key, PartNumber=2, **sse_args)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidPart'

    # request for PartNumber = 1 gives back the entire object
    response = client.get_object(Bucket=bucket_name, Key=key, PartNumber=1, **sse_args)
    assert response['ETag'] == etag
    assert _get_body(response) == 'body'


def _simple_http_req_100_cont(host, port, is_secure, method, resource):
    """
    Send the specified request w/expect 100-continue
    and await confirmation.
    """
    req_str = '{method} {resource} HTTP/1.1\r\nHost: {host}\r\nAccept-Encoding: identity\r\nContent-Length: 123\r\nExpect: 100-continue\r\n\r\n'.format(
            method=method,
            resource=resource,
            host=host,
            )

    req = bytes(req_str, 'utf-8')

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if is_secure:
        s = ssl.wrap_socket(s);
    s.settimeout(5)
    s.connect((host, port))
    s.send(req)

    try:
        data = s.recv(1024)
    except socket.error as msg:
        print('got response: ', msg)
        print('most likely server doesn\'t support 100-continue')

    s.close()
    data_str = data.decode()
    l = data_str.split(' ')

    assert l[0].startswith('HTTP')

    return l[1]

def test_100_continue():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    objname='testobj'
    resource = '/{bucket}/{obj}'.format(bucket=bucket_name, obj=objname)

    host = get_config_host()
    port = get_config_port()
    is_secure = get_config_is_secure()

    #NOTES: this test needs to be tested when is_secure is True
    status = _simple_http_req_100_cont(host, port, is_secure, 'PUT', resource)
    assert status == '403'

    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')

    status = _simple_http_req_100_cont(host, port, is_secure, 'PUT', resource)
    assert status == '100'

def test_set_cors():
    bucket_name = get_new_bucket()
    client = get_client()
    allowed_methods = ['GET', 'PUT']
    allowed_origins = ['*.get', '*.put']

    cors_config ={
        'CORSRules': [
            {'AllowedMethods': allowed_methods,
             'AllowedOrigins': allowed_origins,
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)
    response = client.get_bucket_cors(Bucket=bucket_name)
    assert response['CORSRules'][0]['AllowedMethods'] == allowed_methods
    assert response['CORSRules'][0]['AllowedOrigins'] == allowed_origins

    client.delete_bucket_cors(Bucket=bucket_name)
    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

def _cors_request_and_check(func, url, headers, expect_status, expect_allow_origin, expect_allow_methods):
    r = func(url, headers=headers, verify=get_config_ssl_verify())
    assert r.status_code == expect_status

    assert r.headers.get('access-control-allow-origin', None) == expect_allow_origin
    assert r.headers.get('access-control-allow-methods', None) == expect_allow_methods

def test_cors_origin_response():
    bucket_name = _setup_bucket_acl(bucket_acl='public-read')
    client = get_client()

    cors_config ={
        'CORSRules': [
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['*suffix'],
            },
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['start*end'],
            },
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['prefix*'],
            },
            {'AllowedMethods': ['PUT'],
             'AllowedOrigins': ['*.put'],
            }
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    _cors_request_and_check(requests.get, url, None, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.suffix'}, 200, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.bar'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'foo.suffix.get'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'startend'}, 200, 'startend', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'start1end'}, 200, 'start1end', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'start12end'}, 200, 'start12end', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': '0start12end'}, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'prefix'}, 200, 'prefix', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'prefix.suffix'}, 200, 'prefix.suffix', 'GET')
    _cors_request_and_check(requests.get, url, {'Origin': 'bla.prefix'}, 200, None, None)

    obj_url = '{u}/{o}'.format(u=url, o='bar')
    _cors_request_and_check(requests.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                                    'content-length': '0'}, 403, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'PUT',
                                                    'content-length': '0'}, 403, None, None)

    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'DELETE',
                                                    'content-length': '0'}, 403, None, None)
    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.suffix', 'content-length': '0'}, 403, None, None)

    _cors_request_and_check(requests.put, obj_url, {'Origin': 'foo.put', 'content-length': '0'}, 403, 'foo.put', 'PUT')

    _cors_request_and_check(requests.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')

    _cors_request_and_check(requests.options, url, None, 400, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.suffix'}, 400, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'bla'}, 400, None, None)
    _cors_request_and_check(requests.options, obj_url, {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                                    'content-length': '0'}, 200, 'foo.suffix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.bar', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.suffix.get', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'startend', 'Access-Control-Request-Method': 'GET'}, 200, 'startend', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'start1end', 'Access-Control-Request-Method': 'GET'}, 200, 'start1end', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'start12end', 'Access-Control-Request-Method': 'GET'}, 200, 'start12end', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': '0start12end', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'prefix', 'Access-Control-Request-Method': 'GET'}, 200, 'prefix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'prefix.suffix', 'Access-Control-Request-Method': 'GET'}, 200, 'prefix.suffix', 'GET')
    _cors_request_and_check(requests.options, url, {'Origin': 'bla.prefix', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.put', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
    _cors_request_and_check(requests.options, url, {'Origin': 'foo.put', 'Access-Control-Request-Method': 'PUT'}, 200, 'foo.put', 'PUT')

def test_cors_origin_wildcard():
    bucket_name = _setup_bucket_acl(bucket_acl='public-read')
    client = get_client()

    cors_config ={
        'CORSRules': [
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['*'],
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)

    _cors_request_and_check(requests.get, url, None, 200, None, None)
    _cors_request_and_check(requests.get, url, {'Origin': 'example.origin'}, 200, '*', 'GET')

def test_cors_header_option():
    bucket_name = _setup_bucket_acl(bucket_acl='public-read')
    client = get_client()

    cors_config ={
        'CORSRules': [
            {'AllowedMethods': ['GET'],
             'AllowedOrigins': ['*'],
             'ExposeHeaders': ['x-amz-meta-header1'],
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
    status = _get_status(e.response)
    assert status == 404

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    time.sleep(3)

    url = _get_post_url(bucket_name)
    obj_url = '{u}/{o}'.format(u=url, o='bar')

    _cors_request_and_check(requests.options, obj_url, {'Origin': 'example.origin','Access-Control-Request-Headers':'x-amz-meta-header2','Access-Control-Request-Method':'GET'}, 403, None, None)

def _test_cors_options_presigned_method(client, method, cannedACL=None):
    bucket_name = _setup_bucket_object_acl('public-read', 'public-read', client=client)
    params = {'Bucket': bucket_name, 'Key': 'foo'}

    if cannedACL is not None:
        params['ACL'] = cannedACL

    if method == 'get_object':
        httpMethod = 'GET'
    elif method == 'put_object':
        httpMethod = 'PUT'
    else:
        raise ValueError('invalid method')

    url = client.generate_presigned_url(ClientMethod=method, Params=params, ExpiresIn=100000, HttpMethod=httpMethod)

    res = requests.options(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 400

    allowed_methods = [httpMethod]
    allowed_origins = ['example']

    cors_config ={
        'CORSRules': [
            {'AllowedMethods': allowed_methods,
             'AllowedOrigins': allowed_origins,
            },
        ]
    }

    client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

    headers = {
        'Origin': 'example',
        'Access-Control-Request-Method': httpMethod,
    }
    _cors_request_and_check(requests.options, url, headers,
                            200, 'example', httpMethod)

def test_cors_presigned_get_object():
    _test_cors_options_presigned_method(
        client=get_client(),
        method='get_object',
    )

def test_cors_presigned_get_object_tenant():
    _test_cors_options_presigned_method(
        client=get_tenant_client(),
        method='get_object',
    )

@pytest.mark.fails_on_rgw
def test_cors_presigned_get_object_v2():
    _test_cors_options_presigned_method(
        client=get_v2_client(),
        method='get_object',
    )

@pytest.mark.fails_on_rgw
def test_cors_presigned_get_object_tenant_v2():
    _test_cors_options_presigned_method(
        client=get_v2_tenant_client(),
        method='get_object',
    )

def test_cors_presigned_put_object():
    _test_cors_options_presigned_method(
        client=get_client(),
        method='put_object',
    )

def test_cors_presigned_put_object_with_acl():
    _test_cors_options_presigned_method(
        client=get_client(),
        method='put_object',
        cannedACL='private',
    )

@pytest.mark.fails_on_rgw
def test_cors_presigned_put_object_v2():
    _test_cors_options_presigned_method(
        client=get_v2_client(),
        method='put_object',
    )

@pytest.mark.fails_on_rgw
def test_cors_presigned_put_object_tenant_v2():
    _test_cors_options_presigned_method(
        client=get_v2_tenant_client(),
        method='put_object',
    )

def test_cors_presigned_put_object_tenant():
    _test_cors_options_presigned_method(
        client=get_tenant_client(),
        method='put_object',
    )

def test_cors_presigned_put_object_tenant_with_acl():
    _test_cors_options_presigned_method(
        client=get_tenant_client(),
        method='put_object',
        cannedACL='private',
    )

@pytest.mark.tagging
def test_set_bucket_tagging():
    bucket_name = get_new_bucket()
    client = get_client()

    tags={
        'TagSet': [
            {
                'Key': 'Hello',
                'Value': 'World'
            },
        ]
    }

    e = assert_raises(ClientError, client.get_bucket_tagging, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchTagSet'

    client.put_bucket_tagging(Bucket=bucket_name, Tagging=tags)

    response = client.get_bucket_tagging(Bucket=bucket_name)
    assert len(response['TagSet']) == 1
    assert response['TagSet'][0]['Key'] == 'Hello'
    assert response['TagSet'][0]['Value'] == 'World'

    response = client.delete_bucket_tagging(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    e = assert_raises(ClientError, client.get_bucket_tagging, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchTagSet'


class FakeFile(object):
    """
    file that simulates seek, tell, and current character
    """
    def __init__(self, char='A', interrupt=None):
        self.offset = 0
        self.char = bytes(char, 'utf-8')
        self.interrupt = interrupt

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset;
        elif whence == os.SEEK_CUR:
            self.offset += offset

    def tell(self):
        return self.offset

class FakeWriteFile(FakeFile):
    """
    file that simulates interruptable reads of constant data
    """
    def __init__(self, size, char='A', interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.size = size

    def read(self, size=-1):
        if size < 0:
            size = self.size - self.offset
        count = min(size, self.size - self.offset)
        self.offset += count

        # Sneaky! do stuff before we return (the last time)
        if self.interrupt != None and self.offset == self.size and count > 0:
            self.interrupt()

        return self.char*count

class FakeReadFile(FakeFile):
    """
    file that simulates writes, interrupting after the second
    """
    def __init__(self, size, char='A', interrupt=None):
        FakeFile.__init__(self, char, interrupt)
        self.interrupted = False
        self.size = 0
        self.expected_size = size

    def write(self, chars):
        assert chars == self.char*len(chars)
        self.offset += len(chars)
        self.size += len(chars)

        # Sneaky! do stuff on the second seek
        if not self.interrupted and self.interrupt != None \
                and self.offset > 0:
            self.interrupt()
            self.interrupted = True

    def close(self):
        assert self.size == self.expected_size

class FakeFileVerifier(object):
    """
    file that verifies expected data has been written
    """
    def __init__(self, char=None):
        self.char = char
        self.size = 0

    def write(self, data):
        size = len(data)
        if self.char == None:
            self.char = data[0]
        self.size += size
        assert data.decode() == self.char*size

def _verify_atomic_key_data(bucket_name, key, size=-1, char=None):
    """
    Make sure file is of the expected size and (simulated) content
    """
    fp_verify = FakeFileVerifier(char)
    client = get_client()
    client.download_fileobj(bucket_name, key, fp_verify)
    if size >= 0:
        assert fp_verify.size == size

def _test_atomic_read(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()


    fp_a = FakeWriteFile(file_size, 'A')
    client.put_object(Bucket=bucket_name, Key='testobj', Body=fp_a)

    fp_b = FakeWriteFile(file_size, 'B')
    fp_a2 = FakeReadFile(file_size, 'A',
        lambda: client.put_object(Bucket=bucket_name, Key='testobj', Body=fp_b)
        )

    read_client = get_client()

    read_client.download_fileobj(bucket_name, 'testobj', fp_a2)
    fp_a2.close()

    _verify_atomic_key_data(bucket_name, 'testobj', file_size, 'B')

def test_atomic_read_1mb():
    _test_atomic_read(1024*1024)

def test_atomic_read_4mb():
    _test_atomic_read(1024*1024*4)

def test_atomic_read_8mb():
    _test_atomic_read(1024*1024*8)

def _test_atomic_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    objname = 'testobj'


    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, 'A')
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)


    # verify A's
    _verify_atomic_key_data(bucket_name, objname, file_size, 'A')

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    fp_b = FakeWriteFile(file_size, 'B',
        lambda: _verify_atomic_key_data(bucket_name, objname, file_size, 'A')
        )

    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    # verify B's
    _verify_atomic_key_data(bucket_name, objname, file_size, 'B')

def test_atomic_write_1mb():
    _test_atomic_write(1024*1024)

def test_atomic_write_4mb():
    _test_atomic_write(1024*1024*4)

def test_atomic_write_8mb():
    _test_atomic_write(1024*1024*8)

def _test_atomic_dual_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket_name = get_new_bucket()
    objname = 'testobj'
    client = get_client()
    client.put_object(Bucket=bucket_name, Key=objname)

    # write <file_size> file of B's
    # but before we're done, try to write all A's
    fp_a = FakeWriteFile(file_size, 'A')

    def rewind_put_fp_a():
        fp_a.seek(0)
        client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)

    fp_b = FakeWriteFile(file_size, 'B', rewind_put_fp_a)
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    # verify the file
    _verify_atomic_key_data(bucket_name, objname, file_size, 'B')

def test_atomic_dual_write_1mb():
    _test_atomic_dual_write(1024*1024)

def test_atomic_dual_write_4mb():
    _test_atomic_dual_write(1024*1024*4)

def test_atomic_dual_write_8mb():
    _test_atomic_dual_write(1024*1024*8)

def _test_atomic_conditional_write(file_size):
    """
    Create a file of A's, use it to set_contents_from_file.
    Verify the contents are all A's.
    Create a file of B's, use it to re-set_contents_from_file.
    Before re-set continues, verify content's still A's
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    objname = 'testobj'
    client = get_client()

    # create <file_size> file of A's
    fp_a = FakeWriteFile(file_size, 'A')
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)

    fp_b = FakeWriteFile(file_size, 'B',
        lambda: _verify_atomic_key_data(bucket_name, objname, file_size, 'A')
        )

    # create <file_size> file of B's
    # but try to verify the file before we finish writing all the B's
    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    # verify B's
    _verify_atomic_key_data(bucket_name, objname, file_size, 'B')

@pytest.mark.fails_on_aws
def test_atomic_conditional_write_1mb():
    _test_atomic_conditional_write(1024*1024)

def _test_atomic_dual_conditional_write(file_size):
    """
    create an object, two sessions writing different contents
    confirm that it is all one or the other
    """
    bucket_name = get_new_bucket()
    objname = 'testobj'
    client = get_client()

    fp_a = FakeWriteFile(file_size, 'A')
    response = client.put_object(Bucket=bucket_name, Key=objname, Body=fp_a)
    _verify_atomic_key_data(bucket_name, objname, file_size, 'A')
    etag_fp_a = response['ETag'].replace('"', '')

    # write <file_size> file of C's
    # but before we're done, try to write all B's
    fp_b = FakeWriteFile(file_size, 'B')
    lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': etag_fp_a}))
    client.meta.events.register('before-call.s3.PutObject', lf)
    def rewind_put_fp_b():
        fp_b.seek(0)
        client.put_object(Bucket=bucket_name, Key=objname, Body=fp_b)

    fp_c = FakeWriteFile(file_size, 'C', rewind_put_fp_b)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=objname, Body=fp_c)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

    # verify the file
    _verify_atomic_key_data(bucket_name, objname, file_size, 'B')

@pytest.mark.fails_on_aws
# TODO: test not passing with SSL, fix this
@pytest.mark.fails_on_rgw
def test_atomic_dual_conditional_write_1mb():
    _test_atomic_dual_conditional_write(1024*1024)

@pytest.mark.fails_on_aws
# TODO: test not passing with SSL, fix this
@pytest.mark.fails_on_rgw
def test_atomic_write_bucket_gone():
    bucket_name = get_new_bucket()
    client = get_client()

    def remove_bucket():
        client.delete_bucket(Bucket=bucket_name)

    objname = 'foo'
    fp_a = FakeWriteFile(1024*1024, 'A', remove_bucket)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=objname, Body=fp_a)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchBucket'

def test_atomic_multipart_upload_write():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    response = client.create_multipart_upload(Bucket=bucket_name, Key='foo')
    upload_id = response['UploadId']

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

    client.abort_multipart_upload(Bucket=bucket_name, Key='foo', UploadId=upload_id)

    response = client.get_object(Bucket=bucket_name, Key='foo')
    body = _get_body(response)
    assert body == 'bar'

class Counter:
    def __init__(self, default_val):
        self.val = default_val

    def inc(self):
        self.val = self.val + 1

class ActionOnCount:
    def __init__(self, trigger_count, action):
        self.count = 0
        self.trigger_count = trigger_count
        self.action = action
        self.result = 0

    def trigger(self):
        self.count = self.count + 1

        if self.count == self.trigger_count:
            self.result = self.action()

def test_multipart_resend_first_finishes_last():
    bucket_name = get_new_bucket()
    client = get_client()
    key_name = "mymultipart"

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
    upload_id = response['UploadId']

    #file_size = 8*1024*1024
    file_size = 8

    counter = Counter(0)
    # upload_part might read multiple times from the object
    # first time when it calculates md5, second time when it writes data
    # out. We want to interject only on the last time, but we can't be
    # sure how many times it's going to read, so let's have a test run
    # and count the number of reads

    fp_dry_run = FakeWriteFile(file_size, 'C',
        lambda: counter.inc()
        )

    parts = []

    response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key_name, PartNumber=1, Body=fp_dry_run)

    parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': 1})
    client.complete_multipart_upload(Bucket=bucket_name, Key=key_name, UploadId=upload_id, MultipartUpload={'Parts': parts})

    client.delete_object(Bucket=bucket_name, Key=key_name)

    # clear parts
    parts[:] = []

    # ok, now for the actual test
    fp_b = FakeWriteFile(file_size, 'B')
    def upload_fp_b():
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key_name, Body=fp_b, PartNumber=1)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': 1})

    action = ActionOnCount(counter.val, lambda: upload_fp_b())

    response = client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
    upload_id = response['UploadId']

    fp_a = FakeWriteFile(file_size, 'A',
        lambda: action.trigger()
        )

    response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key_name, PartNumber=1, Body=fp_a)

    parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': 1})
    client.complete_multipart_upload(Bucket=bucket_name, Key=key_name, UploadId=upload_id, MultipartUpload={'Parts': parts})

    _verify_atomic_key_data(bucket_name, key_name, file_size, 'A')

@pytest.mark.fails_on_dbstore
def test_ranged_request_response_code():
    content = 'testcontent'

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
    response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=4-7')

    fetched_content = _get_body(response)
    assert fetched_content == content[4:8]
    assert response['ResponseMetadata']['HTTPHeaders']['content-range'] == 'bytes 4-7/11'
    assert response['ResponseMetadata']['HTTPStatusCode'] == 206

def _generate_random_string(size):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(size))

@pytest.mark.fails_on_dbstore
def test_ranged_big_request_response_code():
    content = _generate_random_string(8*1024*1024)

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
    response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=3145728-5242880')

    fetched_content = _get_body(response)
    assert fetched_content == content[3145728:5242881]
    assert response['ResponseMetadata']['HTTPHeaders']['content-range'] == 'bytes 3145728-5242880/8388608'
    assert response['ResponseMetadata']['HTTPStatusCode'] == 206

@pytest.mark.fails_on_dbstore
def test_ranged_request_skip_leading_bytes_response_code():
    content = 'testcontent'

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
    response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=4-')

    fetched_content = _get_body(response)
    assert fetched_content == content[4:]
    assert response['ResponseMetadata']['HTTPHeaders']['content-range'] == 'bytes 4-10/11'
    assert response['ResponseMetadata']['HTTPStatusCode'] == 206

@pytest.mark.fails_on_dbstore
def test_ranged_request_return_trailing_bytes_response_code():
    content = 'testcontent'

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
    response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=-7')

    fetched_content = _get_body(response)
    assert fetched_content == content[-7:]
    assert response['ResponseMetadata']['HTTPHeaders']['content-range'] == 'bytes 4-10/11'
    assert response['ResponseMetadata']['HTTPStatusCode'] == 206

def test_ranged_request_invalid_range():
    content = 'testcontent'

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='testobj', Body=content)

    # test invalid range
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='testobj', Range='bytes=40-50')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 416
    assert error_code == 'InvalidRange'

def test_ranged_request_empty_object():
    content = ''

    bucket_name = get_new_bucket()
    client = get_client()

    client.put_object(Bucket=bucket_name, Key='testobj', Body=content)

    # test invalid range
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='testobj', Range='bytes=40-50')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 416
    assert error_code == 'InvalidRange'

def test_versioning_bucket_create_suspend():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

def check_obj_content(client, bucket_name, key, version_id, content):
    response = client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
    if content is not None:
        body = _get_body(response)
        assert body == content
    else:
        assert response['DeleteMarker'] == True

def check_obj_versions(client, bucket_name, key, version_ids, contents):
    # check to see if objects is pointing at correct version

    response = client.list_object_versions(Bucket=bucket_name)
    versions = []
    versions = response['Versions']
    # obj versions in versions come out created last to first not first to last like version_ids & contents
    versions.reverse()
    i = 0

    for version in versions:
        assert version['VersionId'] == version_ids[i]
        assert version['Key'] == key
        check_obj_content(client, bucket_name, key, version['VersionId'], contents[i])
        i += 1

def create_multiple_versions(client, bucket_name, key, num_versions, version_ids = None, contents = None, check_versions = True):
    contents = contents or []
    version_ids = version_ids or []

    for i in range(num_versions):
        body = 'content-{i}'.format(i=i)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        version_id = response['VersionId']

        contents.append(body)
        version_ids.append(version_id)

#    if check_versions:
#        check_obj_versions(client, bucket_name, key, version_ids, contents)

    return (version_ids, contents)

def remove_obj_version(client, bucket_name, key, version_ids, contents, index):
    assert len(version_ids) == len(contents)
    index = index % len(version_ids)
    rm_version_id = version_ids.pop(index)
    rm_content = contents.pop(index)

    check_obj_content(client, bucket_name, key, rm_version_id, rm_content)

    client.delete_object(Bucket=bucket_name, Key=key, VersionId=rm_version_id)

    if len(version_ids) != 0:
        check_obj_versions(client, bucket_name, key, version_ids, contents)

def clean_up_bucket(client, bucket_name, key, version_ids):
    for version_id in version_ids:
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)

    client.delete_bucket(Bucket=bucket_name)

def _do_test_create_remove_versions(client, bucket_name, key, num_versions, remove_start_idx, idx_inc):
    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)

    idx = remove_start_idx

    for j in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)
        idx += idx_inc

    response = client.list_object_versions(Bucket=bucket_name)
    if 'Versions' in response:
        print(response['Versions'])


def test_versioning_obj_create_read_remove():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'MFADelete': 'Disabled', 'Status': 'Enabled'})
    key = 'testobj'
    num_versions = 5

    _do_test_create_remove_versions(client, bucket_name, key, num_versions, -1, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, -1, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 0, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 1, 0)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 4, -1)
    _do_test_create_remove_versions(client, bucket_name, key, num_versions, 3, 3)

def test_versioning_obj_create_read_remove_head():
    bucket_name = get_new_bucket()

    client = get_client()
    client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'MFADelete': 'Disabled', 'Status': 'Enabled'})
    key = 'testobj'
    num_versions = 5

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)

    # removes old head object, checks new one
    removed_version_id = version_ids.pop()
    contents.pop()
    num_versions = num_versions-1

    response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=removed_version_id)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == contents[-1]

    # add a delete marker
    response = client.delete_object(Bucket=bucket_name, Key=key)
    assert response['DeleteMarker'] == True

    delete_marker_version_id = response['VersionId']
    version_ids.append(delete_marker_version_id)

    response = client.list_object_versions(Bucket=bucket_name)
    assert len(response['Versions']) == num_versions
    assert len(response['DeleteMarkers']) == 1
    assert response['DeleteMarkers'][0]['VersionId'] == delete_marker_version_id

    clean_up_bucket(client, bucket_name, key, version_ids)

@pytest.mark.fails_on_dbstore
def test_versioning_stack_delete_merkers():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 1)
    client.delete_object(Bucket=bucket_name, Key="test1/a")
    client.delete_object(Bucket=bucket_name, Key="test1/a")
    client.delete_object(Bucket=bucket_name, Key="test1/a")

    response  = client.list_object_versions(Bucket=bucket_name)
    versions = response['Versions']
    delete_markers = response['DeleteMarkers']
    assert len(versions) == 1
    assert len(delete_markers) == 3

def test_versioning_obj_plain_null_version_removal():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    client = get_client()
    key = 'testobjfoo'
    content = 'fooz'
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    client.delete_object(Bucket=bucket_name, Key=key, VersionId='null')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response

def test_versioning_obj_plain_null_version_overwrite():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    client = get_client()
    key = 'testobjfoo'
    content = 'fooz'
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    content2 = 'zzz'
    response = client.put_object(Bucket=bucket_name, Key=key, Body=content2)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == content2

    version_id = response['VersionId']
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == content

    client.delete_object(Bucket=bucket_name, Key=key, VersionId='null')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response

def test_versioning_obj_plain_null_version_overwrite_suspended():
    bucket_name = get_new_bucket()
    check_versioning(bucket_name, None)

    client = get_client()
    key = 'testobjbar'
    content = 'foooz'
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

    content2 = 'zzz'
    response = client.put_object(Bucket=bucket_name, Key=key, Body=content2)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == content2

    response = client.list_object_versions(Bucket=bucket_name)
    # original object with 'null' version id still counts as a version
    assert len(response['Versions']) == 1

    client.delete_object(Bucket=bucket_name, Key=key, VersionId='null')

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchKey'

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response

def delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents):
    client.delete_object(Bucket=bucket_name, Key=key)

    # clear out old null objects in lists since they will get overwritten
    assert len(version_ids) == len(contents)
    i = 0
    for version_id in version_ids:
        if version_id == 'null':
            version_ids.pop(i)
            contents.pop(i)
        i += 1

    return (version_ids, contents)

def overwrite_suspended_versioning_obj(client, bucket_name, key, version_ids, contents, content):
    client.put_object(Bucket=bucket_name, Key=key, Body=content)

    # clear out old null objects in lists since they will get overwritten
    assert len(version_ids) == len(contents)
    i = 0
    for version_id in version_ids:
        if version_id == 'null':
            version_ids.pop(i)
            contents.pop(i)
        i += 1

    # add new content with 'null' version id to the end
    contents.append(content)
    version_ids.append('null')

    return (version_ids, contents)


def test_versioning_obj_suspend_versions():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'testobj'
    num_versions = 5

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)

    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)

    overwrite_suspended_versioning_obj(client, bucket_name, key, version_ids, contents, 'null content 1')
    overwrite_suspended_versioning_obj(client, bucket_name, key, version_ids, contents, 'null content 2')
    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
    overwrite_suspended_versioning_obj(client, bucket_name, key, version_ids, contents, 'null content 3')
    delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, 3, version_ids, contents)
    num_versions += 3

    for idx in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

    assert len(version_ids) == 0
    assert len(version_ids) == len(contents)

@pytest.mark.fails_on_dbstore
def test_versioning_obj_suspended_copy():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key1 = 'testobj1'
    num_versions = 1
    (version_ids, contents) = create_multiple_versions(client, bucket_name, key1, num_versions)

    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

    content = 'null content'
    overwrite_suspended_versioning_obj(client, bucket_name, key1, version_ids, contents, content)

    # copy to another object
    key2 = 'testobj2'
    copy_source = {'Bucket': bucket_name, 'Key': key1}
    client.copy_object(Bucket=bucket_name, Key=key2, CopySource=copy_source)

    # copy to another non-versioned bucket
    bucket_name2 = get_new_bucket()
    copy_source = {'Bucket': bucket_name, 'Key': key1}
    client.copy_object(Bucket=bucket_name2, Key=key1, CopySource=copy_source)

    # delete the source object. keep the 'null' entry in version_ids
    client.delete_object(Bucket=bucket_name, Key=key1)

    # get the target object
    response = client.get_object(Bucket=bucket_name, Key=key2)
    body = _get_body(response)
    assert body == content

    # get the target object from the other bucket
    response = client.get_object(Bucket=bucket_name2, Key=key1)
    body = _get_body(response)
    assert body == content

    # cleaning up
    client.delete_object(Bucket=bucket_name, Key=key2)
    client.delete_object(Bucket=bucket_name, Key=key2, VersionId='null')
    clean_up_bucket(client, bucket_name, key1, version_ids)

    client.delete_object(Bucket=bucket_name2, Key=key1)
    client.delete_bucket(Bucket=bucket_name2)

def test_versioning_obj_create_versions_remove_all():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'testobj'
    num_versions = 10

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)
    for idx in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

    assert len(version_ids) == 0
    assert len(version_ids) == len(contents)

def test_versioning_obj_create_versions_remove_special_names():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    keys = ['_testobj', '_', ':', ' ']
    num_versions = 10

    for key in keys:
        (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)
        for idx in range(num_versions):
            remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

        assert len(version_ids) == 0
        assert len(version_ids) == len(contents)

@pytest.mark.fails_on_dbstore
def test_versioning_obj_create_overwrite_multipart():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'testobj'
    num_versions = 3
    contents = []
    version_ids = []

    for i in range(num_versions):
        ret =  _do_test_multipart_upload_contents(bucket_name, key, 3)
        contents.append(ret)

    response = client.list_object_versions(Bucket=bucket_name)
    for version in response['Versions']:
        version_ids.append(version['VersionId'])

    version_ids.reverse()
    check_obj_versions(client, bucket_name, key, version_ids, contents)

    for idx in range(num_versions):
        remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

    assert len(version_ids) == 0
    assert len(version_ids) == len(contents)

def test_versioning_obj_list_marker():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'testobj'
    key2 = 'testobj-1'
    num_versions = 5

    contents = []
    version_ids = []
    contents2 = []
    version_ids2 = []

    # for key #1
    for i in range(num_versions):
        body = 'content-{i}'.format(i=i)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        version_id = response['VersionId']

        contents.append(body)
        version_ids.append(version_id)

    # for key #2
    for i in range(num_versions):
        body = 'content-{i}'.format(i=i)
        response = client.put_object(Bucket=bucket_name, Key=key2, Body=body)
        version_id = response['VersionId']

        contents2.append(body)
        version_ids2.append(version_id)

    response = client.list_object_versions(Bucket=bucket_name)
    versions = response['Versions']
    # obj versions in versions come out created last to first not first to last like version_ids & contents
    versions.reverse()

    i = 0
    # test the last 5 created objects first
    for i in range(5):
        version = versions[i]
        assert version['VersionId'] == version_ids2[i]
        assert version['Key'] == key2
        check_obj_content(client, bucket_name, key2, version['VersionId'], contents2[i])
        i += 1

    # then the first 5
    for j in range(5):
        version = versions[i]
        assert version['VersionId'] == version_ids[j]
        assert version['Key'] == key
        check_obj_content(client, bucket_name, key, version['VersionId'], contents[j])
        i += 1

@pytest.mark.fails_on_dbstore
def test_versioning_copy_obj_version():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'testobj'
    num_versions = 3

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)

    for i in range(num_versions):
        new_key_name = 'key_{i}'.format(i=i)
        copy_source = {'Bucket': bucket_name, 'Key': key, 'VersionId': version_ids[i]}
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=new_key_name)
        response = client.get_object(Bucket=bucket_name, Key=new_key_name)
        body = _get_body(response)
        assert body == contents[i]

    another_bucket_name = get_new_bucket()

    for i in range(num_versions):
        new_key_name = 'key_{i}'.format(i=i)
        copy_source = {'Bucket': bucket_name, 'Key': key, 'VersionId': version_ids[i]}
        client.copy_object(Bucket=another_bucket_name, CopySource=copy_source, Key=new_key_name)
        response = client.get_object(Bucket=another_bucket_name, Key=new_key_name)
        body = _get_body(response)
        assert body == contents[i]

    new_key_name = 'new_key'
    copy_source = {'Bucket': bucket_name, 'Key': key}
    client.copy_object(Bucket=another_bucket_name, CopySource=copy_source, Key=new_key_name)

    response = client.get_object(Bucket=another_bucket_name, Key=new_key_name)
    body = _get_body(response)
    assert body == contents[-1]

def test_versioning_multi_object_delete():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'key'
    num_versions = 2

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)
    assert len(version_ids) == 2

    # delete both versions
    objects = [{'Key': key, 'VersionId': v} for v in version_ids]
    client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response

    # now remove again, should all succeed due to idempotency
    client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response

def test_versioning_multi_object_delete_with_marker():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'key'
    num_versions = 2

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)
    assert len(version_ids) == num_versions
    objects = [{'Key': key, 'VersionId': v} for v in version_ids]

    # create a delete marker
    response = client.delete_object(Bucket=bucket_name, Key=key)
    assert response['DeleteMarker']
    objects += [{'Key': key, 'VersionId': response['VersionId']}]

    # delete all versions
    client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response
    assert not 'DeleteMarkers' in response

    # now remove again, should all succeed due to idempotency
    client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response
    assert not 'DeleteMarkers' in response

@pytest.mark.fails_on_dbstore
def test_versioning_multi_object_delete_with_marker_create():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'key'

    # use delete_objects() to create a delete marker
    response = client.delete_objects(Bucket=bucket_name, Delete={'Objects': [{'Key': key}]})
    assert len(response['Deleted']) == 1
    assert response['Deleted'][0]['DeleteMarker']
    delete_marker_version_id = response['Deleted'][0]['DeleteMarkerVersionId']

    response = client.list_object_versions(Bucket=bucket_name)
    delete_markers = response['DeleteMarkers']

    assert len(delete_markers) == 1
    assert delete_marker_version_id == delete_markers[0]['VersionId']
    assert key == delete_markers[0]['Key']

def test_versioned_object_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'xyz'
    num_versions = 3

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)

    version_id = version_ids[1]

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    assert response['Owner']['DisplayName'] == display_name
    assert response['Owner']['ID'] == user_id

    grants = response['Grants']
    default_policy = [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ]

    check_grants(grants, default_policy)

    client.put_object_acl(ACL='public-read',Bucket=bucket_name, Key=key, VersionId=version_id)

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

    client.put_object(Bucket=bucket_name, Key=key)

    response = client.get_object_acl(Bucket=bucket_name, Key=key)
    grants = response['Grants']
    check_grants(grants, default_policy)

@pytest.mark.fails_on_dbstore
def test_versioned_object_acl_no_version_specified():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'xyz'
    num_versions = 3

    (version_ids, contents) = create_multiple_versions(client, bucket_name, key, num_versions)

    response = client.get_object(Bucket=bucket_name, Key=key)
    version_id = response['VersionId']

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)

    display_name = get_main_display_name()
    user_id = get_main_user_id()

    assert response['Owner']['DisplayName'] == display_name
    assert response['Owner']['ID'] == user_id

    grants = response['Grants']
    default_policy = [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ]

    check_grants(grants, default_policy)

    client.put_object_acl(ACL='public-read',Bucket=bucket_name, Key=key)

    response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
    grants = response['Grants']
    check_grants(
        grants,
        [
            dict(
                Permission='READ',
                ID=None,
                DisplayName=None,
                URI='http://acs.amazonaws.com/groups/global/AllUsers',
                EmailAddress=None,
                Type='Group',
                ),
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
                ),
            ],
        )

def _do_create_object(client, bucket_name, key, i):
    body = 'data {i}'.format(i=i)
    client.put_object(Bucket=bucket_name, Key=key, Body=body)

def _do_remove_ver(client, bucket_name, key, version_id):
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)

def _do_create_versioned_obj_concurrent(client, bucket_name, key, num):
    t = []
    for i in range(num):
        thr = threading.Thread(target = _do_create_object, args=(client, bucket_name, key, i))
        thr.start()
        t.append(thr)
    return t

def _do_clear_versioned_bucket_concurrent(client, bucket_name):
    t = []
    response = client.list_object_versions(Bucket=bucket_name)
    for version in response.get('Versions', []):
        thr = threading.Thread(target = _do_remove_ver, args=(client, bucket_name, version['Key'], version['VersionId']))
        thr.start()
        t.append(thr)
    return t

def test_versioned_concurrent_object_create_concurrent_remove():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'myobj'
    num_versions = 5

    for i in range(5):
        t = _do_create_versioned_obj_concurrent(client, bucket_name, key, num_versions)
        _do_wait_completion(t)

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']

        assert len(versions) == num_versions

        t = _do_clear_versioned_bucket_concurrent(client, bucket_name)
        _do_wait_completion(t)

        response = client.list_object_versions(Bucket=bucket_name)
        assert not 'Versions' in response

def test_versioned_concurrent_object_create_and_remove():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    key = 'myobj'
    num_versions = 3

    all_threads = []

    for i in range(3):

        t = _do_create_versioned_obj_concurrent(client, bucket_name, key, num_versions)
        all_threads.append(t)

        t = _do_clear_versioned_bucket_concurrent(client, bucket_name)
        all_threads.append(t)

    for t in all_threads:
        _do_wait_completion(t)

    t = _do_clear_versioned_bucket_concurrent(client, bucket_name)
    _do_wait_completion(t)

    response = client.list_object_versions(Bucket=bucket_name)
    assert not 'Versions' in response

@pytest.mark.lifecycle
def test_lifecycle_set():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'test1/', 'Status':'Enabled'},
           {'ID': 'rule2', 'Expiration': {'Days': 2}, 'Prefix': 'test2/', 'Status':'Disabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.lifecycle
def test_lifecycle_get():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'test1/', 'Expiration': {'Days': 31}, 'Prefix': 'test1/', 'Status':'Enabled'},
           {'ID': 'test2/', 'Expiration': {'Days': 120}, 'Prefix': 'test2/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    assert response['Rules'] == rules

@pytest.mark.lifecycle
def test_lifecycle_delete():
    client = get_client()
    bucket_name = get_new_bucket(client)

    e = assert_raises(ClientError, client.get_bucket_lifecycle_configuration, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchLifecycleConfiguration'

    # returns 204 even if there is no lifecycle config
    response = client.delete_bucket_lifecycle(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    rules=[{'ID': 'test1/', 'Expiration': {'Days': 31}, 'Prefix': 'test1/', 'Status':'Enabled'},
           {'ID': 'test2/', 'Expiration': {'Days': 120}, 'Prefix': 'test2/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    client.get_bucket_lifecycle_configuration(Bucket=bucket_name)

    response = client.delete_bucket_lifecycle(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    e = assert_raises(ClientError, client.get_bucket_lifecycle_configuration, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'NoSuchLifecycleConfiguration'

    # returns 204 even if there is no lifecycle config
    response = client.delete_bucket_lifecycle(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

@pytest.mark.lifecycle
def test_lifecycle_get_no_id():
    bucket_name = get_new_bucket()
    client = get_client()

    rules=[{'Expiration': {'Days': 31}, 'Prefix': 'test1/', 'Status':'Enabled'},
           {'Expiration': {'Days': 120}, 'Prefix': 'test2/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    current_lc = response['Rules']

    Rule = namedtuple('Rule',['prefix','status','days'])
    rules = {'rule1' : Rule('test1/','Enabled',31),
             'rule2' : Rule('test2/','Enabled',120)}

    for lc_rule in current_lc:
        if lc_rule['Prefix'] == rules['rule1'].prefix:
            assert lc_rule['Expiration']['Days'] == rules['rule1'].days
            assert lc_rule['Status'] == rules['rule1'].status
            assert 'ID' in lc_rule
        elif lc_rule['Prefix'] == rules['rule2'].prefix:
            assert lc_rule['Expiration']['Days'] == rules['rule2'].days
            assert lc_rule['Status'] == rules['rule2'].status
            assert 'ID' in lc_rule
        else:
            # neither of the rules we supplied was returned, something wrong
            print("rules not right")
            assert False

# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration():
    bucket_name = _create_objects(keys=['expire1/foo', 'expire1/bar', 'keep2/foo',
                                        'keep2/bar', 'expire3/foo', 'expire3/bar'])
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'expire1/', 'Status':'Enabled'},
           {'ID': 'rule2', 'Expiration': {'Days': 5}, 'Prefix': 'expire3/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response = client.list_objects(Bucket=bucket_name)
    init_objects = response['Contents']

    lc_interval = get_lc_debug_interval()

    time.sleep(3*lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire1_objects = response['Contents']

    time.sleep(lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    keep2_objects = response['Contents']

    time.sleep(3*lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire3_objects = response['Contents']

    assert len(init_objects) == 6
    assert len(expire1_objects) == 4
    assert len(keep2_objects) == 4
    assert len(expire3_objects) == 2

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.list_objects_v2
@pytest.mark.fails_on_dbstore
def test_lifecyclev2_expiration():
    bucket_name = _create_objects(keys=['expire1/foo', 'expire1/bar', 'keep2/foo',
                                        'keep2/bar', 'expire3/foo', 'expire3/bar'])
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'expire1/', 'Status':'Enabled'},
           {'ID': 'rule2', 'Expiration': {'Days': 5}, 'Prefix': 'expire3/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response = client.list_objects_v2(Bucket=bucket_name)
    init_objects = response['Contents']

    lc_interval = get_lc_debug_interval()

    time.sleep(3*lc_interval)
    response = client.list_objects_v2(Bucket=bucket_name)
    expire1_objects = response['Contents']

    time.sleep(lc_interval)
    response = client.list_objects_v2(Bucket=bucket_name)
    keep2_objects = response['Contents']

    time.sleep(3*lc_interval)
    response = client.list_objects_v2(Bucket=bucket_name)
    expire3_objects = response['Contents']

    assert len(init_objects) == 6
    assert len(expire1_objects) == 4
    assert len(keep2_objects) == 4
    assert len(expire3_objects) == 2

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
def test_lifecycle_expiration_versioning_enabled():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 1)
    client.delete_object(Bucket=bucket_name, Key="test1/a")

    rules=[{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()

    time.sleep(3*lc_interval)

    response  = client.list_object_versions(Bucket=bucket_name)
    versions = response['Versions']
    delete_markers = response['DeleteMarkers']
    assert len(versions) == 1
    assert len(delete_markers) == 1

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
def test_lifecycle_expiration_tags1():
    bucket_name = get_new_bucket()
    client = get_client()

    tom_key = 'days1/tom'
    tom_tagset = {'TagSet':
                  [{'Key': 'tom', 'Value': 'sawyer'}]}

    client.put_object(Bucket=bucket_name, Key=tom_key, Body='tom_body')

    response = client.put_object_tagging(Bucket=bucket_name, Key=tom_key,
                                         Tagging=tom_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lifecycle_config = {
        'Rules': [
            {
                'Expiration': {
                    'Days': 1,
                },
                'ID': 'rule_tag1',
                'Filter': {
                    'Prefix': 'days1/',
                    'Tag': {
                        'Key': 'tom',
                        'Value': 'sawyer'
                    },
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lc_interval = get_lc_debug_interval()

    time.sleep(3*lc_interval)

    try:
        expire_objects = response['Contents']
    except KeyError:
        expire_objects = []

    assert len(expire_objects) == 0

# factor out common setup code
def setup_lifecycle_tags2(client, bucket_name):
    tom_key = 'days1/tom'
    tom_tagset = {'TagSet':
                  [{'Key': 'tom', 'Value': 'sawyer'}]}

    client.put_object(Bucket=bucket_name, Key=tom_key, Body='tom_body')

    response = client.put_object_tagging(Bucket=bucket_name, Key=tom_key,
                                         Tagging=tom_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    huck_key = 'days1/huck'
    huck_tagset = {
        'TagSet':
        [{'Key': 'tom', 'Value': 'sawyer'},
         {'Key': 'huck', 'Value': 'finn'}]}

    client.put_object(Bucket=bucket_name, Key=huck_key, Body='huck_body')

    response = client.put_object_tagging(Bucket=bucket_name, Key=huck_key,
                                         Tagging=huck_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lifecycle_config = {
        'Rules': [
            {
                'Expiration': {
                    'Days': 1,
                },
                'ID': 'rule_tag1',
                'Filter': {
                    'Prefix': 'days1/',
                    'Tag': {
                        'Key': 'tom',
                        'Value': 'sawyer'
                    },
                    'And': {
                        'Prefix': 'days1',
                        'Tags': [
                            {
                                'Key': 'huck',
                                'Value': 'finn'
                            },
                        ]
                    }
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return response

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_tags2():
    bucket_name = get_new_bucket()
    client = get_client()

    response = setup_lifecycle_tags2(client, bucket_name)

    lc_interval = get_lc_debug_interval()

    time.sleep(3*lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire1_objects = response['Contents']

    assert len(expire1_objects) == 1

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_versioned_tags2():
    bucket_name = get_new_bucket()
    client = get_client()

    # mix in versioning
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    response = setup_lifecycle_tags2(client, bucket_name)

    lc_interval = get_lc_debug_interval()

    time.sleep(3*lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire1_objects = response['Contents']

    assert len(expire1_objects) == 1

# setup for scenario based on vidushi mishra's in rhbz#1877737
def setup_lifecycle_noncur_tags(client, bucket_name, days):

    # first create and tag the objects (10 versions of 1)
    key = "myobject_"
    tagset = {'TagSet':
              [{'Key': 'vidushi', 'Value': 'mishra'}]}

    for ix in range(10):
        body = "%s v%d" % (key, ix)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        response = client.put_object_tagging(Bucket=bucket_name, Key=key,
                                             Tagging=tagset)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lifecycle_config = {
        'Rules': [
            {
                'NoncurrentVersionExpiration': {
                    'NoncurrentDays': days,
                },
                'ID': 'rule_tag1',
                'Filter': {
                    'Prefix': '',
                    'Tag': {
                        'Key': 'vidushi',
                        'Value': 'mishra'
                    },
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return response

def verify_lifecycle_expiration_noncur_tags(client, bucket_name, secs):
    time.sleep(secs)
    try:
        response  = client.list_object_versions(Bucket=bucket_name)
        objs_list = response['Versions']
    except:
        objs_list = []
    return len(objs_list)

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_noncur_tags1():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    # create 10 object versions (9 noncurrent) and a tag-filter
    # noncurrent version expiration at 4 "days"
    response = setup_lifecycle_noncur_tags(client, bucket_name, 4)

    lc_interval = get_lc_debug_interval()

    num_objs = verify_lifecycle_expiration_noncur_tags(
        client, bucket_name, 2*lc_interval)

    # at T+20, 10 objects should exist
    assert num_objs == 10

    num_objs = verify_lifecycle_expiration_noncur_tags(
        client, bucket_name, 5*lc_interval)

    # at T+60, only the current object version should exist
    assert num_objs == 1

def wait_interval_list_object_versions(client, bucket_name, secs):
    time.sleep(secs)
    try:
        response  = client.list_object_versions(Bucket=bucket_name)
        objs_list = response['Versions']
    except:
        objs_list = []
    return len(objs_list)

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_newer_noncurrent():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    # create 10 object versions (9 noncurrent)
    key = "myobject_"

    for ix in range(10):
        body = "%s v%d" % (key, ix)
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # add a lifecycle rule which sets newer-noncurrent-versions to 5
    days = 1
    lifecycle_config = {
        'Rules': [
            {
                'NoncurrentVersionExpiration': {
                    'NoncurrentDays': days,
                    'NewerNoncurrentVersions': 5,
                },
                'ID': 'newer_noncurrent1',
                'Filter': {
                    'Prefix': '',
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lc_interval = get_lc_debug_interval()

    num_objs = wait_interval_list_object_versions(
        client, bucket_name, 2*lc_interval)

    # at T+20, 6 objects should exist (1 current and (9 - 5) noncurrent)
    assert num_objs == 6

def get_byte_buffer(nbytes):
    buf = BytesIO(b"")
    for x in range(nbytes):
        buf.write(b"b")
    buf.seek(0)
    return buf

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_size_gt():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    # create one object lt and one object gt 2000 bytes
    key = "myobject_small"
    body = get_byte_buffer(1000)
    response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    key = "myobject_big"
    body = get_byte_buffer(3000)
    response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # add a lifecycle rule which expires objects greater than 2000 bytes
    days = 1
    lifecycle_config = {
        'Rules': [
            {
                'Expiration': {
                    'Days': days
                },
                'ID': 'object_gt1',
                'Filter': {
                    'Prefix': '',
                    'ObjectSizeGreaterThan': 2000
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lc_interval = get_lc_debug_interval()
    time.sleep(10*lc_interval)

    # we should find only the small object present
    response = client.list_objects(Bucket=bucket_name)
    objects = response['Contents']

    assert len(objects) == 1
    assert objects[0]['Key'] == "myobject_small"

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_size_lt():
    bucket_name = get_new_bucket()
    client = get_client()

    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    # create one object lt and one object gt 2000 bytes
    key = "myobject_small"
    body = get_byte_buffer(1000)
    response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    key = "myobject_big"
    body = get_byte_buffer(3000)
    response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # add a lifecycle rule which expires objects greater than 2000 bytes
    days = 1
    lifecycle_config = {
        'Rules': [
            {
                'Expiration': {
                    'Days': days
                },
                'ID': 'object_lt1',
                'Filter': {
                    'Prefix': '',
                    'ObjectSizeLessThan': 2000
                },
                'Status': 'Enabled',
            },
        ]
    }

    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lc_interval = get_lc_debug_interval()
    time.sleep(2*lc_interval)

    # we should find only the large object present
    response = client.list_objects(Bucket=bucket_name)
    objects = response['Contents']

    assert len(objects) == 1
    assert objects[0]['Key'] == "myobject_big"

@pytest.mark.lifecycle
def test_lifecycle_id_too_long():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 256*'a', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}

    e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.lifecycle
def test_lifecycle_same_id():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'test1/', 'Status':'Enabled'},
           {'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test2/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}

    e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.lifecycle
def test_lifecycle_invalid_status():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status':'enabled'}]
    lifecycle = {'Rules': rules}

    e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'

    rules=[{'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status':'disabled'}]
    lifecycle = {'Rules': rules}

    e = assert_raises(ClientError, client.put_bucket_lifecycle, Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'

    rules=[{'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status':'invalid'}]
    lifecycle = {'Rules': rules}

    e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'

@pytest.mark.lifecycle
def test_lifecycle_set_date():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Date': '2017-09-27'}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}

    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.lifecycle
def test_lifecycle_set_invalid_date():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Date': '20200101'}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}

    e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_date():
    bucket_name = _create_objects(keys=['past/foo', 'future/bar'])
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Date': '2015-01-01'}, 'Prefix': 'past/', 'Status':'Enabled'},
           {'ID': 'rule2', 'Expiration': {'Date': '2030-01-01'}, 'Prefix': 'future/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    response = client.list_objects(Bucket=bucket_name)
    init_objects = response['Contents']

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(3*lc_interval)
    response = client.list_objects(Bucket=bucket_name)
    expire_objects = response['Contents']

    assert len(init_objects) == 2
    assert len(expire_objects) == 1

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
def test_lifecycle_expiration_days0():
    bucket_name = _create_objects(keys=['days0/foo', 'days0/bar'])
    client = get_client()

    rules=[{'Expiration': {'Days': 0}, 'ID': 'rule1', 'Prefix': 'days0/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}

    # days: 0 is legal in a transition rule, but not legal in an
    # expiration rule
    response_code = ""
    try:
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    except botocore.exceptions.ClientError as e:
        response_code = e.response['Error']['Code']

    assert response_code == 'InvalidArgument'


def setup_lifecycle_expiration(client, bucket_name, rule_id, delta_days,
                                    rule_prefix):
    rules=[{'ID': rule_id,
            'Expiration': {'Days': delta_days}, 'Prefix': rule_prefix,
            'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    key = rule_prefix + 'foo'
    body = 'bar'
    response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return response

def check_lifecycle_expiration_header(response, start_time, rule_id,
                                      delta_days):
    expr_exists = ('x-amz-expiration' in response['ResponseMetadata']['HTTPHeaders'])
    if (not expr_exists):
        return False
    expr_hdr = response['ResponseMetadata']['HTTPHeaders']['x-amz-expiration']

    m = re.search(r'expiry-date="(.+)", rule-id="(.+)"', expr_hdr)

    expiration = dateutil.parser.parse(m.group(1))
    days_to_expire = ((expiration.replace(tzinfo=None) - start_time).days == delta_days)
    rule_eq_id = (m.group(2) == rule_id)

    return  days_to_expire and rule_eq_id

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
def test_lifecycle_expiration_header_put():
    bucket_name = get_new_bucket()
    client = get_client()

    now = datetime.datetime.utcnow()
    response = setup_lifecycle_expiration(
        client, bucket_name, 'rule1', 1, 'days1/')
    assert check_lifecycle_expiration_header(response, now, 'rule1', 1)

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_header_head():
    bucket_name = get_new_bucket()
    client = get_client()

    now = datetime.datetime.utcnow()
    response = setup_lifecycle_expiration(
        client, bucket_name, 'rule1', 1, 'days1/')

    key = 'days1/' + 'foo'

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert check_lifecycle_expiration_header(response, now, 'rule1', 1)

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_header_tags_head():
    bucket_name = get_new_bucket()
    client = get_client()
    lifecycle={
        "Rules": [
        {
            "Filter": {
                "Tag": {"Key": "key1", "Value": "tag1"}
            },
            "Status": "Enabled",
            "Expiration": {
                "Days": 1
            },
            "ID": "rule1"
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    key1 = "obj_key1"
    body1 = "obj_key1_body"
    tags1={'TagSet': [{'Key': 'key1', 'Value': 'tag1'},
          {'Key': 'key5','Value': 'tag5'}]}
    response = client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key1,Tagging=tags1)

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert check_lifecycle_expiration_header(response, datetime.datetime.now(None), 'rule1', 1)

    # test that header is not returning when it should not
    lifecycle={
        "Rules": [
        {
            "Filter": {
                "Tag": {"Key": "key2", "Value": "tag1"}
            },
            "Status": "Enabled",
            "Expiration": {
                "Days": 1
            },
            "ID": "rule1"
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert check_lifecycle_expiration_header(response, datetime.datetime.now(None), 'rule1', 1) == False

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_dbstore
def test_lifecycle_expiration_header_and_tags_head():
    now = datetime.datetime.utcnow()
    bucket_name = get_new_bucket()
    client = get_client()
    lifecycle={
        "Rules": [
        {
            "Filter": {
                "And": {
                    "Tags": [
                        {
                            "Key": "key1",
                            "Value": "tag1"
                        },
                        {
                            "Key": "key5",
                            "Value": "tag6"
                        }
                    ]
                }
            },
            "Status": "Enabled",
            "Expiration": {
                "Days": 1
            },
            "ID": "rule1"
            },
        ]
    }
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    key1 = "obj_key1"
    body1 = "obj_key1_body"
    tags1={'TagSet': [{'Key': 'key1', 'Value': 'tag1'},
          {'Key': 'key5','Value': 'tag5'}]}
    response = client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key1,Tagging=tags1)

    # stat the object, check header
    response = client.head_object(Bucket=bucket_name, Key=key1)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert check_lifecycle_expiration_header(response, datetime.datetime.now(None), 'rule1', 1) == False

@pytest.mark.lifecycle
def test_lifecycle_set_noncurrent():
    bucket_name = _create_objects(keys=['past/foo', 'future/bar'])
    client = get_client()
    rules=[{'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 2}, 'Prefix': 'past/', 'Status':'Enabled'},
           {'ID': 'rule2', 'NoncurrentVersionExpiration': {'NoncurrentDays': 3}, 'Prefix': 'future/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_noncur_expiration():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 3)
    # not checking the object contents on the second run, because the function doesn't support multiple checks
    create_multiple_versions(client, bucket_name, "test2/abc", 3, check_versions=False)

    response  = client.list_object_versions(Bucket=bucket_name)
    init_versions = response['Versions']

    rules=[{'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 2}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(5*lc_interval)

    response  = client.list_object_versions(Bucket=bucket_name)
    expire_versions = response['Versions']
    assert len(init_versions) == 6
    assert len(expire_versions) == 4

@pytest.mark.lifecycle
def test_lifecycle_set_deletemarker():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.lifecycle
def test_lifecycle_set_filter():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Filter': {'Prefix': 'foo'}, 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.lifecycle
def test_lifecycle_set_empty_filter():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Filter': {}, 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_deletemarker_expiration():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 1)
    create_multiple_versions(client, bucket_name, "test2/abc", 1, check_versions=False)
    client.delete_object(Bucket=bucket_name, Key="test1/a")
    client.delete_object(Bucket=bucket_name, Key="test2/abc")

    response  = client.list_object_versions(Bucket=bucket_name)
    init_versions = response['Versions']
    deleted_versions = response['DeleteMarkers']
    total_init_versions = init_versions + deleted_versions

    rules=[{'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 1}, 'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(7*lc_interval)

    response  = client.list_object_versions(Bucket=bucket_name)
    init_versions = response['Versions']
    deleted_versions = response['DeleteMarkers']
    total_expire_versions = init_versions + deleted_versions

    assert len(total_init_versions) == 4
    assert len(total_expire_versions) == 2

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_deletemarker_expiration_with_days_tag():
    bucket_name = get_new_bucket()
    client = get_client()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    create_multiple_versions(client, bucket_name, "test1/a", 1)
    client.delete_object(Bucket=bucket_name, Key="test1/a")

    rules=[{'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 1}, 'Expiration': {'Days': 5}, 'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(2*lc_interval)

    response  = client.list_object_versions(Bucket=bucket_name)
    versions = response['Versions'] if ('Versions' in response) else []
    delete_markers = response['DeleteMarkers'] if ('DeleteMarkers' in response) else []

    assert len(versions) == 0
    assert len(delete_markers) == 1

    time.sleep(4*lc_interval)

    response  = client.list_object_versions(Bucket=bucket_name)
    delete_markers = response['DeleteMarkers'] if ('DeleteMarkers' in response) else []

    assert len(delete_markers) == 0

@pytest.mark.lifecycle
def test_lifecycle_set_multipart():
    bucket_name = get_new_bucket()
    client = get_client()
    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled',
         'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 2}},
        {'ID': 'rule2', 'Prefix': 'test2/', 'Status': 'Disabled',
         'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 3}}
    ]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_multipart_expiration():
    bucket_name = get_new_bucket()
    client = get_client()

    key_names = ['test1/a', 'test2/']
    upload_ids = []

    for key in key_names:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_ids.append(response['UploadId'])

    response = client.list_multipart_uploads(Bucket=bucket_name)
    init_uploads = response['Uploads']

    rules = [
        {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled',
         'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 2}},
    ]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(5*lc_interval)

    response = client.list_multipart_uploads(Bucket=bucket_name)
    expired_uploads = response['Uploads']
    assert len(init_uploads) == 2
    assert len(expired_uploads) == 1

@pytest.mark.lifecycle
def test_lifecycle_transition_set_invalid_date():
    bucket_name = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Expiration': {'Date': '2023-09-27'},'Transitions': [{'Date': '20220927','StorageClass': 'GLACIER'}],'Prefix': 'test1/', 'Status':'Enabled'}]
    lifecycle = {'Rules': rules}
    e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name, LifecycleConfiguration=lifecycle)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

def _test_encryption_sse_customer_write(file_size):
    """
    Tests Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'testobj'
    data = 'A'*file_size
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == data

# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.fails_on_aws
def test_lifecycle_transition():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip('requires 3 or more storage classes')

    bucket_name = _create_objects(keys=['expire1/foo', 'expire1/bar', 'keep2/foo',
                                        'keep2/bar', 'expire3/foo', 'expire3/bar'])
    client = get_client()
    rules=[{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': sc[1]}], 'Prefix': 'expire1/', 'Status': 'Enabled'},
           {'ID': 'rule2', 'Transitions': [{'Days': 6, 'StorageClass': sc[2]}], 'Prefix': 'expire3/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 6

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(4*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys['STANDARD']) == 4
    assert len(expire1_keys[sc[1]]) == 2
    assert len(expire1_keys[sc[2]]) == 0

    # Wait for next expiration cycle
    time.sleep(lc_interval)
    keep2_keys = list_bucket_storage_class(client, bucket_name)
    assert len(keep2_keys['STANDARD']) == 4
    assert len(keep2_keys[sc[1]]) == 2
    assert len(keep2_keys[sc[2]]) == 0

    # Wait for final expiration cycle
    time.sleep(5*lc_interval)
    expire3_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire3_keys['STANDARD']) == 2
    assert len(expire3_keys[sc[1]]) == 2
    assert len(expire3_keys[sc[2]]) == 2

# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.fails_on_aws
def test_lifecycle_transition_single_rule_multi_trans():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip('requires 3 or more storage classes')

    bucket_name = _create_objects(keys=['expire1/foo', 'expire1/bar', 'keep2/foo',
                                        'keep2/bar', 'expire3/foo', 'expire3/bar'])
    client = get_client()
    rules=[{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': sc[1]}, {'Days': 7, 'StorageClass': sc[2]}], 'Prefix': 'expire1/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 6

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(5*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys['STANDARD']) == 4
    assert len(expire1_keys[sc[1]]) == 2
    assert len(expire1_keys[sc[2]]) == 0

    # Wait for next expiration cycle
    time.sleep(lc_interval)
    keep2_keys = list_bucket_storage_class(client, bucket_name)
    assert len(keep2_keys['STANDARD']) == 4
    assert len(keep2_keys[sc[1]]) == 2
    assert len(keep2_keys[sc[2]]) == 0

    # Wait for final expiration cycle
    time.sleep(6*lc_interval)
    expire3_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire3_keys['STANDARD']) == 4
    assert len(expire3_keys[sc[1]]) == 0
    assert len(expire3_keys[sc[2]]) == 2

@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
def test_lifecycle_set_noncurrent_transition():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip('requires 3 or more storage classes')

    bucket = get_new_bucket()
    client = get_client()
    rules = [
        {
            'ID': 'rule1',
            'Prefix': 'test1/',
            'Status': 'Enabled',
            'NoncurrentVersionTransitions': [
                {
                    'NoncurrentDays': 2,
                    'StorageClass': sc[1]
                },
                {
                    'NoncurrentDays': 4,
                    'StorageClass': sc[2]
                }
            ],
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': 6
            }
        },
        {'ID': 'rule2', 'Prefix': 'test2/', 'Status': 'Disabled', 'NoncurrentVersionExpiration': {'NoncurrentDays': 3}}
    ]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.lifecycle_transition
@pytest.mark.fails_on_aws
def test_lifecycle_noncur_transition():
    sc = configured_storage_classes()
    if len(sc) < 3:
        pytest.skip('requires 3 or more storage classes')

    bucket = get_new_bucket()
    client = get_client()

    # before enabling versioning, create a plain entry
    # which should get transitioned/expired similar to
    # other non-current versioned entries.
    key = 'test1/a'
    content = 'fooz'
    client.put_object(Bucket=bucket, Key=key, Body=content)

    check_configure_versioning_retry(bucket, "Enabled", "Enabled")

    rules = [
        {
            'ID': 'rule1',
            'Prefix': 'test1/',
            'Status': 'Enabled',
            'NoncurrentVersionTransitions': [
                {
                    'NoncurrentDays': 1,
                    'StorageClass': sc[1]
                },
                {
                    'NoncurrentDays': 5,
                    'StorageClass': sc[2]
                }
            ],
            'NoncurrentVersionExpiration': {
                'NoncurrentDays': 9
            }
        }
    ]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    create_multiple_versions(client, bucket, "test1/a", 2)
    create_multiple_versions(client, bucket, "test1/b", 3)

    init_keys = list_bucket_storage_class(client, bucket)
    assert len(init_keys['STANDARD']) == 6

    lc_interval = get_lc_debug_interval()

    time.sleep(4*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys['STANDARD']) == 2
    assert len(expire1_keys[sc[1]]) == 4
    assert len(expire1_keys[sc[2]]) == 0

    time.sleep(4*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys['STANDARD']) == 2
    assert len(expire1_keys[sc[1]]) == 0
    assert len(expire1_keys[sc[2]]) == 4

    time.sleep(6*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys['STANDARD']) == 2
    assert len(expire1_keys[sc[1]]) == 0
    assert len(expire1_keys[sc[2]]) == 0

@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.lifecycle_transition
def test_lifecycle_plain_null_version_current_transition():
    sc = configured_storage_classes()
    if len(sc) < 2:
        pytest.skip('requires 2 or more storage classes')

    target_sc = sc[1]
    assert target_sc != 'STANDARD'

    bucket = get_new_bucket()
    check_versioning(bucket, None)

    # create a plain object before enabling versioning;
    # this will be transitioned as a current version
    client = get_client()
    key = 'testobjfoo'
    content = 'fooz'
    client.put_object(Bucket=bucket, Key=key, Body=content)

    check_configure_versioning_retry(bucket, "Enabled", "Enabled")

    client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={
            'Rules': [
                {
                    'ID': 'rule1',
                    'Prefix': 'testobj',
                    'Status': 'Enabled',
                    'Transitions': [
                        {
                            'Days': 1,
                            'StorageClass': target_sc
                        },
                    ]
                }
            ]
        })

    lc_interval = get_lc_debug_interval()
    time.sleep(4*lc_interval)

    keys = list_bucket_storage_class(client, bucket)
    assert len(keys['STANDARD']) == 0
    assert len(keys[target_sc]) == 1

def verify_object(client, bucket, key, content=None, sc=None):
    response = client.get_object(Bucket=bucket, Key=key)

    if (sc == None):
        sc = 'STANDARD'

    if ('StorageClass' in response):
        assert response['StorageClass'] == sc
    else: #storage class should be STANDARD
        assert 'STANDARD' == sc

    if (content != None):
        body = _get_body(response)
        assert body == content

def verify_transition(client, bucket, key, sc=None, version=None):
    if (version != None):
        response = client.head_object(Bucket=bucket, Key=key, VersionId=version)
    else:
        response = client.head_object(Bucket=bucket, Key=key)

    # Iterate over the contents to find the StorageClass
    if 'StorageClass' in response:
        assert response['StorageClass'] == sc
    else: # storage class should be STANDARD
        assert 'STANDARD' == sc

# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_cloud_transition():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip('no cloud_storage_class configured')

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    keys=['expire1/foo', 'expire1/bar', 'keep2/foo', 'keep2/bar']
    bucket_name = _create_objects(keys=keys)
    client = get_client()
    rules=[{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': cloud_sc}], 'Prefix': 'expire1/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 4

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(10*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys['STANDARD']) == 2

    if (retain_head_object != None and retain_head_object == "true"):
        assert len(expire1_keys[cloud_sc]) == 2
    else:
        assert len(expire1_keys[cloud_sc]) == 0

    time.sleep(2*lc_interval)
    # Check if objects copied to target path
    if target_path == None:
        target_path = "rgwx-default-" + cloud_sc.lower() + "-cloud-bucket"
    prefix = bucket_name + "/"

    cloud_client = get_cloud_client()

    time.sleep(12*lc_interval)
    expire1_key1_str = prefix + keys[0]
    verify_object(cloud_client, target_path, expire1_key1_str, keys[0], target_sc)

    expire1_key2_str = prefix + keys[1]
    verify_object(cloud_client, target_path, expire1_key2_str, keys[1], target_sc)

    # Now verify the object on source rgw
    src_key = keys[0]
    if (retain_head_object != None and retain_head_object == "true"):
        # verify HEAD response
        response = client.head_object(Bucket=bucket_name, Key=keys[0])
        assert 0 == response['ContentLength']
        assert cloud_sc == response['StorageClass']

        allow_readthrough = get_allow_read_through()
        if (allow_readthrough == None or allow_readthrough == "false"):
            # GET should return InvalidObjectState error
            e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=src_key)
            status, error_code = _get_status_and_error_code(e.response)
            assert status == 403
            assert error_code == 'InvalidObjectState'

            # COPY of object should return InvalidObjectState error
            copy_source = {'Bucket': bucket_name, 'Key': src_key}
            e = assert_raises(ClientError, client.copy, CopySource=copy_source, Bucket=bucket_name, Key='copy_obj')
            status, error_code = _get_status_and_error_code(e.response)
            assert status == 403
            assert error_code == 'InvalidObjectState'

        # DELETE should succeed
        response = client.delete_object(Bucket=bucket_name, Key=src_key)
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=src_key)
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 404
        assert error_code == 'NoSuchKey'

# Similar to 'test_lifecycle_transition' but for cloud transition
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_cloud_multiple_transition():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip('[s3 cloud] section missing cloud_storage_class')

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    sc1 = get_cloud_regular_storage_class()

    if (sc1 == None):
        pytest.skip('[s3 cloud] section missing storage_class')

    sc = ['STANDARD', sc1, cloud_sc]

    keys=['expire1/foo', 'expire1/bar', 'keep2/foo', 'keep2/bar']
    bucket_name = _create_objects(keys=keys)
    client = get_client()
    rules=[{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': sc1}], 'Prefix': 'expire1/', 'Status': 'Enabled'},
           {'ID': 'rule2', 'Transitions': [{'Days': 5, 'StorageClass': cloud_sc}], 'Prefix': 'expire1/', 'Status': 'Enabled'},
           {'ID': 'rule3', 'Expiration': {'Days': 9}, 'Prefix': 'expire1/', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

    # Get list of all keys
    response = client.list_objects(Bucket=bucket_name)
    init_keys = _get_keys(response)
    assert len(init_keys) == 4

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(4*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys['STANDARD']) == 2
    assert len(expire1_keys[sc[1]]) == 2
    assert len(expire1_keys[sc[2]]) == 0

    # Wait for next expiration cycle
    time.sleep(7*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire1_keys['STANDARD']) == 2
    assert len(expire1_keys[sc[1]]) == 0

    if (retain_head_object != None and retain_head_object == "true"):
        assert len(expire1_keys[sc[2]]) == 2
    else:
        assert len(expire1_keys[sc[2]]) == 0

    # Wait for final expiration cycle
    time.sleep(12*lc_interval)
    expire3_keys = list_bucket_storage_class(client, bucket_name)
    assert len(expire3_keys['STANDARD']) == 2
    assert len(expire3_keys[sc[1]]) == 0
    assert len(expire3_keys[sc[2]]) == 0

# Noncurrent objects for cloud transition
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_noncur_cloud_transition():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip('[s3 cloud] section missing cloud_storage_class')

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    sc1 = get_cloud_regular_storage_class()
    if (sc1 == None):
        pytest.skip('[s3 cloud] section missing storage_class')

    sc = ['STANDARD', sc1, cloud_sc]

    bucket = get_new_bucket()
    client = get_client()

    # before enabling versioning, create a plain entry
    # which should get transitioned/expired similar to
    # other non-current versioned entries.
    key = 'test1/a'
    content = 'fooz'
    client.put_object(Bucket=bucket, Key=key, Body=content)

    check_configure_versioning_retry(bucket, "Enabled", "Enabled")

    rules = [
        {
            'ID': 'rule1',
            'Prefix': 'test1/',
            'Status': 'Enabled',
            'NoncurrentVersionTransitions': [
                {
                    'NoncurrentDays': 1,
                    'StorageClass': sc[1]
                },
                {
                    'NoncurrentDays': 5,
                    'StorageClass': sc[2]
                }
            ],
        }
    ]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    keys = ['test1/a', 'test1/b']

    create_multiple_versions(client, bucket, "test1/a", 2)
    create_multiple_versions(client, bucket, "test1/b", 3)

    init_keys = list_bucket_storage_class(client, bucket)
    assert len(init_keys['STANDARD']) == 6

    response  = client.list_object_versions(Bucket=bucket)

    lc_interval = get_lc_debug_interval()

    time.sleep(4*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys['STANDARD']) == 2
    assert len(expire1_keys[sc[1]]) == 4
    assert len(expire1_keys[sc[2]]) == 0

    time.sleep(15*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys['STANDARD']) == 2
    assert len(expire1_keys[sc[1]]) == 0

    if (retain_head_object == None or retain_head_object == "false"):
        assert len(expire1_keys[sc[2]]) == 0
    else:
        assert len(expire1_keys[sc[2]]) == 4

    #check if versioned object exists on cloud endpoint
    if target_path == None:
        target_path = "rgwx-default-" + cloud_sc.lower() + "-cloud-bucket"
    prefix = bucket + "/"

    cloud_client = get_cloud_client()

    time.sleep(lc_interval)
    result = list_bucket_versions(client, bucket)

    for src_key in keys:
        for k in result[src_key]: 
            expire1_key1_str = prefix + 'test1/a' + "-" + k['VersionId']
            verify_object(cloud_client, target_path, expire1_key1_str, None, target_sc)

# The test harness for lifecycle is configured to treat days as 10 second intervals.
@pytest.mark.lifecycle
@pytest.mark.lifecycle_transition
@pytest.mark.cloud_transition
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_lifecycle_cloud_transition_large_obj():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip('[s3 cloud] section missing cloud_storage_class')

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    bucket = get_new_bucket()
    client = get_client()
    rules=[{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': cloud_sc}], 'Prefix': 'expire1/', 'Status': 'Enabled'}]

    keys = ['keep/multi', 'expire1/multi']
    size = 9*1024*1024
    data = 'A'*size

    for k in keys:
        client.put_object(Bucket=bucket, Body=data, Key=k)
        verify_object(client, bucket, k, data)

    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()

    # Wait for first expiration (plus fudge to handle the timer window)
    time.sleep(12*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys['STANDARD']) == 1

    
    if (retain_head_object != None and retain_head_object == "true"):
        assert len(expire1_keys[cloud_sc]) == 1
    else:
        assert len(expire1_keys[cloud_sc]) == 0

    # Check if objects copied to target path
    if target_path == None:
        target_path = "rgwx-default-" + cloud_sc.lower() + "-cloud-bucket"
    prefix = bucket + "/"

    # multipart upload takes time
    time.sleep(12*lc_interval)
    cloud_client = get_cloud_client()

    expire1_key1_str = prefix + keys[1]
    verify_object(cloud_client, target_path, expire1_key1_str, data, target_sc)

@pytest.mark.cloud_restore
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_restore_object_temporary():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc is None:
        pytest.skip('[s3 cloud] section missing cloud_storage_class')

    bucket = get_new_bucket()
    client = get_client()
    key = 'test_restore_temp'
    data = 'temporary restore data'

    # Put object
    client.put_object(Bucket=bucket, Key=key, Body=data)
    verify_object(client, bucket, key, data)

    # Transition object to cloud storage class
    rules = [{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': cloud_sc}], 'Prefix': '', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()
    restore_interval = get_restore_debug_interval()
    restore_period = get_restore_processor_period()
    time.sleep(10 * lc_interval)

    # Verify object is transitioned
    verify_transition(client, bucket, key, cloud_sc)

    # delete lifecycle to prevent re-transition before restore check
    response = client.delete_bucket_lifecycle(Bucket=bucket)

    # Restore object temporarily
    client.restore_object(Bucket=bucket, Key=key, RestoreRequest={'Days': 20})
    time.sleep(3*restore_period)

    # Verify object is restored temporarily
    verify_transition(client, bucket, key, cloud_sc)
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentLength'] == len(data)

    # Now re-issue the request with 'Days' set to lower value
    client.restore_object(Bucket=bucket, Key=key, RestoreRequest={'Days': 2})
    time.sleep(2*restore_period)
    # ensure the object is still restored
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentLength'] == len(data)

    # now verify if the object is expired as per the updated days value
    time.sleep(2 * (restore_interval + lc_interval))
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentLength'] == 0

@pytest.mark.cloud_restore
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_restore_object_permanent():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc is None:
        pytest.skip('[s3 cloud] section missing cloud_storage_class')
    
    bucket = get_new_bucket()
    client = get_client()
    key = 'test_restore_perm'
    data = 'permanent restore data'

    # Put object
    client.put_object(Bucket=bucket, Key=key, Body=data)
    verify_object(client, bucket, key, data)

    # Transition object to cloud storage class
    rules = [{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': cloud_sc}], 'Prefix': '', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()
    time.sleep(10 * lc_interval)

    # Verify object is transitioned
    verify_transition(client, bucket, key, cloud_sc)

    # delete lifecycle to prevent re-transition post permanent restore
    response = client.delete_bucket_lifecycle(Bucket=bucket)

    restore_period = get_restore_processor_period()
    # Restore object permanently
    client.restore_object(Bucket=bucket, Key=key, RestoreRequest={})
    time.sleep(3*restore_period)

    # Verify object is restored permanently
    response = client.head_object(Bucket=bucket, Key=key)
    assert response['ContentLength'] == len(data)
    verify_transition(client, bucket, key, 'STANDARD')

@pytest.mark.cloud_restore
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_read_through():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc is None:
        pytest.skip('[s3 cloud] section missing cloud_storage_class')

    bucket = get_new_bucket()
    client_config = botocore.config.Config(connect_timeout=100, read_timeout=100)
    client = get_client(client_config=client_config)

    key = 'test_restore_readthrough'
    data = 'restore data with readthrough'

     # Put object
    client.put_object(Bucket=bucket, Key=key, Body=data)
    verify_object(client, bucket, key, data)

    # Transition object to cloud storage class
    rules = [{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': cloud_sc}], 'Prefix': '', 'Status': 'Enabled'}]
    lifecycle = {'Rules': rules}
    client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    lc_interval = get_lc_debug_interval()
    restore_interval = get_restore_debug_interval()
    time.sleep(10 * lc_interval)

    # Check the storage class after transitioning
    verify_transition(client, bucket, key, cloud_sc)

    # delete lifecycle to prevent re-transition before restore check
    response = client.delete_bucket_lifecycle(Bucket=bucket)

    # Restore the object using read_through request
    allow_readthrough = get_allow_read_through()
    read_through_days = get_read_through_days()
    restore_period = get_restore_processor_period()

    if (allow_readthrough != None and allow_readthrough == "true"):
        try:
            response = client.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            status, error_code = _get_status_and_error_code(e.response)
            assert status == 400
            time.sleep(2 * restore_interval)
            response = client.head_object(Bucket=bucket, Key=key)

        assert response['ContentLength'] == len(data)
        time.sleep(2 * read_through_days * (restore_interval + lc_interval))
        # verify object expired
        response = client.head_object(Bucket=bucket, Key=key)
        assert response['ContentLength'] == 0
    else:
        e = assert_raises(ClientError, client.get_object, Bucket=bucket, Key=key)
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 403
        assert error_code == 'InvalidObjectState'

@pytest.mark.cloud_restore
@pytest.mark.fails_on_aws
@pytest.mark.fails_on_dbstore
def test_restore_noncur_obj():
    cloud_sc = get_cloud_storage_class()
    if cloud_sc == None:
        pytest.skip('[s3 cloud] section missing cloud_storage_class')

    retain_head_object = get_cloud_retain_head_object()
    target_path = get_cloud_target_path()
    target_sc = get_cloud_target_storage_class()

    sc = ['STANDARD', cloud_sc]

    bucket = get_new_bucket()
    client = get_client()

    # before enabling versioning, create a plain entry
    # which should get transitioned/expired similar to
    # other non-current versioned entries.
    key = 'test1/a'
    content = 'fooz'
    client.put_object(Bucket=bucket, Key=key, Body=content)

    check_configure_versioning_retry(bucket, "Enabled", "Enabled")

    rules = [
        {
            'ID': 'rule1',
            'Prefix': 'test1/',
            'Status': 'Enabled',
            'NoncurrentVersionTransitions': [
                {
                    'NoncurrentDays': 2,
                    'StorageClass': cloud_sc
                }
            ],
        }
    ]
    lifecycle = {'Rules': rules}
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

    keys = ['test1/a']

    (version_ids, contents) = create_multiple_versions(client, bucket, "test1/a", 2)

    contents.append(content)

    init_keys = list_bucket_storage_class(client, bucket)
    print(init_keys)
    assert len(init_keys['STANDARD']) == 3

    version_ids = []
    response = client.list_object_versions(Bucket=bucket)
    for version in response['Versions']:
        version_ids.append(version['VersionId'])

    lc_interval = get_lc_debug_interval()

    time.sleep(7*lc_interval)
    expire1_keys = list_bucket_storage_class(client, bucket)
    assert len(expire1_keys['STANDARD']) == 1
    assert len(expire1_keys[cloud_sc]) == 2

    restore_interval = get_restore_debug_interval()
    restore_period = get_restore_processor_period()

    for num in range(1, 2):
        verify_transition(client, bucket, key, cloud_sc, version_ids[num])
        # Restore object temporarily
        client.restore_object(Bucket=bucket, Key=key, VersionId=version_ids[num], RestoreRequest={'Days': 2})
        time.sleep(2*restore_period)

        # Verify object is restored temporarily
        response = client.head_object(Bucket=bucket, Key=key, VersionId=version_ids[num])
        assert response['ContentLength'] == len(contents[num])
        response  = client.list_object_versions(Bucket=bucket)
        versions = response['Versions']
        assert versions[1]['IsLatest'] == False

    time.sleep(2 * (restore_interval + lc_interval))

    #verify object expired
    for num in range(1, 2):
        response = client.head_object(Bucket=bucket, Key=key, VersionId=version_ids[num])
        assert response['ContentLength'] == 0

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_1b():
    _test_encryption_sse_customer_write(1)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_1kb():
    _test_encryption_sse_customer_write(1024)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_1MB():
    _test_encryption_sse_customer_write(1024*1024)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encrypted_transfer_13b():
    _test_encryption_sse_customer_write(13)


@pytest.mark.encryption
def test_encryption_sse_c_method_head():
    bucket_name = get_new_bucket()
    client = get_client()
    data = 'A'*1000
    key = 'testobj'
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.HeadObject', lf)
    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.encryption
def test_encryption_sse_c_present():
    bucket_name = get_new_bucket()
    client = get_client()
    data = 'A'*1000
    key = 'testobj'
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
def test_encryption_sse_c_other_key():
    bucket_name = get_new_bucket()
    client = get_client()
    data = 'A'*100
    key = 'testobj'
    sse_client_headers_A = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    sse_client_headers_B = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers_A))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers_B))
    client.meta.events.register('before-call.s3.GetObject', lf)
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
def test_encryption_sse_c_invalid_md5():
    bucket_name = get_new_bucket()
    client = get_client()
    data = 'A'*100
    key = 'testobj'
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'AAAAAAAAAAAAAAAAAAAAAA=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
def test_encryption_sse_c_no_md5():
    bucket_name = get_new_bucket()
    client = get_client()
    data = 'A'*100
    key = 'testobj'
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)

@pytest.mark.encryption
def test_encryption_sse_c_no_key():
    bucket_name = get_new_bucket()
    client = get_client()
    data = 'A'*100
    key = 'testobj'
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)

@pytest.mark.encryption
def test_encryption_key_no_sse_c():
    bucket_name = get_new_bucket()
    client = get_client()
    data = 'A'*100
    key = 'testobj'
    sse_client_headers = {
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

def _multipart_upload_enc(client, bucket_name, key, size, part_size, init_headers, part_headers, metadata, resend_parts):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """
    if client == None:
        client = get_client()

    lf = (lambda **kwargs: kwargs['params']['headers'].update(init_headers))
    client.meta.events.register('before-call.s3.CreateMultipartUpload', lf)
    if metadata == None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    else:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata)

    upload_id = response['UploadId']
    s = ''
    parts = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i+1
        s += part
        lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
        client.meta.events.register('before-call.s3.UploadPart', lf)
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})
        if i in resend_parts:
            lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
            client.meta.events.register('before-call.s3.UploadPart', lf)
            client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)

    return (upload_id, s, parts)

def _check_content_using_range_enc(client, bucket_name, key, data, size, step, enc_headers=None):
    for ofs in range(0, size, step):
        toread = size - ofs
        if toread > step:
            toread = step
        end = ofs + toread - 1
        lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
        client.meta.events.register('before-call.s3.GetObject', lf)
        r = 'bytes={s}-{e}'.format(s=ofs, e=end)
        response = client.get_object(Bucket=bucket_name, Key=key, Range=r)
        read_range = response['ContentLength']
        body = _get_body(response)
        assert read_range == toread
        assert body == data[ofs:end+1]

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    partlen = 5*1024*1024
    metadata = {'foo': 'bar'}
    enc_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, objlen,
            part_size=partlen, init_headers=enc_headers, part_headers=enc_headers, metadata=metadata, resend_parts=resend_parts)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
    client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response['Contents']) == 1
    assert response['Contents'][0]['Size'] == objlen

    lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response['Metadata'] == metadata
    assert response['ResponseMetadata']['HTTPHeaders']['content-type'] == content_type

    body = _get_body(response)
    assert body == data
    size = response['ContentLength']
    assert len(body) == size

    _check_content_using_range_enc(client, bucket_name, key, data, size, 1000000, enc_headers=enc_headers)
    _check_content_using_range_enc(client, bucket_name, key, data, size, 10000000, enc_headers=enc_headers)
    for i in range(-1,2):
        _check_content_using_range_enc(client, bucket_name, key, data, size, partlen + i, enc_headers=enc_headers)

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_unaligned_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    partlen = 1 + 5 * 1024 * 1024 # not a multiple of the 4k encryption block size
    metadata = {'foo': 'bar'}
    enc_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, objlen,
            part_size=partlen, init_headers=enc_headers, part_headers=enc_headers, metadata=metadata, resend_parts=resend_parts)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
    client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response['Contents']) == 1
    assert response['Contents'][0]['Size'] == objlen

    lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response['Metadata'] == metadata
    assert response['ResponseMetadata']['HTTPHeaders']['content-type'] == content_type

    body = _get_body(response)
    assert body == data
    size = response['ContentLength']
    assert len(body) == size

    _check_content_using_range_enc(client, bucket_name, key, data, size, 1000000, enc_headers=enc_headers)
    _check_content_using_range_enc(client, bucket_name, key, data, size, 10000000, enc_headers=enc_headers)
    for i in range(-1,2):
        _check_content_using_range_enc(client, bucket_name, key, data, size, partlen + i, enc_headers=enc_headers)

@pytest.mark.encryption
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
def test_encryption_sse_c_multipart_invalid_chunks_1():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    init_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }
    resend_parts = []

    e = assert_raises(ClientError, _multipart_upload_enc, client=client,  bucket_name=bucket_name,
            key=key, size=objlen, part_size=5*1024*1024, init_headers=init_headers, part_headers=part_headers, metadata=metadata, resend_parts=resend_parts)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
def test_encryption_sse_c_multipart_invalid_chunks_2():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    init_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'AAAAAAAAAAAAAAAAAAAAAA=='
    }
    resend_parts = []

    e = assert_raises(ClientError, _multipart_upload_enc, client=client,  bucket_name=bucket_name,
            key=key, size=objlen, part_size=5*1024*1024, init_headers=init_headers, part_headers=part_headers, metadata=metadata, resend_parts=resend_parts)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
def test_encryption_sse_c_multipart_bad_download():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    put_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
        'Content-Type': content_type
    }
    get_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
        'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, objlen,
            part_size=5*1024*1024, init_headers=put_headers, part_headers=put_headers, metadata=metadata, resend_parts=resend_parts)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(put_headers))
    client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response['Contents']) == 1
    assert response['Contents'][0]['Size'] == objlen

    lf = (lambda **kwargs: kwargs['params']['headers'].update(put_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response['Metadata'] == metadata
    assert response['ResponseMetadata']['HTTPHeaders']['content-type'] == content_type

    lf = (lambda **kwargs: kwargs['params']['headers'].update(get_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_post_object_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["starts-with", "$x-amz-server-side-encryption-customer-algorithm", ""], \
    ["starts-with", "$x-amz-server-side-encryption-customer-key", ""], \
    ["starts-with", "$x-amz-server-side-encryption-customer-key-md5", ""], \
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),
    ('x-amz-server-side-encryption-customer-algorithm', 'AES256'), \
    ('x-amz-server-side-encryption-customer-key', 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs='), \
    ('x-amz-server-side-encryption-customer-key-md5', 'DWygnHRtgiJ77HCm+1rvHw=='), \
    ('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

    get_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    lf = (lambda **kwargs: kwargs['params']['headers'].update(get_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = _get_body(response)
    assert body == 'bar'


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_enforced_with_bucket_policy():
    bucket_name = get_new_bucket()
    client = get_client()

    deny_unencrypted_obj = {
        "Null" : {
          "s3:x-amz-server-side-encryption-customer-algorithm": "true"
        }
    }

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s = Statement("s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj)
    policy_document = p.add_statement(s).to_json()

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    check_access_denied(client.put_object, Bucket=bucket_name, Key='foo', Body='bar')

    client.put_object(
        Bucket=bucket_name, Key='foo', Body='bar',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        SSECustomerKeyMD5='DWygnHRtgiJ77HCm+1rvHw=='
    )


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_encryption_sse_c_deny_algo_with_bucket_policy():
    bucket_name = get_new_bucket()
    client = get_client()

    deny_incorrect_algo = {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption-customer-algorithm": "AES256"
        }
    }

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s = Statement("s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo)
    policy_document = p.add_statement(s).to_json()

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    check_access_denied(client.put_object, Bucket=bucket_name, Key='foo', SSECustomerAlgorithm='AES192')

    client.put_object(
        Bucket=bucket_name, Key='foo', Body='bar',
        SSECustomerAlgorithm='AES256',
        SSECustomerKey='pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        SSECustomerKeyMD5='DWygnHRtgiJ77HCm+1rvHw=='
    )


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def _test_sse_kms_customer_write(file_size, key_id = 'testkey-1'):
    """
    Tests Create a file of A's, use it to set_contents_from_file.
    Create a file of B's, use it to re-set_contents_from_file.
    Re-read the contents, and confirm we get B's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': key_id
    }
    data = 'A'*file_size

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key='testobj', Body=data)

    response = client.get_object(Bucket=bucket_name, Key='testobj')
    body = _get_body(response)
    assert body == data


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_method_head():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid
    }
    data = 'A'*1000
    key = 'testobj'

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'aws:kms'
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id'] == kms_keyid

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.HeadObject', lf)
    e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_present():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid
    }
    data = 'A'*100
    key = 'testobj'

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == data

@pytest.mark.encryption
def test_sse_kms_no_key():
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
    }
    data = 'A'*100
    key = 'testobj'

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)


@pytest.mark.encryption
def test_sse_kms_not_declared():
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-2'
    }
    data = 'A'*100
    key = 'testobj'

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_multipart_upload():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    enc_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'Content-Type': content_type
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, objlen,
            part_size=5*1024*1024, init_headers=enc_headers, part_headers=enc_headers, metadata=metadata, resend_parts=resend_parts)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
    client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response['Contents']) == 1
    assert response['Contents'][0]['Size'] == objlen

    lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
    client.meta.events.register('before-call.s3.UploadPart', lf)

    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response['Metadata'] == metadata
    assert response['ResponseMetadata']['HTTPHeaders']['content-type'] == content_type

    body = _get_body(response)
    assert body == data
    size = response['ContentLength']
    assert len(body) == size

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_multipart_invalid_chunks_1():
    kms_keyid = get_main_kms_keyid()
    kms_keyid2 = get_secondary_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/bla'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    init_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid2
    }
    resend_parts = []

    _multipart_upload_enc(client, bucket_name, key, objlen, part_size=5*1024*1024,
            init_headers=init_headers, part_headers=part_headers, metadata=metadata,
            resend_parts=resend_parts)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_multipart_invalid_chunks_2():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()
    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    init_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'Content-Type': content_type
    }
    part_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-not-present'
    }
    resend_parts = []

    _multipart_upload_enc(client, bucket_name, key, objlen, part_size=5*1024*1024,
            init_headers=init_headers, part_headers=part_headers, metadata=metadata,
            resend_parts=resend_parts)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_post_object_authenticated_request():
    kms_keyid = get_main_kms_keyid()
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["starts-with", "$x-amz-server-side-encryption", ""], \
    ["starts-with", "$x-amz-server-side-encryption-aws-kms-key-id", ""], \
    ["content-length-range", 0, 1024]\
    ]\
    }


    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),
    ('x-amz-server-side-encryption', 'aws:kms'), \
    ('x-amz-server-side-encryption-aws-kms-key-id', kms_keyid), \
    ('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = _get_body(response)
    assert body == 'bar'

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_1b():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip('[s3 main] section missing kms_keyid')
    _test_sse_kms_customer_write(1, key_id = kms_keyid)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_1kb():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip('[s3 main] section missing kms_keyid')
    _test_sse_kms_customer_write(1024, key_id = kms_keyid)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_1MB():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip('[s3 main] section missing kms_keyid')
    _test_sse_kms_customer_write(1024*1024, key_id = kms_keyid)


@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_transfer_13b():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip('[s3 main] section missing kms_keyid')
    _test_sse_kms_customer_write(13, key_id = kms_keyid)


@pytest.mark.encryption
def test_sse_kms_read_declare():
    bucket_name = get_new_bucket()
    client = get_client()
    sse_kms_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1'
    }
    data = 'A'*100
    key = 'testobj'

    client.put_object(Bucket=bucket_name, Key=key, Body=data)
    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)

    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.bucket_policy
def test_bucket_policy():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    response = alt_client.list_objects(Bucket=bucket_name)
    assert len(response['Contents']) == 1

@pytest.mark.bucket_policy
@pytest.mark.list_objects_v2
def test_bucketv2_policy():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    response = alt_client.list_objects_v2(Bucket=bucket_name)
    assert len(response['Contents']) == 1

@pytest.mark.bucket_policy
@pytest.mark.iam_account
@pytest.mark.iam_user
def test_bucket_policy_deny_self_denied_policy(iam_root):
    root_client = get_iam_root_client(service_name="s3")
    bucket_name = get_new_bucket(root_client)

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Deny",
        "Principal": "*",
        "Action": [
            "s3:PutBucketPolicy",
            "s3:GetBucketPolicy",
            "s3:DeleteBucketPolicy",
        ],
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    root_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    # non-root account should not be able to get, put or delete bucket policy
    root_alt_client = create_iam_user_s3client(iam_root)
    check_access_denied(root_alt_client.get_bucket_policy, Bucket=bucket_name)
    check_access_denied(root_alt_client.delete_bucket_policy, Bucket=bucket_name)
    check_access_denied(root_alt_client.put_bucket_policy, Bucket=bucket_name, Policy=policy_document)

    # root account should be able to get, put or delete bucket policy
    response = root_client.get_bucket_policy(Bucket=bucket_name)
    assert response['Policy'] == policy_document
    root_client.delete_bucket_policy(Bucket=bucket_name)
    root_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

@pytest.mark.bucket_policy
@pytest.mark.iam_account
@pytest.mark.iam_user
def test_bucket_policy_deny_self_denied_policy_confirm_header(iam_root):
    root_client = get_iam_root_client(service_name="s3")
    bucket_name = get_new_bucket(root_client)

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Deny",
        "Principal": "*",
        "Action": [
            "s3:PutBucketPolicy",
            "s3:GetBucketPolicy",
            "s3:DeleteBucketPolicy",
        ],
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    root_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document, ConfirmRemoveSelfBucketAccess=True)

    # non-root account should not be able to get, put or delete bucket policy
    root_alt_client = create_iam_user_s3client(iam_root)
    check_access_denied(root_alt_client.get_bucket_policy, Bucket=bucket_name)
    check_access_denied(root_alt_client.delete_bucket_policy, Bucket=bucket_name)
    check_access_denied(root_alt_client.put_bucket_policy, Bucket=bucket_name, Policy=policy_document)

    # root account should not be able to get, put or delete bucket policy
    check_access_denied(root_client.get_bucket_policy, Bucket=bucket_name)
    check_access_denied(root_client.delete_bucket_policy, Bucket=bucket_name)
    check_access_denied(root_client.put_bucket_policy, Bucket=bucket_name, Policy=policy_document)

@pytest.mark.bucket_policy
def test_bucket_policy_acl():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document =  json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Deny",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    client.put_bucket_acl(Bucket=bucket_name, ACL='authenticated-read')
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    e = assert_raises(ClientError, alt_client.list_objects, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    client.delete_bucket_policy(Bucket=bucket_name)
    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

@pytest.mark.bucket_policy
@pytest.mark.list_objects_v2
def test_bucketv2_policy_acl():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document =  json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Deny",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    client.put_bucket_acl(Bucket=bucket_name, ACL='authenticated-read')
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    e = assert_raises(ClientError, alt_client.list_objects_v2, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    client.delete_bucket_policy(Bucket=bucket_name)
    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

@pytest.mark.bucket_policy
def test_bucket_policy_different_tenant():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    # use the tenanted client to list the global tenant's bucket
    tenant_client = get_tenant_client()
    tenant_client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)
    response = tenant_client.list_objects(Bucket=":{}".format(bucket_name))

    assert len(response['Contents']) == 1

@pytest.mark.bucket_policy
def test_bucket_policy_multipart():
    client = get_client()
    alt_client = get_alt_client()
    bucket_name = get_new_bucket(client)
    key = 'mpobj'

    # alt user has no permission
    assert_raises(ClientError, alt_client.create_multipart_upload, Bucket=bucket_name, Key=key)

    # grant permission on bucket ARN but not objects
    client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": f"arn:aws:s3:::{bucket_name}"
            }]
        }))
    assert_raises(ClientError, alt_client.create_multipart_upload, Bucket=bucket_name, Key=key)

    # grant permission on object ARN
    client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:PutObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/{key}"
            }]
        }))
    alt_client.create_multipart_upload(Bucket=bucket_name, Key=key)

@pytest.mark.bucket_policy
def test_bucket_policy_tenanted_bucket():
    tenant_client = get_tenant_client()
    bucket_name = get_new_bucket(tenant_client)
    key = 'asdf'
    tenant_client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    tenant_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    tenant = get_tenant_name()

    # use the global tenant's client to list the tenanted bucket
    client = get_client()
    client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)

    response = client.list_objects(Bucket="{}:{}".format(tenant, bucket_name))
    assert len(response['Contents']) == 1

@pytest.mark.bucket_policy
def test_bucket_policy_another_bucket():
    bucket_name = get_new_bucket()
    bucket_name2 = get_new_bucket()
    client = get_client()
    key = 'asdf'
    key2 = 'abcd'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')
    client.put_object(Bucket=bucket_name2, Key=key2, Body='abcd')
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "arn:aws:s3:::*",
            "arn:aws:s3:::*/*"
          ]
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    response = client.get_bucket_policy(Bucket=bucket_name)
    response_policy = response['Policy']

    client.put_bucket_policy(Bucket=bucket_name2, Policy=response_policy)

    alt_client = get_alt_client()
    response = alt_client.list_objects(Bucket=bucket_name)
    assert len(response['Contents']) == 1

    alt_client = get_alt_client()
    response = alt_client.list_objects(Bucket=bucket_name2)
    assert len(response['Contents']) == 1

@pytest.mark.bucket_policy
@pytest.mark.list_objects_v2
def test_bucketv2_policy_another_bucket():
    bucket_name = get_new_bucket()
    bucket_name2 = get_new_bucket()
    client = get_client()
    key = 'asdf'
    key2 = 'abcd'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')
    client.put_object(Bucket=bucket_name2, Key=key2, Body='abcd')
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "arn:aws:s3:::*",
            "arn:aws:s3:::*/*"
          ]
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    response = client.get_bucket_policy(Bucket=bucket_name)
    response_policy = response['Policy']

    client.put_bucket_policy(Bucket=bucket_name2, Policy=response_policy)

    alt_client = get_alt_client()
    response = alt_client.list_objects_v2(Bucket=bucket_name)
    assert len(response['Contents']) == 1

    alt_client = get_alt_client()
    response = alt_client.list_objects_v2(Bucket=bucket_name2)
    assert len(response['Contents']) == 1

@pytest.mark.bucket_policy
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
def test_bucket_policy_set_condition_operator_end_with_IfExists():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'foo'
    client.put_object(Bucket=bucket_name, Key=key)
    policy = '''{
      "Version":"2012-10-17",
      "Statement": [{
        "Sid": "Allow Public Access to All Objects",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Condition": {
                    "StringLikeIfExists": {
                        "aws:Referer": "http://www.example.com/*"
                    }
                },
        "Resource": "arn:aws:s3:::%s/*"
      }
     ]
    }''' % bucket_name
    # boto3.set_stream_logger(name='botocore')
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy)

    request_headers={'referer': 'http://www.example.com/'}

    lf = (lambda **kwargs: kwargs['params']['headers'].update(request_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    request_headers={'referer': 'http://www.example.com/index.html'}

    lf = (lambda **kwargs: kwargs['params']['headers'].update(request_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)

    response = client.get_object(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # the 'referer' headers need to be removed for this one
    #response = client.get_object(Bucket=bucket_name, Key=key)
    #assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    request_headers={'referer': 'http://example.com'}

    lf = (lambda **kwargs: kwargs['params']['headers'].update(request_headers))
    client.meta.events.register('before-call.s3.GetObject', lf)

    # TODO: Compare Requests sent in Boto3, Wireshark, RGW Log for both boto and boto3
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    response =  client.get_bucket_policy(Bucket=bucket_name)
    print(response)

@pytest.mark.bucket_policy
def test_set_get_del_bucket_policy():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'asdf'
    client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    response = client.get_bucket_policy(Bucket=bucket_name)
    response_policy = response['Policy']
    assert policy_document == response_policy

    client.delete_bucket_policy(Bucket=bucket_name)

    try:
        response = client.get_bucket_policy(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        print (e)
        assert e.response['Error']['Code'] == 'NoSuchBucketPolicy'

def _create_simple_tagset(count):
    tagset = []
    for i in range(count):
        tagset.append({'Key': str(i), 'Value': str(i)})

    return {'TagSet': tagset}

def _make_random_string(size):
    return ''.join(random.choice(string.ascii_letters) for _ in range(size))

@pytest.mark.tagging
def test_set_multipart_tagging():
  bucket_name = get_new_bucket()
  client = get_client()
  tags='Hello=World&foo=bar'
  key = "mymultipart"
  objlen = 1

  (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, tagging=tags)
  response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
  assert response['ResponseMetadata']['HTTPStatusCode'] == 200

  response = client.get_object_tagging(Bucket=bucket_name, Key=key)
  assert len(response['TagSet']) == 2
  assert response['TagSet'][0]['Key'] == 'Hello'
  assert response['TagSet'][0]['Value'] == 'World'
  assert response['TagSet'][1]['Key'] == 'foo'
  assert response['TagSet'][1]['Value'] == 'bar'

  response = client.delete_object_tagging(Bucket=bucket_name, Key=key)
  assert response['ResponseMetadata']['HTTPStatusCode'] == 204

  response = client.get_object_tagging(Bucket=bucket_name, Key=key)
  assert len(response['TagSet']) == 0

@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_get_obj_tagging():
    key = 'testputtags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(2)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response['TagSet'] == input_tagset['TagSet']


@pytest.mark.tagging
def test_get_obj_head_tagging():
    key = 'testputtags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()
    count = 2

    input_tagset = _create_simple_tagset(count)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-tagging-count'] == str(count)

@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_max_tags():
    key = 'testputmaxtags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(10)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response['TagSet'] == input_tagset['TagSet']

@pytest.mark.tagging
def test_put_excess_tags():
    key = 'testputmaxtags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(11)
    e = assert_raises(ClientError, client.put_object_tagging, Bucket=bucket_name, Key=key, Tagging=input_tagset)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidTag'

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response['TagSet']) == 0

@pytest.mark.tagging
def test_put_max_kvsize_tags():
    key = 'testputmaxkeysize'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    for i in range(10):
        k = _make_random_string(128)
        v = _make_random_string(256)
        tagset.append({'Key': k, 'Value': v})

    input_tagset = {'TagSet': tagset}

    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    for kv_pair in response['TagSet']:
        assert kv_pair in input_tagset['TagSet']

@pytest.mark.tagging
def test_put_excess_key_tags():
    key = 'testputexcesskeytags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    for i in range(10):
        k = _make_random_string(129)
        v = _make_random_string(256)
        tagset.append({'Key': k, 'Value': v})

    input_tagset = {'TagSet': tagset}

    e = assert_raises(ClientError, client.put_object_tagging, Bucket=bucket_name, Key=key, Tagging=input_tagset)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidTag'

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response['TagSet']) == 0

@pytest.mark.tagging
def test_put_excess_val_tags():
    key = 'testputexcesskeytags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    for i in range(10):
        k = _make_random_string(128)
        v = _make_random_string(257)
        tagset.append({'Key': k, 'Value': v})

    input_tagset = {'TagSet': tagset}

    e = assert_raises(ClientError, client.put_object_tagging, Bucket=bucket_name, Key=key, Tagging=input_tagset)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidTag'

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response['TagSet']) == 0

@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_modify_tags():
    key = 'testputmodifytags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    tagset = []
    tagset.append({'Key': 'key', 'Value': 'val'})
    tagset.append({'Key': 'key2', 'Value': 'val2'})

    input_tagset = {'TagSet': tagset}

    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response['TagSet'] == input_tagset['TagSet']

    tagset2 = []
    tagset2.append({'Key': 'key3', 'Value': 'val3'})

    input_tagset2 = {'TagSet': tagset2}

    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset2)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response['TagSet'] == input_tagset2['TagSet']

@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_delete_tags():
    key = 'testputmodifytags'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    input_tagset = _create_simple_tagset(2)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response['TagSet'] == input_tagset['TagSet']

    response = client.delete_object_tagging(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response['TagSet']) == 0

@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_post_object_tags_anonymous_request():
    bucket_name = get_new_bucket_name()
    client = get_client()
    url = _get_post_url(bucket_name)
    client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    key_name = "foo.txt"
    input_tagset = _create_simple_tagset(2)
    # xml_input_tagset is the same as input_tagset in xml.
    # There is not a simple way to change input_tagset to xml like there is in the boto2 tetss
    xml_input_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"


    payload = OrderedDict([
        ("key" , key_name),
        ("acl" , "public-read"),
        ("Content-Type" , "text/plain"),
        ("tagging", xml_input_tagset),
        ('file', ('bar')),
    ])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key=key_name)
    body = _get_body(response)
    assert body == 'bar'

    response = client.get_object_tagging(Bucket=bucket_name, Key=key_name)
    assert response['TagSet'] == input_tagset['TagSet']

@pytest.mark.tagging
def test_post_object_tags_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [
    {"bucket": bucket_name},
        ["starts-with", "$key", "foo"],
        {"acl": "private"},
        ["starts-with", "$Content-Type", "text/plain"],
        ["content-length-range", 0, 1024],
        ["starts-with", "$tagging", ""]
    ]}

    # xml_input_tagset is the same as `input_tagset = _create_simple_tagset(2)` in xml
    # There is not a simple way to change input_tagset to xml like there is in the boto2 tetss
    xml_input_tagset = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([
        ("key" , "foo.txt"),
        ("AWSAccessKeyId" , aws_access_key_id),\
        ("acl" , "private"),("signature" , signature),("policy" , policy),\
        ("tagging", xml_input_tagset),
        ("Content-Type" , "text/plain"),
        ('file', ('bar'))])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204
    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    body = _get_body(response)
    assert body == 'bar'


@pytest.mark.tagging
@pytest.mark.fails_on_dbstore
def test_put_obj_with_tags():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'testtagobj1'
    data = 'A'*100

    tagset = []
    tagset.append({'Key': 'bar', 'Value': ''})
    tagset.append({'Key': 'foo', 'Value': 'bar'})

    put_obj_tag_headers = {
        'x-amz-tagging' : 'foo=bar&bar'
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(put_obj_tag_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)

    client.put_object(Bucket=bucket_name, Key=key, Body=data)
    response = client.get_object(Bucket=bucket_name, Key=key)
    body = _get_body(response)
    assert body == data

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    response_tagset = response['TagSet']
    tagset = tagset
    assert response_tagset == tagset

def _make_arn_resource(path="*"):
    return "arn:aws:s3:::{}".format(path)

@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_get_tags_acl_public():
    key = 'testputtagsacl'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    resource = _make_arn_resource("{}/{}".format(bucket_name, key))
    policy_document = make_json_policy("s3:GetObjectTagging",
                                       resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    input_tagset = _create_simple_tagset(10)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    alt_client = get_alt_client()

    response = alt_client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response['TagSet'] == input_tagset['TagSet']

@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_put_tags_acl_public():
    key = 'testputtagsacl'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    resource = _make_arn_resource("{}/{}".format(bucket_name, key))
    policy_document = make_json_policy("s3:PutObjectTagging",
                                       resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    input_tagset = _create_simple_tagset(10)
    alt_client = get_alt_client()
    response = alt_client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert response['TagSet'] == input_tagset['TagSet']

@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_delete_tags_obj_public():
    key = 'testputtagsacl'
    bucket_name = _create_key_with_random_content(key)
    client = get_client()

    resource = _make_arn_resource("{}/{}".format(bucket_name, key))
    policy_document = make_json_policy("s3:DeleteObjectTagging",
                                       resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    input_tagset = _create_simple_tagset(10)
    response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    alt_client = get_alt_client()

    response = alt_client.delete_object_tagging(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response = client.get_object_tagging(Bucket=bucket_name, Key=key)
    assert len(response['TagSet']) == 0

def test_versioning_bucket_atomic_upload_return_version_id():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'bar'

    # for versioning-enabled-bucket, an non-empty version-id should return
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    response = client.put_object(Bucket=bucket_name, Key=key)
    version_id = response['VersionId']

    response  = client.list_object_versions(Bucket=bucket_name)
    versions = response['Versions']
    for version in versions:
        assert version['VersionId'] == version_id


    # for versioning-default-bucket, no version-id should return.
    bucket_name = get_new_bucket()
    key = 'baz'
    response = client.put_object(Bucket=bucket_name, Key=key)
    assert not 'VersionId' in response

    # for versioning-suspended-bucket, no version-id should return.
    bucket_name = get_new_bucket()
    key = 'baz'
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")
    response = client.put_object(Bucket=bucket_name, Key=key)
    assert not 'VersionId' in response

def test_versioning_bucket_multipart_upload_return_version_id():
    content_type='text/bla'
    objlen = 30 * 1024 * 1024

    bucket_name = get_new_bucket()
    client = get_client()
    key = 'bar'
    metadata={'foo': 'baz'}

    # for versioning-enabled-bucket, an non-empty version-id should return
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")

    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client, content_type=content_type, metadata=metadata)

    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    version_id = response['VersionId']

    response  = client.list_object_versions(Bucket=bucket_name)
    versions = response['Versions']
    for version in versions:
        assert version['VersionId'] == version_id

    # for versioning-default-bucket, no version-id should return.
    bucket_name = get_new_bucket()
    key = 'baz'

    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client, content_type=content_type, metadata=metadata)

    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    assert not 'VersionId' in response

    # for versioning-suspended-bucket, no version-id should return
    bucket_name = get_new_bucket()
    key = 'foo'
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")

    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client, content_type=content_type, metadata=metadata)

    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    assert not 'VersionId' in response

@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_get_obj_existing_tag():
    bucket_name = _create_objects(keys=['publictag', 'privatetag', 'invalidtag'])
    client = get_client()

    tag_conditional = {"StringEquals": {
        "s3:ExistingObjectTag/security" : "public"
    }}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject",
                                       resource,
                                       conditions=tag_conditional)


    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({'Key': 'security', 'Value': 'public'})
    tagset.append({'Key': 'foo', 'Value': 'bar'})

    input_tagset = {'TagSet': tagset}

    response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    tagset2 = []
    tagset2.append({'Key': 'security', 'Value': 'private'})

    input_tagset = {'TagSet': tagset2}

    response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    tagset3 = []
    tagset3.append({'Key': 'security1', 'Value': 'public'})

    input_tagset = {'TagSet': tagset3}

    response = client.put_object_tagging(Bucket=bucket_name, Key='invalidtag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=bucket_name, Key='publictag')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='privatetag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='invalidtag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_get_obj_tagging_existing_tag():
    bucket_name = _create_objects(keys=['publictag', 'privatetag', 'invalidtag'])
    client = get_client()

    tag_conditional = {"StringEquals": {
        "s3:ExistingObjectTag/security" : "public"
    }}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObjectTagging",
                                       resource,
                                       conditions=tag_conditional)


    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({'Key': 'security', 'Value': 'public'})
    tagset.append({'Key': 'foo', 'Value': 'bar'})

    input_tagset = {'TagSet': tagset}

    response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    tagset2 = []
    tagset2.append({'Key': 'security', 'Value': 'private'})

    input_tagset = {'TagSet': tagset2}

    response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    tagset3 = []
    tagset3.append({'Key': 'security1', 'Value': 'public'})

    input_tagset = {'TagSet': tagset3}

    response = client.put_object_tagging(Bucket=bucket_name, Key='invalidtag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    alt_client = get_alt_client()
    response = alt_client.get_object_tagging(Bucket=bucket_name, Key='publictag')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # A get object itself should fail since we allowed only GetObjectTagging
    e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='publictag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='privatetag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


    e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='invalidtag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_tagging_existing_tag():
    bucket_name = _create_objects(keys=['publictag', 'privatetag', 'invalidtag'])
    client = get_client()

    tag_conditional = {"StringEquals": {
        "s3:ExistingObjectTag/security" : "public"
    }}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:PutObjectTagging",
                                       resource,
                                       conditions=tag_conditional)


    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({'Key': 'security', 'Value': 'public'})
    tagset.append({'Key': 'foo', 'Value': 'bar'})

    input_tagset = {'TagSet': tagset}

    response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    tagset2 = []
    tagset2.append({'Key': 'security', 'Value': 'private'})

    input_tagset = {'TagSet': tagset2}

    response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    alt_client = get_alt_client()
    # PUT requests with object tagging are a bit wierd, if you forget to put
    # the tag which is supposed to be existing anymore well, well subsequent
    # put requests will fail

    testtagset1 = []
    testtagset1.append({'Key': 'security', 'Value': 'public'})
    testtagset1.append({'Key': 'foo', 'Value': 'bar'})

    input_tagset = {'TagSet': testtagset1}

    response = alt_client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    e = assert_raises(ClientError, alt_client.put_object_tagging, Bucket=bucket_name, Key='privatetag', Tagging=input_tagset)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    testtagset2 = []
    testtagset2.append({'Key': 'security', 'Value': 'private'})

    input_tagset = {'TagSet': testtagset2}

    response = alt_client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # Now try putting the original tags again, this should fail
    input_tagset = {'TagSet': testtagset1}

    e = assert_raises(ClientError, alt_client.put_object_tagging, Bucket=bucket_name, Key='publictag', Tagging=input_tagset)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_upload_part_copy():
    bucket_name = _create_objects(keys=['public/foo', 'public/bar', 'private/foo'])
    client = get_client()

    src_resource = _make_arn_resource("{}/{}".format(bucket_name, "public/*"))
    policy_document = make_json_policy("s3:GetObject", src_resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    bucket_name2 = get_new_bucket(alt_client)

    copy_source = {'Bucket': bucket_name, 'Key': 'public/foo'}

    # Create a multipart upload
    response = alt_client.create_multipart_upload(Bucket=bucket_name2, Key='new_foo')
    upload_id = response['UploadId']
    # Upload a part
    response = alt_client.upload_part_copy(Bucket=bucket_name2, Key='new_foo', PartNumber=1, UploadId=upload_id, CopySource=copy_source)
    # Complete the multipart upload
    response = alt_client.complete_multipart_upload(
        Bucket=bucket_name2, Key='new_foo', UploadId=upload_id,
        MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': response['CopyPartResult']['ETag']}]},
    )

    response = alt_client.get_object(Bucket=bucket_name2, Key='new_foo')
    body = _get_body(response)
    assert body == 'public/foo'

    copy_source = {'Bucket': bucket_name, 'Key': 'public/bar'}
    # Create a multipart upload
    response = alt_client.create_multipart_upload(Bucket=bucket_name2, Key='new_foo2')
    upload_id = response['UploadId']
    # Upload a part
    response = alt_client.upload_part_copy(Bucket=bucket_name2, Key='new_foo2', PartNumber=1, UploadId=upload_id, CopySource=copy_source)
    # Complete the multipart upload
    response = alt_client.complete_multipart_upload(
        Bucket=bucket_name2, Key='new_foo2', UploadId=upload_id,
        MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': response['CopyPartResult']['ETag']}]},
    )

    response = alt_client.get_object(Bucket=bucket_name2, Key='new_foo2')
    body = _get_body(response)
    assert body == 'public/bar'

    copy_source = {'Bucket': bucket_name, 'Key': 'private/foo'}
    # Create a multipart upload
    response = alt_client.create_multipart_upload(Bucket=bucket_name2, Key='new_foo2')
    upload_id = response['UploadId']
    # Upload a part
    check_access_denied(alt_client.upload_part_copy, Bucket=bucket_name2, Key='new_foo2', PartNumber=1, UploadId=upload_id, CopySource=copy_source)
    # Abort the multipart upload
    alt_client.abort_multipart_upload(Bucket=bucket_name2, Key='new_foo2', UploadId=upload_id)


@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_copy_source():
    bucket_name = _create_objects(keys=['public/foo', 'public/bar', 'private/foo'])
    client = get_client()

    src_resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject",
                                       src_resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    bucket_name2 = get_new_bucket()

    tag_conditional = {"StringLike": {
        "s3:x-amz-copy-source" : bucket_name + "/public/*"
    }}

    resource = _make_arn_resource("{}/{}".format(bucket_name2, "*"))
    policy_document = make_json_policy("s3:PutObject",
                                       resource,
                                       conditions=tag_conditional)

    client.put_bucket_policy(Bucket=bucket_name2, Policy=policy_document)

    alt_client = get_alt_client()
    copy_source = {'Bucket': bucket_name, 'Key': 'public/foo'}

    alt_client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key='new_foo')

    # This is possible because we are still the owner, see the grants with
    # policy on how to do this right
    response = alt_client.get_object(Bucket=bucket_name2, Key='new_foo')
    body = _get_body(response)
    assert body == 'public/foo'

    copy_source = {'Bucket': bucket_name, 'Key': 'public/bar'}
    alt_client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key='new_foo2')

    response = alt_client.get_object(Bucket=bucket_name2, Key='new_foo2')
    body = _get_body(response)
    assert body == 'public/bar'

    copy_source = {'Bucket': bucket_name, 'Key': 'private/foo'}
    check_access_denied(alt_client.copy_object, Bucket=bucket_name2, CopySource=copy_source, Key='new_foo2')

@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_copy_source_meta():
    src_bucket_name = _create_objects(keys=['public/foo', 'public/bar'])
    client = get_client()

    src_resource = _make_arn_resource("{}/{}".format(src_bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject",
                                       src_resource)

    client.put_bucket_policy(Bucket=src_bucket_name, Policy=policy_document)

    bucket_name = get_new_bucket()

    tag_conditional = {"StringEquals": {
        "s3:x-amz-metadata-directive" : "COPY"
    }}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:PutObject",
                                       resource,
                                       conditions=tag_conditional)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()

    lf = (lambda **kwargs: kwargs['params']['headers'].update({"x-amz-metadata-directive": "COPY"}))
    alt_client.meta.events.register('before-call.s3.CopyObject', lf)

    copy_source = {'Bucket': src_bucket_name, 'Key': 'public/foo'}
    alt_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='new_foo')

    # This is possible because we are still the owner, see the grants with
    # policy on how to do this right
    response = alt_client.get_object(Bucket=bucket_name, Key='new_foo')
    body = _get_body(response)
    assert body == 'public/foo'

    # remove the x-amz-metadata-directive header
    def remove_header(**kwargs):
        if ("x-amz-metadata-directive" in kwargs['params']['headers']):
            del kwargs['params']['headers']["x-amz-metadata-directive"]

    alt_client.meta.events.register('before-call.s3.CopyObject', remove_header)

    copy_source = {'Bucket': src_bucket_name, 'Key': 'public/bar'}
    check_access_denied(alt_client.copy_object, Bucket=bucket_name, CopySource=copy_source, Key='new_foo2', Metadata={"foo": "bar"})


@pytest.mark.tagging
@pytest.mark.bucket_policy
def test_bucket_policy_put_obj_acl():
    bucket_name = get_new_bucket()
    client = get_client()

    # An allow conditional will require atleast the presence of an x-amz-acl
    # attribute a Deny conditional would negate any requests that try to set a
    # public-read/write acl
    conditional = {"StringLike": {
        "s3:x-amz-acl" : "public*"
    }}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    s1 = Statement("s3:PutObject",resource)
    s2 = Statement("s3:PutObject", resource, effect="Deny", condition=conditional)

    policy_document = p.add_statement(s1).add_statement(s2).to_json()
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    key1 = 'private-key'

    # if we want to be really pedantic, we should check that this doesn't raise
    # and mark a failure, however if this does raise nosetests would mark this
    # as an ERROR anyway
    response = alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1)
    #response = alt_client.put_object_acl(Bucket=bucket_name, Key=key1, ACL='private')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    key2 = 'public-key'

    lf = (lambda **kwargs: kwargs['params']['headers'].update({"x-amz-acl": "public-read"}))
    alt_client.meta.events.register('before-call.s3.PutObject', lf)

    e = assert_raises(ClientError, alt_client.put_object, Bucket=bucket_name, Key=key2, Body=key2)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.bucket_policy
def test_bucket_policy_put_obj_grant():

    bucket_name = get_new_bucket()
    bucket_name2 = get_new_bucket()
    client = get_client()

    # In normal cases a key owner would be the uploader of a key in first case
    # we explicitly require that the bucket owner is granted full control over
    # the object uploaded by any user, the second bucket is where no such
    # policy is enforced meaning that the uploader still retains ownership

    main_user_id = get_main_user_id()
    alt_user_id = get_alt_user_id()

    owner_id_str = "id=" + main_user_id
    s3_conditional = {"StringEquals": {
        "s3:x-amz-grant-full-control" : owner_id_str
    }}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:PutObject",
                                       resource,
                                       conditions=s3_conditional)

    resource = _make_arn_resource("{}/{}".format(bucket_name2, "*"))
    policy_document2 = make_json_policy("s3:PutObject", resource)

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    client.put_bucket_policy(Bucket=bucket_name2, Policy=policy_document2)

    alt_client = get_alt_client()
    key1 = 'key1'

    lf = (lambda **kwargs: kwargs['params']['headers'].update({"x-amz-grant-full-control" : owner_id_str}))
    alt_client.meta.events.register('before-call.s3.PutObject', lf)

    response = alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    def remove_header(**kwargs):
        if ("x-amz-grant-full-control" in kwargs['params']['headers']):
            del kwargs['params']['headers']["x-amz-grant-full-control"]

    alt_client.meta.events.register('before-call.s3.PutObject', remove_header)

    key2 = 'key2'
    response = alt_client.put_object(Bucket=bucket_name2, Key=key2, Body=key2)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    acl1_response = client.get_object_acl(Bucket=bucket_name, Key=key1)

    # user 1 is trying to get acl for the object from user2 where ownership
    # wasn't transferred
    check_access_denied(client.get_object_acl, Bucket=bucket_name2, Key=key2)

    acl2_response = alt_client.get_object_acl(Bucket=bucket_name2, Key=key2)

    assert acl1_response['Grants'][0]['Grantee']['ID'] == main_user_id
    assert acl2_response['Grants'][0]['Grantee']['ID'] == alt_user_id


@pytest.mark.encryption
def test_put_obj_enc_conflict_c_s3():
    bucket_name = get_new_bucket()
    client = get_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str ='testobj'

    sse_client_headers = {
        'x-amz-server-side-encryption' : 'AES256',
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.encryption
def test_put_obj_enc_conflict_c_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = 'fool-me-once'
    bucket_name = get_new_bucket()
    client = get_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str ='testobj'

    sse_client_headers = {
        'x-amz-server-side-encryption' : 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.encryption
def test_put_obj_enc_conflict_s3_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = 'fool-me-once'
    bucket_name = get_new_bucket()
    client = get_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str ='testobj'

    sse_client_headers = {
        'x-amz-server-side-encryption' : 'AES256',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.encryption
def test_put_obj_enc_conflict_bad_enc_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = 'fool-me-once'
    bucket_name = get_new_bucket()
    client = get_client()

    # boto3.set_stream_logger(name='botocore')

    key1_str ='testobj'

    sse_client_headers = {
        'x-amz-server-side-encryption' : 'aes:kms',	# aes != aws
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key1_str)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument'

@pytest.mark.encryption
@pytest.mark.bucket_policy
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_s3_noenc():
    bucket_name = get_new_bucket()
    client = get_client()

    deny_unencrypted_obj = {
        "Null" : {
          "s3:x-amz-server-side-encryption": "true"
        }
    }

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s = Statement("s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj)
    policy_document = p.add_statement(s).to_json()

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str ='testobj'

    check_access_denied(client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str)

    response = client.put_object(Bucket=bucket_name, Key=key1_str, ServerSideEncryption='AES256')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'


@pytest.mark.encryption
@pytest.mark.bucket_policy
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_bucket_policy_put_obj_s3_incorrect_algo_sse_s3():
    bucket_name = get_new_bucket()
    client = get_client()

    deny_incorrect_algo = {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "AES256"
        }
    }

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s = Statement("s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo)
    policy_document = p.add_statement(s).to_json()

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str ='testobj'

    check_access_denied(client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str, ServerSideEncryption='AES192')

    response = client.put_object(Bucket=bucket_name, Key=key1_str, ServerSideEncryption='AES256')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'


@pytest.mark.encryption
@pytest.mark.bucket_policy
@pytest.mark.sse_s3
def test_bucket_policy_put_obj_s3_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = 'fool-me-twice'
    bucket_name = get_new_bucket()
    client = get_client()

    deny_incorrect_algo = {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "AES256"
        }
    }

    deny_unencrypted_obj = {
        "Null" : {
          "s3:x-amz-server-side-encryption": "true"
        }
    }

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo)
    s2 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj)
    policy_document = p.add_statement(s1).add_statement(s2).to_json()

    # boto3.set_stream_logger(name='botocore')

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str ='testobj'

    #response = client.get_bucket_policy(Bucket=bucket_name)
    #print response

    sse_client_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    check_access_denied(client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str)

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
@pytest.mark.bucket_policy
def test_bucket_policy_put_obj_kms_noenc():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip('[s3 main] section missing kms_keyid')
    bucket_name = get_new_bucket()
    client = get_client()

    deny_incorrect_algo = {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "aws:kms"
        }
    }

    deny_unencrypted_obj = {
        "Null" : {
          "s3:x-amz-server-side-encryption": "true"
        }
    }

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo)
    s2 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj)
    policy_document = p.add_statement(s1).add_statement(s2).to_json()

    # boto3.set_stream_logger(name='botocore')

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str ='testobj'
    key2_str ='unicorn'

    #response = client.get_bucket_policy(Bucket=bucket_name)
    #print response

    # must do check_access_denied last - otherwise, pending data
    #  breaks next call...
    response = client.put_object(Bucket=bucket_name, Key=key1_str,
         ServerSideEncryption='aws:kms', SSEKMSKeyId=kms_keyid)
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'aws:kms'
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id'] == kms_keyid

    check_access_denied(client.put_object, Bucket=bucket_name, Key=key2_str, Body=key2_str)

@pytest.mark.encryption
@pytest.mark.bucket_policy
def test_bucket_policy_put_obj_kms_s3():
    bucket_name = get_new_bucket()
    client = get_client()

    deny_incorrect_algo = {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "aws:kms"
        }
    }

    deny_unencrypted_obj = {
        "Null" : {
          "s3:x-amz-server-side-encryption": "true"
        }
    }

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo)
    s2 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj)
    policy_document = p.add_statement(s1).add_statement(s2).to_json()

    # boto3.set_stream_logger(name='botocore')

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    key1_str ='testobj'

    #response = client.get_bucket_policy(Bucket=bucket_name)
    #print response

    sse_client_headers = {
        'x-amz-server-side-encryption' : 'AES256',
    }

    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    check_access_denied(client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str)

@pytest.mark.tagging
@pytest.mark.bucket_policy
# TODO: remove this fails_on_rgw when I fix it
@pytest.mark.fails_on_rgw
def test_bucket_policy_put_obj_request_obj_tag():
    bucket_name = get_new_bucket()
    client = get_client()

    tag_conditional = {"StringEquals": {
        "s3:RequestObjectTag/security" : "public"
    }}

    p = Policy()
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    s1 = Statement("s3:PutObject", resource, effect="Allow", condition=tag_conditional)
    policy_document = p.add_statement(s1).to_json()

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    alt_client = get_alt_client()
    key1_str ='testobj'
    check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str)

    headers = {"x-amz-tagging" : "security=public"}
    lf = (lambda **kwargs: kwargs['params']['headers'].update(headers))
    client.meta.events.register('before-call.s3.PutObject', lf)
    #TODO: why is this a 400 and not passing
    alt_client.put_object(Bucket=bucket_name, Key=key1_str, Body=key1_str)

@pytest.mark.tagging
@pytest.mark.bucket_policy
@pytest.mark.fails_on_dbstore
def test_bucket_policy_get_obj_acl_existing_tag():
    bucket_name = _create_objects(keys=['publictag', 'privatetag', 'invalidtag'])
    client = get_client()

    tag_conditional = {"StringEquals": {
        "s3:ExistingObjectTag/security" : "public"
    }}

    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObjectAcl",
                                       resource,
                                       conditions=tag_conditional)


    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    tagset = []
    tagset.append({'Key': 'security', 'Value': 'public'})
    tagset.append({'Key': 'foo', 'Value': 'bar'})

    input_tagset = {'TagSet': tagset}

    response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    tagset2 = []
    tagset2.append({'Key': 'security', 'Value': 'private'})

    input_tagset = {'TagSet': tagset2}

    response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    tagset3 = []
    tagset3.append({'Key': 'security1', 'Value': 'public'})

    input_tagset = {'TagSet': tagset3}

    response = client.put_object_tagging(Bucket=bucket_name, Key='invalidtag', Tagging=input_tagset)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    alt_client = get_alt_client()
    response = alt_client.get_object_acl(Bucket=bucket_name, Key='publictag')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # A get object itself should fail since we allowed only GetObjectTagging
    e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='publictag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='privatetag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='invalidtag')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_lock():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Days':1
                }
            }}
    response = client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration=conf)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'COMPLIANCE',
                    'Years':1
                }
            }}
    response = client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration=conf)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_bucket_versioning(Bucket=bucket_name)
    assert response['Status'] == 'Enabled'


def test_object_lock_put_obj_lock_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Days':1
                }
            }}
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'InvalidBucketState'

def test_object_lock_put_obj_lock_enable_after_create():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Days':1
                }
            }}

    # fail if bucket is not versioned
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'InvalidBucketState'

    # fail if versioning is suspended
    check_configure_versioning_retry(bucket_name, "Suspended", "Suspended")
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'InvalidBucketState'

    # enable object lock if versioning is enabled
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    response = client.put_object_lock_configuration(Bucket=bucket_name, ObjectLockConfiguration=conf)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_lock_with_days_and_years():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Days':1,
                    'Years':1
                }
            }}
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_lock_invalid_days():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Days':0
                }
            }}
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidRetentionPeriod'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_lock_invalid_years():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Years':-1
                }
            }}
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidRetentionPeriod'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_lock_invalid_mode():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'abc',
                    'Years':1
                }
            }}
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'

    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'governance',
                    'Years':1
                }
            }}
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_lock_invalid_status():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Disabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Years':1
                }
            }}
    e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name, ObjectLockConfiguration=conf)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'


@pytest.mark.fails_on_dbstore
def test_object_lock_suspend_versioning():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    e = assert_raises(ClientError, client.put_bucket_versioning, Bucket=bucket_name, VersioningConfiguration={'Status': 'Suspended'})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 409
    assert error_code == 'InvalidBucketState'


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_lock():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Days':1
                }
            }}
    client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration=conf)
    response = client.get_object_lock_configuration(Bucket=bucket_name)
    assert response['ObjectLockConfiguration'] == conf


def test_object_lock_get_obj_lock_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    e = assert_raises(ClientError, client.get_object_lock_configuration, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 404
    assert error_code == 'ObjectLockConfigurationNotFoundError'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2140,1,1,tzinfo=pytz.UTC)}
    response = client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response['Retention'] == retention
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)



def test_object_lock_put_obj_retention_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidRequest'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_invalid_mode():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    retention = {'Mode':'governance', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'

    retention = {'Mode':'abc', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response['Retention'] == retention
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_retention_iso8601():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    date = datetime.datetime.today() + datetime.timedelta(days=365)
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate': date}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    client.meta.events.register('after-call.s3.HeadObject', get_http_response)
    client.head_object(Bucket=bucket_name,VersionId=version_id,Key=key)
    retain_date = http_response['headers']['x-amz-object-lock-retain-until-date']
    isodate.parse_datetime(retain_date)
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)


def test_object_lock_get_obj_retention_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    e = assert_raises(ClientError, client.get_object_retention, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidRequest'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_versionid():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, VersionId=version_id, Retention=retention)
    response = client.get_object_retention(Bucket=bucket_name, Key=key, VersionId=version_id)
    assert response['Retention'] == retention
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_override_default_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    conf = {'ObjectLockEnabled':'Enabled',
            'Rule': {
                'DefaultRetention':{
                    'Mode':'GOVERNANCE',
                    'Days':1
                }
            }}
    client.put_object_lock_configuration(
        Bucket=bucket_name,
        ObjectLockConfiguration=conf)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response['Retention'] == retention
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_increase_period():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    retention1 = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention1)
    retention2 = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,3,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention2)
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response['Retention'] == retention2
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_shorten_period():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,3,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)


@pytest.mark.fails_on_dbstore
def test_object_lock_put_obj_retention_shorten_period_bypass():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    version_id = response['VersionId']
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,3,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention, BypassGovernanceRetention=True)
    response = client.get_object_retention(Bucket=bucket_name, Key=key)
    assert response['Retention'] == retention
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)


@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'

    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'], BypassGovernanceRetention=True)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

@pytest.mark.fails_on_dbstore
def test_object_lock_delete_multipart_object_with_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    key = 'file1'
    body = 'abc'
    response = client.create_multipart_upload(Bucket=bucket_name, Key=key, ObjectLockMode='GOVERNANCE',
                                              ObjectLockRetainUntilDate=datetime.datetime(2030,1,1,tzinfo=pytz.UTC))
    upload_id = response['UploadId']

    response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=1, Body=body)
    parts = [{'ETag': response['ETag'].strip('"'), 'PartNumber': 1}]

    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'], BypassGovernanceRetention=True)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_retention_and_marker():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'

    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    del_response = client.delete_object(Bucket=bucket_name, Key=key)
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    client.delete_object(Bucket=bucket_name, Key=key, VersionId=del_response['VersionId'])
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

    response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'], BypassGovernanceRetention=True)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

@pytest.mark.fails_on_dbstore
def test_object_lock_multi_delete_object_with_retention():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key1 = 'file1'
    key2 = 'file2'

    response1 = client.put_object(Bucket=bucket_name, Body='abc', Key=key1)
    response2 = client.put_object(Bucket=bucket_name, Body='abc', Key=key2)

    versionId1 = response1['VersionId']
    versionId2 = response2['VersionId']

    # key1 is under retention, but key2 isn't.
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key1, Retention=retention)

    delete_response = client.delete_objects(
        Bucket=bucket_name,
        Delete={
            'Objects': [
                {
                    'Key': key1,
                    'VersionId': versionId1
                },
                {
                    'Key': key2,
                    'VersionId': versionId2
                }
            ]
        }
    )

    assert len(delete_response['Deleted']) == 1
    assert len(delete_response['Errors']) == 1
    
    failed_object = delete_response['Errors'][0]
    assert failed_object['Code'] == 'AccessDenied'
    assert failed_object['Key'] == key1
    assert failed_object['VersionId'] == versionId1

    deleted_object = delete_response['Deleted'][0]
    assert deleted_object['Key'] == key2
    assert deleted_object['VersionId'] == versionId2

    delete_response = client.delete_objects(
        Bucket=bucket_name,
        Delete={
            'Objects': [
                {
                    'Key': key1,
                    'VersionId': versionId1
                }
            ]
        },
        BypassGovernanceRetention=True
    )

    assert( ('Errors' not in delete_response) or (len(delete_response['Errors']) == 0) )
    assert len(delete_response['Deleted']) == 1
    deleted_object = delete_response['Deleted'][0]
    assert deleted_object['Key'] == key1
    assert deleted_object['VersionId'] == versionId1



@pytest.mark.fails_on_dbstore
def test_object_lock_put_legal_hold():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    legal_hold = {'Status': 'ON'}
    response = client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status':'OFF'})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_object_lock_put_legal_hold_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    legal_hold = {'Status': 'ON'}
    e = assert_raises(ClientError, client.put_object_legal_hold, Bucket=bucket_name, Key=key, LegalHold=legal_hold)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidRequest'


@pytest.mark.fails_on_dbstore
def test_object_lock_put_legal_hold_invalid_status():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    legal_hold = {'Status': 'abc'}
    e = assert_raises(ClientError, client.put_object_legal_hold, Bucket=bucket_name, Key=key, LegalHold=legal_hold)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'MalformedXML'


@pytest.mark.fails_on_dbstore
def test_object_lock_get_legal_hold():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    legal_hold = {'Status': 'ON'}
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
    response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
    assert response['LegalHold'] == legal_hold
    legal_hold_off = {'Status': 'OFF'}
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold_off)
    response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
    assert response['LegalHold'] == legal_hold_off


def test_object_lock_get_legal_hold_invalid_bucket():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    e = assert_raises(ClientError, client.get_object_legal_hold, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidRequest'


@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_legal_hold_on():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'ON'})
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status':'OFF'})

@pytest.mark.fails_on_dbstore
def test_object_lock_delete_multipart_object_with_legal_hold_on():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)

    key = 'file1'
    body = 'abc'
    response = client.create_multipart_upload(Bucket=bucket_name, Key=key, ObjectLockLegalHoldStatus='ON')
    upload_id = response['UploadId']

    response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=1, Body=body)
    parts = [{'ETag': response['ETag'].strip('"'), 'PartNumber': 1}]

    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status':'OFF'})

@pytest.mark.fails_on_dbstore
def test_object_lock_delete_object_with_legal_hold_off():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'OFF'})
    response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204


@pytest.mark.fails_on_dbstore
def test_object_lock_get_obj_metadata():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key)
    legal_hold = {'Status': 'ON'}
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':datetime.datetime(2030,1,1,tzinfo=pytz.UTC)}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response['ObjectLockMode'] == retention['Mode']
    assert response['ObjectLockRetainUntilDate'] == retention['RetainUntilDate']
    assert response['ObjectLockLegalHoldStatus'] == legal_hold['Status']

    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status':'OFF'})
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'], BypassGovernanceRetention=True)


@pytest.mark.fails_on_dbstore
def test_object_lock_uploading_obj():
    bucket_name = get_new_bucket_name()
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    key = 'file1'
    client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='GOVERNANCE',
                      ObjectLockRetainUntilDate=datetime.datetime(2030,1,1,tzinfo=pytz.UTC), ObjectLockLegalHoldStatus='ON')

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response['ObjectLockMode'] == 'GOVERNANCE'
    assert response['ObjectLockRetainUntilDate'] == datetime.datetime(2030,1,1,tzinfo=pytz.UTC)
    assert response['ObjectLockLegalHoldStatus'] == 'ON'
    client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status':'OFF'})
    client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'], BypassGovernanceRetention=True)

@pytest.mark.fails_on_dbstore
def test_object_lock_changing_mode_from_governance_with_bypass():
    bucket_name = get_new_bucket_name()
    key = 'file1'
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    # upload object with mode=GOVERNANCE
    retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
    client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='GOVERNANCE',
                      ObjectLockRetainUntilDate=retain_until)
    # change mode to COMPLIANCE
    retention = {'Mode':'COMPLIANCE', 'RetainUntilDate':retain_until}
    client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention, BypassGovernanceRetention=True)

@pytest.mark.fails_on_dbstore
def test_object_lock_changing_mode_from_governance_without_bypass():
    bucket_name = get_new_bucket_name()
    key = 'file1'
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    # upload object with mode=GOVERNANCE
    retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
    client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='GOVERNANCE',
                      ObjectLockRetainUntilDate=retain_until)
    # try to change mode to COMPLIANCE
    retention = {'Mode':'COMPLIANCE', 'RetainUntilDate':retain_until}
    e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.fails_on_dbstore
def test_object_lock_changing_mode_from_compliance():
    bucket_name = get_new_bucket_name()
    key = 'file1'
    client = get_client()
    client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
    # upload object with mode=COMPLIANCE
    retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
    client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='COMPLIANCE',
                      ObjectLockRetainUntilDate=retain_until)
    # try to change mode to GOVERNANCE
    retention = {'Mode':'GOVERNANCE', 'RetainUntilDate':retain_until}
    e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403
    assert error_code == 'AccessDenied'

@pytest.mark.fails_on_dbstore
def test_copy_object_ifmatch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    client.copy_object(Bucket=bucket_name, CopySource=bucket_name+'/foo', CopySourceIfMatch=resp['ETag'], Key='bar')
    response = client.get_object(Bucket=bucket_name, Key='bar')
    body = _get_body(response)
    assert body == 'bar'

# TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40808 is resolved
@pytest.mark.fails_on_rgw
def test_copy_object_ifmatch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    e = assert_raises(ClientError, client.copy_object, Bucket=bucket_name, CopySource=bucket_name+'/foo', CopySourceIfMatch='ABCORZ', Key='bar')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

# TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40808 is resolved
@pytest.mark.fails_on_rgw
def test_copy_object_ifnonematch_good():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    e = assert_raises(ClientError, client.copy_object, Bucket=bucket_name, CopySource=bucket_name+'/foo', CopySourceIfNoneMatch=resp['ETag'], Key='bar')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 412
    assert error_code == 'PreconditionFailed'

@pytest.mark.fails_on_dbstore
def test_copy_object_ifnonematch_failed():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

    client.copy_object(Bucket=bucket_name, CopySource=bucket_name+'/foo', CopySourceIfNoneMatch='ABCORZ', Key='bar')
    response = client.get_object(Bucket=bucket_name, Key='bar')
    body = _get_body(response)
    assert body == 'bar'

# TODO: results in a 404 instead of 400 on the RGW
@pytest.mark.fails_on_rgw
def test_object_read_unreadable():
    bucket_name = get_new_bucket()
    client = get_client()
    e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='\xae\x8a-')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert e.response['Error']['Message'] == 'Couldn\'t parse the specified URI.'

def test_get_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == False

def test_get_public_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == True

def test_get_authpublic_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_bucket_acl(Bucket=bucket_name, ACL='authenticated-read')
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == True


def test_get_publicpolicy_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()

    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == False

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == True


def test_get_nonpublicpolicy_acl_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()

    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == False

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ],
        "Condition": {
            "IpAddress":
            {"aws:SourceIp": "10.0.0.0/32"}
        }
        }]
     })

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == False


def test_get_nonpublicpolicy_principal_bucket_policy_status():
    bucket_name = get_new_bucket()
    client = get_client()

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "arn:aws:iam::s3tenant1:root"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
            ],
        }]
    })
    
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    resp = client.get_bucket_policy_status(Bucket=bucket_name)
    assert resp['PolicyStatus']['IsPublic'] == False


def test_bucket_policy_allow_notprincipal():
    bucket_name = get_new_bucket()
    client = get_client()

    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "NotPrincipal": {"AWS": "arn:aws:iam::s3tenant1:root"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ],
        }]
     })

    e = assert_raises(ClientError,
                      client.put_bucket_policy, Bucket=bucket_name, Policy=policy_document)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'InvalidArgument' or error_code == 'MalformedPolicy'


def test_get_undefined_public_block():
    bucket_name = get_new_bucket()
    client = get_client()

    # delete the existing public access block configuration
    # as AWS creates a default public access block configuration
    resp = client.delete_public_access_block(Bucket=bucket_name)
    assert resp['ResponseMetadata']['HTTPStatusCode'] == 204

    response_code = ""
    try:
        resp = client.get_public_access_block(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response['Error']['Code']

    assert response_code == 'NoSuchPublicAccessBlockConfiguration'

def test_get_public_block_deny_bucket_policy():
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {'BlockPublicAcls': True,
                   'IgnorePublicAcls': True,
                   'BlockPublicPolicy': True,
                   'RestrictPublicBuckets': False}
    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

    # make sure we can get the public access block
    resp = client.get_public_access_block(Bucket=bucket_name)
    assert resp['PublicAccessBlockConfiguration']['BlockPublicAcls'] == access_conf['BlockPublicAcls']
    assert resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'] == access_conf['BlockPublicPolicy']
    assert resp['PublicAccessBlockConfiguration']['IgnorePublicAcls'] == access_conf['IgnorePublicAcls']
    assert resp['PublicAccessBlockConfiguration']['RestrictPublicBuckets'] == access_conf['RestrictPublicBuckets']

    # make bucket policy to deny access
    resource = _make_arn_resource(bucket_name)
    policy_document = make_json_policy("s3:GetBucketPublicAccessBlock",
                                       resource, effect="Deny")
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

    # check if the access is denied
    e = assert_raises(ClientError, client.get_public_access_block, Bucket=bucket_name)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

def test_put_public_block():
    #client = get_svc_client(svc='s3control', client_config=Config(s3={'addressing_style': 'path'}))
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {'BlockPublicAcls': True,
                   'IgnorePublicAcls': True,
                   'BlockPublicPolicy': True,
                   'RestrictPublicBuckets': False}

    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

    resp = client.get_public_access_block(Bucket=bucket_name)
    assert resp['PublicAccessBlockConfiguration']['BlockPublicAcls'] == access_conf['BlockPublicAcls']
    assert resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'] == access_conf['BlockPublicPolicy']
    assert resp['PublicAccessBlockConfiguration']['IgnorePublicAcls'] == access_conf['IgnorePublicAcls']
    assert resp['PublicAccessBlockConfiguration']['RestrictPublicBuckets'] == access_conf['RestrictPublicBuckets']


def test_block_public_put_bucket_acls():
    #client = get_svc_client(svc='s3control', client_config=Config(s3={'addressing_style': 'path'}))
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {'BlockPublicAcls': True,
                   'IgnorePublicAcls': False,
                   'BlockPublicPolicy': True,
                   'RestrictPublicBuckets': False}

    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

    resp = client.get_public_access_block(Bucket=bucket_name)
    assert resp['PublicAccessBlockConfiguration']['BlockPublicAcls'] == access_conf['BlockPublicAcls']
    assert resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'] == access_conf['BlockPublicPolicy']

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name,ACL='public-read')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name,ACL='public-read-write')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403

    e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name,ACL='authenticated-read')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 403


def test_block_public_object_canned_acls():
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {'BlockPublicAcls': True,
                   'IgnorePublicAcls': False,
                   'BlockPublicPolicy': False,
                   'RestrictPublicBuckets': False}

    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

    # resp = client.get_public_access_block(Bucket=bucket_name)
    # assert resp['PublicAccessBlockConfiguration']['BlockPublicAcls'] == access_conf['BlockPublicAcls']
    # assert resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'] == access_conf['BlockPublicPolicy']

    #FIXME: use empty body until #42208
    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo1', Body='', ACL='public-read')
    assert 403 == _get_status(e.response)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo2', Body='', ACL='public-read-write')
    assert 403 == _get_status(e.response)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo3', Body='', ACL='authenticated-read')
    assert 403 == _get_status(e.response)

    client.put_object(Bucket=bucket_name, Key='foo4', Body='', ACL='private')


def test_block_public_policy():
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {'BlockPublicAcls': False,
                   'IgnorePublicAcls': False,
                   'BlockPublicPolicy': True,
                   'RestrictPublicBuckets': False}

    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject",
                                       resource)

    check_access_denied(client.put_bucket_policy, Bucket=bucket_name, Policy=policy_document)


def test_block_public_policy_with_principal():
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {'BlockPublicAcls': False,
                   'IgnorePublicAcls': False,
                   'BlockPublicPolicy': True,
                   'RestrictPublicBuckets': False}
    
    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject",
                                        resource, principal={"AWS": "arn:aws:iam::s3tenant1:root"})

    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)


@pytest.mark.fails_on_dbstore
def test_block_public_restrict_public_buckets():
    bucket_name = get_new_bucket()
    client = get_client()

    # remove any existing public access block configuration
    resp = client.delete_public_access_block(Bucket=bucket_name)
    assert resp['ResponseMetadata']['HTTPStatusCode'] == 204

    # upload an object
    response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # upload a bucket policy that allows public access
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))
    policy_document = make_json_policy("s3:GetObject",
                                        resource)
    resp = client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    assert resp['ResponseMetadata']['HTTPStatusCode'] == 204

    # check if the object is accessible publicly
    unauthenticated_client = get_unauthenticated_client()
    response = unauthenticated_client.get_object(Bucket=bucket_name, Key='foo')
    assert _get_body(response) == 'bar'

    # put a public access block configuration that restricts public buckets
    access_conf = {'BlockPublicAcls': False,
                   'IgnorePublicAcls': False,
                   'BlockPublicPolicy': False,
                   'RestrictPublicBuckets': True}
    resp = client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)
    assert resp['ResponseMetadata']['HTTPStatusCode'] == 200

    # check if the object is no longer accessible publicly
    check_access_denied(unauthenticated_client.get_object, Bucket=bucket_name, Key='foo')

    # check if the object is still accessible by the owner
    response = client.get_object(Bucket=bucket_name, Key='foo')
    assert _get_body(response) == 'bar'


def test_ignore_public_acls():
    bucket_name = get_new_bucket()
    client = get_client()
    alt_client = get_alt_client()

    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
    # Public bucket should be accessible
    alt_client.list_objects(Bucket=bucket_name)

    client.put_object(Bucket=bucket_name,Key='key1',Body='abcde',ACL='public-read')
    resp=alt_client.get_object(Bucket=bucket_name, Key='key1')
    assert _get_body(resp) == 'abcde'

    access_conf = {'BlockPublicAcls': False,
                   'IgnorePublicAcls': True,
                   'BlockPublicPolicy': False,
                   'RestrictPublicBuckets': False}

    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)
    resource = _make_arn_resource("{}/{}".format(bucket_name, "*"))

    client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
    # IgnorePublicACLs is true, so regardless this should behave as a private bucket
    check_access_denied(alt_client.list_objects, Bucket=bucket_name)
    check_access_denied(alt_client.get_object, Bucket=bucket_name, Key='key1')

def test_put_get_delete_public_block():
    bucket_name = get_new_bucket()
    client = get_client()

    access_conf = {'BlockPublicAcls': True,
                   'IgnorePublicAcls': True,
                   'BlockPublicPolicy': True,
                   'RestrictPublicBuckets': False}

    client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

    resp = client.get_public_access_block(Bucket=bucket_name)
    assert resp['PublicAccessBlockConfiguration']['BlockPublicAcls'] == access_conf['BlockPublicAcls']
    assert resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'] == access_conf['BlockPublicPolicy']
    assert resp['PublicAccessBlockConfiguration']['IgnorePublicAcls'] == access_conf['IgnorePublicAcls']
    assert resp['PublicAccessBlockConfiguration']['RestrictPublicBuckets'] == access_conf['RestrictPublicBuckets']

    resp = client.delete_public_access_block(Bucket=bucket_name)
    assert resp['ResponseMetadata']['HTTPStatusCode'] == 204

    response_code = ""
    try:
        resp = client.get_public_access_block(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response['Error']['Code']

    assert response_code == 'NoSuchPublicAccessBlockConfiguration'

def test_multipart_upload_on_a_bucket_with_policy():
    bucket_name = get_new_bucket()
    client = get_client()
    resource1 = "arn:aws:s3:::" + bucket_name
    resource2 = "arn:aws:s3:::" + bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": "*",
        "Action": "*",
        "Resource": [
            resource1,
            resource2
          ],
        }]
     })
    key = "foo"
    objlen=50*1024*1024
    client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client)
    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def _put_bucket_encryption_s3(client, bucket_name):
    """
    enable a default encryption policy on the given bucket
    """
    server_side_encryption_conf = {
        'Rules': [
            {
                'ApplyServerSideEncryptionByDefault': {
                    'SSEAlgorithm': 'AES256'
                }
            },
        ]
    }
    response = client.put_bucket_encryption(Bucket=bucket_name, ServerSideEncryptionConfiguration=server_side_encryption_conf)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def _put_bucket_encryption_kms(client, bucket_name):
    """
    enable a default encryption policy on the given bucket
    """
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = 'fool-me-again'
    server_side_encryption_conf = {
        'Rules': [
            {
                'ApplyServerSideEncryptionByDefault': {
                    'SSEAlgorithm': 'aws:kms',
                    'KMSMasterKeyID': kms_keyid
                }
            },
        ]
    }
    response = client.put_bucket_encryption(Bucket=bucket_name, ServerSideEncryptionConfiguration=server_side_encryption_conf)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.sse_s3
def test_put_bucket_encryption_s3():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

@pytest.mark.encryption
def test_put_bucket_encryption_kms():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_kms(client, bucket_name)


@pytest.mark.sse_s3
def test_get_bucket_encryption_s3():
    bucket_name = get_new_bucket()
    client = get_client()

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response['Error']['Code']

    assert response_code == 'ServerSideEncryptionConfigurationNotFoundError'

    _put_bucket_encryption_s3(client, bucket_name)

    response = client.get_bucket_encryption(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] == 'AES256'


@pytest.mark.encryption
def test_get_bucket_encryption_kms():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        kms_keyid = 'fool-me-again'
    bucket_name = get_new_bucket()
    client = get_client()

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response['Error']['Code']

    assert response_code == 'ServerSideEncryptionConfigurationNotFoundError'

    _put_bucket_encryption_kms(client, bucket_name)

    response = client.get_bucket_encryption(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert response['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'] == 'aws:kms'
    assert response['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault']['KMSMasterKeyID'] == kms_keyid


@pytest.mark.sse_s3
def test_delete_bucket_encryption_s3():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    _put_bucket_encryption_s3(client, bucket_name)

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response['Error']['Code']

    assert response_code == 'ServerSideEncryptionConfigurationNotFoundError'


@pytest.mark.encryption
def test_delete_bucket_encryption_kms():
    bucket_name = get_new_bucket()
    client = get_client()

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    _put_bucket_encryption_kms(client, bucket_name)

    response = client.delete_bucket_encryption(Bucket=bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response_code = ""
    try:
        client.get_bucket_encryption(Bucket=bucket_name)
    except ClientError as e:
        response_code = e.response['Error']['Code']

    assert response_code == 'ServerSideEncryptionConfigurationNotFoundError'

def _test_sse_s3_default_upload(file_size):
    """
    Test enables bucket encryption.
    Create a file of A's of certain size, and use it to set_contents_from_file.
    Re-read the contents, and confirm we get same content as input i.e., A's
    """
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    data = 'A'*file_size
    response = client.put_object(Bucket=bucket_name, Key='testobj', Body=data)
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'

    response = client.get_object(Bucket=bucket_name, Key='testobj')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'
    body = _get_body(response)
    assert body == data

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_default_upload_1b():
    _test_sse_s3_default_upload(1)

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_default_upload_1kb():
    _test_sse_s3_default_upload(1024)

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_default_upload_1mb():
    _test_sse_s3_default_upload(1024*1024)

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_default_upload_8mb():
    _test_sse_s3_default_upload(8*1024*1024)

def _test_sse_kms_default_upload(file_size):
    """
    Test enables bucket encryption.
    Create a file of A's of certain size, and use it to set_contents_from_file.
    Re-read the contents, and confirm we get same content as input i.e., A's
    """
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip('[s3 main] section missing kms_keyid')
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_kms(client, bucket_name)

    data = 'A'*file_size
    response = client.put_object(Bucket=bucket_name, Key='testobj', Body=data)
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'aws:kms'
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id'] == kms_keyid

    response = client.get_object(Bucket=bucket_name, Key='testobj')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'aws:kms'
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id'] == kms_keyid
    body = _get_body(response)
    assert body == data

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_kms_default_upload_1b():
    _test_sse_kms_default_upload(1)

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_kms_default_upload_1kb():
    _test_sse_kms_default_upload(1024)

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_kms_default_upload_1mb():
    _test_sse_kms_default_upload(1024*1024)

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_kms_default_upload_8mb():
    _test_sse_kms_default_upload(8*1024*1024)



@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_default_method_head():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    data = 'A'*1000
    key = 'testobj'
    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = client.head_object(Bucket=bucket_name, Key=key)
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'

    sse_s3_headers = {
        'x-amz-server-side-encryption': 'AES256',
    }
    lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_s3_headers))
    client.meta.events.register('before-call.s3.HeadObject', lf)
    e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_default_multipart_upload():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    key = "multipart_enc"
    content_type = 'text/plain'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}
    enc_headers = {
        'Content-Type': content_type
    }
    resend_parts = []

    (upload_id, data, parts) = _multipart_upload_enc(client, bucket_name, key, objlen,
            part_size=5*1024*1024, init_headers=enc_headers, part_headers=enc_headers, metadata=metadata, resend_parts=resend_parts)

    lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
    client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    response = client.list_objects_v2(Bucket=bucket_name, Prefix=key)
    assert len(response['Contents']) == 1
    assert response['Contents'][0]['Size'] == objlen

    lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
    client.meta.events.register('before-call.s3.UploadPart', lf)

    response = client.get_object(Bucket=bucket_name, Key=key)

    assert response['Metadata'] == metadata
    assert response['ResponseMetadata']['HTTPHeaders']['content-type'] == content_type
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'

    body = _get_body(response)
    assert body == data
    size = response['ContentLength']
    assert len(body) == size

    _check_content_using_range(key, bucket_name, data, 1000000)
    _check_content_using_range(key, bucket_name, data, 10000000)

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_default_post_object_authenticated_request():
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_s3(client, bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
            "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", "text/plain"],
                ["starts-with", "$x-amz-server-side-encryption", ""], 
                ["content-length-range", 0, 1024]
            ]
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),
    ('file', ('bar'))])

    r = requests.post(url, files = payload)
    assert r.status_code == 204

    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'
    body = _get_body(response)
    assert body == 'bar'

@pytest.mark.encryption
@pytest.mark.bucket_encryption
@pytest.mark.fails_on_dbstore
def test_sse_kms_default_post_object_authenticated_request():
    kms_keyid = get_main_kms_keyid()
    if kms_keyid is None:
        pytest.skip('[s3 main] section missing kms_keyid')
    bucket_name = get_new_bucket()
    client = get_client()
    _put_bucket_encryption_kms(client, bucket_name)

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {
            "expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", "text/plain"],
                ["starts-with", "$x-amz-server-side-encryption", ""], 
                ["content-length-range", 0, 1024]
            ]
    }

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    payload = OrderedDict([ ("key" , "foo.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),
    ('file', ('bar'))])

    r = requests.post(url, files = payload)
    assert r.status_code == 204

    response = client.get_object(Bucket=bucket_name, Key='foo.txt')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'aws:kms'
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id'] == kms_keyid
    body = _get_body(response)
    assert body == 'bar'


def _test_sse_s3_encrypted_upload(file_size):
    """
    Test upload of the given size, specifically requesting sse-s3 encryption.
    """
    bucket_name = get_new_bucket()
    client = get_client()

    data = 'A'*file_size
    response = client.put_object(Bucket=bucket_name, Key='testobj', Body=data, ServerSideEncryption='AES256')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'

    response = client.get_object(Bucket=bucket_name, Key='testobj')
    assert response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'] == 'AES256'
    body = _get_body(response)
    assert body == data

@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_1b():
    _test_sse_s3_encrypted_upload(1)

@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_1kb():
    _test_sse_s3_encrypted_upload(1024)

@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_1mb():
    _test_sse_s3_encrypted_upload(1024*1024)

@pytest.mark.encryption
@pytest.mark.sse_s3
@pytest.mark.fails_on_dbstore
def test_sse_s3_encrypted_upload_8mb():
    _test_sse_s3_encrypted_upload(8*1024*1024)

def test_get_object_torrent():
    client = get_client()
    bucket_name = get_new_bucket()
    key = 'Avatar.mpg'

    file_size = 7 * 1024 * 1024
    data = 'A' * file_size

    client.put_object(Bucket=bucket_name, Key=key, Body=data)

    response = None
    try:
        response = client.get_object_torrent(Bucket=bucket_name, Key=key)
        # if successful, verify the torrent contents are different from the body
        assert data != _get_body(response)
    except ClientError as e:
        # accept 404 errors - torrent support may not be configured
        status, error_code = _get_status_and_error_code(e.response)
        assert status == 404
        assert error_code == 'NoSuchKey'

@pytest.mark.checksum
def test_object_checksum_sha256():
    bucket = get_new_bucket()
    client = get_client()

    key = "myobj"
    size = 1024
    body = FakeWriteFile(size, 'A')
    sha256sum = 'arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0='
    response = client.put_object(Bucket=bucket, Key=key, Body=body, ChecksumAlgorithm='SHA256', ChecksumSHA256=sha256sum)
    assert sha256sum == response['ChecksumSHA256']

    response = client.head_object(Bucket=bucket, Key=key)
    assert 'ChecksumSHA256' not in response
    response = client.head_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
    assert sha256sum == response['ChecksumSHA256']

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, Body=body, ChecksumAlgorithm='SHA256', ChecksumSHA256='bad')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'BadDigest'

@pytest.mark.checksum
def test_object_checksum_crc64nvme():
    bucket = get_new_bucket()
    client = get_client()

    key = "myobj"
    size = 1024
    body = FakeWriteFile(size, 'A')
    crc64sum = 'Qeh8oXvGiSo='
    response = client.put_object(Bucket=bucket, Key=key, Body=body, ChecksumAlgorithm='CRC64NVME', ChecksumCRC64NVME=crc64sum)
    assert crc64sum == response['ChecksumCRC64NVME']

    response = client.head_object(Bucket=bucket, Key=key)
    assert 'ChecksumCRC64NVME' not in response
    response = client.head_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
    assert crc64sum == response['ChecksumCRC64NVME']

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, Body=body, ChecksumAlgorithm='CRC64NVME', ChecksumCRC64NVME='bad')
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'BadDigest'

@pytest.mark.checksum
@pytest.mark.fails_on_dbstore
def test_multipart_checksum_sha256():
    bucket = get_new_bucket()
    client = get_client()

    key = "mymultipart"
    response = client.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm='SHA256')
    assert 'SHA256' == response['ChecksumAlgorithm']
    upload_id = response['UploadId']

    size = 1024
    body = FakeWriteFile(size, 'A')
    part_sha256sum = 'arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0='
    response = client.upload_part(UploadId=upload_id, Bucket=bucket, Key=key, PartNumber=1, Body=body, ChecksumAlgorithm='SHA256', ChecksumSHA256=part_sha256sum)

    # should reject the bad request checksum
    e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket, Key=key, UploadId=upload_id, ChecksumSHA256='bad', MultipartUpload={'Parts': [
        {'ETag': response['ETag'].strip('"'), 'ChecksumSHA256': response['ChecksumSHA256'], 'PartNumber': 1}]})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'BadDigest'

    # XXXX re-trying the complete is failing in RGW due to an internal error that appears not caused
    # checksums;
    # 2024-04-25T17:47:47.991-0400 7f78e3a006c0  0 req 4931907640780566174 0.011000143s s3:complete_multipart check_previously_completed() ERROR: get_obj_attrs() returned ret=-2
    # 2024-04-25T17:47:47.991-0400 7f78e3a006c0  2 req 4931907640780566174 0.011000143s s3:complete_multipart completing
    # 2024-04-25T17:47:47.991-0400 7f78e3a006c0  1 req 4931907640780566174 0.011000143s s3:complete_multipart ERROR: either op_ret is negative (execute failed) or target_obj is null, op_ret: -2200
    # -2200 turns into 500, InternalError

    key = "mymultipart2"
    response = client.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm='SHA256')
    assert 'SHA256' == response['ChecksumAlgorithm']
    upload_id = response['UploadId']

    body = FakeWriteFile(size, 'A')
    part_sha256sum = 'arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0='
    response = client.upload_part(UploadId=upload_id, Bucket=bucket, Key=key, PartNumber=1, Body=body, ChecksumAlgorithm='SHA256', ChecksumSHA256=part_sha256sum)

    # should reject the missing part checksum
    e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket, Key=key, UploadId=upload_id, ChecksumSHA256='bad', MultipartUpload={'Parts': [
        {'ETag': response['ETag'].strip('"'), 'PartNumber': 1}]})
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400
    assert error_code == 'BadDigest'

    key = "mymultipart3"
    response = client.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm='SHA256')
    assert 'SHA256' == response['ChecksumAlgorithm']
    upload_id = response['UploadId']

    body = FakeWriteFile(size, 'A')
    part_sha256sum = 'arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0='
    response = client.upload_part(UploadId=upload_id, Bucket=bucket, Key=key, PartNumber=1, Body=body, ChecksumAlgorithm='SHA256', ChecksumSHA256=part_sha256sum)

    composite_sha256sum = 'Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1'
    response = client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, ChecksumSHA256=composite_sha256sum, MultipartUpload={'Parts': [
        {'ETag': response['ETag'].strip('"'), 'ChecksumSHA256': response['ChecksumSHA256'], 'PartNumber': 1}]})
    assert composite_sha256sum == response['ChecksumSHA256']

    response = client.head_object(Bucket=bucket, Key=key)
    assert 'ChecksumSHA256' not in response
    response = client.head_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
    assert composite_sha256sum == response['ChecksumSHA256']

def multipart_checksum_3parts_helper(key=None, checksum_algo=None, checksum_type=None, **kwargs):

    bucket = get_new_bucket()
    client = get_client()

    response = client.create_multipart_upload(Bucket=bucket, Key=key, ChecksumAlgorithm=checksum_algo, ChecksumType=checksum_type)
    assert checksum_algo == response['ChecksumAlgorithm']
    upload_id = response['UploadId']

    cksum_arg_name = "Checksum" + checksum_algo

    upload_args = {cksum_arg_name : kwargs['part1_cksum']}
    response = client.upload_part(UploadId=upload_id, Bucket=bucket, Key=key, PartNumber=1, Body=kwargs['body1'], ChecksumAlgorithm=checksum_algo, **upload_args)
    etag1 = response['ETag'].strip('"')
    cksum1 = response[cksum_arg_name]

    upload_args = {cksum_arg_name : kwargs['part2_cksum']}
    response = client.upload_part(UploadId=upload_id, Bucket=bucket, Key=key, PartNumber=2, Body=kwargs['body2'], ChecksumAlgorithm=checksum_algo, **upload_args)
    etag2 = response['ETag'].strip('"')
    cksum2 = response[cksum_arg_name]

    upload_args = {cksum_arg_name : kwargs['part3_cksum']}
    response = client.upload_part(UploadId=upload_id, Bucket=bucket, Key=key, PartNumber=3, Body=kwargs['body3'], ChecksumAlgorithm=checksum_algo, **upload_args)
    etag3 = response['ETag'].strip('"')
    cksum3 = response[cksum_arg_name]

    upload_args = {cksum_arg_name : kwargs['composite_cksum']}
    response = client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={'Parts': [
        {'ETag': etag1, cksum_arg_name: cksum1, 'PartNumber': 1},
        {'ETag': etag2, cksum_arg_name: cksum2, 'PartNumber': 2},
        {'ETag': etag3, cksum_arg_name: cksum3, 'PartNumber': 3}]},
        **upload_args)

    assert response['ChecksumType'] == checksum_type
    assert response[cksum_arg_name] == kwargs['composite_cksum']

    response1 = client.head_object(Bucket=bucket, Key=key)
    assert cksum_arg_name not in response1

    response2 = client.head_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
    assert response2['ChecksumType'] == checksum_type
    assert response2[cksum_arg_name] == kwargs['composite_cksum']

    request_attributes = ['Checksum']
    response3 = client.get_object_attributes(Bucket=bucket, Key=key, \
                                             ObjectAttributes=request_attributes)
    assert response3['Checksum']['ChecksumType'] == checksum_type
    assert response3['Checksum'][cksum_arg_name] == kwargs['composite_cksum']

@pytest.mark.checksum
@pytest.mark.fails_on_dbstore
def test_multipart_use_cksum_helper_sha256():
    size = 5 * 1024 * 1024 # each part but the last must be at least 5M

    # code to compute checksums for these is in unittest_rgw_cksum
    body1 = FakeWriteFile(size, 'A')
    body2 = FakeWriteFile(size, 'B')
    body3 = FakeWriteFile(size, 'C')

    upload_args = {
        "body1" : body1,
        "part1_cksum" : '275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=',
        "body2" : body2,
        "part2_cksum" : 'mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=',
        "body3" : body3,
        "part3_cksum" : 'Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=',
        # the composite OR combined checksum, as appropriate
        "composite_cksum" : 'uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3',
    }

    res = multipart_checksum_3parts_helper(key="mymultipart3", checksum_algo="SHA256", checksum_type="COMPOSITE", **upload_args)
    #eof

@pytest.mark.checksum
@pytest.mark.fails_on_dbstore
def test_multipart_use_cksum_helper_crc64nvme():
    size = 5 * 1024 * 1024 # each part but the last must be at least 5M

    # code to compute checksums for these is in unittest_rgw_cksum
    body1 = FakeWriteFile(size, 'A')
    body2 = FakeWriteFile(size, 'B')
    body3 = FakeWriteFile(size, 'C')

    upload_args = {
        "body1" : body1,
        "part1_cksum" : 'L/E4WYn8v98=',
        "body2" : body2,
        "part2_cksum" : 'xW1l19VobYM=',
        "body3" : body3,
        "part3_cksum" : 'cK5MnNaWrW4=',
        # the composite OR combined checksum, as appropriate
        "composite_cksum" : 'i+6LR0y3eFo=',
    }

    res = multipart_checksum_3parts_helper(key="mymultipart3", checksum_algo="CRC64NVME", checksum_type="FULL_OBJECT", **upload_args)
    #eof

@pytest.mark.checksum
@pytest.mark.fails_on_dbstore
def test_multipart_use_cksum_helper_crc32():
    size = 5 * 1024 * 1024 # each part but the last must be at least 5M

    # code to compute checksums for these is in unittest_rgw_cksum
    body1 = FakeWriteFile(size, 'A')
    body2 = FakeWriteFile(size, 'B')
    body3 = FakeWriteFile(size, 'C')

    upload_args = {
        "body1" : body1,
        "part1_cksum" : 'JRTCyQ==',
        "body2" : body2,
        "part2_cksum" : 'QoZTGg==',
        "body3" : body3,
        "part3_cksum" : 'YAgjqw==',
        # the composite OR combined checksum, as appropriate
        "composite_cksum" : 'WgDhBQ==',
    }

    res = multipart_checksum_3parts_helper(key="mymultipart3", checksum_algo="CRC32", checksum_type="FULL_OBJECT", **upload_args)
    #eof

@pytest.mark.checksum
@pytest.mark.fails_on_dbstore
def test_multipart_use_cksum_helper_crc32c():
    size = 5 * 1024 * 1024 # each part but the last must be at least 5M

    # code to compute checksums for these is in unittest_rgw_cksum
    body1 = FakeWriteFile(size, 'A')
    body2 = FakeWriteFile(size, 'B')
    body3 = FakeWriteFile(size, 'C')

    upload_args = {
        "body1" : body1,
        "part1_cksum" : 'MDaLrw==',
        "body2" : body2,
        "part2_cksum" : 'TH4EZg==',
        "body3" : body3,
        "part3_cksum" : 'Z7mBIQ==',
        # the composite OR combined checksum, as appropriate
        "composite_cksum" : 'xU+Krw==',
    }

    res = multipart_checksum_3parts_helper(key="mymultipart3", checksum_algo="CRC32C", checksum_type="FULL_OBJECT", **upload_args)
    #eof

@pytest.mark.checksum
@pytest.mark.fails_on_dbstore
def test_multipart_use_cksum_helper_sha1():
    size = 5 * 1024 * 1024 # each part but the last must be at least 5M

    # code to compute checksums for these is in unittest_rgw_cksum
    body1 = FakeWriteFile(size, 'A')
    body2 = FakeWriteFile(size, 'B')
    body3 = FakeWriteFile(size, 'C')

    upload_args = {
        "body1" : body1,
        "part1_cksum" : 'iIaTCGbm+vdVjNqIMF2S0T7ibMk=',
        "body2" : body2,
        "part2_cksum" : 'LS/TJ32bAVKEwRu+sE3X7awh/lk=',
        "body3" : body3,
        "part3_cksum" : '6DDwovUaHwrKNXDMzOGbuvj9kxI=',
        # the composite OR combined checksum, as appropriate
        "composite_cksum" : 'sizjvY4eud3MrcHdZM3cQ/ol39o=-3',
    }

    res = multipart_checksum_3parts_helper(key="mymultipart3", checksum_algo="SHA1", checksum_type="COMPOSITE", **upload_args)
    #eof    

@pytest.mark.checksum
def test_post_object_upload_checksum():
    megabytes = 1024 * 1024
    min_size = 0
    max_size = 5 * megabytes
    test_payload_size = 2 * megabytes

    bucket_name = get_new_bucket()
    client = get_client()

    url = _get_post_url(bucket_name)
    utc = pytz.utc
    expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

    policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),\
    "conditions": [\
    {"bucket": bucket_name},\
    ["starts-with", "$key", "foo_cksum_test"],\
    {"acl": "private"},\
    ["starts-with", "$Content-Type", "text/plain"],\
    ["content-length-range", min_size, max_size],\
    ]\
    }

    test_payload = b'x' * test_payload_size

    json_policy_document = json.JSONEncoder().encode(policy_document)
    bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
    policy = base64.b64encode(bytes_json_policy_document)
    aws_secret_access_key = get_main_aws_secret_key()
    aws_access_key_id = get_main_aws_access_key()

    signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

    # good checksum payload (checked via upload from awscli)
    payload = OrderedDict([ ("key" , "foo_cksum_test.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),\
    ('x-amz-checksum-sha256', 'aTL9MeXa9HObn6eP93eygxsJlcwdCwCTysgGAZAgE7w='),\
    ('file', (test_payload)),])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 204

    # bad checksum payload
    payload = OrderedDict([ ("key" , "foo_cksum_test.txt"),("AWSAccessKeyId" , aws_access_key_id),\
    ("acl" , "private"),("signature" , signature),("policy" , policy),\
    ("Content-Type" , "text/plain"),\
    ('x-amz-checksum-sha256', 'sailorjerry'),\
    ('file', (test_payload)),])

    r = requests.post(url, files=payload, verify=get_config_ssl_verify())
    assert r.status_code == 400


def _set_log_bucket_policy_tenant(client, log_tenant, log_bucket_name, src_tenant, src_user, src_buckets, log_prefixes):
    statements = []
    for j in range(len(src_buckets)):
        if len(log_prefixes) == 1:
            prefix = log_prefixes[0]
        else:
            prefix = log_prefixes[j]
        statements.append({
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"Service": "logging.s3.amazonaws.com"},
            "Action": ["s3:PutObject"],
            "Resource": "arn:aws:s3::{}:{}/{}".format(log_tenant, log_bucket_name, prefix),
            "Condition": {
                "ArnLike": {"aws:SourceArn": "arn:aws:s3::{}:{}".format(src_tenant, src_buckets[j])},
                "StringEquals": {
                    "aws:SourceAccount": src_user
                    }
            }
        })

    policy_document = json.dumps({
        "Version": "2012-10-17",
        "Statement": statements
        })

    result = client.put_bucket_policy(Bucket=log_bucket_name, Policy=policy_document)
    assert(result['ResponseMetadata']['HTTPStatusCode'] == 204)


def _set_log_bucket_policy(client, log_bucket_name, src_buckets, log_prefixes):
    _set_log_bucket_policy_tenant(client, "", log_bucket_name, "", get_main_user_id(), src_buckets, log_prefixes)


def _has_bucket_logging_extension():
    src_bucket_name = get_new_bucket_name()
    log_bucket_name = get_new_bucket_name()
    client = get_client()
    log_prefix = 'log/'
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': log_prefix, 'LoggingType': 'Journal'}
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
    except ParamValidationError as e:
        return False
    except ClientError as e:
        # should fail on non-existing bucket
        return True

    return True


def _has_target_object_key_format():
    src_bucket_name = get_new_bucket_name()
    log_bucket_name = get_new_bucket_name()
    client = get_client()
    log_prefix = 'log/'
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': log_prefix, 'TargetObjectKeyFormat': {'SimplePrefix': {}}}
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
    except ParamValidationError as e:
        return False
    finally:
        # should fail on non-existing bucket
        return True


import shlex

def _parse_standard_log_record(record):
    record = record.replace('[', '"').replace(']', '"')
    chunks = shlex.split(record)
    assert len(chunks) == 26
    return {
            'BucketOwner':      chunks[0],
            'BucketName':       chunks[1],
            'RequestDateTime':  chunks[2],
            'RemoteIP':         chunks[3],
            'Requester':        chunks[4],
            'RequestID':        chunks[5],
            'Operation':        chunks[6],
            'Key':              chunks[7],
            'RequestURI':       chunks[8],
            'HTTPStatus':       chunks[9],
            'ErrorCode':        chunks[10],
            'BytesSent':        chunks[11],
            'ObjectSize':       chunks[12],
            'TotalTime':        chunks[13],
            'TurnAroundTime':   chunks[14],
            'Referrer':         chunks[15],
            'UserAgent':        chunks[16],
            'VersionID':        chunks[17],
            'HostID':           chunks[18],
            'SigVersion':       chunks[19],
            'CipherSuite':      chunks[20],
            'AuthType':         chunks[21],
            'HostHeader':       chunks[22],
            'TLSVersion':       chunks[23],
            'AccessPointARN':   chunks[24],
            'ACLRequired':      chunks[25],
            }


def _parse_journal_log_record(record):
    record = record.replace('[', '"').replace(']', '"')
    chunks = shlex.split(record)
    assert len(chunks) == 8
    return {
            'BucketOwner':      chunks[0],
            'BucketName':       chunks[1],
            'RequestDateTime':  chunks[2],
            'Operation':        chunks[3],
            'Key':              chunks[4],
            'ObjectSize':       chunks[5],
            'VersionID':        chunks[6],
            'ETAG':             chunks[7],
            }

def _parse_log_record(record, record_type):
    if record_type == 'Standard':
        return _parse_standard_log_record(record)
    elif record_type == 'Journal':
        return _parse_journal_log_record(record)
    else:
        assert False, 'unknown log record type'

expected_object_roll_time = 5

import logging

logger = logging.getLogger(__name__)


def _verify_records(records, bucket_name, event_type, src_keys, record_type, expected_count, exact_match=False):
    keys_found = []
    all_keys = []
    for record in iter(records.splitlines()):
        parsed_record = _parse_log_record(record, record_type)
        logger.info('bucket log record: %s', json.dumps(parsed_record, indent=4))
        if bucket_name in record and event_type in record:
            all_keys.append(parsed_record['Key'])
            for key in src_keys:
                if key in record:
                    keys_found.append(key)
                    break
    logger.info('%d keys found in bucket log: %s', len(all_keys), str(all_keys))
    logger.info('%d keys from the source bucket: %s', len(src_keys), str(src_keys))
    if exact_match:
        return len(keys_found) == expected_count and len(keys_found) == len(all_keys)
    return len(keys_found) == expected_count

def _verify_record_field(records, bucket_name, event_type, object_key, record_type, field_name, expected_value):
    for record in iter(records.splitlines()):
        if bucket_name in record and event_type in record and object_key in record:
            parsed_record = _parse_log_record(record, record_type)
            logger.info('bucket log record: %s', json.dumps(parsed_record, indent=4))
            try:
                value = parsed_record[field_name]
                return expected_value == value
            except KeyError:
                return False
    return False

def randcontent():
    letters = string.ascii_lowercase
    length = random.randint(10, 1024)
    return ''.join(random.choice(letters) for i in range(length))


@pytest.mark.bucket_logging
def test_put_bucket_logging():
    has_extensions = _has_bucket_logging_extension()
    has_key_format = _has_target_object_key_format()
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {
            'TargetBucket': log_bucket_name,
            'TargetPrefix': prefix
            }

    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    if has_extensions:
        logging_enabled['LoggingType'] = 'Standard'
        logging_enabled['RecordsBatchSize'] = 0
    if has_key_format:
        # default value for key prefix is returned
        logging_enabled['TargetObjectKeyFormat'] = {'SimplePrefix': {}}
    assert response['LoggingEnabled'] == logging_enabled

    if has_key_format:
        # with simple target object prefix
        logging_enabled = {
            'TargetBucket': log_bucket_name,
            'TargetPrefix': 'log/',
            'TargetObjectKeyFormat': {
                'SimplePrefix': {}
            }
        }
        if has_extensions:
            logging_enabled['ObjectRollTime'] = expected_object_roll_time
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        response = client.get_bucket_logging(Bucket=src_bucket_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        if has_extensions:
            logging_enabled['LoggingType'] = 'Standard'
            logging_enabled['RecordsBatchSize'] = 0
        assert response['LoggingEnabled'] == logging_enabled

        # with partitioned target object prefix
        logging_enabled = {
            'TargetBucket': log_bucket_name,
            'TargetPrefix': 'log/',
            'TargetObjectKeyFormat': {
                'PartitionedPrefix': {
                    'PartitionDateSource': 'DeliveryTime'
                }
            }
        }
        if has_extensions:
            logging_enabled['ObjectRollTime'] = expected_object_roll_time
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        response = client.get_bucket_logging(Bucket=src_bucket_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        if has_extensions:
            logging_enabled['LoggingType'] = 'Standard'
            logging_enabled['RecordsBatchSize'] = 0
        assert response['LoggingEnabled'] == logging_enabled

    # with target grant (not implemented in RGW)
    main_display_name = get_main_display_name()
    main_user_id = get_main_user_id()
    logging_enabled = {
        'TargetBucket': log_bucket_name,
        'TargetPrefix': 'log/',
        'TargetGrants': [{'Grantee': {'DisplayName': main_display_name, 'ID': main_user_id,'Type': 'CanonicalUser'},'Permission': 'FULL_CONTROL'}] 
    }
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    if has_extensions:
        logging_enabled['LoggingType'] = 'Standard'
        logging_enabled['RecordsBatchSize'] = 0
    # target grants are not implemented
    logging_enabled.pop('TargetGrants')
    if has_key_format:
        # default value for key prefix is returned
        logging_enabled['TargetObjectKeyFormat'] = {'SimplePrefix': {}}
    assert response['LoggingEnabled'] == logging_enabled


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_mtime():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {
            'TargetBucket': log_bucket_name,
            'TargetPrefix': prefix
            }

    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    mtime = response['ResponseMetadata']['HTTPHeaders']['last-modified']

    # wait and set the same conf - mtime should be the same
    time.sleep(1)
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert mtime == response['ResponseMetadata']['HTTPHeaders']['last-modified']

    # wait and change conf - new mtime should be larger
    time.sleep(1)
    prefix = 'another-log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {
            'TargetBucket': log_bucket_name,
            'TargetPrefix': prefix
            }

    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert mtime < response['ResponseMetadata']['HTTPHeaders']['last-modified']
    mtime = response['ResponseMetadata']['HTTPHeaders']['last-modified']

    # wait and disable/enable conf - new mtime should be larger
    time.sleep(1)
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert mtime < response['ResponseMetadata']['HTTPHeaders']['last-modified']


def _flush_logs(client, src_bucket_name, dummy_key="dummy"):
    if _has_bucket_logging_extension():
        result = client.post_bucket_logging(Bucket=src_bucket_name)
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        return result['FlushedLoggingObject']
    else:
        time.sleep(expected_object_roll_time*1.1)
        client.put_object(Bucket=src_bucket_name, Key=dummy_key, Body='dummy')
        return None


def _verify_logging_key(key_format, key, expected_prefix, expected_dt, expected_src_account, expected_src_region, expected_src_bucket):
    # NOTE: prefix value has to be terminated with non alphanumeric character
    # for the regex to work properly
    if key_format == 'SimplePrefix':
        #[DestinationPrefix][YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
        pattern = r'^(.+?)(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$'
        no_prefix_pattern = r'^(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$'
    elif key_format == 'PartitionedPrefix':
        # [DestinationPrefix][SourceAccountId]/[SourceRegion]/[SourceBucket]/[YYYY]/[MM]/[DD]/[YYYY]-[MM]-[DD]-[hh]-[mm]-[ss]-[UniqueString]
        pattern = r'^(.+?)([^/]+)/([^/]+)/([^/]+)/(\d{4})/(\d{2})/(\d{2})/(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$'
        no_prefix_pattern = r'^([^/]+)/([^/]+)/([^/]+)/(\d{4})/(\d{2})/(\d{2})/(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(.+)$'
    else:
        assert False, 'unknown key format: {}'.format(key_format)

    match = re.match(no_prefix_pattern, key)
    if not match:
        match = re.match(pattern, key)
        assert match
        match_index = 1
        prefix = match.group(match_index)
    else:
        prefix = ''
        match_index = 0

    _year = None
    _month = None
    _day = None
    assert prefix == expected_prefix
    if key_format == 'PartitionedPrefix':
        match_index += 1
        src_account = match.group(match_index)
        assert src_account == expected_src_account
        match_index += 1
        src_region = match.group(match_index)
        assert src_region == expected_src_region
        match_index += 1
        src_bucket = match.group(match_index)
        assert src_bucket == expected_src_bucket
        match_index += 1
        _year = int(match.group(match_index))
        match_index += 1
        _month = int(match.group(match_index))
        match_index += 1
        _day = int(match.group(match_index))

    match_index += 1
    year = int(match.group(match_index))
    if _year:
        assert year == _year
    match_index += 1
    month = int(match.group(match_index))
    if _month:
        assert month == _month
    match_index += 1
    day = int(match.group(match_index))
    if _day:
        assert day == _day
    match_index += 1
    hour = int(match.group(match_index))
    match_index += 1
    minute = int(match.group(match_index))
    match_index += 1
    second = int(match.group(match_index))
    match_index += 1
    unique_string = match.group(match_index)
    try:
        dt = datetime.datetime(year, month, day, hour, minute, second)
        assert abs(dt - expected_dt) <= datetime.timedelta(seconds=5)
    except ValueError:
        # Invalid date/time values
        assert False, 'invalid date/time values in key: {}'.format(key)


def _bucket_logging_object_name(key_format, prefix):
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])

    logging_enabled = {
            'TargetBucket': log_bucket_name,
            'TargetPrefix': prefix
            }
    if key_format == 'SimplePrefix':
        logging_enabled['TargetObjectKeyFormat'] = {'SimplePrefix': {}}
    elif key_format == 'PartitionedPrefix':
        logging_enabled['TargetObjectKeyFormat'] = {
                'PartitionedPrefix': {
                    'PartitionDateSource': 'DeliveryTime'
                    }
                }
    else:
        assert False, 'unknown key format: %s' % key_format

    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    _flush_logs(client, src_bucket_name)
    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    user_id = get_main_user_id()
    timestamp = datetime.datetime.now()
    _verify_logging_key(key_format, keys[0], prefix, timestamp, user_id, 'default', src_bucket_name)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_simple_key():
    _bucket_logging_object_name('SimplePrefix', 'log/')
    _bucket_logging_object_name('SimplePrefix', '')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_partitioned_key():
    _bucket_logging_object_name('PartitionedPrefix', 'log/')
    _bucket_logging_object_name('PartitionedPrefix', '')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_bucket_auth_type():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    key = 'my-test-object'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])

    client.put_object(Bucket=src_bucket_name, Key=key, Body=randcontent())
    client.put_object_acl(ACL='public-read',Bucket=src_bucket_name, Key=key)

    response = client.list_objects_v2(Bucket=src_bucket_name)
    keys = _get_keys(response)

    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # AuthType AuthHeader:
    response = client.get_object(Bucket=src_bucket_name, Key=key)
    client.list_objects_v2(Bucket=src_bucket_name)

    _flush_logs(client, src_bucket_name)
    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)
    assert len(log_keys) == 1

    for log_key in log_keys:
        response = client.get_object(Bucket=log_bucket_name, Key=log_key)
        body = _get_body(response)
        assert _verify_records(body, src_bucket_name, 'REST.GET.OBJECT', [key], "Standard", 1)
        assert _verify_record_field(body, src_bucket_name, 'REST.GET.OBJECT', key, "Standard", "AuthType", "AuthHeader")


    #AuthType "-" (unauthenticated)
    unauthenticated_client = get_unauthenticated_client()
    unauthenticated_client.get_object(Bucket=src_bucket_name, Key=key)

    _flush_logs(client, src_bucket_name)
    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)

    log_key = log_keys[-1]
    response = client.get_object(Bucket=log_bucket_name, Key=log_key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.GET.OBJECT', [key], "Standard", 1)
    assert _verify_record_field(body, src_bucket_name, 'REST.GET.OBJECT', key, "Standard", "AuthType", "-")

    #AuthType "QueryString" (presigned)
    params = {'Bucket': src_bucket_name, 'Key': key}
    url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=100000, HttpMethod='GET')

    res = requests.options(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 400

    res = requests.get(url, verify=get_config_ssl_verify()).__dict__
    assert res['status_code'] == 200

    _flush_logs(client, src_bucket_name)
    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)

    log_key = log_keys[-1]
    response = client.get_object(Bucket=log_bucket_name, Key=log_key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.GET.OBJECT', [key], "Standard", 1)
    assert _verify_record_field(body, src_bucket_name, 'REST.GET.OBJECT', key, "Standard", "AuthType", "QueryString")


def _bucket_logging_key_filter(log_type):
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])

    logging_enabled = {
            'TargetBucket': log_bucket_name,
            'LoggingType': log_type,
            'TargetPrefix': prefix,
            'ObjectRollTime': expected_object_roll_time,
            'TargetObjectKeyFormat': {'SimplePrefix': {}},
            'RecordsBatchSize': 0,
             'Filter':
                {
                    'Key': {
                        'FilterRules': [
                            {'Name': 'prefix', 'Value': 'test/'},
                            {'Name': 'suffix', 'Value': '.txt'}
                        ]
                    }
                }
            }

    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    if log_type == 'Journal':
        assert response['LoggingEnabled'] == logging_enabled
    elif log_type == 'Standard':
        print('TODO')
    else:
        assert False, 'unknown log type: %s' % log_type

    names = []
    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        if log_type == 'Standard':
            # standard log records are not filtered
            names.append(name)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    for j in range(num_keys):
        name = 'test/'+'myobject'+str(j)+'.txt'
        names.append(name)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    expected_count = len(names)

    flushed_obj = _flush_logs(client, src_bucket_name, dummy_key="test/dummy.txt")

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    for key in keys:
        if flushed_obj is not None:
            assert key == flushed_obj
        assert key.startswith('log/')
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body = _get_body(response)
        assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', names, log_type, expected_count, exact_match=True)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_bucket_acl_required():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    key = 'my-test-object'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])

    client.put_object(Bucket=src_bucket_name, Key=key, Body=randcontent())
    client.put_bucket_acl(ACL='public-read',Bucket=src_bucket_name)

    response = client.list_objects_v2(Bucket=src_bucket_name)
    keys = _get_keys(response)

    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    #This will require ACLs
    alt_client = get_alt_client()
    response = alt_client.list_objects_v2(Bucket=src_bucket_name)
    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)
    assert len(log_keys) == 1

    for log_key in log_keys:
        response = client.get_object(Bucket=log_bucket_name, Key=log_key)
        body = _get_body(response)
        assert _verify_records(body, src_bucket_name, 'REST.GET.BUCKET', ["-"], "Standard", 1)
        assert _verify_record_field(body, src_bucket_name, 'REST.GET.BUCKET', "-", "Standard", "ACLRequired", "Yes")

    #Set a bucket policy that will allow access without requiring ACLs
    resource1 = "arn:aws:s3:::" + src_bucket_name
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:ListBucket",
        "Resource": [
            "{}".format(resource1),
          ]
        }]
     })

    response = client.put_bucket_policy(Bucket=src_bucket_name, Policy=policy_document)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response = alt_client.list_objects_v2(Bucket=src_bucket_name)
    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)

    log_key = log_keys[-1]
    response = client.get_object(Bucket=log_bucket_name, Key=log_key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.GET.BUCKET', ["-"], "Standard", 1)
    assert _verify_record_field(body, src_bucket_name, 'REST.GET.BUCKET', "-", "Standard", "ACLRequired", "-")


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_object_acl_required():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    key = 'my-test-object'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])

    client.put_object(Bucket=src_bucket_name, Key=key, Body=randcontent())
    client.put_object_acl(ACL='public-read',Bucket=src_bucket_name, Key=key)

    response = client.list_objects_v2(Bucket=src_bucket_name)
    keys = _get_keys(response)

    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    #This will require ACLs
    alt_client = get_alt_client()
    response = alt_client.get_object(Bucket=src_bucket_name, Key=key)
    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)
    assert len(log_keys) == 1

    for log_key in log_keys:
        response = client.get_object(Bucket=log_bucket_name, Key=log_key)
        body = _get_body(response)
        assert _verify_records(body, src_bucket_name, 'REST.GET.OBJECT', [key], "Standard", 1)
        assert _verify_record_field(body, src_bucket_name, 'REST.GET.OBJECT', key, "Standard", "ACLRequired", "Yes")

    #Set a bucket policy that will allow access to the object without requiring ACLs
    resource1 = "arn:aws:s3:::" + src_bucket_name
    resource2 = "arn:aws:s3:::" + src_bucket_name + "/*"
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": "*"},
        "Action": "s3:GetObject",
        "Resource": [
            "{}".format(resource1),
            "{}".format(resource2)
          ]
        }]
     })

    response = client.put_bucket_policy(Bucket=src_bucket_name, Policy=policy_document)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    response = alt_client.get_object(Bucket=src_bucket_name, Key=key)
    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)

    log_key = log_keys[-1]
    response = client.get_object(Bucket=log_bucket_name, Key=log_key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.GET.OBJECT', [key], "Standard", 1)
    assert _verify_record_field(body, src_bucket_name, 'REST.GET.OBJECT', key, "Standard", "ACLRequired", "-")



@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_key_filter_s():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _bucket_logging_key_filter('Standard')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_key_filter_j():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _bucket_logging_key_filter('Journal')


def _post_bucket_logging(client, src_bucket_name, flushed_objs):
    result = client.post_bucket_logging(Bucket=src_bucket_name)
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200
    flushed_objs[src_bucket_name] = result['FlushedLoggingObject']


def _bucket_logging_flush(logging_type, single_prefix, concurrency, num_keys):
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()

    num_buckets = 5
    buckets = []
    log_prefixes = []
    longer_time = expected_object_roll_time*10
    for j in range(num_buckets):
        src_bucket_name = get_new_bucket_name()
        src_bucket = get_new_bucket_resource(name=src_bucket_name)
        buckets.append(src_bucket_name)
        if single_prefix:
            log_prefixes.append('log/')
        else:
            log_prefixes.append(src_bucket_name+'/')

    _set_log_bucket_policy(client, log_bucket_name, buckets, log_prefixes)

    for j in range(num_buckets):
        logging_enabled = {'TargetBucket': log_bucket_name,
                           'ObjectRollTime': longer_time,
                           'LoggingType': logging_type,
                           'TargetPrefix': log_prefixes[j]
                           }

        response = client.put_bucket_logging(Bucket=buckets[j], BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    src_names = []
    for j in range(num_keys):
        src_names.append('myobject'+str(j))

    for src_bucket_name in buckets:
        for name in src_names:
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
            client.delete_object(Bucket=src_bucket_name, Key=name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 0

    t = []
    flushed_objs = {}
    for src_bucket_name in buckets:
        if concurrency:
            thr = threading.Thread(target = _post_bucket_logging,
                                   args=(client, src_bucket_name, flushed_objs))
            thr.start()
            t.append(thr)
        else:
            result = client.post_bucket_logging(Bucket=src_bucket_name)
            assert result['ResponseMetadata']['HTTPStatusCode'] == 200
            flushed_objs[src_bucket_name] = result['FlushedLoggingObject']
        if single_prefix:
            break

    _do_wait_completion(t)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)

    if single_prefix:
        assert len(keys) == 1
        assert len(flushed_objs) == 1
    else:
        assert len(keys) == num_buckets
        assert len(flushed_objs) >= num_buckets

    for key in keys:
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body = _get_body(response)
        found = False
        for j in range(num_buckets):
            prefix = log_prefixes[j]
            if key.startswith(prefix):
                flushed_obj = flushed_objs.get(buckets[j])
                if flushed_obj is not None:
                    assert key == flushed_obj
                found = True
                assert _verify_records(body, buckets[j], 'REST.PUT.OBJECT', src_names, logging_type, num_keys)
                assert _verify_records(body, buckets[j], 'REST.DELETE.OBJECT', src_names, logging_type, num_keys)
        assert found


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_flush_empty():
    # testing empty commits
    _bucket_logging_flush('Journal', False, False, 0)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_flush_j():
    _bucket_logging_flush('Journal', False, False, 10)
    _bucket_logging_flush('Journal', False, False, 100)
    # testing empty commits after some activity
    _bucket_logging_flush('Journal', False, False, 0)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_flush_s():
    _bucket_logging_flush('Standard', False, False, 10)
    _bucket_logging_flush('Standard', False, False, 100)
    # no empty commits in "standard" mode
    # the POST command itself will be logged


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_flush_j_single():
    _bucket_logging_flush('Journal', True, False, 10)
    _bucket_logging_flush('Journal', True, False, 100)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_flush_s_single():
    _bucket_logging_flush('Standard', True, False, 10)
    _bucket_logging_flush('Standard', True, False, 100)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_concurrent_flush_j():
    _bucket_logging_flush('Journal', False, True, 10)
    _bucket_logging_flush('Journal', False, True, 100)
    _bucket_logging_flush('Journal', False, True, 0)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_concurrent_flush_s():
    _bucket_logging_flush('Standard', False, True, 10)
    _bucket_logging_flush('Standard', False, True, 100)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_concurrent_flush_j_single():
    _bucket_logging_flush('Journal', True, True, 10)
    _bucket_logging_flush('Journal', True, True, 100)
    _bucket_logging_flush('Journal', True, True, 0)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_concurrent_flush_s_single():
    _bucket_logging_flush('Standard', True, True, 10)
    _bucket_logging_flush('Standard', True, True, 100)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_put_and_flush():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client_config = botocore.config.Config(max_pool_connections=100)
    client = get_client(client_config=client_config)

    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [log_prefix])

    logging_enabled = {'TargetBucket': log_bucket_name,
                       'LoggingType': 'Journal',
                       'TargetPrefix': log_prefix
                       }

    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 300
    flush_rate = 10
    put_threads = []
    flush_threads = []
    src_names = []
    for j in range(num_keys):
        name = 'myobject'+str(j)
        src_names.append(name)
        put_thr = threading.Thread(target=client.put_object,
                                      kwargs={'Bucket': src_bucket_name,
                                              'Key': name,
                                              'Body': randcontent()})
        put_thr.start()
        put_threads.append(put_thr)

        if j % flush_rate == 0:
            flush_thr = threading.Thread(target = client.post_bucket_logging,
                                         kwargs={'Bucket': src_bucket_name})
            flush_thr.start()
            flush_threads.append(flush_thr)

    _do_wait_completion(put_threads)
    _do_wait_completion(flush_threads)

    # making sure everything is flushed synchronously
    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)

    body = ''
    prev_key = ''
    for key in keys:
        logger.info('bucket log record: %s', key)
        assert key > prev_key
        prev_key = key
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body += _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_names, 'Journal', num_keys)


@pytest.mark.bucket_logging
def test_put_bucket_logging_errors():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name1 = get_new_bucket_name()
    log_bucket1 = get_new_bucket_resource(name=log_bucket_name1)
    client = get_client()
    prefix = 'log/'

    # invalid source bucket
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name+'kaboom', BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name1, 'TargetPrefix': prefix},
        })
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'NoSuchBucket'

    # invalid log bucket
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name1+'kaboom', 'TargetPrefix': prefix},
        })
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'NoSuchKey'

    # log bucket has bucket logging
    log_bucket_name2 = get_new_bucket_name()
    log_bucket2 = get_new_bucket_resource(name=log_bucket_name2)
    _set_log_bucket_policy(client, log_bucket_name1, [log_bucket_name2], [prefix])
    response = client.put_bucket_logging(Bucket=log_bucket_name2, BucketLoggingStatus={
        'LoggingEnabled': {'TargetBucket': log_bucket_name1, 'TargetPrefix': prefix},
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _set_log_bucket_policy(client, log_bucket_name2, [src_bucket_name], [prefix])
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name2, 'TargetPrefix': prefix},
        })
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'

    # invalid partition prefix
    if _has_target_object_key_format():
        logging_enabled = {
            'TargetBucket': log_bucket_name1,
            'TargetPrefix': prefix,
            'TargetObjectKeyFormat': {
                'PartitionedPrefix': {
                    'PartitionDateSource': 'kaboom'
                }
            }
        }
        try:
            _set_log_bucket_policy(client, log_bucket_name1, [src_bucket_name], [prefix])
            response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
                'LoggingEnabled': logging_enabled,
            })
            assert False, 'expected failure'
        except ClientError as e:
            assert e.response['Error']['Code'] == 'MalformedXML'

    # log bucket is the same as source bucket
    _set_log_bucket_policy(client, src_bucket_name, [src_bucket_name], [prefix])
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': src_bucket_name, 'TargetPrefix': prefix},
        })
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'

    # log bucket is encrypted
    _put_bucket_encryption_s3(client, log_bucket_name1)
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name1, 'TargetPrefix': prefix},
        })
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'

    # "requester pays" is set on log bucket
    log_bucket_name3 = get_new_bucket_name()
    log_bucket3 = get_new_bucket_resource(name=log_bucket_name3)
    _set_log_bucket_policy(client, log_bucket_name3, [src_bucket_name], [prefix])
    response = client.put_bucket_request_payment(Bucket=log_bucket_name3,
                                                 RequestPaymentConfiguration={'Payer': 'Requester'})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name3, 'TargetPrefix': prefix},
        })
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'InvalidArgument'

    # invalid log type
    if _has_bucket_logging_extension():
        try:
            response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
                'LoggingEnabled': {'TargetBucket': log_bucket_name1, 'TargetPrefix': prefix, 'LoggingType': 'kaboom'},
            })
            assert False, 'expected failure'
        except ClientError as e:
            assert e.response['Error']['Code'] == 'MalformedXML'


def _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix):
    try:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}})
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'AccessDenied'


@pytest.mark.bucket_logging
def test_bucket_logging_owner():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    alt_client = get_alt_client()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])

    # set policy to allow all action on source bucket
    policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{
        "Effect": "Allow",
        "Principal": "*",
        "Action": ["s3:PutBucketLogging"],
        }]
     })

    response = client.put_bucket_policy(Bucket=src_bucket_name, Policy=policy_document)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    # try set bucket logging from another user
    _verify_access_denied(alt_client, src_bucket_name, log_bucket_name, prefix)

    # set bucket logging from the bucket owner user
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # try remove bucket logging from another user
    try:
        response = alt_client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={})
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'AccessDenied'


def _set_bucket_policy(client, log_bucket_name, policy):
    policy_document = json.dumps(policy)
    response = client.put_bucket_policy(Bucket=log_bucket_name, Policy=policy_document)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204


@pytest.mark.bucket_logging
def test_put_bucket_logging_permissions():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    log_tenant = ""
    src_tenant = ""
    src_user = get_main_user_id()

    # missing log bucket policy
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)

    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"Service": "logging.s3.amazonaws.com"},
            "Action": ["s3:PutObject"],
            "Resource": "arn:aws:s3::{}:{}/{}".format(log_tenant, log_bucket_name, prefix),
            "Condition": {
                "ArnLike": {"aws:SourceArn": "arn:aws:s3::{}:{}".format(src_tenant, src_bucket_name)},
                "StringEquals": {
                    "aws:SourceAccount": "{}${}".format(src_tenant, src_user) if src_tenant else src_user
                    }
            }
        }]
    }

    # missing service principal
    policy['Statement'][0]['Principal'] = {"AWS": "*"}
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)

    # invalid service principal
    policy['Statement'][0]['Principal'] = {"Service": "logging.s3.amazonaws.com"+"kaboom"}
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)
    # set valid principal
    policy['Statement'][0]['Principal'] = {"Service": "logging.s3.amazonaws.com"}

    # invalid action
    policy['Statement'][0]['Action'] = ["s3:GetObject"]
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)
    # set valid action
    policy['Statement'][0]['Action'] = ["s3:PutObject"]

    # invalid resource
    policy['Statement'][0]['Resource'] = "arn:aws:s3::{}:{}/{}".format(log_tenant, log_bucket_name, "kaboom")
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)
    # set valid resource
    policy['Statement'][0]['Resource'] = "arn:aws:s3::{}:{}/{}".format(log_tenant, log_bucket_name, prefix)

    # invalid source bucket
    policy['Statement'][0]['Condition']['ArnLike']['aws:SourceArn'] = "arn:aws:s3::{}:{}".format(src_tenant, "kaboom")
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)
    policy['Statement'][0]['Condition']['ArnLike']['aws:SourceArn'] = "arn:aws:s3::{}:{}".format("kaboom", src_bucket)
    # set valid source bucket
    policy['Statement'][0]['Condition']['ArnLike']['aws:SourceArn'] = "arn:aws:s3::{}:{}".format(src_tenant, src_bucket)

    # invalid source account
    src_user = "kaboom"
    policy['Statement'][0]['Condition']['StringEquals']['aws:SourceAccount'] = "{}${}".format(src_tenant, src_user) if src_tenant else src_user
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)
    src_user = get_main_user_id()
    src_tenant = "kaboom"
    policy['Statement'][0]['Condition']['StringEquals']['aws:SourceAccount'] = "{}${}".format(src_tenant, src_user) if src_tenant else src_user
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(client, src_bucket_name, log_bucket_name, prefix)


def _put_bucket_logging_policy_wildcard(add_objects):
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    alt_client = get_alt_client()
    alt_src_bucket_name = get_new_bucket_name()
    alt_src_bucket = get_new_bucket(client=alt_client, name=alt_src_bucket_name)
    prefix = 'log/'
    log_tenant = ""
    src_tenant = ""
    src_user = get_main_user_id()
    alt_user = get_alt_user_id()

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ServerAccessLogsPolicy",
                "Effect": "Allow",
                "Principal": {"Service": "logging.s3.amazonaws.com"},
                "Action": ["s3:PutObject"],
                "Resource": "arn:aws:s3::{}:{}/{}".format(log_tenant, log_bucket_name, prefix),
                "Condition": {
                    "ArnLike": {"aws:SourceArn": "arn:aws:s3::{}:{}".format(src_tenant, src_bucket_name)},
                    "StringLike": {
                        "aws:SourceAccount": "{}${}".format(src_tenant, src_user) if src_tenant else src_user
                        }
                }
            },
            {
                "Sid": "S3ServerAccessLogsPolicy",
                "Effect": "Allow",
                "Principal": {"Service": "logging.s3.amazonaws.com"},
                "Action": ["s3:PutObject"],
                "Resource": "arn:aws:s3::{}:{}/{}".format(log_tenant, log_bucket_name, prefix),
                "Condition": {
                    "ArnLike": {"aws:SourceArn": "arn:aws:s3::{}:{}".format(src_tenant, alt_src_bucket_name)},
                    "StringLike": {
                        "aws:SourceAccount": "{}${}".format(src_tenant, src_user) if src_tenant else alt_user
                        }
                }
            }
        ]
    }

    # verify policy is ok
    _set_bucket_policy(client, log_bucket_name, policy)
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = alt_client.put_bucket_logging(Bucket=alt_src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # remove the 2nd statement from the policy
    # and make sure it fails
    policy['Statement'].pop()
    _set_bucket_policy(client, log_bucket_name, policy)
    _verify_access_denied(alt_client, alt_src_bucket_name, log_bucket_name, prefix)

    # use wildcards account/bucket
    policy['Statement'][0]['Condition']['StringLike']['aws:SourceAccount'] = "*"
    policy['Statement'][0]['Condition']['ArnLike']['aws:SourceArn'] = "arn:aws:s3::{}:*".format(src_tenant)
    _set_bucket_policy(client, log_bucket_name, policy)
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = alt_client.put_bucket_logging(Bucket=alt_src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    if not add_objects:
        return

    # put objects when policy is ok
    num_keys = 5
    src_keys = []
    for j in range(num_keys):
        name = 'myobject_1_'+str(j)
        src_keys.append(name)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        name = 'myobject_2_'+str(j)
        src_keys.append(name)
        alt_client.put_object(Bucket=alt_src_bucket_name, Key=name, Body=randcontent())

    _flush_logs(client, src_bucket_name)
    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1
    for key in keys:
        assert key.startswith('log/')
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body = _get_body(response)
        assert _verify_records(body, " ", 'REST.PUT.OBJECT', src_keys, "Standard", num_keys*2)

    # non wildcard source account policy
    policy['Statement'][0]['Condition']['StringLike']['aws:SourceAccount'] = "{}${}".format(src_tenant, src_user) if src_tenant else src_user
    _set_bucket_policy(client, log_bucket_name, policy)
    for j in range(num_keys):
        name = 'myobject_2_'+str(j)
        src_keys.append(name)
        alt_client.put_object(Bucket=alt_src_bucket_name, Key=name, Body=randcontent())
    try:
        _flush_logs(alt_client, alt_src_bucket_name)
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'AccessDenied'
    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    # no new keys were added
    assert len(keys) == 1


@pytest.mark.bucket_logging
def test_put_bucket_logging_policy_wildcard():
    _put_bucket_logging_policy_wildcard(False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_put_bucket_logging_policy_wildcard_objects():
    _put_bucket_logging_policy_wildcard(True)


def _bucket_logging_permission_change(logging_type):
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    log_tenant = ""
    src_tenant = ""
    src_user = get_main_user_id()

    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "S3ServerAccessLogsPolicy",
            "Effect": "Allow",
            "Principal": {"Service": "logging.s3.amazonaws.com"},
            "Action": ["s3:PutObject"],
            "Resource": "arn:aws:s3::{}:{}/{}".format(log_tenant, log_bucket_name, prefix),
            "Condition": {
                "ArnLike": {"aws:SourceArn": "arn:aws:s3::{}:{}".format(src_tenant, src_bucket_name)},
                "StringEquals": {
                    "aws:SourceAccount": "{}${}".format(src_tenant, src_user) if src_tenant else src_user
                    }
            }
        }]
    }

    policy_document = json.dumps(policy)
    response = client.put_bucket_policy(Bucket=log_bucket_name, Policy=policy_document)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204
    if logging_type == 'Journal':
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix, 'LoggingType': logging_type}})
    else:
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    # put objects when policy is ok
    num_keys = 5
    good_src_keys = []
    for j in range(num_keys):
        name = 'myobject'+str(j)
        good_src_keys.append(name)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
    # change policy to invalid source bucket
    policy['Statement'][0]['Condition']['ArnLike']['aws:SourceArn'] = "arn:aws:s3::{}:{}".format(src_tenant, "kaboom")
    _set_bucket_policy(client, log_bucket_name, policy)

    bad_src_keys = []
    # put objects when policy should reject them
    for j in range(num_keys, num_keys*2):
        name = 'myobject'+str(j)
        bad_src_keys.append(name)
        if logging_type == 'Standard':
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        else:
            try:
                client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
                assert False, 'expected failure'
            except ClientError as e:
                assert e.response['Error']['Code'] == 'AccessDenied'
                assert e.response['Error']['Message'].startswith('Logging bucket')

    try:
        _flush_logs(client, src_bucket_name)
        assert False, 'expected failure'
    except ClientError as e:
        assert e.response['Error']['Code'] == 'AccessDenied'

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 0


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_permission_change_s():
    _bucket_logging_permission_change('Standard')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_permission_change_j():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _bucket_logging_permission_change('Journal')


def _bucket_logging_objects(src_client, src_bucket_name, log_client, log_bucket_name, log_type, op_name):
    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        src_client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    expected_count = num_keys

    response = src_client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)

    flushed_obj = _flush_logs(src_client, src_bucket_name)

    response = log_client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    for key in keys:
        if flushed_obj is not None:
            assert key == flushed_obj
        assert key.startswith('log/')
        response = log_client.get_object(Bucket=log_bucket_name, Key=key)
        body = _get_body(response)
        assert _verify_records(body, src_bucket_name, op_name, src_keys, log_type, expected_count)


def _put_bucket_logging_tenant(log_type):
    # src is on default tenant and log is on a different tenant
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    tenant_client = get_tenant_client()
    log_bucket = get_new_bucket(client=tenant_client, name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    log_tenant = get_tenant_name()
    _set_log_bucket_policy_tenant(tenant_client, log_tenant, log_bucket_name, "", get_main_user_id(), [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_tenant+':'+log_bucket_name, 'TargetPrefix': prefix}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, tenant_client, log_bucket_name, log_type, 'REST.PUT.OBJECT')

    # src is on default tenant and log is on a different tenant with the same name
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = src_bucket_name
    tenant_client = get_tenant_client()
    log_bucket = get_new_bucket(client=tenant_client, name=log_bucket_name)
    client = get_client()
    tenant_name = get_tenant_name()
    _set_log_bucket_policy_tenant(tenant_client, tenant_name, log_bucket_name, "", get_main_user_id(),  [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': tenant_name+':'+log_bucket_name, 'TargetPrefix': prefix}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, tenant_client, log_bucket_name, log_type, 'REST.PUT.OBJECT')


    try:
        # src is on default tenant and log is on a different tenant
        # log bucket name not set correctly
        src_bucket_name = get_new_bucket_name()
        src_bucket = get_new_bucket_resource(name=src_bucket_name)
        log_bucket_name = get_new_bucket_name()
        log_client = get_tenant_client()
        log_bucket = get_new_bucket(client=log_client, name=log_bucket_name)
        client = get_client()
        _set_log_bucket_policy_tenant(log_client, get_tenant_name(), log_bucket_name, "", get_main_user_id(), [src_bucket_name], [prefix])
        logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
        if log_type == 'Journal':
            logging_enabled['LoggingType'] = 'Journal'
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
    except ClientError as e:
        assert e.response['Error']['Code'] == 'NoSuchKey'
    else:
        assert False, 'expected failure'

    # src and log are on the same tenant
    # log bucket name is set with tenant
    client = get_tenant_client()
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket(client=client, name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket(client=client, name=log_bucket_name)
    tenant_name = get_tenant_name()
    _set_log_bucket_policy_tenant(client, tenant_name, log_bucket_name, tenant_name, get_tenant_user_id(), [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': tenant_name+':'+log_bucket_name, 'TargetPrefix': prefix}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, client, log_bucket_name, log_type, 'REST.PUT.OBJECT')

    # src and log are on the same tenant
    # log bucket name is set without tenant
    client = get_tenant_client()
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket(client=client, name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket(client=client, name=log_bucket_name)
    tenant_name = get_tenant_name()
    _set_log_bucket_policy_tenant(client, tenant_name, log_bucket_name, tenant_name, get_tenant_user_id(), [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, client, log_bucket_name, log_type, 'REST.PUT.OBJECT')

    # src is on tenant and log is on the default tenant
    # log bucket name is set with explicit default tenant
    client = get_tenant_client()
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket(client=client, name=src_bucket_name)
    log_client = get_client()
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket(client=log_client, name=log_bucket_name)
    _set_log_bucket_policy_tenant(log_client, "", log_bucket_name, get_tenant_name(), get_tenant_user_id(), [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': ':'+log_bucket_name, 'TargetPrefix': 'log/'}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, log_client, log_bucket_name, log_type, 'REST.PUT.OBJECT')

    try:
        # src is on tenant and log is on the default tenant
        client = get_tenant_client()
        src_bucket_name = get_new_bucket_name()
        src_bucket = get_new_bucket(client=client, name=src_bucket_name)
        log_bucket_name = get_new_bucket_name()
        log_client = get_client()
        log_bucket = get_new_bucket(client=log_client, name=log_bucket_name)
        _set_log_bucket_policy_tenant(log_client, "", log_bucket_name, get_tenant_name(), get_tenant_user_id(), [src_bucket_name], [prefix])
        logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': 'log/'}
        if log_type == 'Journal':
            logging_enabled['LoggingType'] = 'Journal'
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
    except ClientError as e:
        assert e.response['Error']['Code'] == 'NoSuchKey'
    else:
        assert False, 'expected failure'


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_put_bucket_logging_tenant_s():
    _put_bucket_logging_tenant('Standard')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_put_bucket_logging_tenant_j():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _put_bucket_logging_tenant('Journal')


def _put_bucket_logging_account(log_type):
    # src is default user and log is in an account user
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_client = get_iam_root_s3client()
    log_bucket = get_new_bucket(client=log_client, name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    _set_log_bucket_policy_tenant(log_client, "", log_bucket_name, "", get_main_user_id(), [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, log_client, log_bucket_name, log_type, 'REST.PUT.OBJECT')

    # src and log are in an iam account
    client = get_iam_root_s3client()
    iam_client = get_iam_root_client()
    iam_user = iam_client.get_user()['User']['Arn'].split(':')[-2]  # Get the IAM user name from ARN
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket(client=client, name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket(client=client, name=log_bucket_name)
    _set_log_bucket_policy_tenant(client, "", log_bucket_name, "", iam_user, [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, client, log_bucket_name, log_type, 'REST.PUT.OBJECT')

    # src is in an iam account and log is the default user
    client = get_iam_root_s3client()
    iam_client = get_iam_root_client()
    iam_user = iam_client.get_user()['User']['Arn'].split(':')[-2]  # Get the IAM user name from ARN
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket(client=client, name=src_bucket_name)
    log_client = get_client()
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket(client=log_client, name=log_bucket_name)
    _set_log_bucket_policy_tenant(log_client, "", log_bucket_name, "", iam_user, [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': 'log/'}
    if log_type == 'Journal':
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    _bucket_logging_objects(client, src_bucket_name, log_client, log_bucket_name, log_type, 'REST.PUT.OBJECT')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_put_bucket_logging_account_s():
    _put_bucket_logging_account('Standard')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_put_bucket_logging_account_j():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _put_bucket_logging_account('Journal')


@pytest.mark.bucket_logging
def test_rm_bucket_logging():
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={})
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert not 'LoggingEnabled' in response


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_put_bucket_logging_extensions():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name,
                       'TargetPrefix': prefix,
                       'LoggingType': 'Standard',
                       'ObjectRollTime': expected_object_roll_time,
                       'RecordsBatchSize': 0
                       }
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.get_bucket_logging(Bucket=src_bucket_name)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    logging_enabled['TargetObjectKeyFormat'] = {'SimplePrefix': {}}
    assert response['LoggingEnabled'] == logging_enabled


def _bucket_logging_put_objects(versioned):
    src_bucket_name = get_new_bucket()
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])

    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        if versioned:
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    if versioned:
        expected_count = 2*num_keys
    else:
        expected_count = num_keys

    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)

    flushed_obj = _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    record_type = 'Standard' if not has_extensions else 'Journal'

    for key in keys:
        if flushed_obj is not None:
            assert key == flushed_obj
        assert key.startswith('log/')
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body = _get_body(response)
        assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, record_type, expected_count)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_put_objects():
    _bucket_logging_put_objects(False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_put_objects_versioned():
    _bucket_logging_put_objects(True)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_put_concurrency():
    src_bucket_name = get_new_bucket()
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client(client_config=botocore.config.Config(max_pool_connections=50))
    has_extensions = _has_bucket_logging_extension()

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 50
    t = []
    for i in range(num_keys):
        name = 'myobject'+str(i)
        thr = threading.Thread(target = client.put_object,
                               kwargs={'Bucket': src_bucket_name, 'Key': name, 'Body': randcontent()})
        thr.start()
        t.append(thr)
    _do_wait_completion(t)

    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)
    if not has_extensions:
        time.sleep(expected_object_roll_time*1.1)
    t = []
    for i in range(num_keys):
        if has_extensions:
            thr = threading.Thread(target = client.post_bucket_logging,
                                   kwargs={'Bucket': src_bucket_name})
        else:
            thr = threading.Thread(target = client.put_object,
                                   kwargs={'Bucket': src_bucket_name, 'Key': 'dummy', 'Body': 'dummy'})
        thr.start()
        t.append(thr)
    _do_wait_completion(t)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)

    record_type = 'Standard' if not has_extensions else 'Journal'

    body = ""
    for key in keys:
        assert key.startswith('log/')
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body += _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, record_type, num_keys)


def _bucket_logging_delete_objects(versioned):
    src_bucket_name = get_new_bucket()
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        if versioned:
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)
    for key in src_keys:
        if versioned:
            response = client.head_object(Bucket=src_bucket_name, Key=key)
            client.delete_object(Bucket=src_bucket_name, Key=key, VersionId=response['VersionId'])
        client.delete_object(Bucket=src_bucket_name, Key=key)

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    if versioned:
        expected_count = 2*num_keys
    else:
        expected_count = num_keys

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    record_type = 'Standard' if not has_extensions else 'Journal'
    assert _verify_records(body, src_bucket_name, 'REST.DELETE.OBJECT', src_keys, record_type, expected_count)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_delete_objects():
    _bucket_logging_delete_objects(False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_delete_objects_versioned():
    _bucket_logging_delete_objects(True)


@pytest.mark.bucket_logging
def _bucket_logging_get_objects(versioned):
    src_bucket_name = get_new_bucket()
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        if versioned:
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Standard'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)
    for key in src_keys:
        if versioned:
            response = client.head_object(Bucket=src_bucket_name, Key=key)
            client.get_object(Bucket=src_bucket_name, Key=key, VersionId=response['VersionId'])
        client.get_object(Bucket=src_bucket_name, Key=key)

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    if versioned:
        expected_count = 2*num_keys
    else:
        expected_count = num_keys

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.GET.OBJECT', src_keys, 'Standard', expected_count)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_get_objects():
    _bucket_logging_get_objects(False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_get_objects_versioned():
    _bucket_logging_get_objects(True)


@pytest.mark.bucket_logging
def _bucket_logging_copy_objects(versioned, another_bucket):
    src_bucket_name = get_new_bucket()
    if another_bucket:
        dst_bucket_name = get_new_bucket()
    else:
        dst_bucket_name = src_bucket_name
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        if versioned:
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name, dst_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    if another_bucket:
        response = client.put_bucket_logging(Bucket=dst_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)
    dst_keys = []
    for key in src_keys:
        dst_keys.append('copy_of_'+key)
        if another_bucket:
            client.copy_object(Bucket=dst_bucket_name, Key='copy_of_'+key, CopySource={'Bucket': src_bucket_name, 'Key': key})
        else:
            client.copy_object(Bucket=src_bucket_name, Key='copy_of_'+key, CopySource={'Bucket': src_bucket_name, 'Key': key})

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    record_type = 'Standard' if not has_extensions else 'Journal'
    assert _verify_records(body, dst_bucket_name, 'REST.PUT.OBJECT', dst_keys, record_type, num_keys)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_copy_objects():
    _bucket_logging_copy_objects(False, False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_copy_objects_versioned():
    _bucket_logging_copy_objects(True, False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_copy_objects_bucket():
    _bucket_logging_copy_objects(False, True)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_copy_objects_bucket_versioned():
    _bucket_logging_copy_objects(True, True)


@pytest.mark.bucket_logging
def _bucket_logging_head_objects(versioned):
    src_bucket_name = get_new_bucket()
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Standard'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)
    for key in src_keys:
        if versioned:
            response = client.head_object(Bucket=src_bucket_name, Key=key)
            client.head_object(Bucket=src_bucket_name, Key=key, VersionId=response['VersionId'])
        else:
            client.head_object(Bucket=src_bucket_name, Key=key)

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    if versioned:
        expected_count = 2*num_keys
    else:
        expected_count = num_keys

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.HEAD.OBJECT', src_keys, 'Standard', expected_count)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_head_objects():
    _bucket_logging_head_objects(False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_head_objects_versioned():
    _bucket_logging_head_objects(True)


@pytest.mark.bucket_logging
def _bucket_logging_mpu(versioned, record_type):
    src_bucket_name = get_new_bucket()
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if record_type == 'Journal':
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    src_key = "myobject"
    objlen = 30 * 1024 * 1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=src_bucket_name, key=src_key, size=objlen)
    client.complete_multipart_upload(Bucket=src_bucket_name, Key=src_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    if versioned:
        (upload_id, data, parts) = _multipart_upload(bucket_name=src_bucket_name, key=src_key, size=objlen)
        client.complete_multipart_upload(Bucket=src_bucket_name, Key=src_key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    if versioned:
        expected_count = 4 if not record_type == 'Journal' else 2
    else:
        expected_count = 2 if not record_type == 'Journal' else 1

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.POST.UPLOAD', [src_key, src_key], record_type, expected_count)
    if record_type == 'Standard':
        if versioned:
            expected_count = 12
        else:
            expected_count = 6
        assert _verify_records(body, src_bucket_name, 'REST.PUT.PART', [src_key, src_key], record_type, expected_count)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_mpu_s():
    _bucket_logging_mpu(False, 'Standard')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_mpu_versioned_s():
    _bucket_logging_mpu(True, 'Standard')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_mpu_j():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _bucket_logging_mpu(False, 'Journal')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_mpu_versioned_j():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _bucket_logging_mpu(True, 'Journal')

@pytest.mark.bucket_logging
def _bucket_logging_mpu_copy(versioned):
    src_bucket_name = get_new_bucket()
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    src_key = "myobject"
    objlen = 30 * 1024 * 1024
    (upload_id, data, parts) = _multipart_upload(bucket_name=src_bucket_name, key=src_key, size=objlen)
    client.complete_multipart_upload(Bucket=src_bucket_name, Key=src_key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    if versioned:
        (upload_id, data, parts) = _multipart_upload(bucket_name=src_bucket_name, key=src_key, size=objlen)
        client.complete_multipart_upload(Bucket=src_bucket_name, Key=src_key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    client.copy_object(Bucket=src_bucket_name, Key='copy_of_'+src_key, CopySource={'Bucket': src_bucket_name, 'Key': src_key})

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    record_type = 'Standard' if not has_extensions else 'Journal'
    assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', ['copy_of_'+src_key], record_type, 1)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_mpu_copy():
    _bucket_logging_mpu_copy(False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_mpu_copy_versioned():
    _bucket_logging_mpu_copy(True)


def _bucket_logging_multi_delete(versioned):
    src_bucket_name = get_new_bucket()
    if versioned:
        check_configure_versioning_retry(src_bucket_name, "Enabled", "Enabled")
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        if versioned:
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    # minimal configuration
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix}
    if has_extensions:
        logging_enabled['ObjectRollTime'] = expected_object_roll_time
        logging_enabled['LoggingType'] = 'Journal'
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)
    if versioned:
        response = client.list_object_versions(Bucket=src_bucket_name)
        objs_list = []
        for version in response['Versions']:
            obj_dict = {'Key': version['Key'], 'VersionId': version['VersionId']}
            objs_list.append(obj_dict)
        objs_dict = {'Objects': objs_list}
        client.delete_objects(Bucket=src_bucket_name, Delete=objs_dict)
    else:
        objs_dict = _make_objs_dict(key_names=src_keys)
        client.delete_objects(Bucket=src_bucket_name, Delete=objs_dict)

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    if versioned:
        expected_count = 2*num_keys
    else:
        expected_count = num_keys

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    record_type = 'Standard' if not has_extensions else 'Journal'
    assert _verify_records(body, src_bucket_name, "REST.POST.DELETE_MULTI_OBJECT", src_keys, record_type, expected_count)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_multi_delete():
    _bucket_logging_multi_delete(False)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_multi_delete_versioned():
    _bucket_logging_multi_delete(True)


def _bucket_logging_type(logging_type):
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    logging_enabled = {
            'TargetBucket': log_bucket_name,
            'TargetPrefix': prefix,
            'ObjectRollTime': expected_object_roll_time,
            'LoggingType': logging_type
            }
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        client.head_object(Bucket=src_bucket_name, Key=name)

    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    if logging_type == 'Journal':
        assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, 'Journal', num_keys)
        assert _verify_records(body, src_bucket_name, 'REST.HEAD.OBJECT', src_keys, 'Journal', num_keys) == False
    elif logging_type == 'Standard':
        assert _verify_records(body, src_bucket_name, 'REST.HEAD.OBJECT', src_keys, 'Standard', num_keys)
        assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, 'Standard', num_keys)
    else:
        assert False, 'invalid logging type:'+logging_type


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_event_type_j():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _bucket_logging_type('Journal')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_event_type_s():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    _bucket_logging_type('Standard')


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_roll_time():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    src_bucket_name = get_new_bucket_name()
    src_bucket = get_new_bucket_resource(name=src_bucket_name)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    roll_time = 10
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix, 'ObjectRollTime': roll_time}
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 5
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)

    time.sleep(roll_time/2)
    client.put_object(Bucket=src_bucket_name, Key='myobject', Body=randcontent())

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 0

    time.sleep(roll_time/2)
    client.put_object(Bucket=src_bucket_name, Key='myobject', Body=randcontent())

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    key = keys[0]
    assert key.startswith('log/')
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, 'Standard', num_keys)
    client.delete_object(Bucket=log_bucket_name, Key=key)

    num_keys = 25
    for j in range(num_keys):
        name = 'myobject'+str(j)
        client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
        time.sleep(1)

    response = client.list_objects_v2(Bucket=src_bucket_name)
    src_keys = _get_keys(response)

    time.sleep(roll_time)
    client.put_object(Bucket=src_bucket_name, Key='myobject', Body=randcontent())

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) > 1

    body = ''
    for key in keys:
        assert key.startswith('log/')
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body += _get_body(response)
    assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, 'Standard', num_keys+1)


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_multiple_prefixes():
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    num_buckets = 5
    log_prefixes = []
    buckets = []
    bucket_name_prefix = get_new_bucket_name()
    for j in range(num_buckets):
        src_bucket_name = bucket_name_prefix+str(j)
        src_bucket = get_new_bucket_resource(name=src_bucket_name)
        log_prefixes.append(src_bucket_name+'/')
        buckets.append(src_bucket_name)

    _set_log_bucket_policy(client, log_bucket_name, buckets, log_prefixes)

    for j in range(num_buckets):
        logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': log_prefixes[j]}
        if has_extensions:
            logging_enabled['ObjectRollTime'] = expected_object_roll_time
        response = client.put_bucket_logging(Bucket=buckets[j], BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 5
    for src_bucket_name in buckets:
        for j in range(num_keys):
            name = 'myobject'+str(j)
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    for src_bucket_name in buckets:
        _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) >= num_buckets

    for key in keys:
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body = _get_body(response)
        found = False
        for src_bucket_name in buckets:
            if key.startswith(src_bucket_name):
                found = True
                response = client.list_objects_v2(Bucket=src_bucket_name)
                src_keys = _get_keys(response)
                assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, 'Standard', num_keys)
        assert found


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
@pytest.mark.fails_without_logging_rollover
def test_bucket_logging_single_prefix():
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()
    has_extensions = _has_bucket_logging_extension()

    num_buckets = 5
    buckets = []
    prefix = 'log/'
    bucket_name_prefix = get_new_bucket_name()
    for j in range(num_buckets):
        src_bucket_name = bucket_name_prefix+str(j)
        src_bucket = get_new_bucket_resource(name=src_bucket_name)
        buckets.append(src_bucket_name)

    _set_log_bucket_policy(client, log_bucket_name, buckets, [prefix])

    for j in range(num_buckets):
        # minimal configuration
        logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': 'log/'}
        if has_extensions:
            logging_enabled['ObjectRollTime'] = expected_object_roll_time
        response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 5
    bucket_ind = 0
    for src_bucket_name in buckets:
        bucket_ind += 1
        for j in range(num_keys):
            name = 'myobject'+str(bucket_ind)+str(j)
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())

    _flush_logs(client, src_bucket_name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 1

    key = keys[0]
    response = client.get_object(Bucket=log_bucket_name, Key=key)
    body = _get_body(response)
    found = False
    for src_bucket_name in buckets:
        response = client.list_objects_v2(Bucket=src_bucket_name)
        src_keys = _get_keys(response)
        found = _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_keys, 'Standard', num_keys)
    assert found


@pytest.mark.bucket_logging
@pytest.mark.fails_on_aws
def test_bucket_logging_object_meta():
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    client = get_client()
    src_bucket_name = get_new_bucket_name()
    src_bucket = client.create_bucket(Bucket=src_bucket_name, ObjectLockEnabledForBucket=True)
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)

    prefix = 'log/'
    _set_log_bucket_policy(client, log_bucket_name, [src_bucket_name], [prefix])
    logging_enabled = {'TargetBucket': log_bucket_name, 'TargetPrefix': prefix, 'LoggingType': 'Journal'}
    response = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
        'LoggingEnabled': logging_enabled,
    })
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    name = 'myobject'
    response = client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
    version_id = response['VersionId']
    # PutObjectAcl
    client.put_object_acl(ACL='public-read-write', Bucket=src_bucket_name, Key=name, VersionId=version_id)
    # PutObjectTagging
    client.put_object_tagging(Bucket=src_bucket_name, Key=name, VersionId=version_id,
                              Tagging={'TagSet': [{'Key': 'tag1', 'Value': 'value1'}, {'Key': 'tag2', 'Value': 'value2'}]})
    # DeleteObjectTagging
    client.delete_object_tagging(Bucket=src_bucket_name, Key=name, VersionId=version_id)
    # PutObjectLegalHold
    client.put_object_legal_hold(Bucket=src_bucket_name, Key=name, LegalHold={'Status': 'ON'})
    # PutObjectRetention
    client.put_object_retention(Bucket=src_bucket_name, Key=name, Retention={'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2026, 1, 1)})

    _flush_logs(client, src_bucket_name)
    response = client.list_objects_v2(Bucket=log_bucket_name)
    log_keys = _get_keys(response)
    assert len(log_keys) == 1
    log_key = log_keys[0]
    expected_op_list = ['REST.PUT.OBJECT', 'REST.PUT.ACL', 'REST.PUT.LEGAL_HOLD', 'REST.PUT.RETENTION', 'REST.PUT.OBJECT_TAGGING', 'REST.DELETE.OBJECT_TAGGING']
    op_list = []
    response = client.get_object(Bucket=log_bucket_name, Key=log_key)
    body = _get_body(response)
    for record in iter(body.splitlines()):
        parsed_record = _parse_log_record(record, 'Journal')
        logger.info('bucket log record: %s', json.dumps(parsed_record, indent=4))
        op_list.append(parsed_record['Operation'])
        version = parsed_record['VersionID']
        if version != '-':
            assert version == version_id

    assert sorted(expected_op_list) == sorted(op_list)

    # allow cleanup
    client.put_object_legal_hold(Bucket=src_bucket_name, Key=name, LegalHold={'Status': 'OFF'})
    client.delete_object(Bucket=src_bucket_name, Key=name, VersionId=version_id, BypassGovernanceRetention=True)

def _verify_flushed_on_put(result):
    if _has_bucket_logging_extension():
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200
        return result['FlushedLoggingObject']


def _bucket_logging_cleanup(cleanup_type, logging_type, single_prefix, concurrency):
    if not _has_bucket_logging_extension():
        pytest.skip('ceph extension to bucket logging not supported at client')
    log_bucket_name = get_new_bucket_name()
    log_bucket = get_new_bucket_resource(name=log_bucket_name)
    client = get_client()

    num_buckets = 5
    buckets = []
    log_prefixes = []
    longer_time = expected_object_roll_time*10
    for j in range(num_buckets):
        src_bucket_name = get_new_bucket_name()
        src_bucket = get_new_bucket_resource(name=src_bucket_name)
        buckets.append(src_bucket_name)
        if single_prefix:
            log_prefixes.append('log/')
        else:
            log_prefixes.append(src_bucket_name+'/')

    _set_log_bucket_policy(client, log_bucket_name, buckets, log_prefixes)

    for j in range(num_buckets):
        logging_enabled = {'TargetBucket': log_bucket_name,
                           'ObjectRollTime': longer_time,
                           'LoggingType': logging_type,
                           'TargetPrefix': log_prefixes[j]}
        response = client.put_bucket_logging(Bucket=buckets[j], BucketLoggingStatus={
            'LoggingEnabled': logging_enabled,
        })
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    num_keys = 10
    src_names = []
    for j in range(num_keys):
        src_names.append('myobject'+str(j))

    for src_bucket_name in buckets:
        for name in src_names:
            client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
            client.delete_object(Bucket=src_bucket_name, Key=name)

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)
    assert len(keys) == 0

    t = []
    flushed_obj = None
    updated_longer_time = expected_object_roll_time*20
    for src_bucket_name in buckets:
        if not single_prefix:
            logging_enabled['TargetPrefix'] = src_bucket_name+'/'
        if cleanup_type == 'deletion':
            #  cleanup based on bucket deletion
            if concurrency:
                thr = threading.Thread(target = client.delete_bucket,
                                       kwargs={'Bucket': src_bucket_name})
                thr.start()
                t.append(thr)
            else:
                client.delete_bucket(Bucket=src_bucket_name)
        elif cleanup_type == 'disabling':
            # cleanup based on disabling bucket logging
            if concurrency:
                thr = threading.Thread(target = client.put_bucket_logging,
                                       kwargs={'Bucket': src_bucket_name, 'BucketLoggingStatus': {}})
                thr.start()
                t.append(thr)
            else:
                result = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={})
                flushed_obj = _verify_flushed_on_put(result)
        elif cleanup_type == 'updating':
            # cleanup based on updating bucket logging parameters
            logging_enabled['ObjectRollTime'] = updated_longer_time
            if concurrency:
                thr = threading.Thread(target = client.put_bucket_logging,
                                       kwargs={'Bucket': src_bucket_name, 'BucketLoggingStatus': {'LoggingEnabled': logging_enabled}})
                thr.start()
                t.append(thr)
            else:
                result = client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
                    'LoggingEnabled': logging_enabled,
                })
                flushed_obj = _verify_flushed_on_put(result)
        elif cleanup_type == 'notupdating':
            # no concurrecy testing
            client.put_bucket_logging(Bucket=src_bucket_name, BucketLoggingStatus={
                'LoggingEnabled': logging_enabled,
            })
        elif cleanup_type != 'target':
            assert False, 'invalid cleanup type: ' + cleanup_type

    exact_match = False
    _do_wait_completion(t)

    if cleanup_type == 'target':
        # delete the log bucket and then create it to make sure that no pending objects remained
        # no concurrecy testing
        client.delete_bucket(Bucket=log_bucket_name)
        log_bucket = get_new_bucket_resource(name=log_bucket_name)
        _set_log_bucket_policy(client, log_bucket_name, buckets, log_prefixes)
        old_names = src_names
        src_names = []
        for j in range(num_keys):
            src_names.append('after_deletion_myobject'+str(j))
        for src_bucket_name in buckets:
            for name in src_names:
                client.put_object(Bucket=src_bucket_name, Key=name, Body=randcontent())
                client.delete_object(Bucket=src_bucket_name, Key=name)
        response = client.list_objects_v2(Bucket=log_bucket_name)
        keys = _get_keys(response)
        assert len(keys) == 0
        # flsuh any new pending objects
        for src_bucket_name in buckets:
            client.post_bucket_logging(Bucket=src_bucket_name)
        # make sure that only the new objects are logged
        exact_match = True

    response = client.list_objects_v2(Bucket=log_bucket_name)
    keys = _get_keys(response)

    if flushed_obj:
        assert flushed_obj in keys

    if cleanup_type == 'notupdating':
        # no cleanup expected
        assert len(keys) == 0
        return

    if concurrency:
        assert len(keys) >= 1 and len(keys) <= num_buckets
    else:
        if single_prefix and not (cleanup_type == 'target' or cleanup_type == 'updating'):
            assert len(keys) == 1
        else:
            assert len(keys) == num_buckets

    prefixes = []
    for src_bucket_name in buckets:
        if single_prefix:
            prefix = 'log/'
        else:
            prefix = src_bucket_name+'/'
        prefixes.append(prefix)

    body = ""
    for key in keys:
        response = client.get_object(Bucket=log_bucket_name, Key=key)
        body += _get_body(response)
        found = False
        for prefix in prefixes:
            if key.startswith(prefix):
                found = True
        assert found, 'log key does not match any expected prefix: ' + key + ' expected prefixes: ' + str(prefixes)

    assert _verify_records(body, src_bucket_name, 'REST.PUT.OBJECT', src_names, logging_type, num_keys, exact_match)
    assert _verify_records(body, src_bucket_name, 'REST.DELETE.OBJECT', src_names, logging_type, num_keys, exact_match)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_deletion_j():
    _bucket_logging_cleanup('deletion', 'Journal', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_deletion_j_single():
    _bucket_logging_cleanup('deletion', 'Journal', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_disabling_j():
    _bucket_logging_cleanup('disabling', 'Journal', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_disabling_j_single():
    _bucket_logging_cleanup('disabling', 'Journal', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_updating_j():
    _bucket_logging_cleanup('updating', 'Journal', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_updating_j_single():
    _bucket_logging_cleanup('updating', 'Journal', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_notupdating_j():
    _bucket_logging_cleanup('notupdating', 'Journal', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_notupdating_j_single():
    _bucket_logging_cleanup('notupdating', 'Journal', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_deletion_s():
    _bucket_logging_cleanup('deletion', 'Standard', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_deletion_s_single():
    _bucket_logging_cleanup('deletion', 'Standard', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_disabling_s():
    _bucket_logging_cleanup('disabling', 'Standard', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_disabling_s_single():
    _bucket_logging_cleanup('disabling', 'Standard', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_updating_s():
    _bucket_logging_cleanup('updating', 'Standard', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_updating_s_single():
    _bucket_logging_cleanup('updating', 'Standard', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_notupdating_s():
    _bucket_logging_cleanup('notupdating', 'Standard', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_notupdating_s_single():
    _bucket_logging_cleanup('notupdating', 'Standard', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_target_cleanup_j():
    _bucket_logging_cleanup('target', 'Journal', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_target_cleanup_j_single():
    _bucket_logging_cleanup('target', 'Journal', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_target_cleanup_s():
    _bucket_logging_cleanup('target', 'Standard', False, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_target_cleanup_s_single():
    _bucket_logging_cleanup('target', 'Standard', True, False)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_concurrent_deletion_j():
    _bucket_logging_cleanup('deletion', 'Journal', False, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_concurrent_deletion_j_single():
    _bucket_logging_cleanup('deletion', 'Journal', True, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_disabling_j():
    _bucket_logging_cleanup('disabling', 'Journal', False, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_disabling_j_single():
    _bucket_logging_cleanup('disabling', 'Journal', True, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_updating_j():
    _bucket_logging_cleanup('updating', 'Journal', False, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_updating_j_single():
    _bucket_logging_cleanup('updating', 'Journal', True, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_concurrent_deletion_s():
    _bucket_logging_cleanup('deletion', 'Standard', False, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_bucket_concurrent_deletion_s_single():
    _bucket_logging_cleanup('deletion', 'Standard', True, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_disabling_s():
    _bucket_logging_cleanup('disabling', 'Standard', False, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_disabling_s_single():
    _bucket_logging_cleanup('disabling', 'Standard', True, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_updating_s():
    _bucket_logging_cleanup('updating', 'Standard', False, True)


@pytest.mark.bucket_logging
@pytest.mark.bucket_logging_cleanup
@pytest.mark.fails_on_aws
def test_bucket_logging_cleanup_concurrent_updating_s_single():
    _bucket_logging_cleanup('updating', 'Standard', True, True)


def check_parts_count(parts, expected):
    # AWS docs disagree on the name of this element
    if 'TotalPartsCount' in parts:
        assert parts['TotalPartsCount'] == expected
    else:
        assert parts['PartsCount'] == expected

@pytest.mark.checksum
@pytest.mark.fails_on_dbstore
def test_get_multipart_checksum_object_attributes():
    bucket_name = get_new_bucket()
    client = get_client()

    #pdb.set_trace()
    key = "multipart_checksum"
    key_metadata = {'foo': 'bar'}
    content_type = 'text/plain'
    objlen = 64 * 1024 * 1024

    (upload_id, data, parts, checksums) = \
    _multipart_upload_checksum(bucket_name=bucket_name, key=key, size=objlen,
                               content_type=content_type, metadata=key_metadata)
    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key,
                                                UploadId=upload_id,
                                                MultipartUpload={'Parts': parts})
    upload_checksum = response['ChecksumSHA256']

    response = client.get_object(Bucket=bucket_name, Key=key)

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']

    response = client.get_object_attributes(Bucket=bucket_name, Key=key, \
                                            ObjectAttributes=request_attributes)

    # check overall object
    nparts = len(parts)
    assert response['ObjectSize'] == objlen
    assert response['Checksum']['ChecksumSHA256'] == upload_checksum
    check_parts_count(response['ObjectParts'], nparts)

    # check the parts
    partno = 1
    for obj_part in response['ObjectParts']['Parts']:
        assert obj_part['PartNumber'] == partno
        if partno < len(parts):
            assert obj_part['Size'] == 5 * 1024 * 1024
        else:
            assert obj_part['Size'] == objlen - ((nparts-1) * (5 * 1024 * 1024))
        assert obj_part['ChecksumSHA256'] == checksums[partno - 1]
        partno += 1

@pytest.mark.fails_on_dbstore
def test_get_multipart_object_attributes():
    bucket_name = get_new_bucket()
    client = get_client()

    key = "multipart"
    part_size = 5*1024*1024
    objlen = 30*1024*1024

    (upload_id, data, parts) = _multipart_upload(bucket_name, key, objlen, part_size)
    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key,
                                                UploadId=upload_id,
                                                MultipartUpload={'Parts': parts})
    etag = response['ETag'].strip('"')
    assert len(etag)

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key, \
                                            ObjectAttributes=request_attributes)

    # check overall object
    nparts = len(parts)
    assert response['ObjectSize'] == objlen
    check_parts_count(response['ObjectParts'], len(parts))
    assert response['ObjectParts']['IsTruncated'] == False
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'

    # check the parts
    partno = 1
    for obj_part in response['ObjectParts']['Parts']:
        assert obj_part['PartNumber'] == partno
        assert obj_part['Size'] == part_size
        assert 'ChecksumSHA256' not in obj_part
        partno += 1

@pytest.mark.fails_on_dbstore
def test_get_paginated_multipart_object_attributes():
    bucket_name = get_new_bucket()
    client = get_client()

    key = "multipart"
    part_size = 5*1024*1024
    objlen = 30*1024*1024

    (upload_id, data, parts) = _multipart_upload(bucket_name, key, objlen, part_size)
    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key,
                                                UploadId=upload_id,
                                                MultipartUpload={'Parts': parts})
    etag = response['ETag'].strip('"')
    assert len(etag)

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key,
                                            ObjectAttributes=request_attributes,
                                            MaxParts=1, PartNumberMarker=3)

    # check overall object
    assert response['ObjectSize'] == objlen
    check_parts_count(response['ObjectParts'], len(parts))
    assert response['ObjectParts']['MaxParts'] == 1
    assert response['ObjectParts']['PartNumberMarker'] == 3
    assert response['ObjectParts']['IsTruncated'] == True
    assert response['ObjectParts']['NextPartNumberMarker'] == 4
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'

    # check the part
    assert len(response['ObjectParts']['Parts']) == 1
    obj_part = response['ObjectParts']['Parts'][0]
    assert obj_part['PartNumber'] == 4
    assert obj_part['Size'] == part_size
    assert 'ChecksumSHA256' not in obj_part

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key,
                                            ObjectAttributes=request_attributes,
                                            MaxParts=10, PartNumberMarker=4)

    # check overall object
    assert response['ObjectSize'] == objlen
    check_parts_count(response['ObjectParts'], len(parts))
    assert response['ObjectParts']['MaxParts'] == 10
    assert response['ObjectParts']['IsTruncated'] == False
    assert response['ObjectParts']['PartNumberMarker'] == 4
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'

    # check the parts
    assert len(response['ObjectParts']['Parts']) == 2
    partno = 5
    for obj_part in response['ObjectParts']['Parts']:
        assert obj_part['PartNumber'] == partno
        assert obj_part['Size'] == part_size
        assert 'ChecksumSHA256' not in obj_part
        partno += 1

@pytest.mark.fails_on_dbstore
def test_get_single_multipart_object_attributes():
    bucket_name = get_new_bucket()
    client = get_client()

    key = "multipart"
    part_size = 5*1024*1024
    part_sizes = [part_size] # just one part
    part_count = len(part_sizes)
    total_size = sum(part_sizes)

    (upload_id, data, parts) = _multipart_upload(bucket_name, key, total_size, part_size)
    response = client.complete_multipart_upload(Bucket=bucket_name, Key=key,
                                                UploadId=upload_id,
                                                MultipartUpload={'Parts': parts})
    etag = response['ETag'].strip('"')
    assert len(etag)

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key,
                                            ObjectAttributes=request_attributes)

    assert response['ObjectSize'] == total_size
    check_parts_count(response['ObjectParts'], 1)
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'

    assert len(response['ObjectParts']['Parts']) == 1
    obj_part = response['ObjectParts']['Parts'][0]
    assert obj_part['PartNumber'] == 1
    assert obj_part['Size'] == part_size
    assert 'ChecksumSHA256' not in obj_part

def test_get_checksum_object_attributes():
    bucket_name = get_new_bucket()
    client = get_client()

    key = "myobj"
    size = 1024
    body = FakeWriteFile(size, 'A')
    sha256sum = 'arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0='
    response = client.put_object(Bucket=bucket_name, Key=key, Body=body, ChecksumAlgorithm='SHA256', ChecksumSHA256=sha256sum)
    assert sha256sum == response['ChecksumSHA256']
    etag = response['ETag'].strip('"')
    assert len(etag)

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key,
                                            ObjectAttributes=request_attributes)

    assert response['ObjectSize'] == size
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'
    assert response['Checksum']['ChecksumSHA256'] == sha256sum
    assert 'ObjectParts' not in response

def test_get_versioned_object_attributes():
    bucket_name = get_new_bucket()
    check_configure_versioning_retry(bucket_name, "Enabled", "Enabled")
    client = get_client()
    key = "obj"
    objlen = 3

    response = client.put_object(Bucket=bucket_name, Key=key, Body='foo')
    etag = response['ETag'].strip('"')
    assert len(etag)
    version = response['VersionId']
    assert len(version)

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key,
                                            ObjectAttributes=request_attributes)

    assert 'DeleteMarker' not in response
    assert response['VersionId'] == version

    assert response['ObjectSize'] == 3
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'
    assert 'ObjectParts' not in response

    # write a new current version
    client.put_object(Bucket=bucket_name, Key=key, Body='foo')

    # ask for the original version again
    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key, VersionId=version,
                                            ObjectAttributes=request_attributes)

    assert 'DeleteMarker' not in response
    assert response['VersionId'] == version

    assert response['ObjectSize'] == 3
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'
    assert 'ObjectParts' not in response

@pytest.mark.encryption
@pytest.mark.fails_on_dbstore
def test_get_sse_c_encrypted_object_attributes():
    bucket_name = get_new_bucket()
    client = get_client()
    key = 'obj'
    objlen = 1000
    data = 'A'*objlen
    sse_args = {
        'SSECustomerAlgorithm': 'AES256',
        'SSECustomerKey': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'SSECustomerKeyMD5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    attrs = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']

    response = client.put_object(Bucket=bucket_name, Key=key, Body=data, **sse_args)
    etag = response['ETag'].strip('"')
    assert len(etag)

    # GetObjectAttributes fails without sse-c headers
    e = assert_raises(ClientError, client.get_object_attributes,
                      Bucket=bucket_name, Key=key, ObjectAttributes=attrs)
    status, error_code = _get_status_and_error_code(e.response)
    assert status == 400

    # and succeeds sse-c headers
    response = client.get_object_attributes(Bucket=bucket_name, Key=key,
                                            ObjectAttributes=attrs, **sse_args)

    assert 'DeleteMarker' not in response
    assert 'VersionId' not in response

    assert response['ObjectSize'] == objlen
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'
    assert 'ObjectParts' not in response

@pytest.mark.fails_on_dbstore
def test_get_object_attributes():
    bucket_name = get_new_bucket()
    client = get_client()
    key = "obj"
    objlen = 3

    response = client.put_object(Bucket=bucket_name, Key=key, Body='foo')
    etag = response['ETag'].strip('"')
    assert len(etag)

    request_attributes = ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize']
    response = client.get_object_attributes(Bucket=bucket_name, Key=key,
                                            ObjectAttributes=request_attributes)

    assert 'DeleteMarker' not in response
    assert 'VersionId' not in response

    assert response['ObjectSize'] == 3
    assert response['ETag'] == etag
    assert response['StorageClass'] == 'STANDARD'
    assert 'ObjectParts' not in response


def test_upload_part_copy_percent_encoded_key():
    
    s3_client = get_client()
    bucket_name = get_new_bucket()
    key = "anyfile.txt"
    encoded_key = "anyfilename%25.txt"
    raw_key = "anyfilename%.txt"

    ## PutObject: the copy source
    s3_client.put_object(
        Bucket=bucket_name,
        Key=encoded_key,
        Body=b"foo",
        ContentType="text/plain"
    )

    # Upload the target object (initial state)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"foo",
        ContentType="text/plain"
    )

    # Initiate multipart upload
    mp_response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=key
    )
    upload_id = mp_response["UploadId"]

    # The following operation is expected to fail
    with pytest.raises(s3_client.exceptions.ClientError) as exc_info:
        s3_client.upload_part_copy(
            Bucket=bucket_name,
            Key=key,
            PartNumber=1,
            UploadId=upload_id,
            CopySource={'Bucket': bucket_name, 'Key': raw_key}
        )

    # Download the object and verify content
    final_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    content = final_obj['Body'].read()
    assert content == b"foo"

def check_delete_marker(client, bucket, key, status):
    dm_header_val = 'none'

    try:
        res = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        response_headers = e.response['ResponseMetadata']['HTTPHeaders']
        dm_header_val = response_headers['x-amz-delete-marker']

    assert dm_header_val == status

@pytest.mark.delete_marker
@pytest.mark.fails_on_dbstore
def test_delete_marker_nonversioned():
    bucket = get_new_bucket()
    client = get_client()
    key = "frodo.txt"
    body = "body version %s" % (key)
    # no versioning case
    res = client.put_object(Bucket=bucket, Key=key, Body=body)
    res = client.delete_object(Bucket=bucket, Key=key)
    check_delete_marker(client, bucket, key, 'false')

@pytest.mark.delete_marker
@pytest.mark.fails_on_dbstore
def test_delete_marker_versioned():
    bucket = get_new_bucket()
    client = get_client()
    key = "bilbo.txt"
    body = "body version %s" % (key)
    # versioning case
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    res = client.put_object(Bucket=bucket, Key=key, Body=body)
    res = client.delete_object(Bucket=bucket, Key=key)
    check_delete_marker(client, bucket, key, 'true')

@pytest.mark.delete_marker
@pytest.mark.fails_on_dbstore
def test_delete_marker_suspended():
    bucket = get_new_bucket()
    client = get_client()
    key = "ringo.txt"
    body = "body version %s" % (key)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    res = client.put_object(Bucket=bucket, Key=key, Body=body)
    check_configure_versioning_retry(bucket, "Suspended", "Suspended")
    res = client.delete_object(Bucket=bucket, Key=key)
    check_delete_marker(client, bucket, key, 'true')

@pytest.mark.delete_marker
@pytest.mark.lifecycle
@pytest.mark.lifecycle_expiration
@pytest.mark.fails_on_dbstore
def test_delete_marker_expiration():
    bucket = get_new_bucket()
    client = get_client()
    key = "nugent.ted"
    body = "body version %s" % (key)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    res = client.put_object(Bucket=bucket, Key=key, Body=body)
    res = client.delete_object(Bucket=bucket, Key=key)
    # object should be a delete marker
    check_delete_marker(client, bucket, key, 'true')

    # to observe delete marker expiration, we must expire any existing
    # noncurrent version, as these inhibit delete marker expiration
    lifecycle = {
        'Rules': [{'Expiration': {'ExpiredObjectDeleteMarker': True}, 'ID': 'dm-1-days', 'Prefix': '', 'Status': 'Enabled'}, {'ID': 'noncur-1-days', 'Prefix': '', 'Status': 'Enabled', 'NoncurrentVersionExpiration': {'NoncurrentDays': 1}}]
    }
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    lc_interval = get_lc_debug_interval()
    time.sleep(6*lc_interval)

    # delete marker should have expired
    check_delete_marker(client, bucket, key, 'false')

@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_put_object_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'obj'

    etag = client.put_object(Bucket=bucket, Key=key, IfNoneMatch='*')['ETag']

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfNoneMatch='*')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfNoneMatch=etag)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)
    response = client.put_object(Bucket=bucket, Key=key, IfNoneMatch='badetag')
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

    client.put_object(Bucket=bucket, Key=key, IfMatch=etag)

    client.delete_object(Bucket=bucket, Key=key)

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='*')
    assert (404, 'NoSuchKey') == _get_status_and_error_code(e.response)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (404, 'NoSuchKey') == _get_status_and_error_code(e.response)

    response = client.put_object(Bucket=bucket, Key=key, IfNoneMatch=etag)
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']
    response = client.put_object(Bucket=bucket, Key=key, IfMatch='*')
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)
    response = client.put_object(Bucket=bucket, Key=key, IfMatch=etag)
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_put_current_object_if_none_match():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'
    data1 = 'data1'
    data2 = 'data2'

    response = client.put_object(Bucket=bucket, Key=key, Body=data1, IfNoneMatch='*')
    etag = response['ETag']

    response = client.put_object(Bucket=bucket, Key=key, Body=data2)
    etag2 = response['ETag']

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfNoneMatch='*')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfNoneMatch=etag2)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    # we can't specify a version, so we only check against current object
    response = client.put_object(Bucket=bucket, Key=key, IfNoneMatch=etag)
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']
    response = client.put_object(Bucket=bucket, Key=key, IfNoneMatch='badetag')
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

    client.delete_object(Bucket=bucket, Key=key)

@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_put_current_object_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'
    data1 = 'data1'
    data2 = 'data2'
    etag = 'deadbeef'

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='*')
    assert (404, 'NoSuchKey') == _get_status_and_error_code(e.response)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (404, 'NoSuchKey') == _get_status_and_error_code(e.response)

    response = client.put_object(Bucket=bucket, Key=key, Body=data1, IfNoneMatch=etag)
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']
    etag = response['ETag']
    versionId = response['VersionId']
    response = client.put_object(Bucket=bucket, Key=key, Body=data2, IfMatch='*')
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']
    etag2 = response['ETag']

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch=etag)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)
    response = client.put_object(Bucket=bucket, Key=key, IfMatch=etag2)
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_put_object_current_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    etag = client.put_object(Bucket=bucket, Key=key, IfNoneMatch='*')['ETag']

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfNoneMatch='*')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfNoneMatch=etag)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    client.put_object(Bucket=bucket, Key=key, IfMatch=etag)

    response = client.delete_object(Bucket=bucket, Key=key)
    assert response['DeleteMarker']

    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='*')
    assert (404, 'NoSuchKey') == _get_status_and_error_code(e.response)
    e = assert_raises(ClientError, client.put_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (404, 'NoSuchKey') == _get_status_and_error_code(e.response)

    client.put_object(Bucket=bucket, Key=key, IfNoneMatch=etag)

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'obj'

    etag = client.put_object(Bucket=bucket, Key=key)['ETag']

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    client.delete_object(Bucket=bucket, Key=key, IfMatch=etag)

    # -ENOENT doesn't raise error in delete op
    response = client.delete_object(Bucket=bucket, Key=key, IfMatch='*')
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']
    response = client.delete_object(Bucket=bucket, Key=key, IfMatch='badetag')
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']

    # recreate to test IfMatch='*'
    client.put_object(Bucket=bucket, Key=key)
    client.delete_object(Bucket=bucket, Key=key, IfMatch='*')

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_current_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    # -ENOENT doesn't raise error in delete op
    response = client.delete_object(Bucket=bucket, Key=key, IfMatch='*')
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']
    response = client.delete_object(Bucket=bucket, Key=key, IfMatch='badetag')
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']

    response = client.put_object(Bucket=bucket, Key=key)
    version = response['VersionId']
    etag = response['ETag']

    # on current version
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    response = client.delete_object(Bucket=bucket, Key=key, IfMatch=etag)
    assert response['DeleteMarker']

    # -ENOENT doesn't raise error in delete op
    client.delete_object(Bucket=bucket, Key=key, IfMatch='*')
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatch='badetag')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    # remove delete marker to retest IfMatch='*'
    client.delete_object(Bucket=bucket, Key=key, VersionId=version)
    client.delete_object(Bucket=bucket, Key=key, IfMatch='*')

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_version_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    response = client.put_object(Bucket=bucket, Key=key)
    version = response['VersionId']
    etag = response['ETag']

    # on specific version
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, VersionId=version, IfMatch='badetag')
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    response = client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatch=etag)
    assert 'DeleteMarker' not in response

    # -ENOENT doesn't raise error in delete op
    client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatch='*')
    client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatch='badetag')

    # recreate to test IfMatch='*'
    response = client.put_object(Bucket=bucket, Key=key)
    version = response['VersionId']
    client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatch='*')

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_if_match_last_modified_time():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'obj'

    badmtime = datetime.datetime(2015, 1, 1)

    client.put_object(Bucket=bucket, Key=key)
    mtime = client.head_object(Bucket=bucket, Key=key)['LastModified']

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatchLastModifiedTime=badmtime)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    client.delete_object(Bucket=bucket, Key=key, IfMatchLastModifiedTime=mtime)

    response = client.delete_object(Bucket=bucket, Key=key, IfMatchLastModifiedTime=badmtime)
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_current_if_match_last_modified_time():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    badmtime = datetime.datetime(2015, 1, 1)

    client.put_object(Bucket=bucket, Key=key)
    mtime = client.head_object(Bucket=bucket, Key=key)['LastModified']

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatchLastModifiedTime=badmtime)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    response = client.delete_object(Bucket=bucket, Key=key, IfMatchLastModifiedTime=mtime)
    assert response['DeleteMarker']

    # object is marked as deleted but still exists
    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatchLastModifiedTime=badmtime)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_version_if_match_last_modified_time():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    badmtime = datetime.datetime(2015, 1, 1)

    version = client.put_object(Bucket=bucket, Key=key)['VersionId']
    mtime = client.head_object(Bucket=bucket, Key=key, VersionId=version)['LastModified']

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, VersionId=version, IfMatchLastModifiedTime=badmtime)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    response = client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatchLastModifiedTime=mtime)
    assert 'DeleteMarker' not in response

    response = client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatchLastModifiedTime=badmtime)
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_if_match_size():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'obj'

    size = 0
    badsize = 9999

    client.put_object(Bucket=bucket, Key=key)

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatchSize=badsize)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    client.delete_object(Bucket=bucket, Key=key, IfMatchSize=size)

    response = client.delete_object(Bucket=bucket, Key=key, IfMatchSize=badsize)
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_current_if_match_size():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    size = 0
    badsize = 9999

    client.put_object(Bucket=bucket, Key=key)

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatchSize=badsize)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    response = client.delete_object(Bucket=bucket, Key=key, IfMatchSize=size)
    assert 'DeleteMarker' in response

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, IfMatchSize=badsize)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_object_version_if_match_size():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    size = 0
    badsize = 9999

    version = client.put_object(Bucket=bucket, Key=key)['VersionId']

    e = assert_raises(ClientError, client.delete_object, Bucket=bucket, Key=key, VersionId=version, IfMatchSize=badsize)
    assert (412, 'PreconditionFailed') == _get_status_and_error_code(e.response)

    response = client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatchSize=size)
    assert 'DeleteMarker' not in response

    response = client.delete_object(Bucket=bucket, Key=key, VersionId=version, IfMatchSize=badsize)
    assert 204 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'obj'

    etag = client.put_object(Bucket=bucket, Key=key)['ETag']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'ETag': 'badetag'}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'ETag': etag}]})
    assert key == response['Deleted'][0]['Key'] # success

    # -ENOENT doesn't raise error in delete op
    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'ETag': 'badetag'}]})
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_current_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    etag = client.put_object(Bucket=bucket, Key=key)['ETag']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'ETag': 'badetag'}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'ETag': etag}]})
    assert key == response['Deleted'][0]['Key'] # success
    assert response['Deleted'][0]['DeleteMarker']

    # object is marked as deleted but still exists
    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'ETag': 'badetag'}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_version_if_match():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    response = client.put_object(Bucket=bucket, Key=key)
    etag = response['ETag']
    version = response['VersionId']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'ETag': 'badetag'}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'ETag': etag}]})
    assert key == response['Deleted'][0]['Key'] # success
    assert 'DeleteMarker' not in response['Deleted'][0]

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'ETag': 'badetag'}]})
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_if_match_last_modified_time():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'obj'

    badmtime = datetime.datetime(2015, 1, 1)

    client.put_object(Bucket=bucket, Key=key)
    mtime = client.head_object(Bucket=bucket, Key=key)['LastModified']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'LastModifiedTime': badmtime}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'LastModifiedTime': mtime}]})
    assert key == response['Deleted'][0]['Key'] # success

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'LastModifiedTime': badmtime}]})
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_current_if_match_last_modified_time():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    badmtime = datetime.datetime(2015, 1, 1)

    client.put_object(Bucket=bucket, Key=key)
    mtime = client.head_object(Bucket=bucket, Key=key)['LastModified']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'LastModifiedTime': badmtime}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'LastModifiedTime': mtime}]})
    assert key == response['Deleted'][0]['Key'] # success
    assert response['Deleted'][0]['DeleteMarker']

    # object exists, but marked as deleted
    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'LastModifiedTime': badmtime}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_version_if_match_last_modified_time():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    badmtime = datetime.datetime(2015, 1, 1)

    version = client.put_object(Bucket=bucket, Key=key)['VersionId']
    mtime = client.head_object(Bucket=bucket, Key=key, VersionId=version)['LastModified']

    # create a different version as current
    client.put_object(Bucket=bucket, Key=key)

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'LastModifiedTime': badmtime}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'LastModifiedTime': mtime}]})
    assert key == response['Deleted'][0]['Key'] # success

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'LastModifiedTime': badmtime}]})
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_if_match_size():
    client = get_client()
    bucket = get_new_bucket(client)
    key = 'obj'

    size = 0
    badsize = 9999

    client.put_object(Bucket=bucket, Key=key)

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'Size': badsize}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'Size': size}]})
    assert key == response['Deleted'][0]['Key'] # success

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'Size': badsize}]})
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_current_if_match_size():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    size = 0
    badsize = 9999

    client.put_object(Bucket=bucket, Key=key)

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'Size': badsize}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'Size': size}]})
    assert key == response['Deleted'][0]['Key'] # success
    assert response['Deleted'][0]['DeleteMarker']

    # object exists, but marked as deleted
    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'Size': badsize}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

@pytest.mark.fails_on_aws # only supported for directory buckets
@pytest.mark.conditional_write
@pytest.mark.fails_on_dbstore
def test_delete_objects_version_if_match_size():
    client = get_client()
    bucket = get_new_bucket(client)
    check_configure_versioning_retry(bucket, "Enabled", "Enabled")
    key = 'obj'

    size = 0
    badsize = 9999

    version = client.put_object(Bucket=bucket, Key=key)['VersionId']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'Size': badsize}]})
    assert 'PreconditionFailed' == response['Errors'][0]['Code']

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'Size': size}]})
    assert key == response['Deleted'][0]['Key'] # success

    response = client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': key, 'VersionId': version, 'Size': badsize}]})
    assert 200 == response['ResponseMetadata']['HTTPStatusCode']
