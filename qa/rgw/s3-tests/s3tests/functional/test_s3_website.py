import sys
from collections.abc import Container
import pytest
import string
import random
from pprint import pprint
import time
import boto.exception
import socket

from urllib.parse import urlparse

from .. import common

from . import (
    configfile,
    setup_teardown,
    get_new_bucket,
    get_new_bucket_name,
    s3,
    config,
    _make_raw_request,
    choose_bucket_prefix,
    )

IGNORE_FIELD = 'IGNORETHIS'

SLEEP_INTERVAL = 0.01
SLEEP_MAX = 2.0

WEBSITE_CONFIGS_XMLFRAG = {
        'IndexDoc': '<IndexDocument><Suffix>${IndexDocument_Suffix}</Suffix></IndexDocument>${RoutingRules}',
        'IndexDocErrorDoc': '<IndexDocument><Suffix>${IndexDocument_Suffix}</Suffix></IndexDocument><ErrorDocument><Key>${ErrorDocument_Key}</Key></ErrorDocument>${RoutingRules}',
        'RedirectAll': '<RedirectAllRequestsTo><HostName>${RedirectAllRequestsTo_HostName}</HostName></RedirectAllRequestsTo>${RoutingRules}',
        'RedirectAll+Protocol': '<RedirectAllRequestsTo><HostName>${RedirectAllRequestsTo_HostName}</HostName><Protocol>${RedirectAllRequestsTo_Protocol}</Protocol></RedirectAllRequestsTo>${RoutingRules}',
        }
INDEXDOC_TEMPLATE = '<html><h1>IndexDoc</h1><body>{random}</body></html>'
ERRORDOC_TEMPLATE = '<html><h1>ErrorDoc</h1><body>{random}</body></html>'

CAN_WEBSITE = None

@pytest.fixture(autouse=True, scope="module")
def check_can_test_website():
    bucket = get_new_bucket()
    try:
        wsconf = bucket.get_website_configuration()
        return True
    except boto.exception.S3ResponseError as e:
        if e.status == 404 and e.reason == 'Not Found' and e.error_code in ['NoSuchWebsiteConfiguration', 'NoSuchKey']:
            return True
        elif e.status == 405 and e.reason == 'Method Not Allowed' and e.error_code == 'MethodNotAllowed':
            pytest.skip('rgw_enable_static_website is false')
        elif e.status == 403 and e.reason == 'SignatureDoesNotMatch' and e.error_code == 'Forbidden':
            # This is older versions that do not support the website code
            pytest.skip('static website is not implemented')
        elif e.status == 501 and e.error_code == 'NotImplemented':
            pytest.skip('static website is not implemented')
        else:
            raise RuntimeError("Unknown response in checking if WebsiteConf is supported", e)
    finally:
        bucket.delete()

def make_website_config(xml_fragment):
    """
    Take the tedious stuff out of the config
    """
    return '<?xml version="1.0" encoding="UTF-8"?><WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' + xml_fragment + '</WebsiteConfiguration>'

def get_website_url(**kwargs):
    """
    Return the URL to a website page
    """
    proto, bucket, hostname, path = 'http', None, None, '/'

    if 'proto' in kwargs:
        proto = kwargs['proto']
    if 'bucket' in kwargs:
        bucket = kwargs['bucket']
    if 'hostname' in kwargs:
        hostname = kwargs['hostname']
    if 'path' in kwargs:
        path = kwargs['path']

    if hostname is None and bucket is None:
        return '/' + path.lstrip('/')

    domain = config['main']['host']
    if('s3website_domain' in config['main']):
        domain = config['main']['s3website_domain']
    elif('s3website_domain' in config['alt']):
        domain = config['DEFAULT']['s3website_domain']
    if hostname is None and bucket is not None:
        hostname = '%s.%s' % (bucket, domain)
    path = path.lstrip('/')
    return "%s://%s/%s" % (proto, hostname, path)

def _test_website_populate_fragment(xml_fragment, fields):
    for k in ['RoutingRules']:
      if k in list(fields.keys()) and len(fields[k]) > 0:
         fields[k] = '<%s>%s</%s>' % (k, fields[k], k)
    f = {
          'IndexDocument_Suffix': choose_bucket_prefix(template='index-{random}.html', max_len=32),
          'ErrorDocument_Key': choose_bucket_prefix(template='error-{random}.html', max_len=32),
          'RedirectAllRequestsTo_HostName': choose_bucket_prefix(template='{random}.{random}.com', max_len=32),
          'RoutingRules': ''
        }
    f.update(fields)
    xml_fragment = string.Template(xml_fragment).safe_substitute(**f)
    return xml_fragment, f

def _test_website_prep(bucket, xml_template, hardcoded_fields = {}, expect_fail=None):
    xml_fragment, f = _test_website_populate_fragment(xml_template, hardcoded_fields)
    f['WebsiteConfiguration'] = ''
    if not xml_template:
        bucket.delete_website_configuration()
        return f

    config_xmlnew = make_website_config(xml_fragment)

    config_xmlold = ''
    try:
        config_xmlold = common.normalize_xml(bucket.get_website_configuration_xml(), pretty_print=True)
    except boto.exception.S3ResponseError as e:
        if str(e.status) == str(404) \
            and ('NoSuchWebsiteConfiguration' in e.body or 'NoSuchWebsiteConfiguration' in e.code or
                    'NoSuchKey' in e.body or 'NoSuchKey' in e.code):
            pass
        else:
            raise e

    try:
        bucket.set_website_configuration_xml(common.trim_xml(config_xmlnew))
        config_xmlnew = common.normalize_xml(config_xmlnew, pretty_print=True)
    except boto.exception.S3ResponseError as e:
        if expect_fail is not None:
            if isinstance(expect_fail, dict):
                pass
            elif isinstance(expect_fail, str):
                pass
        raise e

    # TODO: in some cases, it takes non-zero time for the config to be applied by AmazonS3
    # We should figure out how to poll for changes better
    # WARNING: eu-west-1 as of 2015/06/22 was taking at least 4 seconds to propogate website configs, esp when you cycle between non-null configs
    time.sleep(0.1)
    config_xmlcmp = common.normalize_xml(bucket.get_website_configuration_xml(), pretty_print=True)

    #if config_xmlold is not None:
    #    print('old',config_xmlold.replace("\n",''))
    #if config_xmlcmp is not None:
    #    print('cmp',config_xmlcmp.replace("\n",''))
    #if config_xmlnew is not None:
    #    print('new',config_xmlnew.replace("\n",''))
    # Cleanup for our validation
    common.assert_xml_equal(config_xmlcmp, config_xmlnew)
    #print("config_xmlcmp\n", config_xmlcmp)
    #assert config_xmlnew == config_xmlcmp
    f['WebsiteConfiguration'] = config_xmlcmp
    return f

def __website_expected_reponse_status(res, status, reason):
    if not isinstance(status, Container):
        status = set([status])
    if not isinstance(reason, Container):
        reason = set([reason])

    if status is not IGNORE_FIELD:
        assert res.status in status, 'HTTP code was %s should be %s' % (res.status, status)
    if reason is not IGNORE_FIELD:
        assert res.reason in reason, 'HTTP reason was was %s should be %s' % (res.reason, reason)

def _website_expected_default_html(**kwargs):
    fields = []
    for k in list(kwargs.keys()):
        # AmazonS3 seems to be inconsistent, some HTML errors include BucketName, but others do not.
        if k is 'BucketName':
            continue

        v = kwargs[k]
        if isinstance(v, str):
            v = [v]
        elif not isinstance(v, Container):
            v = [v]
        for v2 in v:
            s = '<li>%s: %s</li>' % (k,v2)
            fields.append(s)
    return fields

def _website_expected_error_response(res, bucket_name, status, reason, code, content=None, body=None):
    if body is None:
        body = res.read()
        print(body)
    __website_expected_reponse_status(res, status, reason)

    # Argh, AmazonS3 is really inconsistent, so we have a conditional test!
    # This is most visible if you have an ErrorDoc present
    errorcode = res.getheader('x-amz-error-code', None)
    if errorcode is not None:
        if code is not IGNORE_FIELD:
            assert errorcode == code

    if not isinstance(content, Container):
        content = set([content])
    for f in content:
        if f is not IGNORE_FIELD and f is not None:
            f = bytes(f, 'utf-8')
            assert f in body, 'HTML should contain "%s"' % (f, )

def _website_expected_redirect_response(res, status, reason, new_url):
    body = res.read()
    print(body)
    __website_expected_reponse_status(res, status, reason)
    loc = res.getheader('Location', None)
    assert loc == new_url, 'Location header should be set "%s" != "%s"' % (loc,new_url,)
    assert len(body) == 0, 'Body of a redirect should be empty'

def _website_request(bucket_name, path, connect_hostname=None, method='GET', timeout=None):
    url = get_website_url(proto='http', bucket=bucket_name, path=path)
    print("url", url)
    o = urlparse(url)
    if connect_hostname is None:
        connect_hostname = o.hostname
    path = o.path + '?' + o.query
    request_headers={}
    request_headers['Host'] = o.hostname
    request_headers['Accept'] = '*/*'
    print('Request: {method} {path}\n{headers}'.format(method=method, path=path, headers=''.join([t[0]+':'+t[1]+"\n" for t in list(request_headers.items())])))
    res = _make_raw_request(connect_hostname, config.main.port, method, path, request_headers=request_headers, secure=False, timeout=timeout)
    for (k,v) in res.getheaders():
        print(k,v)
    return res

# ---------- Non-existant buckets via the website endpoint
@pytest.mark.s3website
@pytest.mark.fails_on_rgw
def test_website_nonexistant_bucket_s3():
    bucket_name = get_new_bucket_name()
    res = _website_request(bucket_name, '')
    _website_expected_error_response(res, bucket_name, 404, 'Not Found', 'NoSuchBucket', content=_website_expected_default_html(Code='NoSuchBucket'))

@pytest.mark.s3website
@pytest.mark.fails_on_s3
@pytest.mark.fails_on_dbstore
def test_website_nonexistant_bucket_rgw():
    bucket_name = get_new_bucket_name()
    res = _website_request(bucket_name, '')
    #_website_expected_error_response(res, bucket_name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))
    _website_expected_error_response(res, bucket_name, 404, 'Not Found', 'NoSuchBucket', content=_website_expected_default_html(Code='NoSuchBucket'))

#------------- IndexDocument only, successes
@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
@pytest.mark.timeout(10)
def test_website_public_bucket_list_public_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.make_public()
    #time.sleep(1)
    while bucket.get_key(f['IndexDocument_Suffix']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    body = res.read()
    print(body)
    indexstring = bytes(indexstring, 'utf-8')
    assert body == indexstring # default content should match index.html set content
    __website_expected_reponse_status(res, 200, 'OK')
    indexhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_public_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.make_public()
    #time.sleep(1)
    while bucket.get_key(f['IndexDocument_Suffix']) is None:
        time.sleep(SLEEP_INTERVAL)


    res = _website_request(bucket.name, '')
    __website_expected_reponse_status(res, 200, 'OK')
    body = res.read()
    print(body)
    indexstring = bytes(indexstring, 'utf-8')
    assert body == indexstring, 'default content should match index.html set content'
    indexhtml.delete()
    bucket.delete()


# ---------- IndexDocument only, failures
@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_empty():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.set_canned_acl('private')
    # TODO: wait for sync

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_empty():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey', content=_website_expected_default_html(Code='NoSuchKey'))
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_private_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    #time.sleep(1)
    #time.sleep(1)
    while bucket.get_key(f['IndexDocument_Suffix']) is None:
        time.sleep(SLEEP_INTERVAL)


    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))
    indexhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_private_index():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    ##time.sleep(1)
    while bucket.get_key(f['IndexDocument_Suffix']) is None:
        time.sleep(SLEEP_INTERVAL)


    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))

    indexhtml.delete()
    bucket.delete()

# ---------- IndexDocument & ErrorDocument, failures due to errordoc assigned but missing
@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_empty_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))

    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_empty_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey')
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_private_index_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    #time.sleep(1)
    while bucket.get_key(f['IndexDocument_Suffix']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))

    indexhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_private_index_missingerrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    #time.sleep(1)
    while bucket.get_key(f['IndexDocument_Suffix']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))

    indexhtml.delete()
    bucket.delete()

# ---------- IndexDocument & ErrorDocument, failures due to errordoc assigned but not accessible
@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_empty_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')
    #time.sleep(1)
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    body = res.read()
    print(body)
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'), body=body)
    errorstring = bytes(errorstring, 'utf-8')
    assert errorstring not in body, 'error content should NOT match error.html set content'

    errorhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_pubilc_errordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')

    url = get_website_url(proto='http', bucket=bucket.name, path='')
    o = urlparse(url)
    host = o.hostname
    port = s3.main.port

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    request = "GET / HTTP/1.1\r\nHost:%s.%s:%s\r\n\r\n" % (bucket.name, host, port)
    sock.send(request.encode())
    
    #receive header
    resp = sock.recv(4096)
    print(resp)

    #receive body
    resp = sock.recv(4096)
    print('payload length=%d' % len(resp))
    print(resp)

    #check if any additional payload is left
    resp_len = 0
    sock.settimeout(2)
    try:
        resp = sock.recv(4096)
        resp_len = len(resp)
        print('invalid payload length=%d' % resp_len)
        print(resp)
    except socket.timeout:
        print('no invalid payload')

    assert resp_len == 0, 'invalid payload'

    errorhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_empty_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    body = res.read()
    print(body)
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey', content=_website_expected_default_html(Code='NoSuchKey'), body=body)
    errorstring = bytes(errorstring, 'utf-8')
    assert errorstring not in body, 'error content should match error.html set content'

    errorhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_private_index_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')
    #time.sleep(1)
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    body = res.read()
    print(body)
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'), body=body)
    errorstring = bytes(errorstring, 'utf-8')
    assert errorstring not in body, 'error content should match error.html set content'

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_private_index_blockederrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('private')
    #time.sleep(1)
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    body = res.read()
    print(body)
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'), body=body)
    errorstring = bytes(errorstring, 'utf-8')
    assert errorstring not in body, 'error content should match error.html set content'

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()

# ---------- IndexDocument & ErrorDocument, failures with errordoc available
@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_empty_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring, policy='public-read')
    #time.sleep(1)
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=[errorstring])

    errorhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_empty_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')
   #time.sleep(1)
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchKey', content=[errorstring])

    errorhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_public_bucket_list_private_index_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.make_public()
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')
    #time.sleep(1)
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=[errorstring])

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_private_bucket_list_private_index_gooderrordoc():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
    bucket.set_canned_acl('private')
    indexhtml = bucket.new_key(f['IndexDocument_Suffix'])
    indexstring = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=256)
    indexhtml.set_contents_from_string(indexstring)
    indexhtml.set_canned_acl('private')
    errorhtml = bucket.new_key(f['ErrorDocument_Key'])
    errorstring = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=256)
    errorhtml.set_contents_from_string(errorstring)
    errorhtml.set_canned_acl('public-read')
    #time.sleep(1)
    while bucket.get_key(f['ErrorDocument_Key']) is None:
        time.sleep(SLEEP_INTERVAL)

    res = _website_request(bucket.name, '')
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=[errorstring])

    indexhtml.delete()
    errorhtml.delete()
    bucket.delete()

# ------ RedirectAll tests
@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_bucket_private_redirectall_base():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['RedirectAll'])
    bucket.set_canned_acl('private')

    res = _website_request(bucket.name, '')
    new_url = 'http://%s/' % f['RedirectAllRequestsTo_HostName']
    _website_expected_redirect_response(res, 301, ['Moved Permanently'], new_url)

    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_bucket_private_redirectall_path():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['RedirectAll'])
    bucket.set_canned_acl('private')

    pathfragment = choose_bucket_prefix(template='/{random}', max_len=16)

    res = _website_request(bucket.name, pathfragment)
    new_url = 'http://%s%s' % (f['RedirectAllRequestsTo_HostName'], pathfragment)
    _website_expected_redirect_response(res, 301, ['Moved Permanently'], new_url)

    bucket.delete()

@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
def test_website_bucket_private_redirectall_path_upgrade():
    bucket = get_new_bucket()
    x = string.Template(WEBSITE_CONFIGS_XMLFRAG['RedirectAll+Protocol']).safe_substitute(RedirectAllRequestsTo_Protocol='https')
    f = _test_website_prep(bucket, x)
    bucket.set_canned_acl('private')

    pathfragment = choose_bucket_prefix(template='/{random}', max_len=16)

    res = _website_request(bucket.name, pathfragment)
    new_url = 'https://%s%s' % (f['RedirectAllRequestsTo_HostName'], pathfragment)
    _website_expected_redirect_response(res, 301, ['Moved Permanently'], new_url)

    bucket.delete()

# ------ x-amz redirect tests
@pytest.mark.s3website
@pytest.mark.s3website_redirect_location
@pytest.mark.fails_on_dbstore
def test_website_xredirect_nonwebsite():
    bucket = get_new_bucket()
    #f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['RedirectAll'])
    #bucket.set_canned_acl('private')

    k = bucket.new_key('page')
    content = 'wrong-content'
    redirect_dest = '/relative'
    headers = {'x-amz-website-redirect-location': redirect_dest}
    k.set_contents_from_string(content, headers=headers, policy='public-read')
    redirect = k.get_redirect()
    assert k.get_redirect() == redirect_dest

    res = _website_request(bucket.name, '/page')
    body = res.read()
    print(body)
    expected_content = _website_expected_default_html(Code='NoSuchWebsiteConfiguration', BucketName=bucket.name)
    # TODO: RGW does not have custom error messages for different 404s yet
    #expected_content = _website_expected_default_html(Code='NoSuchWebsiteConfiguration', BucketName=bucket.name, Message='The specified bucket does not have a website configuration')
    print(expected_content)
    _website_expected_error_response(res, bucket.name, 404, 'Not Found', 'NoSuchWebsiteConfiguration', content=expected_content, body=body)

    k.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.s3website_redirect_location
@pytest.mark.fails_on_dbstore
def test_website_xredirect_public_relative():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    k = bucket.new_key('page')
    content = 'wrong-content'
    redirect_dest = '/relative'
    headers = {'x-amz-website-redirect-location': redirect_dest}
    k.set_contents_from_string(content, headers=headers, policy='public-read')
    redirect = k.get_redirect()
    assert k.get_redirect() == redirect_dest

    res = _website_request(bucket.name, '/page')
    #new_url =  get_website_url(bucket_name=bucket.name, path=redirect_dest)
    _website_expected_redirect_response(res, 301, ['Moved Permanently'], redirect_dest)

    k.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.s3website_redirect_location
@pytest.mark.fails_on_dbstore
def test_website_xredirect_public_abs():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    k = bucket.new_key('page')
    content = 'wrong-content'
    redirect_dest = 'http://example.com/foo'
    headers = {'x-amz-website-redirect-location': redirect_dest}
    k.set_contents_from_string(content, headers=headers, policy='public-read')
    redirect = k.get_redirect()
    assert k.get_redirect() == redirect_dest

    res = _website_request(bucket.name, '/page')
    new_url =  get_website_url(proto='http', hostname='example.com', path='/foo')
    _website_expected_redirect_response(res, 301, ['Moved Permanently'], new_url)

    k.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.s3website_redirect_location
@pytest.mark.fails_on_dbstore
def test_website_xredirect_private_relative():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    k = bucket.new_key('page')
    content = 'wrong-content'
    redirect_dest = '/relative'
    headers = {'x-amz-website-redirect-location': redirect_dest}
    k.set_contents_from_string(content, headers=headers, policy='private')
    redirect = k.get_redirect()
    assert k.get_redirect() == redirect_dest

    res = _website_request(bucket.name, '/page')
    # We get a 403 because the page is private
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))

    k.delete()
    bucket.delete()

@pytest.mark.s3website
@pytest.mark.s3website_redirect_location
@pytest.mark.fails_on_dbstore
def test_website_xredirect_private_abs():
    bucket = get_new_bucket()
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDoc'])
    bucket.make_public()

    k = bucket.new_key('page')
    content = 'wrong-content'
    redirect_dest = 'http://example.com/foo'
    headers = {'x-amz-website-redirect-location': redirect_dest}
    k.set_contents_from_string(content, headers=headers, policy='private')
    redirect = k.get_redirect()
    assert k.get_redirect() == redirect_dest

    res = _website_request(bucket.name, '/page')
    new_url =  get_website_url(proto='http', hostname='example.com', path='/foo')
    # We get a 403 because the page is private
    _website_expected_error_response(res, bucket.name, 403, 'Forbidden', 'AccessDenied', content=_website_expected_default_html(Code='AccessDenied'))

    k.delete()
    bucket.delete()
# ------ RoutingRules tests

# RoutingRules
ROUTING_RULES = {
    'empty': '',
    'AmazonExample1': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
    'AmazonExample1+Protocol=https': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <Protocol>https</Protocol>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
    'AmazonExample1+Protocol=https+Hostname=xyzzy': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <Protocol>https</Protocol>
      <HostName>xyzzy</HostName>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
    'AmazonExample1+Protocol=http2': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>docs/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <Protocol>http2</Protocol>
      <ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample2': \
"""
    <RoutingRule>
    <Condition>
       <KeyPrefixEquals>images/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <ReplaceKeyWith>folderdeleted.html</ReplaceKeyWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample2+HttpRedirectCode=TMPL': \
"""
    <RoutingRule>
    <Condition>
       <KeyPrefixEquals>images/</KeyPrefixEquals>
    </Condition>
    <Redirect>
      <HttpRedirectCode>{HttpRedirectCode}</HttpRedirectCode>
      <ReplaceKeyWith>folderdeleted.html</ReplaceKeyWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample3': \
"""
    <RoutingRule>
    <Condition>
      <HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>
    </Condition>
    <Redirect>
      <HostName>ec2-11-22-333-44.compute-1.amazonaws.com</HostName>
      <ReplaceKeyPrefixWith>report-404/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
   'AmazonExample3+KeyPrefixEquals': \
"""
    <RoutingRule>
    <Condition>
      <KeyPrefixEquals>images/</KeyPrefixEquals>
      <HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>
    </Condition>
    <Redirect>
      <HostName>ec2-11-22-333-44.compute-1.amazonaws.com</HostName>
      <ReplaceKeyPrefixWith>report-404/</ReplaceKeyPrefixWith>
    </Redirect>
    </RoutingRule>
""",
}

for k in list(ROUTING_RULES.keys()):
  if len(ROUTING_RULES[k]) > 0:
    ROUTING_RULES[k] = "<!-- %s -->\n%s" % (k, ROUTING_RULES[k])

ROUTING_RULES_TESTS = [
  dict(xml=dict(RoutingRules=ROUTING_RULES['empty']), url='', location=None, code=200),
  dict(xml=dict(RoutingRules=ROUTING_RULES['empty']), url='/', location=None, code=200),
  dict(xml=dict(RoutingRules=ROUTING_RULES['empty']), url='/x', location=None, code=404),

  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/', location=None, code=200),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/x', location=None, code=404),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/docs/', location=dict(proto='http',bucket='{bucket_name}',path='/documents/'), code=301),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1']), url='/docs/x', location=dict(proto='http',bucket='{bucket_name}',path='/documents/x'), code=301),

  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/', location=None, code=200),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/x', location=None, code=404),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/docs/', location=dict(proto='https',bucket='{bucket_name}',path='/documents/'), code=301),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https']), url='/docs/x', location=dict(proto='https',bucket='{bucket_name}',path='/documents/x'), code=301),

  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/', location=None, code=200),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/x', location=None, code=404),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/docs/', location=dict(proto='https',hostname='xyzzy',path='/documents/'), code=301),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=https+Hostname=xyzzy']), url='/docs/x', location=dict(proto='https',hostname='xyzzy',path='/documents/x'), code=301),

  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample2']), url='/images/', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=301),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample2']), url='/images/x', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=301),


  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3']), url='/x', location=dict(proto='http',hostname='ec2-11-22-333-44.compute-1.amazonaws.com',path='/report-404/x'), code=301),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3']), url='/images/x', location=dict(proto='http',hostname='ec2-11-22-333-44.compute-1.amazonaws.com',path='/report-404/images/x'), code=301),

  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3+KeyPrefixEquals']), url='/x', location=None, code=404),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample3+KeyPrefixEquals']), url='/images/x', location=dict(proto='http',hostname='ec2-11-22-333-44.compute-1.amazonaws.com',path='/report-404/x'), code=301),
]

ROUTING_ERROR_PROTOCOL = dict(code=400, reason='Bad Request', errorcode='InvalidRequest', bodyregex=r'Invalid protocol, protocol can be http or https. If not defined the protocol will be selected automatically.')

ROUTING_RULES_TESTS_ERRORS = [ # TODO: Unused!
  # Invalid protocol, protocol can be http or https. If not defined the protocol will be selected automatically.
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/', location=None, code=400, error=ROUTING_ERROR_PROTOCOL),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/x', location=None, code=400, error=ROUTING_ERROR_PROTOCOL),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/docs/', location=None, code=400, error=ROUTING_ERROR_PROTOCOL),
  dict(xml=dict(RoutingRules=ROUTING_RULES['AmazonExample1+Protocol=http2']), url='/docs/x', location=None, code=400, error=ROUTING_ERROR_PROTOCOL),
]

VALID_AMZ_REDIRECT = set([301,302,303,304,305,307,308])

# General lots of tests
for redirect_code in VALID_AMZ_REDIRECT:
  rules = ROUTING_RULES['AmazonExample2+HttpRedirectCode=TMPL'].format(HttpRedirectCode=redirect_code)
  result = redirect_code
  ROUTING_RULES_TESTS.append(
    dict(xml=dict(RoutingRules=rules), url='/images/', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=result)
  )
  ROUTING_RULES_TESTS.append(
    dict(xml=dict(RoutingRules=rules), url='/images/x', location=dict(proto='http',bucket='{bucket_name}',path='/folderdeleted.html'), code=result)
  )

# TODO:
# codes other than those in VALID_AMZ_REDIRECT
# give an error of 'The provided HTTP redirect code (314) is not valid. Valid codes are 3XX except 300.' during setting the website config
# we should check that we can return that too on ceph

@pytest.fixture
def routing_setup():
  kwargs = {'obj':[]}
  bucket = get_new_bucket()
  kwargs['bucket'] = bucket
  kwargs['obj'].append(bucket)
  #f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'])
  f = _test_website_prep(bucket, '')
  kwargs.update(f)
  bucket.set_canned_acl('public-read')
  
  k = bucket.new_key('debug-ws.xml')
  kwargs['obj'].append(k)
  k.set_contents_from_string('', policy='public-read')

  k = bucket.new_key(f['IndexDocument_Suffix'])
  kwargs['obj'].append(k)
  s = choose_bucket_prefix(template=INDEXDOC_TEMPLATE, max_len=64)
  k.set_contents_from_string(s)
  k.set_canned_acl('public-read')

  k = bucket.new_key(f['ErrorDocument_Key'])
  kwargs['obj'].append(k)
  s = choose_bucket_prefix(template=ERRORDOC_TEMPLATE, max_len=64)
  k.set_contents_from_string(s)
  k.set_canned_acl('public-read')

  #time.sleep(1)
  while bucket.get_key(f['ErrorDocument_Key']) is None:
      time.sleep(SLEEP_INTERVAL)

  yield kwargs

  for o in reversed(kwargs['obj']):
    print('Deleting', str(o))
    o.delete()

def routing_check(*args, **kwargs):
    bucket = kwargs['bucket']
    args=args[0]
    #print(args)
    pprint(args)
    xml_fields = kwargs.copy()
    xml_fields.update(args['xml'])

    k = bucket.get_key('debug-ws.xml')
    k.set_contents_from_string(str(args)+str(kwargs), policy='public-read')

    pprint(xml_fields)
    f = _test_website_prep(bucket, WEBSITE_CONFIGS_XMLFRAG['IndexDocErrorDoc'], hardcoded_fields=xml_fields)
    #print(f)
    config_xmlcmp = bucket.get_website_configuration_xml()
    config_xmlcmp = common.normalize_xml(config_xmlcmp, pretty_print=True) # For us to read
    res = _website_request(bucket.name, args['url'])
    print(config_xmlcmp)
    new_url = args['location']
    if new_url is not None:
        new_url = get_website_url(**new_url)
        new_url = new_url.format(bucket_name=bucket.name)
    if args['code'] >= 200 and args['code'] < 300:
        #body = res.read()
        #print(body)
        #assert body == args['content'], 'default content should match index.html set content'
        assert int(res.getheader('Content-Length', -1)) > 0
    elif args['code'] >= 300 and args['code'] < 400:
        _website_expected_redirect_response(res, args['code'], IGNORE_FIELD, new_url)
    elif args['code'] >= 400:
        _website_expected_error_response(res, bucket.name, args['code'], IGNORE_FIELD, IGNORE_FIELD)
    else:
        assert(False)

@pytest.mark.s3website_routing_rules
@pytest.mark.s3website
@pytest.mark.fails_on_dbstore
@pytest.mark.parametrize('t', ROUTING_RULES_TESTS)
def test_routing_generator(t, routing_setup):
    if 'xml' in t and 'RoutingRules' in t['xml'] and len(t['xml']['RoutingRules']) > 0:
        t['xml']['RoutingRules'] = common.trim_xml(t['xml']['RoutingRules'])
    routing_check(t, **routing_setup)
