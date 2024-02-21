#!/usr/bin/python3

'''
This workunits tests the functionality of the D4N read workflow on a small object of size 4.
'''

import logging as log
from configobj import ConfigObj
import boto3
import redis
import subprocess
import json
import os
import hashlib
import string
import random

log.basicConfig(level=log.DEBUG)

""" Constants """
ACCESS_KEY = 'test3'
SECRET_KEY = 'test3'

def exec_cmd(cmd):
    log.debug("exec_cmd(%s)", cmd)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = proc.communicate()
        if proc.returncode == 0:
            log.info('command succeeded')
            if out is not None: log.info(out)
            return out
        else:
            raise Exception("error: %s \nreturncode: %s" % (err, proc.returncode))
    except Exception as e:
        log.error('command failed')
        log.error(e)
        return False

def get_radosgw_endpoint():
    out = exec_cmd('sudo netstat -nltp | egrep "rados|valgr"')  # short for radosgw/valgrind
    x = out.decode('utf8').split(" ")
    port = [i for i in x if ':' in i][0].split(':')[1]
    log.info('radosgw port: %s' % port)
    proto = "http"
    hostname = '127.0.0.1'

    if port == '443':
        proto = "https"

    endpoint = "%s://%s:%s" % (proto, hostname, port)

    log.info("radosgw endpoint is: %s", endpoint)
    return endpoint, proto

def create_s3cmd_config(path, proto):
    """
    Creates a minimal config file for s3cmd
    """
    log.info("Creating s3cmd config...")

    use_https_config = "False"
    log.info("proto for s3cmd config is %s", proto)
    if proto == "https":
        use_https_config = "True"

    s3cmd_config = ConfigObj(
        indent_type='',
        infile={
            'default':
                {
                'host_bucket': 'no.way.in.hell',
                'use_https': use_https_config,
                },
            }
    )

    f = open(path, 'wb')
    s3cmd_config.write(f)
    f.close()
    log.info("s3cmd config written")

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

def _multipart_upload(bucket_name, key, size, part_size=5*1024*1024, client=None, content_type=None, metadata=None, resend_parts=[]):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """

    if content_type == None and metadata == None:
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

def get_cmd_output(cmd_out):
    out = cmd_out.decode('utf8')
    out = out.strip('\n')
    return out

def get_body(response):
    body = response['Body']
    got = body.read()
    if type(got) is bytes:
        got = got.decode()
    return got

def test_small_object(r, client, obj):
    test_txt = 'test'

    response_put = obj.put(Body=test_txt)
    assert(response_put.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    # first get call
    response_get = obj.get()
    assert(response_get.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    # check logs to ensure object was retrieved from storage backend
    res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): Fetching object from backend store"', '/var/log/ceph/rgw.ceph.client.0.log'])
    assert(res >= 1)

    # retrieve and compare cache contents
    body = get_body(response_get)
    assert(body == "test")

    data = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache/'])
    data = data.decode('latin-1').strip()
    output = subprocess.check_output(['md5sum', '/tmp/rgw_d4n_datacache/' + data]).decode('latin-1')

    assert(output.splitlines()[0].split()[0] == hashlib.md5("test".encode('utf-8')).hexdigest())

    data = r.hgetall('bkt_test.txt_0_4')
    output = subprocess.check_output(['radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=test.txt'])
    attrs = json.loads(output.decode('latin-1'))

    # directory entry comparisons
    assert(data.get('blockID') == '0')
    assert(data.get('version') == attrs.get('tag'))
    assert(data.get('size') == '4')
    assert(data.get('globalWeight') == '0')
    assert(data.get('blockHosts') == '127.0.0.1:6379')
    assert(data.get('objName') == 'test.txt')
    assert(data.get('bucketName') == 'bkt')
    assert(data.get('creationTime') == attrs.get('mtime'))
    assert(data.get('dirty') == '0')
    assert(data.get('objHosts') == '')

    # repopulate cache
    response_put = obj.put(Body=test_txt)
    assert(response_put.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    # second get call
    response_get = obj.get()
    assert(response_get.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    # check logs to ensure object was retrieved from cache
    res = subprocess.call(['grep', '"SSDCache: get_async(): ::aio_read(), ret=0"', '/var/log/ceph/rgw.ceph.client.0.log'])
    assert(res >= 1)

    # retrieve and compare cache contents
    body = get_body(response_get)
    assert(body == "test")

    data = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache/'])
    data = data.decode('latin-1').strip()
    output = subprocess.check_output(['md5sum', '/tmp/rgw_d4n_datacache/' + data]).decode('latin-1')

    assert(output.splitlines()[0].split()[0] == hashlib.md5("test".encode('utf-8')).hexdigest())

    data = r.hgetall('bkt_test.txt_0_4')
    output = subprocess.check_output(['radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=test.txt'])
    attrs = json.loads(output.decode('latin-1'))

    # directory entries should remain consistent
    assert(data.get('blockID') == '0')
    assert(data.get('version') == attrs.get('tag'))
    assert(data.get('size') == '4')
    assert(data.get('globalWeight') == '0')
    assert(data.get('blockHosts') == '127.0.0.1:6379')
    assert(data.get('objName') == 'test.txt')
    assert(data.get('bucketName') == 'bkt')
    assert(data.get('creationTime') == attrs.get('mtime'))
    assert(data.get('dirty') == '0')
    assert(data.get('objHosts') == '')

    r.flushall()

def test_large_object(r, client, s3):
    key="mymultipart"
    bucket_name="bkt"
    content_type='text/bla'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}

    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client, content_type=content_type, metadata=metadata)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    file_path = os.path.dirname(__file__)+'mymultipart'

    # first get
    s3.Object(bucket_name, key).download_file(file_path)

    # check logs to ensure object was retrieved from storage backend
    res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): Fetching object from backend store"', '/var/log/ceph/rgw.ceph.client.0.log'])
    assert(res >= 1)

    # retrieve and compare cache contents
    with open(file_path, 'r') as body:
        assert(body.read() == data)

    datacache_path = '/tmp/rgw_d4n_datacache/'
    datacache = subprocess.check_output(['ls', datacache_path])
    datacache = datacache.decode('latin-1').splitlines()

    for file in datacache:
        ofs = int(file.split("_")[3])
        size = int(file.split("_")[4])
        output = subprocess.check_output(['md5sum', datacache_path + file]).decode('latin-1')
        assert(output.splitlines()[0].split()[0] == hashlib.md5(data[ofs:ofs+size].encode('utf-8')).hexdigest())

    output = subprocess.check_output(['radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=mymultipart'])
    attrs = json.loads(output.decode('latin-1'))

    for entry in r.scan_iter("bkt_mymultipart_*"):
        data = r.hgetall(entry)
        name = entry.split("_")

        # directory entry comparisons
        assert(data.get('blockID') == name[2])
        assert(data.get('version') == attrs.get('tag'))
        assert(data.get('size') == name[3])
        assert(data.get('globalWeight') == '0')
        assert(data.get('blockHosts') == '127.0.0.1:6379')
        assert(data.get('objName') == 'mymultipart')
        assert(data.get('bucketName') == 'bkt')
        assert(data.get('creationTime') == attrs.get('mtime'))
        assert(data.get('dirty') == '0')
        assert(data.get('objHosts') == '')

    # repopulate cache
    (upload_id, data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client, content_type=content_type, metadata=metadata)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    #second get
    s3.Object(bucket_name, key).download_file(file_path)

    # check logs to ensure object was retrieved from cache
    res = subprocess.call(['grep', '"SSDCache: get_async(): ::aio_read(), ret=0"', '/var/log/ceph/rgw.ceph.client.0.log'])
    assert(res >= 1)

    # retrieve and compare cache contents
    with open(file_path, 'r') as body:
        assert(body.read() == data)

    datacache_path = '/tmp/rgw_d4n_datacache/'
    datacache = subprocess.check_output(['ls', datacache_path])
    datacache = datacache.decode('latin-1').splitlines()

    for file in datacache:
        ofs = int(file.split("_")[3])
        size = int(file.split("_")[4])
        output = subprocess.check_output(['md5sum', datacache_path + file]).decode('latin-1')
        assert(output.splitlines()[0].split()[0] == hashlib.md5(data[ofs:ofs+size].encode('utf-8')).hexdigest())

    output = subprocess.check_output(['radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=mymultipart'])
    attrs = json.loads(output.decode('latin-1'))

    for key in r.scan_iter("bkt_mymultipart_*"):
        data = r.hgetall(key)
        name = key.split("_")

        # directory entry comparisons
        assert(data.get('blockID') == name[2])
        assert(data.get('version') == attrs.get('tag'))
        assert(data.get('size') == name[3])
        assert(data.get('globalWeight') == '0')
        assert(data.get('blockHosts') == '127.0.0.1:6379')
        assert(data.get('objName') == 'mymultipart')
        assert(data.get('bucketName') == 'bkt')
        assert(data.get('creationTime') == attrs.get('mtime'))
        assert(data.get('dirty') == '0')
        assert(data.get('objHosts') == '')

    r.flushall()

def main():
    """
    execute the d4n test
    """

    # Setup for test
    log.info("D4NFilterTest setup.")

    out = exec_cmd('pwd')
    pwd = get_cmd_output(out)
    log.debug("pwd is: %s", pwd)

    endpoint, proto = get_radosgw_endpoint()

    client = boto3.client(service_name='s3',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                endpoint_url=endpoint,
                use_ssl=False,
                verify=False)

    s3 = boto3.resource('s3', 
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                endpoint_url=endpoint, 
                use_ssl=False,
                verify=False)

    bucket = s3.Bucket('bkt')
    bucket.create()
    obj = s3.Object(bucket_name='bkt', key='test.txt')

    # Check for Redis instance
    try:
        connection = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        connection.ping() 
    except:
        log.debug("ERROR: Redis instance not running.")
        raise

    # Create s3cmd config
    s3cmd_config_path = pwd + '/s3cfg'
    create_s3cmd_config(s3cmd_config_path, proto)

    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Run small object test
    test_small_object(r, client, obj)

    # Run large object test
    test_large_object(r, client, s3)

    log.info("D4NFilterTest completed.")

main()
log.info("Completed D4N tests")
