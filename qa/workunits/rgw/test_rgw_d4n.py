#!/usr/bin/python3

'''
This workunit tests the functionality of the D4N read workflow on a small object of size 4 and 
a multipart object of a randomly generated size. Each test runs the following workflow:

1. Upload the object
2. Perform a GET call (object should be retrieved from backend)
3. Compare the cached object's contents to the original object
4. Check the directory contents
5. Perform another GET call (object should be retrieved from datacache)
6. Compare the cached object's contents to the original object
7. Check the directory contents once more
'''

import logging as log
from configobj import ConfigObj
import botocore
import boto3
import redis
import subprocess
import os
import hashlib
import string
import random
import time

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

def test_small_object(r, client, s3):
    obj = s3.Object(bucket_name='bkt', key='test.txt')
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

    bucketID = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache/']).decode('latin-1').strip()
    datacache_path = '/tmp/rgw_d4n_datacache/' + bucketID + '/test.txt/'
    datacache = subprocess.check_output(['ls', '-a', datacache_path])
    datacache = datacache.decode('latin-1').strip().splitlines()
    if '#' in datacache[3]: # datablock key
      datacache = datacache[3]
    else:
      datacache = datacache[2]
    output = subprocess.check_output(['md5sum', datacache_path + datacache]).decode('latin-1')
    assert(output.splitlines()[0].split()[0] == hashlib.md5("test".encode('utf-8')).hexdigest())

    data = {}
    for entry in r.scan_iter("*_test.txt_0_4"):
        data = r.hgetall(entry)

        # directory entry comparisons
        assert(data.get('blockID') == '0')
        assert(data.get('deleteMarker') == '0')
        assert(data.get('size') == '4')
        assert(data.get('globalWeight') == '0')
        assert(data.get('objName') == 'test.txt')
        assert(data.get('bucketName') == bucketID)
        assert(data.get('dirty') == '0')
        assert(data.get('hosts') == '127.0.0.1:6379')

    # second get call
    response_get = obj.get()
    assert(response_get.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    # check logs to ensure object was retrieved from cache
    oid_in_cache = bucketID + "#" + data.get('version') + "test.txt#0" + data.get('size')
    res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): READ FROM CACHE: oid="' + oid_in_cache, '/var/log/ceph/rgw.ceph.client.0.log'])
    assert(res >= 1)

    # retrieve and compare cache contents
    body = get_body(response_get)
    assert(body == "test")

    datacache = subprocess.check_output(['ls', '-a', datacache_path])
    datacache = datacache.decode('latin-1').strip().splitlines()
    if '#' in datacache[3]: # datablock key
      datacache = datacache[3]
    else:
      datacache = datacache[2]
    output = subprocess.check_output(['md5sum', datacache_path + datacache]).decode('latin-1')
    assert(output.splitlines()[0].split()[0] == hashlib.md5("test".encode('utf-8')).hexdigest())

    for entry in r.scan_iter("*_test.txt_0_4"):
        data = r.hgetall(entry)

        # directory entries should remain consistent
        assert(data.get('blockID') == '0')
        assert(data.get('deleteMarker') == '0')
        assert(data.get('size') == '4')
        assert(data.get('globalWeight') == '0')
        assert(data.get('objName') == 'test.txt')
        assert(data.get('bucketName') == bucketID)
        assert(data.get('dirty') == '0')
        assert(data.get('hosts') == '127.0.0.1:6379')

    r.flushall()

def test_large_object(r, client, s3):
    key="mymultipart"
    bucket_name="bkt"
    content_type='text/bla'
    objlen = 30 * 1024 * 1024
    metadata = {'foo': 'bar'}

    (upload_id, multipart_data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client, content_type=content_type, metadata=metadata)
    client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

    file_path = os.path.dirname(__file__)+'mymultipart'

    # first get
    try:
        s3.Object(bucket_name, key).download_file(file_path)
    except botocore.exceptions.ClientError as e:
        log.error("ERROR: " + e)
        raise

    # check logs to ensure object was retrieved from storage backend
    res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): Fetching object from backend store"', '/var/log/ceph/rgw.ceph.client.0.log'])
    assert(res >= 1)

    # retrieve and compare cache contents
    with open(file_path, 'r') as body:
        assert(body.read() == multipart_data)

    time.sleep(0.1)
    bucketID = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache/']).decode('latin-1').strip()
    datacache_path = '/tmp/rgw_d4n_datacache/' + bucketID + '/mymultipart/'
    datacache = subprocess.check_output(['ls', '-a', datacache_path])
    datacache = datacache.decode('latin-1').splitlines()[2:]

    for file in datacache:
        if '#' in file: # data blocks
            ofs = int(file.split("#")[1])
            size = file.split("#")[2]
            if '_' in file: # account for temp files
                size = size.split("_")[0]

            output = subprocess.check_output(['md5sum', datacache_path + file]).decode('latin-1')
            assert(output.splitlines()[0].split()[0] == hashlib.md5(multipart_data[ofs:ofs+int(size)].encode('utf-8')).hexdigest())

    data = {}
    for entry in r.scan_iter("*_mymultipart_*"):
        data = r.hgetall(entry)
        entry_name = entry.split("_")

        # directory entry comparisons
        if len(entry_name) == 6: # versioned block
            assert(data.get('blockID') == entry_name[4])
            assert(data.get('deleteMarker') == '0')
            assert(data.get('size') == entry_name[5])
            assert(data.get('globalWeight') == '0')
            assert(data.get('objName') == '_:null_mymultipart')
            assert(data.get('bucketName') == bucketID)
            assert(data.get('dirty') == '0')
            assert(data.get('hosts') == '127.0.0.1:6379')
            continue

        assert(data.get('blockID') == entry_name[2])
        assert(data.get('deleteMarker') == '0')
        assert(data.get('size') == entry_name[3])
        assert(data.get('globalWeight') == '0')
        assert(data.get('objName') == 'mymultipart')
        assert(data.get('bucketName') == bucketID)
        assert(data.get('dirty') == '0')
        assert(data.get('hosts') == '127.0.0.1:6379')

    # second get
    try:
        s3.Object(bucket_name, key).download_file(file_path)
    except botocore.exceptions.ClientError as e:
        log.error("ERROR: " + e)
        raise

    # check logs to ensure object was retrieved from cache
    oid_in_cache = bucketID + "#" + data.get('version') + "mymultipart#0" + data.get('size')
    res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): READ FROM CACHE: oid="' + oid_in_cache, '/var/log/ceph/rgw.ceph.client.0.log'])
    assert(res >= 1)

    # retrieve and compare cache contents
    with open(file_path, 'r') as body:
        assert(body.read() == multipart_data)

    datacache = subprocess.check_output(['ls', '-a', datacache_path])
    datacache = datacache.decode('latin-1').splitlines()[2:]

    for file in datacache:
        if '#' in file: # data blocks
            ofs = int(file.split("#")[1])
            size = file.split("#")[2]
            if '_' in file: # account for temp files
                size = size.split("_")[0]

            output = subprocess.check_output(['md5sum', datacache_path + file]).decode('latin-1')
            assert(output.splitlines()[0].split()[0] == hashlib.md5(multipart_data[ofs:ofs+int(size)].encode('utf-8')).hexdigest())

    for entry in r.scan_iter("*_mymultipart_*"):
        data = r.hgetall(entry)
        entry_name = entry.split("_")

        # directory entries should remain consistent
        if len(entry_name) == 6: # versioned block
            assert(data.get('blockID') == entry_name[4])
            assert(data.get('deleteMarker') == '0')
            assert(data.get('size') == entry_name[5])
            assert(data.get('globalWeight') == '0')
            assert(data.get('objName') == '_:null_mymultipart')
            assert(data.get('bucketName') == bucketID)
            assert(data.get('dirty') == '0')
            assert(data.get('hosts') == '127.0.0.1:6379')
            continue

        assert(data.get('blockID') == entry_name[2])
        assert(data.get('deleteMarker') == '0')
        assert(data.get('size') == entry_name[3])
        assert(data.get('globalWeight') == '0')
        assert(data.get('objName') == 'mymultipart')
        assert(data.get('bucketName') == bucketID)
        assert(data.get('dirty') == '0')
        assert(data.get('hosts') == '127.0.0.1:6379')

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

    # Check for Redis instance
    try:
        connection = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        connection.ping() 
    except:
        log.error("ERROR: Redis instance not running.")
        raise

    # Create s3cmd config
    s3cmd_config_path = pwd + '/s3cfg'
    create_s3cmd_config(s3cmd_config_path, proto)

    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Run small object test
    test_small_object(r, client, s3)

    # Run large object test
    test_large_object(r, client, s3)
    
    # close filter client
    filter_client = [client for client in r.client_list()
                       if client.get('name') in ['D4N.Filter']]
    r.client_kill_filter(_id=filter_client[0].get('id'))

    log.info("D4NFilterTest completed.")

main()
log.info("Completed D4N tests")
