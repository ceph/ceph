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
import time

log.basicConfig(level=log.DEBUG)

""" Constants """
USER = 'test3'
DISPLAY_NAME = 'test3'
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

def test_remote_cache_api(r, client, obj):
    # cause eviction
    test_txt = 'hello world'

    response_put = obj.put(Body=test_txt)

    # give second cluster a different local host value
    exec_cmd('ceph config set client rgw_local_cache_address 127.0.0.1:8001')

    assert(response_put.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    assert(os.path.exists('/tmp/rgw_d4n_datacache/D_bkt_test.txt_0_11') == True)

    data = r.hgetall('bkt_test.txt_0_11')

    # directory entry comparisons
    assert(data.get('blockID') == '0')
    assert(data.get('version') == '')
    assert(data.get('size') == '11')
    assert(data.get('globalWeight') == '0')
    assert(data.get('blockHosts') == '127.0.0.1:8000') 
    assert(data.get('objName') == 'test.txt')
    assert(data.get('bucketName') == 'bkt')
    assert(data.get('creationTime') == '')
    assert(data.get('dirty') == '1')
    assert(data.get('objHosts') == '')
    
    # allow cleaning cycle to pass
    time.sleep(20)

    log.debug(subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache']).decode('latin-1'))
    assert(os.path.exists('/tmp/rgw_d4n_datacache/bkt_test.txt_0_11') == True)
    assert(os.path.exists('/tmp/rgw_d4n_datacache/RD_bkt_test.txt_0_11') == True)

    # check contents of both files
    out = subprocess.check_output(['cat', '/tmp/rgw_d4n_datacache/bkt_test.txt_0_11']).decode('latin-1')
    assert(out == "hello world")

    out = subprocess.check_output(['cat', '/tmp/rgw_d4n_datacache/RD_bkt_test.txt_0_11']).decode('latin-1')
    assert(out == "hello world")

    log.debug("keys:")
    log.debug(r.keys('*'))
    data = r.hgetall('bkt_test.txt_0_11')
    log.debug(data)

    # directory entry comparisons
    assert(data.get('blockID') == '0')
    assert(data.get('version') == '')
    assert(data.get('size') == '11')
    assert(data.get('globalWeight') == '0')
    assert(data.get('blockHosts') == '127.0.0.1:8000_127.0.0.1:8001') 
    assert(data.get('objName') == 'test.txt')
    assert(data.get('bucketName') == 'bkt')
    assert(data.get('creationTime') == '')
    assert(data.get('dirty') == 'false')
    assert(data.get('objHosts') == '')

    r.flushall()
     
def main():
    """
    execute the d4n remote test
    """

    # Setup for test
    log.info("D4N Remote Test Setup.")

    out = exec_cmd('pwd')
    pwd = get_cmd_output(out)
    log.debug("pwd is: %s", pwd)

    endpoint, proto = get_radosgw_endpoint()

    # Create user
    exec_cmd('radosgw-admin user create --uid %s --display-name %s --access-key %s --secret %s'
            % (USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY))

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

    # Run remote cache API test 
    test_remote_cache_api(r, client, obj)

    log.info("D4NFilterTest completed.")

main()
log.info("Completed D4N tests")
