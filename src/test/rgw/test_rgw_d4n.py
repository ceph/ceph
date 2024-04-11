#!/usr/bin/python3

import boto3
import redis
import subprocess
import json
import os
import hashlib
import string
import random
import time

""" Constants """
ACCESS_KEY = 'test3'
SECRET_KEY = 'test3'

def exec_cmd(cmd):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = proc.communicate()
        if proc.returncode == 0:
            return out
        else:
            raise Exception("error: %s \nreturncode: %s" % (err, proc.returncode))
    except Exception as e:
        return False

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
    
    out = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache']).decode('latin-1') 
    assert(out.splitlines()[0] == "D_bkt_test.txt_0_4")

    data = r.hgetall('bkt_test.txt_0_4')

    # directory entry comparisons
    assert(data.get('blockID') == '0')
    assert(data.get('version') == '')
    assert(data.get('size') == '4')
    assert(data.get('globalWeight') == '0')
    assert(data.get('blockHosts') == '127.0.0.1:8001')
    assert(data.get('objName') == 'test.txt')
    assert(data.get('bucketName') == 'bkt')
    assert(data.get('creationTime') == '')
    assert(data.get('dirty') == '1')
    assert(data.get('objHosts') == '')
    
    # Allow cleaning cycle to pass
    time.sleep(6)

    out = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache']).decode('latin-1') 
    assert(out.splitlines()[0] == "RD_bkt_test.txt_0_4")
    assert(out.splitlines()[1] == "bkt_test.txt_0_4")

    # Check contents of both files
    out = subprocess.check_output(['cat', '/tmp/rgw_d4n_datacache/bkt_test.txt_0_4']).decode('latin-1') 
    assert(out == "test")

    out = subprocess.check_output(['cat', '/tmp/rgw_d4n_datacache/RD_bkt_test.txt_0_4']).decode('latin-1') 
    assert(out == "test")

    data = r.hgetall('bkt_test.txt_0_4')

    # directory entry comparisons
    assert(data.get('blockID') == '0')
    assert(data.get('version') == '')
    assert(data.get('size') == '4')
    assert(data.get('globalWeight') == '0')
    assert(data.get('blockHosts') == '127.0.0.1:8001_127.0.0.1:8002')
    assert(data.get('objName') == 'test.txt')
    assert(data.get('bucketName') == 'bkt')
    assert(data.get('creationTime') == '')
    assert(data.get('dirty') == 'false')
    assert(data.get('objHosts') == '')

def main():
    out = exec_cmd('pwd')
    pwd = get_cmd_output(out)
    endpoint = "http://localhost:8001"

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
        raise

    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Run small object test
    test_small_object(r, client, obj)

main()
