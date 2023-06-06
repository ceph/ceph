#!/usr/bin/python3

import logging as log
from configobj import ConfigObj
import boto3
import redis
import subprocess
import json

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

def get_cmd_output(cmd_out):
    out = cmd_out.decode('utf8')
    out = out.strip('\n')
    return out

def test_directory_methods(r, client, obj):
    test_txt = b'test'

    # setValue call
    response_put = obj.put(Body=test_txt)

    assert(response_put.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    data = r.hgetall('rgw-object:test.txt:directory')

    assert(data.get('key') == 'rgw-object:test.txt:directory')
    assert(data.get('size') == '4')
    assert(data.get('bucket_name') == 'bkt')
    assert(data.get('obj_name') == 'test.txt')
    assert(data.get('hosts') == '127.0.0.1:6379')

    # getValue call
    response_get = obj.get()

    assert(response_get.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    data = r.hgetall('rgw-object:test.txt:directory')

    assert(data.get('key') == 'rgw-object:test.txt:directory')
    assert(data.get('size') == '4')
    assert(data.get('bucket_name') == 'bkt')
    assert(data.get('obj_name') == 'test.txt')
    assert(data.get('hosts') == '127.0.0.1:6379')

    # delValue call
    response_del = obj.delete()

    assert(response_del.get('ResponseMetadata').get('HTTPStatusCode') == 204)
    assert(r.exists('rgw-object:test.txt:directory') == False)

    r.flushall()

def test_cache_methods(r, client, obj):
    test_txt = b'test'

    # setObject call
    response_put = obj.put(Body=test_txt)

    assert(response_put.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    data = r.hgetall('rgw-object:test.txt:cache')
    output = subprocess.check_output(['radosgw-admin', 'object', 'stat', '--bucket=bkt', '--object=test.txt'])
    attrs = json.loads(output.decode('latin-1'))

    assert((data.get(b'user.rgw.tail_tag')) == attrs.get('attrs').get('user.rgw.tail_tag').encode("latin-1") + b'\x00')
    assert((data.get(b'user.rgw.idtag')) == attrs.get('tag').encode("latin-1") + b'\x00')
    assert((data.get(b'user.rgw.etag')) == attrs.get('etag').encode("latin-1"))
    assert((data.get(b'user.rgw.x-amz-content-sha256')) == attrs.get('attrs').get('user.rgw.x-amz-content-sha256').encode("latin-1") + b'\x00')
    assert((data.get(b'user.rgw.x-amz-date')) == attrs.get('attrs').get('user.rgw.x-amz-date').encode("latin-1") + b'\x00')

    tmp1 = '\x08\x06L\x01\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x06\x84\x00\x00\x00\n\nj\x00\x00\x00\x03\x00\x00\x00bkt+\x00\x00\x00'
    tmp2 = '+\x00\x00\x00'
    tmp3 = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\x00\x00test.txt\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00!\x00\x00\x00'
    tmp4 = '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x01 \x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
           '\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00default-placement\x11\x00\x00\x00default-placement\x00\x00\x00\x00\x02\x02\x18' \
           '\x00\x00\x00\x04\x00\x00\x00none\x01\x01\t\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    assert(data.get(b'user.rgw.manifest') == tmp1.encode("latin-1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                     + tmp2.encode("latin-1") + attrs.get('manifest').get('tail_placement').get('bucket').get('bucket_id').encode("utf-8")
                     + tmp3.encode("latin-1") + attrs.get('manifest').get('prefix').encode("utf-8")
                     + tmp4.encode("latin-1"))

    tmp5 = '\x02\x02\x81\x00\x00\x00\x03\x02\x11\x00\x00\x00\x06\x00\x00\x00s3main\x03\x00\x00\x00Foo\x04\x03d\x00\x00\x00\x01\x01\x00\x00\x00\x06\x00\x00' \
           '\x00s3main\x0f\x00\x00\x00\x01\x00\x00\x00\x06\x00\x00\x00s3main\x05\x035\x00\x00\x00\x02\x02\x04\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00s3main' \
           '\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\x04\x00\x00\x00\x0f\x00\x00\x00\x03\x00\x00\x00Foo\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
           '\x00\x00\x00'
    assert((data.get(b'user.rgw.acl')) == tmp5.encode("latin-1"))

    # getObject call
    response_get = obj.get()

    assert(response_get.get('ResponseMetadata').get('HTTPStatusCode') == 200)

    # Copy to new object with 'COPY' directive; metadata value should not change
    obj.metadata.update({'test':'value'})
    m = obj.metadata
    m['test'] = 'value_replace'

    # copyObject call
    client.copy_object(Bucket='bkt', Key='test_copy.txt', CopySource='bkt/test.txt', Metadata = m, MetadataDirective='COPY')

    assert(r.hexists('rgw-object:test_copy.txt:cache', b'user.rgw.x-amz-meta-test') == 0)

    # Update object with 'REPLACE' directive; metadata value should change
    client.copy_object(Bucket='bkt', Key='test.txt', CopySource='bkt/test.txt', Metadata = m, MetadataDirective='REPLACE')

    data = r.hget('rgw-object:test.txt:cache', b'user.rgw.x-amz-meta-test')

    assert(data == b'value_replace\x00')

    # Ensure cache entry exists in cache before deletion
    assert(r.exists('rgw-object:test.txt:cache') == True)

    # delObject call
    response_del = obj.delete()

    assert(response_del.get('ResponseMetadata').get('HTTPStatusCode') == 204)
    assert(r.exists('rgw-object:test.txt:cache') == False)

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

    test_directory_methods(r, client, obj)

    # Responses should not be decoded
    r = redis.Redis(host='localhost', port=6379, db=0)

    test_cache_methods(r, client, obj)

    log.info("D4NFilterTest successfully completed.")

main()
log.info("Completed D4N tests")
