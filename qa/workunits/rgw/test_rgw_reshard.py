#!/usr/bin/python3

import logging as log
import time
import subprocess
import json
import boto3

"""
Rgw manual and dynamic resharding  testing against a running instance
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

log.basicConfig(level=log.DEBUG)
log.getLogger('botocore').setLevel(log.CRITICAL)
log.getLogger('boto3').setLevel(log.CRITICAL)
log.getLogger('urllib3').setLevel(log.CRITICAL)

""" Constants """
USER = 'tester'
DISPLAY_NAME = 'Testing'
ACCESS_KEY = 'NX5QOQKC6BH2IDN8HC7A'
SECRET_KEY = 'LnEsqNNqZIpkzauboDcLXLcYaWwLQ3Kop0zAnKIn'
BUCKET_NAME1 = 'myfoo'
BUCKET_NAME2 = 'mybar'
VER_BUCKET_NAME = 'myver'


def exec_cmd(cmd):
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


def get_radosgw_port():
    out = exec_cmd('sudo netstat -nltp | grep radosgw')
    log.debug('output: %s' % out)
    x = out.decode('utf8').split(" ")
    port = [i for i in x if ':' in i][0].split(':')[1]
    log.info('radosgw port: %s' % port)
    return port


class BucketStats:
    def __init__(self, bucket_name, bucket_id, num_objs=0, size_kb=0, num_shards=0):
        self.bucket_name = bucket_name
        self.bucket_id = bucket_id
        self.num_objs = num_objs
        self.size_kb = size_kb
        self.num_shards = num_shards if num_shards > 0 else 1

    def get_num_shards(self):
        self.num_shards = get_bucket_num_shards(self.bucket_name, self.bucket_id)


def get_bucket_stats(bucket_name):
    """
    function to get bucket stats
    """
    cmd = exec_cmd("radosgw-admin bucket stats --bucket %s" % bucket_name)
    json_op = json.loads(cmd)
    bucket_id = json_op['id']
    num_shards_op = json_op['num_shards']
    if len(json_op['usage']) > 0:
        num_objects = json_op['usage']['rgw.main']['num_objects']
        size_kb = json_op['usage']['rgw.main']['size_kb']
    else:
        num_objects = 0
        size_kb = 0
    log.debug("bucket %s id %s num_objects %d size_kb %d num_shards %d", bucket_name, bucket_id,
              num_objects, size_kb, num_shards_op)
    return BucketStats(bucket_name, bucket_id, num_objects, size_kb, num_shards_op)


def get_bucket_num_shards(bucket_name, bucket_id):
    """
    function to get bucket num shards
    """
    metadata = 'bucket.instance:' + bucket_name + ':' + bucket_id
    log.debug("metadata %s", metadata)
    cmd = exec_cmd('radosgw-admin metadata get %s' % metadata)
    json_op = json.loads(cmd)
    num_shards = json_op['data']['bucket_info']['num_shards']
    log.debug("bucket %s id %s num_shards %d", bucket_name, bucket_id, num_shards)
    return num_shards


def main():
    """
    execute manual and dynamic resharding commands
    """
    # create user
    exec_cmd('radosgw-admin user create --uid %s --display-name %s --access-key %s --secret %s'
                   % (USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY))

    def boto_connect(portnum, ssl, proto):
        endpoint = proto + '://localhost:' + portnum
        conn = boto3.resource('s3',
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY,
                              use_ssl=ssl,
                              endpoint_url=endpoint,
                              verify=False,
                              config=None,
                              )
        return conn

    port = get_radosgw_port()

    if port == '80':
        connection = boto_connect(port, ssl=False, proto='http')
    elif port == '443':
        connection = boto_connect(port, ssl=True, proto='https')

    # create a bucket
    bucket1 = connection.create_bucket(Bucket=BUCKET_NAME1)
    bucket2 = connection.create_bucket(Bucket=BUCKET_NAME2)
    ver_bucket = connection.create_bucket(Bucket=VER_BUCKET_NAME)
    connection.BucketVersioning('ver_bucket')

    bucket_stats1 = get_bucket_stats(BUCKET_NAME1)
    bucket_stats2 = get_bucket_stats(BUCKET_NAME2)
    ver_bucket_stats = get_bucket_stats(VER_BUCKET_NAME)

    bucket1_acl = connection.BucketAcl(BUCKET_NAME1).load()
    bucket2_acl = connection.BucketAcl(BUCKET_NAME2).load()
    ver_bucket_acl = connection.BucketAcl(VER_BUCKET_NAME).load()

    # TESTCASE 'reshard-add','reshard','add','add bucket to resharding queue','succeeds'
    log.debug(' test: reshard add')
    num_shards_expected = bucket_stats1.num_shards + 1
    cmd = exec_cmd('radosgw-admin reshard add --bucket %s --num-shards %s' % (BUCKET_NAME1, num_shards_expected))
    cmd = exec_cmd('radosgw-admin reshard list')
    json_op = json.loads(cmd)
    log.debug('bucket name %s', json_op[0]['bucket_name'])
    assert json_op[0]['bucket_name'] == BUCKET_NAME1
    assert json_op[0]['new_num_shards'] == num_shards_expected

    # TESTCASE 'reshard-process','reshard','','process bucket resharding','succeeds'
    log.debug(' test: reshard process')
    cmd = exec_cmd('radosgw-admin reshard process')
    time.sleep(5)
    # check bucket shards num
    bucket_stats1 = get_bucket_stats(BUCKET_NAME1)
    bucket_stats1.get_num_shards()
    if bucket_stats1.num_shards != num_shards_expected:
        log.error("Resharding failed on bucket %s. Expected number of shards are not created" % BUCKET_NAME1)

    # TESTCASE 'reshard-add','reshard','add','add non empty bucket to resharding queue','succeeds'
    log.debug(' test: reshard add non empty bucket')
    # create objs
    num_objs = 8
    for i in range(0, num_objs):
        connection.Object(BUCKET_NAME1, ('key'+str(i))).put(Body=b"some_data")

    num_shards_expected = bucket_stats1.num_shards + 1
    cmd = exec_cmd('radosgw-admin reshard add --bucket %s --num-shards %s' % (BUCKET_NAME1, num_shards_expected))
    cmd = exec_cmd('radosgw-admin reshard list')
    json_op = json.loads(cmd)
    log.debug('bucket name %s', json_op[0]['bucket_name'])
    assert json_op[0]['bucket_name'] == BUCKET_NAME1
    assert json_op[0]['new_num_shards'] == num_shards_expected

    # TESTCASE 'reshard process ,'reshard','process','reshard non empty bucket','succeeds'
    log.debug(' test: reshard process non empty bucket')
    cmd = exec_cmd('radosgw-admin reshard process')
    # check bucket shards num
    bucket_stats1 = get_bucket_stats(BUCKET_NAME1)
    bucket_stats1.get_num_shards()
    if bucket_stats1.num_shards != num_shards_expected:
        log.error("Resharding failed on bucket %s. Expected number of shards are not created" % BUCKET_NAME1)

    # TESTCASE 'manual resharding','bucket', 'reshard','','manual bucket resharding','succeeds'
    log.debug(' test: manual reshard bucket')
    # create objs
    num_objs = 11
    for i in range(0, num_objs):
        connection.Object(BUCKET_NAME2, ('key' + str(i))).put(Body=b"some_data")

    time.sleep(10)
    num_shards_expected = bucket_stats2.num_shards + 1
    cmd = exec_cmd('radosgw-admin bucket reshard --bucket %s --num-shards %s' % (BUCKET_NAME2,
                                                                                 num_shards_expected))
    # check bucket shards num
    bucket_stats2 = get_bucket_stats(BUCKET_NAME2)
    bucket_stats2.get_num_shards()
    if bucket_stats2.num_shards != num_shards_expected:
        log.error("Resharding failed on bucket %s. Expected number of shards are not created" % BUCKET_NAME2)

    # TESTCASE 'versioning reshard-','bucket', reshard','versioning reshard','succeeds'
    log.debug(' test: reshard versioned bucket')
    num_shards_expected = ver_bucket_stats.num_shards + 1
    cmd = exec_cmd('radosgw-admin bucket reshard --bucket %s --num-shards %s' % (VER_BUCKET_NAME,
                                                                                 num_shards_expected))
    # check bucket shards num
    ver_bucket_stats = get_bucket_stats(VER_BUCKET_NAME)
    assert ver_bucket_stats.num_shards == num_shards_expected

    # TESTCASE 'check acl'
    new_bucket1_acl = connection.BucketAcl(BUCKET_NAME1).load()
    assert new_bucket1_acl == bucket1_acl
    new_bucket2_acl = connection.BucketAcl(BUCKET_NAME2).load()
    assert new_bucket2_acl == bucket2_acl
    new_ver_bucket_acl = connection.BucketAcl(VER_BUCKET_NAME).load()
    assert new_ver_bucket_acl == ver_bucket_acl

    # Clean up
    log.debug("Deleting bucket %s", BUCKET_NAME1)
    bucket1.objects.all().delete()
    bucket1.delete()
    log.debug("Deleting bucket %s", BUCKET_NAME2)
    bucket2.objects.all().delete()
    bucket2.delete()
    log.debug("Deleting bucket %s", VER_BUCKET_NAME)
    ver_bucket.delete()


main()
log.info("Completed resharding tests")
