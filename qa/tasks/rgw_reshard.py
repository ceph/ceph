"""
Rgw manual and dynamic resharding  testing against a running instance
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#


import sys
from teuthology.config import config
from teuthology.orchestra import cluster, remote
import argparse;

import copy
import json
import logging
import time
import datetime
import Queue
import bunch

import sys

from cStringIO import StringIO

import boto.exception
import boto.s3.connection
import boto.s3.acl
from boto.utils import RequestHook

import httplib2

import util.rgw as rgw_utils

from util.rgw import rgwadmin, get_user_summary, get_user_successful_ops

log = logging.getLogger(__name__)

class UserStats:
    def __init__(self, uid, num_entries =0 , total_bytes = 0, total_bytes_rounded = 0):
        self.uid = uid
        self.num_entries = num_entries
        self.total_bytes = total_bytes
        self.total_bytes_rounded = total_bytes_rounded

    def __eq__(self, other):
        if other is None:
            return False
        return (self.uid, self.num_entries, self.total_bytes, self.total_bytes_rounded)   == (other.uid, other.num_entries, other.total_bytes, other.total_bytes_rounded)

class BucketStats:
    def __init__(self, bucket_name, bucket_id, num_objs=0, size_kb=0, num_shards = 0):
        self.bucket_name= bucket_name
        self.bucket_id= bucket_id
        self.num_objs= num_objs
        self.size_kb= size_kb
        self.num_shards = num_shards if num_shards > 0 else 1

    def get_num_shards(self, ctx, client):
        self.num_shards = get_bucket_num_shards(ctx, client, self.bucket_name, self.bucket_id)

def  get_bucket_stats(ctx, client, bucket_name):
    """
    function to get bucket stats
    """
    (err, out) = rgwadmin(ctx, client,
                          ['bucket', 'stats', '--bucket', bucket_name], check_status=True)
    assert err == 0
    bucket_id = out['id']
    num_objects = 0
    size_kb = 0
    if (len(out['usage']) > 0):
        num_objects = out['usage']['rgw.main']['num_objects']
        size_kb = out['usage']['rgw.main']['size_kb']

    num_shards = get_bucket_num_shards(ctx, client, bucket_name, bucket_id)
    log.debug("bucket %s id %s num_objects %d size_kb %d num_shards %d", bucket_name, bucket_id, num_objects, size_kb, num_shards)
    return BucketStats(bucket_name, bucket_id, num_objects, size_kb, num_shards)


def  get_user_stats(ctx, client, uid):
    """
    function to get user stats
    """
    (err, out) = rgwadmin(ctx, client,
                          ['user', 'stats', '--uid', uid, '--sync-stats'], check_status=True)
    assert err == 0
    total_entries = out['stats']['total_entries']
    total_bytes = out['stats']['total_bytes']
    total_bytes_rounded = out['stats']['total_bytes_rounded']
    log.debug("user %s total_entries %d total_bytes %d rounded %d", uid, total_entries, total_bytes, total_bytes_rounded)
    return UserStats(uid, total_entries, total_bytes, total_bytes_rounded)

def get_bucket_num_shards(ctx, client, bucket_name, bucket_id):
    """
    function to get bucket num shards
    """
    metadata = 'bucket.instance:' + bucket_name +':' + bucket_id
    log.debug("metadata %s", metadata)
    (err, out) = rgwadmin(ctx, client,
                          ['metadata', 'get', metadata], check_status=True)
    assert err == 0
    num_shards = out['data']['bucket_info']['num_shards']
    log.debug("bucket %s id %s num_shards %d", bucket_name, bucket_id, num_shards )
    return num_shards

def task(ctx, config):
    """
    Test resharding functionality against a running rgw instance.
    """
    global log

    log.debug('rgw_reshard config is: %r', config)

    assert ctx.rgw.config, \
        "rgw_reshard task needs a config passed from the rgw task"
    config = ctx.rgw.config
    log.debug('rgw_reshard config is: %r', config)

    clients_from_config = config.keys()
    log.debug('rgw_reshard config is: %r', config)

    # choose first client as default
    client = clients_from_config[0]
    log.debug('rgw_reshard client is: %r', client)

    # once the client is chosen, pull the host name and  assigned port out of
    # the role_endpoints that were assigned by the rgw task
    endpoint  = ctx.rgw.role_endpoints[client]
    remote_host = endpoint.hostname
    remote_port = endpoint.port
    log.debug('remote host %r remote port %r', remote_host, remote_port)

    ##
    user = 'testid'
    display_name = 'M. Tester'
    email = 'tester@ceph.com'
    access_key='0555b35654ad1656d804'
    secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
    bucket_name1='myfoo'
    bucket_name2='mybar'
    ver_bucket_name ='myver'

    (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', user,
            '--display-name', display_name,
            '--access-key', access_key,
            '--secret',  secret_key,
    ])

    # connect to rgw
    connection = boto.s3.connection.S3Connection(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        is_secure=False,
        port=remote_port,
        host=remote_host,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    # create a bucket
    bucket1 = connection.create_bucket(bucket_name1)
    bucket2 = connection.create_bucket(bucket_name2)
    ver_bucket = connection.create_bucket(ver_bucket_name)
    ver_bucket.configure_versioning(True)

    bucket_stats1 = get_bucket_stats(ctx, client, bucket_name1)
    bucket_stats2 = get_bucket_stats(ctx, client, bucket_name2)
    ver_bucket_stats = get_bucket_stats(ctx, client, ver_bucket_name)

    bucket1_acl = bucket1.get_xml_acl()
    bucket2_acl = bucket2.get_xml_acl()
    ver_bucket_acl = ver_bucket.get_xml_acl()

    # TESTCASE 'reshard-list','reshard','list','no resharding','succeeds, empty list'
    log.debug(' test: empty reshard list')
    (err, out) = rgwadmin(ctx, client, ['reshard', 'list'], check_status=True)
    assert len(out) == 0

    # TESTCASE 'reshard-add','reshard','add','add bucket to resharding queue','succeeds'
    log.debug(' test: reshard add')
    old_user_stats = get_user_stats(ctx, client, user)
    num_shards = bucket_stats1.num_shards + 1
    (err, out) = rgwadmin(ctx, client, ['reshard', 'add', '--bucket', bucket_name1, '--num-shards', str(num_shards)], check_status=True)

    (err, out) = rgwadmin(ctx, client, ['reshard', 'list'], check_status=True)
    assert len(out) == 1
    assert out[0]['bucket_name'] == bucket_name1

    # TESTCASE 'reshard-process','reshard','','process bucket resharding','succeeds'
    log.debug(' test: reshard process')
    (err, out) = rgwadmin(ctx, client, ['reshard', 'process', '--bucket', bucket_name1], check_status=True)
    log.debug("process err %d" , err)
    # check bucket shards num
    bucket_stats1 = get_bucket_stats(ctx, client, bucket_name1)
    assert  bucket_stats1.num_shards == num_shards
    new_user_stats = get_user_stats(ctx, client, user)
    assert old_user_stats == new_user_stats


    # TESTCASE 'reshard-add','reshard','add','add non empty bucket to resharding queue','succeeds'
    log.debug(' test: reshard add non empty bucket')
    # create objs
    num_objs = 8
    for i in range(0, num_objs):
        key = boto.s3.key.Key(bucket1)
        key.key = 'obj' + `i`
        key.set_contents_from_string(key.key)

    old_user_stats = get_user_stats(ctx, client, user)
    num_shards = bucket_stats1.num_shards + 1
    (err, out) = rgwadmin(ctx, client, ['reshard', 'add', '--bucket', bucket_name1, '--num-shards', str(num_shards)], check_status=True)

    (err, out) = rgwadmin(ctx, client, ['reshard', 'list'], check_status=True)
    assert len(out) == 1
    log.debug('bucket name %s', out[0]['bucket_name'])
    assert out[0]['bucket_name'] == bucket_name1

    # TESTCASE 'reshard process ,'reshard','process','reshard non empty bucket','succeeds'
    log.debug(' test: reshard process non empty bucket')
    (err, out) = rgwadmin(ctx, client, ['reshard', 'process', '--bucket', bucket_name1], check_status=True)
    # check bucket shards num
    bucket_stats1 = get_bucket_stats(ctx, client, bucket_name1)
    assert  bucket_stats1.num_shards == num_shards
    new_user_stats = get_user_stats(ctx, client, user)
    assert old_user_stats == new_user_stats

    # TESTCASE 'dynamic reshard-process','reshard','','process bucket resharding','succeeds'
    log.debug(' test: dynamic reshard bucket')
    # create objs
    bucket_stats2 = get_bucket_stats(ctx, client, bucket_name2)
    old_num_shards = bucket_stats2.num_shards
    num_objs = 11
    for i in range(0, num_objs):
        key = boto.s3.key.Key(bucket2)
        key.key = 'obj' + `i`
        key.set_contents_from_string(key.key)
    old_user_stats = get_user_stats(ctx, client, user)

    # check bucket shards num
    (err, out) = rgwadmin(ctx, client, ['reshard', 'list'], check_status=True)
    assert len(out) == 1
    log.debug('bucket name %s', out[0]['bucket_name'])
    assert out[0]['bucket_name'] == bucket_name2

    time.sleep(30)
    bucket_stats2 = get_bucket_stats(ctx, client, bucket_name2)
    assert  bucket_stats2.num_shards > old_num_shards
    new_user_stats = get_user_stats(ctx, client, user)
    assert old_user_stats == new_user_stats

    # TESTCASE 'manual resharding','bucket', 'reshard','','manual bucket resharding','succeeds'
    log.debug(' test: manual reshard bucket')
    old_user_stats = get_user_stats(ctx, client, user)
    num_shards = bucket_stats1.num_shards + 1
    (err, out) = rgwadmin(ctx, client, ['bucket', 'reshard', '--bucket', bucket_name2,  '--num-shards', str(num_shards)], check_status=True)
    # check bucket shards num
    bucket_stats2 = get_bucket_stats(ctx, client, bucket_name2)
    assert  bucket_stats2.num_shards == num_shards
    new_user_stats = get_user_stats(ctx, client, user)
    assert old_user_stats == new_user_stats

    # TESTCASE 'versioning reshard-','bucket', reshard','versioning reshard','succeeds'
    log.debug(' test: reshard versioned bucket')
    old_user_stats = get_user_stats(ctx, client, user)
    num_shards = ver_bucket_stats.num_shards + 1
    (err, out) = rgwadmin(ctx, client, ['bucket', 'reshard', '--bucket', ver_bucket_name,  '--num-shards', str(num_shards)], check_status=True)
    # check bucket shards num
    ver_bucket_stats = get_bucket_stats(ctx, client, ver_bucket_name)
    assert ver_bucket_stats.num_shards == num_shards
    new_user_stats = get_user_stats(ctx, client, user)
    assert old_user_stats == new_user_stats

    # TESTCASE 'check acl'
    new_bucket1_acl = bucket1.get_xml_acl()
    assert new_bucket1_acl == bucket1_acl
    new_bucket2_acl = bucket2.get_xml_acl()
    assert new_bucket2_acl == bucket2_acl
    new_ver_bucket_acl = ver_bucket.get_xml_acl()
    assert new_ver_bucket_acl == ver_bucket_acl

    # Clean up
    log.debug("Deleting bucket %s", bucket_name1)
    for key in bucket1:
        key.delete()
    bucket1.delete()
    log.debug("Deleting bucket %s", bucket_name2)
    for key in bucket2:
        key.delete()
    bucket2.delete()
    log.debug("Deleting bucket %s", ver_bucket_name)
    for key in ver_bucket:
        key.delete()
    ver_bucket.delete()

def main():
    if len(sys.argv) == 3:
	user = sys.argv[1] + "@"
	# host = sys.argv[2]task
    elif len(sys.argv) == 2:
        user = ""
	host = sys.argv[1]
    else:
        sys.stderr.write("usage: rgw_reshard.py [user] host\n")
	exit(1)
    client0 = remote.Remote(user + host)
    ctx = config
    ctx.cluster=cluster.Cluster(remotes=[(client0,
     [ 'ceph.client.rgw.%s' % (host),  ]),])

    ctx.rgw = argparse.Namespace()
    endpoints = {}
    endpoints['ceph.client.rgw.%s' % host] = (host, 80)
    ctx.rgw.role_endpoints = endpoints
    ctx.rgw.realm = None
    ctx.rgw.regions = {'region0': { 'api name': 'api1',
	    'is master': True, 'master zone': 'r0z0',
	    'zones': ['r0z0', 'r0z1'] }}
    ctx.rgw.config = {'ceph.client.rgw.%s' % host: {'system user': {'name': '%s-system-user' % host}}}
    task(config, None)
    exit()

if __name__ == '__main__':
    main()
