"""
Rgw manual and dynamic resharding  testing against a running instance
"""
# The test cases in this file have been annotated for inventory.
# To extract the inventory (in csv format) use the command:
#
#   grep '^ *# TESTCASE' | sed 's/^ *# TESTCASE //'
#
#

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

def get_acl(key):
    """
    Helper function to get the xml acl from a key, ensuring that the xml
    version tag is removed from the acl response
    """
    raw_acl = key.get_xml_acl()

    def remove_version(string):
        return string.split(
            '<?xml version="1.0" encoding="UTF-8"?>'
        )[-1]

    def remove_newlines(string):
        return string.strip('\n')

    return remove_version(
        remove_newlines(raw_acl)
    )

def task(ctx, config):
    """
    Test resharding functionality against a running rgw instance.
    """
    global log

    assert ctx.rgw.config, \
        "rgw_reshard task needs a config passed from the rgw task"
    config = ctx.rgw.config
    log.debug('rgw_reshard config is: %r', config)

    clients_from_config = config.keys()

    # choose first client as default
    client = clients_from_config[0]

    # once the client is chosen, pull the host name and  assigned port out of
    # the role_endpoints that were assigned by the rgw task
    (remote_host, remote_port) = ctx.rgw.role_endpoints[client]
    log.debug('remote host %r remote port %r', remote_host, remote_port)

    ##
    user = 'testid'
    access_key='0555b35654ad1656d804'
    secret_key='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
    bucket_name='myfoo'
    bucket_name2='mybar'
    bucket_name3="myver"

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
    bucket = connection.create_bucket(bucket_name)
    bucket2 = connection.create_bucket(bucket_name2)
    bucket3 = connection.create_bucket(bucket_name3)
    bucket3.configure_versioning(True)

    # TESTCASE 'reshard-list','reshard','list','no resharding','succeeds, empty list'
    (err, out) = rgwadmin(ctx, client, ['reshard', 'list'], check_status=True)

    # TESTCASE 'reshard-add','reshard','add','add bucket to resharding queue','succeeds'
    num_shards = '8'
    (err, out) = rgwadmin(ctx, client, ['reshard', 'add', '--bucket', bucket_name, '--num-shards', num_shards], check_status=True)
    (err, out) = rgwadmin(ctx, client, ['reshard', 'list'], check_status=True)
    assert len(out) == 1
    assert out[0]['bucket_name'] == bucket_name

    # TESTCASE 'reshard-process','reshard','','process bucket resharding','succeeds'
    num_shards = '16'
    (err, out) = rgwadmin(ctx, client, ['reshard', 'process', '--bucket', bucket_name], check_status=True)
    # check bucket shards num
    #assert(out['shards']) == num_shards

    # create objs
    num_objs = 8
    for i in range(0, num_objs):
        key = boto.s3.key.Key(bucket)
        key.key = 'obj' + `i`
        key.set_contents_from_string(key.key)

    (err, out) = rgwadmin(ctx, client, ['reshard', 'process', '--bucket', bucket_name], check_status=True)
    # check bucket shards num
    #assert(out['shards']) == num_shards

    # TESTCASE 'dynamic reshard-process','reshard','','process bucket resharding','succeeds'
    # create objs
    num_objs = 10
    for i in range(0, num_objs):
        key = boto.s3.key.Key(bucket2)
        key.key = 'obj' + `i`
        key.set_contents_from_string(key.key)
    # check bucket shards num
    #assert(out['shards']) == num_shards

    # TESTCASE 'manual resharding','bucket', 'reshard','','manual bucket resharding','succeeds'
    num_shards = '32'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'reshard', '--bucket', bucket_name,  '--num-shards', num_shards], check_status=True)
    # check bucket shards num
    #assert(out['shards']) == num_shards

    # TESTCASE 'versioning reshard-','bucket', reshard','versioning reshard','succeeds'
    num_shards = '8'
    (err, out) = rgwadmin(ctx, client, ['bucket', 'reshard', '--bucket', bucket_name3,  '--num-shards', num_shards], check_status=True)
    # check bucket shards num
    #assert(out['shards']) == num_shards

import sys
from teuthology.config import config
from teuthology.orchestra import cluster, remote
import argparse;

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
