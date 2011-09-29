from cStringIO import StringIO
from configobj import ConfigObj
import base64
import contextlib
import logging
import os
import json

import boto.exception
import boto.s3.connection
import boto.s3.acl

from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run
from ..orchestra.connection import split_user

log = logging.getLogger(__name__)

def rgwadmin(ctx, client, cmd):
    pre = [
        'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
        '/tmp/cephtest/enable-coredump',
        '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
        '/tmp/cephtest/archive/coverage',
        '/tmp/cephtest/binary/usr/local/bin/radosgw-admin',
        '-c', '/tmp/cephtest/ceph.conf',
        '--log-to-stderr', '0',
        '--format', 'json',
        ]
    pre.extend(cmd)
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    proc = remote.run(
        args=pre,
        check_status=False,
        stdout=StringIO(),
        stderr=StringIO(),
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    j = None
    if not r and out != '':
        j = json.loads(out)
        log.info(j)
    return (r, j)

def task(ctx, config):
    """
    Test radosgw-admin functionality against a running rgw instance.
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task s3tests only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    # just use the first client...
    client = clients[0];

    ##
    user='foo'
    display_name='Foo'
    email='foo@foo.com'
    auid='1234'
    access_key='9te6NH5mcdcq0Tc5i8i1'
    secret_key='Ny4IOauQoL18Gp2zM7lC1vLmoawgqcYP/YGcWfXu'
    
    bucket_name='myfoo'

    # create
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user])
    assert err
    (err, out) = rgwadmin(ctx, client, [
            'user', 'create',
            '--uid', user,
            '--display-name', display_name,
            '--email', email,
            '--access-key', access_key,
            '--secret', secret_key
            ])
    assert not err
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user])
    assert not err
    assert out['user_id'] == user
    assert out['email'] == email
    assert out['display_name'] == display_name
    assert out['keys'][0]['access_key'] == access_key
    assert out['keys'][0]['secret_key'] == secret_key
    assert not out['suspended']

    # suspend
    (err, out) = rgwadmin(ctx, client, ['user', 'suspend', '--uid', user])
    assert not err
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user])
    assert not err
    assert out['suspended']

    # enable
    (err, out) = rgwadmin(ctx, client, ['user', 'enable', '--uid', user])
    assert not err
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user])
    assert not err
    assert not out['suspended']

    # add key

    # remove key
    
    # connect to rgw
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    (remote_user, remote_host) = remote.name.split('@')
    connection = boto.s3.connection.S3Connection(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        is_secure=False,
        port=7280,
        host=remote_host,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    # create bucket
    (err, out) = rgwadmin(ctx, client, ['bucket', 'list', '--uid', user])
    assert not err
    assert len(out) == 0

    bucket = connection.create_bucket(bucket_name)

    (err, out) = rgwadmin(ctx, client, ['bucket', 'list', '--uid', user])
    assert not err
    assert len(out) == 1
    assert out[0] == bucket_name

    # it should be empty
    (err, out) = rgwadmin(ctx, client, [
            'bucket', 'stats', '--bucket', bucket_name])
    assert not err
    bucket_id = out['id']

    # use some space
    key = boto.s3.key.Key(bucket)
    key.set_contents_from_string('one')
    (err, out) = rgwadmin(ctx, client, [
            'bucket', 'stats', '--bucket-id', '%d' % bucket_id])
    assert not err
    assert out['id'] == '%d' % bucket_id
    
    # remove user
    (err, out) = rgwadmin(ctx, client, ['user', 'rm', '--uid', user])
    assert not err
    (err, out) = rgwadmin(ctx, client, ['user', 'info', '--uid', user])
    assert err
