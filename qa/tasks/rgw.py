"""
rgw routines
"""
import argparse
import contextlib
import json
import logging
import os
import errno
import util.rgw as rgw_utils

from teuthology.orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra.run import CommandFailedError
from util.rgw import rgwadmin, wait_for_radosgw
from util.rados import (rados, create_ec_pool,
                                        create_replicated_pool,
                                        create_cache_pool)

log = logging.getLogger(__name__)

@contextlib.contextmanager
def start_rgw(ctx, config, clients):
    """
    Start rgw on remote sites.
    """
    log.info('Starting rgw...')
    testdir = teuthology.get_testdir(ctx)
    for client in clients:
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        client_with_id = daemon_type + '.' + client_id
        client_with_cluster = cluster_name + '.' + client_with_id

        client_config = config.get(client)
        if client_config is None:
            client_config = {}
        log.info("rgw %s config is %s", client, client_config)
        cmd_prefix = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'daemon-helper',
            'term',
            ]

        rgw_cmd = ['radosgw']

        log.info("Using %s as radosgw frontend", ctx.rgw.frontend)

        host, port = ctx.rgw.role_endpoints[client]
        rgw_cmd.extend([
            '--rgw-frontends',
            '{frontend} port={port}'.format(frontend=ctx.rgw.frontend, port=port),
            '-n', client_with_id,
            '--cluster', cluster_name,
            '-k', '/etc/ceph/{client_with_cluster}.keyring'.format(client_with_cluster=client_with_cluster),
            '--log-file',
            '/var/log/ceph/rgw.{client_with_cluster}.log'.format(client_with_cluster=client_with_cluster),
            '--rgw_ops_log_socket_path',
            '{tdir}/rgw.opslog.{client_with_cluster}.sock'.format(tdir=testdir,
                                                     client_with_cluster=client_with_cluster),
            '--foreground',
            run.Raw('|'),
            'sudo',
            'tee',
            '/var/log/ceph/rgw.{client_with_cluster}.stdout'.format(tdir=testdir,
                                                       client_with_cluster=client_with_cluster),
            run.Raw('2>&1'),
            ])

        if client_config.get('valgrind'):
            cmd_prefix = teuthology.get_valgrind_args(
                testdir,
                client_with_cluster,
                cmd_prefix,
                client_config.get('valgrind')
                )

        run_cmd = list(cmd_prefix)
        run_cmd.extend(rgw_cmd)

        ctx.daemons.add_daemon(
            remote, 'rgw', client_with_id,
            cluster=cluster_name,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )

    # XXX: add_daemon() doesn't let us wait until radosgw finishes startup
    for client in config.keys():
        host, port = ctx.rgw.role_endpoints[client]
        endpoint = 'http://{host}:{port}/'.format(host=host, port=port)
        log.info('Polling {client} until it starts accepting connections on {endpoint}'.format(client=client, endpoint=endpoint))
        wait_for_radosgw(endpoint)

    try:
        yield
    finally:
        for client in config.iterkeys():
            cluster_name, daemon_type, client_id = teuthology.split_role(client)
            client_with_id = daemon_type + '.' + client_id
            client_with_cluster = cluster_name + '.' + client_with_id
            ctx.daemons.get_daemon('rgw', client_with_id, cluster_name).stop()
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/rgw.opslog.{client}.sock'.format(tdir=testdir,
                                                             client=client_with_cluster),
                    ],
                )

def assign_ports(ctx, config):
    """
    Assign port numberst starting with port 7280.
    """
    port = 7280
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for role in roles_for_host:
            if role in config:
                role_endpoints[role] = (remote.name.split('@')[1], port)
                port += 1

    return role_endpoints

@contextlib.contextmanager
def create_pools(ctx, clients):
    """Create replicated or erasure coded data pools for rgw."""

    log.info('Creating data pools')
    for client in clients:
        log.debug("Obtaining remote for client {}".format(client))
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()
        data_pool = '.rgw.buckets'
        cluster_name, daemon_type, client_id = teuthology.split_role(client)

        if ctx.rgw.ec_data_pool:
            create_ec_pool(remote, data_pool, client, 64,
                           ctx.rgw.erasure_code_profile, cluster_name)
        else:
            create_replicated_pool(remote, data_pool, 64, cluster_name)
        if ctx.rgw.cache_pools:
            create_cache_pool(remote, data_pool, data_pool + '.cache', 64,
                              64*1024*1024, cluster_name)
    log.debug('Pools created')
    yield

@contextlib.contextmanager
def configure_compression(ctx, clients, compression):
    """ set a compression type in the default zone placement """
    log.info('Configuring compression type = %s', compression)
    for client in clients:
        # XXX: the 'default' zone and zonegroup aren't created until we run RGWRados::init_complete().
        # issue a 'radosgw-admin user list' command to trigger this
        rgwadmin(ctx, client, cmd=['user', 'list'], check_status=True)

        rgwadmin(ctx, client,
                cmd=['zone', 'placement', 'modify', '--rgw-zone', 'default',
                     '--placement-id', 'default-placement',
                     '--compression', compression],
                check_status=True)
    yield

@contextlib.contextmanager
def task(ctx, config):
    """
    For example, to run rgw on all clients::

        tasks:
        - ceph:
        - rgw:

    To only run on certain clients::

        tasks:
        - ceph:
        - rgw: [client.0, client.3]

    or

        tasks:
        - ceph:
        - rgw:
            client.0:
            client.3:

    To run radosgw through valgrind:

        tasks:
        - ceph:
        - rgw:
            client.0:
              valgrind: [--tool=memcheck]
            client.3:
              valgrind: [--tool=memcheck]
    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                      for id_ in teuthology.all_roles_of_type(
                          ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    clients = config.keys() # http://tracker.ceph.com/issues/20417

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('rgw', {}))

    role_endpoints = assign_ports(ctx, config)
    ctx.rgw = argparse.Namespace()
    ctx.rgw.role_endpoints = role_endpoints

    ctx.rgw.ec_data_pool = bool(config.pop('ec-data-pool', False))
    ctx.rgw.erasure_code_profile = config.pop('erasure_code_profile', {})
    ctx.rgw.cache_pools = bool(config.pop('cache-pools', False))
    ctx.rgw.frontend = config.pop('frontend', 'civetweb')
    ctx.rgw.compression_type = config.pop('compression type', None)
    ctx.rgw.config = config

    log.debug("config is {}".format(config))
    log.debug("client list is {}".format(clients))
    subtasks = [
        lambda: create_pools(ctx=ctx, clients=clients),
    ]
    if ctx.rgw.compression_type:
        subtasks.extend([
            lambda: configure_compression(ctx=ctx, clients=clients,
                                          compression=ctx.rgw.compression_type),
        ])
    subtasks.extend([
        lambda: start_rgw(ctx=ctx, config=config, clients=clients),
    ])

    with contextutil.nested(*subtasks):
        yield
