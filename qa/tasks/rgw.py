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
from teuthology.exceptions import ConfigError
from util import get_remote_for_role
from util.rgw import rgwadmin, wait_for_radosgw
from util.rados import (rados, create_ec_pool,
                                        create_replicated_pool,
                                        create_cache_pool)

log = logging.getLogger(__name__)

class RGWEndpoint:
    def __init__(self, hostname=None, port=None, cert=None):
        self.hostname = hostname
        self.port = port
        self.cert = cert

    def url(self):
        proto = 'https' if self.cert else 'http'
        return '{proto}://{hostname}:{port}/'.format(proto=proto, hostname=self.hostname, port=self.port)

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

        endpoint = ctx.rgw.role_endpoints[client]
        frontends = ctx.rgw.frontend
        frontend_prefix = client_config.get('frontend_prefix', None)
        if frontend_prefix:
            frontends += ' prefix={pfx}'.format(pfx=frontend_prefix)

        if endpoint.cert:
            # add the ssl certificate path
            frontends += ' ssl_certificate={}'.format(endpoint.cert.certificate)
            if ctx.rgw.frontend == 'civetweb':
                frontends += ' port={}s'.format(endpoint.port)
            else:
                frontends += ' ssl_port={}'.format(endpoint.port)
        else:
            frontends += ' port={}'.format(endpoint.port)

        rgw_cmd.extend([
            '--rgw-frontends', frontends,
            '-n', client_with_id,
            '--cluster', cluster_name,
            '-k', '/etc/ceph/{client_with_cluster}.keyring'.format(client_with_cluster=client_with_cluster),
            '--log-file',
            '/var/log/ceph/rgw.{client_with_cluster}.log'.format(client_with_cluster=client_with_cluster),
            '--rgw_ops_log_socket_path',
            '{tdir}/rgw.opslog.{client_with_cluster}.sock'.format(tdir=testdir,
                                                     client_with_cluster=client_with_cluster)
	    ])

        keystone_role = client_config.get('use-keystone-role', None)
        if keystone_role is not None:
            if not ctx.keystone:
                raise ConfigError('rgw must run after the keystone task')
            url = 'http://{host}:{port}/v1/KEY_$(tenant_id)s'.format(host=endpoint.hostname,
                                                                     port=endpoint.port)
            ctx.keystone.create_endpoint(ctx, keystone_role, 'swift', url)

            keystone_host, keystone_port = \
                ctx.keystone.public_endpoints[keystone_role]
            rgw_cmd.extend([
                '--rgw_keystone_url',
                'http://{khost}:{kport}'.format(khost=keystone_host,
                                                kport=keystone_port),
                ])

        rgw_cmd.extend([
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
    for client in clients:
        endpoint = ctx.rgw.role_endpoints[client]
        url = endpoint.url()
        log.info('Polling {client} until it starts accepting connections on {url}'.format(client=client, url=url))
        wait_for_radosgw(url)

    try:
        yield
    finally:
        for client in clients:
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

def assign_endpoints(ctx, config, default_cert):
    """
    Assign port numbers starting with port 7280.
    """
    port = 7280
    role_endpoints = {}

    for role, client_config in config.iteritems():
        client_config = client_config or {}
        remote = get_remote_for_role(ctx, role)

        cert = client_config.get('ssl certificate', default_cert)
        if cert:
            # find the certificate created by the ssl task
            if not hasattr(ctx, 'ssl_certificates'):
                raise ConfigError('rgw: no ssl task found for option "ssl certificate"')
            ssl_certificate = ctx.ssl_certificates.get(cert, None)
            if not ssl_certificate:
                raise ConfigError('rgw: missing ssl certificate "{}"'.format(cert))
        else:
            ssl_certificate = None

        role_endpoints[role] = RGWEndpoint(remote.hostname, port, ssl_certificate)
        port += 1

    return role_endpoints

@contextlib.contextmanager
def create_pools(ctx, clients):
    """Create replicated or erasure coded data pools for rgw."""

    log.info('Creating data pools')
    for client in clients:
        log.debug("Obtaining remote for client {}".format(client))
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()
        data_pool = 'default.rgw.buckets.data'
        cluster_name, daemon_type, client_id = teuthology.split_role(client)

        if ctx.rgw.ec_data_pool:
            create_ec_pool(remote, data_pool, client, ctx.rgw.data_pool_pg_size,
                           ctx.rgw.erasure_code_profile, cluster_name, 'rgw')
        else:
            create_replicated_pool(remote, data_pool, ctx.rgw.data_pool_pg_size, cluster_name, 'rgw')

        index_pool = 'default.rgw.buckets.index'
        create_replicated_pool(remote, index_pool, ctx.rgw.index_pool_pg_size, cluster_name, 'rgw')

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
def configure_storage_classes(ctx, clients, storage_classes):
    """ set a compression type in the default zone placement """

    sc = [s.strip() for s in storage_classes.split(',')]

    for client in clients:
        # XXX: the 'default' zone and zonegroup aren't created until we run RGWRados::init_complete().
        # issue a 'radosgw-admin user list' command to trigger this
        rgwadmin(ctx, client, cmd=['user', 'list'], check_status=True)

        for storage_class in sc:
            log.info('Configuring storage class type = %s', storage_class)
            rgwadmin(ctx, client,
                    cmd=['zonegroup', 'placement', 'add',
                        '--rgw-zone', 'default',
                        '--placement-id', 'default-placement',
                        '--storage-class', storage_class],
                    check_status=True)
            rgwadmin(ctx, client,
                    cmd=['zone', 'placement', 'add',
                        '--rgw-zone', 'default',
                        '--placement-id', 'default-placement',
                        '--storage-class', storage_class,
                        '--data-pool', 'default.rgw.buckets.data.' + storage_class.lower()],
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

    To configure data or index pool pg_size:

        overrides:
          rgw:
            data_pool_pg_size: 256
            index_pool_pg_size: 128
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

    ctx.rgw = argparse.Namespace()

    ctx.rgw.ec_data_pool = bool(config.pop('ec-data-pool', False))
    ctx.rgw.erasure_code_profile = config.pop('erasure_code_profile', {})
    ctx.rgw.cache_pools = bool(config.pop('cache-pools', False))
    ctx.rgw.frontend = config.pop('frontend', 'civetweb')
    ctx.rgw.compression_type = config.pop('compression type', None)
    ctx.rgw.storage_classes = config.pop('storage classes', None)
    default_cert = config.pop('ssl certificate', None)
    ctx.rgw.data_pool_pg_size = config.pop('data_pool_pg_size', 64)
    ctx.rgw.index_pool_pg_size = config.pop('index_pool_pg_size', 64)
    ctx.rgw.config = config

    log.debug("config is {}".format(config))
    log.debug("client list is {}".format(clients))

    ctx.rgw.role_endpoints = assign_endpoints(ctx, config, default_cert)

    subtasks = [
        lambda: create_pools(ctx=ctx, clients=clients),
    ]
    if ctx.rgw.compression_type:
        subtasks.extend([
            lambda: configure_compression(ctx=ctx, clients=clients,
                                          compression=ctx.rgw.compression_type),
        ])
    if ctx.rgw.storage_classes:
        subtasks.extend([
            lambda: configure_storage_classes(ctx=ctx, clients=clients,
                                              storage_classes=ctx.rgw.storage_classes),
        ])
    subtasks.extend([
        lambda: start_rgw(ctx=ctx, config=config, clients=clients),
    ])

    with contextutil.nested(*subtasks):
        yield
