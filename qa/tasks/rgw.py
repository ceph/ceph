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
def create_apache_dirs(ctx, config, on_client = None, except_client = None):
    """
    Remotely create apache directories.  Delete when finished.
    """
    log.info('Creating apache directories...')
    log.debug('client is %r', on_client)
    testdir = teuthology.get_testdir(ctx)
    clients_to_create_as = [on_client]
    if on_client is None:
        clients_to_create_as = config.keys()
    for client in clients_to_create_as:
        if client == except_client:
            continue
    	cluster_name, daemon_type, client_id = teuthology.split_role(client)
    	client_with_cluster = cluster_name + '.' + daemon_type + '.' + client_id
        ctx.cluster.only(client).run(
            args=[
                'mkdir',
                '-p',
                '{tdir}/apache/htdocs.{client_with_cluster}'.format(tdir=testdir,
                                                       client_with_cluster=client_with_cluster),
                '{tdir}/apache/tmp.{client_with_cluster}/fastcgi_sock'.format(
                    tdir=testdir,
                    client_with_cluster=client_with_cluster),
                run.Raw('&&'),
                'mkdir',
                '{tdir}/archive/apache.{client_with_cluster}'.format(tdir=testdir,
                                                        client_with_cluster=client_with_cluster),
                ],
            )
    try:
        yield
    finally:
        log.info('Cleaning up apache directories...')
        for client in clients_to_create_as:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/apache/tmp.{client_with_cluster}'.format(tdir=testdir,
                                                        client_with_cluster=client_with_cluster),
                    run.Raw('&&'),
                    'rmdir',
                    '{tdir}/apache/htdocs.{client_with_cluster}'.format(tdir=testdir,
                                                           client_with_cluster=client_with_cluster),
                    ],
                )
        for client in clients_to_create_as:
            ctx.cluster.only(client).run(
                args=[
                    'rmdir',
                    '{tdir}/apache'.format(tdir=testdir),
                    ],
                check_status=False,  # only need to remove once per host
                )


def _use_uds_with_fcgi(remote):
    """
    Returns true if this node supports the usage of
    unix domain sockets with mod_proxy_fcgi.

    FIXME: returns False always for now until we know for
    sure what distros will support UDS. RHEL 7.0 is the only one
    currently I know of, but we can't install that version of apache
    yet in the labs.
    """
    return False


@contextlib.contextmanager
def ship_apache_configs(ctx, config, role_endpoints, on_client = None,
                        except_client = None):
    """
    Ship apache config and rgw.fgci to all clients.  Clean up on termination
    """
    assert isinstance(config, dict)
    assert isinstance(role_endpoints, dict)
    testdir = teuthology.get_testdir(ctx)
    log.info('Shipping apache config and rgw.fcgi...')
    src = os.path.join(os.path.dirname(__file__), 'apache.conf.template')
    clients_to_create_as = [on_client]
    if on_client is None:
        clients_to_create_as = config.keys()
    for client in clients_to_create_as:
        if client == except_client:
            continue
    	cluster_name, daemon_type, client_id = teuthology.split_role(client)
    	client_with_id = daemon_type + '.' + client_id
    	client_with_cluster = cluster_name + '.' + client_with_id
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
        conf = config.get(client)
        if not conf:
            conf = {}
        idle_timeout = conf.get('idle_timeout', ctx.rgw.default_idle_timeout)
        if system_type == 'deb':
            mod_path = '/usr/lib/apache2/modules'
            print_continue = 'on'
            user = 'www-data'
            group = 'www-data'
            apache24_modconfig = '''
  IncludeOptional /etc/apache2/mods-available/mpm_event.conf
  IncludeOptional /etc/apache2/mods-available/mpm_event.load
'''
        else:
            mod_path = '/usr/lib64/httpd/modules'
            print_continue = 'off'
            user = 'apache'
            group = 'apache'
            apache24_modconfig = \
                'IncludeOptional /etc/httpd/conf.modules.d/00-mpm.conf'
        host, port = role_endpoints[client]

        # decide if we want to use mod_fastcgi or mod_proxy_fcgi
        template_dir = os.path.dirname(__file__)
        fcgi_config = os.path.join(template_dir,
                                   'mod_proxy_fcgi.tcp.conf.template')
        if ctx.rgw.use_fastcgi:
            log.info("Apache is configured to use mod_fastcgi")
            fcgi_config = os.path.join(template_dir,
                                       'mod_fastcgi.conf.template')
        elif _use_uds_with_fcgi(remote):
            log.info("Apache is configured to use mod_proxy_fcgi with UDS")
            fcgi_config = os.path.join(template_dir,
                                       'mod_proxy_fcgi.uds.conf.template')
        else:
            log.info("Apache is configured to use mod_proxy_fcgi with TCP")

        with file(fcgi_config, 'rb') as f:
            fcgi_config = f.read()
        with file(src, 'rb') as f:
            conf = f.read() + fcgi_config
            conf = conf.format(
                testdir=testdir,
                mod_path=mod_path,
                print_continue=print_continue,
                host=host,
                port=port,
                client=client_with_cluster,
                idle_timeout=idle_timeout,
                user=user,
                group=group,
                apache24_modconfig=apache24_modconfig,
                )
            teuthology.write_file(
                remote=remote,
                path='{tdir}/apache/apache.{client_with_cluster}.conf'.format(
                    tdir=testdir,
                    client_with_cluster=client_with_cluster),
                data=conf,
                )
        rgw_options = []
        if ctx.rgw.use_fastcgi or _use_uds_with_fcgi(remote):
            rgw_options = [
                '--rgw-socket-path',
                '{tdir}/apache/tmp.{client_with_cluster}/fastcgi_sock/rgw_sock'.format(
                    tdir=testdir,
                    client_with_cluster=client_with_cluster
                ),
                '--rgw-frontends',
                'fastcgi',
            ]
        else:
            rgw_options = [
                '--rgw-socket-path', '""',
                '--rgw-print-continue', 'false',
                '--rgw-frontends',
                'fastcgi socket_port=9000 socket_host=0.0.0.0',
            ]

        teuthology.write_file(
            remote=remote,
            path='{tdir}/apache/htdocs.{client_with_cluster}/rgw.fcgi'.format(
                tdir=testdir,
                client_with_cluster=client_with_cluster),
            data="""#!/bin/sh
ulimit -c unlimited
exec radosgw -f -n {client_with_id} --cluster {cluster_name} -k /etc/ceph/{client_with_cluster}.keyring {rgw_options}

""".format(tdir=testdir, client_with_id=client_with_id, client_with_cluster=client_with_cluster, cluster_name=cluster_name, rgw_options=" ".join(rgw_options))
            )
        remote.run(
            args=[
                'chmod',
                'a=rx',
                '{tdir}/apache/htdocs.{client_with_cluster}/rgw.fcgi'.format(tdir=testdir,
                                                                client_with_cluster=client_with_cluster),
                ],
            )
    try:
        yield
    finally:
        log.info('Removing apache config...')
        for client in clients_to_create_as:
            cluster_name, daemon_type, client_id = teuthology.split_role(client)
            client_with_cluster = '.'.join((cluster_name, daemon_type, client_id))
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/apache/apache.{client_with_cluster}.conf'.format(tdir=testdir,
                                                                client_with_cluster=client_with_cluster),
                    run.Raw('&&'),
                    'rm',
                    '-f',
                    '{tdir}/apache/htdocs.{client_with_cluster}/rgw.fcgi'.format(
                        tdir=testdir,
                        client_with_cluster=client_with_cluster),
                    ],
                )


@contextlib.contextmanager
def start_rgw(ctx, config, on_client = None, except_client = None):
    """
    Start rgw on remote sites.
    """
    log.info('Starting rgw...')
    log.debug('client %r', on_client)
    clients_to_run = [on_client]
    if on_client is None:
        clients_to_run = config.keys()
        log.debug('client %r', clients_to_run)
    testdir = teuthology.get_testdir(ctx)
    for client in clients_to_run:
        if client == except_client:
            continue
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
        if ctx.rgw.frontend == 'apache':
            if ctx.rgw.use_fastcgi or _use_uds_with_fcgi(remote):
                rgw_cmd.extend([
                    '--rgw-socket-path',
                    '{tdir}/apache/tmp.{client_with_cluster}/fastcgi_sock/rgw_sock'.format(
                        tdir=testdir,
                        client_with_cluster=client_with_cluster,
                    ),
                    '--rgw-frontends',
                    'fastcgi',
                ])
            else:
                log.info("Using mod_proxy_fcgi instead of mod_fastcgi...")
                # for mod_proxy_fcgi, using tcp
                rgw_cmd.extend([
                    '--rgw-socket-path', '',
                    '--rgw-print-continue', 'false',
                    '--rgw-frontends',
                    'fastcgi socket_port=9000 socket_host=0.0.0.0',
                ])

        else:
            host, port = ctx.rgw.role_endpoints[client]
            rgw_cmd.extend([
                '--rgw-frontends',
                '{frontend} port={port}'.format(frontend=ctx.rgw.frontend, port=port),
            ])

        rgw_cmd.extend([
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
    for client in clients_to_run:
        if client == except_client:
            continue
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


@contextlib.contextmanager
def start_apache(ctx, config, on_client = None, except_client = None):
    """
    Start apache on remote sites.
    """
    log.info('Starting apache...')
    testdir = teuthology.get_testdir(ctx)
    apaches = {}
    clients_to_run = [on_client]
    if on_client is None:
        clients_to_run = config.keys()
    for client in clients_to_run:
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        client_with_cluster = cluster_name + '.' + daemon_type + '.' + client_id
        if client == except_client:
            continue
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
        if system_type == 'deb':
            apache_name = 'apache2'
        else:
            try:
                remote.run(
                    args=[
                        'stat',
                        '/usr/sbin/httpd.worker',
                    ],
                )
                apache_name = '/usr/sbin/httpd.worker'
            except CommandFailedError:
                apache_name = '/usr/sbin/httpd'

        proc = remote.run(
            args=[
                'adjust-ulimits',
                'daemon-helper',
                'kill',
                apache_name,
                '-X',
                '-f',
                '{tdir}/apache/apache.{client_with_cluster}.conf'.format(tdir=testdir,
                                                            client_with_cluster=client_with_cluster),
                ],
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )
        apaches[client_with_cluster] = proc

    try:
        yield
    finally:
        log.info('Stopping apache...')
        for client, proc in apaches.iteritems():
            proc.stdin.close()

        run.wait(apaches.itervalues())

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
def create_pools(ctx, config):
    """Create replicated or erasure coded data pools for rgw."""

    log.info('Creating data pools')
    for client in config.keys():
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
def configure_compression(ctx, config):
    """ set a compression type in the default zone placement """
    compression = ctx.rgw.compression_type
    if not compression:
        return

    log.info('Configuring compression type = %s', compression)
    for client, c_config in config.iteritems():
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
    Either use configure apache to run a rados gateway, or use the built-in
    civetweb server.
    Only one should be run per machine, since it uses a hard-coded port for
    now.

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

    You can adjust the idle timeout for fastcgi (default is 30 seconds):

        tasks:
        - ceph:
        - rgw:
            client.0:
              idle_timeout: 90

    To run radosgw through valgrind:

        tasks:
        - ceph:
        - rgw:
            client.0:
              valgrind: [--tool=memcheck]
            client.3:
              valgrind: [--tool=memcheck]

    To use civetweb instead of apache:

        tasks:
        - ceph:
        - rgw:
          - client.0
        overrides:
          rgw:
            frontend: civetweb

    Note that without a modified fastcgi module e.g. with the default
    one on CentOS, you must have rgw print continue = false in ceph.conf::

        tasks:
        - ceph:
            conf:
              global:
                rgw print continue: false
        - rgw: [client.0]

    To use mod_proxy_fcgi instead of mod_fastcgi:

        overrides:
          rgw:
            use_fcgi: true

    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                      for id_ in teuthology.all_roles_of_type(
                          ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('rgw', {}))

    role_endpoints = assign_ports(ctx, config)
    ctx.rgw = argparse.Namespace()
    ctx.rgw.role_endpoints = role_endpoints

    ctx.rgw.ec_data_pool = bool(config.pop('ec-data-pool', False))
    ctx.rgw.erasure_code_profile = config.pop('erasure_code_profile', {})
    ctx.rgw.default_idle_timeout = int(config.pop('default_idle_timeout', 30))
    ctx.rgw.cache_pools = bool(config.pop('cache-pools', False))
    ctx.rgw.frontend = config.pop('frontend', 'civetweb')
    ctx.rgw.use_fastcgi = not config.pop('use_fcgi', True)
    ctx.rgw.compression_type = config.pop('compression type', None)
    ctx.rgw.config = config

    subtasks = []
    if ctx.rgw.frontend == 'apache':
        subtasks.extend([
            lambda: create_apache_dirs(ctx=ctx, config=config),
            lambda: ship_apache_configs(ctx=ctx, config=config,
                                        role_endpoints=role_endpoints),
            lambda: start_apache(ctx=ctx, config=config),
            ])

    subtasks.extend([
        lambda: create_pools(ctx=ctx, config=config),
        lambda: configure_compression(ctx=ctx, config=config),
        lambda: start_rgw(ctx=ctx, config=config),
    ])

    with contextutil.nested(*subtasks):
        yield
