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

from requests.packages.urllib3 import PoolManager
from requests.packages.urllib3.util import Retry

from cStringIO import StringIO

from teuthology.orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra.run import CommandFailedError
from util.rgw import rgwadmin, get_config_master_client, extract_zone_info, extract_region_info
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
        zone = rgw_utils.zone_for_client(ctx, client)
        log.debug('zone %s', zone)

        client_config = config.get(client)
        if client_config is None:
            client_config = {}
        log.info("rgw %s config is %s", client, client_config)
        id_ = client.split('.', 1)[1]
        log.info('client {client} is id {id}'.format(client=client, id=id_))
        cmd_prefix = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'daemon-helper',
            'term',
            ]

        rgw_cmd = ['radosgw']

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
                # for mod_proxy_fcgi, using tcp
                rgw_cmd.extend([
                    '--rgw-socket-path', '',
                    '--rgw-print-continue', 'false',
                    '--rgw-frontends',
                    'fastcgi socket_port=9000 socket_host=0.0.0.0',
                ])

        elif ctx.rgw.frontend == 'civetweb':
            host, port = ctx.rgw.role_endpoints[client]
            rgw_cmd.extend([
                '--rgw-frontends',
                'civetweb port={port}'.format(port=port),
            ])

        if zone is not None:
            rgw_cmd.extend(['--rgw-zone', zone])

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
                client,
                cmd_prefix,
                client_config.get('valgrind')
                )

        run_cmd = list(cmd_prefix)
        run_cmd.extend(rgw_cmd)

        ctx.daemons.add_daemon(
            remote, 'rgw', client,
            cluster=cluster_name,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )

    # XXX: add_daemon() doesn't let us wait until radosgw finishes startup
    # use a connection pool with retry/backoff to poll each gateway until it starts listening
    http = PoolManager(retries=Retry(connect=8, backoff_factor=1))
    for client in clients_to_run:
        if client == except_client:
            continue
        host, port = ctx.rgw.role_endpoints[client]
        endpoint = 'http://{host}:{port}/'.format(host=host, port=port)
        log.info('Polling {client} until it starts accepting connections on {endpoint}'.format(client=client, endpoint=endpoint))
        http.request('GET', endpoint)

    try:
        yield
    finally:
        teuthology.stop_daemons_of_type(ctx, 'rgw')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/rgw.opslog.{client_with_cluster}.sock'.format(tdir=testdir,
                                                             client_with_cluster=client_with_cluster),
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

def extract_user_info(client_config):
    """
    Extract user info from the client config specified.  Returns a dict
    that includes system key information.
    """
    # test if there isn't a system user or if there isn't a name for that
    # user, return None
    if ('system user' not in client_config or
            'name' not in client_config['system user']):
        return None

    user_info = dict()
    user_info['system_key'] = dict(
        user=client_config['system user']['name'],
        access_key=client_config['system user']['access key'],
        secret_key=client_config['system user']['secret key'],
        )
    return user_info


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


def fill_in_endpoints(region_info, role_zones, role_endpoints):
    """
    Iterate through the list of role_endpoints, filling in zone information

    :param region_info: region data
    :param role_zones: region and zone information.
    :param role_endpoints: endpoints being used
    """
    for role, (host, port) in role_endpoints.iteritems():
        region, zone, zone_info, _ = role_zones[role]
        host, port = role_endpoints[role]
        endpoint = 'http://{host}:{port}/'.format(host=host, port=port)
        # check if the region specified under client actually exists
        # in region_info (it should, if properly configured).
        # If not, throw a reasonable error
        if region not in region_info:
            raise Exception(
                'Region: {region} was specified but no corresponding'
                ' entry was found under \'regions\''.format(region=region))

        region_conf = region_info[region]
        region_conf.setdefault('endpoints', [])
        region_conf['endpoints'].append(endpoint)

        # this is the payload for the 'zones' field in the region field
        zone_payload = dict()
        zone_payload['endpoints'] = [endpoint]
        zone_payload['name'] = zone

        # Pull the log meta and log data settings out of zone_info, if they
        # exist, then pop them as they don't actually belong in the zone info
        for key in ['rgw log meta', 'rgw log data']:
            new_key = key.split(' ', 1)[1]
            new_key = new_key.replace(' ', '_')

            if key in zone_info:
                value = zone_info.pop(key)
            else:
                value = 'false'

            zone_payload[new_key] = value

        region_conf.setdefault('zones', [])
        region_conf['zones'].append(zone_payload)


@contextlib.contextmanager
def configure_users_for_client(ctx, config, client, everywhere=False):
    """
    Create users by remotely running rgwadmin commands using extracted
    user information.
    """
    log.info('Configuring users...')
    log.info('for client %s', client)
    log.info('everywhere %s', everywhere)

    # For data sync the master zones and regions must have the
    # system users of the secondary zones. To keep this simple,
    # just create the system users on every client if regions are
    # configured.
    clients_to_create_as = [client]
    if everywhere:
        clients_to_create_as = config.keys()

    # extract the user info and append it to the payload tuple for the given
    # client
    for client, c_config in config.iteritems():
        if not c_config:
            continue
        user_info = extract_user_info(c_config)
        if not user_info:
            continue

        for client_name in clients_to_create_as:
            log.debug('Creating user {user} on {client}'.format(
                user=user_info['system_key']['user'], client=client_name))
            rgwadmin(ctx, client_name,
                     cmd=[
                         'user', 'create',
                         '--uid', user_info['system_key']['user'],
                         '--access-key', user_info['system_key']['access_key'],
                         '--secret', user_info['system_key']['secret_key'],
                         '--display-name', user_info['system_key']['user'],
                         '--system',
                     ],
                     check_status=True,
            )
    yield

@contextlib.contextmanager
def configure_users(ctx, config,  everywhere=False):
    """
    Create users by remotely running rgwadmin commands using extracted
    user information.
    """
    log.info('Configuring users...')

    # extract the user info and append it to the payload tuple for the given
    # client
    for client, c_config in config.iteritems():
        if not c_config:
            continue
        user_info = extract_user_info(c_config)
        if not user_info:
            continue

        # For data sync the master zones and regions must have the
        # system users of the secondary zones. To keep this simple,
        # just create the system users on every client if regions are
        # configured.
        clients_to_create_as = [client]
        if everywhere:
            clients_to_create_as = config.keys()
        for client_name in clients_to_create_as:
            log.debug('Creating user {user} on {client}'.format(
                      user=user_info['system_key']['user'], client=client))
            rgwadmin(ctx, client_name,
                     cmd=[
                         'user', 'create',
                         '--uid', user_info['system_key']['user'],
                         '--access-key', user_info['system_key']['access_key'],
                         '--secret', user_info['system_key']['secret_key'],
                         '--display-name', user_info['system_key']['user'],
                         '--system',
                     ],
                     check_status=True,
                     )

    yield

@contextlib.contextmanager
def create_nonregion_pools(ctx, config, regions):
    """Create replicated or erasure coded data pools for rgw."""
    if regions:
        yield
        return

    log.info('creating data pools')
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
    yield

@contextlib.contextmanager
def configure_multisite_regions_and_zones(ctx, config, regions, role_endpoints, realm, master_client):
    """
    Configure multisite regions and zones from rados and rgw.
    """
    if not regions:
        log.debug(
            'In rgw.configure_multisite_regions_and_zones() and regions is None. '
            'Bailing')
        yield
        return

    if not realm:
        log.debug(
            'In rgw.configure_multisite_regions_and_zones() and realm is None. '
            'Bailing')
        yield
        return

    log.info('Configuring multisite regions and zones...')

    log.debug('config is %r', config)
    log.debug('regions are %r', regions)
    log.debug('role_endpoints = %r', role_endpoints)
    log.debug('realm is %r', realm)

    # extract the zone info
    role_zones = dict([(client, extract_zone_info(ctx, client, c_config))
                       for client, c_config in config.iteritems()])
    log.debug('role_zones = %r', role_zones)

    # extract the user info and append it to the payload tuple for the given
    # client
    for client, c_config in config.iteritems():
        if not c_config:
            user_info = None
        else:
            user_info = extract_user_info(c_config)

        (region, zone, zone_info) = role_zones[client]
        role_zones[client] = (region, zone, zone_info, user_info)

    region_info = dict([
        (region_name, extract_region_info(region_name, r_config))
        for region_name, r_config in regions.iteritems()])

    fill_in_endpoints(region_info, role_zones, role_endpoints)

    # clear out the old defaults
    cluster_name, daemon_type, client_id = teuthology.split_role(master_client)
    first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    # read master zonegroup and master_zone
    for zonegroup, zg_info in region_info.iteritems():
        if zg_info['is_master']:
            master_zonegroup = zonegroup
            master_zone = zg_info['master_zone']
            break

    log.debug('master zonegroup =%r', master_zonegroup)
    log.debug('master zone = %r', master_zone)
    log.debug('master client = %r', master_client)

    rgwadmin(ctx, master_client,
             cmd=['realm', 'create', '--rgw-realm', realm, '--default'],
             check_status=True)

    for region, info in region_info.iteritems():
        region_json = json.dumps(info)
        log.debug('region info is: %s', region_json)
        rgwadmin(ctx, master_client,
                 cmd=['zonegroup', 'set'],
                 stdin=StringIO(region_json),
                 check_status=True)

    rgwadmin(ctx, master_client,
             cmd=['zonegroup', 'default', '--rgw-zonegroup', master_zonegroup],
             check_status=True)

    for role, (zonegroup, zone, zone_info, user_info) in role_zones.iteritems():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        for pool_info in zone_info['placement_pools']:
            remote.run(args=['sudo', 'ceph', 'osd', 'pool', 'create',
                             pool_info['val']['index_pool'], '64', '64', '--cluster', cluster_name])
            if ctx.rgw.ec_data_pool:
                create_ec_pool(remote, pool_info['val']['data_pool'],
                               zone, 64, ctx.rgw.erasure_code_profile, cluster_name)
            else:
                create_replicated_pool(remote, pool_info['val']['data_pool'], 64, cluster_name)

    (zonegroup, zone, zone_info, user_info) = role_zones[master_client]
    zone_json = json.dumps(dict(zone_info.items() + user_info.items()))
    log.debug("zone info is: %r", zone_json)
    rgwadmin(ctx, master_client,
             cmd=['zone', 'set', '--rgw-zonegroup', zonegroup,
                  '--rgw-zone', zone],
             stdin=StringIO(zone_json),
             check_status=True)

    rgwadmin(ctx, master_client,
             cmd=['zone', 'default', '--rgw-zone', zone],
             check_status=True)

    rgwadmin(ctx, master_client,
             cmd=['period', 'update', '--commit'],
             check_status=True)

    yield

def configure_compression_in_default_zone(ctx, config):
    ceph_config = ctx.ceph['ceph'].conf.get('global', {})
    ceph_config.update(ctx.ceph['ceph'].conf.get('client', {}))
    for client, c_config in config.iteritems():
        ceph_config.update(ctx.ceph['ceph'].conf.get(client, {}))
        key = 'rgw compression type'
        if not key in ceph_config:
            log.debug('No compression setting to enable')
            break
        compression = ceph_config[key]
        log.debug('Configuring compression type = %s', compression)

        # XXX: the 'default' zone and zonegroup aren't created until we run RGWRados::init_complete().
        # issue a 'radosgw-admin user list' command to trigger this
        rgwadmin(ctx, client, cmd=['user', 'list'], check_status=True)

        rgwadmin(ctx, client,
                cmd=['zone', 'placement', 'modify', '--rgw-zone', 'default',
                     '--placement-id', 'default-placement', '--compression', compression],
                check_status=True)
        break # only the first client

@contextlib.contextmanager
def configure_regions_and_zones(ctx, config, regions, role_endpoints, realm):
    """
    Configure regions and zones from rados and rgw.
    """
    if not regions:
        log.debug(
            'In rgw.configure_regions_and_zones() and regions is None. '
            'Bailing')
        configure_compression_in_default_zone(ctx, config)
        yield
        return

    if not realm:
        log.debug(
            'In rgw.configure_regions_and_zones() and realm is None. '
            'Bailing')
        configure_compression_in_default_zone(ctx, config)
        yield
        return

    log.info('Configuring regions and zones...')

    log.debug('config is %r', config)
    log.debug('regions are %r', regions)
    log.debug('role_endpoints = %r', role_endpoints)
    log.debug('realm is %r', realm)

    # extract the zone info
    role_zones = dict([(client, extract_zone_info(ctx, client, c_config))
                       for client, c_config in config.iteritems()])
    log.debug('roles_zones = %r', role_zones)

    # extract the user info and append it to the payload tuple for the given
    # client
    for client, c_config in config.iteritems():
        if not c_config:
            user_info = None
        else:
            user_info = extract_user_info(c_config)

        (region, zone, zone_info) = role_zones[client]
        role_zones[client] = (region, zone, zone_info, user_info)

    region_info = dict([
        (region_name, extract_region_info(region_name, r_config))
        for region_name, r_config in regions.iteritems()])

    fill_in_endpoints(region_info, role_zones, role_endpoints)

    # clear out the old defaults
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    first_mon = teuthology.get_first_mon(ctx, config, cluster_name)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    # removing these objects from .rgw.root and the per-zone root pools
    # may or may not matter
    rados(ctx, mon,
          cmd=['-p', '.rgw.root', 'rm', 'region_info.default', '--cluster', cluster_name])
    rados(ctx, mon,
          cmd=['-p', '.rgw.root', 'rm', 'zone_info.default', '--cluster', cluster_name])

    # read master zonegroup and master_zone
    for zonegroup, zg_info in region_info.iteritems():
        if zg_info['is_master']:
            master_zonegroup = zonegroup
            master_zone = zg_info['master_zone']
            break

    for client in config.iterkeys():
        (zonegroup, zone, zone_info, user_info) = role_zones[client]
        if zonegroup == master_zonegroup and zone == master_zone:
            master_client = client
            break

    log.debug('master zonegroup =%r', master_zonegroup)
    log.debug('master zone = %r', master_zone)
    log.debug('master client = %r', master_client)
    log.debug('config %r ', config)

    (ret, out)=rgwadmin(ctx, client,
                        cmd=['realm', 'create', '--rgw-realm', realm, '--default'])
    log.debug('realm create ret %r exists %r', -ret, errno.EEXIST)
    assert ret == 0 or ret != -errno.EEXIST
    if ret is -errno.EEXIST:
        log.debug('realm %r exists', realm)

    for client in config.iterkeys():
        for role, (zonegroup, zone, zone_info, user_info) in role_zones.iteritems():
            rados(ctx, mon,
                  cmd=['-p', zone_info['domain_root'],
                       'rm', 'region_info.default', '--cluster', cluster_name])
            rados(ctx, mon,
                  cmd=['-p', zone_info['domain_root'],
                       'rm', 'zone_info.default', '--cluster', cluster_name])

            (remote,) = ctx.cluster.only(role).remotes.keys()
            for pool_info in zone_info['placement_pools']:
                remote.run(args=['sudo', 'ceph', 'osd', 'pool', 'create',
                                 pool_info['val']['index_pool'], '64', '64', '--cluster', cluster_name])
                if ctx.rgw.ec_data_pool:
                    create_ec_pool(remote, pool_info['val']['data_pool'],
                                   zone, 64, ctx.rgw.erasure_code_profile, cluster_name)
                else:
                    create_replicated_pool(
                        remote, pool_info['val']['data_pool'],
                        64, cluster_name)
            zone_json = json.dumps(dict(zone_info.items() + user_info.items()))
            log.debug('zone info is: %r', zone_json)
            rgwadmin(ctx, client,
                 cmd=['zone', 'set', '--rgw-zonegroup', zonegroup,
                      '--rgw-zone', zone],
                 stdin=StringIO(zone_json),
                 check_status=True)

        for region, info in region_info.iteritems():
            region_json = json.dumps(info)
            log.debug('region info is: %s', region_json)
            rgwadmin(ctx, client,
                     cmd=['zonegroup', 'set'],
                     stdin=StringIO(region_json),
                     check_status=True)
            if info['is_master']:
                rgwadmin(ctx, client,
                         cmd=['zonegroup', 'default', '--rgw-zonegroup', master_zonegroup],
                         check_status=True)

        (zonegroup, zone, zone_info, user_info) = role_zones[client]
        rgwadmin(ctx, client,
                 cmd=['zone', 'default', '--rgw-zone', zone],
                 check_status=True)

    #this used to take master_client, need to edit that accordingly
    rgwadmin(ctx, client,
             cmd=['period', 'update', '--commit'],
             check_status=True)

    yield

@contextlib.contextmanager
def pull_configuration(ctx, config, regions, role_endpoints, realm, master_client):
    """
    Configure regions and zones from rados and rgw.
    """
    if not regions:
        log.debug(
            'In rgw.pull_confguration() and regions is None. '
            'Bailing')
        yield
        return

    if not realm:
        log.debug(
            'In rgw.pull_configuration() and realm is None. '
            'Bailing')
        yield
        return

    log.info('Pulling configuration...')

    log.debug('config is %r', config)
    log.debug('regions are %r', regions)
    log.debug('role_endpoints = %r', role_endpoints)
    log.debug('realm is %r', realm)
    log.debug('master client = %r', master_client)

    # extract the zone info
    role_zones = dict([(client, extract_zone_info(ctx, client, c_config))
                       for client, c_config in config.iteritems()])
    log.debug('roles_zones = %r', role_zones)

    # extract the user info and append it to the payload tuple for the given
    # client
    for client, c_config in config.iteritems():
        if not c_config:
            user_info = None
        else:
            user_info = extract_user_info(c_config)

        (region, zone, zone_info) = role_zones[client]
        role_zones[client] = (region, zone, zone_info, user_info)

    region_info = dict([
        (region_name, extract_region_info(region_name, r_config))
        for region_name, r_config in regions.iteritems()])

    fill_in_endpoints(region_info, role_zones, role_endpoints)

    for client in config.iterkeys():
        if client != master_client:
            cluster_name, daemon_type, client_id = teuthology.split_role(client)
            host, port = role_endpoints[master_client]
            endpoint = 'http://{host}:{port}/'.format(host=host, port=port)
            log.debug("endpoint: %s", endpoint)
            rgwadmin(ctx, client,
                cmd=['realm', 'pull', '--rgw-realm', realm, '--default', '--url',
                     endpoint, '--access_key',
                     user_info['system_key']['access_key'], '--secret',
                     user_info['system_key']['secret_key']],
                     check_status=True)

            (zonegroup, zone, zone_info, zone_user_info) = role_zones[client]
            zone_json = json.dumps(dict(zone_info.items() + zone_user_info.items()))
            log.debug("zone info is: %r", zone_json)
            rgwadmin(ctx, client,
                     cmd=['zone', 'set', '--default',
                          '--rgw-zone', zone],
                     stdin=StringIO(zone_json),
                     check_status=True)

            rgwadmin(ctx, client,
                     cmd=['zonegroup', 'add', '--rgw-zonegroup', zonegroup, '--rgw-zone', zone],
                     check_status=True)

            rgwadmin(ctx, client,
                     cmd=['zonegroup', 'default', '--rgw-zonegroup', zonegroup],
                     check_status=True)

            rgwadmin(ctx, client,
                     cmd=['period', 'update', '--commit', '--url',
                          endpoint, '--access_key',
                          user_info['system_key']['access_key'], '--secret',
                          user_info['system_key']['secret_key']],
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

    To run rgws for multiple regions or zones, describe the regions
    and their zones in a regions section. The endpoints will be
    generated by this task. Each client must have a region, zone,
    and pools assigned in ceph.conf::

        tasks:
        - install:
        - ceph:
            conf:
              client.0:
                rgw region: foo
                rgw zone: foo-1
                rgw region root pool: .rgw.rroot.foo
                rgw zone root pool: .rgw.zroot.foo
                rgw log meta: true
                rgw log data: true
              client.1:
                rgw region: bar
                rgw zone: bar-master
                rgw region root pool: .rgw.rroot.bar
                rgw zone root pool: .rgw.zroot.bar
                rgw log meta: true
                rgw log data: true
              client.2:
                rgw region: bar
                rgw zone: bar-secondary
                rgw region root pool: .rgw.rroot.bar
                rgw zone root pool: .rgw.zroot.bar-secondary
        - rgw:
            default_idle_timeout: 30
            ec-data-pool: true
            erasure_code_profile:
              k: 2
              m: 1
              ruleset-failure-domain: osd
            realm: foo
            regions:
              foo:
                api name: api_name # default: region name
                is master: true    # default: false
                master zone: foo-1 # default: first zone
                zones: [foo-1]
                log meta: true
                log data: true
                placement targets: [target1, target2] # default: []
                default placement: target2            # default: ''
              bar:
                api name: bar-api
                zones: [bar-master, bar-secondary]
            client.0:
              system user:
                name: foo-system
                access key: X2IYPSTY1072DDY1SJMC
                secret key: YIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm
            client.1:
              system user:
                name: bar1
                access key: Y2IYPSTY1072DDY1SJMC
                secret key: XIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm
            client.2:
              system user:
                name: bar2
                access key: Z2IYPSTY1072DDY1SJMC
                secret key: ZIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm
    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                      for id_ in teuthology.all_roles_of_type(
                          ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('rgw', {}))

    regions = {}
    if 'regions' in config:
        # separate region info so only clients are keys in config
        regions = config['regions']
        del config['regions']

    role_endpoints = assign_ports(ctx, config)
    ctx.rgw = argparse.Namespace()
    ctx.rgw.role_endpoints = role_endpoints
    # stash the region info for later, since it was deleted from the config
    # structure
    ctx.rgw.regions = regions

    realm = None
    if 'realm' in config:
        # separate region info so only clients are keys in config
        realm = config['realm']
        del config['realm']
    ctx.rgw.realm = realm

    ctx.rgw.ec_data_pool = False
    if 'ec-data-pool' in config:
        ctx.rgw.ec_data_pool = bool(config['ec-data-pool'])
        del config['ec-data-pool']
    ctx.rgw.erasure_code_profile = {}
    if 'erasure_code_profile' in config:
        ctx.rgw.erasure_code_profile = config['erasure_code_profile']
        del config['erasure_code_profile']
    ctx.rgw.default_idle_timeout = 30
    if 'default_idle_timeout' in config:
        ctx.rgw.default_idle_timeout = int(config['default_idle_timeout'])
        del config['default_idle_timeout']
    ctx.rgw.cache_pools = False
    if 'cache-pools' in config:
        ctx.rgw.cache_pools = bool(config['cache-pools'])
        del config['cache-pools']

    ctx.rgw.frontend = 'civetweb'
    if 'frontend' in config:
        ctx.rgw.frontend = config['frontend']
        del config['frontend']

    ctx.rgw.use_fastcgi = True
    if "use_fcgi" in config:
        ctx.rgw.use_fastcgi = False
        log.info("Using mod_proxy_fcgi instead of mod_fastcgi...")
        del config['use_fcgi']

    subtasks = [
        lambda: create_nonregion_pools(
            ctx=ctx, config=config, regions=regions),
        ]
    log.debug('Nonregion pools created')

    multisite = len(regions) > 1

    if not multisite:
        for zonegroup, zonegroup_info in regions.iteritems():
            log.debug("zonegroup_info =%r", zonegroup_info)
            if len(zonegroup_info['zones']) > 1:
                multisite = True
                break

    log.debug('multisite %s', multisite)

    multi_cluster = False
    if multisite:
        prev_cluster_name = None
        roles = ctx.config['roles']
        #check if any roles have a different cluster_name from eachother
        for lst in roles:
            for role in lst:
                cluster_name, daemon_type, client_id = teuthology.split_role(role)
                if cluster_name != prev_cluster_name and prev_cluster_name != None:
                    multi_cluster = True
                    break
                prev_cluster_name = cluster_name
            if multi_cluster:
                break

    log.debug('multi_cluster %s', multi_cluster)
    ctx.rgw.config = config
    master_client = None

    if multi_cluster:
        log.debug('multi cluster run')

        master_client = get_config_master_client(ctx=ctx,
                                                 config=config,
                                                 regions=regions)
        log.debug('master_client %r', master_client)
        subtasks.extend([
            lambda: configure_multisite_regions_and_zones(
                ctx=ctx,
                config=config,
                regions=regions,
                role_endpoints=role_endpoints,
                realm=realm,
                master_client = master_client,
            )
        ])

        subtasks.extend([
            lambda: configure_users_for_client(
                ctx=ctx,
                config=config,
                client=master_client,
                everywhere=False,
            ),
        ])

        if ctx.rgw.frontend == 'apache':
            subtasks.insert(0,
                            lambda: create_apache_dirs(ctx=ctx, config=config,
                                                       on_client=master_client))
            subtasks.extend([
                lambda: ship_apache_configs(ctx=ctx, config=config,
                                            role_endpoints=role_endpoints, on_client=master_client),
                lambda: start_apache(ctx=ctx, config=config, on_client=master_client),
                lambda: start_rgw(ctx=ctx, config=config, on_client=master_client),
            ])
        elif ctx.rgw.frontend == 'civetweb':
            subtasks.extend([
                lambda: start_rgw(ctx=ctx, config=config, on_client=master_client),
            ])
        else:
            raise ValueError("frontend must be 'apache' or 'civetweb'")

        subtasks.extend([
            lambda: pull_configuration(ctx=ctx,
                                       config=config,
                                       regions=regions,
                                       role_endpoints=role_endpoints,
                                       realm=realm,
                                       master_client=master_client
            ),
        ])

        subtasks.extend([
            lambda: configure_users_for_client(
                ctx=ctx,
                config=config,
                client=master_client,
                everywhere=True
            ),
        ])

        if ctx.rgw.frontend == 'apache':
            subtasks.insert(0,
                            lambda: create_apache_dirs(ctx=ctx, config=config,
                                                       on_client=None,
                                                       except_client = master_client))
            subtasks.extend([
                lambda: ship_apache_configs(ctx=ctx, config=config,
                                            role_endpoints=role_endpoints,
                                            on_client=None,
                                            except_client = master_client,
                ),
                lambda: start_apache(ctx=ctx,
                                     config = config,
                                     on_client=None,
                                     except_client = master_client,
                ),
                lambda: start_rgw(ctx=ctx,
                                  config=config,
                                  on_client=None,
                                  except_client = master_client),
            ])
        elif ctx.rgw.frontend == 'civetweb':
            subtasks.extend([
                lambda: start_rgw(ctx=ctx,
                                  config=config,
                                  on_client=None,
                                  except_client = master_client),
            ])
        else:
            raise ValueError("frontend must be 'apache' or 'civetweb'")
                
    else:
        log.debug('single cluster run')
        subtasks.extend([
            lambda: configure_regions_and_zones(
                ctx=ctx,
                config=config,
                regions=regions,
                role_endpoints=role_endpoints,
                realm=realm,
            ),
            lambda: configure_users(
                ctx=ctx,
                config=config,
                everywhere=True,
            ),
        ])
        if ctx.rgw.frontend == 'apache':
            subtasks.insert(0, lambda: create_apache_dirs(ctx=ctx, config=config))
            subtasks.extend([
                lambda: ship_apache_configs(ctx=ctx, config=config,
                                            role_endpoints=role_endpoints),
                lambda: start_apache(ctx=ctx, config=config),
                lambda: start_rgw(ctx=ctx,
                                  config=config),
                ])
        elif ctx.rgw.frontend == 'civetweb':
            subtasks.extend([
                lambda: start_rgw(ctx=ctx,
                                  config=config),
            ])
        else:
            raise ValueError("frontend must be 'apache' or 'civetweb'")

    log.info("Using %s as radosgw frontend", ctx.rgw.frontend)
    with contextutil.nested(*subtasks):
        yield
