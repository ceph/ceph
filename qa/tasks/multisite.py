import argparse
import contextlib
import json
import logging
import os
import errno
import util.multisite_rgw as rgw_utils

from requests.packages.urllib3 import PoolManager
from requests.packages.urllib3.util import Retry

from cStringIO import StringIO

from teuthology.orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra.run import CommandFailedError
# from util.rgw import rgwadmin
from util.multisite_rgw import rgwadmin, get_config_master_client, extract_zone_info, extract_region_info
from util.multisite_rados import (rados, create_ec_pool,
                                        create_replicated_pool,
                                        create_cache_pool)


log = logging.getLogger(__name__)


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

        host, port = ctx.multisite.role_endpoints[client]
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

        run_cmd = list(cmd_prefix)
        run_cmd.extend(rgw_cmd)

        ctx.daemons.add_daemon(
            remote, 'rgw', client,
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
        host, port = ctx.multisite.role_endpoints[client]
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
                    '{tdir}/rgw.opslog.{client}.sock'.format(tdir=testdir,
                                                             client=client),
                    ],
                )


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


def fill_in_endpoints(region_info, role_zones, role_endpoints):
    """
    Iterate through the list of role_endpoints, filling in zone information

    :param region_info: region data
    :param role_zones: region and zone information.
    :param role_endpoints: endpoints being used
    """
    for role, (host, port) in role_endpoints.iteritems():
        region, zone, _ = role_zones[role]
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


def assign_ports(ctx, config):

    port = 8080
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for role in roles_for_host:
            if role in config:
                role_endpoints[role] = (remote.name.split('@')[1], port)
                port += 1

    return role_endpoints


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

        (region, zone) = role_zones[client]
        role_zones[client] = (region, zone, user_info)

    region_info = dict([
        (region_name, extract_region_info(region_name, r_config))
        for region_name, r_config in regions.iteritems()])

    fill_in_endpoints(region_info, role_zones, role_endpoints)


    host, port = role_endpoints[master_client]
    endpoint = 'http://{host}:{port}/'.format(host=host, port=port)
    log.debug("endpoint: %s", endpoint)

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

    rgwadmin(ctx, master_client,
             cmd=['zonegroup', 'create', '--rgw-zonegroup', master_zonegroup, '--master', '--endpoints', endpoint,
                  '--default'])

    rgwadmin(ctx, master_client,
             cmd=['zone', 'create', '--rgw-zonegroup', master_zonegroup,
                  '--rgw-zone', master_zone, '--endpoints', endpoint, '--access-key',
                  user_info['system_key']['access_key'], '--secret',
                  user_info['system_key']['secret_key'], '--master', '--default'],
             check_status=True)

    rgwadmin(ctx, master_client,
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

        (region, zone) = role_zones[client]
        role_zones[client] = (region, zone, user_info)

    region_info = dict([
        (region_name, extract_region_info(region_name, r_config))
        for region_name, r_config in regions.iteritems()])

    fill_in_endpoints(region_info, role_zones, role_endpoints)

    for client in config.iterkeys():
        if client != master_client:
            host, port = role_endpoints[master_client]
            endpoint = 'http://{host}:{port}/'.format(host=host, port=port)
            log.debug("endpoint: %s", endpoint)
            rgwadmin(ctx, client,
                cmd=['realm', 'pull', '--url',
                     endpoint, '--access_key',
                     user_info['system_key']['access_key'], '--secret',
                     user_info['system_key']['secret_key']],
                     check_status=True)

            rgwadmin(ctx, client,
                cmd=['realm', 'default', '--rgw-realm', realm])

            rgwadmin(ctx, client,
                cmd=['period', 'pull', '--url', endpoint, '--access_key',
                     user_info['system_key']['access_key'], '--secret',
                     user_info['system_key']['secret_key']],
                     check_status=True)

            (zonegroup, zone, zone_user_info) = role_zones[client]
            # slave endpoint
            shost, sport = role_endpoints[client]
            sendpoint = 'http://{host}:{port}/'.format(host=shost, port=sport)
            log.debug("sendpoint: %s", sendpoint)
            rgwadmin(ctx, client,
                     cmd=['zone', 'create', '--rgw-zonegroup', zonegroup,
                          '--rgw-zone', zone, '--endpoints', sendpoint, '--access-key',
                          user_info['system_key']['access_key'], '--secret',
                          user_info['system_key']['secret_key'], '--default'],
                     check_status=True)

            rgwadmin(ctx, client,
                     cmd=['period', 'update', '--commit',
                          '--access_key',
                          user_info['system_key']['access_key'], '--secret',
                          user_info['system_key']['secret_key']],
                     check_status=True)

    yield


@contextlib.contextmanager
def task(ctx, config):

    """
        - rgw:
            [client.0]
            [client.1]
        - multisite:
            realm: foo
            regions:
              zg-1:
                api name: api_name # default: zonegroup name
                is master: true    # default: false
                master zone: foo-1 # default: first zone
                zones: [foo-1]
                log meta: true
                log data: true
              zg-2:
                api name: zg-2
                zones: [foo-2]
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
    ctx.multisite = argparse.Namespace()
    ctx.multisite.role_endpoints = role_endpoints
    # stash the region info for later, since it was deleted from the config
    # structure
    ctx.multisite.regions = regions

    realm = None
    if 'realm' in config:
        # separate region info so only clients are keys in config
        realm = config['realm']
        del config['realm']
    ctx.multisite.realm = realm
    ctx.multisite.config = config
    master_client = None

    master_client = get_config_master_client(ctx=ctx,
                                             config=config,
                                             regions=regions)

    log.debug('master_client %r', master_client)
    subtasks = [
        lambda: configure_multisite_regions_and_zones(
            ctx=ctx,
            config=config,
            regions=regions,
            role_endpoints=role_endpoints,
            realm=realm,
            master_client=master_client,
        )
    ]

    subtasks.extend([
        lambda: configure_users_for_client(
            ctx=ctx,
            config=config,
            client=master_client,
            everywhere=False,
        ),
    ])

    subtasks.extend([
        lambda: start_rgw(ctx=ctx, config=config, on_client=master_client),
    ])

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
        lambda: start_rgw(ctx=ctx,
                          config=config,
                          on_client=None,
                          except_client = master_client),
    ])

    with contextutil.nested(*subtasks):
        yield

