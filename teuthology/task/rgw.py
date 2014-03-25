"""
rgw routines
"""
import argparse
import contextlib
import json
import logging
import os

from cStringIO import StringIO

from ..orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.task_util.rgw import rgwadmin
from teuthology.task_util.rados import rados, create_ec_pool, create_replicated_pool

log = logging.getLogger(__name__)

@contextlib.contextmanager
def create_dirs(ctx, config):
    """
    Remotely create apache directories.  Delete when finished.
    """
    log.info('Creating apache directories...')
    testdir = teuthology.get_testdir(ctx)
    for client in config.iterkeys():
        ctx.cluster.only(client).run(
            args=[
                'mkdir',
                '-p',
                '{tdir}/apache/htdocs.{client}'.format(tdir=testdir,
                                                       client=client),
                '{tdir}/apache/tmp.{client}/fastcgi_sock'.format(tdir=testdir,
                                                                 client=client),
                run.Raw('&&'),
                'mkdir',
                '{tdir}/archive/apache.{client}'.format(tdir=testdir,
                                                        client=client),
                ],
            )
    try:
        yield
    finally:
        log.info('Cleaning up apache directories...')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/apache/tmp.{client}'.format(tdir=testdir,
                                                        client=client),
                    run.Raw('&&'),
                    'rmdir',
                    '{tdir}/apache/htdocs.{client}'.format(tdir=testdir,
                                                           client=client),
                    ],
                )

        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rmdir',
                    '{tdir}/apache'.format(tdir=testdir),
                    ],
                check_status=False, # only need to remove once per host
                )


@contextlib.contextmanager
def ship_config(ctx, config, role_endpoints):
    """
    Ship apache config and rgw.fgci to all clients.  Clean up on termination
    """
    assert isinstance(config, dict)
    assert isinstance(role_endpoints, dict)
    testdir = teuthology.get_testdir(ctx)
    log.info('Shipping apache config and rgw.fcgi...')
    src = os.path.join(os.path.dirname(__file__), 'apache.conf.template')
    for client, conf in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
        if not conf:
            conf = {}
        idle_timeout = conf.get('idle_timeout', 30)
        if system_type == 'deb':
            mod_path = '/usr/lib/apache2/modules'
            print_continue = 'on'
        else:
            mod_path = '/usr/lib64/httpd/modules'
            print_continue = 'off'
        host, port = role_endpoints[client]
        with file(src, 'rb') as f:
            conf = f.read().format(
                testdir=testdir,
                mod_path=mod_path,
                print_continue=print_continue,
                host=host,
                port=port,
                client=client,
                idle_timeout=idle_timeout,
                )
            teuthology.write_file(
                remote=remote,
                path='{tdir}/apache/apache.{client}.conf'.format(tdir=testdir,
                                                                 client=client),
                data=conf,
                )
        teuthology.write_file(
            remote=remote,
            path='{tdir}/apache/htdocs.{client}/rgw.fcgi'.format(tdir=testdir,
                                                                 client=client),
            data="""#!/bin/sh
ulimit -c unlimited
exec radosgw -f -n {client} -k /etc/ceph/ceph.{client}.keyring --rgw-socket-path {tdir}/apache/tmp.{client}/fastcgi_sock/rgw_sock

""".format(tdir=testdir, client=client)
            )
        remote.run(
            args=[
                'chmod',
                'a=rx',
                '{tdir}/apache/htdocs.{client}/rgw.fcgi'.format(tdir=testdir,
                                                                client=client),
                ],
            )
    try:
        yield
    finally:
        log.info('Removing apache config...')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/apache/apache.{client}.conf'.format(tdir=testdir,
                                                                client=client),
                    run.Raw('&&'),
                    'rm',
                    '-f',
                    '{tdir}/apache/htdocs.{client}/rgw.fcgi'.format(tdir=testdir,
                                                                    client=client),
                    ],
                )


@contextlib.contextmanager
def start_rgw(ctx, config):
    """
    Start rgw on remote sites.
    """
    log.info('Starting rgw...')
    testdir = teuthology.get_testdir(ctx)
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()

        client_config = config.get(client)
        if client_config is None:
            client_config = {}
        log.info("rgw %s config is %s", client, client_config)
        id_ = client.split('.', 1)[1]
        log.info('client {client} is id {id}'.format(client=client, id=id_))
        run_cmd = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'daemon-helper',
            'term',
            ]
        run_cmd_tail = [
            'radosgw',
            '-n', client,
            '-k', '/etc/ceph/ceph.{client}.keyring'.format(client=client),
            '--rgw-socket-path',
            '{tdir}/apache/tmp.{client}/fastcgi_sock/rgw_sock'.format(
                tdir=testdir,
                client=client,
                ),
            '--log-file',
            '/var/log/ceph/rgw.{client}.log'.format(client=client),
            '--rgw_ops_log_socket_path',
            '{tdir}/rgw.opslog.{client}.sock'.format(tdir=testdir,
                                                     client=client),
            '{tdir}/apache/apache.{client}.conf'.format(tdir=testdir,
                                                        client=client),
            '--foreground',
            run.Raw('|'),
            'sudo',
            'tee',
            '/var/log/ceph/rgw.{client}.stdout'.format(tdir=testdir,
                                                       client=client),
            run.Raw('2>&1'),
            ]

        if client_config.get('valgrind'):
            run_cmd = teuthology.get_valgrind_args(
                testdir,
                client,
                run_cmd,
                client_config.get('valgrind')
                )

        run_cmd.extend(run_cmd_tail)

        ctx.daemons.add_daemon(
            remote, 'rgw', client,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )

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


@contextlib.contextmanager
def start_apache(ctx, config):
    """
    Start apache on remote sites.
    """
    log.info('Starting apache...')
    testdir = teuthology.get_testdir(ctx)
    apaches = {}
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
        if system_type == 'deb':
            apache_name = 'apache2'
        else:
            apache_name = '/usr/sbin/httpd.worker'
        proc = remote.run(
            args=[
                'adjust-ulimits',
                'daemon-helper',
                'kill',
                apache_name,
                '-X',
                '-f',
                '{tdir}/apache/apache.{client}.conf'.format(tdir=testdir,
                                                            client=client),
                ],
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )
        apaches[client] = proc

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
    # test if there isn't a system user or if there isn't a name for that user, return None
    if 'system user' not in client_config or 'name' not in client_config['system user']:
        return None

    user_info = dict()
    user_info['system_key'] = dict(
        user=client_config['system user']['name'],
        access_key=client_config['system user']['access key'],
        secret_key=client_config['system user']['secret key'],
        )
    return user_info

def extract_zone_info(ctx, client, client_config):
    """
    Get zone information.
    :param client: dictionary of client information
    :param client_config: dictionary of client configuration information
    :returns: zone extracted from client and client_config information
    """
    ceph_config = ctx.ceph.conf.get('global', {})
    ceph_config.update(ctx.ceph.conf.get('client', {}))
    ceph_config.update(ctx.ceph.conf.get(client, {}))
    for key in ['rgw zone', 'rgw region', 'rgw zone root pool']:
        assert key in ceph_config, \
               'ceph conf must contain {key} for {client}'.format(key=key,
                                                                  client=client)
    region = ceph_config['rgw region']
    zone = ceph_config['rgw zone']
    zone_info = dict()
    for key in ['rgw control pool', 'rgw gc pool', 'rgw log pool', 'rgw intent log pool',
                'rgw usage log pool', 'rgw user keys pool', 'rgw user email pool',
                'rgw user swift pool', 'rgw user uid pool', 'rgw domain root']:
        new_key = key.split(' ', 1)[1]
        new_key = new_key.replace(' ', '_')

        if key in ceph_config:
            value = ceph_config[key]
            log.debug('{key} specified in ceph_config ({val})'.format(key=key, val=value))
            zone_info[new_key] = value
        else:
            zone_info[new_key] = '.' + region + '.' + zone + '.' + new_key

    index_pool = '.' + region + '.' + zone + '.' + 'index_pool'
    data_pool = '.' + region + '.' + zone + '.' + 'data_pool'

    zone_info['placement_pools'] = [{'key': 'default_placement',
                                     'val': {'index_pool': index_pool,
                                             'data_pool': data_pool}
                                     }]

    # these keys are meant for the zones argument in the region info.
    # We insert them into zone_info with a different format and then remove them
    # in the fill_in_endpoints() method
    for key in ['rgw log meta', 'rgw log data']:
        if key in ceph_config:
            zone_info[key] = ceph_config[key]

    # these keys are meant for the zones argument in the region info.
    # We insert them into zone_info with a different format and then remove them
    # in the fill_in_endpoints() method
    for key in ['rgw log meta', 'rgw log data']:
        if key in ceph_config:
            zone_info[key] = ceph_config[key]

    return region, zone, zone_info

def extract_region_info(region, region_info):
    """
    Extract region information from the region_info parameter, using get
    to set default values.

    :param region: name of the region
    :param region_info: region information (in dictionary form).
    :returns: dictionary of region information set from region_info, using
            default values for missing fields.
    """
    assert isinstance(region_info['zones'], list) and region_info['zones'], \
           'zones must be a non-empty list'
    return dict(
        name=region,
        api_name=region_info.get('api name', region),
        is_master=region_info.get('is master', False),
        log_meta=region_info.get('log meta', False),
        log_data=region_info.get('log data', False),
        master_zone=region_info.get('master zone', region_info['zones'][0]),
        placement_targets=region_info.get('placement targets',
                                          [{'name': 'default_placement',
                                            'tags': []}]),
        default_placement=region_info.get('default placement',
                                          'default_placement'),
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
            raise Exception('Region: {region} was specified but no corresponding' \
                            ' entry was found under \'regions\''.format(region=region))

        region_conf = region_info[region]
        region_conf.setdefault('endpoints', [])
        region_conf['endpoints'].append(endpoint)

        # this is the payload for the 'zones' field in the region field
        zone_payload = dict()
        zone_payload['endpoints'] = [endpoint]
        zone_payload['name'] = zone

        # Pull the log meta and log data settings out of zone_info, if they exist, then pop them
        # as they don't actually belong in the zone info
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
def configure_users(ctx, config, everywhere=False):
    """
    Create users by remotely running rgwadmin commands using extracted
    user information.
    """
    log.info('Configuring users...')

    # extract the user info and append it to the payload tuple for the given client
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
                      user=user_info['system_key']['user'],client=client))
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
        if ctx.rgw.ec_data_pool:
            create_ec_pool(remote, data_pool, client, 64)
        else:
            create_replicated_pool(remote, data_pool, 64)
    yield

@contextlib.contextmanager
def configure_regions_and_zones(ctx, config, regions, role_endpoints):
    """
    Configure regions and zones from rados and rgw.
    """
    if not regions:
        log.debug('In rgw.configure_regions_and_zones() and regions is None. Bailing')
        yield
        return

    log.info('Configuring regions and zones...')

    log.debug('config is %r', config)
    log.debug('regions are %r', regions)
    log.debug('role_endpoints = %r', role_endpoints)
    # extract the zone info
    role_zones = dict([(client, extract_zone_info(ctx, client, c_config))
                       for client, c_config in config.iteritems()])
    log.debug('roles_zones = %r', role_zones)

    # extract the user info and append it to the payload tuple for the given client
    for client, c_config in config.iteritems():
        if not c_config:
            user_info = None
        else:
            user_info = extract_user_info(c_config)

        (region, zone, zone_info) = role_zones[client]
        role_zones[client] = (region, zone, zone_info, user_info)

    region_info = dict([(region, extract_region_info(region, r_config))
                        for region, r_config in regions.iteritems()])

    fill_in_endpoints(region_info, role_zones, role_endpoints)

    # clear out the old defaults
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    # removing these objects from .rgw.root and the per-zone root pools
    # may or may not matter
    rados(ctx, mon,
          cmd=['-p', '.rgw.root', 'rm', 'region_info.default'])
    rados(ctx, mon,
          cmd=['-p', '.rgw.root', 'rm', 'zone_info.default'])

    for client in config.iterkeys():
        for role, (_, zone, zone_info, user_info) in role_zones.iteritems():
            rados(ctx, mon,
                  cmd=['-p', zone_info['domain_root'],
                       'rm', 'region_info.default'])
            rados(ctx, mon,
                  cmd=['-p', zone_info['domain_root'],
                       'rm', 'zone_info.default'])

            (remote,) = ctx.cluster.only(role).remotes.keys()
            for pool_info in zone_info['placement_pools']:
                remote.run(args=['ceph', 'osd', 'pool', 'create',
                                 pool_info['val']['index_pool'], '64', '64'])
                if ctx.rgw.ec_data_pool:
                    create_ec_pool(remote, pool_info['val']['data_pool'],
                                   zone, 64)
                else:
                    create_replicated_pool(remote, pool_info['val']['data_pool'],
                                           64)

            rgwadmin(ctx, client,
                     cmd=['-n', client, 'zone', 'set', '--rgw-zone', zone],
                     stdin=StringIO(json.dumps(dict(zone_info.items() + user_info.items()))),
                     check_status=True)

        for region, info in region_info.iteritems():
            region_json = json.dumps(info)
            log.debug('region info is: %s', region_json)
            rgwadmin(ctx, client,
                     cmd=['-n', client, 'region', 'set'],
                     stdin=StringIO(region_json),
                     check_status=True)
            if info['is_master']:
                rgwadmin(ctx, client,
                         cmd=['-n', client,
                              'region', 'default',
                              '--rgw-region', region],
                         check_status=True)

        rgwadmin(ctx, client, cmd=['-n', client, 'regionmap', 'update'])
    yield

@contextlib.contextmanager
def task(ctx, config):
    """
    Spin up apache configured to run a rados gateway.
    Only one should be run per machine, since it uses a hard-coded port for now.

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

    Note that without a modified fastcgi module e.g. with the default
    one on CentOS, you must have rgw print continue = false in ceph.conf::

        tasks:
        - ceph:
            conf:
              global:
                rgw print continue: false
        - rgw: [client.0]

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
            ec-data-pool: true
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
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    regions = {}
    if 'regions' in config:
        # separate region info so only clients are keys in config
        regions = config['regions']
        del config['regions']

    role_endpoints = assign_ports(ctx, config)
    ctx.rgw = argparse.Namespace()
    ctx.rgw.role_endpoints = role_endpoints
    # stash the region info for later, since it was deleted from the config structure
    ctx.rgw.regions = regions

    ctx.rgw.ec_data_pool = False
    if 'ec-data-pool' in config:
        ctx.rgw.ec_data_pool = bool(config['ec-data-pool'])
        del config['ec-data-pool']

    with contextutil.nested(
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: configure_regions_and_zones(
            ctx=ctx,
            config=config,
            regions=regions,
            role_endpoints=role_endpoints,
            ),
        lambda: configure_users(
            ctx=ctx,
            config=config,
            everywhere=bool(regions),
            ),
        lambda: create_nonregion_pools(ctx=ctx, config=config, regions=regions),
        lambda: ship_config(ctx=ctx, config=config,
                            role_endpoints=role_endpoints),
        lambda: start_rgw(ctx=ctx, config=config),
        lambda: start_apache(ctx=ctx, config=config),
        ):
        yield
