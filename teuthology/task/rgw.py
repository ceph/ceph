import contextlib
import json
import logging
import os

from cStringIO import StringIO

from ..orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.task_util.rgw import rgwadmin
from teuthology.task_util.rados import rados

log = logging.getLogger(__name__)

@contextlib.contextmanager
def create_dirs(ctx, config):
    log.info('Creating apache directories...')
    testdir = teuthology.get_testdir(ctx)
    for client in config.iterkeys():
        ctx.cluster.only(client).run(
            args=[
                'mkdir',
                '-p',
                '{tdir}/apache/htdocs.{client}'.format(tdir=testdir,
                                                       client=client),
                '{tdir}/apache/tmp.{client}'.format(tdir=testdir,
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
        ctx.cluster.only(client).run(
            'rmdir',
            '{tdir}/apache.{client}'.format(tdir=testdir,
                                            client=client),
            )


@contextlib.contextmanager
def ship_config(ctx, config, role_endpoints):
    assert isinstance(config, dict)
    assert isinstance(role_endpoints, dict)
    testdir = teuthology.get_testdir(ctx)
    log.info('Shipping apache config and rgw.fcgi...')
    src = os.path.join(os.path.dirname(__file__), 'apache.conf.template')
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
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
exec radosgw -f -n {client}
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
        run_cmd=[
            'sudo',
            '{tdir}/adjust-ulimits'.format(tdir=testdir),
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            '{tdir}/daemon-helper'.format(tdir=testdir),
            'term',
            ]
        run_cmd_tail=[
            'radosgw',
            '-n', client,
            '-k', '/etc/ceph/ceph.{client}.keyring'.format(client=client),
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

        run_cmd.extend(
            teuthology.get_valgrind_args(
                testdir,
                client,
                client_config.get('valgrind')
                )
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
    log.info('Starting apache...')
    testdir = teuthology.get_testdir(ctx)
    apaches = {}
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        system_type = teuthology.get_system_type(remote)
        if system_type == 'deb':
            apache_name = 'apache2'
        else:
            apache_name = '/usr/sbin/httpd'
        proc = remote.run(
            args=[
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                '{tdir}/daemon-helper'.format(tdir=testdir),
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

def extract_zone_info(ctx, client, client_config):
    user_info = client_config['system user']
    system_user = user_info['name']
    system_access_key = user_info['access key']
    system_secret_key = user_info['secret key']

    ceph_config = ctx.ceph.conf.get('global', {})
    ceph_config.update(ctx.ceph.conf.get('client', {}))
    ceph_config.update(ctx.ceph.conf.get(client, {}))
    for key in ['rgw zone', 'rgw region', 'rgw zone root pool']:
        assert key in ceph_config, \
               'ceph conf must contain {key} for {client}'.format(key=key,
                                                                  client=client)
    region = ceph_config['rgw region']
    zone = ceph_config['rgw zone']
    zone_info = dict(
        domain_root=ceph_config['rgw zone root pool'],
        )
    for key in ['control_pool', 'gc_pool', 'log_pool', 'intent_log_pool',
                'usage_log_pool', 'user_keys_pool', 'user_email_pool',
                'user_swift_pool', 'user_uid_pool']:
        zone_info[key] = '.' + region + '.' + zone + '.' + key

    zone_info['system_key'] = dict(
        user=system_user,
        access_key=system_access_key,
        secret_key=system_secret_key,
        )
    return region, zone, zone_info

def extract_region_info(region, region_info):
    assert isinstance(region_info['zones'], list) and region_info['zones'], \
           'zones must be a non-empty list'
    return dict(
        name=region,
        api_name=region_info.get('api name', region),
        is_master=region_info.get('is master', False),
        master_zone=region_info.get('master zone', region_info['zones'][0]),
        placement_targets=region_info.get('placement targets', []),
        default_placement=region_info.get('default placement', ''),
        )

def assign_ports(ctx, config):
    port = 7280
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for role in roles_for_host:
            if role in config:
                role_endpoints[role] = (remote.name.split('@')[1], port)
                port += 1

    return role_endpoints

def fill_in_endpoints(region_info, role_zones, role_endpoints):
    for role, (host, port) in role_endpoints.iteritems():
        region, zone, _ = role_zones[role]
        host, port = role_endpoints[role]
        endpoint = 'http://{host}:{port}/'.format(host=host, port=port)
        region_conf = region_info[region]
        region_conf.setdefault('endpoints', [])
        region_conf['endpoints'].append(endpoint)
        region_conf.setdefault('zones', [])
        region_conf['zones'].append(dict(name=zone, endpoints=[endpoint]))

@contextlib.contextmanager
def configure_regions_and_zones(ctx, config, regions, role_endpoints):
    if not regions:
        yield
        return

    log.info('Configuring regions and zones...')

    log.debug('config is %r', config)
    log.debug('regions are %r', regions)
    log.debug('role_endpoints = %r', role_endpoints)
    role_zones = dict([(client, extract_zone_info(ctx, client, c_config))
                       for client, c_config in config.iteritems()])
    log.debug('roles_zones = %r', role_zones)
    region_info = dict([(region, extract_region_info(region, r_config))
                        for region, r_config in regions.iteritems()])

    fill_in_endpoints(region_info, role_zones, role_endpoints)
    for client in config.iterkeys():
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
        for role, (_, zone, info) in role_zones.iteritems():
            rgwadmin(ctx, client,
                     cmd=['-n', client, 'zone', 'set', '--rgw-zone', zone],
                     stdin=StringIO(json.dumps(info)),
                     check_status=True)

    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    # removing these objects from .rgw.root and the per-zone root pools
    # may or may not matter
    rados(ctx, mon,
          cmd=['-p', '.rgw.root', 'rm', 'region_info.default'])
    rados(ctx, mon,
          cmd=['-p', '.rgw.root', 'rm', 'zone_info.default'])

    for client in config.iterkeys():
        rgwadmin(ctx, client, cmd=['-n', client, 'regionmap', 'update'])
        for role, (_, zone, zone_info) in role_zones.iteritems():
            rados(ctx, mon,
                  cmd=['-p', zone_info['domain_root'],
                       'rm', 'region_info.default'])
            rados(ctx, mon,
                  cmd=['-p', zone_info['domain_root'],
                       'rm', 'zone_info.default'])
            rgwadmin(ctx, client,
                     cmd=[
                         '-n', client,
                         'user', 'create',
                         '--uid', zone_info['system_key']['user'],
                         '--access-key', zone_info['system_key']['access_key'],
                         '--secret-key', zone_info['system_key']['secret_key'],
                         '--display-name', zone_info['system_key']['user'],
                         ],
                     check_status=True,
                     )
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
                rgw region root pool: foo.rgw.root
                rgw zone root pool: foo.rgw.root
              client.1:
                rgw region: bar
                rgw zone: bar-master
                rgw region root pool: bar.rgw.root
                rgw zone root pool: bar.rgw.root
              client.2:
                rgw region: bar
                rgw zone: bar-secondary
                rgw region root pool: bar.rgw.root
                rgw zone root pool: bar-secondary.rgw.root
        - rgw:
            regions:
              foo:
                api name: api_name # default: region name
                is master: true    # default: false
                master zone: foo-1 # default: first zone
                zones: [foo-1]
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

    with contextutil.nested(
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: configure_regions_and_zones(
            ctx=ctx,
            config=config,
            regions=regions,
            role_endpoints=role_endpoints,
            ),
        lambda: ship_config(ctx=ctx, config=config,
                            role_endpoints=role_endpoints),
        lambda: start_rgw(ctx=ctx, config=config),
        lambda: start_apache(ctx=ctx, config=config),
        ):
        yield
