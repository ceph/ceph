import argparse
import contextlib
import json
import logging
import random
import string
from cStringIO import StringIO
from teuthology import misc as teuthology
from teuthology import contextutil
from requests.packages.urllib3 import PoolManager
from requests.packages.urllib3.util import Retry
import ConfigParser

log = logging.getLogger(__name__)

access_key = None
secret = None


def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False,
             format='json', decode=True, log_level=logging.DEBUG):
    log.info('rgwadmin: {client} : {cmd}'.format(client=client, cmd=cmd))
    testdir = teuthology.get_testdir(ctx)
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    pre = ['sudo',
           'radosgw-admin'.format(tdir=testdir),
           '--log-to-stderr',
           ]
    pre.extend(cmd)
    log.log(log_level, 'rgwadmin: cmd=%s' % pre)
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    proc = remote.run(
        args=pre,
        check_status=check_status,
        stdout=StringIO(),
        stderr=StringIO(),
        stdin=stdin,
        )
    r = proc.exitstatus
    out = proc.stdout.getvalue()
    if not decode:
        return r, out
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.log(log_level, ' json result: %s' % j)
        except ValueError:
            j = out
            log.log(log_level, ' raw result: %s' % j)
    return r, j


def extract_endpoints(ctx, roles):

    port = 8080
    url_endpoint = {}

    if isinstance(roles, basestring):
        roles = [roles]

    for role in roles:
        remote,  = ctx.cluster.only(role).remotes.iterkeys()
        url_endpoint[role] = (remote.name.split('@')[1], port)

    log.info('Endpoints are {}'.format(url_endpoint))

    url = ''
    for machine, (host, port) in url_endpoint.iteritems():
        url = url + 'http://{host}:{port}'.format(host=host, port=port) + ','
    url = url[:-1]

    log.debug("endpoints: %s", url)

    return url


def get_config_clients(ctx, config):

    master_zonegroup = None
    master_zone = None
    target_zone = None

    zonegroups_config = config['zonegroups']
    for zonegroup_config in zonegroups_config:
        if zonegroup_config.get('is_master', False):
            master_zonegroup = zonegroup_config.get('name')
        for zone in zonegroup_config['zones']:
            if zone.get('is_master', False):
                mz_config = zone
                master_zone = mz_config.get('name')
                master_clients = mz_config.get('endpoints')
            else:
                tz_config = zone
                target_zone = tz_config.get('name')
                target_clients = tz_config.get('endpoints')

    return master_zonegroup, master_zone, master_clients, target_zone, target_clients


def zone_to_conf(ctx, hosts, zone_name):

    """
    Add zone entry in ceph conf file
    """
    parser = ConfigParser.ConfigParser()

    for host in hosts:
        cluster_name, _, _ = teuthology.split_role(host)
        (remote,) = ctx.cluster.only(host).remotes.iterkeys()
        conf_path = '/etc/ceph/ceph.conf'
        conf_file = remote.get_file(conf_path, '/tmp')
        config_section = 'client.rgw.{}'.format(remote.shortname)
        parser.read(conf_file)
        if not parser.has_section(config_section):
            log.info('RGW might not be installed')
            raise ConfigParser.NoSectionError
        else:
            parser.set(config_section, 'rgw_zone', zone_name)

        with open(conf_file, 'w') as fp:
            parser.write(fp)
            fp.close()

        remote.put_file(conf_file, '/tmp/ceph.conf')
        remote.run(args=['sudo', 'cp', '/tmp/ceph.conf', conf_path])


def gen_access_key():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))


def gen_secret():
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(32))


def wait_for_radosgw(ctx, client):

    url_endpoint = extract_endpoints(ctx, client)
    http = PoolManager(retries=Retry(connect=8, backoff_factor=1))
    http.request('GET', url_endpoint)


@contextlib.contextmanager
def create_zone(ctx, config, target_zone, master_zonegroup, target_clients):

    # used by addzone() and task() to configure secondary zone

    url_endpoint = extract_endpoints(ctx, target_clients)
    log.info('creating zone on {}'.format(target_clients))

    if config.get('is_read_only', False):
        rgwadmin(ctx, target_clients[0],
                 cmd=['zone', 'create', '--rgw-zonegroup', master_zonegroup,
                      '--rgw-zone', target_zone, '--endpoints', url_endpoint, '--access-key',
                      access_key, '--secret',
                      secret, '--default', '--read-only'],
                 check_status=True)
    else:
        rgwadmin(ctx, target_clients[0],
                 cmd=['zone', 'create', '--rgw-zonegroup', master_zonegroup,
                      '--rgw-zone', target_zone, '--endpoints', url_endpoint, '--access-key',
                      access_key, '--secret',
                      secret, '--default'],
                 check_status=True)

    rgwadmin(ctx, target_clients[0],
             cmd=['period', 'update', '--commit',
                  '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    zone_to_conf(ctx, target_clients, target_zone)

    yield


@contextlib.contextmanager
def configure_master_zonegroup_and_zones(ctx, config, master_zonegroup, master_zone, realm, master_clients):

    """ Create zonegroup and zone on master"""
    global access_key, secret
    access_key = gen_access_key()
    secret = gen_secret()

    zone_endpoint = extract_endpoints(ctx, master_clients)
    log.info('client {}'.format(master_clients[0]))
    zg_endpoint = extract_endpoints(ctx, master_clients[0])

    log.info('creating master zonegroup and zone on {}'.format(master_clients))
    rgwadmin(ctx, master_clients[0],
             cmd=['realm', 'create', '--rgw-realm', realm, '--default'],
             check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['zonegroup', 'create', '--rgw-zonegroup', master_zonegroup, '--master', '--endpoints', zg_endpoint,
                  '--default'], check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['zone', 'create', '--rgw-zonegroup', master_zonegroup,
                  '--rgw-zone', master_zone, '--endpoints', zone_endpoint, '--access-key',
                  access_key, '--secret',
                  secret, '--master', '--default'],
             check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['period', 'update', '--commit'],
             check_status=True)

    zone_to_conf(ctx, master_clients, master_zone)

    yield


@contextlib.contextmanager
def configure_user_for_client(ctx, master_client):

    """ Create system user"""

    user = 'sysadmin'

    log.debug('Creating system user {user} on {client}'.format(
        user=user, client=master_client))
    rgwadmin(ctx, master_client,
                cmd=[
                    'user', 'create',
                    '--uid', user,
                    '--access-key', access_key,
                    '--secret', secret,
                    '--display-name', user,
                    '--system',
                ],
                check_status=True,
        )
    yield


@contextlib.contextmanager
def pull_configuration(ctx, realm,  master_client, target_client):

    """ Pull realm and period from master zone"""

    url_endpoint = extract_endpoints(ctx, master_client)

    log.info('Pulling master config information from {}'.format(master_client))
    rgwadmin(ctx, target_client,
             cmd=['realm', 'pull', '--url',
                  url_endpoint, '--access_key',
                  access_key, '--secret',
                  secret],
            check_status=True)

    rgwadmin(ctx, target_client,
             cmd=['realm', 'default', '--rgw-realm', realm])

    rgwadmin(ctx, target_client,
             cmd=['period', 'pull', '--url', url_endpoint, '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    yield


def restart_rgw(ctx, role):

    log.info('Restarting rgw...')
    log.debug('client %r', role)
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    hostname = remote.name.split('@')[1].split('.')[0]
    rgw_cmd = [
        'sudo', 'systemctl', 'restart', 'ceph-radosgw@rgw.{hostname}'.format(hostname=hostname)]

    run_cmd = list(rgw_cmd)
    remote.run(args=run_cmd)

    wait_for_radosgw(ctx, role)


@contextlib.contextmanager
def check_sync_status(ctx, clients):

    """Check multisite sync status"""

    log.info("Clients are {}".format(clients))
    for each_client in clients:
        rgwadmin(ctx, each_client,
                 cmd=['sync', 'status'],
                 check_status=True)

    yield


@contextlib.contextmanager
def start_rgw(ctx, on_client):

    for client in on_client:
        restart_rgw(ctx, client)

    yield


@contextlib.contextmanager
def failover(ctx, config):
    """
    - new-rgw-multisite.failover:
        new_master_zone: test-zone2
        new_master: c2.client.1

    """
    # When master is down, bring up secondary as the master zone

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    new_master = config['new_master']
    zone = config['new_master_zone']
    # Make secondary zone master
    rgwadmin(ctx, new_master,
             cmd=['zone', 'modify', '--rgw-zone', zone, '--master', '--default', '--access-key',
                  access_key, '--secret',
                  secret, '--read-only=false'],
             check_status=True)

    # Do period commit
    rgwadmin(ctx, new_master,
             cmd=['period', 'update', '--commit',
                  '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Restart gateway

    restart_rgw(ctx, new_master)

    yield


@contextlib.contextmanager
def failback(ctx, config):

    """
    - new-rgw-multisite.failback:
    """
    # When master node is back, failback to original master zone

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    master_zonegroup, master_zone, master_clients, target_zone, target_clients = \
        get_config_clients(ctx, ctx.new_rgw_multisite.config)

    url_endpoint = extract_endpoints(ctx, target_clients[0])

    # Period pull in former master zone from current master zone

    rgwadmin(ctx, master_clients[0],
             cmd=['period', 'pull', '--url', url_endpoint, '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Make the original master zone as master

    rgwadmin(ctx, master_clients[0],
             cmd=['zone', 'modify', '--rgw-zone', master_zone, '--master', '--default', '--access-key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Do period commit

    rgwadmin(ctx, master_clients[0],
             cmd=['period', 'update', '--commit',
                  '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Restart gateway
    for client in master_clients:
        restart_rgw(ctx, client)

    # If secondary zone was read-only before failover, explicitly set it to --read-only again.

    zone_config = {}

    zgs = ctx.new_rgw_multisite.config['zonegroups']
    for zg in zgs:
        for zone in zg.get('zones'):
            zone_config = zone

    if zone_config.get('is_read_only', False):
        rgwadmin(ctx, target_clients[0],
                 cmd=['zone', 'modify', '--rgw-zone', target_zone, '--read-only', '--access-key',
                      access_key, '--secret',
                      secret],
                 check_status=True)

        # Do period commit
        rgwadmin(ctx, target_clients[0],
                 cmd=['period', 'update', '--commit',
                      '--access_key',
                      access_key, '--secret',
                      secret],
                 check_status=True)

        # Restart gateway
        for client in target_clients:
            restart_rgw(ctx, client)

    yield


@contextlib.contextmanager
def addzone(ctx, config):


    # to add a new zone

    """
    new-rgw_multisite.addzone:
          name: test-zone2
          is_read_only: true
          endpoints: c1.client.0

    """

    log.info('config %s' % config)

    if config is None:
        config = {}

    log.info('config is {}'.format(config))
    master_zonegroup = None

    roles = config.get('endpoints')
    if isinstance(roles, basestring):
        roles = [roles]
    zone_name = config.get('name')

    log.info('creating zone on {}'.format(roles))

    zgs = ctx.new_rgw_multisite.config['zonegroups']
    for zg in zgs:
        if zg.get('is_master', False):
            master_zonegroup = zg.get('name')

    log.info('Pull configuration from master node')

    subtasks = [
        lambda: pull_configuration(ctx=ctx,
                                   realm=ctx.new_rgw_multisite.realm_name,
                                   master_client=ctx.new_rgw_multisite.master_clients[0],
                                   target_client=roles[0],
                                   ),
        ]

    subtasks.extend([
        lambda: create_zone(ctx=ctx,
                            config=config,
                            target_clients=roles,
                            master_zonegroup=master_zonegroup,
                            target_zone=zone_name)
    ])

    subtasks.extend([
        lambda: start_rgw(ctx=ctx,
                          on_client=roles),
    ])

    # Also restart former master client and the target client.

    subtasks.extend([
        lambda: start_rgw(ctx=ctx,
                          on_client=ctx.new_rgw_multisite.master_clients),
    ])

    subtasks.extend([
        lambda: start_rgw(ctx=ctx,
                          on_client=ctx.new_rgw_multisite.target_clients),
    ])

    subtasks.extend([
        lambda: check_sync_status(ctx=ctx,
                                  clients=ctx.new_rgw_multisite.clients),
    ])

    with contextutil.nested(*subtasks):
        yield


@contextlib.contextmanager
def modify_master(ctx, config, master_zonegroup, master_zone, realm, master_clients):

    """ Create zonegroup and zone on master."""

    global access_key, secret
    access_key = gen_access_key()
    secret = gen_secret()

    url_endpoint = extract_endpoints(ctx, master_clients)

    log.info('creating realm {}'.format(realm))
    rgwadmin(ctx, master_clients[0],
             cmd=['realm', 'create', '--rgw-realm', realm, '--default'],
             check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['zonegroup', 'rename', '--rgw-zonegroup', 'default', '--zonegroup-new-name',
                  master_zonegroup], check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['zone', 'rename', '--rgw-zone', 'default', '--zone-new-name', master_zone,
                  '--rgw-zonegroup', master_zonegroup],
             check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['zonegroup', 'modify', '--rgw-realm', realm, '--rgw-zonegroup', master_zonegroup, '--master',
                  '--endpoints', url_endpoint,
                  '--default'], check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['zone', 'modify', '--rgw-realm', realm, '--rgw-zonegroup', master_zonegroup,
                  '--rgw-zone', master_zone, '--endpoints', url_endpoint, '--access-key',
                  access_key, '--secret',
                  secret, '--master', '--default'],
             check_status=True)

    rgwadmin(ctx, master_clients[0],
             cmd=['period', 'update', '--commit'],
             check_status=True)

    yield


def remove_cluster_names(clients):

    for idx, client in enumerate(clients):
        clients[idx] = teuthology.ceph_role(client)
    return clients


@contextlib.contextmanager
def task(ctx, config):

    """
    - new-multisite:
            migrate: true
            realm:
              name: test-realm
              is_default: true
            zonegroups:
              - name: test-zonegroup
                is_master: true
                is_default: true
                zones:
                  - name: test-zone1
                    is_master: true
                    is_default: true
                    endpoints: [c1.client.0]
                  - name: test-zone2
                    is_default: true
                    is_read_only: true
                    endpoints: [c2.client.0]
    """

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    zonegroups = {}

    if 'zonegroups' in config:
        zonegroups = config['zonegroups']

    realm = None
    if 'realm' in config:
        realm = config['realm']
    realm_name = realm.get('name')

    ctx.new_rgw_multisite = argparse.Namespace()
    ctx.new_rgw_multisite.realm = realm
    ctx.new_rgw_multisite.realm_name = realm_name
    ctx.new_rgw_multisite.zonegroups = zonegroups
    ctx.new_rgw_multisite.config = config

    master_zonegroup, master_zone, master_clients, target_zone, target_clients = get_config_clients(ctx, config)

    ctx.new_rgw_multisite.master_clients = master_clients
    ctx.new_rgw_multisite.target_clients = target_clients

    ctx.new_rgw_multisite.clients = [master_clients[0], target_clients[0]]

    zone_config = {}

    zgs = ctx.new_rgw_multisite.config['zonegroups']
    for zg in zgs:
        for zone in zg.get('zones'):
            zone_config = zone

    # procedure for migrating from single-site to multisite

    if config.get('migrate', False):

        subtasks = [
            lambda: modify_master(
                ctx=ctx,
                config=config,
                master_zonegroup=master_zonegroup,
                master_zone=master_zone,
                realm=realm_name,
                master_clients=master_clients
            ),
        ]

        subtasks.extend([
            lambda: configure_user_for_client(
                ctx=ctx,
                master_client=master_clients[0]
            ),
        ])

        subtasks.extend([
            lambda: start_rgw(ctx=ctx, on_client=master_clients),
        ])

        subtasks.extend([
            lambda: pull_configuration(ctx=ctx,
                                       realm=realm_name,
                                       master_client=master_clients[0],
                                       target_client=target_clients[0],
                                       ),
        ])

        subtasks.extend([
            lambda: create_zone(ctx=ctx,
                                config=zone_config,
                                target_zone=target_zone,
                                master_zonegroup=master_zonegroup,
                                target_clients=target_clients,
                                ),
        ]),

        subtasks.extend([
            lambda: start_rgw(ctx=ctx,
                              on_client=target_clients),
        ])

        subtasks.extend([
            lambda: check_sync_status(ctx=ctx,
                                      clients=ctx.new_rgw_multisite.clients),
        ])

        with contextutil.nested(*subtasks):
            yield

    else:
        # procedure for creating a new multisite cluster
        subtasks = [
            lambda: configure_master_zonegroup_and_zones(
                ctx=ctx,
                config=config,
                master_zonegroup=master_zonegroup,
                master_zone = master_zone,
                realm=realm_name,
                master_clients=master_clients
            ),
        ]

        subtasks.extend([
            lambda: configure_user_for_client(
                ctx=ctx,
                master_client=master_clients[0]
            ),
        ])

        subtasks.extend([
            lambda: start_rgw(ctx=ctx, on_client=master_clients),
        ])

        subtasks.extend([
            lambda: pull_configuration(ctx=ctx,
                                       realm=realm_name,
                                       master_client=master_clients[0],
                                       target_client=target_clients[0],
                                       ),
        ])

        subtasks.extend([
            lambda: create_zone(ctx=ctx,
                                config=zone_config,
                                target_zone=target_zone,
                                master_zonegroup=master_zonegroup,
                                target_clients=target_clients,
                                ),
        ]),

        subtasks.extend([
            lambda: start_rgw(ctx=ctx,
                              on_client=target_clients),
        ])

        subtasks.extend([
            lambda: check_sync_status(ctx=ctx,
                                      clients=ctx.new_rgw_multisite.clients),
        ])

        with contextutil.nested(*subtasks):
            yield







