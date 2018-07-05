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

log = logging.getLogger(__name__)

access_key = None
secret = None

def rgwadmin(ctx, client, cmd, stdin=StringIO(), check_status=False,
             format='json', decode=True, log_level=logging.DEBUG):
    log.info('rgwadmin: {client} : {cmd}'.format(client=client,cmd=cmd))
    testdir = teuthology.get_testdir(ctx)
    cluster_name, daemon_type, client_id = teuthology.split_role(client)
    pre = ['sudo',
        'radosgw-admin'.format(tdir=testdir),
        '--log-to-stderr',
        '--cluster', cluster_name,
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
        return (r, out)
    j = None
    if not r and out != '':
        try:
            j = json.loads(out)
            log.log(log_level, ' json result: %s' % j)
        except ValueError:
            j = out
            log.log(log_level, ' raw result: %s' % j)
    return (r, j)


def extract_endpoints(ctx, role):

    port = 8080
    role_endpoints = {}
    remote,  = ctx.cluster.only(role).remotes.iterkeys()
    role_endpoints[role] = (remote.name.split('@')[1], port)
    log.info('Endpoints are {role_endpoints}'.format(role_endpoints=role_endpoints))

    return role_endpoints


def get_config_clients(ctx, config):

    master_zonegroup = None
    master_zone = None
    master_client = None
    target_zone = None
    target_client = None

    zonegroups_config = config['zonegroups']
    for zonegroup_config in zonegroups_config:
        if zonegroup_config.get('is_master', False):
            master_zonegroup = zonegroup_config.get('name')
        for zone in zonegroup_config['zones']:
            if zone.get('is_master', False):
                mz_config = zone
                master_zone = mz_config.get('name')
                master_client = mz_config.get('endpoints')[0]
            else:
                tz_config = zone
                target_zone = tz_config.get('name')
                target_client = tz_config.get('endpoints')[0]

    return master_zonegroup, master_zone, master_client, target_zone, target_client


def gen_access_key():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))


def gen_secret():
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(32))


def wait_for_radosgw(ctx, client):

    role_endpoints = extract_endpoints(ctx, client)
    host, port = role_endpoints[client]
    url = "http://%s:%d" % (host, port)
    http = PoolManager(retries=Retry(connect=8, backoff_factor=1))
    http.request('GET', url)


@contextlib.contextmanager
def configure_master_zonegroup_and_zones(ctx, config, master_zonegroup, master_zone, realm, master_client):

    """ Create zonegroup and zone on master"""
    global access_key, secret
    access_key = gen_access_key()
    secret = gen_secret()

    role_endpoints  = extract_endpoints(ctx, master_client)
    host, port = role_endpoints[master_client]

    endpoint = 'http://{host}:{port}'.format(host=host, port=port)
    log.debug("endpoint: %s", endpoint)

    log.info('creating master zonegroup and zone on {}'.format(master_client))
    rgwadmin(ctx, master_client,
             cmd=['realm', 'create', '--rgw-realm', realm, '--default'],
             check_status=True)

    rgwadmin(ctx, master_client,
             cmd=['zonegroup', 'create', '--rgw-zonegroup', master_zonegroup, '--master', '--endpoints', endpoint,
                  '--default'], check_status=True)

    rgwadmin(ctx, master_client,
             cmd=['zone', 'create', '--rgw-zonegroup', master_zonegroup,
                  '--rgw-zone', master_zone, '--endpoints', endpoint, '--access-key',
                  access_key, '--secret',
                  secret, '--master', '--default'],
             check_status=True)

    rgwadmin(ctx, master_client,
             cmd=['period', 'update', '--commit'],
             check_status=True)

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

    role_endpoints = extract_endpoints(ctx, master_client)
    host, port = role_endpoints[master_client]

    endpoint = 'http://{host}:{port}'.format(host=host, port=port)
    log.debug("endpoint: %s", endpoint)

    log.info('Pulling master config information from {}'.format(master_client))
    rgwadmin(ctx, target_client,
             cmd=['realm', 'pull', '--url',
                  endpoint, '--access_key',
                  access_key, '--secret',
                  secret],
            check_status=True)

    rgwadmin(ctx, target_client,
             cmd=['realm', 'default', '--rgw-realm', realm])

    rgwadmin(ctx, target_client,
             cmd=['period', 'pull', '--url', endpoint, '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    yield


@contextlib.contextmanager
def configure_target_zone(ctx, config, target_zone, master_zonegroup, target_client):

    role_endpoints  = extract_endpoints(ctx, target_client)
    host, port = role_endpoints[target_client]

    endpoint = 'http://{host}:{port}'.format(host=host, port=port)
    log.debug("endpoint: %s", endpoint)

    log.info('creating zone on {}'.format(target_client))

    zone_config = {}

    zgs = ctx.new_rgw_multisite.config['zonegroups']
    for zg in zgs:
        for zone in zg.get('zones'):
            zone_config = zone

    if zone_config.get('is_read_only', False):
        rgwadmin(ctx, target_client,
                 cmd=['zone', 'create', '--rgw-zonegroup', master_zonegroup,
                      '--rgw-zone', target_zone, '--endpoints', endpoint, '--access-key',
                      access_key, '--secret',
                      secret, '--default', '--read-only'],
                 check_status=True)
    else:
        rgwadmin(ctx, target_client,
                 cmd=['zone', 'create', '--rgw-zonegroup', master_zonegroup,
                      '--rgw-zone', target_zone, '--endpoints', endpoint, '--access-key',
                      access_key, '--secret',
                      secret, '--default'],
                 check_status=True)

    rgwadmin(ctx, target_client,
             cmd=['period', 'update', '--commit',
                  '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)


    yield


@contextlib.contextmanager
def restart_rgw(ctx, on_client):

    log.info('Restarting rgw...')
    log.debug('client %r', on_client)
    (remote,) = ctx.cluster.only(on_client).remotes.iterkeys()
    hostname = remote.name.split('@')[1].split('.')[0]
    rgw_cmd = [
        'sudo', 'systemctl', 'restart', 'ceph-radosgw@rgw.{hostname}'.format(hostname=hostname)]

    run_cmd = list(rgw_cmd)
    remote.run(args=run_cmd)

    wait_for_radosgw(ctx, on_client)

    yield


@contextlib.contextmanager
def failover(ctx, config):
    """
    - new-rgw-multisite.failover:

    """
    # When master is down, bring up secondary as the master zone

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    master_zonegroup, master_zone, master_client, target_zone, target_client = \
        get_config_clients(ctx, ctx.new_rgw_multisite.config)

    # Make secondary zone master
    rgwadmin(ctx, target_client,
             cmd=['zone', 'modify', '--rgw-zone', target_zone, '--master', '--default', '--access-key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Do period commit
    rgwadmin(ctx, target_client,
             cmd=['period', 'update', '--commit',
                  '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Restart gateway

    restart_rgw(ctx, target_client)

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

    master_zonegroup, master_zone, master_client, target_zone, target_client = \
        get_config_clients(ctx, ctx.new_rgw_multisite.config)

    role_endpoints  = extract_endpoints(ctx, target_client)
    host, port = role_endpoints[target_client]

    endpoint = 'http://{host}:{port}'.format(host=host, port=port)

    # Period pull in former master zone from current master zone

    rgwadmin(ctx, master_client,
             cmd=['period', 'pull', '--url', endpoint, '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Make the original master zone as master

    rgwadmin(ctx, master_client,
             cmd=['zone', 'modify', '--rgw-zone', master_zone, '--master', '--default', '--access-key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Do period commit

    rgwadmin(ctx, master_client,
             cmd=['period', 'update', '--commit',
                  '--access_key',
                  access_key, '--secret',
                  secret],
             check_status=True)

    # Restart gateway

    restart_rgw(ctx, master_client)

    # If secondary zone was read-only before failover, explicitly set it to --read-only again.

    zone_config = {}

    zgs = ctx.new_rgw_multisite.config['zonegroups']
    for zg in zgs:
        for zone in zg.get('zones'):
            zone_config = zone

    if zone_config.get('is_read_only', False):
        rgwadmin(ctx, target_client,
                 cmd=['zone', 'modify', '--rgw-zone', target_zone, '--read-only', '--access-key',
                      access_key, '--secret',
                      secret],
                 check_status=True)

        # Do period commit
        rgwadmin(ctx, target_client,
                 cmd=['period', 'update', '--commit',
                      '--access_key',
                      access_key, '--secret',
                      secret],
                 check_status=True)

        # Restart gateway

        restart_rgw(ctx, target_client)

    yield


@contextlib.contextmanager
def task(ctx, config):

    """
    - new-multisite:
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
    ctx.new_rgw_multisite.zonegroups = zonegroups
    ctx.new_rgw_multisite.config = config

    master_zonegroup, master_zone, master_client, target_zone, target_client = get_config_clients(ctx, config)

    ctx.new_rgw_multisite.master_client = master_client
    ctx.new_rgw_multisite.target_client = target_client

    subtasks = [
        lambda: configure_master_zonegroup_and_zones(
            ctx=ctx,
            config=config,
            master_zonegroup=master_zonegroup,
            master_zone = master_zone,
            realm=realm_name,
            master_client=master_client
        ),
    ]

    subtasks.extend([
        lambda: configure_user_for_client(
            ctx=ctx,
            master_client=master_client
        ),
    ])

    subtasks.extend([
        lambda: restart_rgw(ctx=ctx, on_client=master_client),
    ])

    subtasks.extend([
        lambda: pull_configuration(ctx=ctx,
                                   realm=realm_name,
                                   master_client=master_client,
                                   target_client=target_client,
                                   ),
    ])

    subtasks.extend([
        lambda: configure_target_zone(ctx=ctx,
                                      config=config,
                                      target_zone=target_zone,
                                      master_zonegroup=master_zonegroup,
                                      target_client=target_client,
                                      ),
    ]),

    subtasks.extend([
        lambda: restart_rgw(ctx=ctx,
                            on_client=target_client),
    ])

    with contextutil.nested(*subtasks):
        yield







