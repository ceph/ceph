"""
rgw multisite configuration routines
"""
import argparse
import contextlib
import logging
import random
import string
from copy import deepcopy
from util.rgw import rgwadmin, wait_for_radosgw
from util.rados import create_ec_pool, create_replicated_pool
from rgw_multi import multisite
from rgw_multi.zone_rados import RadosZone as RadosZone

from teuthology.orchestra import run
from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.task import Task

log = logging.getLogger(__name__)

class RGWMultisite(Task):
    """
    Performs rgw multisite configuration to match the given realm definition.

        - rgw-multisite:
            realm:
              name: test-realm
              is_default: true

    List one or more zonegroup definitions. These are provided as json
    input to `radosgw-admin zonegroup set`, with the exception of these keys:

    * 'is_master' is passed on the command line as --master
    * 'is_default' is passed on the command line as --default
    * 'endpoints' given as client names are replaced with actual endpoints

            zonegroups:
              - name: test-zonegroup
                api_name: test-api
                is_master: true
                is_default: true
                endpoints: [c1.client.0]

    List each of the zones to be created in this zonegroup.

                zones:
                  - name: test-zone1
                    is_master: true
                    is_default: true
                    endpoints: [c1.client.0]
                  - name: test-zone2
                    is_default: true
                    endpoints: [c2.client.0]

    A complete example:

        tasks:
        - install:
        - ceph: {cluster: c1}
        - ceph: {cluster: c2}
        - rgw:
            c1.client.0:
            c2.client.0:
        - rgw-multisite:
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
                    endpoints: [c2.client.0]

    """
    def __init__(self, ctx, config):
        super(RGWMultisite, self).__init__(ctx, config)

    def setup(self):
        super(RGWMultisite, self).setup()

        overrides = self.ctx.config.get('overrides', {})
        misc.deep_merge(self.config, overrides.get('rgw-multisite', {}))

        if not self.ctx.rgw:
            raise ConfigError('rgw-multisite must run after the rgw task')
        role_endpoints = self.ctx.rgw.role_endpoints

        # construct Clusters and Gateways for each client in the rgw task
        clusters, gateways = extract_clusters_and_gateways(self.ctx,
                                                           role_endpoints)

        # get the master zone and zonegroup configuration
        mz, mzg = extract_master_zone_zonegroup(self.config['zonegroups'])
        cluster1 = cluster_for_zone(clusters, mz)

        # create the realm and period on the master zone's cluster
        log.info('creating realm..')
        realm = create_realm(cluster1, self.config['realm'])
        period = realm.current_period

        creds = gen_credentials()

        # create the master zonegroup and its master zone
        log.info('creating master zonegroup..')
        master_zonegroup = create_zonegroup(cluster1, gateways, period,
                                            deepcopy(mzg))
        period.master_zonegroup = master_zonegroup

        log.info('creating master zone..')
        master_zone = create_zone(self.ctx, cluster1, gateways, creds,
                                  master_zonegroup, deepcopy(mz))
        master_zonegroup.master_zone = master_zone

        period.update(master_zone, commit=True)
        restart_zone_gateways(master_zone) # restart with --rgw-zone

        # create the admin user on the master zone
        log.info('creating admin user..')
        user_args = ['--display-name', 'Realm Admin', '--system']
        user_args += creds.credential_args()
        admin_user = multisite.User('realm-admin')
        admin_user.create(master_zone, user_args)

        # process 'zonegroups'
        for zg_config in self.config['zonegroups']:
            zones_config = zg_config.pop('zones')

            zonegroup = None
            for zone_config in zones_config:
                # get the cluster for this zone
                cluster = cluster_for_zone(clusters, zone_config)

                if cluster != cluster1: # already created on master cluster
                    log.info('pulling realm configuration to %s', cluster.name)
                    realm.pull(cluster, master_zone.gateways[0], creds)

                # use the first zone's cluster to create the zonegroup
                if not zonegroup:
                    if zg_config['name'] == master_zonegroup.name:
                        zonegroup = master_zonegroup
                    else:
                        log.info('creating zonegroup..')
                        zonegroup = create_zonegroup(cluster, gateways,
                                                     period, zg_config)

                if zone_config['name'] == master_zone.name:
                    # master zone was already created
                    zone = master_zone
                else:
                    # create the zone and commit the period
                    log.info('creating zone..')
                    zone = create_zone(self.ctx, cluster, gateways, creds,
                                       zonegroup, zone_config)
                    period.update(zone, commit=True)

                    restart_zone_gateways(zone) # restart with --rgw-zone

        # attach configuration to the ctx for other tasks
        self.ctx.rgw_multisite = argparse.Namespace()
        self.ctx.rgw_multisite.clusters = clusters
        self.ctx.rgw_multisite.gateways = gateways
        self.ctx.rgw_multisite.realm = realm
        self.ctx.rgw_multisite.admin_user = admin_user

        log.info('rgw multisite configuration completed')

    def end(self):
        del self.ctx.rgw_multisite

class Cluster(multisite.Cluster):
    """ Issues 'radosgw-admin' commands with the rgwadmin() helper """
    def __init__(self, ctx, name, client):
        super(Cluster, self).__init__()
        self.ctx = ctx
        self.name = name
        self.client = client

    def admin(self, args = None, **kwargs):
        """ radosgw-admin command """
        args = args or []
        args += ['--cluster', self.name]
        args += ['--debug-rgw', str(kwargs.pop('debug_rgw', 0))]
        args += ['--debug-ms', str(kwargs.pop('debug_ms', 0))]
        if kwargs.pop('read_only', False):
            args += ['--rgw-cache-enabled', 'false']
        kwargs['decode'] = False
        check_retcode = kwargs.pop('check_retcode', True)
        r, s = rgwadmin(self.ctx, self.client, args, **kwargs)
        if check_retcode:
            assert r == 0
        return s, r

class Gateway(multisite.Gateway):
    """ Controls a radosgw instance using its daemon """
    def __init__(self, role, remote, daemon, *args, **kwargs):
        super(Gateway, self).__init__(*args, **kwargs)
        self.role = role
        self.remote = remote
        self.daemon = daemon

    def set_zone(self, zone):
        """ set the zone and add its args to the daemon's command line """
        assert self.zone is None, 'zone can only be set once'
        self.zone = zone
        # daemon.restart_with_args() would be perfect for this, except that
        # radosgw args likely include a pipe and redirect. zone arguments at
        # the end won't actually apply to radosgw
        args = self.daemon.command_kwargs.get('args', [])
        try:
            # insert zone args before the first |
            pipe = args.index(run.Raw('|'))
            args = args[0:pipe] + zone.zone_args() + args[pipe:]
        except ValueError, e:
            args += zone.zone_args()
        self.daemon.command_kwargs['args'] = args

    def start(self, args = None):
        """ (re)start the daemon """
        self.daemon.restart()
        # wait until startup completes
        wait_for_radosgw(self.endpoint())

    def stop(self):
        """ stop the daemon """
        self.daemon.stop()

def extract_clusters_and_gateways(ctx, role_endpoints):
    """ create cluster and gateway instances for all of the radosgw roles """
    clusters = {}
    gateways = {}
    for role, endpoint in role_endpoints.iteritems():
        cluster_name, daemon_type, client_id = misc.split_role(role)
        # find or create the cluster by name
        cluster = clusters.get(cluster_name)
        if not cluster:
            clusters[cluster_name] = cluster = Cluster(ctx, cluster_name, role)
        # create a gateway for this daemon
        client_with_id = daemon_type + '.' + client_id # match format from rgw.py
        daemon = ctx.daemons.get_daemon('rgw', client_with_id, cluster_name)
        if not daemon:
            raise ConfigError('no daemon for role=%s cluster=%s type=rgw id=%s' % \
                              (role, cluster_name, client_id))
        (remote,) = ctx.cluster.only(role).remotes.keys()
        gateways[role] = Gateway(role, remote, daemon, endpoint.hostname,
                endpoint.port, cluster)
    return clusters, gateways

def create_realm(cluster, config):
    """ create a realm from configuration and initialize its first period """
    realm = multisite.Realm(config['name'])
    args = []
    if config.get('is_default', False):
        args += ['--default']
    realm.create(cluster, args)
    realm.current_period = multisite.Period(realm)
    return realm

def extract_user_credentials(config):
    """ extract keys from configuration """
    return multisite.Credentials(config['access_key'], config['secret_key'])

def extract_master_zone(zonegroup_config):
    """ find and return the master zone definition """
    master = None
    for zone in zonegroup_config['zones']:
        if not zone.get('is_master', False):
            continue
        if master:
            raise ConfigError('zones %s and %s cannot both set \'is_master\'' % \
                              (master['name'], zone['name']))
        master = zone
        # continue the loop so we can detect duplicates
    if not master:
        raise ConfigError('one zone must set \'is_master\' in zonegroup %s' % \
                          zonegroup_config['name'])
    return master

def extract_master_zone_zonegroup(zonegroups_config):
    """ find and return the master zone and zonegroup definitions """
    master_zone, master_zonegroup = (None, None)
    for zonegroup in zonegroups_config:
        # verify that all zonegroups have a master zone set, even if they
        # aren't in the master zonegroup
        zone = extract_master_zone(zonegroup)
        if not zonegroup.get('is_master', False):
            continue
        if master_zonegroup:
            raise ConfigError('zonegroups %s and %s cannot both set \'is_master\'' % \
                              (master_zonegroup['name'], zonegroup['name']))
        master_zonegroup = zonegroup
        master_zone = zone
        # continue the loop so we can detect duplicates
    if not master_zonegroup:
        raise ConfigError('one zonegroup must set \'is_master\'')
    return master_zone, master_zonegroup

def extract_zone_cluster_name(zone_config):
    """ return the cluster (must be common to all zone endpoints) """
    cluster_name = None
    endpoints = zone_config.get('endpoints')
    if not endpoints:
        raise ConfigError('zone %s missing \'endpoints\' list' % \
                          zone_config['name'])
    for role in endpoints:
        name, _, _ = misc.split_role(role)
        if not cluster_name:
            cluster_name = name
        elif cluster_name != name:
            raise ConfigError('all zone %s endpoints must be in the same cluster' % \
                              zone_config['name'])
    return cluster_name

def cluster_for_zone(clusters, zone_config):
    """ return the cluster entry for the given zone """
    name = extract_zone_cluster_name(zone_config)
    try:
        return clusters[name]
    except KeyError:
        raise ConfigError('no cluster %s found' % name)

def gen_access_key():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

def gen_secret():
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(32))

def gen_credentials():
    return multisite.Credentials(gen_access_key(), gen_secret())

def extract_gateway_endpoints(gateways, endpoints_config):
    """ return a list of gateway endpoints associated with the given roles """
    endpoints = []
    for role in endpoints_config:
        try:
            # replace role names with their gateway's endpoint
            endpoints.append(gateways[role].endpoint())
        except KeyError:
            raise ConfigError('no radosgw endpoint found for role %s' % role)
    return endpoints

def is_default_arg(config):
    return ['--default'] if config.pop('is_default', False) else []

def is_master_arg(config):
    return ['--master'] if config.pop('is_master', False) else []

def create_zonegroup(cluster, gateways, period, config):
    """ pass the zonegroup configuration to `zonegroup set` """
    config.pop('zones', None) # remove 'zones' from input to `zonegroup set`
    endpoints = config.get('endpoints')
    if endpoints:
        # replace client names with their gateway endpoints
        config['endpoints'] = extract_gateway_endpoints(gateways, endpoints)
    zonegroup = multisite.ZoneGroup(config['name'], period)
    # `zonegroup set` needs --default on command line, and 'is_master' in json
    args = is_default_arg(config)
    zonegroup.set(cluster, config, args)
    period.zonegroups.append(zonegroup)
    return zonegroup

def create_zone(ctx, cluster, gateways, creds, zonegroup, config):
    """ create a zone with the given configuration """
    zone = multisite.Zone(config['name'], zonegroup, cluster)
    zone = RadosZone(config['name'], zonegroup, cluster)

    # collect Gateways for the zone's endpoints
    endpoints = config.get('endpoints')
    if not endpoints:
        raise ConfigError('no \'endpoints\' for zone %s' % config['name'])
    zone.gateways = [gateways[role] for role in endpoints]
    for gateway in zone.gateways:
        gateway.set_zone(zone)

    # format the gateway endpoints
    endpoints = [g.endpoint() for g in zone.gateways]

    args = is_default_arg(config)
    args += is_master_arg(config)
    args += creds.credential_args()
    if len(endpoints):
        args += ['--endpoints', ','.join(endpoints)]
    zone.create(cluster, args)
    zonegroup.zones.append(zone)

    create_zone_pools(ctx, zone)
    if ctx.rgw.compression_type:
        configure_zone_compression(zone, ctx.rgw.compression_type)

    zonegroup.zones_by_type.setdefault(zone.tier_type(), []).append(zone)

    if zone.is_read_only():
        zonegroup.ro_zones.append(zone)
    else:
        zonegroup.rw_zones.append(zone)

    return zone

def create_zone_pools(ctx, zone):
    """ Create the data_pool for each placement type """
    gateway = zone.gateways[0]
    cluster = zone.cluster
    for pool_config in zone.data.get('placement_pools', []):
        pool_name = pool_config['val']['storage_classes']['STANDARD']['data_pool']
        if ctx.rgw.ec_data_pool:
            create_ec_pool(gateway.remote, pool_name, zone.name, 64,
                           ctx.rgw.erasure_code_profile, cluster.name, 'rgw')
        else:
            create_replicated_pool(gateway.remote, pool_name, 64, cluster.name, 'rgw')

def configure_zone_compression(zone, compression):
    """ Set compression type in the zone's default-placement """
    zone.json_command(zone.cluster, 'placement', ['modify',
                          '--placement-id', 'default-placement',
                          '--compression', compression
                      ])

def restart_zone_gateways(zone):
    zone.stop()
    zone.start()

task = RGWMultisite
