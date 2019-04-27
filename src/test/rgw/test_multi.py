import subprocess
import os
import random
import string
import argparse
import sys
import logging
try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import nose.core

from rgw_multi import multisite
from rgw_multi.zone_rados import RadosZone as RadosZone
from rgw_multi.zone_es import ESZone as ESZone
from rgw_multi.zone_es import ESZoneConfig as ESZoneConfig
from rgw_multi.zone_cloud import CloudZone as CloudZone
from rgw_multi.zone_cloud import CloudZoneConfig as CloudZoneConfig
from rgw_multi.zone_ps import PSZone as PSZone

# make tests from rgw_multi.tests available to nose
from rgw_multi.tests import *
from rgw_multi.tests_es import *
from rgw_multi.tests_ps import *

mstart_path = os.getenv('MSTART_PATH')
if mstart_path is None:
    mstart_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__)) + '/../..') + '/'

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/'

# configure logging for the tests module
log = logging.getLogger('rgw_multi.tests')

def bash(cmd, **kwargs):
    log.debug('running cmd: %s', ' '.join(cmd))
    check_retcode = kwargs.pop('check_retcode', True)
    kwargs['stdout'] = subprocess.PIPE
    process = subprocess.Popen(cmd, **kwargs)
    s = process.communicate()[0]
    log.debug('command returned status=%d stdout=%s', process.returncode, s.decode('utf-8'))
    if check_retcode:
        assert(process.returncode == 0)
    return (s, process.returncode)

class Cluster(multisite.Cluster):
    """ cluster implementation based on mstart/mrun scripts """
    def __init__(self, cluster_id):
        super(Cluster, self).__init__()
        self.cluster_id = cluster_id
        self.needs_reset = True

    def admin(self, args = None, **kwargs):
        """ radosgw-admin command """
        cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', self.cluster_id]
        if args:
            cmd += args
        cmd += ['--debug-rgw', str(kwargs.pop('debug_rgw', 0))]
        cmd += ['--debug-ms', str(kwargs.pop('debug_ms', 0))]
        if kwargs.pop('read_only', False):
            cmd += ['--rgw-cache-enabled', 'false']
        return bash(cmd, **kwargs)

    def start(self):
        cmd = [mstart_path + 'mstart.sh', self.cluster_id]
        env = None
        if self.needs_reset:
            env = os.environ.copy()
            env['CEPH_NUM_MDS'] = '0'
            cmd += ['-n']
        bash(cmd, env=env)
        self.needs_reset = False

    def stop(self):
        cmd = [mstart_path + 'mstop.sh', self.cluster_id]
        bash(cmd)

class Gateway(multisite.Gateway):
    """ gateway implementation based on mrgw/mstop scripts """
    def __init__(self, client_id = None, *args, **kwargs):
        super(Gateway, self).__init__(*args, **kwargs)
        self.id = client_id

    def start(self, args = None):
        """ start the gateway """
        assert(self.cluster)
        cmd = [mstart_path + 'mrgw.sh', self.cluster.cluster_id, str(self.port)]
        if self.id:
            cmd += ['-i', self.id]
        cmd += ['--debug-rgw=20', '--debug-ms=1']
        if args:
            cmd += args
        bash(cmd)

    def stop(self):
        """ stop the gateway """
        assert(self.cluster)
        cmd = [mstart_path + 'mstop.sh', self.cluster.cluster_id, 'radosgw', self.id]
        bash(cmd)

def gen_access_key():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

def gen_secret():
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(32))

def gen_credentials():
    return multisite.Credentials(gen_access_key(), gen_secret())

def cluster_name(cluster_num):
    return 'c' + str(cluster_num)

def zonegroup_name(zonegroup_num):
    return string.ascii_lowercase[zonegroup_num]

def zone_name(zonegroup_num, zone_num):
    return zonegroup_name(zonegroup_num) + str(zone_num + 1)

def gateway_port(zonegroup_num, gateway_num):
    return 8000 + 100 * zonegroup_num + gateway_num

def gateway_name(zonegroup_num, zone_num, gateway_num):
    return zone_name(zonegroup_num, zone_num) + '-' + str(gateway_num + 1)

def zone_endpoints(zonegroup_num, zone_num, gateways_per_zone):
    endpoints = []
    base = gateway_port(zonegroup_num, zone_num * gateways_per_zone)
    for i in range(0, gateways_per_zone):
        endpoints.append('http://localhost:' + str(base + i))
    return endpoints

def get_log_level(log_level):
    if log_level >= 20:
        return logging.DEBUG
    if log_level >= 10:
        return logging.INFO
    if log_level >= 5:
        return logging.WARN
    if log_level >= 1:
        return logging.ERROR
    return logging.CRITICAL

def setup_logging(log_level_console, log_file, log_level_file):
    if log_file:
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        fh.setLevel(get_log_level(log_level_file))
        log.addHandler(fh)

    formatter = logging.Formatter('%(levelname)s %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(get_log_level(log_level_console))
    log.addHandler(ch)
    log.setLevel(get_log_level(log_level_console))

def init(parse_args):
    cfg = configparser.RawConfigParser({
                                         'num_zonegroups': 1,
                                         'num_zones': 3,
                                         'num_ps_zones': 0,
                                         'gateways_per_zone': 2,
                                         'no_bootstrap': 'false',
                                         'log_level': 20,
                                         'log_file': None,
                                         'file_log_level': 20,
                                         'tenant': None,
                                         'checkpoint_retries': 60,
                                         'checkpoint_delay': 5,
                                         'reconfigure_delay': 5,
                                         })
    try:
        path = os.environ['RGW_MULTI_TEST_CONF']
    except KeyError:
        path = test_path + 'test_multi.conf'

    try:
        with open(path) as f:
            cfg.readfp(f)
    except:
        print('WARNING: error reading test config. Path can be set through the RGW_MULTI_TEST_CONF env variable')
        pass

    parser = argparse.ArgumentParser(
            description='Run rgw multi-site tests',
            usage='test_multi [--num-zonegroups <num>] [--num-zones <num>] [--no-bootstrap]')

    section = 'DEFAULT'
    parser.add_argument('--num-zonegroups', type=int, default=cfg.getint(section, 'num_zonegroups'))
    parser.add_argument('--num-zones', type=int, default=cfg.getint(section, 'num_zones'))
    parser.add_argument('--gateways-per-zone', type=int, default=cfg.getint(section, 'gateways_per_zone'))
    parser.add_argument('--no-bootstrap', action='store_true', default=cfg.getboolean(section, 'no_bootstrap'))
    parser.add_argument('--log-level', type=int, default=cfg.getint(section, 'log_level'))
    parser.add_argument('--log-file', type=str, default=cfg.get(section, 'log_file'))
    parser.add_argument('--file-log-level', type=int, default=cfg.getint(section, 'file_log_level'))
    parser.add_argument('--tenant', type=str, default=cfg.get(section, 'tenant'))
    parser.add_argument('--checkpoint-retries', type=int, default=cfg.getint(section, 'checkpoint_retries'))
    parser.add_argument('--checkpoint-delay', type=int, default=cfg.getint(section, 'checkpoint_delay'))
    parser.add_argument('--reconfigure-delay', type=int, default=cfg.getint(section, 'reconfigure_delay'))
    parser.add_argument('--num-ps-zones', type=int, default=cfg.getint(section, 'num_ps_zones'))

    es_cfg = []
    cloud_cfg = []

    for s in cfg.sections():
        if s.startswith('elasticsearch'):
            es_cfg.append(ESZoneConfig(cfg, s))
        elif s.startswith('cloud'):
            cloud_cfg.append(CloudZoneConfig(cfg, s))

    argv = []

    if parse_args:
        argv = sys.argv[1:]

    args = parser.parse_args(argv)
    bootstrap = not args.no_bootstrap

    setup_logging(args.log_level, args.log_file, args.file_log_level)

    # start first cluster
    c1 = Cluster(cluster_name(1))
    if bootstrap:
        c1.start()
    clusters = []
    clusters.append(c1)

    admin_creds = gen_credentials()
    admin_user = multisite.User('zone.user')

    user_creds = gen_credentials()
    user = multisite.User('tester')

    realm = multisite.Realm('r')
    if bootstrap:
        # create the realm on c1
        realm.create(c1)
    else:
        realm.get(c1)
    period = multisite.Period(realm=realm)
    realm.current_period = period

    num_es_zones = len(es_cfg)
    num_cloud_zones = len(cloud_cfg)

    num_zones = args.num_zones + num_es_zones + num_cloud_zones + args.num_ps_zones

    for zg in range(0, args.num_zonegroups):
        zonegroup = multisite.ZoneGroup(zonegroup_name(zg), period)
        period.zonegroups.append(zonegroup)

        is_master_zg = zg == 0
        if is_master_zg:
            period.master_zonegroup = zonegroup

        for z in range(0, num_zones):
            is_master = z == 0
            # start a cluster, or use c1 for first zone
            cluster = None
            if is_master_zg and is_master:
                cluster = c1
            else:
                cluster = Cluster(cluster_name(len(clusters) + 1))
                clusters.append(cluster)
                if bootstrap:
                    cluster.start()
                    # pull realm configuration from the master's gateway
                    gateway = realm.meta_master_zone().gateways[0]
                    realm.pull(cluster, gateway, admin_creds)

            endpoints = zone_endpoints(zg, z, args.gateways_per_zone)
            if is_master:
                if bootstrap:
                    # create the zonegroup on its first zone's cluster
                    arg = []
                    if is_master_zg:
                        arg += ['--master']
                    if len(endpoints): # use master zone's endpoints
                        arg += ['--endpoints', ','.join(endpoints)]
                    zonegroup.create(cluster, arg)
                else:
                    zonegroup.get(cluster)

            es_zone = (z >= args.num_zones and z < args.num_zones + num_es_zones)
            cloud_zone = (z >= args.num_zones + num_es_zones and z < args.num_zones + num_es_zones + num_cloud_zones)
            ps_zone = (z >= args.num_zones + num_es_zones + num_cloud_zones)

            # create the zone in its zonegroup
            zone = multisite.Zone(zone_name(zg, z), zonegroup, cluster)
            if es_zone:
                zone_index = z - args.num_zones
                zone = ESZone(zone_name(zg, z), es_cfg[zone_index].endpoint, zonegroup, cluster)
            elif cloud_zone:
                zone_index = z - args.num_zones - num_es_zones
                ccfg = cloud_cfg[zone_index]
                zone = CloudZone(zone_name(zg, z), ccfg.endpoint, ccfg.credentials, ccfg.source_bucket,
                                 ccfg.target_path, zonegroup, cluster)
            elif ps_zone:
                zone_index = z - args.num_zones - num_es_zones - num_cloud_zones
                zone = PSZone(zone_name(zg, z), zonegroup, cluster)
            else:
                zone = RadosZone(zone_name(zg, z), zonegroup, cluster)

            if bootstrap:
                arg = admin_creds.credential_args()
                if is_master:
                    arg += ['--master']
                if len(endpoints):
                    arg += ['--endpoints', ','.join(endpoints)]
                zone.create(cluster, arg)
            else:
                zone.get(cluster)
            zonegroup.zones.append(zone)
            if is_master:
                zonegroup.master_zone = zone

            zonegroup.zones_by_type.setdefault(zone.tier_type(), []).append(zone)

            if zone.is_read_only():
                zonegroup.ro_zones.append(zone)
            else:
                zonegroup.rw_zones.append(zone)

            # update/commit the period
            if bootstrap:
                period.update(zone, commit=True)

            # start the gateways
            for g in range(0, args.gateways_per_zone):
                port = gateway_port(zg, g + z * args.gateways_per_zone)
                client_id = gateway_name(zg, z, g)
                gateway = Gateway(client_id, 'localhost', port, cluster, zone)
                if bootstrap:
                    gateway.start()
                zone.gateways.append(gateway)

            if is_master_zg and is_master:
                if bootstrap:
                    # create admin user
                    arg = ['--display-name', '"Zone User"', '--system']
                    arg += admin_creds.credential_args()
                    admin_user.create(zone, arg)
                    # create test user
                    arg = ['--display-name', '"Test User"']
                    arg += user_creds.credential_args()
                    if args.tenant:
                        cmd += ['--tenant', args.tenant]
                    user.create(zone, arg)
                else:
                    # read users and update keys
                    admin_user.info(zone)
                    admin_creds = admin_user.credentials[0]
                    user.info(zone)
                    user_creds = user.credentials[0]

    if not bootstrap:
        period.get(c1)

    config = Config(checkpoint_retries=args.checkpoint_retries,
                    checkpoint_delay=args.checkpoint_delay,
                    reconfigure_delay=args.reconfigure_delay)
    init_multi(realm, user, config)

def setup_module():
    init(False)

if __name__ == "__main__":
    init(True)

