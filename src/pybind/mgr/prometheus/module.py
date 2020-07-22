import cherrypy
from distutils.version import StrictVersion
import json
import errno
import math
import os
import re
import socket
import threading
import time
from mgr_module import MgrModule, MgrStandbyModule, CommandResult, PG_STATES
from mgr_util import get_default_addr, profile_method
from rbd import RBD
try:
    from typing import Optional, Dict, Any, Set
except:
    pass

# Defaults for the Prometheus HTTP server.  Can also set in config-key
# see https://github.com/prometheus/prometheus/wiki/Default-port-allocations
# for Prometheus exporter port registry

DEFAULT_PORT = 9283

# When the CherryPy server in 3.2.2 (and later) starts it attempts to verify
# that the ports its listening on are in fact bound. When using the any address
# "::" it tries both ipv4 and ipv6, and in some environments (e.g. kubernetes)
# ipv6 isn't yet configured / supported and CherryPy throws an uncaught
# exception.
if cherrypy is not None:
    v = StrictVersion(cherrypy.__version__)
    # the issue was fixed in 3.2.3. it's present in 3.2.2 (current version on
    # centos:7) and back to at least 3.0.0.
    if StrictVersion("3.1.2") <= v < StrictVersion("3.2.3"):
        # https://github.com/cherrypy/cherrypy/issues/1100
        from cherrypy.process import servers
        servers.wait_for_occupied_port = lambda host, port: None


# cherrypy likes to sys.exit on error.  don't let it take us down too!
def os_exit_noop(*args, **kwargs):
    pass


os._exit = os_exit_noop

# to access things in class Module from subclass Root.  Because
# it's a dict, the writer doesn't need to declare 'global' for access

_global_instance = None  # type: Optional[Module]


def health_status_to_number(status):
    if status == 'HEALTH_OK':
        return 0
    elif status == 'HEALTH_WARN':
        return 1
    elif status == 'HEALTH_ERR':
        return 2


DF_CLUSTER = ['total_bytes', 'total_used_bytes', 'total_used_raw_bytes']

DF_POOL = ['max_avail', 'stored', 'stored_raw', 'objects', 'dirty',
           'quota_bytes', 'quota_objects', 'rd', 'rd_bytes', 'wr', 'wr_bytes']

OSD_POOL_STATS = ('recovering_objects_per_sec', 'recovering_bytes_per_sec',
                  'recovering_keys_per_sec', 'num_objects_recovered',
                  'num_bytes_recovered', 'num_bytes_recovered')

OSD_FLAGS = ('noup', 'nodown', 'noout', 'noin', 'nobackfill', 'norebalance',
             'norecover', 'noscrub', 'nodeep-scrub')

FS_METADATA = ('data_pools', 'fs_id', 'metadata_pool', 'name')

MDS_METADATA = ('ceph_daemon', 'fs_id', 'hostname', 'public_addr', 'rank',
                'ceph_version')

MON_METADATA = ('ceph_daemon', 'hostname',
                'public_addr', 'rank', 'ceph_version')

MGR_METADATA = ('ceph_daemon', 'hostname', 'ceph_version')

MGR_STATUS = ('ceph_daemon',)

MGR_MODULE_STATUS = ('name',)

MGR_MODULE_CAN_RUN = ('name',)

OSD_METADATA = ('back_iface', 'ceph_daemon', 'cluster_addr', 'device_class',
                'front_iface', 'hostname', 'objectstore', 'public_addr',
                'ceph_version')

OSD_STATUS = ['weight', 'up', 'in']

OSD_STATS = ['apply_latency_ms', 'commit_latency_ms']

POOL_METADATA = ('pool_id', 'name')

RGW_METADATA = ('ceph_daemon', 'hostname', 'ceph_version')

RBD_MIRROR_METADATA = ('ceph_daemon', 'id', 'instance_id', 'hostname',
                       'ceph_version')

DISK_OCCUPATION = ('ceph_daemon', 'device', 'db_device',
                   'wal_device', 'instance', 'devices', 'device_ids')

NUM_OBJECTS = ['degraded', 'misplaced', 'unfound']


class Metric(object):
    def __init__(self, mtype, name, desc, labels=None):
        self.mtype = mtype
        self.name = name
        self.desc = desc
        self.labelnames = labels    # tuple if present
        self.value = {}             # indexed by label values

    def clear(self):
        self.value = {}

    def set(self, value, labelvalues=None):
        # labelvalues must be a tuple
        labelvalues = labelvalues or ('',)
        self.value[labelvalues] = value

    def str_expfmt(self):

        def promethize(path):
            ''' replace illegal metric name characters '''
            result = re.sub(r'[./\s]|::', '_', path).replace('+', '_plus')

            # Hyphens usually turn into underscores, unless they are
            # trailing
            if result.endswith("-"):
                result = result[0:-1] + "_minus"
            else:
                result = result.replace("-", "_")

            return "ceph_{0}".format(result)

        def floatstr(value):
            ''' represent as Go-compatible float '''
            if value == float('inf'):
                return '+Inf'
            if value == float('-inf'):
                return '-Inf'
            if math.isnan(value):
                return 'NaN'
            return repr(float(value))

        name = promethize(self.name)
        expfmt = '''
# HELP {name} {desc}
# TYPE {name} {mtype}'''.format(
            name=name,
            desc=self.desc,
            mtype=self.mtype,
        )

        for labelvalues, value in self.value.items():
            if self.labelnames:
                labels_list = zip(self.labelnames, labelvalues)
                labels = ','.join('%s="%s"' % (k, v) for k, v in labels_list)
            else:
                labels = ''
            if labels:
                fmtstr = '\n{name}{{{labels}}} {value}'
            else:
                fmtstr = '\n{name} {value}'
            expfmt += fmtstr.format(
                name=name,
                labels=labels,
                value=floatstr(value),
            )
        return expfmt


class MetricCollectionThread(threading.Thread):
    def __init__(self, module):
        # type: (Module) -> None
        self.mod = module
        super(MetricCollectionThread, self).__init__(target=self.collect)

    def collect(self):
        self.mod.log.info('starting metric collection thread')
        while True:
            self.mod.log.debug('collecting cache in thread')
            if self.mod.have_mon_connection():
                start_time = time.time()
                data = self.mod.collect()
                duration = time.time() - start_time

                self.mod.log.debug('collecting cache in thread done')
                
                sleep_time = self.mod.scrape_interval - duration
                if sleep_time < 0:
                    self.mod.log.warning(
                        'Collecting data took more time than configured scrape interval. '
                        'This possibly results in stale data. Please check the '
                        '`stale_cache_strategy` configuration option. '
                        'Collecting data took {:.2f} seconds but scrape interval is configured '
                        'to be {:.0f} seconds.'.format(
                            duration,
                            self.mod.scrape_interval,
                        )
                    )
                    sleep_time = 0

                with self.mod.collect_lock:
                    self.mod.collect_cache = data
                    self.mod.collect_time = duration

                time.sleep(sleep_time)
            else:
                self.mod.log.error('No MON connection')
                time.sleep(self.mod.scrape_interval)


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "prometheus file_sd_config",
            "desc": "Return file_sd compatible prometheus config for mgr cluster",
            "perm": "r"
        },
    ]

    MODULE_OPTIONS = [
        {'name': 'server_addr'},
        {'name': 'server_port'},
        {'name': 'scrape_interval'},
        {'name': 'stale_cache_strategy'},
        {'name': 'rbd_stats_pools'},
        {'name': 'rbd_stats_pools_refresh_interval', 'type': 'int', 'default': 300},
    ]

    STALE_CACHE_FAIL = 'fail'
    STALE_CACHE_RETURN = 'return'

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.metrics = self._setup_static_metrics()
        self.shutdown_event = threading.Event()
        self.collect_lock = threading.Lock()
        self.collect_time = 0.0
        self.scrape_interval = 15.0
        self.stale_cache_strategy = self.STALE_CACHE_FAIL
        self.collect_cache = None
        self.rbd_stats = {
            'pools': {},
            'pools_refresh_time': 0,
            'counters_info': {
                'write_ops': {'type': self.PERFCOUNTER_COUNTER,
                              'desc': 'RBD image writes count'},
                'read_ops': {'type': self.PERFCOUNTER_COUNTER,
                             'desc': 'RBD image reads count'},
                'write_bytes': {'type': self.PERFCOUNTER_COUNTER,
                                'desc': 'RBD image bytes written'},
                'read_bytes': {'type': self.PERFCOUNTER_COUNTER,
                               'desc': 'RBD image bytes read'},
                'write_latency': {'type': self.PERFCOUNTER_LONGRUNAVG,
                                  'desc': 'RBD image writes latency (msec)'},
                'read_latency': {'type': self.PERFCOUNTER_LONGRUNAVG,
                                 'desc': 'RBD image reads latency (msec)'},
            },
        }  # type: Dict[str, Any]
        global _global_instance
        _global_instance = self
        MetricCollectionThread(_global_instance).start()

    def _setup_static_metrics(self):
        metrics = {}
        metrics['health_status'] = Metric(
            'untyped',
            'health_status',
            'Cluster health status'
        )
        metrics['mon_quorum_status'] = Metric(
            'gauge',
            'mon_quorum_status',
            'Monitors in quorum',
            ('ceph_daemon',)
        )
        metrics['fs_metadata'] = Metric(
            'untyped',
            'fs_metadata',
            'FS Metadata',
            FS_METADATA
        )
        metrics['mds_metadata'] = Metric(
            'untyped',
            'mds_metadata',
            'MDS Metadata',
            MDS_METADATA
        )
        metrics['mon_metadata'] = Metric(
            'untyped',
            'mon_metadata',
            'MON Metadata',
            MON_METADATA
        )
        metrics['mgr_metadata'] = Metric(
            'gauge',
            'mgr_metadata',
            'MGR metadata',
            MGR_METADATA
        )
        metrics['mgr_status'] = Metric(
            'gauge',
            'mgr_status',
            'MGR status (0=standby, 1=active)',
            MGR_STATUS
        )
        metrics['mgr_module_status'] = Metric(
            'gauge',
            'mgr_module_status',
            'MGR module status (0=disabled, 1=enabled, 2=auto-enabled)',
            MGR_MODULE_STATUS
        )
        metrics['mgr_module_can_run'] = Metric(
            'gauge',
            'mgr_module_can_run',
            'MGR module runnable state i.e. can it run (0=no, 1=yes)',
            MGR_MODULE_CAN_RUN
        )
        metrics['osd_metadata'] = Metric(
            'untyped',
            'osd_metadata',
            'OSD Metadata',
            OSD_METADATA
        )

        # The reason for having this separate to OSD_METADATA is
        # so that we can stably use the same tag names that
        # the Prometheus node_exporter does
        metrics['disk_occupation'] = Metric(
            'untyped',
            'disk_occupation',
            'Associate Ceph daemon with disk used',
            DISK_OCCUPATION
        )

        metrics['pool_metadata'] = Metric(
            'untyped',
            'pool_metadata',
            'POOL Metadata',
            POOL_METADATA
        )

        metrics['rgw_metadata'] = Metric(
            'untyped',
            'rgw_metadata',
            'RGW Metadata',
            RGW_METADATA
        )

        metrics['rbd_mirror_metadata'] = Metric(
            'untyped',
            'rbd_mirror_metadata',
            'RBD Mirror Metadata',
            RBD_MIRROR_METADATA
        )

        metrics['pg_total'] = Metric(
            'gauge',
            'pg_total',
            'PG Total Count per Pool',
            ('pool_id',)
        )

        for flag in OSD_FLAGS:
            path = 'osd_flag_{}'.format(flag)
            metrics[path] = Metric(
                'untyped',
                path,
                'OSD Flag {}'.format(flag)
            )
        for state in OSD_STATUS:
            path = 'osd_{}'.format(state)
            metrics[path] = Metric(
                'untyped',
                path,
                'OSD status {}'.format(state),
                ('ceph_daemon',)
            )
        for stat in OSD_STATS:
            path = 'osd_{}'.format(stat)
            metrics[path] = Metric(
                'gauge',
                path,
                'OSD stat {}'.format(stat),
                ('ceph_daemon',)
            )
        for stat in OSD_POOL_STATS:
            path = 'pool_{}'.format(stat)
            metrics[path] = Metric(
                'gauge',
                path,
                "OSD pool stats: {}".format(stat),
                ('pool_id',)
            )
        for state in PG_STATES:
            path = 'pg_{}'.format(state)
            metrics[path] = Metric(
                'gauge',
                path,
                'PG {} per pool'.format(state),
                ('pool_id',)
            )
        for state in DF_CLUSTER:
            path = 'cluster_{}'.format(state)
            metrics[path] = Metric(
                'gauge',
                path,
                'DF {}'.format(state),
            )
        for state in DF_POOL:
            path = 'pool_{}'.format(state)
            metrics[path] = Metric(
                'gauge',
                path,
                'DF pool {}'.format(state),
                ('pool_id',)
            )
        for state in NUM_OBJECTS:
            path = 'num_objects_{}'.format(state)
            metrics[path] = Metric(
                'gauge',
                path,
                'Number of {} objects'.format(state),
            )

        return metrics

    @profile_method()
    def get_health(self):
        health = json.loads(self.get('health')['json'])
        self.metrics['health_status'].set(
            health_status_to_number(health['status'])
        )

    @profile_method()
    def get_pool_stats(self):
        # retrieve pool stats to provide per pool recovery metrics
        # (osd_pool_stats moved to mgr in Mimic)
        pstats = self.get('osd_pool_stats')
        for pool in pstats['pool_stats']:
            for stat in OSD_POOL_STATS:
                self.metrics['pool_{}'.format(stat)].set(
                    pool['recovery_rate'].get(stat, 0),
                    (pool['pool_id'],)
                )

    @profile_method()
    def get_df(self):
        # maybe get the to-be-exported metrics from a config?
        df = self.get('df')
        for stat in DF_CLUSTER:
            self.metrics['cluster_{}'.format(stat)].set(df['stats'][stat])

        for pool in df['pools']:
            for stat in DF_POOL:
                self.metrics['pool_{}'.format(stat)].set(
                    pool['stats'][stat],
                    (pool['id'],)
                )

    @profile_method()
    def get_fs(self):
        fs_map = self.get('fs_map')
        servers = self.get_service_list()
        self.log.debug('standbys: {}'.format(fs_map['standbys']))
        # export standby mds metadata, default standby fs_id is '-1'
        for standby in fs_map['standbys']:
            id_ = standby['name']
            host_version = servers.get((id_, 'mds'), ('', ''))
            self.metrics['mds_metadata'].set(1, (
                'mds.{}'.format(id_), '-1',
                host_version[0], standby['addr'],
                standby['rank'], host_version[1]
            ))
        for fs in fs_map['filesystems']:
            # collect fs metadata
            data_pools = ",".join([str(pool)
                                   for pool in fs['mdsmap']['data_pools']])
            self.metrics['fs_metadata'].set(1, (
                data_pools,
                fs['id'],
                fs['mdsmap']['metadata_pool'],
                fs['mdsmap']['fs_name']
            ))
            self.log.debug('mdsmap: {}'.format(fs['mdsmap']))
            for gid, daemon in fs['mdsmap']['info'].items():
                id_ = daemon['name']
                host_version = servers.get((id_, 'mds'), ('', ''))
                self.metrics['mds_metadata'].set(1, (
                    'mds.{}'.format(id_), fs['id'],
                    host_version[0], daemon['addr'],
                    daemon['rank'], host_version[1]
                ))

    @profile_method()
    def get_quorum_status(self):
        mon_status = json.loads(self.get('mon_status')['json'])
        servers = self.get_service_list()
        for mon in mon_status['monmap']['mons']:
            rank = mon['rank']
            id_ = mon['name']
            host_version = servers.get((id_, 'mon'), ('', ''))
            self.metrics['mon_metadata'].set(1, (
                'mon.{}'.format(id_), host_version[0],
                mon['public_addr'].split(':')[0], rank,
                host_version[1]
            ))
            in_quorum = int(rank in mon_status['quorum'])
            self.metrics['mon_quorum_status'].set(in_quorum, (
                'mon.{}'.format(id_),
            ))

    @profile_method()
    def get_mgr_status(self):
        mgr_map = self.get('mgr_map')
        servers = self.get_service_list()

        active = mgr_map['active_name']
        standbys = [s.get('name') for s in mgr_map['standbys']]

        all_mgrs = list(standbys)
        all_mgrs.append(active)

        all_modules = {module.get('name'):module.get('can_run') for module in mgr_map['available_modules']}

        ceph_release = None
        for mgr in all_mgrs:
            host_version = servers.get((mgr, 'mgr'), ('', ''))
            if mgr == active:
                _state = 1
                ceph_release = host_version[1].split()[-2] # e.g. nautilus
            else:
                _state = 0

            self.metrics['mgr_metadata'].set(1, (
                'mgr.{}'.format(mgr), host_version[0],
                host_version[1]
            ))
            self.metrics['mgr_status'].set(_state, (
                'mgr.{}'.format(mgr),
            ))
        always_on_modules = mgr_map['always_on_modules'].get(ceph_release, [])
        active_modules = list(always_on_modules)
        active_modules.extend(mgr_map['modules'])

        for mod_name in all_modules.keys():

            if mod_name in always_on_modules:
                _state = 2
            elif mod_name in active_modules:
                _state = 1
            else:
                _state = 0

            _can_run = 1 if all_modules[mod_name] else 0
            self.metrics['mgr_module_status'].set(_state, (mod_name,))
            self.metrics['mgr_module_can_run'].set(_can_run, (mod_name,))

    @profile_method()
    def get_pg_status(self):

        pg_summary = self.get('pg_summary')

        for pool in pg_summary['by_pool']:
            num_by_state = dict((state, 0) for state in PG_STATES)
            num_by_state['total'] = 0

            for state_name, count in pg_summary['by_pool'][pool].items():
                for state in state_name.split('+'):
                    num_by_state[state] += count
                num_by_state['total'] += count

            for state, num in num_by_state.items():
                try:
                    self.metrics["pg_{}".format(state)].set(num, (pool,))
                except KeyError:
                    self.log.warning("skipping pg in unknown state {}".format(state))

    @profile_method()
    def get_osd_stats(self):
        osd_stats = self.get('osd_stats')
        for osd in osd_stats['osd_stats']:
            id_ = osd['osd']
            for stat in OSD_STATS:
                val = osd['perf_stat'][stat]
                self.metrics['osd_{}'.format(stat)].set(val, (
                    'osd.{}'.format(id_),
                ))

    def get_service_list(self):
        ret = {}
        for server in self.list_servers():
            version = server.get('ceph_version', '')
            host = server.get('hostname', '')
            for service in server.get('services', []):
                ret.update({(service['id'], service['type']): (host, version)})
        return ret

    @profile_method()
    def get_metadata_and_osd_status(self):
        osd_map = self.get('osd_map')
        osd_flags = osd_map['flags'].split(',')
        for flag in OSD_FLAGS:
            self.metrics['osd_flag_{}'.format(flag)].set(
                int(flag in osd_flags)
            )

        osd_devices = self.get('osd_map_crush')['devices']
        servers = self.get_service_list()
        for osd in osd_map['osds']:
            # id can be used to link osd metrics and metadata
            id_ = osd['osd']
            # collect osd metadata
            p_addr = osd['public_addr'].split(':')[0]
            c_addr = osd['cluster_addr'].split(':')[0]
            if p_addr == "-" or c_addr == "-":
                self.log.info(
                    "Missing address metadata for osd {0}, skipping occupation"
                    " and metadata records for this osd".format(id_)
                )
                continue

            dev_class = None
            for osd_device in osd_devices:
                if osd_device['id'] == id_:
                    dev_class = osd_device.get('class', '')
                    break

            if dev_class is None:
                self.log.info("OSD {0} is missing from CRUSH map, "
                              "skipping output".format(id_))
                continue

            host_version = servers.get((str(id_), 'osd'), ('', ''))

            # collect disk occupation metadata
            osd_metadata = self.get_metadata("osd", str(id_))
            if osd_metadata is None:
                continue

            obj_store = osd_metadata.get('osd_objectstore', '')
            f_iface = osd_metadata.get('front_iface', '')
            b_iface = osd_metadata.get('back_iface', '')

            self.metrics['osd_metadata'].set(1, (
                b_iface,
                'osd.{}'.format(id_),
                c_addr,
                dev_class,
                f_iface,
                host_version[0],
                obj_store,
                p_addr,
                host_version[1]
            ))

            # collect osd status
            for state in OSD_STATUS:
                status = osd[state]
                self.metrics['osd_{}'.format(state)].set(status, (
                    'osd.{}'.format(id_),
                ))

            osd_dev_node = None
            if obj_store == "filestore":
                # collect filestore backend device
                osd_dev_node = osd_metadata.get(
                    'backend_filestore_dev_node', None)
                # collect filestore journal device
                osd_wal_dev_node = osd_metadata.get('osd_journal', '')
                osd_db_dev_node = ''
            elif obj_store == "bluestore":
                # collect bluestore backend device
                osd_dev_node = osd_metadata.get(
                    'bluestore_bdev_dev_node', None)
                # collect bluestore wal backend
                osd_wal_dev_node = osd_metadata.get('bluefs_wal_dev_node', '')
                # collect bluestore db backend
                osd_db_dev_node = osd_metadata.get('bluefs_db_dev_node', '')
            if osd_dev_node and osd_dev_node == "unknown":
                osd_dev_node = None

            # fetch the devices and ids (vendor, model, serial) from the
            # osd_metadata
            osd_devs = osd_metadata.get('devices', '') or 'N/A'
            osd_dev_ids = osd_metadata.get('device_ids', '') or 'N/A'

            osd_hostname = osd_metadata.get('hostname', None)
            if osd_dev_node and osd_hostname:
                self.log.debug("Got dev for osd {0}: {1}/{2}".format(
                    id_, osd_hostname, osd_dev_node))
                self.metrics['disk_occupation'].set(1, (
                    "osd.{0}".format(id_),
                    osd_dev_node,
                    osd_db_dev_node,
                    osd_wal_dev_node,
                    osd_hostname,
                    osd_devs,
                    osd_dev_ids,
                ))
            else:
                self.log.info("Missing dev node metadata for osd {0}, skipping "
                              "occupation record for this osd".format(id_))

        for pool in osd_map['pools']:
            self.metrics['pool_metadata'].set(
                1, (pool['pool'], pool['pool_name']))

        # Populate other servers metadata
        for key, value in servers.items():
            service_id, service_type = key
            if service_type == 'rgw':
                hostname, version = value
                self.metrics['rgw_metadata'].set(
                    1,
                    ('{}.{}'.format(service_type, service_id),
                     hostname, version)
                )
            elif service_type == 'rbd-mirror':
                mirror_metadata = self.get_metadata('rbd-mirror', service_id)
                if mirror_metadata is None:
                    continue
                mirror_metadata['ceph_daemon'] = '{}.{}'.format(service_type,
                                                                service_id)
                self.metrics['rbd_mirror_metadata'].set(
                    1, (mirror_metadata.get(k, '')
                        for k in RBD_MIRROR_METADATA)
                )

    @profile_method()
    def get_num_objects(self):
        pg_sum = self.get('pg_summary')['pg_stats_sum']['stat_sum']
        for obj in NUM_OBJECTS:
            stat = 'num_objects_{}'.format(obj)
            self.metrics[stat].set(pg_sum[stat])

    @profile_method()
    def get_rbd_stats(self):
        # Per RBD image stats is collected by registering a dynamic osd perf
        # stats query that tells OSDs to group stats for requests associated
        # with RBD objects by pool, namespace, and image id, which are
        # extracted from the request object names or other attributes.
        # The RBD object names have the following prefixes:
        #   - rbd_data.{image_id}. (data stored in the same pool as metadata)
        #   - rbd_data.{pool_id}.{image_id}. (data stored in a dedicated data pool)
        #   - journal_data.{pool_id}.{image_id}. (journal if journaling is enabled)
        # The pool_id in the object name is the id of the pool with the image
        # metdata, and should be used in the image spec. If there is no pool_id
        # in the object name, the image pool is the pool where the object is
        # located.

        # Parse rbd_stats_pools option, which is a comma or space separated
        # list of pool[/namespace] entries. If no namespace is specifed the
        # stats are collected for every namespace in the pool. The wildcard
        # '*' can be used to indicate all pools or namespaces
        pools_string = self.get_localized_module_option('rbd_stats_pools', '')
        pool_keys = []
        for x in re.split('[\s,]+', pools_string):
            if not x:
                continue

            s = x.split('/', 2)
            pool_name = s[0]
            namespace_name = None
            if len(s) == 2:
                namespace_name = s[1]

            if pool_name == "*":
                # collect for all pools
                osd_map = self.get('osd_map')
                for pool in osd_map['pools']:
                    if 'rbd' not in pool.get('application_metadata', {}):
                        continue
                    pool_keys.append((pool['pool_name'], namespace_name))
            else:
                pool_keys.append((pool_name, namespace_name))

        pools = {}  # type: Dict[str, Set[str]]
        for pool_key in pool_keys:
            pool_name = pool_key[0]
            namespace_name = pool_key[1]
            if not namespace_name or namespace_name == "*":
                # empty set means collect for all namespaces
                pools[pool_name] = set()
                continue

            if pool_name not in pools:
                pools[pool_name] = set()
            elif not pools[pool_name]:
                continue
            pools[pool_name].add(namespace_name)

        rbd_stats_pools = {}
        for pool_id in self.rbd_stats['pools'].keys():
            name = self.rbd_stats['pools'][pool_id]['name']
            if name not in pools:
                del self.rbd_stats['pools'][pool_id]
            else:
                rbd_stats_pools[name] = \
                    self.rbd_stats['pools'][pool_id]['ns_names']

        pools_refreshed = False
        if pools:
            next_refresh = self.rbd_stats['pools_refresh_time'] + \
                self.get_localized_module_option(
                'rbd_stats_pools_refresh_interval', 300)
            if rbd_stats_pools != pools or time.time() >= next_refresh:
                self.refresh_rbd_stats_pools(pools)
                pools_refreshed = True

        pool_ids = list(self.rbd_stats['pools'])
        pool_ids.sort()
        pool_id_regex = '^(' + '|'.join([str(x) for x in pool_ids]) + ')$'

        nspace_names = []
        for pool_id, pool in self.rbd_stats['pools'].items():
            if pool['ns_names']:
                nspace_names.extend(pool['ns_names'])
            else:
                nspace_names = []
                break
        if nspace_names:
            namespace_regex = '^(' + \
                              "|".join([re.escape(x)
                                        for x in set(nspace_names)]) + ')$'
        else:
            namespace_regex = '^(.*)$'

        if 'query' in self.rbd_stats and \
           (pool_id_regex != self.rbd_stats['query']['key_descriptor'][0]['regex'] or
                namespace_regex != self.rbd_stats['query']['key_descriptor'][1]['regex']):
            self.remove_osd_perf_query(self.rbd_stats['query_id'])
            del self.rbd_stats['query_id']
            del self.rbd_stats['query']

        if not self.rbd_stats['pools']:
            return

        counters_info = self.rbd_stats['counters_info']

        if 'query_id' not in self.rbd_stats:
            query = {
                'key_descriptor': [
                    {'type': 'pool_id', 'regex': pool_id_regex},
                    {'type': 'namespace', 'regex': namespace_regex},
                    {'type': 'object_name',
                     'regex': '^(?:rbd|journal)_data\.(?:([0-9]+)\.)?([^.]+)\.'},
                ],
                'performance_counter_descriptors': list(counters_info),
            }
            query_id = self.add_osd_perf_query(query)
            if query_id is None:
                self.log.error('failed to add query %s' % query)
                return
            self.rbd_stats['query'] = query
            self.rbd_stats['query_id'] = query_id

        res = self.get_osd_perf_counters(self.rbd_stats['query_id'])
        for c in res['counters']:
            # if the pool id is not found in the object name use id of the
            # pool where the object is located
            if c['k'][2][0]:
                pool_id = int(c['k'][2][0])
            else:
                pool_id = int(c['k'][0][0])
            if pool_id not in self.rbd_stats['pools'] and not pools_refreshed:
                self.refresh_rbd_stats_pools(pools)
                pools_refreshed = True
            if pool_id not in self.rbd_stats['pools']:
                continue
            pool = self.rbd_stats['pools'][pool_id]
            nspace_name = c['k'][1][0]
            if nspace_name not in pool['images']:
                continue
            image_id = c['k'][2][1]
            if image_id not in pool['images'][nspace_name] and \
               not pools_refreshed:
                self.refresh_rbd_stats_pools(pools)
                pool = self.rbd_stats['pools'][pool_id]
                pools_refreshed = True
            if image_id not in pool['images'][nspace_name]:
                continue
            counters = pool['images'][nspace_name][image_id]['c']
            for i in range(len(c['c'])):
                counters[i][0] += c['c'][i][0]
                counters[i][1] += c['c'][i][1]

        label_names = ("pool", "namespace", "image")
        for pool_id, pool in self.rbd_stats['pools'].items():
            pool_name = pool['name']
            for nspace_name, images in pool['images'].items():
                for image_id in images:
                    image_name = images[image_id]['n']
                    counters = images[image_id]['c']
                    i = 0
                    for key in counters_info:
                        counter_info = counters_info[key]
                        stattype = self._stattype_to_str(counter_info['type'])
                        labels = (pool_name, nspace_name, image_name)
                        if counter_info['type'] == self.PERFCOUNTER_COUNTER:
                            path = 'rbd_' + key
                            if path not in self.metrics:
                                self.metrics[path] = Metric(
                                    stattype,
                                    path,
                                    counter_info['desc'],
                                    label_names,
                                )
                            self.metrics[path].set(counters[i][0], labels)
                        elif counter_info['type'] == self.PERFCOUNTER_LONGRUNAVG:
                            path = 'rbd_' + key + '_sum'
                            if path not in self.metrics:
                                self.metrics[path] = Metric(
                                    stattype,
                                    path,
                                    counter_info['desc'] + ' Total',
                                    label_names,
                                )
                            self.metrics[path].set(counters[i][0], labels)
                            path = 'rbd_' + key + '_count'
                            if path not in self.metrics:
                                self.metrics[path] = Metric(
                                    'counter',
                                    path,
                                    counter_info['desc'] + ' Count',
                                    label_names,
                                )
                            self.metrics[path].set(counters[i][1], labels)
                        i += 1

    def refresh_rbd_stats_pools(self, pools):
        self.log.debug('refreshing rbd pools %s' % (pools))

        rbd = RBD()
        counters_info = self.rbd_stats['counters_info']
        for pool_name, cfg_ns_names in pools.items():
            try:
                pool_id = self.rados.pool_lookup(pool_name)
                with self.rados.open_ioctx(pool_name) as ioctx:
                    if pool_id not in self.rbd_stats['pools']:
                        self.rbd_stats['pools'][pool_id] = {'images': {}}
                    pool = self.rbd_stats['pools'][pool_id]
                    pool['name'] = pool_name
                    pool['ns_names'] = cfg_ns_names
                    if cfg_ns_names:
                        nspace_names = list(cfg_ns_names)
                    else:
                        nspace_names = [''] + rbd.namespace_list(ioctx)
                    for nspace_name in pool['images']:
                        if nspace_name not in nspace_names:
                            del pool['images'][nspace_name]
                    for nspace_name in nspace_names:
                        if (nspace_name and
                                not rbd.namespace_exists(ioctx, nspace_name)):
                            self.log.debug('unknown namespace %s for pool %s' %
                                           (nspace_name, pool_name))
                            continue
                        ioctx.set_namespace(nspace_name)
                        if nspace_name not in pool['images']:
                            pool['images'][nspace_name] = {}
                        namespace = pool['images'][nspace_name]
                        images = {}
                        for image_meta in RBD().list2(ioctx):
                            image = {'n': image_meta['name']}
                            image_id = image_meta['id']
                            if image_id in namespace:
                                image['c'] = namespace[image_id]['c']
                            else:
                                image['c'] = [[0, 0] for x in counters_info]
                            images[image_id] = image
                        pool['images'][nspace_name] = images
            except Exception as e:
                self.log.error('failed listing pool %s: %s' % (pool_name, e))
        self.rbd_stats['pools_refresh_time'] = time.time()

    def shutdown_rbd_stats(self):
        if 'query_id' in self.rbd_stats:
            self.remove_osd_perf_query(self.rbd_stats['query_id'])
            del self.rbd_stats['query_id']
            del self.rbd_stats['query']
        self.rbd_stats['pools'].clear()

    def add_fixed_name_metrics(self):
        """
        Add fixed name metrics from existing ones that have details in their names
        that should be in labels (not in name).
        For backward compatibility, a new fixed name metric is created (instead of replacing)
        and details are put in new labels.
        Intended for RGW sync perf. counters but extendable as required.
        See: https://tracker.ceph.com/issues/45311
        """
        new_metrics = {}
        for metric_path in self.metrics.keys():
            # Address RGW sync perf. counters.
            match = re.search('^data-sync-from-(.*)\.', metric_path)
            if match:
                new_path = re.sub('from-([^.]*)', 'from-zone', metric_path)
                if new_path not in new_metrics:
                    new_metrics[new_path] = Metric(
                        self.metrics[metric_path].mtype,
                        new_path,
                        self.metrics[metric_path].desc,
                        self.metrics[metric_path].labelnames + ('source_zone',)
                    )
                for label_values, value in self.metrics[metric_path].value.items():
                    new_metrics[new_path].set(value, label_values + (match.group(1),))

        self.metrics.update(new_metrics)

    @profile_method(True)
    def collect(self):
        # Clear the metrics before scraping
        for k in self.metrics.keys():
            self.metrics[k].clear()

        self.get_health()
        self.get_df()
        self.get_pool_stats()
        self.get_fs()
        self.get_osd_stats()
        self.get_quorum_status()
        self.get_mgr_status()
        self.get_metadata_and_osd_status()
        self.get_pg_status()
        self.get_num_objects()

        for daemon, counters in self.get_all_perf_counters().items():
            for path, counter_info in counters.items():
                # Skip histograms, they are represented by long running avgs
                stattype = self._stattype_to_str(counter_info['type'])
                if not stattype or stattype == 'histogram':
                    self.log.debug('ignoring %s, type %s' % (path, stattype))
                    continue

                path, label_names, labels = self._perfpath_to_path_labels(
                    daemon, path)

                # Get the value of the counter
                value = self._perfvalue_to_value(
                    counter_info['type'], counter_info['value'])

                # Represent the long running avgs as sum/count pairs
                if counter_info['type'] & self.PERFCOUNTER_LONGRUNAVG:
                    _path = path + '_sum'
                    if _path not in self.metrics:
                        self.metrics[_path] = Metric(
                            stattype,
                            _path,
                            counter_info['description'] + ' Total',
                            label_names,
                        )
                    self.metrics[_path].set(value, labels)

                    _path = path + '_count'
                    if _path not in self.metrics:
                        self.metrics[_path] = Metric(
                            'counter',
                            _path,
                            counter_info['description'] + ' Count',
                            label_names,
                        )
                    self.metrics[_path].set(counter_info['count'], labels,)
                else:
                    if path not in self.metrics:
                        self.metrics[path] = Metric(
                            stattype,
                            path,
                            counter_info['description'],
                            label_names,
                        )
                    self.metrics[path].set(value, labels)

        self.add_fixed_name_metrics()
        self.get_rbd_stats()

        # Return formatted metrics and clear no longer used data
        _metrics = [m.str_expfmt() for m in self.metrics.values()]
        for k in self.metrics.keys():
            self.metrics[k].clear()

        return ''.join(_metrics) + '\n'

    def get_file_sd_config(self):
        servers = self.list_servers()
        targets = []
        for server in servers:
            hostname = server.get('hostname', '')
            for service in server.get('services', []):
                if service['type'] != 'mgr':
                    continue
                id_ = service['id']
                # get port for prometheus module at mgr with id_
                # TODO use get_config_prefix or get_config here once
                # https://github.com/ceph/ceph/pull/20458 is merged
                result = CommandResult("")
                assert isinstance(_global_instance, Module)
                _global_instance.send_command(
                    result, "mon", '',
                    json.dumps({
                        "prefix": "config-key get",
                        'key': "config/mgr/mgr/prometheus/{}/server_port".format(id_),
                    }),
                    "")
                r, outb, outs = result.wait()
                if r != 0:
                    _global_instance.log.error("Failed to retrieve port for mgr {}: {}".format(id_, outs))
                    targets.append('{}:{}'.format(hostname, DEFAULT_PORT))
                else:
                    port = json.loads(outb)
                    targets.append('{}:{}'.format(hostname, port))

        ret = [
            {
                "targets": targets,
                "labels": {}
            }
        ]
        return 0, json.dumps(ret), ""

    def self_test(self):
        self.collect()
        self.get_file_sd_config()

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == 'prometheus file_sd_config':
            return self.get_file_sd_config()
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(cmd['prefix']))

    def serve(self):

        class Root(object):

            # collapse everything to '/'
            def _cp_dispatch(self, vpath):
                cherrypy.request.path = ''
                return self

            @cherrypy.expose
            def index(self):
                return '''<!DOCTYPE html>
<html>
    <head><title>Ceph Exporter</title></head>
    <body>
        <h1>Ceph Exporter</h1>
        <p><a href='/metrics'>Metrics</a></p>
    </body>
</html>'''

            @cherrypy.expose
            def metrics(self):
                # Lock the function execution
                assert isinstance(_global_instance, Module)
                with _global_instance.collect_lock:
                    return self._metrics(_global_instance)

            @staticmethod
            def _metrics(instance):
                # type: (Module) -> Any
                # Return cached data if available
                if not instance.collect_cache:
                    raise cherrypy.HTTPError(503, 'No cached data available yet')

                def respond():
                    assert isinstance(instance, Module)
                    cherrypy.response.headers['Content-Type'] = 'text/plain'
                    return instance.collect_cache

                if instance.collect_time < instance.scrape_interval:
                    # Respond if cache isn't stale
                    return respond()

                if instance.stale_cache_strategy == instance.STALE_CACHE_RETURN:
                    # Respond even if cache is stale
                    instance.log.info(
                        'Gathering data took {:.2f} seconds, metrics are stale for {:.2f} seconds, '
                        'returning metrics from stale cache.'.format(
                            instance.collect_time,
                            instance.collect_time - instance.scrape_interval
                        )
                    )
                    return respond()

                if instance.stale_cache_strategy == instance.STALE_CACHE_FAIL:
                    # Fail if cache is stale
                    msg = (
                        'Gathering data took {:.2f} seconds, metrics are stale for {:.2f} seconds, '
                        'returning "service unavailable".'.format(
                            instance.collect_time,
                            instance.collect_time - instance.scrape_interval,
                        )
                    )
                    instance.log.error(msg)
                    raise cherrypy.HTTPError(503, msg)

        # Make the cache timeout for collecting configurable
        self.scrape_interval = float(self.get_localized_module_option('scrape_interval', 15.0))

        self.stale_cache_strategy = self.get_localized_module_option('stale_cache_strategy', 'log')
        if self.stale_cache_strategy not in [self.STALE_CACHE_FAIL,
                                             self.STALE_CACHE_RETURN]:
            self.stale_cache_strategy = self.STALE_CACHE_FAIL

        server_addr = self.get_localized_module_option(
            'server_addr', get_default_addr())
        server_port = self.get_localized_module_option(
            'server_port', DEFAULT_PORT)
        self.log.info(
            "server_addr: %s server_port: %s" %
            (server_addr, server_port)
        )

        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri('http://{0}:{1}/'.format(
            socket.getfqdn() if server_addr in ['::', '0.0.0.0'] else server_addr,
            server_port
        ))

        cherrypy.config.update({
            'server.socket_host': server_addr,
            'server.socket_port': int(server_port),
            'engine.autoreload.on': False
        })
        cherrypy.tree.mount(Root(), "/")
        self.log.info('Starting engine...')
        cherrypy.engine.start()
        self.log.info('Engine started.')
        # wait for the shutdown event
        self.shutdown_event.wait()
        self.shutdown_event.clear()
        cherrypy.engine.stop()
        self.log.info('Engine stopped.')
        self.shutdown_rbd_stats()

    def shutdown(self):
        self.log.info('Stopping engine...')
        self.shutdown_event.set()


class StandbyModule(MgrStandbyModule):
    def __init__(self, *args, **kwargs):
        super(StandbyModule, self).__init__(*args, **kwargs)
        self.shutdown_event = threading.Event()

    def serve(self):
        server_addr = self.get_localized_module_option(
            'server_addr', get_default_addr())
        server_port = self.get_localized_module_option(
            'server_port', DEFAULT_PORT)
        self.log.info("server_addr: %s server_port: %s" %
                      (server_addr, server_port))
        cherrypy.config.update({
            'server.socket_host': server_addr,
            'server.socket_port': int(server_port),
            'engine.autoreload.on': False
        })

        module = self

        class Root(object):
            @cherrypy.expose
            def index(self):
                active_uri = module.get_active_uri()
                return '''<!DOCTYPE html>
<html>
    <head><title>Ceph Exporter</title></head>
    <body>
        <h1>Ceph Exporter</h1>
        <p><a href='{}metrics'>Metrics</a></p>
    </body>
</html>'''.format(active_uri)

            @cherrypy.expose
            def metrics(self):
                cherrypy.response.headers['Content-Type'] = 'text/plain'
                return ''

        cherrypy.tree.mount(Root(), '/', {})
        self.log.info('Starting engine...')
        cherrypy.engine.start()
        self.log.info('Engine started.')
        # Wait for shutdown event
        self.shutdown_event.wait()
        self.shutdown_event.clear()
        cherrypy.engine.stop()
        self.log.info('Engine stopped.')

    def shutdown(self):
        self.log.info("Stopping engine...")
        self.shutdown_event.set()
        self.log.info("Stopped engine")
