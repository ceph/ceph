import cherrypy
from distutils.version import StrictVersion
import json
import errno
import math
import os
import socket
import threading
import time
from mgr_module import MgrModule, MgrStandbyModule

# Defaults for the Prometheus HTTP server.  Can also set in config-key
# see https://github.com/prometheus/prometheus/wiki/Default-port-allocations
# for Prometheus exporter port registry

DEFAULT_ADDR = '::'
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

_global_instance = {'plugin': None}


def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']


def health_status_to_number(status):

    if status == 'HEALTH_OK':
        return 0
    elif status == 'HEALTH_WARN':
        return 1
    elif status == 'HEALTH_ERR':
        return 2

PG_STATES = [
        "active",
        "clean",
        "down",
        "recovery_unfound",
        "backfill_unfound",
        "scrubbing",
        "degraded",
        "inconsistent",
        "peering",
        "repair",
        "recovering",
        "forced_recovery",
        "backfill_wait",
        "incomplete",
        "stale",
        "remapped",
        "deep",
        "backfilling",
        "forced_backfill",
        "backfill_toofull",
        "recovery_wait",
        "recovery_toofull",
        "undersized",
        "activating",
        "peered",
        "snaptrim",
        "snaptrim_wait",
        "snaptrim_error",
        "creating",
        "unknown"]

DF_CLUSTER = ['total_bytes', 'total_used_bytes', 'total_objects']

DF_POOL = ['max_avail', 'bytes_used', 'raw_bytes_used', 'objects', 'dirty',
           'quota_bytes', 'quota_objects', 'rd', 'rd_bytes', 'wr', 'wr_bytes']

OSD_FLAGS = ('noup', 'nodown', 'noout', 'noin', 'nobackfill', 'norebalance',
             'norecover', 'noscrub', 'nodeep-scrub')

FS_METADATA = ('data_pools', 'fs_id', 'metadata_pool', 'name')

MDS_METADATA = ('ceph_daemon', 'fs_id', 'hostname', 'public_addr', 'rank',
                'ceph_version')

MON_METADATA = ('ceph_daemon', 'hostname', 'public_addr', 'rank', 'ceph_version')

OSD_METADATA = ('back_iface', 'ceph_daemon', 'cluster_addr', 'device_class',
                'front_iface', 'hostname', 'objectstore', 'public_addr',
                'ceph_version')

OSD_STATUS = ['weight', 'up', 'in']

OSD_STATS = ['apply_latency_ms', 'commit_latency_ms']

POOL_METADATA = ('pool_id', 'name')

RGW_METADATA = ('ceph_daemon', 'hostname', 'ceph_version')

DISK_OCCUPATION = ('ceph_daemon', 'device', 'db_device', 'wal_device', 'instance')

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
            result = path.replace('.', '_').replace(
                '+', '_plus').replace('::', '_').replace(' ', '_')

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
                labels = zip(self.labelnames, labelvalues)
                labels = ','.join('%s="%s"' % (k, v) for k, v in labels)
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


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "prometheus self-test",
            "desc": "Run a self test on the prometheus module",
            "perm": "rw"
        },
    ]

    OPTIONS = [
            {'name': 'server_addr'},
            {'name': 'server_port'},
            {'name': 'scrape_interval'},
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.metrics = self._setup_static_metrics()
        self.shutdown_event = threading.Event()
        self.collect_lock = threading.RLock()
        self.collect_time = 0
        self.collect_timeout = 5.0
        self.collect_cache = None
        _global_instance['plugin'] = self

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

        metrics['pg_total'] = Metric(
            'gauge',
            'pg_total',
            'PG Total Count'
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
        for state in PG_STATES:
            path = 'pg_{}'.format(state)
            metrics[path] = Metric(
                'gauge',
                path,
                'PG {}'.format(state),
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

    def get_health(self):
        health = json.loads(self.get('health')['json'])
        self.metrics['health_status'].set(
            health_status_to_number(health['status'])
        )

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

    def get_fs(self):
        fs_map = self.get('fs_map')
        servers = self.get_service_list()
        active_daemons = []
        for fs in fs_map['filesystems']:
            # collect fs metadata
            data_pools = ",".join([str(pool) for pool in fs['mdsmap']['data_pools']])
            self.metrics['fs_metadata'].set(1, (
                data_pools,
                fs['id'],
                fs['mdsmap']['metadata_pool'],
                fs['mdsmap']['fs_name']
            ))
            self.log.debug('mdsmap: {}'.format(fs['mdsmap']))
            for gid, daemon in fs['mdsmap']['info'].items():
                id_ = daemon['name']
                host_version = servers.get((id_, 'mds'), ('',''))
                self.metrics['mds_metadata'].set(1, (
                    'mds.{}'.format(id_), fs['id'],
                    host_version[0], daemon['addr'],
                    daemon['rank'], host_version[1]
                ))

    def get_quorum_status(self):
        mon_status = json.loads(self.get('mon_status')['json'])
        servers = self.get_service_list()
        for mon in mon_status['monmap']['mons']:
            rank = mon['rank']
            id_ = mon['name']
            host_version = servers.get((id_, 'mon'), ('',''))
            self.metrics['mon_metadata'].set(1, (
                'mon.{}'.format(id_), host_version[0],
                mon['public_addr'].split(':')[0], rank,
                host_version[1]
            ))
            in_quorum = int(rank in mon_status['quorum'])
            self.metrics['mon_quorum_status'].set(in_quorum, (
                'mon.{}'.format(id_),
            ))

    def get_pg_status(self):
        # TODO add per pool status?
        pg_status = self.get('pg_status')

        # Set total count of PGs, first
        self.metrics['pg_total'].set(pg_status['num_pgs'])

        reported_states = {}
        for pg in pg_status['pgs_by_state']:
            for state in pg['state_name'].split('+'):
                reported_states[state] =  reported_states.get(state, 0) + pg['count']

        for state in reported_states:
            path = 'pg_{}'.format(state)
            try:
                self.metrics[path].set(reported_states[state])
            except KeyError:
                self.log.warn("skipping pg in unknown state {}".format(state))

        for state in PG_STATES:
            if state not in reported_states:
                try:
                    self.metrics['pg_{}'.format(state)].set(0)
                except KeyError:
                    self.log.warn("skipping pg in unknown state {}".format(state))

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
                self.log.info(
                    "OSD {0} is missing from CRUSH map, skipping output".format(
                        id_))
                continue

            host_version = servers.get((str(id_), 'osd'), ('',''))

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
                osd_dev_node = osd_metadata.get('backend_filestore_dev_node', None)
            # collect filestore journal device
                osd_wal_dev_node = osd_metadata.get('osd_journal', '')
                osd_db_dev_node = ''
            elif obj_store == "bluestore":
            # collect bluestore backend device
                osd_dev_node = osd_metadata.get('bluestore_bdev_dev_node', None)
            # collect bluestore wal backend
                osd_wal_dev_node = osd_metadata.get('bluefs_wal_dev_node', '')
            # collect bluestore db backend
                osd_db_dev_node = osd_metadata.get('bluefs_db_dev_node', '')
            if osd_dev_node and osd_dev_node == "unknown":
                osd_dev_node = None

            osd_hostname = osd_metadata.get('hostname', None)
            if osd_dev_node and osd_hostname:
                self.log.debug("Got dev for osd {0}: {1}/{2}".format(
                    id_, osd_hostname, osd_dev_node))
                self.metrics['disk_occupation'].set(1, (
                    "osd.{0}".format(id_),
                    osd_dev_node,
                    osd_db_dev_node,
                    osd_wal_dev_node,
                    osd_hostname
                ))
            else:
                self.log.info("Missing dev node metadata for osd {0}, skipping "
                               "occupation record for this osd".format(id_))

        pool_meta = []
        for pool in osd_map['pools']:
            self.metrics['pool_metadata'].set(1, (pool['pool'], pool['pool_name']))

        # Populate rgw_metadata
        for key, value in servers.items():
            service_id, service_type = key
            if service_type != 'rgw':
                continue
            hostname, version = value
            self.metrics['rgw_metadata'].set(
                1,
                ('{}.{}'.format(service_type, service_id), hostname, version)
            )

    def get_num_objects(self):
        pg_sum = self.get('pg_summary')['pg_stats_sum']['stat_sum']
        for obj in NUM_OBJECTS:
            stat = 'num_objects_{}'.format(obj)
            self.metrics[stat].set(pg_sum[stat])

    def collect(self):
        # Clear the metrics before scraping
        for k in self.metrics.keys():
            self.metrics[k].clear()

        self.get_health()
        self.get_df()
        self.get_fs()
        self.get_osd_stats()
        self.get_quorum_status()
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

                # Get the value of the counter
                value = self._perfvalue_to_value(counter_info['type'], counter_info['value'])

                # Represent the long running avgs as sum/count pairs
                if counter_info['type'] & self.PERFCOUNTER_LONGRUNAVG:
                    _path = path + '_sum'
                    if _path not in self.metrics:
                        self.metrics[_path] = Metric(
                            stattype,
                            _path,
                            counter_info['description'] + ' Total',
                            ("ceph_daemon",),
                        )
                    self.metrics[_path].set(value, (daemon,))

                    _path = path + '_count'
                    if _path not in self.metrics:
                        self.metrics[_path] = Metric(
                            'counter',
                            _path,
                            counter_info['description'] + ' Count',
                            ("ceph_daemon",),
                        )
                    self.metrics[_path].set(counter_info['count'], (daemon,))
                else:
                    if path not in self.metrics:
                        self.metrics[path] = Metric(
                            stattype,
                            path,
                            counter_info['description'],
                            ("ceph_daemon",),
                        )
                    self.metrics[path].set(value, (daemon,))

        # Return formatted metrics and clear no longer used data
        _metrics = [m.str_expfmt() for m in self.metrics.values()]
        for k in self.metrics.keys():
            self.metrics[k].clear()

        return ''.join(_metrics) + '\n'

    def handle_command(self, cmd):
        if cmd['prefix'] == 'prometheus self-test':
            self.collect()
            return 0, '', 'Self-test OK'
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
                instance = global_instance()
                # Lock the function execution
                try:
                    instance.collect_lock.acquire()
                    return self._metrics(instance)
                finally:
                    instance.collect_lock.release()

            def _metrics(self, instance):
                # Return cached data if available and collected before the cache times out
                if instance.collect_cache and time.time() - instance.collect_time  < instance.collect_timeout:
                    cherrypy.response.headers['Content-Type'] = 'text/plain'
                    return instance.collect_cache

                if instance.have_mon_connection():
                    instance.collect_cache = None
                    instance.collect_time = time.time()
                    instance.collect_cache = instance.collect()
                    cherrypy.response.headers['Content-Type'] = 'text/plain'
                    return instance.collect_cache
                else:
                    raise cherrypy.HTTPError(503, 'No MON connection')

        # Make the cache timeout for collecting configurable
        self.collect_timeout = float(self.get_localized_config(
            'scrape_interval', 5.0))

        server_addr = self.get_localized_config('server_addr', DEFAULT_ADDR)
        server_port = self.get_localized_config('server_port', DEFAULT_PORT)
        self.log.info(
            "server_addr: %s server_port: %s" %
            (server_addr, server_port)
        )

        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri('http://{0}:{1}/'.format(
            socket.getfqdn() if server_addr == '::' else server_addr,
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

    def shutdown(self):
        self.log.info('Stopping engine...')
        self.shutdown_event.set()


class StandbyModule(MgrStandbyModule):
    def __init__(self, *args, **kwargs):
        super(StandbyModule, self).__init__(*args, **kwargs)
        self.shutdown_event = threading.Event()

    def serve(self):
        server_addr = self.get_localized_config('server_addr', '::')
        server_port = self.get_localized_config('server_port', DEFAULT_PORT)
        self.log.info("server_addr: %s server_port: %s" % (server_addr, server_port))
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
