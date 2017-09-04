import cherrypy
import json
import math
import os
from collections import OrderedDict
from mgr_module import MgrModule

# Defaults for the Prometheus HTTP server.  Can also set in config-key
# see https://github.com/prometheus/prometheus/wiki/Default-port-allocations
# for Prometheus exporter port registry

DEFAULT_ADDR = '::'
DEFAULT_PORT = 9283


# cherrypy likes to sys.exit on error.  don't let it take us down too!
def os_exit_noop():
    pass


os._exit = os_exit_noop


# to access things in class Module from subclass Root.  Because
# it's a dict, the writer doesn't need to declare 'global' for access

_global_instance = {'plugin': None}


def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']


# counter value types
PERFCOUNTER_TIME = 1
PERFCOUNTER_U64 = 2

# counter types
PERFCOUNTER_LONGRUNAVG = 4
PERFCOUNTER_COUNTER = 8
PERFCOUNTER_HISTOGRAM = 0x10
PERFCOUNTER_TYPE_MASK = ~2


def stattype_to_str(stattype):

    typeonly = stattype & PERFCOUNTER_TYPE_MASK
    if typeonly == 0:
        return 'gauge'
    if typeonly == PERFCOUNTER_LONGRUNAVG:
        # this lie matches the DaemonState decoding: only val, no counts
        return 'counter'
    if typeonly == PERFCOUNTER_COUNTER:
        return 'counter'
    if typeonly == PERFCOUNTER_HISTOGRAM:
        return 'histogram'

    return ''


def health_status_to_number(status):

    if status == 'HEALTH_OK':
        return 0
    elif status == 'HEALTH_WARN':
        return 1
    elif status == 'HEALTH_ERR':
        return 2

PG_STATES = ['creating', 'active', 'clean', 'down', 'scrubbing', 'degraded',
        'inconsistent', 'peering', 'repair', 'recovering', 'forced-recovery',
        'backfill', 'forced-backfill', 'wait-backfill', 'backfill-toofull',
        'incomplete', 'stale', 'remapped', 'undersized', 'peered']

DF_CLUSTER = ['total_bytes', 'total_used_bytes', 'total_objects']

DF_POOL = ['max_avail', 'bytes_used', 'raw_bytes_used', 'objects', 'dirty',
           'quota_bytes', 'quota_objects', 'rd', 'rd_bytes', 'wr', 'wr_bytes']

OSD_METADATA = ('cluster_addr', 'device_class', 'id', 'public_addr', 'weight')

POOL_METADATA= ('pool_id', 'name')

PERF_COUNTERS = {
    'mon': [
        'mon.election_call',
        'mon.election_lose',
        'mon.election_win',
        'mon.num_elections',
        'mon.num_sessions',
        'mon.session_add',
        'mon.session_rm',
        'mon.session_trim',
        'paxos.accept_timeout',
        'paxos.begin',
        'paxos.begin_bytes',
        'paxos.begin_keys',
        'paxos.begin_latency',
        'paxos.collect',
        'paxos.collect_bytes',
        'paxos.collect_keys',
        'paxos.collect_latency',
        'paxos.collect_timeout',
        'paxos.collect_uncommitted',
        'paxos.commit',
        'paxos.commit_bytes',
        'paxos.commit_keys',
        'paxos.commit_latency',
        'paxos.lease_ack_timeout',
        'paxos.lease_timeout',
        'paxos.new_pn',
        'paxos.new_pn_latency',
        'paxos.refresh',
        'paxos.refresh_latency',
        'paxos.restart',
        'paxos.share_state',
        'paxos.share_state_bytes',
        'paxos.share_state_keys',
        'paxos.start_leader',
        'paxos.start_peon',
        'paxos.store_state',
        'paxos.store_state_bytes',
        'paxos.store_state_keys',
        'paxos.store_state_latency',
        'rocksdb.compact',
        'rocksdb.compact_queue_len',
        'rocksdb.compact_queue_merge',
        'rocksdb.compact_range',
        'rocksdb.get',
        'rocksdb.get_latency',
        'rocksdb.rocksdb_write_delay_time',
        'rocksdb.rocksdb_write_memtable_time',
        'rocksdb.rocksdb_write_pre_and_post_time',
        'rocksdb.rocksdb_write_wal_time',
        'rocksdb.submit_latency',
        'rocksdb.submit_sync_latency',
        'rocksdb.submit_transaction',
        'rocksdb.submit_transaction_sync'
    ],
    'osd': [
        'osd.stat_bytes',
        'osd.stat_bytes_used',
        'osd.buffer_size',
        'osd.op_cache_hit',
        'osd.op_in_bytes',
        'osd.op_latency',
        'osd.op_out_bytes',
        'osd.op_prepare_latency',
        'osd.op_process_latency',
        'osd.op_r',
        'osd.op_r_latency',
        'osd.op_r_out_bytes',
        'osd.op_r_prepare_latency',
        'osd.op_r_process_latency',
        'osd.op_rw',
        'osd.op_rw_in_bytes',
        'osd.op_rw_latency',
        'osd.op_rw_out_bytes',
        'osd.op_rw_prepare_latency',
        'osd.op_rw_process_latency',
        'osd.op_w',
        'osd.op_w_in_bytes',
        'osd.op_w_latency',
        'osd.op_w_prepare_latency',
        'osd.op_w_process_latency',
    ]
}


class Metric(object):
    def __init__(self, mtype, name, desc, labels=None):
        self.mtype = mtype
        self.name = name
        self.desc = desc
        self.labelnames = labels    # tuple if present
        self.value = dict()         # indexed by label values

    def set(self, value, labelvalues=None):
        # labelvalues must be a tuple
        labelvalues = labelvalues or ('',)
        self.value[labelvalues] = value

    def str_expfmt(self):

        def promethize(path):
            ''' replace illegal metric name characters '''
            result = path.replace('.', '_').replace('+', '_plus').replace('::', '_')

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

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.notified = False
        self.serving = False
        self.metrics = self._setup_static_metrics()
        self.schema = OrderedDict()
        _global_instance['plugin'] = self

    def _setup_static_metrics(self):
        metrics = {}
        metrics['health_status'] = Metric(
            'undef',
            'health_status',
            'Cluster health status'
        )
        metrics['mon_quorum_count'] = Metric(
            'gauge',
            'mon_quorum_count',
            'Monitors in quorum'
        )
        metrics['osd_metadata'] = Metric(
            'undef',
            'osd_metadata',
            'OSD Metadata',
            OSD_METADATA
        )
        metrics['pool_metadata'] = Metric(
            'undef',
            'pool_metadata',
            'POOL Metadata',
            POOL_METADATA
        )
        for state in PG_STATES:
            path = 'pg_{}'.format(state)
            self.log.debug("init: creating {}".format(path))
            metrics[path] = Metric(
                'gauge',
                path,
                'PG {}'.format(state),
            )
        for state in DF_CLUSTER:
            path = 'cluster_{}'.format(state)
            self.log.debug("init: creating {}".format(path))
            metrics[path] = Metric(
                'gauge',
                path,
                'DF {}'.format(state),
            )
        for state in DF_POOL:
            path = 'pool_{}'.format(state)
            self.log.debug("init: creating {}".format(path))
            metrics[path] = Metric(
                'gauge',
                path,
                'DF pool {}'.format(state),
                ('pool_id',)
            )

        return metrics

    def shutdown(self):
        self.serving = False
        pass

    def get_health(self):
        health = json.loads(self.get('health')['json'])
        self.metrics['health_status'].set(
            health_status_to_number(health['status'])
        )

    def get_df(self):
        # maybe get the to-be-exported metrics from a config?
        df = self.get('df')
        for stat in DF_CLUSTER:
            path = 'cluster_{}'.format(stat)
            self.metrics[path].set(df['stats'][stat])

        for pool in df['pools']:
            for stat in DF_POOL:
                path = 'pool_{}'.format(stat)
                self.metrics[path].set(pool['stats'][stat], (pool['id'],))

    def get_quorum_status(self):
        mon_status = json.loads(self.get('mon_status')['json'])
        self.metrics['mon_quorum_count'].set(len(mon_status['quorum']))

    def get_pg_status(self):
        # TODO add per pool status?
        pg_s = self.get('pg_summary')['all']
        reported_pg_s = [(s,v) for key, v in pg_s.items() for s in
                         key.split('+')]
        for state, value in reported_pg_s:
            path = 'pg_{}'.format(state)
            self.metrics[path].set(value)
        reported_states = [s[0] for s in reported_pg_s]
        for state in PG_STATES:
            path = 'pg_{}'.format(state)
            if state not in reported_states:
                self.metrics[path].set(0)

    def get_metadata(self):
        osd_map = self.get('osd_map')
        osd_dev = self.get('osd_map_crush')['devices']
        for osd in osd_map['osds']:
            id_ = osd['osd']
            p_addr = osd['public_addr']
            c_addr = osd['cluster_addr']
            w = osd['weight']
            dev_class = next((osd for osd in osd_dev if osd['id'] == id_))
            self.metrics['osd_metadata'].set(0, (
                c_addr,
                dev_class['class'],
                id_,
                p_addr,
                w
            ))

        for pool in osd_map['pools']:
            id_ = pool['pool']
            name = pool['pool_name']
            self.metrics['pool_metadata'].set(0, (id_, name))

    def collect(self):
        self.get_health()
        self.get_df()
        self.get_quorum_status()
        self.get_metadata()
        self.get_pg_status()

        for daemon, counters in self.get_all_perf_counters().iteritems():
            for path, counter_info in counters.items():
                stattype = stattype_to_str(counter_info['type'])
                # XXX simplify first effort: no histograms
                # averages are already collapsed to one value for us
                if not stattype or stattype == 'histogram':
                    self.log.debug('ignoring %s, type %s' % (path, stattype))
                    continue

                if path not in self.metrics:
                    self.metrics[path] = Metric(
                        stattype,
                        path,
                        counter_info['description'],
                        ("ceph_daemon",),
                    )

                self.metrics[path].set(
                    counter_info['value'],
                    (daemon,)
                )

        return self.metrics

    def serve(self):

        class Root(object):

            # collapse everything to '/'
            def _cp_dispatch(self, vpath):
                cherrypy.request.path = ''
                return self

            def format_metrics(self, metrics):
                formatted = ''
                for m in metrics.values():
                    formatted += m.str_expfmt()
                return formatted + '\n'

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
                metrics = global_instance().collect()
                cherrypy.response.headers['Content-Type'] = 'text/plain'
                if metrics:
                    return self.format_metrics(metrics)

        server_addr = self.get_localized_config('server_addr', DEFAULT_ADDR)
        server_port = self.get_localized_config('server_port', DEFAULT_PORT)
        self.log.info(
            "server_addr: %s server_port: %s" %
            (server_addr, server_port)
        )

        cherrypy.config.update({
            'server.socket_host': server_addr,
            'server.socket_port': server_port,
            'engine.autoreload.on': False
        })
        cherrypy.tree.mount(Root(), "/")
        cherrypy.engine.start()
        cherrypy.engine.block()
