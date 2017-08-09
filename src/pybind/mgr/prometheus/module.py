import cherrypy
import json
import math
import os
import time
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

OSD_STATS = ['kb', 'kb_used', 'num_snap_trimming', 'snap_trim_queue_len']

OSD_PERF_STATS = ['apply_latency_ms', 'commit_latency_ms']


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
        for state in PG_STATES:
            path = 'pg_{}'.format(state)
            self.log.debug("init: creating {}".format(path))
            metrics[path] = Metric(
                'gauge',
                path,
                'PG {}'.format(state),
                ('osd',),
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
                ('name', 'id')
            )
        for state in OSD_STATS + OSD_PERF_STATS:
            path = 'osd_{}'.format(state)
            self.log.debug("init: creating {}".format(path))
            metrics[path] = Metric(
                'gauge',
                path,
                'OSD {}'.format(state),
                ('osd','device_class')
            )

        return metrics

    def _get_ordered_schema(self, **kwargs):

        '''
        fetch an ordered-by-key performance counter schema
        ['perf_schema'][daemontype.id][countername] with keys
        'nick' (if present)
        'description'
        'type' (counter type....counter/gauge/avg/histogram/etc.)
        '''

        daemon_type = kwargs.get('daemon_type', '')
        daemon_id = kwargs.get('daemon_id', '')

        schema = self.get_perf_schema(daemon_type, daemon_id)
        if not schema:
            self.log.warning('_get_ordered_schema: no data')
            return

        new_schema = dict()
        for k1 in schema.keys():    # 'perf_schema', but assume only one
            for k2 in sorted(schema[k1].keys()):
                sorted_dict = OrderedDict(
                    sorted(schema[k1][k2].items(), key=lambda i: i[0])
                )
                new_schema[k2] = sorted_dict
        for k in sorted(new_schema.keys()):
            self.log.debug("updating schema for %s" % k)
            self.schema[k] = new_schema[k]

    def shutdown(self):
        self.serving = False
        pass

    # XXX duplicated from dashboard; factor out?
    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]
        else:
            return 0

    def get_stat(self, daemon, path):

        perfcounter = self.schema[daemon][path]
        stattype = stattype_to_str(perfcounter['type'])
        # XXX simplify first effort: no histograms
        # averages are already collapsed to one value for us
        if not stattype or stattype == 'histogram':
            self.log.debug('ignoring %s, type %s' % (path, stattype))
            return

        if path not in self.metrics:
            self.metrics[path] = Metric(
                stattype,
                path,
                perfcounter['description'],
                ('daemon',),
            )

        daemon_type, daemon_id = daemon.split('.')

        self.metrics[path].set(
            self.get_latest(daemon_type, daemon_id, path),
            (daemon,)
        )

    def get_health(self):
        health = json.loads(self.get('health')['json'])
        self.metrics['health_status'].set(
            health_status_to_number(health['status'])
        )

    def get_df(self):
        df = self.get('df')
        # maybe get the to-be-exported metrics from a config?
        for stat in DF_CLUSTER:
            path = 'cluster_{}'.format(stat)
            self.metrics[path].set(df['stats'][stat])

        for pool in df['pools']:
            for stat in DF_POOL:
                path = 'pool_{}'.format(stat)
                self.metrics[path].set(pool['stats'][stat], (pool['name'], pool['id']))

    def get_mon_status(self):
        mon_status = json.loads(self.get('mon_status')['json'])
        self.metrics['mon_quorum_count'].set(len(mon_status['quorum']))

    def get_osd_status(self):
        '''TODO add device_class label!!!'''
        osd_status = self.get('osd_stats')['osd_stats']
        osd_devs = self.get('osd_metadata')
        for osd in osd_status:
            id_ = osd['osd']
            dev_class = :q
            for stat in OSD_STATS:
                path = 'osd_{}'.format(stat)
                self.metrics[path].set(osd[stat], (id_,))
            for p_stat in OSD_PERF_STAT:
                path = 'osd_{}'.format(stat)
                self.metrics[path].set(osd['perf_stat'][stat], (id_,))

    def get_pg_status(self):
        pg_s = self.get('pg_summary')['by_osd']
        for osd in pg_s:
            reported_pg_s = [(s,v) for key, v in pg_s[osd].items() for s in
                             key.split('+')]
            for state in PG_STATES:
                path = 'pg_{}'.format(state)
                if state not in reported_pg_s:
                    self.metrics[path].set(0, ('osd.{}'.format(osd),))

    def collect(self):
        self.get_health()
        self.get_df()
        self.get_mon_status()
        self.get_osd_status()
        self.get_pg_status()
        # for daemon in self.schema.keys():
        #     for path in self.schema[daemon].keys():
        #         self.get_stat(daemon, path)
        return self.metrics

    def notify(self, ntype, nid):
        ''' Just try to sync and not run until we're notified once '''
        if not self.notified:
            self.serving = True
            self.notified = True
        if ntype == 'perf_schema_update':
            daemon_type, daemon_id = nid.split('.')
            self._get_ordered_schema(
                daemon_type=daemon_type,
                daemon_id=daemon_id
            )

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
        # wait for first notification (of any kind) to start up
        # while not self.serving:
        #     time.sleep(1)

        cherrypy.config.update({
            'server.socket_host': server_addr,
            'server.socket_port': server_port,
            'engine.autoreload.on': False
        })
        cherrypy.tree.mount(Root(), "/")
        cherrypy.engine.start()
        cherrypy.engine.block()
