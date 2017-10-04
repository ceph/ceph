import cherrypy
import json
import errno
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

PG_STATES = ['creating', 'active', 'clean', 'down', 'scrubbing', 'degraded',
        'inconsistent', 'peering', 'repair', 'recovering', 'forced-recovery',
        'backfill', 'forced-backfill', 'wait-backfill', 'backfill-toofull',
        'incomplete', 'stale', 'remapped', 'undersized', 'peered']

DF_CLUSTER = ['total_bytes', 'total_used_bytes', 'total_objects']

DF_POOL = ['max_avail', 'bytes_used', 'raw_bytes_used', 'objects', 'dirty',
           'quota_bytes', 'quota_objects', 'rd', 'rd_bytes', 'wr', 'wr_bytes']

OSD_METADATA = ('cluster_addr', 'device_class', 'id', 'public_addr')

OSD_STATUS = ['weight', 'up', 'in']

POOL_METADATA = ('pool_id', 'name')

DISK_OCCUPATION = ('instance', 'device', 'ceph_daemon')


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
    COMMANDS = [
        {
            "cmd": "prometheus self-test",
            "desc": "Run a self test on the prometheus module",
            "perm": "rw"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.notified = False
        self.serving = False
        self.metrics = self._setup_static_metrics()
        self.schema = OrderedDict()
        _global_instance['plugin'] = self

    def _stattype_to_str(self, stattype):

        typeonly = stattype & self.PERFCOUNTER_TYPE_MASK
        if typeonly == 0:
            return 'gauge'
        if typeonly == self.PERFCOUNTER_LONGRUNAVG:
            # this lie matches the DaemonState decoding: only val, no counts
            return 'counter'
        if typeonly == self.PERFCOUNTER_COUNTER:
            return 'counter'
        if typeonly == self.PERFCOUNTER_HISTOGRAM:
            return 'histogram'

        return ''

    def _setup_static_metrics(self):
        metrics = {}
        metrics['health_status'] = Metric(
            'untyped',
            'health_status',
            'Cluster health status'
        )
        metrics['mon_quorum_count'] = Metric(
            'gauge',
            'mon_quorum_count',
            'Monitors in quorum'
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
            'undef',
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
        for state in OSD_STATUS:
            path = 'osd_{}'.format(state)
            self.log.debug("init: creating {}".format(path))
            metrics[path] = Metric(
                'untyped',
                path,
                'OSD status {}'.format(state),
                ('ceph_daemon',)
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

    def get_metadata_and_osd_status(self):
        osd_map = self.get('osd_map')
        osd_devices = self.get('osd_map_crush')['devices']
        for osd in osd_map['osds']:
            id_ = osd['osd']
            p_addr = osd['public_addr'].split(':')[0]
            c_addr = osd['cluster_addr'].split(':')[0]
            dev_class = next((osd for osd in osd_devices if osd['id'] == id_))
            self.metrics['osd_metadata'].set(0, (
                c_addr,
                dev_class['class'],
                id_,
                p_addr
            ))
            for state in OSD_STATUS:
                status = osd[state]
                self.metrics['osd_{}'.format(state)].set(
                    status,
                    ('osd.{}'.format(id_),))

            osd_metadata = self.get_metadata("osd", str(id_))
            dev_keys = ("backend_filestore_dev_node", "bluestore_bdev_dev_node")
            osd_dev_node = None
            for dev_key in dev_keys:
                val = osd_metadata.get(dev_key, None)
                if val and val != "unknown":
                    osd_dev_node = val
                    break
            osd_hostname = osd_metadata.get('hostname', None)
            if osd_dev_node and osd_hostname:
                self.log.debug("Got dev for osd {0}: {1}/{2}".format(
                    id_, osd_hostname, osd_dev_node))
                self.metrics['disk_occupation'].set(0, (
                    osd_hostname,
                    osd_dev_node,
                    "osd.{0}".format(id_)
                ))
            else:
                self.log.info("Missing dev node metadata for osd {0}, skipping "
                               "occupation record for this osd".format(id_))

        for pool in osd_map['pools']:
            id_ = pool['pool']
            name = pool['pool_name']
            self.metrics['pool_metadata'].set(0, (id_, name))

    def collect(self):
        self.get_health()
        self.get_df()
        self.get_quorum_status()
        self.get_metadata_and_osd_status()
        self.get_pg_status()

        for daemon, counters in self.get_all_perf_counters().iteritems():
            for path, counter_info in counters.items():
                stattype = self._stattype_to_str(counter_info['type'])
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
            'server.socket_port': int(server_port),
            'engine.autoreload.on': False
        })
        cherrypy.tree.mount(Root(), "/")
        cherrypy.engine.start()
        cherrypy.engine.block()
