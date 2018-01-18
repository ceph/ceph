import cherrypy
import json
import errno
import os
import socket
from collections import OrderedDict
from mgr_module import MgrModule, MgrStandbyModule

from metric import Metric, get_metrics_spec

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
        self.metrics_spec = get_metrics_spec()
        self.metrics = self._setup_static_metrics(self.metrics_spec)
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

    def _setup_static_metrics(self, metrics_spec):
        metrics = dict()
        for group_name, group in metrics_spec.items():
            for spec in group['metrics'].values():
                spec['mtype'] = spec.pop('type')
                metrics[spec['name']] = Metric(**spec)
        return metrics

    def _get_spec_group(self, group_name):
        group = self.metrics_spec[group_name]
        return group['metrics']

    def get_health(self):
        health = json.loads(self.get('health')['json'])
        self.metrics['health_status'].set(
            health_status_to_number(health['status'])
        )

    def get_df(self):
        df = self.get('df')
        for stat in self._get_spec_group('df_cluster').keys():
            path = 'cluster_{}'.format(stat)
            self.metrics[path].set(df['stats'][stat])

        for pool in df['pools']:
            for stat in self._get_spec_group('df_pool').keys():
                path = 'pool_{}'.format(stat)
                self.metrics[path].set(pool['stats'][stat], (pool['id'],))

    def get_quorum_status(self):
        mon_status = json.loads(self.get('mon_status')['json'])
        self.metrics['mon_quorum_count'].set(len(mon_status['quorum']))

    def get_pg_status(self):
        # TODO add per pool status?
        pg_s = self.get('pg_summary')['all']
        reported_pg_s = [(s, v) for key, v in pg_s.items() for s in
                         key.split('+')]
        for state, value in reported_pg_s:
            path = 'pg_{}'.format(state)
            try:
                self.metrics[path].set(value)
            except KeyError:
                self.log.warn("skipping pg in unknown state {}".format(state))
        reported_states = [s[0] for s in reported_pg_s]
        for state in self._get_spec_group('pg_states').keys():
            path = 'pg_{}'.format(state)
            if state not in reported_states:
                try:
                    self.metrics[path].set(0)
                except KeyError:
                    self.log.warn(
                        "skipping pg in unknown state {}".format(state))

    def get_osd_stats(self):
        osd_stats = self.get('osd_stats')
        for osd in osd_stats['osd_stats']:
            id_ = osd['osd']
            for stat in self._get_spec_group('osd_stats').keys():
                status = osd['perf_stat'][stat]
                self.metrics['osd_{}'.format(stat)].set(
                    status,
                    ('osd.{}'.format(id_),))

    def get_metadata_and_osd_status(self):
        osd_map = self.get('osd_map')
        osd_devices = self.get('osd_map_crush')['devices']
        for osd in osd_map['osds']:
            id_ = osd['osd']
            p_addr = osd['public_addr'].split(':')[0]
            c_addr = osd['cluster_addr'].split(':')[0]
            dev_class = next((osd for osd in osd_devices if osd['id'] == id_))
            self.metrics['osd_metadata'].set(1, (
                c_addr,
                dev_class['class'],
                id_,
                p_addr
            ))
            for state in self._get_spec_group('osd_status').keys():
                status = osd[state]
                self.metrics['osd_{}'.format(state)].set(
                    status,
                    ('osd.{}'.format(id_),))

            osd_metadata = self.get_metadata("osd", str(id_))
            dev_keys = ("backend_filestore_dev_node",
                        "bluestore_bdev_dev_node")
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
                self.metrics['disk_occupation'].set(1, (
                    osd_hostname,
                    osd_dev_node,
                    "osd.{0}".format(id_)
                ))
            else:
                self.log.info("Missing dev node metadata for osd {0}, "
                              "skipping occupation record for this "
                              "osd".format(id_))

        for pool in osd_map['pools']:
            id_ = pool['pool']
            name = pool['pool_name']
            self.metrics['pool_metadata'].set(1, (id_, name))

    def collect(self):
        self.get_health()
        self.get_df()
        self.get_osd_stats()
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
                if global_instance().have_mon_connection():
                    metrics = global_instance().collect()
                    cherrypy.response.headers['Content-Type'] = 'text/plain'
                    if metrics:
                        return self.format_metrics(metrics)
                else:
                    raise cherrypy.HTTPError(503, 'No MON connection')

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
        cherrypy.engine.block()

    def shutdown(self):
        self.log.info('Stopping engine...')
        cherrypy.engine.wait(state=cherrypy.engine.states.STARTED)
        cherrypy.engine.exit()
        self.log.info('Stopped engine')


class StandbyModule(MgrStandbyModule):
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
        self.log.info("Waiting for engine...")
        cherrypy.engine.wait(state=cherrypy.engine.states.STOPPED)
        self.log.info('Engine started.')

    def shutdown(self):
        self.log.info("Stopping engine...")
        cherrypy.engine.wait(state=cherrypy.engine.states.STARTED)
        cherrypy.engine.stop()
        self.log.info("Stopped engine")
