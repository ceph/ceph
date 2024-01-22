import cherrypy
from collections import defaultdict
from pkg_resources import packaging  # type: ignore
import json
import math
import os
import re
import threading
import time
import enum
from mgr_module import CLIReadCommand, MgrModule, MgrStandbyModule, PG_STATES, Option, ServiceInfoT, HandleCommandResult, CLIWriteCommand
from mgr_util import get_default_addr, profile_method, build_url
from rbd import RBD
from collections import namedtuple
import yaml

from typing import DefaultDict, Optional, Dict, Any, Set, cast, Tuple, Union, List, Callable

LabelValues = Tuple[str, ...]
Number = Union[int, float]
MetricValue = Dict[LabelValues, Number]

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
    Version = packaging.version.Version
    v = Version(cherrypy.__version__)
    # the issue was fixed in 3.2.3. it's present in 3.2.2 (current version on
    # centos:7) and back to at least 3.0.0.
    if Version("3.1.2") <= v < Version("3.2.3"):
        # https://github.com/cherrypy/cherrypy/issues/1100
        from cherrypy.process import servers
        servers.wait_for_occupied_port = lambda host, port: None


# cherrypy likes to sys.exit on error.  don't let it take us down too!
def os_exit_noop(status: int) -> None:
    pass


os._exit = os_exit_noop   # type: ignore

# to access things in class Module from subclass Root.  Because
# it's a dict, the writer doesn't need to declare 'global' for access

_global_instance = None  # type: Optional[Module]
cherrypy.config.update({
    'response.headers.server': 'Ceph-Prometheus'
})


def health_status_to_number(status: str) -> int:
    if status == 'HEALTH_OK':
        return 0
    elif status == 'HEALTH_WARN':
        return 1
    elif status == 'HEALTH_ERR':
        return 2
    raise ValueError(f'unknown status "{status}"')


DF_CLUSTER = ['total_bytes', 'total_used_bytes', 'total_used_raw_bytes']

DF_POOL = ['max_avail', 'avail_raw', 'stored', 'stored_raw', 'objects', 'dirty',
           'quota_bytes', 'quota_objects', 'rd', 'rd_bytes', 'wr', 'wr_bytes',
           'compress_bytes_used', 'compress_under_bytes', 'bytes_used', 'percent_used']

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

POOL_METADATA = ('pool_id', 'name', 'type', 'description', 'compression_mode')

RGW_METADATA = ('ceph_daemon', 'hostname', 'ceph_version', 'instance_id')

RBD_MIRROR_METADATA = ('ceph_daemon', 'id', 'instance_id', 'hostname',
                       'ceph_version')

DISK_OCCUPATION = ('ceph_daemon', 'device', 'db_device',
                   'wal_device', 'instance', 'devices', 'device_ids')

NUM_OBJECTS = ['degraded', 'misplaced', 'unfound']

alert_metric = namedtuple('alert_metric', 'name description')
HEALTH_CHECKS = [
    alert_metric('SLOW_OPS', 'OSD or Monitor requests taking a long time to process'),
]

HEALTHCHECK_DETAIL = ('name', 'severity')


class Severity(enum.Enum):
    ok = "HEALTH_OK"
    warn = "HEALTH_WARN"
    error = "HEALTH_ERR"


class Format(enum.Enum):
    plain = 'plain'
    json = 'json'
    json_pretty = 'json-pretty'
    yaml = 'yaml'


class HealthCheckEvent:

    def __init__(self, name: str, severity: Severity, first_seen: float, last_seen: float, count: int, active: bool = True):
        self.name = name
        self.severity = severity
        self.first_seen = first_seen
        self.last_seen = last_seen
        self.count = count
        self.active = active

    def as_dict(self) -> Dict[str, Any]:
        """Return the instance as a dictionary."""
        return self.__dict__


class HealthHistory:
    kv_name = 'health_history'
    titles = "{healthcheck_name:<24}  {first_seen:<20}  {last_seen:<20}  {count:>5}  {active:^6}"
    date_format = "%Y/%m/%d %H:%M:%S"

    def __init__(self, mgr: MgrModule):
        self.mgr = mgr
        self.lock = threading.Lock()
        self.healthcheck: Dict[str, HealthCheckEvent] = {}
        self._load()

    def _load(self) -> None:
        """Load the current state from the mons KV store."""
        data = self.mgr.get_store(self.kv_name)
        if data:
            try:
                healthcheck_data = json.loads(data)
            except json.JSONDecodeError:
                self.mgr.log.warn(
                    f"INVALID data read from mgr/prometheus/{self.kv_name}. Resetting")
                self.reset()
                return
            else:
                for k, v in healthcheck_data.items():
                    self.healthcheck[k] = HealthCheckEvent(
                        name=k,
                        severity=v.get('severity'),
                        first_seen=v.get('first_seen', 0),
                        last_seen=v.get('last_seen', 0),
                        count=v.get('count', 1),
                        active=v.get('active', True))
        else:
            self.reset()

    def reset(self) -> None:
        """Reset the healthcheck history."""
        with self.lock:
            self.mgr.set_store(self.kv_name, "{}")
            self.healthcheck = {}

    def save(self) -> None:
        """Save the current in-memory healthcheck history to the KV store."""
        with self.lock:
            self.mgr.set_store(self.kv_name, self.as_json())

    def check(self, health_checks: Dict[str, Any]) -> None:
        """Look at the current health checks and compare existing the history.

        Args:
            health_checks (Dict[str, Any]): current health check data
        """

        current_checks = health_checks.get('checks', {})
        changes_made = False

        # first turn off any active states we're tracking
        for seen_check in self.healthcheck:
            check = self.healthcheck[seen_check]
            if check.active and seen_check not in current_checks:
                check.active = False
                changes_made = True

        # now look for any additions to track
        now = time.time()
        for name, info in current_checks.items():
            if name not in self.healthcheck:
                # this healthcheck is new, so start tracking it
                changes_made = True
                self.healthcheck[name] = HealthCheckEvent(
                    name=name,
                    severity=info.get('severity'),
                    first_seen=now,
                    last_seen=now,
                    count=1,
                    active=True
                )
            else:
                # seen it before, so update its metadata
                check = self.healthcheck[name]
                if check.active:
                    # check has been registered as active already, so skip
                    continue
                else:
                    check.last_seen = now
                    check.count += 1
                    check.active = True
                    changes_made = True

        if changes_made:
            self.save()

    def __str__(self) -> str:
        """Print the healthcheck history.

        Returns:
            str: Human readable representation of the healthcheck history
        """
        out = []

        if len(self.healthcheck.keys()) == 0:
            out.append("No healthchecks have been recorded")
        else:
            out.append(self.titles.format(
                healthcheck_name="Healthcheck Name",
                first_seen="First Seen (UTC)",
                last_seen="Last seen (UTC)",
                count="Count",
                active="Active")
            )
            for k in sorted(self.healthcheck.keys()):
                check = self.healthcheck[k]
                out.append(self.titles.format(
                    healthcheck_name=check.name,
                    first_seen=time.strftime(self.date_format, time.localtime(check.first_seen)),
                    last_seen=time.strftime(self.date_format, time.localtime(check.last_seen)),
                    count=check.count,
                    active="Yes" if check.active else "No")
                )
            out.extend([f"{len(self.healthcheck)} health check(s) listed", ""])

        return "\n".join(out)

    def as_dict(self) -> Dict[str, Any]:
        """Return the history in a dictionary.

        Returns:
            Dict[str, Any]: dictionary indexed by the healthcheck name
        """
        return {name: self.healthcheck[name].as_dict() for name in self.healthcheck}

    def as_json(self, pretty: bool = False) -> str:
        """Return the healthcheck history object as a dict (JSON).

        Args:
            pretty (bool, optional): whether to json pretty print the history. Defaults to False.

        Returns:
            str: str representation of the healthcheck in JSON format
        """
        if pretty:
            return json.dumps(self.as_dict(), indent=2)
        else:
            return json.dumps(self.as_dict())

    def as_yaml(self) -> str:
        """Return the healthcheck history in yaml format.

        Returns:
            str: YAML representation of the healthcheck history
        """
        return yaml.safe_dump(self.as_dict(), explicit_start=True, default_flow_style=False)


class Metric(object):
    def __init__(self, mtype: str, name: str, desc: str, labels: Optional[LabelValues] = None) -> None:
        self.mtype = mtype
        self.name = name
        self.desc = desc
        self.labelnames = labels  # tuple if present
        self.value: Dict[LabelValues, Number] = {}

    def clear(self) -> None:
        self.value = {}

    def set(self, value: Number, labelvalues: Optional[LabelValues] = None) -> None:
        # labelvalues must be a tuple
        labelvalues = labelvalues or ('',)
        self.value[labelvalues] = value

    def str_expfmt(self) -> str:

        def promethize(path: str) -> str:
            ''' replace illegal metric name characters '''
            result = re.sub(r'[./\s]|::', '_', path).replace('+', '_plus')

            # Hyphens usually turn into underscores, unless they are
            # trailing
            if result.endswith("-"):
                result = result[0:-1] + "_minus"
            else:
                result = result.replace("-", "_")

            return "ceph_{0}".format(result)

        def floatstr(value: float) -> str:
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

    def group_by(
        self,
        keys: List[str],
        joins: Dict[str, Callable[[List[str]], str]],
        name: Optional[str] = None,
    ) -> "Metric":
        """
        Groups data by label names.

        Label names not passed are being removed from the resulting metric but
        by providing a join function, labels of metrics can be grouped.

        The purpose of this method is to provide a version of a metric that can
        be used in matching where otherwise multiple results would be returned.

        As grouping is possible in Prometheus, the only additional value of this
        method is the possibility to join labels when grouping. For that reason,
        passing joins is required. Please use PromQL expressions in all other
        cases.

        >>> m = Metric('type', 'name', '', labels=('label1', 'id'))
        >>> m.value = {
        ...     ('foo', 'x'): 1,
        ...     ('foo', 'y'): 1,
        ... }
        >>> m.group_by(['label1'], {'id': lambda ids: ','.join(ids)}).value
        {('foo', 'x,y'): 1}

        The functionality of group by could roughly be compared with Prometheus'

            group (ceph_disk_occupation) by (device, instance)

        with the exception that not all labels which aren't used as a condition
        to group a metric are discarded, but their values can are joined and the
        label is thereby preserved.

        This function takes the value of the first entry of a found group to be
        used for the resulting value of the grouping operation.

        >>> m = Metric('type', 'name', '', labels=('label1', 'id'))
        >>> m.value = {
        ...     ('foo', 'x'): 555,
        ...     ('foo', 'y'): 10,
        ... }
        >>> m.group_by(['label1'], {'id': lambda ids: ','.join(ids)}).value
        {('foo', 'x,y'): 555}
        """
        assert self.labelnames, "cannot match keys without label names"
        for key in keys:
            assert key in self.labelnames, "unknown key: {}".format(key)
        assert joins, "joins must not be empty"
        assert all(callable(c) for c in joins.values()), "joins must be callable"

        # group
        grouped: Dict[LabelValues, List[Tuple[Dict[str, str], Number]]] = defaultdict(list)
        for label_values, metric_value in self.value.items():
            labels = dict(zip(self.labelnames, label_values))
            if not all(k in labels for k in keys):
                continue
            group_key = tuple(labels[k] for k in keys)
            grouped[group_key].append((labels, metric_value))

        # as there is nothing specified on how to join labels that are not equal
        # and Prometheus `group` aggregation functions similarly, we simply drop
        # those labels.
        labelnames = tuple(
            label for label in self.labelnames if label in keys or label in joins
        )
        superfluous_labelnames = [
            label for label in self.labelnames if label not in labelnames
        ]

        # iterate and convert groups with more than one member into a single
        # entry
        values: MetricValue = {}
        for group in grouped.values():
            labels, metric_value = group[0]

            for label in superfluous_labelnames:
                del labels[label]

            if len(group) > 1:
                for key, fn in joins.items():
                    labels[key] = fn(list(labels[key] for labels, _ in group))

            values[tuple(labels.values())] = metric_value

        new_metric = Metric(self.mtype, name if name else self.name, self.desc, labelnames)
        new_metric.value = values

        return new_metric


class MetricCounter(Metric):
    def __init__(self,
                 name: str,
                 desc: str,
                 labels: Optional[LabelValues] = None) -> None:
        super(MetricCounter, self).__init__('counter', name, desc, labels)
        self.value = defaultdict(lambda: 0)

    def clear(self) -> None:
        pass  # Skip calls to clear as we want to keep the counters here.

    def set(self,
            value: Number,
            labelvalues: Optional[LabelValues] = None) -> None:
        msg = 'This method must not be used for instances of MetricCounter class'
        raise NotImplementedError(msg)

    def add(self,
            value: Number,
            labelvalues: Optional[LabelValues] = None) -> None:
        # labelvalues must be a tuple
        labelvalues = labelvalues or ('',)
        self.value[labelvalues] += value


class MetricCollectionThread(threading.Thread):
    def __init__(self, module: 'Module') -> None:
        self.mod = module
        self.active = True
        self.event = threading.Event()
        super(MetricCollectionThread, self).__init__(target=self.collect)

    def collect(self) -> None:
        self.mod.log.info('starting metric collection thread')
        while self.active:
            self.mod.log.debug('collecting cache in thread')
            if self.mod.have_mon_connection():
                start_time = time.time()

                try:
                    data = self.mod.collect()
                except Exception:
                    # Log any issues encountered during the data collection and continue
                    self.mod.log.exception("failed to collect metrics:")
                    self.event.wait(self.mod.scrape_interval)
                    continue

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

                self.event.wait(sleep_time)
            else:
                self.mod.log.error('No MON connection')
                self.event.wait(self.mod.scrape_interval)

    def stop(self) -> None:
        self.active = False
        self.event.set()


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(
            'server_addr'
        ),
        Option(
            'server_port',
            type='int',
            default=DEFAULT_PORT,
            desc='the port on which the module listens for HTTP requests',
            runtime=True
        ),
        Option(
            'scrape_interval',
            type='float',
            default=15.0
        ),
        Option(
            'stale_cache_strategy',
            default='log'
        ),
        Option(
            'cache',
            type='bool',
            default=True,
        ),
        Option(
            'rbd_stats_pools',
            default=''
        ),
        Option(
            name='rbd_stats_pools_refresh_interval',
            type='int',
            default=300
        ),
        Option(
            name='standby_behaviour',
            type='str',
            default='default',
            enum_allowed=['default', 'error'],
            runtime=True
        ),
        Option(
            name='standby_error_status_code',
            type='int',
            default=500,
            min=400,
            max=599,
            runtime=True
        )
    ]

    STALE_CACHE_FAIL = 'fail'
    STALE_CACHE_RETURN = 'return'

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.metrics = self._setup_static_metrics()
        self.shutdown_event = threading.Event()
        self.collect_lock = threading.Lock()
        self.collect_time = 0.0
        self.scrape_interval: float = 15.0
        self.cache = True
        self.stale_cache_strategy: str = self.STALE_CACHE_FAIL
        self.collect_cache: Optional[str] = None
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
        self.metrics_thread = MetricCollectionThread(_global_instance)
        self.health_history = HealthHistory(self)

    def _setup_static_metrics(self) -> Dict[str, Metric]:
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

        metrics['disk_occupation_human'] = Metric(
            'untyped',
            'disk_occupation_human',
            'Associate Ceph daemon with disk used for displaying to humans,'
            ' not for joining tables (vector matching)',
            DISK_OCCUPATION,  # label names are automatically decimated on grouping
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

        metrics['health_detail'] = Metric(
            'gauge',
            'health_detail',
            'healthcheck status by type (0=inactive, 1=active)',
            HEALTHCHECK_DETAIL
        )

        metrics['pool_objects_repaired'] = Metric(
            'counter',
            'pool_objects_repaired',
            'Number of objects repaired in a pool',
            ('pool_id',)
        )

        metrics['daemon_health_metrics'] = Metric(
            'gauge',
            'daemon_health_metrics',
            'Health metrics for Ceph daemons',
            ('type', 'ceph_daemon',)
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
                'counter' if state in ('rd', 'rd_bytes', 'wr', 'wr_bytes') else 'gauge',
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

        for check in HEALTH_CHECKS:
            path = 'healthcheck_{}'.format(check.name.lower())
            metrics[path] = Metric(
                'gauge',
                path,
                check.description,
            )

        return metrics

    def get_server_addr(self) -> str:
        """
        Return the current mgr server IP.
        """
        server_addr = cast(str, self.get_localized_module_option('server_addr', get_default_addr()))
        if server_addr in ['::', '0.0.0.0']:
            return self.get_mgr_ip()
        return server_addr

    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.
        """
        # https://stackoverflow.com/questions/7254845/change-cherrypy-port-and-restart-web-server
        # if we omit the line: cherrypy.server.httpserver = None
        # then the cherrypy server is not restarted correctly
        self.log.info('Restarting engine...')
        cherrypy.engine.stop()
        cherrypy.server.httpserver = None
        server_port = cast(int, self.get_localized_module_option('server_port', DEFAULT_PORT))
        self.set_uri(build_url(scheme='http', host=self.get_server_addr(), port=server_port, path='/'))
        cherrypy.config.update({'server.socket_port': server_port})
        cherrypy.engine.start()
        self.log.info('Engine started.')

    @profile_method()
    def get_health(self) -> None:

        def _get_value(message: str, delim: str = ' ', word_pos: int = 0) -> Tuple[int, int]:
            """Extract value from message (default is 1st field)"""
            v_str = message.split(delim)[word_pos]
            if v_str.isdigit():
                return int(v_str), 0
            return 0, 1

        health = json.loads(self.get('health')['json'])
        # set overall health
        self.metrics['health_status'].set(
            health_status_to_number(health['status'])
        )

        # Examine the health to see if any health checks triggered need to
        # become a specific metric with a value from the health detail
        active_healthchecks = health.get('checks', {})
        active_names = active_healthchecks.keys()

        for check in HEALTH_CHECKS:
            path = 'healthcheck_{}'.format(check.name.lower())

            if path in self.metrics:

                if check.name in active_names:
                    check_data = active_healthchecks[check.name]
                    message = check_data['summary'].get('message', '')
                    v, err = 0, 0

                    if check.name == "SLOW_OPS":
                        # 42 slow ops, oldest one blocked for 12 sec, daemons [osd.0, osd.3] have
                        # slow ops.
                        v, err = _get_value(message)

                    if err:
                        self.log.error(
                            "healthcheck %s message format is incompatible and has been dropped",
                            check.name)
                        # drop the metric, so it's no longer emitted
                        del self.metrics[path]
                        continue
                    else:
                        self.metrics[path].set(v)
                else:
                    # health check is not active, so give it a default of 0
                    self.metrics[path].set(0)

        self.health_history.check(health)
        for name, info in self.health_history.healthcheck.items():
            v = 1 if info.active else 0
            self.metrics['health_detail'].set(
                v, (
                    name,
                    str(info.severity))
            )

    @profile_method()
    def get_pool_stats(self) -> None:
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
    def get_df(self) -> None:
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
    def get_fs(self) -> None:
        fs_map = self.get('fs_map')
        servers = self.get_service_list()
        self.log.debug('standbys: {}'.format(fs_map['standbys']))
        # export standby mds metadata, default standby fs_id is '-1'
        for standby in fs_map['standbys']:
            id_ = standby['name']
            host, version, _ = servers.get((id_, 'mds'), ('', '', ''))
            addr, rank = standby['addr'], standby['rank']
            self.metrics['mds_metadata'].set(1, (
                'mds.{}'.format(id_), '-1',
                cast(str, host),
                cast(str, addr),
                cast(str, rank),
                cast(str, version)
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
                host, version, _ = servers.get((id_, 'mds'), ('', '', ''))
                self.metrics['mds_metadata'].set(1, (
                    'mds.{}'.format(id_), fs['id'],
                    host, daemon['addr'],
                    daemon['rank'], version
                ))

    @profile_method()
    def get_quorum_status(self) -> None:
        mon_status = json.loads(self.get('mon_status')['json'])
        servers = self.get_service_list()
        for mon in mon_status['monmap']['mons']:
            rank = mon['rank']
            id_ = mon['name']
            mon_version = servers.get((id_, 'mon'), ('', '', ''))
            self.metrics['mon_metadata'].set(1, (
                'mon.{}'.format(id_), mon_version[0],
                mon['public_addr'].rsplit(':', 1)[0], rank,
                mon_version[1]
            ))
            in_quorum = int(rank in mon_status['quorum'])
            self.metrics['mon_quorum_status'].set(in_quorum, (
                'mon.{}'.format(id_),
            ))

    @profile_method()
    def get_mgr_status(self) -> None:
        mgr_map = self.get('mgr_map')
        servers = self.get_service_list()

        active = mgr_map['active_name']
        standbys = [s.get('name') for s in mgr_map['standbys']]

        all_mgrs = list(standbys)
        all_mgrs.append(active)

        all_modules = {module.get('name'): module.get('can_run')
                       for module in mgr_map['available_modules']}

        for mgr in all_mgrs:
            host, version, _ = servers.get((mgr, 'mgr'), ('', '', ''))
            if mgr == active:
                _state = 1
            else:
                _state = 0

            self.metrics['mgr_metadata'].set(1, (
                f'mgr.{mgr}', host, version
            ))
            self.metrics['mgr_status'].set(_state, (
                f'mgr.{mgr}',))
        always_on_modules = mgr_map['always_on_modules'].get(self.release_name, [])
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
    def get_pg_status(self) -> None:

        pg_summary = self.get('pg_summary')

        for pool in pg_summary['by_pool']:
            num_by_state: DefaultDict[str, int] = defaultdict(int)
            for state in PG_STATES:
                num_by_state[state] = 0

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
    def get_osd_stats(self) -> None:
        osd_stats = self.get('osd_stats')
        for osd in osd_stats['osd_stats']:
            id_ = osd['osd']
            for stat in OSD_STATS:
                val = osd['perf_stat'][stat]
                self.metrics['osd_{}'.format(stat)].set(val, (
                    'osd.{}'.format(id_),
                ))

    def get_service_list(self) -> Dict[Tuple[str, str], Tuple[str, str, str]]:
        ret = {}
        for server in self.list_servers():
            host = cast(str, server.get('hostname', ''))
            for service in cast(List[ServiceInfoT], server.get('services', [])):
                ret.update({(service['id'], service['type']): (host,
                                                               service.get('ceph_version', 'unknown'),
                                                               service.get('name', ''))})
        return ret

    @profile_method()
    def get_metadata_and_osd_status(self) -> None:
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
            p_addr = osd['public_addr'].rsplit(':', 1)[0]
            c_addr = osd['cluster_addr'].rsplit(':', 1)[0]
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

            osd_version = servers.get((str(id_), 'osd'), ('', '', ''))

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
                osd_version[0],
                obj_store,
                p_addr,
                osd_version[1]
            ))

            # collect osd status
            for state in OSD_STATUS:
                status = osd[state]
                self.metrics['osd_{}'.format(state)].set(status, (
                    'osd.{}'.format(id_),
                ))

            osd_dev_node = None
            osd_wal_dev_node = ''
            osd_db_dev_node = ''
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

        if 'disk_occupation' in self.metrics:
            try:
                self.metrics['disk_occupation_human'] = \
                    self.metrics['disk_occupation'].group_by(
                        ['device', 'instance'],
                        {'ceph_daemon': lambda daemons: ', '.join(daemons)},
                        name='disk_occupation_human',
                )
            except Exception as e:
                self.log.error(e)

        ec_profiles = osd_map.get('erasure_code_profiles', {})

        def _get_pool_info(pool: Dict[str, Any]) -> Tuple[str, str]:
            pool_type = 'unknown'
            description = 'unknown'

            if pool['type'] == 1:
                pool_type = "replicated"
                description = f"replica:{pool['size']}"
            elif pool['type'] == 3:
                pool_type = "erasure"
                name = pool.get('erasure_code_profile', '')
                profile = ec_profiles.get(name, {})
                if profile:
                    description = f"ec:{profile['k']}+{profile['m']}"
                else:
                    description = "ec:unknown"

            return pool_type, description

        for pool in osd_map['pools']:

            compression_mode = 'none'
            pool_type, pool_description = _get_pool_info(pool)

            if 'options' in pool:
                compression_mode = pool['options'].get('compression_mode', 'none')

            self.metrics['pool_metadata'].set(
                1, (
                    pool['pool'],
                    pool['pool_name'],
                    pool_type,
                    pool_description,
                    compression_mode)
            )

        # Populate other servers metadata
        for key, value in servers.items():
            service_id, service_type = key
            if service_type == 'rgw':
                hostname, version, name = value
                self.metrics['rgw_metadata'].set(
                    1,
                    ('{}.{}'.format(service_type, name),
                     hostname, version, service_id)
                )
            elif service_type == 'rbd-mirror':
                mirror_metadata = self.get_metadata('rbd-mirror', service_id)
                if mirror_metadata is None:
                    continue
                mirror_metadata['ceph_daemon'] = '{}.{}'.format(service_type,
                                                                service_id)
                rbd_mirror_metadata = cast(LabelValues,
                                           (mirror_metadata.get(k, '')
                                            for k in RBD_MIRROR_METADATA))
                self.metrics['rbd_mirror_metadata'].set(
                    1, rbd_mirror_metadata
                )

    @profile_method()
    def get_num_objects(self) -> None:
        pg_sum = self.get('pg_summary')['pg_stats_sum']['stat_sum']
        for obj in NUM_OBJECTS:
            stat = 'num_objects_{}'.format(obj)
            self.metrics[stat].set(pg_sum[stat])

    @profile_method()
    def get_rbd_stats(self) -> None:
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
        pools_string = cast(str, self.get_localized_module_option('rbd_stats_pools'))
        pool_keys = set()
        osd_map = self.get('osd_map')
        rbd_pools = [pool['pool_name'] for pool in osd_map['pools']
                     if 'rbd' in pool.get('application_metadata', {})]
        for x in re.split(r'[\s,]+', pools_string):
            if not x:
                continue

            s = x.split('/', 2)
            pool_name = s[0]
            namespace_name = None
            if len(s) == 2:
                namespace_name = s[1]

            if pool_name == "*":
                # collect for all pools
                for pool in rbd_pools:
                    pool_keys.add((pool, namespace_name))
            else:
                if pool_name in rbd_pools:
                    pool_keys.add((pool_name, namespace_name))  # avoids adding deleted pool

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

        if ('query' in self.rbd_stats
            and (pool_id_regex != self.rbd_stats['query']['key_descriptor'][0]['regex']
                 or namespace_regex != self.rbd_stats['query']['key_descriptor'][1]['regex'])):
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
                     'regex': r'^(?:rbd|journal)_data\.(?:([0-9]+)\.)?([^.]+)\.'},
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
        assert res
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

    def refresh_rbd_stats_pools(self, pools: Dict[str, Set[str]]) -> None:
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
                        if nspace_name and\
                           not rbd.namespace_exists(ioctx, nspace_name):
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

    def shutdown_rbd_stats(self) -> None:
        if 'query_id' in self.rbd_stats:
            self.remove_osd_perf_query(self.rbd_stats['query_id'])
            del self.rbd_stats['query_id']
            del self.rbd_stats['query']
        self.rbd_stats['pools'].clear()

    def add_fixed_name_metrics(self) -> None:
        """
        Add fixed name metrics from existing ones that have details in their names
        that should be in labels (not in name).
        For backward compatibility, a new fixed name metric is created (instead of replacing)
        and details are put in new labels.
        Intended for RGW sync perf. counters but extendable as required.
        See: https://tracker.ceph.com/issues/45311
        """
        new_metrics = {}
        for metric_path, metrics in self.metrics.items():
            # Address RGW sync perf. counters.
            match = re.search(r'^data-sync-from-(.*)\.', metric_path)
            if match:
                new_path = re.sub('from-([^.]*)', 'from-zone', metric_path)
                if new_path not in new_metrics:
                    new_metrics[new_path] = Metric(
                        metrics.mtype,
                        new_path,
                        metrics.desc,
                        cast(LabelValues, metrics.labelnames) + ('source_zone',)
                    )
                for label_values, value in metrics.value.items():
                    new_metrics[new_path].set(value, label_values + (match.group(1),))

        self.metrics.update(new_metrics)

    def get_collect_time_metrics(self) -> None:
        sum_metric = self.metrics.get('prometheus_collect_duration_seconds_sum')
        count_metric = self.metrics.get('prometheus_collect_duration_seconds_count')
        if sum_metric is None:
            sum_metric = MetricCounter(
                'prometheus_collect_duration_seconds_sum',
                'The sum of seconds took to collect all metrics of this exporter',
                ('method',))
            self.metrics['prometheus_collect_duration_seconds_sum'] = sum_metric
        if count_metric is None:
            count_metric = MetricCounter(
                'prometheus_collect_duration_seconds_count',
                'The amount of metrics gathered for this exporter',
                ('method',))
            self.metrics['prometheus_collect_duration_seconds_count'] = count_metric

        # Collect all timing data and make it available as metric, excluding the
        # `collect` method because it has not finished at this point and hence
        # there's no `_execution_duration` attribute to be found. The
        # `_execution_duration` attribute is added by the `profile_method`
        # decorator.
        for method_name, method in Module.__dict__.items():
            duration = getattr(method, '_execution_duration', None)
            if duration is not None:
                cast(MetricCounter, sum_metric).add(duration, (method_name,))
                cast(MetricCounter, count_metric).add(1, (method_name,))

    def get_pool_repaired_objects(self) -> None:
        dump = self.get('pg_dump')
        for stats in dump['pool_stats']:
            path = f'pool_objects_repaired{stats["poolid"]}'
            self.metrics[path] = Metric(
                'counter',
                'pool_objects_repaired',
                'Number of objects repaired in a pool Count',
                ('poolid',)
            )

            self.metrics[path].set(stats['stat_sum']['num_objects_repaired'],
                                   labelvalues=(stats['poolid'],))

    def get_all_daemon_health_metrics(self) -> None:
        daemon_metrics = self.get_daemon_health_metrics()
        self.log.debug('metrics jeje %s' % (daemon_metrics))
        for daemon_name, health_metrics in daemon_metrics.items():
            for health_metric in health_metrics:
                path = 'daemon_health_metrics'
                self.metrics[path].set(health_metric['value'], labelvalues=(
                    health_metric['type'], daemon_name,))

    @profile_method(True)
    def collect(self) -> str:
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
        self.get_pool_repaired_objects()
        self.get_num_objects()
        self.get_all_daemon_health_metrics()

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

        self.get_collect_time_metrics()

        # Return formatted metrics and clear no longer used data
        _metrics = [m.str_expfmt() for m in self.metrics.values()]
        for k in self.metrics.keys():
            self.metrics[k].clear()

        return ''.join(_metrics) + '\n'

    @CLIReadCommand('prometheus file_sd_config')
    def get_file_sd_config(self) -> Tuple[int, str, str]:
        '''
        Return file_sd compatible prometheus config for mgr cluster
        '''
        servers = self.list_servers()
        targets = []
        for server in servers:
            hostname = server.get('hostname', '')
            for service in cast(List[ServiceInfoT], server.get('services', [])):
                if service['type'] != 'mgr':
                    continue
                id_ = service['id']
                port = self._get_module_option('server_port', DEFAULT_PORT, id_)
                targets.append(f'{hostname}:{port}')
        ret = [
            {
                "targets": targets,
                "labels": {}
            }
        ]
        return 0, json.dumps(ret), ""

    def self_test(self) -> None:
        self.collect()
        self.get_file_sd_config()

    def serve(self) -> None:

        class Root(object):

            # collapse everything to '/'
            def _cp_dispatch(self, vpath: str) -> 'Root':
                cherrypy.request.path = ''
                return self

            @cherrypy.expose
            def index(self) -> str:
                return '''<!DOCTYPE html>
<html>
    <head><title>Ceph Exporter</title></head>
    <body>
        <h1>Ceph Exporter</h1>
        <p><a href='/metrics'>Metrics</a></p>
    </body>
</html>'''

            @cherrypy.expose
            def metrics(self) -> Optional[str]:
                # Lock the function execution
                assert isinstance(_global_instance, Module)
                with _global_instance.collect_lock:
                    return self._metrics(_global_instance)

            @staticmethod
            def _metrics(instance: 'Module') -> Optional[str]:
                if not self.cache:
                    self.log.debug('Cache disabled, collecting and returning without cache')
                    cherrypy.response.headers['Content-Type'] = 'text/plain'
                    return self.collect()

                # Return cached data if available
                if not instance.collect_cache:
                    raise cherrypy.HTTPError(503, 'No cached data available yet')

                def respond() -> Optional[str]:
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
                return None

        # Make the cache timeout for collecting configurable
        self.scrape_interval = cast(float, self.get_localized_module_option('scrape_interval'))

        self.stale_cache_strategy = cast(
            str, self.get_localized_module_option('stale_cache_strategy'))
        if self.stale_cache_strategy not in [self.STALE_CACHE_FAIL,
                                             self.STALE_CACHE_RETURN]:
            self.stale_cache_strategy = self.STALE_CACHE_FAIL

        server_addr = cast(str, self.get_localized_module_option(
            'server_addr', get_default_addr()))
        server_port = cast(int, self.get_localized_module_option(
            'server_port', DEFAULT_PORT))
        self.log.info(
            "server_addr: %s server_port: %s" %
            (server_addr, server_port)
        )

        self.cache = cast(bool, self.get_localized_module_option('cache', True))
        if self.cache:
            self.log.info('Cache enabled')
            self.metrics_thread.start()
        else:
            self.log.info('Cache disabled')

        cherrypy.config.update({
            'server.socket_host': server_addr,
            'server.socket_port': server_port,
            'engine.autoreload.on': False
        })
        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri(build_url(scheme='http', host=self.get_server_addr(), port=server_port, path='/'))

        cherrypy.tree.mount(Root(), "/")
        self.log.info('Starting engine...')
        cherrypy.engine.start()
        self.log.info('Engine started.')
        # wait for the shutdown event
        self.shutdown_event.wait()
        self.shutdown_event.clear()
        # tell metrics collection thread to stop collecting new metrics
        self.metrics_thread.stop()
        cherrypy.engine.stop()
        cherrypy.server.httpserver = None
        self.log.info('Engine stopped.')
        self.shutdown_rbd_stats()
        # wait for the metrics collection thread to stop
        self.metrics_thread.join()

    def shutdown(self) -> None:
        self.log.info('Stopping engine...')
        self.shutdown_event.set()

    @CLIReadCommand('healthcheck history ls')
    def _list_healthchecks(self, format: Format = Format.plain) -> HandleCommandResult:
        """List all the healthchecks being tracked

        The format options are parsed in ceph_argparse, before they get evaluated here so
        we can safely assume that what we have to process is valid. ceph_argparse will throw
        a ValueError if the cast to our Format class fails.

        Args:
            format (Format, optional): output format. Defaults to Format.plain.

        Returns:
            HandleCommandResult: return code, stdout and stderr returned to the caller
        """

        out = ""
        if format == Format.plain:
            out = str(self.health_history)
        elif format == Format.yaml:
            out = self.health_history.as_yaml()
        else:
            out = self.health_history.as_json(format == Format.json_pretty)

        return HandleCommandResult(retval=0, stdout=out)

    @CLIWriteCommand('healthcheck history clear')
    def _clear_healthchecks(self) -> HandleCommandResult:
        """Clear the healthcheck history"""
        self.health_history.reset()
        return HandleCommandResult(retval=0, stdout="healthcheck history cleared")


class StandbyModule(MgrStandbyModule):

    MODULE_OPTIONS = Module.MODULE_OPTIONS

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(StandbyModule, self).__init__(*args, **kwargs)
        self.shutdown_event = threading.Event()

    def serve(self) -> None:
        server_addr = self.get_localized_module_option(
            'server_addr', get_default_addr())
        server_port = self.get_localized_module_option(
            'server_port', DEFAULT_PORT)
        self.log.info("server_addr: %s server_port: %s" %
                      (server_addr, server_port))
        cherrypy.config.update({
            'server.socket_host': server_addr,
            'server.socket_port': server_port,
            'engine.autoreload.on': False,
            'request.show_tracebacks': False
        })

        module = self

        class Root(object):
            @cherrypy.expose
            def index(self) -> str:
                standby_behaviour = module.get_module_option('standby_behaviour')
                if standby_behaviour == 'default':
                    active_uri = module.get_active_uri()
                    return '''<!DOCTYPE html>
<html>
    <head><title>Ceph Exporter</title></head>
    <body>
        <h1>Ceph Exporter</h1>
        <p><a href='{}metrics'>Metrics</a></p>
    </body>
</html>'''.format(active_uri)
                else:
                    status = module.get_module_option('standby_error_status_code')
                    raise cherrypy.HTTPError(status, message="Keep on looking")

            @cherrypy.expose
            def metrics(self) -> str:
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
        cherrypy.server.httpserver = None
        self.log.info('Engine stopped.')

    def shutdown(self) -> None:
        self.log.info("Stopping engine...")
        self.shutdown_event.set()
        self.log.info("Stopped engine")
