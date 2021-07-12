import json
import logging
import threading
import time
import inspect
import re
from multiprocessing import Process
from threading import Event
from typing import Callable, Any

from mgr_module import CLICommand, HandleCommandResult, MgrModule

logger = logging.getLogger()

class CLI(MgrModule):

    NATIVE_OPTIONS = [
        'mgr_tick_period',
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run = True
        self.event = Event()
        self.mgr_tick_period = self.get_ceph_option("mgr_tick_period")
        logger = self.log
        

    def serve(self) -> None:
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.error("Starting")
        while self.run:
            # Do some useful background work here.

            # Use mgr_tick_period (default: 2) here just to illustrate
            # consuming native ceph options.  Any real background work
            # would presumably have some more appropriate frequency.
            sleep_interval = self.mgr_tick_period
            self.log.error('Sleeping for %d seconds', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

    def get_method(self, arg: str):
        def wrapper():
            return self.get(arg)
        return wrapper
        
class API:
    def DecoratorFactory(attr: str, default: Any):
        class DecoratorClass:
            _ATTR_TOKEN = f'__ATTR_{attr.upper()}__'

            def __init__(self, value=default):
                self.value = value

            def __call__(self, func):
                setattr(func, self._ATTR_TOKEN, self.value)
                return func

            @classmethod
            def get(cls, func):
                return getattr(func, cls._ATTR_TOKEN, default)

        return DecoratorClass

    hook = DecoratorFactory('hook', default=False)(True)
    perm = DecoratorFactory('perm', default='r')
    internal = DecoratorFactory('internal', default=False)(True)

class MgrModuleHooks:
    COMMANDS = []  # type: List[Any]
    MODULE_OPTIONS = []  # type: List[dict]
    MODULE_OPTION_DEFAULTS = {}  # type: Dict[str, Any]

    @API.hook
    def notify(self, notify_type, notify_id):
        """
        Called by the ceph-mgr service to notify the Python plugin
        that new state is available.
        :param notify_type: string indicating what kind of notification,
                            such as osd_map, mon_map, fs_map, mon_status,
                            health, pg_summary, command, service_map
        :param notify_id:  string (may be empty) that optionally specifies
                            which entity is being notified about.  With
                            "command" notifications this is set to the tag
                            ``from send_command``.
        """
        pass

    @API.hook
    def config_notify(self):
        """
        Called by the ceph-mgr service to notify the Python plugin
        that the configuration may have changed.  Modules will want to
        refresh any configuration values stored in config variables.
        """
        pass

    @API.hook
    def handle_command(self, inbuf, cmd):
        """
        Called by ceph-mgr to request the plugin to handle one
        of the commands that it declared in self.COMMANDS
        Return a status code, an output buffer, and an
        output string.  The output buffer is for data results,
        the output string is for informative text.
        :param inbuf: content of any "-i <file>" supplied to ceph cli
        :type inbuf: str
        :param cmd: from Ceph's cmdmap_t
        :type cmd: dict
        :return: HandleCommandResult or a 3-tuple of (int, str, str)
        """

        # Should never get called if they didn't declare
        # any ``COMMANDS``
        raise NotImplementedError()

    @API.hook
    def serve(self):
        """
        Called by the ceph-mgr service to start any server that
        is provided by this Python plugin.  The implementation
        of this function should block until ``shutdown`` is called.
        You *must* implement ``shutdown`` if you implement ``serve``
        """
        pass

    @staticmethod
    @API.hook
    def can_run():
        """
        Implement this function to report whether the module's dependencies
        are met.  For example, if the module needs to import a particular
        dependency to work, then use a try/except around the import at
        file scope, and then report here if the import failed.
        This will be called in a blocking way from the C++ code, so do not
        do any I/O that could block in this function.
        :return a 2-tuple consisting of a boolean and explanatory string
        """

        return True, ""

    @API.hook
    def shutdown(self):
        """
        Called by the ceph-mgr service to request that this
        module drop out of its serve() function.  You do not
        need to implement this if you do not implement serve()
        :return: None
        """
        if self._rados:
            addrs = self._rados.get_addrs()
            self._rados.shutdown()
            self.unregister_client(addrs)

class MgrModule():
    # Priority definitions for perf counters
    PRIO_CRITICAL = 10
    PRIO_INTERESTING = 8
    PRIO_USEFUL = 5
    PRIO_UNINTERESTING = 2
    PRIO_DEBUGONLY = 0
    # counter value types
    PERFCOUNTER_TIME = 1
    PERFCOUNTER_U64 = 2
    # counter types
    PERFCOUNTER_LONGRUNAVG = 4
    PERFCOUNTER_COUNTER = 8
    PERFCOUNTER_HISTOGRAM = 0x10
    PERFCOUNTER_TYPE_MASK = ~3
    # units supported
    BYTES = 0
    NONE = 1
    # Cluster log priorities
    CLUSTER_LOG_PRIO_DEBUG = 0
    CLUSTER_LOG_PRIO_INFO = 1
    CLUSTER_LOG_PRIO_SEC = 2
    CLUSTER_LOG_PRIO_WARN = 3
    CLUSTER_LOG_PRIO_ERROR = 4

    def __init__(self, module_name, py_modules_ptr, this_ptr):
        self.module_name = module_name
        super(MgrModule, self).__init__(py_modules_ptr, this_ptr)
        for o in self.MODULE_OPTIONS:
            if 'default' in o:
                if 'type' in o:
                    # we'll assume the declared type matches the
                    # supplied default value's type.
                    self.MODULE_OPTION_DEFAULTS[o['name']] = o['default']
                else:
                    # module not declaring it's type, so normalize the
                    # default value to be a string for consistent behavior
                    # with default and user-supplied option values.
                    self.MODULE_OPTION_DEFAULTS[o['name']] = str(o['default'])
        mgr_level = self.get_ceph_option("debug_mgr")
        log_level = self.get_module_option("log_level")
        cluster_level = self.get_module_option('log_to_cluster_level')
        log_to_file = self.get_module_option("log_to_file")
        log_to_cluster = self.get_module_option("log_to_cluster")
        self._configure_logging(mgr_level, log_level, cluster_level,
                                log_to_file, log_to_cluster)
        # for backwards compatibility
        self._logger = self.getLogger()
        self._version = self.get_version()
        self._perf_schema_cache = None
        # Keep a librados instance for those that need it.
        self._rados = None

    def __del__(self):
        self._unconfigure_logging()

    @classmethod
    def _register_options(cls, module_name):
        cls.MODULE_OPTIONS.append(
            Option(name='log_level', type='str', default="", runtime=True,
                   enum_allowed=['info', 'debug', 'critical', 'error',
                                 'warning', '']))
        cls.MODULE_OPTIONS.append(
            Option(name='log_to_file', type='bool', default=False, runtime=True))
        if not [x for x in cls.MODULE_OPTIONS if x['name'] == 'log_to_cluster']:
            cls.MODULE_OPTIONS.append(
                Option(name='log_to_cluster', type='bool', default=False,
                       runtime=True))
        cls.MODULE_OPTIONS.append(
            Option(name='log_to_cluster_level', type='str', default='info',
                   runtime=True,
                   enum_allowed=['info', 'debug', 'critical', 'error',
                                 'warning', '']))
    @classmethod
    def _register_commands(cls, module_name):
        cls.COMMANDS.extend(CLICommand.dump_cmd_list())
    @property
    def log(self):
        return self._logger

    @API.perm('w')
    @CLICommand("mgr cli call cluster_log")
    def cluster_log(self, channel: str, priority: int, message: str):
        """
        Send message to the cluster log.
        :param channel: The log channel. This can be 'cluster', 'audit', ...
        :type channel: str
        :param priority: The log message priority. This can be
                         CLUSTER_LOG_PRIO_DEBUG, CLUSTER_LOG_PRIO_INFO,
                         CLUSTER_LOG_PRIO_SEC, CLUSTER_LOG_PRIO_WARN or
                         CLUSTER_LOG_PRIO_ERROR.
        :type priority: int
        :param message: The message to log.
        :type message: str
        """
        # return HandleCommandResult(channel,priority, message)
        self.cluster_log(channel, priority, message)
        
    @API.perm('w')
    @CLICommand('mgr cli call get')
    def api_get(self, arg: str):
        """
        Called by the plugin to fetch named cluster-wide objects from ceph-mgr.
        :param str data_name: Valid things to fetch are osd_crush_map_text,
                osd_map, osd_map_tree, osd_map_crush, config, mon_map, fs_map,
                osd_metadata, pg_summary, io_rate, pg_dump, df, osd_stats,
                health, mon_status, devices, device <devid>, pg_stats,
                pool_stats, pg_ready, osd_ping_times.
        Note:
            All these structures have their own JSON representations: experiment
            or look at the C++ ``dump()`` methods to learn about them.
        """
        str_arg = self.get(arg)
        return HandleCommandResult(0, json.dumps(str_arg))

    @property
    def version(self):
        return self._version

    @property
    @API.perm('w')
    @CLICommand('mgr cli call release-name')
    def release_name(self):
        """
        Get the release name of the Ceph version, e.g. 'nautilus' or 'octopus'.
        :return: Returns the release name of the Ceph version in lower case.
        :rtype: str
        """
        return self.get_release_name()

    @API.internal
    @API.perm('w')
    @CLICommand('mgr cli call get context')
    def get_context(self):
        """
        :return: a Python capsule containing a C++ CephContext pointer
        """
        return self.get_context()

    def _config_notify(self):
        # check logging options for changes
        mgr_level = self.get_ceph_option("debug_mgr")
        module_level = self.get_module_option("log_level")
        cluster_level = self.get_module_option("log_to_cluster_level")
        log_to_file = self.get_module_option("log_to_file", False)
        log_to_cluster = self.get_module_option("log_to_cluster", False)
        self._set_log_level(mgr_level, module_level, cluster_level)
        if log_to_file != self.log_to_file:
            if log_to_file:
                self._enable_file_log()
            else:
                self._disable_file_log()
        if log_to_cluster != self.log_to_cluster:
            if log_to_cluster:
                self._enable_cluster_log()
            else:
                self._disable_cluster_log()
        # call module subclass implementations
        self.config_notify()

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
    def _perfpath_to_path_labels(self, daemon, path):
        # type: (str, str) -> Tuple[str, Tuple[str, ...], Tuple[str, ...]]
        label_names = ("ceph_daemon",)  # type: Tuple[str, ...]
        labels = (daemon,)  # type: Tuple[str, ...]
        if daemon.startswith('rbd-mirror.'):
            match = re.match(
                r'^rbd_mirror_image_([^/]+)/(?:(?:([^/]+)/)?)(.*)\.(replay(?:_bytes|_latency)?)$',
                path
            )
            if match:
                path = 'rbd_mirror_image_' + match.group(4)
                pool = match.group(1)
                namespace = match.group(2) or ''
                image = match.group(3)
                label_names += ('pool', 'namespace', 'image')
                labels += (pool, namespace, image)
        return path, label_names, labels,
    def _perfvalue_to_value(self, stattype, value):
        if stattype & self.PERFCOUNTER_TIME:
            # Convert from ns to seconds
            return value / 1000000000.0
        else:
            return value
    def _unit_to_str(self, unit):
        if unit == self.NONE:
            return "/s"
        elif unit == self.BYTES:
            return "B/s"

    @API.internal
    @staticmethod
    def to_pretty_iec(n):
        for bits, suffix in [(60, 'Ei'), (50, 'Pi'), (40, 'Ti'), (30, 'Gi'),
                             (20, 'Mi'), (10, 'Ki')]:
            if n > 10 << bits:
                return str(n >> bits) + ' ' + suffix
        return str(n) + ' '

    @API.internal
    @staticmethod
    def get_pretty_row(elems, width):
        """
        Takes an array of elements and returns a string with those elements
        formatted as a table row. Useful for polling modules.
        :param elems: the elements to be printed
        :param width: the width of the terminal
        """
        n = len(elems)
        column_width = int(width / n)
        ret = '|'
        for elem in elems:
            ret += '{0:>{w}} |'.format(elem, w=column_width - 2)

        return ret
    
    @classmethod
    def get_pretty_header(cls, elems, width):
        """
        Like ``get_pretty_row`` but adds dashes, to be used as a table title.
        :param elems: the elements to be printed
        :param width: the width of the terminal
        """
        n = len(elems)
        column_width = int(width / n)
        # dash line
        ret = '+'
        for i in range(0, n):
            ret += '-' * (column_width - 1) + '+'
        ret += '\n'

        # title
        ret += cls.get_pretty_row(elems, width)
        ret += '\n'

        # dash line
        ret += '+'
        for i in range(0, n):
            ret += '-' * (column_width - 1) + '+'
        ret += '\n'

        return ret

    @API.perm('w')
    @CLICommand("mgr cli call get-server")
    def get_server(self, hostname: str):
        """
        Called by the plugin to fetch metadata about a particular hostname from
        ceph-mgr.
        This is information that ceph-mgr has gleaned from the daemon metadata
        reported by daemons running on a particular server.
        :param hostname: a hostname
        """
        self.log.error("Here Host", self.get_server(hostname))
        return self.get_server(hostname)

    @API.perm('w')
    @CLICommand("mgr cli call get-perf-schema")
    def get_perf_schema(self, svc_type: str, svc_name: str):
        """
        Called by the plugin to fetch perf counter schema info.
        svc_name can be nullptr, as can svc_type, in which case
        they are wildcards
        :param str svc_type:
        :param str svc_name:
        :return: list of dicts describing the counters requested
        """
        return self.get_perf_schema(svc_type, svc_name)

    @API.perm('w')
    @CLICommand("mgr cli call get-counter")
    def get_counter(self, svc_type: str, svc_name: str, path: str):
        """
        Called by the plugin to fetch the latest performance counter data for a
        particular counter on a particular service.
        :param str svc_type:
        :param str svc_name:
        :param str path: a period-separated concatenation of the subsystem and the
            counter name, for example "mds.inodes".
        :return: A list of two-tuples of (timestamp, value) is returned.  This may be
            empty if no data is available.
        """
        return self.get_counter(svc_type, svc_name, path)

    @API.perm('w')
    @CLICommand("mgr cli call get-latest-counter")
    def get_latest_counter(self, svc_type: str, svc_name: str, path: str):
        """
        Called by the plugin to fetch only the newest performance counter data
        pointfor a particular counter on a particular service.
        :param str svc_type:
        :param str svc_name:
        :param str path: a period-separated concatenation of the subsystem and the
            counter name, for example "mds.inodes".
        :return: A list of two-tuples of (timestamp, value) is returned.  This may be
            empty if no data is available.
        """
        return self.get_latest_counter(svc_type, svc_name, path)

    @API.perm('w')
    @CLICommand("mgr cli call get list-servers")
    def list_servers(self):
        """
        Like ``get_server``, but gives information about all servers (i.e. all
        unique hostnames that have been mentioned in daemon metadata)
        :return: a list of information about all servers
        :rtype: list
        """
        return self.get_server(None)
