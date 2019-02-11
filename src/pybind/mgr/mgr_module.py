
import ceph_module  # noqa

import logging
import json
import six
import threading
from collections import defaultdict, namedtuple
import rados
import time

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


class CPlusPlusHandler(logging.Handler):
    def __init__(self, module_inst):
        super(CPlusPlusHandler, self).__init__()
        self._module = module_inst

    def emit(self, record):
        if record.levelno <= logging.DEBUG:
            ceph_level = 20
        elif record.levelno <= logging.INFO:
            ceph_level = 4
        elif record.levelno <= logging.WARNING:
            ceph_level = 1
        else:
            ceph_level = 0

        self._module._ceph_log(ceph_level, self.format(record))


def configure_logger(module_inst, name):
    logger = logging.getLogger(name)

    # Don't filter any logs at the python level, leave it to C++
    logger.setLevel(logging.DEBUG)

    # FIXME: we should learn the log level from C++ land, and then
    # avoid calling the C++ level log when we know a message is of
    # an insufficient level to be ultimately output
    logger.addHandler(CPlusPlusHandler(module_inst))

    return logger


def unconfigure_logger(module_inst, name):
    logger = logging.getLogger(name)
    rm_handlers = [
        h for h in logger.handlers if isinstance(h, CPlusPlusHandler)]
    for h in rm_handlers:
        logger.removeHandler(h)


class CommandResult(object):
    """
    Use with MgrModule.send_command
    """

    def __init__(self, tag=None):
        self.ev = threading.Event()
        self.outs = ""
        self.outb = ""
        self.r = 0

        # This is just a convenience for notifications from
        # C++ land, to avoid passing addresses around in messages.
        self.tag = tag if tag else ""

    def complete(self, r, outb, outs):
        self.r = r
        self.outb = outb
        self.outs = outs
        self.ev.set()

    def wait(self):
        self.ev.wait()
        return self.r, self.outb, self.outs


class HandleCommandResult(namedtuple('HandleCommandResult', ['retval', 'stdout', 'stderr'])):
    def __new__(cls, retval=0, stdout="", stderr=""):
        """
        Tuple containing the result of `handle_command()`

        Only write to stderr if there is an error, or in extraordinary circumstances

        Avoid having `ceph foo bar` commands say "did foo bar" on success unless there
        is critical information to include there.

        Everything programmatically consumable should be put on stdout

        :param retval: return code. E.g. 0 or -errno.EINVAL
        :type retval: int
        :param stdout: data of this result.
        :type stdout: str
        :param stderr: Typically used for error messages.
        :type stderr: str
        """
        return super(HandleCommandResult, cls).__new__(cls, retval, stdout, stderr)


class OSDMap(ceph_module.BasePyOSDMap):
    def get_epoch(self):
        return self._get_epoch()

    def get_crush_version(self):
        return self._get_crush_version()

    def dump(self):
        return self._dump()

    def get_pools(self):
        # FIXME: efficient implementation
        d = self._dump()
        return dict([(p['pool'], p) for p in d['pools']])

    def get_pools_by_name(self):
        # FIXME: efficient implementation
        d = self._dump()
        return dict([(p['pool_name'], p) for p in d['pools']])

    def new_incremental(self):
        return self._new_incremental()

    def apply_incremental(self, inc):
        return self._apply_incremental(inc)

    def get_crush(self):
        return self._get_crush()

    def get_pools_by_take(self, take):
        return self._get_pools_by_take(take).get('pools', [])

    def calc_pg_upmaps(self, inc,
                       max_deviation=.01, max_iterations=10, pools=None):
        if pools is None:
            pools = []
        return self._calc_pg_upmaps(
            inc,
            max_deviation, max_iterations, pools)

    def map_pool_pgs_up(self, poolid):
        return self._map_pool_pgs_up(poolid)

    def pg_to_up_acting_osds(self, pool_id, ps):
        return self._pg_to_up_acting_osds(pool_id, ps)

    def pool_raw_used_rate(self, pool_id):
        return self._pool_raw_used_rate(pool_id)

    def get_ec_profile(self, name):
        # FIXME: efficient implementation
        d = self._dump()
        return d['erasure_code_profiles'].get(name, None)


class OSDMapIncremental(ceph_module.BasePyOSDMapIncremental):
    def get_epoch(self):
        return self._get_epoch()

    def dump(self):
        return self._dump()

    def set_osd_reweights(self, weightmap):
        """
        weightmap is a dict, int to float.  e.g. { 0: .9, 1: 1.0, 3: .997 }
        """
        return self._set_osd_reweights(weightmap)

    def set_crush_compat_weight_set_weights(self, weightmap):
        """
        weightmap is a dict, int to float.  devices only.  e.g.,
        { 0: 3.4, 1: 3.3, 2: 3.334 }
        """
        return self._set_crush_compat_weight_set_weights(weightmap)


class CRUSHMap(ceph_module.BasePyCRUSH):
    ITEM_NONE = 0x7fffffff
    DEFAULT_CHOOSE_ARGS = '-1'

    def dump(self):
        return self._dump()

    def get_item_weight(self, item):
        return self._get_item_weight(item)

    def get_item_name(self, item):
        return self._get_item_name(item)

    def find_takes(self):
        return self._find_takes().get('takes', [])

    def get_take_weight_osd_map(self, root):
        uglymap = self._get_take_weight_osd_map(root)
        return {int(k): v for k, v in six.iteritems(uglymap.get('weights', {}))}

    @staticmethod
    def have_default_choose_args(dump):
        return CRUSHMap.DEFAULT_CHOOSE_ARGS in dump.get('choose_args', {})

    @staticmethod
    def get_default_choose_args(dump):
        return dump.get('choose_args').get(CRUSHMap.DEFAULT_CHOOSE_ARGS, [])

    def get_rule(self, rule_name):
        # TODO efficient implementation
        for rule in self.dump()['rules']:
            if rule_name == rule['rule_name']:
                return rule

        return None

    def get_rule_by_id(self, rule_id):
        for rule in self.dump()['rules']:
            if rule['rule_id'] == rule_id:
                return rule

        return None

    def get_rule_root(self, rule_name):
        rule = self.get_rule(rule_name)
        if rule is None:
            return None

        try:
            first_take = [s for s in rule['steps'] if s['op'] == 'take'][0]
        except IndexError:
            self.log.warn("CRUSH rule '{0}' has no 'take' step".format(
                rule_name))
            return None
        else:
            return first_take['item']

    def get_osds_under(self, root_id):
        # TODO don't abuse dump like this
        d = self.dump()
        buckets = dict([(b['id'], b) for b in d['buckets']])

        osd_list = []

        def accumulate(b):
            for item in b['items']:
                if item['id'] >= 0:
                    osd_list.append(item['id'])
                else:
                    try:
                        accumulate(buckets[item['id']])
                    except KeyError:
                        pass

        accumulate(buckets[root_id])

        return osd_list

    def device_class_counts(self):
        result = defaultdict(int)
        # TODO don't abuse dump like this
        d = self.dump()
        for device in d['devices']:
            cls = device.get('class', None)
            result[cls] += 1

        return dict(result)


class CLICommand(object):
    COMMANDS = {}

    def __init__(self, prefix, args="", desc="", perm="rw"):
        self.prefix = prefix
        self.args = args
        self.args_dict = {}
        self.desc = desc
        self.perm = perm
        self.func = None
        self._parse_args()

    def _parse_args(self):
        if not self.args:
            return
        args = self.args.split(" ")
        for arg in args:
            arg_desc = arg.strip().split(",")
            arg_d = {}
            for kv in arg_desc:
                k, v = kv.split("=")
                if k != "name":
                    arg_d[k] = v
                else:
                    self.args_dict[v] = arg_d

    def __call__(self, func):
        self.func = func
        self.COMMANDS[self.prefix] = self
        return self.func

    def call(self, mgr, cmd_dict, inbuf):
        kwargs = {}
        for a, d in self.args_dict.items():
            if 'req' in d and d['req'] == "false" and a not in cmd_dict:
                continue
            kwargs[a.replace("-", "_")] = cmd_dict[a]
        if inbuf:
            kwargs['inbuf'] = inbuf
        return self.func(mgr, **kwargs)

    @classmethod
    def dump_cmd_list(cls):
        return [{
            'cmd': '{} {}'.format(cmd.prefix, cmd.args),
            'desc': cmd.desc,
            'perm': cmd.perm
        } for _, cmd in cls.COMMANDS.items()]


def CLIReadCommand(prefix, args="", desc=""):
    return CLICommand(prefix, args, desc, "r")


def CLIWriteCommand(prefix, args="", desc=""):
    return CLICommand(prefix, args, desc, "w")

class Option(dict):
    """
    Helper class to declare options for MODULE_OPTIONS list.

    Caveat: it uses argument names matching Python keywords (type, min, max),
    so any further processing should happen in a separate method.

    TODO: type validation.
    """
    def __init__(
            self, name,
            default=None,
            type='str',
            desc=None, longdesc=None,
            min=None, max=None,
            enum_allowed=None,
            see_also=None,
            tags=None,
            runtime=False,
    ):
        super(Option, self).__init__(
            (k, v) for k, v in vars().items()
            if k != 'self' and v is not None)


class MgrStandbyModule(ceph_module.BaseMgrStandbyModule):
    """
    Standby modules only implement a serve and shutdown method, they
    are not permitted to implement commands and they do not receive
    any notifications.

    They only have access to the mgrmap (for accessing service URI info
    from their active peer), and to configuration settings (read only).
    """

    MODULE_OPTIONS = []
    MODULE_OPTION_DEFAULTS = {}

    def __init__(self, module_name, capsule):
        super(MgrStandbyModule, self).__init__(capsule)
        self.module_name = module_name
        self._logger = configure_logger(self, module_name)
        # see also MgrModule.__init__()
        for o in self.MODULE_OPTIONS:
            if 'default' in o:
                if 'type' in o:
                    self.MODULE_OPTION_DEFAULTS[o['name']] = o['default']
                else:
                    self.MODULE_OPTION_DEFAULTS[o['name']] = str(o['default'])

    def __del__(self):
        unconfigure_logger(self, self.module_name)

    @property
    def log(self):
        return self._logger

    def serve(self):
        """
        The serve method is mandatory for standby modules.
        :return:
        """
        raise NotImplementedError()

    def get_mgr_id(self):
        return self._ceph_get_mgr_id()

    def get_module_option(self, key, default=None):
        """
        Retrieve the value of a persistent configuration setting

        :param str key:
        :param default: the default value of the config if it is not found
        :return: str
        """
        r = self._ceph_get_module_option(key)
        if r is None:
            final_key = key.split('/')[-1]
            return self.MODULE_OPTION_DEFAULTS.get(final_key, default)
        else:
            return r

    def get_ceph_option(self, key):
        return self._ceph_get_option(key)

    def get_store(self, key):
        """
        Retrieve the value of a persistent KV store entry

        :param key: String
        :return: Byte string or None
        """
        return self._ceph_get_store(key)

    def get_active_uri(self):
        return self._ceph_get_active_uri()

    def get_localized_module_option(self, key, default=None):
        r = self.get_module_option(self.get_mgr_id() + '/' + key)
        if r is None:
            r = self.get_module_option(key)

        if r is None:
            r = default
        return r


class MgrModule(ceph_module.BaseMgrModule):
    COMMANDS = []
    MODULE_OPTIONS = []
    MODULE_OPTION_DEFAULTS = {}

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

        # If we're taking over from a standby module, let's make sure
        # its logger was unconfigured before we hook ours up
        unconfigure_logger(self, self.module_name)
        self._logger = configure_logger(self, module_name)

        super(MgrModule, self).__init__(py_modules_ptr, this_ptr)

        self._version = self._ceph_get_version()

        self._perf_schema_cache = None

        # Keep a librados instance for those that need it.
        self._rados = None

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

    def __del__(self):
        unconfigure_logger(self, self.module_name)

    @classmethod
    def _register_commands(cls):
        cls.COMMANDS.extend(CLICommand.dump_cmd_list())

    @property
    def log(self):
        return self._logger

    def cluster_log(self, channel, priority, message):
        """
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
        self._ceph_cluster_log(channel, priority, message)

    @property
    def version(self):
        return self._version

    def get_context(self):
        """
        :return: a Python capsule containing a C++ CephContext pointer
        """
        return self._ceph_get_context()

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

    def config_notify(self):
        """
        Called by the ceph-mgr service to notify the Python plugin
        that the configuration may have changed.  Modules will want to
        refresh any configuration values stored in config variables.
        """
        pass

    def serve(self):
        """
        Called by the ceph-mgr service to start any server that
        is provided by this Python plugin.  The implementation
        of this function should block until ``shutdown`` is called.

        You *must* implement ``shutdown`` if you implement ``serve``
        """
        pass

    def shutdown(self):
        """
        Called by the ceph-mgr service to request that this
        module drop out of its serve() function.  You do not
        need to implement this if you do not implement serve()

        :return: None
        """
        if self._rados:
            self._rados.shutdown()

    def get(self, data_name):
        """
        Called by the plugin to fetch named cluster-wide objects from ceph-mgr.

        :param str data_name: Valid things to fetch are osd_crush_map_text,
                osd_map, osd_map_tree, osd_map_crush, config, mon_map, fs_map,
                osd_metadata, pg_summary, io_rate, pg_dump, df, osd_stats,
                health, mon_status, devices, device <devid>.

        Note:
            All these structures have their own JSON representations: experiment
            or look at the C++ ``dump()`` methods to learn about them.
        """
        return self._ceph_get(data_name)

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

    @staticmethod
    def to_pretty_iec(n):
        for bits, suffix in [(60, 'Ei'), (50, 'Pi'), (40, 'Ti'), (30, 'Gi'),
                             (20, 'Mi'), (10, 'Ki')]:
            if n > 10 << bits:
                return str(n >> bits) + ' ' + suffix
        return str(n) + ' '

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

    def get_pretty_header(self, elems, width):
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
        ret += self.get_pretty_row(elems, width)
        ret += '\n'

        # dash line
        ret += '+'
        for i in range(0, n):
            ret += '-' * (column_width - 1) + '+'
        ret += '\n'

        return ret

    def get_server(self, hostname):
        """
        Called by the plugin to fetch metadata about a particular hostname from
        ceph-mgr.

        This is information that ceph-mgr has gleaned from the daemon metadata
        reported by daemons running on a particular server.

        :param hostname: a hostname
        """
        return self._ceph_get_server(hostname)

    def get_perf_schema(self, svc_type, svc_name):
        """
        Called by the plugin to fetch perf counter schema info.
        svc_name can be nullptr, as can svc_type, in which case
        they are wildcards

        :param str svc_type:
        :param str svc_name:
        :return: list of dicts describing the counters requested
        """
        return self._ceph_get_perf_schema(svc_type, svc_name)

    def get_counter(self, svc_type, svc_name, path):
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
        return self._ceph_get_counter(svc_type, svc_name, path)

    def get_latest_counter(self, svc_type, svc_name, path):
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
        return self._ceph_get_latest_counter(svc_type, svc_name, path)

    def list_servers(self):
        """
        Like ``get_server``, but gives information about all servers (i.e. all
        unique hostnames that have been mentioned in daemon metadata)

        :return: a list of information about all servers
        :rtype: list
        """
        return self._ceph_get_server(None)

    def get_metadata(self, svc_type, svc_id):
        """
        Fetch the daemon metadata for a particular service.

        ceph-mgr fetches metadata asynchronously, so are windows of time during
        addition/removal of services where the metadata is not available to
        modules.  ``None`` is returned if no metadata is available.

        :param str svc_type: service type (e.g., 'mds', 'osd', 'mon')
        :param str svc_id: service id. convert OSD integer IDs to strings when
            calling this
        :rtype: dict, or None if no metadata found
        """
        return self._ceph_get_metadata(svc_type, svc_id)

    def get_daemon_status(self, svc_type, svc_id):
        """
        Fetch the latest status for a particular service daemon.

        This method may return ``None`` if no status information is
        available, for example because the daemon hasn't fully started yet.

        :param svc_type: string (e.g., 'rgw')
        :param svc_id: string
        :return: dict, or None if the service is not found
        """
        return self._ceph_get_daemon_status(svc_type, svc_id)

    def mon_command(self, cmd_dict):
        """
        Helper for modules that do simple, synchronous mon command
        execution.

        See send_command for general case.

        :return: status int, out std, err str
        """

        t1 = time.time()
        result = CommandResult()
        self.send_command(result, "mon", "", json.dumps(cmd_dict), "")
        r = result.wait()
        t2 = time.time()

        self.log.debug("mon_command: '{0}' -> {1} in {2:.3f}s".format(
            cmd_dict['prefix'], r[0], t2 - t1
        ))

        return r

    def send_command(self, *args, **kwargs):
        """
        Called by the plugin to send a command to the mon
        cluster.

        :param CommandResult result: an instance of the ``CommandResult``
            class, defined in the same module as MgrModule.  This acts as a
            completion and stores the output of the command.  Use
            ``CommandResult.wait()`` if you want to block on completion.
        :param str svc_type:
        :param str svc_id:
        :param str command: a JSON-serialized command.  This uses the same
            format as the ceph command line, which is a dictionary of command
            arguments, with the extra ``prefix`` key containing the command
            name itself.  Consult MonCommands.h for available commands and
            their expected arguments.
        :param str tag: used for nonblocking operation: when a command
            completes, the ``notify()`` callback on the MgrModule instance is
            triggered, with notify_type set to "command", and notify_id set to
            the tag of the command.
        """
        self._ceph_send_command(*args, **kwargs)

    def set_health_checks(self, checks):
        """
        Set the module's current map of health checks.  Argument is a
        dict of check names to info, in this form:

        ::

           {
             'CHECK_FOO': {
               'severity': 'warning',           # or 'error'
               'summary': 'summary string',
               'detail': [ 'list', 'of', 'detail', 'strings' ],
              },
             'CHECK_BAR': {
               'severity': 'error',
               'summary': 'bars are bad',
               'detail': [ 'too hard' ],
             },
           }

        :param list: dict of health check dicts
        """
        self._ceph_set_health_checks(checks)

    def _handle_command(self, inbuf, cmd):
        if cmd['prefix'] not in CLICommand.COMMANDS:
            return self.handle_command(inbuf, cmd)
        return CLICommand.COMMANDS[cmd['prefix']].call(self, cmd, inbuf)

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

    def get_mgr_id(self):
        """
        Retrieve the name of the manager daemon where this plugin
        is currently being executed (i.e. the active manager).

        :return: str
        """
        return self._ceph_get_mgr_id()

    def get_ceph_option(self, key):
        return self._ceph_get_option(key)

    def _validate_module_option(self, key):
        """
        Helper: don't allow get/set config callers to
        access config options that they didn't declare
        in their schema.
        """
        if key not in [o['name'] for o in self.MODULE_OPTIONS]:
            raise RuntimeError("Config option '{0}' is not in {1}.MODULE_OPTIONS".
                               format(key, self.__class__.__name__))

    def _get_module_option(self, key, default):
        r = self._ceph_get_module_option(key)
        if r is None:
            final_key = key.split('/')[-1]
            return self.MODULE_OPTION_DEFAULTS.get(final_key, default)
        else:
            return r

    def get_module_option(self, key, default=None):
        """
        Retrieve the value of a persistent configuration setting

        :param str key:
        :param str default:
        :return: str
        """
        self._validate_module_option(key)
        return self._get_module_option(key, default)

    def get_module_option_ex(self, module, key, default=None):
        """
        Retrieve the value of a persistent configuration setting
        for the specified module.

        :param str module: The name of the module, e.g. 'dashboard'
            or 'telemetry'.
        :param str key: The configuration key, e.g. 'server_addr'.
        :param str,None default: The default value to use when the
            returned value is ``None``. Defaults to ``None``.
        :return: str,int,bool,float,None
        """
        if module == self.module_name:
            self._validate_module_option(key)
        r = self._ceph_get_module_option_ex(module, key)
        return default if r is None else r

    def get_store_prefix(self, key_prefix):
        """
        Retrieve a dict of KV store keys to values, where the keys
        have the given prefix

        :param str key_prefix:
        :return: str
        """
        return self._ceph_get_store_prefix(key_prefix)

    def _get_localized(self, key, default, getter):
        r = getter(self.get_mgr_id() + '/' + key, None)
        if r is None:
            r = getter(key, default)

        return r

    def _set_localized(self, key, val, setter):
        return setter(self.get_mgr_id() + '/' + key, val)

    def get_localized_module_option(self, key, default=None):
        """
        Retrieve localized configuration for this ceph-mgr instance
        :param str key:
        :param str default:
        :return: str
        """
        self._validate_module_option(key)
        return self._get_localized(key, default, self._get_module_option)

    def _set_module_option(self, key, val):
        return self._ceph_set_module_option(key, str(val))

    def set_module_option(self, key, val):
        """
        Set the value of a persistent configuration setting

        :param str key:
        :type val: str | None
        """
        self._validate_module_option(key)
        return self._set_module_option(key, val)

    def set_module_option_ex(self, module, key, val):
        """
        Set the value of a persistent configuration setting
        for the specified module.

        :param str module:
        :param str key:
        :param str val:
        """
        if module == self.module_name:
            self._validate_module_option(key)
        return self._ceph_set_module_option_ex(module, key, str(val))

    def set_localized_module_option(self, key, val):
        """
        Set localized configuration for this ceph-mgr instance
        :param str key:
        :param str val:
        :return: str
        """
        self._validate_module_option(key)
        return self._set_localized(key, val, self._set_module_option)

    def set_store(self, key, val):
        """
        Set a value in this module's persistent key value store.
        If val is None, remove key from store

        :param str key:
        :param str val:
        """
        self._ceph_set_store(key, val)

    def get_store(self, key, default=None):
        """
        Get a value from this module's persistent key value store
        """
        r = self._ceph_get_store(key)
        if r is None:
            return default
        else:
            return r

    def get_localized_store(self, key, default=None):
        return self._get_localized(key, default, self.get_store)

    def set_localized_store(self, key, val):
        return self._set_localized(key, val, self.set_store)

    def self_test(self):
        """
        Run a self-test on the module. Override this function and implement
        a best as possible self-test for (automated) testing of the module

        Indicate any failures by raising an exception.  This does not have
        to be pretty, it's mainly for picking up regressions during
        development, rather than use in the field.

        :return: None, or an advisory string for developer interest, such
                 as a json dump of some state.
        """
        pass

    def get_osdmap(self):
        """
        Get a handle to an OSDMap.  If epoch==0, get a handle for the latest
        OSDMap.
        :return: OSDMap
        """
        return self._ceph_get_osdmap()

    def get_latest(self, daemon_type, daemon_name, counter):
        data = self.get_latest_counter(
            daemon_type, daemon_name, counter)[counter]
        if data:
            return data[1]
        else:
            return 0

    def get_latest_avg(self, daemon_type, daemon_name, counter):
        data = self.get_latest_counter(
            daemon_type, daemon_name, counter)[counter]
        if data:
            return data[1], data[2]
        else:
            return 0, 0

    def get_all_perf_counters(self, prio_limit=PRIO_USEFUL,
                              services=("mds", "mon", "osd",
                                        "rbd-mirror", "rgw")):
        """
        Return the perf counters currently known to this ceph-mgr
        instance, filtered by priority equal to or greater than `prio_limit`.

        The result is a map of string to dict, associating services
        (like "osd.123") with their counters.  The counter
        dict for each service maps counter paths to a counter
        info structure, which is the information from
        the schema, plus an additional "value" member with the latest
        value.
        """

        result = defaultdict(dict)

        for server in self.list_servers():
            for service in server['services']:
                if service['type'] not in services:
                    continue

                schema = self.get_perf_schema(service['type'], service['id'])
                if not schema:
                    self.log.warn("No perf counter schema for {0}.{1}".format(
                        service['type'], service['id']
                    ))
                    continue

                # Value is returned in a potentially-multi-service format,
                # get just the service we're asking about
                svc_full_name = "{0}.{1}".format(
                    service['type'], service['id'])
                schema = schema[svc_full_name]

                # Populate latest values
                for counter_path, counter_schema in schema.items():
                    # self.log.debug("{0}: {1}".format(
                    #     counter_path, json.dumps(counter_schema)
                    # ))
                    if counter_schema['priority'] < prio_limit:
                        continue

                    counter_info = dict(counter_schema)

                    # Also populate count for the long running avgs
                    if counter_schema['type'] & self.PERFCOUNTER_LONGRUNAVG:
                        v, c = self.get_latest_avg(
                            service['type'],
                            service['id'],
                            counter_path
                        )
                        counter_info['value'], counter_info['count'] = v, c
                        result[svc_full_name][counter_path] = counter_info
                    else:
                        counter_info['value'] = self.get_latest(
                            service['type'],
                            service['id'],
                            counter_path
                        )

                    result[svc_full_name][counter_path] = counter_info

        self.log.debug("returning {0} counter".format(len(result)))

        return result

    def set_uri(self, uri):
        """
        If the module exposes a service, then call this to publish the
        address once it is available.

        :return: a string
        """
        return self._ceph_set_uri(uri)

    def have_mon_connection(self):
        """
        Check whether this ceph-mgr daemon has an open connection
        to a monitor.  If it doesn't, then it's likely that the
        information we have about the cluster is out of date,
        and/or the monitor cluster is down.
        """

        return self._ceph_have_mon_connection()

    def update_progress_event(self, evid, desc, progress):
        return self._ceph_update_progress_event(str(evid), str(desc), float(progress))

    def complete_progress_event(self, evid):
        return self._ceph_complete_progress_event(str(evid))

    def clear_all_progress_events(self):
        return self._ceph_clear_all_progress_events()

    @property
    def rados(self):
        """
        A librados instance to be shared by any classes within
        this mgr module that want one.
        """
        if self._rados:
            return self._rados

        ctx_capsule = self.get_context()
        self._rados = rados.Rados(context=ctx_capsule)
        self._rados.connect()

        return self._rados

    @staticmethod
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

    def remote(self, module_name, method_name, *args, **kwargs):
        """
        Invoke a method on another module.  All arguments, and the return
        value from the other module must be serializable.

        Limitation: Do not import any modules within the called method.
        Otherwise you will get an error in Python 2::

            RuntimeError('cannot unmarshal code objects in restricted execution mode',)



        :param module_name: Name of other module.  If module isn't loaded,
                            an ImportError exception is raised.
        :param method_name: Method name.  If it does not exist, a NameError
                            exception is raised.
        :param args: Argument tuple
        :param kwargs: Keyword argument dict
        :return:
        """
        return self._ceph_dispatch_remote(module_name, method_name,
                                          args, kwargs)

    def add_osd_perf_query(self, query):
        """
        Register an OSD perf query.  Argument is a
        dict of the query parameters, in this form:

        ::

           {
             'key_descriptor': [
               {'type': subkey_type, 'regex': regex_pattern},
               ...
             ],
             'performance_counter_descriptors': [
               list, of, descriptor, types
             ],
             'limit': {'order_by': performance_counter_type, 'max_count': n},
           }

        Valid subkey types:
           'client_id', 'client_address', 'pool_id', 'namespace', 'osd_id',
           'pg_id', 'object_name', 'snap_id'
        Valid performance counter types:
           'ops', 'write_ops', 'read_ops', 'bytes', 'write_bytes', 'read_bytes',
           'latency', 'write_latency', 'read_latency'

        :param object query: query
        :rtype: int (query id)
        """
        return self._ceph_add_osd_perf_query(query)

    def remove_osd_perf_query(self, query_id):
        """
        Unregister an OSD perf query.

        :param int query_id: query ID
        """
        return self._ceph_remove_osd_perf_query(query_id)

    def get_osd_perf_counters(self, query_id):
        """
        Get stats collected for an OSD perf query.

        :param int query_id: query ID
        """
        return self._ceph_get_osd_perf_counters(query_id)
