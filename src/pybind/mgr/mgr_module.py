
import ceph_module  # noqa

import json
import logging
import threading
from collections import defaultdict
import rados


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
    rm_handlers = [h for h in logger.handlers if isinstance(h, CPlusPlusHandler)]
    for h in rm_handlers:
        logger.removeHandler(h)

class CommandResult(object):
    """
    Use with MgrModule.send_command
    """
    def __init__(self, tag):
        self.ev = threading.Event()
        self.outs = ""
        self.outb = ""
        self.r = 0

        # This is just a convenience for notifications from
        # C++ land, to avoid passing addresses around in messages.
        self.tag = tag

    def complete(self, r, outb, outs):
        self.r = r
        self.outb = outb
        self.outs = outs
        self.ev.set()

    def wait(self):
        self.ev.wait()
        return self.r, self.outb, self.outs


class OSDMap(ceph_module.BasePyOSDMap):
    def get_epoch(self):
        return self._get_epoch()

    def get_crush_version(self):
        return self._get_crush_version()

    def dump(self):
        return self._dump()

    def new_incremental(self):
        return self._new_incremental()

    def apply_incremental(self, inc):
        return self._apply_incremental(inc)

    def get_crush(self):
        return self._get_crush()

    def get_pools_by_take(self, take):
        return self._get_pools_by_take(take).get('pools', [])

    def calc_pg_upmaps(self, inc,
                       max_deviation=.01, max_iterations=10, pools=[]):
        return self._calc_pg_upmaps(
            inc,
            max_deviation, max_iterations, pools)

    def map_pool_pgs_up(self, poolid):
        return self._map_pool_pgs_up(poolid)

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
        return { int(k): v for k, v in uglymap.get('weights', {}).iteritems() }

    @staticmethod
    def have_default_choose_args(dump):
        return CRUSHMap.DEFAULT_CHOOSE_ARGS in dump.get('choose_args', {})

    @staticmethod
    def get_default_choose_args(dump):
        return dump.get('choose_args').get(CRUSHMap.DEFAULT_CHOOSE_ARGS, [])


class MgrStandbyModule(ceph_module.BaseMgrStandbyModule):
    """
    Standby modules only implement a serve and shutdown method, they
    are not permitted to implement commands and they do not receive
    any notifications.

    They only have access to the mgrmap (for accessing service URI info
    from their active peer), and to configuration settings (read only).
    """

    def __init__(self, module_name, capsule):
        super(MgrStandbyModule, self).__init__(capsule)
        self.module_name = module_name
        self._logger = configure_logger(self, module_name)

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

    def get_config(self, key, default=None):
        """
        Retrieve the value of a persistent configuration setting

        :param str key:
        :param default: the default value of the config if it is not found
        :return: str
        """
        r = self._ceph_get_config(key)
        if r is None:
            return default
        else:
            return r


    def get_active_uri(self):
        return self._ceph_get_active_uri()

    def get_localized_config(self, key, default=None):
        r = self.get_config(self.get_mgr_id() + '/' + key)
        if r is None:
            r = self.get_config(key)

        if r is None:
            r = default
        return r

class MgrModule(ceph_module.BaseMgrModule):
    COMMANDS = []

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
    PERFCOUNTER_TYPE_MASK = ~2

    # units supported
    BYTES = 0
    NONE = 1
    
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

    def __del__(self):
        unconfigure_logger(self, self.module_name)

    def update_perf_schema(self, daemon_type, daemon_name):
        """
        For plugins that use get_all_perf_counters, call this when
        receiving a notification of type 'perf_schema_update', to
        prompt MgrModule to update its cache of counter schemas.

        :param daemon_type:
        :param daemon_name:
        :return:
        """

    @property
    def log(self):
        return self._logger

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
                osd_metadata, pg_summary, df, osd_stats, health, mon_status.

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

    def _unit_to_str(self, unit):
        if unit == self.NONE:
            return "/s"
        elif unit == self.BYTES:
            return "B/s"  
    
    def get_server(self, hostname):
        """
        Called by the plugin to fetch metadata about a particular hostname from
        ceph-mgr.

        This is information that ceph-mgr has gleaned from the daemon metadata
        reported by daemons running on a particular server.

        :param hostname: a hostame
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

    def list_servers(self):
        """
        Like ``get_server``, but gives information about all servers (i.e. all
        unique hostnames that have been mentioned in daemon metadata)

        :return: a list of infomration about all servers
        :rtype: list
        """
        return self._ceph_get_server(None)

    def get_metadata(self, svc_type, svc_id):
        """
        Fetch the daemon metadata for a particular service.

        :param str svc_type: service type (e.g., 'mds', 'osd', 'mon')
        :param str svc_id: service id. convert OSD integer IDs to strings when
            calling this
        :rtype: dict
        """
        return self._ceph_get_metadata(svc_type, svc_id)

    def get_daemon_status(self, svc_type, svc_id):
        """
        Fetch the latest status for a particular service daemon.

        :param svc_type: string (e.g., 'rgw')
        :param svc_id: string
        :return: dict
        """
        return self._ceph_get_daemon_status(svc_type, svc_id)

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
        Set module's health checks

        Set the module's current map of health checks.  Argument is a
        dict of check names to info, in this form:

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

    def handle_command(self, cmd):
        """
        Called by ceph-mgr to request the plugin to handle one
        of the commands that it declared in self.COMMANDS

        Return a status code, an output buffer, and an
        output string.  The output buffer is for data results,
        the output string is for informative text.

        :param dict cmd: from Ceph's cmdmap_t

        :return: 3-tuple of (int, str, str)
        """

        # Should never get called if they didn't declare
        # any ``COMMANDS``
        raise NotImplementedError()

    def get_mgr_id(self):
        """
        Retrieve the mgr id.

        :return: str
        """
        return self._ceph_get_mgr_id()

    def get_config(self, key, default=None):
        """
        Retrieve the value of a persistent configuration setting

        :param str key:
        :return: str
        """
        r = self._ceph_get_config(key)
        if r is None:
            return default
        else:
            return r

    def get_config_prefix(self, key_prefix):
        """
        Retrieve a dict of config values with the given prefix

        :param str key_prefix:
        :return: str
        """
        return self._ceph_get_config_prefix(key_prefix)

    def get_localized_config(self, key, default=None):
        """
        Retrieve localized configuration for this ceph-mgr instance
        :param str key:
        :param str default:
        :return: str
        """
        r = self.get_config(self.get_mgr_id() + '/' + key)
        if r is None:
            r = self.get_config(key)

        if r is None:
            r = default
        return r

    def set_config(self, key, val):
        """
        Set the value of a persistent configuration setting

        :param str key:
        :param str val:
        """
        self._ceph_set_config(key, val)

    def set_localized_config(self, key, val):
        """
        Set localized configuration for this ceph-mgr instance
        :param str key:
        :param str default:
        :return: str
        """
        return self._ceph_set_config(self.get_mgr_id() + '/' + key, val)

    def set_config_json(self, key, val):
        """
        Helper for setting json-serialized-config

        :param str key:
        :param val: json-serializable object
        """
        self._ceph_set_config(key, json.dumps(val))

    def get_config_json(self, key):
        """
        Helper for getting json-serialized config

        :param str key:
        :return: object
        """
        raw = self.get_config(key)
        if raw is None:
            return None
        else:
            return json.loads(raw)

    def self_test(self):
        """
        Run a self-test on the module. Override this function and implement
        a best as possible self-test for (automated) testing of the module
        :return: bool
        """
        pass

    def get_osdmap(self):
        """
        Get a handle to an OSDMap.  If epoch==0, get a handle for the latest
        OSDMap.
        :return: OSDMap
        """
        return self._ceph_get_osdmap()

    def get_all_perf_counters(self, prio_limit=PRIO_USEFUL):
        """
        Return the perf counters currently known to this ceph-mgr
        instance, filtered by priority equal to or greater than `prio_limit`.

        The result us a map of string to dict, associating services
        (like "osd.123") with their counters.  The counter
        dict for each service maps counter paths to a counter
        info structure, which is the information from
        the schema, plus an additional "value" member with the latest
        value.
        """

        result = defaultdict(dict)

        # TODO: improve C++->Python interface to return just
        # the latest if that's all we want.
        def get_latest(daemon_type, daemon_name, counter):
            data = self.get_counter(daemon_type, daemon_name, counter)[counter]
            if data:
                return data[-1][1]
            else:
                return 0

        for server in self.list_servers():
            for service in server['services']:
                if service['type'] not in ("mds", "osd", "mon"):
                    continue

                schema = self.get_perf_schema(service['type'], service['id'])
                if not schema:
                    self.log.warn("No perf counter schema for {0}.{1}".format(
                        service['type'], service['id']
                    ))
                    continue

                # Value is returned in a potentially-multi-service format,
                # get just the service we're asking about
                svc_full_name = "{0}.{1}".format(service['type'], service['id'])
                schema = schema[svc_full_name]

                # Populate latest values
                for counter_path, counter_schema in schema.items():
                    # self.log.debug("{0}: {1}".format(
                    #     counter_path, json.dumps(counter_schema)
                    # ))
                    if counter_schema['priority'] < prio_limit:
                        continue

                    counter_info = counter_schema
                    counter_info['value'] = get_latest(service['type'], service['id'], counter_path)
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
