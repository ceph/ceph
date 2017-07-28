
import ceph_state  #noqa
import ceph_osdmap  #noqa
import ceph_osdmap_incremental  #noqa
import ceph_crushmap  #noqa
import json
import logging
import threading


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


class OSDMap(object):
    def __init__(self, handle):
        self._handle = handle

    def get_epoch(self):
        return ceph_osdmap.get_epoch(self._handle)

    def get_crush_version(self):
        return ceph_osdmap.get_crush_version(self._handle)

    def dump(self):
        return ceph_osdmap.dump(self._handle)

    def new_incremental(self):
        return OSDMapIncremental(ceph_osdmap.new_incremental(self._handle))

    def apply_incremental(self, inc):
        return OSDMap(ceph_osdmap.apply_incremental(self._handle, inc._handle))

    def get_crush(self):
        return CRUSHMap(ceph_osdmap.get_crush(self._handle), self)

    def get_pools_by_take(self, take):
        return ceph_osdmap.get_pools_by_take(self._handle, take).get('pools', [])

    def calc_pg_upmaps(self, inc,
                       max_deviation=.01, max_iterations=10, pools=[]):
        return ceph_osdmap.calc_pg_upmaps(
            self._handle,
            inc._handle,
            max_deviation, max_iterations, pools)

    def map_pool_pgs_up(self, poolid):
        return ceph_osdmap.map_pool_pgs_up(self._handle, poolid)

class OSDMapIncremental(object):
    def __init__(self, handle):
        self._handle = handle

    def get_epoch(self):
        return ceph_osdmap_incremental.get_epoch(self._handle)

    def dump(self):
        return ceph_osdmap_incremental.dump(self._handle)

    def set_osd_reweights(self, weightmap):
        """
        weightmap is a dict, int to float.  e.g. { 0: .9, 1: 1.0, 3: .997 }
        """
        return ceph_osdmap_incremental.set_osd_reweights(self._handle, weightmap)

    def set_crush_compat_weight_set_weights(self, weightmap):
        """
        weightmap is a dict, int to float.  devices only.  e.g.,
        { 0: 3.4, 1: 3.3, 2: 3.334 }
        """
        return ceph_osdmap_incremental.set_crush_compat_weight_set_weights(
            self._handle, weightmap)



class CRUSHMap(object):
    def __init__(self, handle, parent_osdmap):
        self._handle = handle
        # keep ref to parent osdmap since handle lifecycle is owned by it
        self._parent_osdmap = parent_osdmap

    def dump(self):
        return ceph_crushmap.dump(self._handle)

    def get_item_name(self, item):
        return ceph_crushmap.get_item_name(self._handle, item)

    def find_takes(self):
        return ceph_crushmap.find_takes(self._handle).get('takes',[])

    def get_take_weight_osd_map(self, root):
        uglymap = ceph_crushmap.get_take_weight_osd_map(self._handle, root)
        return { int(k): v for k, v in uglymap.get('weights', {}).iteritems() }


class MgrModule(object):
    COMMANDS = []

    def __init__(self, handle):
        self._handle = handle
        self._logger = logging.getLogger(handle)

        # Don't filter any logs at the python level, leave it to C++
        self._logger.setLevel(logging.DEBUG)

        # FIXME: we should learn the log level from C++ land, and then
        # avoid calling ceph_state.log when we know a message is of
        # an insufficient level to be ultimately output

        class CPlusPlusHandler(logging.Handler):
            def emit(self, record):
                if record.levelno <= logging.DEBUG:
                    ceph_level = 20
                elif record.levelno <= logging.INFO:
                    ceph_level = 4
                elif record.levelno <= logging.WARNING:
                    ceph_level = 1
                else:
                    ceph_level = 0

                ceph_state.log(handle, ceph_level, self.format(record))

        self._logger.addHandler(CPlusPlusHandler())

        self._version = ceph_state.get_version()

    @property
    def log(self):
        return self._logger

    @property
    def version(self):
        return self._version

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
        pass

    def get(self, data_name):
        """
        Called by the plugin to load some cluster state from ceph-mgr
        """
        return ceph_state.get(self._handle, data_name)

    def get_server(self, hostname):
        """
        Called by the plugin to load information about a particular
        node from ceph-mgr.

        :param hostname: a hostame
        """
        return ceph_state.get_server(self._handle, hostname)

    def get_perf_schema(self, svc_type, svc_name):
        """
        Called by the plugin to fetch perf counter schema info.
        svc_name can be nullptr, as can svc_type, in which case
        they are wildcards

        :param svc_type:
        :param svc_name:
        :return: list of dicts describing the counters requested
        """
        return ceph_state.get_perf_schema(self._handle, svc_type, svc_name)

    def get_counter(self, svc_type, svc_name, path):
        """
        Called by the plugin to fetch data for a particular perf counter
        on a particular service.

        :param svc_type:
        :param svc_name:
        :param path:
        :return: A list of two-element lists containing time and value
        """
        return ceph_state.get_counter(self._handle, svc_type, svc_name, path)

    def list_servers(self):
        """
        Like ``get_server``, but instead of returning information
        about just one node, return all the nodes in an array.
        """
        return ceph_state.get_server(self._handle, None)

    def get_metadata(self, svc_type, svc_id):
        """
        Fetch the metadata for a particular service.

        :param svc_type: string (e.g., 'mds', 'osd', 'mon')
        :param svc_id: string
        :return: dict
        """
        return ceph_state.get_metadata(self._handle, svc_type, svc_id)

    def get_daemon_status(self, svc_type, svc_id):
        """
        Fetch the latest status for a particular service daemon.

        :param svc_type: string (e.g., 'rgw')
        :param svc_id: string
        :return: dict
        """
        return ceph_state.get_daemon_status(self._handle, svc_type, svc_id)

    def send_command(self, *args, **kwargs):
        """
        Called by the plugin to send a command to the mon
        cluster.
        """
        ceph_state.send_command(self._handle, *args, **kwargs)

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
        ceph_state.set_health_checks(self._handle, checks)

    def handle_command(self, cmd):
        """
        Called by ceph-mgr to request the plugin to handle one
        of the commands that it declared in self.COMMANDS

        Return a status code, an output buffer, and an
        output string.  The output buffer is for data results,
        the output string is for informative text.

        :param cmd: dict, from Ceph's cmdmap_t

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
        return ceph_state.get_mgr_id()

    def get_config(self, key, default=None):
        """
        Retrieve the value of a persistent configuration setting

        :param key: str
        :return: str
        """
        r = ceph_state.get_config(self._handle, key)
        if r is None:
            return default
        else:
            return r

    def get_config_prefix(self, key_prefix):
        """
        Retrieve a dict of config values with the given prefix

        :param key_prefix: str
        :return: str
        """
        return ceph_state.get_config_prefix(self._handle, key_prefix)

    def get_localized_config(self, key, default=None):
        """
        Retrieve localized configuration for this ceph-mgr instance
        :param key: str
        :param default: str
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

        :param key: str
        :param val: str
        """
        ceph_state.set_config(self._handle, key, val)

    def set_localized_config(self, key, val):
        """
        Set localized configuration for this ceph-mgr instance
        :param key: str
        :param default: str
        :return: str
        """
        return self.set_config(self.get_mgr_id() + '/' + key, val)

    def set_config_json(self, key, val):
        """
        Helper for setting json-serialized-config

        :param key: str
        :param val: json-serializable object
        """
        self.set_config(key, json.dumps(val))

    def get_config_json(self, key):
        """
        Helper for getting json-serialized config

        :param key: str
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
        return OSDMap(ceph_state.get_osdmap())
