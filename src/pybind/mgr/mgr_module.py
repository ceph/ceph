
import ceph_state  #noqa
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


class MgrModule(object):
    COMMANDS = []

    def __init__(self, handle):
        self._handle = handle
        self._logger = logging.getLogger(handle)

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

    @property
    def log(self):
        return self._logger

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
        of this function should block until it receives a signal.
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

    def list_servers(self):
        """
        Like ``get_server``, but instead of returning information
        about just one node, return all the nodes in an array.
        """
        return ceph_state.get_server(self._handle, None)

    def get_metadata(self, svc_type, svc_id):
        """
        Fetch the metadata for a particular service.

        :param svc_type: one of 'mds', 'osd', 'mon'
        :param svc_id: string
        :return: dict
        """
        return ceph_state.get_metadata(self._handle, svc_type, svc_id)

    def send_command(self, *args, **kwargs):
        """
        Called by the plugin to send a command to the mon
        cluster.
        """
        ceph_state.send_command(self._handle, *args, **kwargs)

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

    def get_config(self, key):
        """
        Retrieve the value of a persistent configuration setting

        :param key: str
        :return: str
        """
        return ceph_state.get_config(self._handle, key)

    def set_config(self, key, val):
        """
        Set the value of a persistent configuration setting

        :param key: str
        :param val: str
        """
        ceph_state.set_config(self._handle, key, val)

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
