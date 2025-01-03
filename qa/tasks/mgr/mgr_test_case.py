import json
import logging
import socket

from unittest import SkipTest

from teuthology import misc
from tasks.ceph_test_case import CephTestCase

# TODO move definition of CephCluster away from the CephFS stuff
from tasks.cephfs.filesystem import CephClusterBase


log = logging.getLogger(__name__)


class MgrClusterBase(CephClusterBase):
    def __init__(self, ctx):
        super(MgrClusterBase, self).__init__(ctx)
        self.mgr_ids = list(misc.all_roles_of_type(ctx.cluster, 'mgr'))

        if len(self.mgr_ids) == 0:
            raise RuntimeError(
                "This task requires at least one manager daemon")

        self.mgr_daemons = dict(
            [(mgr_id, self._ctx.daemons.get_daemon('mgr', mgr_id)) for mgr_id
             in self.mgr_ids])

    def mgr_stop(self, mgr_id):
        self.mgr_daemons[mgr_id].stop()

    def mgr_fail(self, mgr_id=None):
        if mgr_id is None:
            self.mon_manager.raw_cluster_cmd("mgr", "fail")
        else:
            self.mon_manager.raw_cluster_cmd("mgr", "fail", mgr_id)

    def set_down(self, yes='true'):
        self.mon_manager.raw_cluster_cmd('mgr', 'set', 'down', str(yes))

    def mgr_tell(self, *args, mgr_id=None, mgr_map=None):
        if mgr_id is None:
            if mgr_map is None:
                mgr_map = self.get_mgr_map()
            mgr_id = self.get_active_id(mgr_map=mgr_map)
        J = self.mon_manager.raw_cluster_cmd("tell", f"mgr.{mgr_id}", *args)
        return json.loads(J)

    def mgr_restart(self, mgr_id):
        self.mgr_daemons[mgr_id].restart()

    def get_mgr_map(self):
        return json.loads(
            self.mon_manager.raw_cluster_cmd("mgr", "dump", "--format=json-pretty"))

    def get_registered_clients(self, name, mgr_map = None):
        if mgr_map is None:
            mgr_map = self.get_mgr_map()
        for c in mgr_map['active_clients']:
            if c['name'] == name:
                return c['addrvec']
        return None

    def get_active_gid(self, mgr_map = None):
        if mgr_map is None:
            mgr_map = self.get_mgr_map()
        return mgr_map["active_gid"]

    def get_active_id(self, mgr_map = None):
        if mgr_map is None:
            mgr_map = self.get_mgr_map()
        return mgr_map["active_name"]

    def get_standby_ids(self, mgr_map = None):
        if mgr_map is None:
            mgr_map = self.get_mgr_map()
        return [s['name'] for s in mgr_map["standbys"]]

    def set_module_conf(self, module, key, val):
        self.mon_manager.raw_cluster_cmd("config", "set", "mgr",
                                         "mgr/{0}/{1}".format(
                                             module, key
                                         ), val)

    def set_module_localized_conf(self, module, mgr_id, key, val, force):
        cmd = ["config", "set", "mgr",
               "/".join(["mgr", module, mgr_id, key]),
               val]
        if force:
            cmd.append("--force")
        self.mon_manager.raw_cluster_cmd(*cmd)
MgrCluster = MgrClusterBase

class MgrTestCase(CephTestCase):
    MGRS_REQUIRED = 1

    @classmethod
    def setup_mgrs(cls):
        # Stop all the daemons
        for daemon in cls.mgr_cluster.mgr_daemons.values():
            daemon.stop()

        cls.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "set", "down", "false")

        for mgr_id in cls.mgr_cluster.mgr_ids:
            cls.mgr_cluster.mgr_fail(mgr_id)

        # Unload all non-default plugins
        loaded = json.loads(cls.mgr_cluster.mon_manager.raw_cluster_cmd(
                   "mgr", "module", "ls", "--format=json-pretty"))['enabled_modules']
        unload_modules = set(loaded) - {"cephadm"}

        for m in unload_modules:
            cls.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "module", "disable", m)

        # Start all the daemons
        for daemon in cls.mgr_cluster.mgr_daemons.values():
            daemon.restart()

        # Wait for an active to come up
        cls.wait_until_true(lambda: cls.mgr_cluster.get_active_id() != "",
                             timeout=20)

        expect_standbys = set(cls.mgr_cluster.mgr_ids) \
                          - {cls.mgr_cluster.get_active_id()}
        cls.wait_until_true(
            lambda: set(cls.mgr_cluster.get_standby_ids()) == expect_standbys,
            timeout=20)

    @classmethod
    def setUpClass(cls):
        # The test runner should have populated this
        assert cls.mgr_cluster is not None

        if len(cls.mgr_cluster.mgr_ids) < cls.MGRS_REQUIRED:
            raise SkipTest(
                "Only have {0} manager daemons, {1} are required".format(
                    len(cls.mgr_cluster.mgr_ids), cls.MGRS_REQUIRED))

        # We expect laggy OSDs in this testing environment so turn off this warning.
        # See https://tracker.ceph.com/issues/61907
        cls.mgr_cluster.mon_manager.raw_cluster_cmd('config', 'set', 'mds',
                                                    'defer_client_eviction_on_laggy_osds', 'false')
        cls.setup_mgrs()

    @classmethod
    def _unload_module(cls, module_name):
        def is_disabled():
            enabled_modules = json.loads(cls.mgr_cluster.mon_manager.raw_cluster_cmd(
                'mgr', 'module', 'ls', "--format=json-pretty"))['enabled_modules']
            return module_name not in enabled_modules

        if is_disabled():
            return

        log.debug("Unloading Mgr module %s ...", module_name)
        cls.mgr_cluster.mon_manager.raw_cluster_cmd('mgr', 'module', 'disable', module_name)
        cls.wait_until_true(is_disabled, timeout=30)

    @classmethod
    def _load_module(cls, module_name):
        loaded = json.loads(cls.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "module", "ls", "--format=json-pretty"))['enabled_modules']
        if module_name in loaded:
            # The enable command is idempotent, but our wait for a restart
            # isn't, so let's return now if it's already loaded
            return

        initial_mgr_map = cls.mgr_cluster.get_mgr_map()

        # check if the the module is configured as an always on module
        mgr_daemons = json.loads(cls.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "metadata"))

        for daemon in mgr_daemons:
            if daemon["name"] == initial_mgr_map["active_name"]:
                ceph_version = daemon["ceph_release"]
                always_on = initial_mgr_map["always_on_modules"].get(ceph_version, [])
                if module_name in always_on:
                    return

        log.debug("Loading Mgr module %s ...", module_name)
        initial_gid = initial_mgr_map['active_gid']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "module", "enable", module_name, "--force")

        # Wait for the module to load
        def has_restarted():
            mgr_map = cls.mgr_cluster.get_mgr_map()
            done = mgr_map['active_gid'] != initial_gid and mgr_map['available']
            if done:
                log.debug("Restarted after module load (new active {0}/{1})".format(
                    mgr_map['active_name'], mgr_map['active_gid']))
            return done
        cls.wait_until_true(has_restarted, timeout=30)


    @classmethod
    def _get_uri(cls, service_name):
        # Little dict hack so that I can assign into this from
        # the get_or_none function
        mgr_map = {'x': None}

        def _get_or_none():
            mgr_map['x'] = cls.mgr_cluster.get_mgr_map()
            result = mgr_map['x']['services'].get(service_name, None)
            return result

        cls.wait_until_true(lambda: _get_or_none() is not None, 30)

        uri = mgr_map['x']['services'][service_name]

        log.debug("Found {0} at {1} (daemon {2}/{3})".format(
            service_name, uri, mgr_map['x']['active_name'],
            mgr_map['x']['active_gid']))

        return uri

    @classmethod
    def _assign_ports(cls, module_name, config_name, min_port=7789):
        """
        To avoid the need to run lots of hosts in teuthology tests to
        get different URLs per mgr, we will hand out different ports
        to each mgr here.

        This is already taken care of for us when running in a vstart
        environment.
        """
        # Start handing out ports well above Ceph's range.
        assign_port = min_port
        ip_addr = cls.mgr_cluster.get_mgr_map()['active_addr'].split(':')[0]

        for mgr_id in cls.mgr_cluster.mgr_ids:
            cls.mgr_cluster.mgr_stop(mgr_id)
            cls.mgr_cluster.mgr_fail(mgr_id)


        for mgr_id in cls.mgr_cluster.mgr_ids:
            # Find a port that isn't in use
            while True:
                if not cls.is_port_in_use(ip_addr, assign_port):
                    break
                log.debug(f"Port {assign_port} in use, trying next")
                assign_port += 1

            log.debug(f"Using port {assign_port} for {module_name} on mgr.{mgr_id}")
            cls.mgr_cluster.set_module_localized_conf(module_name, mgr_id,
                                                      config_name,
                                                      str(assign_port),
                                                      force=True)
            assign_port += 1

        for mgr_id in cls.mgr_cluster.mgr_ids:
            cls.mgr_cluster.mgr_restart(mgr_id)

        def is_available():
            mgr_map = cls.mgr_cluster.get_mgr_map()
            done = mgr_map['available']
            if done:
                log.debug("Available after assign ports (new active {0}/{1})".format(
                    mgr_map['active_name'], mgr_map['active_gid']))
            return done
        cls.wait_until_true(is_available, timeout=30)

    @classmethod
    def is_port_in_use(cls, ip_addr: str, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex((ip_addr, port)) == 0
