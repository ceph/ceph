
from unittest import case
import json
import logging

from teuthology import misc
from tasks.ceph_test_case import CephTestCase

# TODO move definition of CephCluster away from the CephFS stuff
from tasks.cephfs.filesystem import CephCluster


log = logging.getLogger(__name__)


class MgrCluster(CephCluster):
    def __init__(self, ctx):
        super(MgrCluster, self).__init__(ctx)
        self.mgr_ids = list(misc.all_roles_of_type(ctx.cluster, 'mgr'))

        if len(self.mgr_ids) == 0:
            raise RuntimeError(
                "This task requires at least one manager daemon")

        self.mgr_daemons = dict(
            [(mgr_id, self._ctx.daemons.get_daemon('mgr', mgr_id)) for mgr_id
             in self.mgr_ids])

    def mgr_stop(self, mgr_id):
        self.mgr_daemons[mgr_id].stop()

    def mgr_fail(self, mgr_id):
        self.mon_manager.raw_cluster_cmd("mgr", "fail", mgr_id)

    def mgr_restart(self, mgr_id):
        self.mgr_daemons[mgr_id].restart()

    def get_mgr_map(self):
        status = json.loads(
            self.mon_manager.raw_cluster_cmd("status", "--format=json-pretty"))

        return status["mgrmap"]

    def get_active_id(self):
        return self.get_mgr_map()["active_name"]

    def get_standby_ids(self):
        return [s['name'] for s in self.get_mgr_map()["standbys"]]

    def set_module_localized_conf(self, module, mgr_id, key, val):
        self.mon_manager.raw_cluster_cmd("config-key", "set",
                                         "mgr/{0}/{1}/{2}".format(
                                             module, mgr_id, key
                                         ), val)


class MgrTestCase(CephTestCase):
    MGRS_REQUIRED = 1

    def setUp(self):
        super(MgrTestCase, self).setUp()

        # The test runner should have populated this
        assert self.mgr_cluster is not None

        if len(self.mgr_cluster.mgr_ids) < self.MGRS_REQUIRED:
            raise case.SkipTest("Only have {0} manager daemons, "
                                "{1} are required".format(
                len(self.mgr_cluster.mgr_ids), self.MGRS_REQUIRED))

        # Restart all the daemons
        for daemon in self.mgr_cluster.mgr_daemons.values():
            daemon.stop()

        for mgr_id in self.mgr_cluster.mgr_ids:
            self.mgr_cluster.mgr_fail(mgr_id)

        for daemon in self.mgr_cluster.mgr_daemons.values():
            daemon.restart()

        # Wait for an active to come up
        self.wait_until_true(lambda: self.mgr_cluster.get_active_id() != "",
                             timeout=20)

        expect_standbys = set(self.mgr_cluster.mgr_ids) \
                          - {self.mgr_cluster.get_active_id()}
        self.wait_until_true(
            lambda: set(self.mgr_cluster.get_standby_ids()) == expect_standbys,
            timeout=20)

    def _load_module(self, module_name):
        loaded = json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd(
                   "mgr", "module", "ls"))['enabled_modules']
        if module_name in loaded:
            # The enable command is idempotent, but our wait for a restart
            # isn't, so let's return now if it's already loaded
            return

        initial_gid = self.mgr_cluster.get_mgr_map()['active_gid']
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable",
                                         module_name)

        # Wait for the module to load
        def has_restarted():
            mgr_map = self.mgr_cluster.get_mgr_map()
            done = mgr_map['active_gid'] != initial_gid and mgr_map['available']
            if done:
                log.info("Restarted after module load (new active {0}/{1})".format(
                    mgr_map['active_name'] , mgr_map['active_gid']))
            return done
        self.wait_until_true(has_restarted, timeout=30)


    def _get_uri(self, service_name):
        # Little dict hack so that I can assign into this from
        # the get_or_none function
        mgr_map = {'x': None}

        def _get_or_none():
            mgr_map['x'] = self.mgr_cluster.get_mgr_map()
            result = mgr_map['x']['services'].get(service_name, None)
            return result

        self.wait_until_true(lambda: _get_or_none() is not None, 30)

        uri = mgr_map['x']['services'][service_name]

        log.info("Found {0} at {1} (daemon {2}/{3})".format(
            service_name, uri, mgr_map['x']['active_name'],
            mgr_map['x']['active_gid']))

        return uri


    def _assign_ports(self, module_name, config_name, min_port=7789):
        """
        To avoid the need to run lots of hosts in teuthology tests to
        get different URLs per mgr, we will hand out different ports
        to each mgr here.

        This is already taken care of for us when running in a vstart
        environment.
        """
        # Start handing out ports well above Ceph's range.
        assign_port = min_port

        for mgr_id in self.mgr_cluster.mgr_ids:
            self.mgr_cluster.mgr_stop(mgr_id)
            self.mgr_cluster.mgr_fail(mgr_id)

        for mgr_id in self.mgr_cluster.mgr_ids:
            log.info("Using port {0} for {1} on mgr.{2}".format(
                assign_port, module_name, mgr_id
            ))
            self.mgr_cluster.set_module_localized_conf(module_name, mgr_id,
                                                       config_name,
                                                       str(assign_port))
            assign_port += 1

        for mgr_id in self.mgr_cluster.mgr_ids:
            self.mgr_cluster.mgr_restart(mgr_id)

        def is_available():
            mgr_map = self.mgr_cluster.get_mgr_map()
            done = mgr_map['available']
            if done:
                log.info("Available after assign ports (new active {0}/{1})".format(
                    mgr_map['active_name'] , mgr_map['active_gid']))
            return done
        self.wait_until_true(is_available, timeout=30)
