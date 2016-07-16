
from unittest import case
import json

from teuthology import misc
from tasks.ceph_test_case import CephTestCase

# TODO move definition of CephCluster
from tasks.cephfs.filesystem import CephCluster


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

    @property
    def admin_remote(self):
        first_mon = misc.get_first_mon(self._ctx, None)
        (result,) = self._ctx.cluster.only(first_mon).remotes.iterkeys()
        return result

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


class MgrTestCase(CephTestCase):
    REQUIRE_MGRS = 1

    def setUp(self):
        super(MgrTestCase, self).setUp()

        # The test runner should have populated this
        assert self.mgr_cluster is not None

        if len(self.mgr_cluster.mgr_ids) < self.REQUIRE_MGRS:
            raise case.SkipTest("Only have {0} manager daemons, "
                                "{1} are required".format(
                len(self.mgr_cluster.mgr_ids), self.REQUIRE_MGRS))

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
