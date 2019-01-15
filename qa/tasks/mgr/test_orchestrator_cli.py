import logging

from mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestOrchestratorCli(MgrTestCase):
    MGRS_REQUIRED = 1

    def _orch_cmd(self, *args):
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd("orchestrator", *args)
        return retstr

    def setUp(self):
        super(TestOrchestratorCli, self).setUp()

        self._load_module("orchestrator_cli")
        self._load_module("test_orchestrator")
        self._orch_cmd("set", "backend", "test_orchestrator")

    def test_status(self):
        ret = self._orch_cmd("status")
        self.assertIn("test_orchestrator", ret)

    def test_device_ls(self):
        ret = self._orch_cmd("device", "ls")
        self.assertIn("localhost:", ret)

    def test_service_ls(self):
        ret = self._orch_cmd("service", "ls")
        self.assertIn("ceph-mgr", ret)

    def test_service_action(self):
        self._orch_cmd("service", "reload", "mds", "cephfs")
        self._orch_cmd("service", "stop", "mds", "cephfs")
        self._orch_cmd("service", "start", "mds", "cephfs")

    def test_service_instance_action(self):
        self._orch_cmd("service-instance", "reload", "mds", "a")
        self._orch_cmd("service-instance", "stop", "mds", "a")
        self._orch_cmd("service-instance", "start", "mds", "a")
