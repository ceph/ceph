import json
import logging
from tempfile import NamedTemporaryFile
from teuthology.exceptions import CommandFailedError
from mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)

class TestOrchestratorCli(MgrTestCase):
    MGRS_REQUIRED = 1

    def _orch_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("orchestrator", *args)

    def setUp(self):
        super(TestOrchestratorCli, self).setUp()
        self._load_module("orchestrator_cli")
        self._load_module("ssh")
        self._orch_cmd("set", "backend", "ssh")

    def test_host_ls(self):
        self._orch_cmd("host", "add", "osd0")
        self._orch_cmd("host", "add", "mon0")
        ret = self._orch_cmd("host", "ls")
        self.assertIn("osd0", ret)
        self.assertIn("mon0", ret)
