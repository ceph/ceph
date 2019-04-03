import errno
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

    def _orch_cmd_result(self, *args, **kwargs):
        """
        raw_cluster_cmd doesn't support kwargs.
        """
        return self.mgr_cluster.mon_manager.raw_cluster_cmd_result("orchestrator", *args, **kwargs)

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

    def test_device_ls_refresh(self):
        ret = self._orch_cmd("device", "ls", "--refresh")
        self.assertIn("localhost:", ret)

    def test_device_ls_hoshs(self):
        ret = self._orch_cmd("device", "ls", "localhost", "host1")
        self.assertIn("localhost:", ret)


    def test_device_ls_json(self):
        ret = self._orch_cmd("device", "ls", "--format", "json")
        self.assertIn("localhost", ret)
        self.assertIsInstance(json.loads(ret), list)

    def test_service_ls(self):
        ret = self._orch_cmd("service", "ls")
        self.assertIn("ceph-mgr", ret)

    def test_service_ls_json(self):
        ret = self._orch_cmd("service", "ls", "--format", "json")
        self.assertIsInstance(json.loads(ret), list)
        self.assertIn("ceph-mgr", ret)


    def test_service_action(self):
        self._orch_cmd("service", "reload", "mds", "cephfs")
        self._orch_cmd("service", "stop", "mds", "cephfs")
        self._orch_cmd("service", "start", "mds", "cephfs")

    def test_service_instance_action(self):
        self._orch_cmd("service-instance", "reload", "mds", "a")
        self._orch_cmd("service-instance", "stop", "mds", "a")
        self._orch_cmd("service-instance", "start", "mds", "a")

    def test_osd_create(self):
        self._orch_cmd("osd", "create", "*:device")
        self._orch_cmd("osd", "create", "*:device,device2")

        drive_group = {
            "host_pattern": "*",
            "data_devices": {"paths": ["/dev/sda"]}
        }

        res = self._orch_cmd_result("osd", "create", "-i", "-", stdin=json.dumps(drive_group))
        self.assertEqual(res, 0)

        with self.assertRaises(CommandFailedError):
            self._orch_cmd("osd", "create", "notfound:device")

    def test_mds_add(self):
        self._orch_cmd("mds", "add", "service_name")

    def test_rgw_add(self):
        self._orch_cmd("rgw", "add", "service_name")

    def test_nfs_add(self):
        self._orch_cmd("nfs", "add", "service_name", "pool", "--namespace", "ns")
        self._orch_cmd("nfs", "add", "service_name", "pool")

    def test_osd_rm(self):
        self._orch_cmd("osd", "rm", "osd.0")

    def test_mds_rm(self):
        self._orch_cmd("mds", "rm", "foo")

    def test_rgw_rm(self):
        self._orch_cmd("rgw", "rm", "foo")

    def test_nfs_rm(self):
        self._orch_cmd("nfs", "rm", "service_name")

    def test_host_ls(self):
        out = self._orch_cmd("host", "ls")
        self.assertEqual(out, "localhost\n")

    def test_host_add(self):
        self._orch_cmd("host", "add", "hostname")

    def test_host_rm(self):
        self._orch_cmd("host", "rm", "hostname")

    def test_mon_update(self):
        self._orch_cmd("mon", "update", "3")
        self._orch_cmd("mon", "update", "3", "host1", "host2", "host3")
        self._orch_cmd("mon", "update", "3", "host1:network", "host2:network", "host3:network")

    def test_mgr_update(self):
        self._orch_cmd("mgr", "update", "3")

    def test_nfs_update(self):
        self._orch_cmd("nfs", "update", "service_name", "2")

    def test_error(self):
        ret = self._orch_cmd_result("host", "add", "raise_no_support")
        self.assertEqual(ret, errno.ENOENT)
        ret = self._orch_cmd_result("host", "add", "raise_bug")
        self.assertEqual(ret, errno.EINVAL)
        ret = self._orch_cmd_result("host", "add", "raise_not_implemented")
        self.assertEqual(ret, errno.ENOENT)
        ret = self._orch_cmd_result("host", "add", "raise_no_orchestrator")
        self.assertEqual(ret, errno.ENOENT)
        ret = self._orch_cmd_result("host", "add", "raise_import_error")
        self.assertEqual(ret, errno.ENOENT)
