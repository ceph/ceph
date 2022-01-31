import json
import logging
import time

from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestCephadmCLI(MgrTestCase):
    def _cmd(self, *args) -> str:
        assert self.mgr_cluster is not None
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _orch_cmd(self, *args) -> str:
        return self._cmd("orch", *args)

    def setUp(self):
        super(TestCephadmCLI, self).setUp()
    
    def _create_and_write_pool(self, pool_name):
        "Simulate some writes"
        self.mgr_cluster.mon_manager.create_pool(pool_name)
        args = [
            "rados", "-p", pool_name, "bench", "30", "write", "-t", "16"]
        self.mgr_cluster.admin_remote.run(args=args, wait=True)

    def test_apply_mon(self):
        self._orch_cmd('apply', 'mon', '3')
        self.wait_for_health_clear(90)
        time.sleep(30)
        self._create_and_write_pool('test_pool1')
        time.sleep(10)
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )
        log.warning("test_apply_mon: crash ls returns %s" % retstr)
        self.assertEqual(0, len(retstr))
        self._orch_cmd('apply', 'mon', '5')
        self.wait_for_health_clear(90)
        time.sleep(30)
        self._create_and_write_pool('test_pool2')
        time.sleep(10)
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )
        log.warning("test_apply_mon: crash ls returns %s" % retstr)
        self.assertEqual(0, len(retstr))
