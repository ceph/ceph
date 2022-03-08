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

    def test_apply_mon_three(self):

        # Evaluating the process of reducing the number of monitors using
        # the `ceph orch apply mon <num>` command.

        retstr = self._cmd('quorum_status') # log the quorum_status before reducing the monitors
        log.warning("test_apply_mon: quorum_status before reduce returns %s" % json.dumps(retstr, indent=2))

        self._orch_cmd('apply', 'mon', '3') # reduce the monitors from 5 -> 3
        time.sleep(10)

        retstr = self._cmd('quorum_status') # log the quorum_status 10s after reducing the monitors
        log.warning("test_apply_mon: monstatus ~10s after reduce returns %s" % json.dumps(retstr, indent=2))

        self._create_and_write_pool('test_pool1') # Create and write some pools

        retstr = self._cmd('quorum_status') # log the quorum_status 26s after reducing the monitors
        log.warning("test_apply_mon: monstatus ~26s after reduce returns %s" % json.dumps(retstr, indent=2))

        quorum_size = len(json.loads(retstr)['quorum']) # get quorum size
        self.assertEqual(quorum_size, 3) # evaluate if quorum size == 3

        time.sleep(5)
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )

        log.warning("test_apply_mon: crash ls returns %s" % retstr)
        self.assertEqual(0, len(retstr)) # check if there are no crashes
