import json
import logging

from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestCephadmCLI(MgrTestCase):

    APPLY_MON_PERIOD = 60

    def _cmd(self, *args) -> str:
        assert self.mgr_cluster is not None
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _orch_cmd(self, *args) -> str:
        return self._cmd("orch", *args)

    def setUp(self):
        super(TestCephadmCLI, self).setUp()

    def _create_and_write_pool(self, pool_name):
        # Create new pool and write to it, simulating a small workload.
        self.mgr_cluster.mon_manager.create_pool(pool_name)
        args = [
            "rados", "-p", pool_name, "bench", "30", "write", "-t", "16"]
        self.mgr_cluster.admin_remote.run(args=args, wait=True)
    
    def _get_quorum_size(self) -> int:
        # Evaluate if the quorum size of the cluster is correct.
        # log the quorum_status before reducing the monitors
        retstr = self._cmd('quorum_status')
        log.info("test_apply_mon._check_quorum_size: %s" % json.dumps(retstr, indent=2))
        quorum_size = len(json.loads(retstr)['quorum']) # get quorum size
        return quorum_size

    def _check_no_crashes(self):
        # Evaluate if there are no crashes
        # log the crash
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'crash', 'ls',
        )
        log.info("test_apply_mon._check_no_crashes: %s" % retstr)
        self.assertEqual(0, len(retstr)) # check if there are no crashes

    def test_apply_mon_three(self):
        # Evaluating the process of reducing the number of 
        # monitors from 5 to 3 and increasing the number of
        # monitors from 3 to 5, using the `ceph orch apply mon <num>` command.

        self.wait_until_equal(lambda : self._get_quorum_size(), 5,
                      timeout=self.APPLY_MON_PERIOD, period=10)

        self._orch_cmd('apply', 'mon', '3') # reduce the monitors from 5 -> 3

        self._create_and_write_pool('test_pool1')

        self.wait_until_equal(lambda : self._get_quorum_size(), 3,
                      timeout=self.APPLY_MON_PERIOD, period=10)

        self._check_no_crashes()

        self._orch_cmd('apply', 'mon', '5') # increase the monitors from 3 -> 5

        self._create_and_write_pool('test_pool2')

        self.wait_until_equal(lambda : self._get_quorum_size(), 5,
                      timeout=self.APPLY_MON_PERIOD, period=10)

        self._check_no_crashes()