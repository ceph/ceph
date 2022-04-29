import json
import logging
import time
from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestCephadmCLI(MgrTestCase):

    APPLY_MON_PERIOD = 60 # Shouldn't be this long, but just incase.

    def _cmd(self, *args) -> str:
        assert self.mgr_cluster is not None
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _orch_cmd(self, *args) -> str:
        return self._cmd("orch", *args)

    def setUp(self):
        super(TestCephadmCLI, self).setUp()

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

    def _apply_mon_write_pool_and_check(self, num, pool_name):
        # Increase/Decrease the monitors to <num> by exercising the orch apply mon command.
        # Write to <pool_name> simulate real workloads during apply command.
        # Check for crashes and unhealthy cluster states.

        self._orch_cmd('apply', 'mon', num)

        args = ["rados", "-p", pool_name, "bench", "60", "write", "-t", "16"]

        self.mgr_cluster.admin_remote.run(args=args, wait=True) # Write to pool.

        self.wait_until_equal(lambda : self._get_quorum_size(), int(num),
                      timeout=self.APPLY_MON_PERIOD, period=10)

        self._check_no_crashes() # Check for any crashes.

        time.sleep(60) # Buffer for fairness, incase any unhealthy status needed time to appear.

        self.wait_for_health_clear(120) # Make sure there is no unhealthy state, e.g., Stray Daemon

    def test_decrease_increase_mon(self):
        # Evaluating the process of reducing the number of
        # monitors from 5 to 3 and increasing the number of
        # monitors from 3 to 5, using the `ceph orch apply mon <num>` command.

        # Make sure we have 5 MONs at the beginning.
        self.wait_until_equal(lambda : self._get_quorum_size(), 5,
                      timeout=self.APPLY_MON_PERIOD, period=10)

        # Create 1 pool to be used as part of the test.
        self.mgr_cluster.mon_manager.create_pool("test_pool1")

        self._apply_mon_write_pool_and_check("3", "test_pool1") # Reduce MON and check.
        self._apply_mon_write_pool_and_check("5", "test_pool1") # Increase MON and check.