from tasks.ceph_test_case import CephTestCase
import logging
from tasks.netsplit import disconnect, reconnect, get_ip_and_ports
import itertools
import time
log = logging.getLogger(__name__)


class TestNetSplit(CephTestCase):
    MON_LIST = ["mon.a", "mon.b", "mon.c"]
    CLUSTER = "ceph"
    WRITE_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 6
    SUCCESS_HOLD_TIME = 10

    def setUp(self):
        """
        Set up the cluster for the test.
        """
        super(TestNetSplit, self).setUp()

    def tearDown(self):
        """
        Clean up the cluter after the test.
        """
        super(TestNetSplit, self).tearDown()

    def _disconnect_mons(self, config):
        """
        Disconnect the mons in the <config> list.
        """
        disconnect(self.ctx, config)

    def _reconnect_mons(self, config):
        """
        Reconnect the mons in the <config> list.
        """
        reconnect(self.ctx, config)

    def _reply_to_mon_command(self):
        """
        Check if the cluster is accessible.
        """
        try:
            self.mgr_cluster.mon_manager.raw_cluster_cmd('status')
            return True
        except Exception:
            return False

    def _check_if_disconnect(self, config):
        """
        Check if the mons in the <config> list are disconnected.
        """
        assert config[0].startswith('mon.')
        assert config[1].startswith('mon.')
        log.info("Checking if the {} and {} are disconnected".format(
            config[0], config[1]))
        (ip1, _) = get_ip_and_ports(self.ctx, config[0])
        (ip2, _) = get_ip_and_ports(self.ctx, config[1])
        (host1,) = self.ctx.cluster.only(config[0]).remotes.keys()
        (host2,) = self.ctx.cluster.only(config[1]).remotes.keys()
        assert host1 is not None
        assert host2 is not None
        # if the mons are disconnected, the ping should fail (exitstatus = 1)
        try:
            if (host1.run(args=["ping", "-c", "1", ip2]).exitstatus == 0 or
                    host2.run(args=["ping", "-c", "1", ip1]).exitstatus == 0):
                return False
        except Exception:
            return True

    def _check_if_connect(self, config):
        """
        Check if the mons in the <config> list are connected.
        """
        assert config[0].startswith('mon.')
        assert config[1].startswith('mon.')
        log.info("Checking if {} and {} are connected".format(
                config[0], config[1]))
        (ip1, _) = get_ip_and_ports(self.ctx, config[0])
        (ip2, _) = get_ip_and_ports(self.ctx, config[1])
        (host1,) = self.ctx.cluster.only(config[0]).remotes.keys()
        (host2,) = self.ctx.cluster.only(config[1]).remotes.keys()
        assert host1 is not None
        assert host2 is not None
        # if the mons are connected, the ping should succeed (exitstatus = 0)
        try:
            if (host1.run(args=["ping", "-c", "1", ip2]).exitstatus == 0 and
                    host2.run(args=["ping", "-c", "1", ip1]).exitstatus == 0):
                return True
        except Exception:
            return False

    def test_mon_netsplit(self):
        """
        Test the mon netsplit.
        """
        log.info("Running test_mon_netsplit")
        # check if all the mons are connected
        self.wait_until_true(
            lambda: all(
                [
                    self._check_if_connect([mon1, mon2])
                    for mon1, mon2 in itertools.combinations(self.MON_LIST, 2)
                ]
            ),
            timeout=self.RECOVERY_PERIOD,
        )
        # Scenario 1: disconnect Site 1 and Site 2
        # Arbiter node is still connected to both sites
        config = ["mon.a", "mon.b"]
        # disconnect the mons
        self._disconnect_mons(config)
        # wait for the mons to be disconnected
        time.sleep(self.RECOVERY_PERIOD)
        # check if the mons are disconnected
        self.wait_until_true(
            lambda: self._check_if_disconnect(config),
            timeout=self.RECOVERY_PERIOD,
        )
        # check the cluster is accessible
        self.wait_until_true_and_hold(
            lambda: self._reply_to_mon_command(),
            timeout=self.RECOVERY_PERIOD * 5,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # reconnect the mons
        self._reconnect_mons(config)
        # wait for the mons to be reconnected
        time.sleep(self.RECOVERY_PERIOD)
        # check if the mons are reconnected
        self.wait_until_true(
            lambda: self._check_if_connect(config),
            timeout=self.RECOVERY_PERIOD,
        )
        log.info("test_mon_netsplit passed!")
