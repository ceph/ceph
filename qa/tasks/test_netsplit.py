from tasks.ceph_test_case import CephTestCase
import logging
import json
from tasks.netsplit import disconnect, reconnect, get_ip_and_ports
import itertools
import time
from io import StringIO
log = logging.getLogger(__name__)


class TestNetSplit(CephTestCase):
    MON_LIST = ["mon.a", "mon.c", "mon.e"]
    CLUSTER = "ceph"
    WRITE_PERIOD = 10
    READ_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 6
    SUCCESS_HOLD_TIME = 10
    CLIENT = "client.0"
    POOL = "stretch_pool"

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

    def _get_pg_stats(self):
        """
        Dump the cluster and get pg stats
        """
        (client,) = self.ctx.cluster.only(self.CLIENT).remotes.keys()
        arg = ['ceph', 'pg', 'dump', '--format=json']
        proc = client.run(args=arg, wait=True, stdout=StringIO(), timeout=30)
        if proc.exitstatus != 0:
            log.error("pg dump failed")
            raise Exception("pg dump failed")
        out = proc.stdout.getvalue()
        j = json.loads('\n'.join(out.split('\n')[1:]))
        try:
            return j['pg_map']['pg_stats']
        except KeyError:
            return j['pg_stats']

    def _get_active_pg(self, pgs):
        """
        Get the number of active PGs
        """
        num_active = 0
        for pg in pgs:
            if (pg['state'].count('active') and
                    not pg['state'].count('stale') and
                    not pg['state'].count('laggy')):
                num_active += 1
        return num_active

    def _print_not_active_clean_pg(self, pgs):
        """
        Print the PGs that are not active+clean.
        """
        for pg in pgs:
            if not (pg['state'].count('active') and
                    pg['state'].count('clean') and
                    not pg['state'].count('stale')):
                log.debug(
                    "PG %s is not active+clean, but %s",
                    pg['pgid'], pg['state']
                )

    def _print_not_active_pg(self, pgs):
        """
        Print the PGs that are not active.
        """
        for pg in pgs:
            if not (pg['state'].count('active') and
                    not pg['state'].count('stale')):
                log.debug(
                    "PG %s is not active, but %s",
                    pg['pgid'], pg['state']
                )

    def _pg_all_active(self):
        """
        Check if all pgs are active.
        """
        pgs = self._get_pg_stats()
        result = self._get_active_pg(pgs) == len(pgs)
        if result:
            log.debug("All PGs are active")
        else:
            log.debug("Not all PGs are active")
            self._print_not_active_pg(pgs)
        return result

    def _get_active_clean_pg(self, pgs):
        """
        Get the number of active+clean PGs
        """
        num_active_clean = 0
        for pg in pgs:
            if (pg['state'].count('active') and
                pg['state'].count('clean') and
                    not pg['state'].count('stale') and
                    not pg['state'].count('laggy')):
                num_active_clean += 1
        return num_active_clean

    def _pg_all_active_clean(self):
        """
        Check if all pgs are active and clean.
        """
        pgs = self._get_pg_stats()
        result = self._get_active_clean_pg(pgs) == len(pgs)
        if result:
            log.debug("All PGs are active+clean")
        else:
            log.debug("Not all PGs are active+clean")
            self._print_not_active_clean_pg(pgs)
        return result

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
        (client,) = self.ctx.cluster.only(self.CLIENT).remotes.keys()
        arg = ['ceph', '-s']
        proc = client.run(args=arg, wait=True, stdout=StringIO(), timeout=30)
        if proc.exitstatus != 0:
            log.error("ceph -s failed, cluster is not accessible")
            return False
        else:
            log.info("Cluster is accessible")
            return True

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

    def test_netsplit_dc1_dc2(self):
        """
        Test Netsplit between dc1 and dc2
        """
        log.info("Running test_mon_netsplit_dc1_dc2")
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
        config = ["mon.a", "mon.c"]
        # disconnect the mons
        self._disconnect_mons(config)
        # wait for the mons to be disconnected (2 minutes)
        time.sleep(self.RECOVERY_PERIOD*2)
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
        # see how many PGs are active or inactive
        start_time = time.time()
        while time.time() - start_time < self.RECOVERY_PERIOD:
            self._pg_all_active()
            time.sleep(1)
        # get the client from the cluster
        (client,) = self.ctx.cluster.only(self.CLIENT).remotes.keys()
        # check if the cluster accepts writes
        args = [
            "rados", "-p", self.POOL, "bench", str(self.WRITE_PERIOD), 'write',
            '-b', '1024', '--no-cleanup'
            ]
        try:
            client.run(args=args, wait=True, timeout=self.WRITE_PERIOD*2)
            log.info("Write operation successful")
        except Exception:
            log.error("Write operation failed")
            assert False, "Write operation failed"
        # check if the cluster accepts random reads
        args = [
            "rados", "-p", self.POOL, "bench", str(self.READ_PERIOD), 'rand'
            ]
        try:
            client.run(args=args, wait=True, timeout=self.READ_PERIOD*2)
            log.info("Read operation successful")
        except Exception:
            log.error("Read operation failed")
            assert False, "Read operation failed"
        # reconnect the mons
        self._reconnect_mons(config)
        # wait for the mons to be reconnected
        time.sleep(self.RECOVERY_PERIOD)
        # check if the mons are reconnected
        self.wait_until_true(
            lambda: self._check_if_connect(config),
            timeout=self.RECOVERY_PERIOD,
        )
        # check if all the PGs are active+clean
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD * 5,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        log.info("test_mon_netsplit_dc1_dc2 passed!")

    def test_netsplit_arbiter_dc1_and_dc1_dc2(self):
        """
        Test Netsplit arbiter-dc1, dc1-dc2
        """
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
        arb_dc1 = ["mon.e", "mon.a"]
        # disconnect the mons
        self._disconnect_mons(arb_dc1)
        # wait for the mons to be disconnected (2 minutes)
        time.sleep(self.RECOVERY_PERIOD*2)
        # check if the mons are disconnected
        self.wait_until_true(
            lambda: self._check_if_disconnect(arb_dc1),
            timeout=self.RECOVERY_PERIOD,
        )
        dc1_dc2 = ["mon.a", "mon.c"]
        # disconnect the mons
        self._disconnect_mons(dc1_dc2)
        # wait for the mons to be disconnected (2 minutes)
        time.sleep(self.RECOVERY_PERIOD*2)
        # check if the mons are disconnected
        self.wait_until_true(
            lambda: self._check_if_disconnect(dc1_dc2),
            timeout=self.RECOVERY_PERIOD,
        )
        # check the cluster is accessible
        self.wait_until_true_and_hold(
            lambda: self._reply_to_mon_command(),
            timeout=self.RECOVERY_PERIOD * 5,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # get the client from the cluster
        (client,) = self.ctx.cluster.only(self.CLIENT).remotes.keys()
        # check if the cluster accepts writes
        args = [
            "rados", "-p", self.POOL, "bench", str(self.WRITE_PERIOD), 'write',
            '-b', '1024', '--no-cleanup'
            ]
        try:
            client.run(args=args, wait=True, timeout=self.WRITE_PERIOD*2)
            log.info("Write operation successful")
        except Exception:
            log.error("Write operation failed")
            assert False, "Write operation failed"
        # check if the cluster accepts random reads
        args = [
            "rados", "-p", self.POOL, "bench", str(self.READ_PERIOD), 'rand'
            ]
        try:
            client.run(args=args, wait=True, timeout=self.READ_PERIOD*2)
            log.info("Read operation successful")
        except Exception:
            log.error("Read operation failed")
            assert False, "Read operation failed"
        # reconnect the mons
        self._reconnect_mons(arb_dc1)
        # wait for the mons to be reconnected
        time.sleep(self.RECOVERY_PERIOD)
        # check if the mons are reconnected
        self.wait_until_true(
            lambda: self._check_if_connect(arb_dc1),
            timeout=self.RECOVERY_PERIOD,
        )
        # reconnect the mons
        self._reconnect_mons(dc1_dc2)
        # wait for the mons to be reconnected
        time.sleep(self.RECOVERY_PERIOD)
        # check if the mons are reconnected
        self.wait_until_true(
            lambda: self._check_if_connect(dc1_dc2),
            timeout=self.RECOVERY_PERIOD,
        )
        # check if all the PGs are active+clean
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD * 5,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        log.info("test_netsplit_arbiter_dc1_and_dc1_dc2 passed!")
