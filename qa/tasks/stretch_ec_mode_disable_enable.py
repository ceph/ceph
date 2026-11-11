"""
An erasure coded pool over two zones moved into one zone and back with
osd pool set num_zones. The rados/singleton stretch-ec-mode-5-mons-8-osds
job runs it after mon-stretch-ec-mode-5-mons-8-osds.sh.
"""
import logging
from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)

class TestStretchMode(MgrTestCase):
    """
    The cluster that mon-stretch-ec-mode-5-mons-8-osds.sh lays out:
    datacenters dc1 and dc2, each of 2 hosts with 2 OSDs, monitors a and b
    in dc1, c and d in dc2, and the tiebreaker e in dc3. setUp removes every
    pool, so the test's pool is the only one: the cluster stretch flags are
    on while it is over the two datacenters, and off once it is in one.
    """
    POOL = 'stretch_ec_pool'
    CLUSTER = "ceph"
    WRITE_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 6
    SUCCESS_HOLD_TIME = 7
    # made by mon-stretch-ec-mode-5-mons-8-osds.sh
    STRETCH_EC_PROFILE = 'stretch_ec_profile'
    STRETCH_EC_CRUSH_RULE = 'stretch_ec_rule'
    # the rules that osd pool set num_zones generates for the pool
    SINGLE_ZONE_CRUSH_RULE = POOL + '-single-zone'
    STRETCH_CRUSH_RULE = POOL + '-stretch'
    STRETCH_BUCKET_TYPE = 'datacenter'
    TIEBREAKER_MON_NAME = 'e'
    DEFAULT_POOL_SIZE = 3
    DEFAULT_POOL_MIN_SIZE = 2
    # This dictionary maps the datacenter to the osd ids and hosts
    DC_OSDS = {
        'dc1': {
            "host01": [0, 1],
            "host02": [2, 3],
        },
        'dc2': {
            "host03": [4, 5],
            "host04": [6, 7],
        },
    }
    DC_MONS = {
        'dc1': {
            "host01": ['a'],
            "host02": ['b'],
        },
        'dc2': {
            "host03": ['c'],
            "host04": ['d'],
        },
        'dc3': {
            "host05": ['e'],
        }
    }
    def _osd_count(self):
        """
        Get the number of OSDs in the cluster.
        """
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return len(osd_map['osds'])

    def setUp(self):
        """
        Setup the cluster and
        ensure we have a clean condition before the test.
        """
        # Ensure we have at least 6 OSDs
        super(TestStretchMode, self).setUp()
        if self._osd_count() < 4:
            self.skipTest("Not enough OSDS!")

        # Remove any filesystems so that we can remove their pools
        if self.mds_cluster:
            self.mds_cluster.mds_stop()
            self.mds_cluster.mds_fail()
            self.mds_cluster.delete_all_filesystems()

        # Remove all other pools
        for pool in self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']:
            try:
                self.mgr_cluster.mon_manager.remove_pool(pool['pool_name'])
            except:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'pool', 'delete',
                    pool['pool_name'],
                    pool['pool_name'],
                    '--yes-i-really-really-mean-it')

    def _setup_pool(self, pool_name=POOL, pg_num=16,
                    crush_rule=STRETCH_EC_CRUSH_RULE):
        """
        Create an erasure coded pool over two zones with the profile and
        the rule of the setup workunit.
        """
        self.mgr_cluster.mon_manager.create_pool(
            pool_name, pg_num=pg_num,
            erasure_code_profile_name=self.STRETCH_EC_PROFILE,
            erasure_code_crush_rule_name=crush_rule, num_zones=2)

    def _write_some_data(self, t):
        """
        Write some data to the pool to simulate a workload.
        """
        args = [
            "rados", "-p", self.POOL, "bench", str(t), "write", "-t", "16"]
        self.mgr_cluster.admin_remote.run(args=args, wait=True)

    def _get_all_mons_from_all_dc(self):
        """
        Get all mons from all datacenters.
        """
        return [mon for dc in self.DC_MONS.values() for mons in dc.values() for mon in mons]

    def _bring_back_mon(self, mon):
        """
        Bring back the mon.
        """
        log.debug("_bring_back_mon %s", mon)
        # If the mon is already up, do nothing
        quorum_names = self.mgr_cluster.mon_manager.get_mon_quorum_names()
        if mon in quorum_names:
            log.debug("mon.%s is already up", mon)
            return
        # If the mon is not up, try to bring it back
        log.debug("Bringing back mon.%s", mon)
        try:
            self.ctx.daemons.get_daemon('mon', mon, self.CLUSTER).restart()
        except Exception:
            log.error("Failed to bring back mon.{}".format(str(mon)))
            pass

    def _get_osd_host(self, osd):
        """
        Get the host of the osd.
        """
        for dc, nodes in self.DC_OSDS.items():
            for node, osds in nodes.items():
                if osd in osds:
                    return node
        return None

    def _move_osd_back_to_host(self, osd):
        """
        Move the osd back to the host.
        """
        host = self._get_osd_host(osd)
        assert host is not None, "The host of osd {} is not found.".format(osd)
        log.debug("Moving osd.%d back to %s", osd, host)
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'crush', 'move', 'osd.{}'.format(str(osd)),
            'host={}'.format(host)
        )

    def tearDown(self):
        """
        Clean up the cluster after the test.
        """
        # Remove the pool
        log.debug("Tear down the test")
        if self.POOL in self.mgr_cluster.mon_manager.pools:
            self.mgr_cluster.mon_manager.remove_pool(self.POOL)

        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for osd in osd_map['osds']:
            # mark all the osds in
            if osd['weight'] == 0.0:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', str(osd['osd']))
            # Bring back all the osds and move it back to the host.
            if osd['up'] == 0:
                self.mgr_cluster.mon_manager.revive_osd(osd['osd'])
                self._move_osd_back_to_host(osd['osd'])

        # Set the mon_netsplit_grace_period to 30 seconds.
        # Sometimes when many mons restart at the same time
        # it can take longer for the monitors to establish
        # a connection.
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mon', 'mon_netsplit_grace_period', '30'
        )
        # Bring back all the mons
        mons = self._get_all_mons_from_all_dc()
        for mon in mons:
            self._bring_back_mon(mon)
        super(TestStretchMode, self).tearDown()

    def _kill_osd(self, osd):
        """
        Kill the osd.
        """
        try:
            self.ctx.daemons.get_daemon('osd', osd, self.CLUSTER).stop()
        except Exception:
            log.error("Failed to stop osd.{}".format(str(osd)))
            pass

    def _get_osds_data(self, want_osds):
        """
        Get the osd data
        """
        all_osds_data = \
            self.mgr_cluster.mon_manager.get_osd_dump_json()['osds']
        return [
            osd_data for osd_data in all_osds_data
            if int(osd_data['osd']) in want_osds
        ]

    def _get_osds_by_dc(self, dc):
        """
        Get osds by datacenter.
        """
        ret = []
        for host, osds in self.DC_OSDS[dc].items():
            ret.extend(osds)
        return ret

    def _fail_over_all_osds_in_dc(self, dc):
        """
        Fail over all osds in specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_OSDS:
            raise ValueError(
                "dc must be one of the following: %s" % self.DC_OSDS.keys()
                )
        log.debug("Failing over all osds in %s", dc)
        osds = self._get_osds_by_dc(dc)
        # fail over all the OSDs in the DC
        log.debug("OSDs to failed over: %s", osds)
        for osd_id in osds:
            self._kill_osd(osd_id)
        # wait until all the osds are down
        self.wait_until_true(
            lambda: all([int(osd['up']) == 0
                        for osd in self._get_osds_data(osds)]),
            timeout=self.RECOVERY_PERIOD
        )

    def _check_mons_out_of_quorum(self, want_mons):
        """
        Check if the mons are not in quorum.
        """
        quorum_names = self.mgr_cluster.mon_manager.get_mon_quorum_names()
        return all([mon not in quorum_names for mon in want_mons])

    def _bring_back_dc(self, dc):
        """
        Bring back the osds and mons of the datacenter.
        """
        for osd in self._get_osds_by_dc(dc):
            self.mgr_cluster.mon_manager.revive_osd(osd)
        for host, mons in self.DC_MONS[dc].items():
            for mon in mons:
                self._bring_back_mon(mon)

    def _kill_mon(self, mon):
        """
        Kill the mon.
        """
        try:
            self.ctx.daemons.get_daemon('mon', mon, self.CLUSTER).stop()
        except Exception:
            log.error("Failed to stop mon.{}".format(str(mon)))
            pass

    def _get_mons_by_dc(self, dc):
        """
        Get mons by datacenter.
        """
        ret = []
        for host, mons in self.DC_MONS[dc].items():
            ret.extend(mons)
        return ret

    def _fail_over_all_mons_in_dc(self, dc):
        """
        Fail over all mons in the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_MONS:
            raise ValueError("dc must be one of the following: %s" %
                             ", ".join(self.DC_MONS.keys()))
        log.debug("Failing over all mons %s", dc)
        mons = self._get_mons_by_dc(dc)
        log.debug("Mons to be failed over: %s", mons)
        for mon in mons:
            self._kill_mon(mon)
        # wait until all the mons are out of quorum
        self.wait_until_true(
            lambda: self._check_mons_out_of_quorum(mons),
            timeout=self.RECOVERY_PERIOD
        )

    def _stretch_mode_enabled_correctly(self, crush_rule):
        """
        Check that every pool is over the two datacenters on crush_rule, with
        6 shards and min_size 2, and that the cluster stretch flags are on in
        the OSDMap and the monmap, with tiebreaker e and no zone down.
        """
        crush_rule_id = self.mgr_cluster.mon_manager.get_crush_rule_id(
            crush_rule)
        # Checking the OSDMap
        osdmap = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for pool in osdmap['pools']:
            # expects crush_rule to be crush_rule
            self.assertEqual(
                crush_rule_id,
                pool['crush_rule']
            )
            # expects pool size to be 6
            self.assertEqual(
                6,
                pool['size']
            )
            # expects pool min_size to be 2
            self.assertEqual(
                2,
                pool['min_size']
            )
            # expects pool is_stretch_pool flag to be true
            self.assertEqual(
                True,
                pool['is_stretch_pool']
            )
            # expects peering_crush_bucket_count = 2 (the two zones)
            self.assertEqual(
                2,
                pool['peering_crush_bucket_count']
            )
            # expects peering_crush_bucket_target = 2 (the two zones)
            self.assertEqual(
                2,
                pool['peering_crush_bucket_target']
            )
            # expects peering_crush_bucket_barrier = 8 (crush type of datacenter is 8)
            self.assertEqual(
                8,
                pool['peering_crush_bucket_barrier']
            )
        # expects stretch_mode_enabled to be True
        self.assertEqual(
            True,
            osdmap['stretch_mode']['stretch_mode_enabled']
        )
        # expects stretch_mode_bucket_count to be 2
        self.assertEqual(
            2,
            osdmap['stretch_mode']['stretch_bucket_count']
        )
        # expects degraded_stretch_mode to be 0
        self.assertEqual(
            0,
            osdmap['stretch_mode']['degraded_stretch_mode']
        )
        # expects recovering_stretch_mode to be 0
        self.assertEqual(
            0,
            osdmap['stretch_mode']['recovering_stretch_mode']
        )
        # expects stretch_mode_bucket to be 8 (datacenter crush type = 8)
        self.assertEqual(
            8,
            osdmap['stretch_mode']['stretch_mode_bucket']
        )
        # Checking the MonMap
        monmap = self.mgr_cluster.mon_manager.get_mon_dump_json()
        # expects stretch_mode to be True
        self.assertEqual(
            True,
            monmap['stretch_mode']
        )
        # expects disallowed_leaders to be tiebreaker_mon
        self.assertEqual(
            self.TIEBREAKER_MON_NAME,
            monmap['disallowed_leaders']
        )
        # expects tiebreaker_mon to be tiebreaker_mon
        self.assertEqual(
            self.TIEBREAKER_MON_NAME,
            monmap['tiebreaker_mon']
        )
        log.debug("Stretch mode is enabled correctly.")

    def _stretch_mode_disabled_correctly(self, crush_rule):
        """
        Check that every pool is in one zone on crush_rule, with 3 shards and
        min_size 2, and that the cluster stretch flags are off in the OSDMap
        and the monmap.
        """
        crush_rule_id = self.mgr_cluster.mon_manager.get_crush_rule_id(
            crush_rule)
        # Checking the OSDMap

        osdmap = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for pool in osdmap['pools']:
            # expects crush_rule to be crush_rule
            self.assertEqual(
                crush_rule_id,
                pool['crush_rule']
            )
            # expects pool size to be default
            self.assertEqual(
                self.DEFAULT_POOL_SIZE,
                pool['size']
            )
            # expects pool min_size to be default
            self.assertEqual(
                self.DEFAULT_POOL_MIN_SIZE,
                pool['min_size']
            )
            # expects pool is_stretch_pool flag to be false
            self.assertEqual(
                False,
                pool['is_stretch_pool']
            )
            # expects peering_crush_bucket_count = 0
            self.assertEqual(
                0,
                pool['peering_crush_bucket_count']
            )
            # expects peering_crush_bucket_target = 0
            self.assertEqual(
                0,
                pool['peering_crush_bucket_target']
            )
            # expects peering_crush_bucket_barrier = 0
            self.assertEqual(
                0,
                pool['peering_crush_bucket_barrier']
            )
        # expects stretch_mode_enabled to be False
        self.assertEqual(
            False,
            osdmap['stretch_mode']['stretch_mode_enabled']
        )
        # expects stretch_mode_bucket to be 0
        self.assertEqual(
            0,
            osdmap['stretch_mode']['stretch_bucket_count']
        )
        # expects degraded_stretch_mode to be 0
        self.assertEqual(
            0,
            osdmap['stretch_mode']['degraded_stretch_mode']
        )
        # expects recovering_stretch_mode to be 0
        self.assertEqual(
            0,
            osdmap['stretch_mode']['recovering_stretch_mode']
        )
        # expects stretch_mode_bucket to be 0
        self.assertEqual(
            0,
            osdmap['stretch_mode']['stretch_mode_bucket']
        )
        # Checking the MonMap
        monmap = self.mgr_cluster.mon_manager.get_mon_dump_json()
        # expects stretch_mode to be False
        self.assertEqual(
            False,
            monmap['stretch_mode']
        )
        # expects disallowed_leaders to be empty
        self.assertEqual(
            "",
            monmap['disallowed_leaders']
        )
        # expects tiebreaker_mon to be empty
        self.assertEqual(
            "",
            monmap['tiebreaker_mon']
        )
        log.debug("Stretch mode is disabled correctly.")

    def test_disable_stretch_mode(self):
        """
        osd pool set <pool> num_zones 1 moves an erasure coded pool over two
        zones into one, onto a rule of its own, <pool>-single-zone, and is
        refused without --yes-i-really-mean-it. This test does it twice for
        the pool on stretch_ec_rule: first with both datacenters up, after
        which num_zones 2 moves the pool back over both, onto
        <pool>-stretch; then with dc1's OSDs and monitors down and
        degraded_stretch_mode set. Each PG keeps the shards in its first
        zone, so the PGs whose first zone is dc1 are unavailable until dc1
        is back; then all PGs are active+clean.
        """
        # Create a pool
        self._setup_pool(self.POOL)
        # Write some data to the pool
        self._write_some_data(self.WRITE_PERIOD)

        # num_zones 1 without --yes-i-really-mean-it (expects -EPERM 1)
        self.assertEqual(
            1,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'osd',
                'pool',
                'set',
                self.POOL,
                'num_zones',
                '1'
            ))
        # num_zones 1 with --yes-i-really-mean-it (expect success 0)
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'osd',
                'pool',
                'set',
                self.POOL,
                'num_zones',
                '1',
                '--yes-i-really-mean-it'
            ))
        # The pool is in one zone and the cluster stretch flags are off
        self._stretch_mode_disabled_correctly(self.SINGLE_ZONE_CRUSH_RULE)
        # all PGs are active + clean
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # write some data to the pool
        self._write_some_data(self.WRITE_PERIOD)
        # num_zones 2 moves the pool back over both zones
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'osd',
                'pool',
                'set',
                self.POOL,
                'num_zones',
                '2'
            ))
        self._stretch_mode_enabled_correctly(self.STRETCH_CRUSH_RULE)
        # all PGs are active + clean
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # write some data to the pool
        # self._write_some_data(self.WRITE_PERIOD)
        # Bring down dc1
        self._fail_over_all_osds_in_dc('dc1')
        self._fail_over_all_mons_in_dc('dc1')
        # degraded_stretch_mode is set
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.is_degraded_stretch_mode(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # num_zones 1 with dc1 down (expect success 0).
        # Each PG keeps the shards of its first zone, which is dc1 for about
        # half of them: those are unavailable until dc1 is back.
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'osd',
                'pool',
                'set',
                self.POOL,
                'num_zones',
                '1',
                '--yes-i-really-mean-it'
            ))
        # The pool is in one zone and the cluster stretch flags are off
        self._stretch_mode_disabled_correctly(self.SINGLE_ZONE_CRUSH_RULE)
        # Those PGs recover once dc1 is back
        self._bring_back_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
