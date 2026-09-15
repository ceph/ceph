import logging
from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)

class TestStretchMode(MgrTestCase):
    """
    Test the stretch mode feature of Ceph
    """
    POOL = 'stretch_pool'
    CLUSTER = "ceph"
    WRITE_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 6
    SUCCESS_HOLD_TIME = 7
    STRETCH_CRUSH_RULE = 'stretch_rule'
    STRETCH_CRUSH_RULE_ID = None
    STRETCH_BUCKET_TYPE = 'datacenter'
    TIEBREAKER_MON_NAME = 'e'
    DEFAULT_POOL_TYPE = 'replicated'
    DEFAULT_POOL_CRUSH_RULE = 'replicated_rule'
    DEFAULT_POOL_SIZE = 3
    DEFAULT_POOL_MIN_SIZE = 2
    DEFAULT_POOL_CRUSH_RULE_ID = None
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
        self.DEFAULT_POOL_CRUSH_RULE_ID = self.mgr_cluster.mon_manager.get_crush_rule_id(self.DEFAULT_POOL_CRUSH_RULE)
        self.STRETCH_CRUSH_RULE_ID = self.mgr_cluster.mon_manager.get_crush_rule_id(self.STRETCH_CRUSH_RULE)
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

    def _setup_pool(
            self,
            pool_name=POOL,
            pg_num=16,
            pool_type=DEFAULT_POOL_TYPE,
            crush_rule=DEFAULT_POOL_CRUSH_RULE,
            size=None,
            min_size=None
        ):
        """
        Create a pool, set its size and pool if specified.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'create', pool_name, str(pg_num), pool_type, crush_rule)

        if size is not None:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'pool', 'set', pool_name, 'size', str(size))

        if min_size is not None:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'pool', 'set', pool_name, 'min_size', str(min_size))

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
        try:
            self.ctx.daemons.get_daemon('mon', mon, self.CLUSTER).restart()
        except Exception:
            log.error("Failed to bring back mon.{}".format(str(mon)))
            pass

    def _get_host(self, osd):
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
        host = self._get_host(osd)
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

    def _stretch_mode_enabled_correctly(self):
        """
        Evaluate whether the stretch mode is enabled correctly.
        by checking the OSDMap and MonMap.
        """
        # Checking the OSDMap
        osdmap = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for pool in osdmap['pools']:
            # expects crush_rule to be stretch_rule
            self.assertEqual(
                self.STRETCH_CRUSH_RULE_ID,
                pool['crush_rule']
            )
            # expects pool size to be 4
            self.assertEqual(
                4,
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
            # expects peering_crush_bucket_count = 2 (always this value for stretch mode)
            self.assertEqual(
                2,
                pool['peering_crush_bucket_count']
            )
            # expects peering_crush_bucket_target = 2 (always this value for stretch mode)
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

    def _stretch_mode_disabled_correctly(self):
        """
        Evaluate whether the stretch mode is disabled correctly.
        by checking the OSDMap and MonMap.
        """
        # Checking the OSDMap
        osdmap = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for pool in osdmap['pools']:
            # expects crush_rule to be default
            self.assertEqual(
                self.DEFAULT_POOL_CRUSH_RULE_ID,
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

    def test_disable_stretch_mode(self):
        """
        Test disabling stretch mode with the following scenario:
        1. Healthy Stretch Mode
        2. Degraded Stretch Mode
        """
        # Create a pool
        self._setup_pool(self.POOL, 16, 'replicated', self.STRETCH_CRUSH_RULE, 4, 2)
        # Write some data to the pool
        self._write_some_data(self.WRITE_PERIOD)
        # disable stretch mode without --yes-i-really-mean-it (expects -EPERM 1)
        self.assertEqual(
            1,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'mon',
                'disable_stretch_mode'
            ))
        # Disable stretch mode with non-existent crush rule (expects -EINVAL 22)
        self.assertEqual(
            22,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'mon',
                'disable_stretch_mode',
                'non_existent_rule',
                '--yes-i-really-mean-it'
            ))
        # Disable stretch mode with the current stretch rule (expect -EINVAL 22)
        self.assertEqual(
            22,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'mon',
                'disable_stretch_mode',
                self.STRETCH_CRUSH_RULE,
                '--yes-i-really-mean-it',

            ))
        # Disable stretch mode without crush rule (expect success 0)
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'mon',
                'disable_stretch_mode',
                '--yes-i-really-mean-it'
            ))
        # Check if stretch mode is disabled correctly
        self._stretch_mode_disabled_correctly()
        # all PGs are active + clean
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # write some data to the pool
        self._write_some_data(self.WRITE_PERIOD)
        # Enable stretch mode
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'mon',
                'enable_stretch_mode',
                self.TIEBREAKER_MON_NAME,
                self.STRETCH_CRUSH_RULE,
                self.STRETCH_BUCKET_TYPE
            ))
        self._stretch_mode_enabled_correctly()
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
        # should be in degraded stretch mode
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.is_degraded_stretch_mode(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # Disable stretch mode with valid crush rule (expect success 0)
        self.assertEqual(
            0,
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'mon',
                'disable_stretch_mode',
                self.DEFAULT_POOL_CRUSH_RULE,
                '--yes-i-really-mean-it'
            ))
        # Check if stretch mode is disabled correctly
        self._stretch_mode_disabled_correctly()
        # all PGs are active
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
