import json
import logging
import random
import threading
from tasks.mgr.mgr_test_case import MgrTestCase
from io import StringIO
from time import sleep

log = logging.getLogger(__name__)


class TestStretchCluster(MgrTestCase):
    """
    Test the stretch cluster feature.
    """
    # Define some constants
    POOL = 'pool_stretch'
    EC_POOL = 'ec_pool_stretch'
    CLUSTER = "ceph"
    WRITE_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 12
    SUCCESS_HOLD_TIME = 7
    # This dictionary maps the datacenter to the osd ids and hosts
    DC_OSDS = {
        'dc1': {
            "node-1": 0,
            "node-2": 1,
            "node-3": 2,
        },
        'dc2': {
            "node-4": 3,
            "node-5": 4,
            "node-6": 5,
        },
    }

    # This dictionary maps the datacenter to the mon ids and hosts
    DC_MONS = {
        'dc1': {
            "node-1": 'a',
            "node-2": 'b',
            "node-3": 'c',
        },
        'dc2': {
            "node-4": 'd',
            "node-5": 'e',
            "node-6": 'f',
        },
    }
    # in a third datacenter, placed by mon-stretch-ec-pool.sh
    TIEBREAKER_MON_NAME = 'g'
    PEERING_CRUSH_BUCKET_COUNT = 2
    PEERING_CRUSH_BUCKET_TARGET = 2
    STRETCH_EC_PROFILE = 'stretch_ec_profile'
    K = 2
    M = 1
    SIZE = PEERING_CRUSH_BUCKET_COUNT * (K + M)
    MIN_SIZE = K

    def setUp(self):
        """
        Setup the cluster and
        ensure we have a clean condition before the test.
        """
        # Ensure we have at least 6 OSDs
        super(TestStretchCluster, self).setUp()
        if self._osd_count() < 6:
            self.skipTest("Not enough OSDS!")

        # Remove any filesystems so that we can remove their pools
        if self.mds_cluster:
            self.mds_cluster.mds_stop()
            self.mds_cluster.mds_fail()
            self.mds_cluster.delete_all_filesystems()

        # Remove all other pools
        for pool in self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']:
            self.mgr_cluster.mon_manager.remove_pool(pool['pool_name'])

    def tearDown(self):
        """
        Clean up the cluster after the test.
        """
        # Remove the pool
        if self.POOL in self.mgr_cluster.mon_manager.pools:
            self.mgr_cluster.mon_manager.remove_pool(self.POOL)

        if self.EC_POOL in self.mgr_cluster.mon_manager.pools:
            self.mgr_cluster.mon_manager.remove_pool(self.EC_POOL)

        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for osd in osd_map['osds']:
            # mark all the osds in
            if osd['weight'] == 0.0:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', str(osd['osd']))
            # Bring back all the osds and move it back to the host.
            if osd['up'] == 0:
                self._bring_back_osd(osd['osd'])
                self._move_osd_back_to_host(osd['osd'])

        # Bring back all the MONS
        mons = self._get_all_mons_from_all_dc()
        for mon in mons:
            self._bring_back_mon(mon)
        super(TestStretchCluster, self).tearDown()

    def _setup_stretch_ec_pool(self, pool_name, min_size=None):
        """
        Create a k=K m=M pool over the two datacenters, with a rule named
        after it. A profile is deleted with its last pool, so set it again
        for each test.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'erasure-code-profile', 'set', self.STRETCH_EC_PROFILE,
            'k={}'.format(self.K), 'm={}'.format(self.M))
        self.mgr_cluster.mon_manager.create_pool(
            pool_name, min_size=min_size,
            erasure_code_profile_name=self.STRETCH_EC_PROFILE, num_zones=2)

    def _osd_count(self):
        """
        Get the number of OSDs in the cluster.
        """
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return len(osd_map['osds'])

    def _write_some_data(self, t, pool_name):
        """
        Write some data to the pool to simulate a workload.
        """

        args = [
            "rados", "-p", pool_name, "bench", str(t), "write", "-t", "16", "--no-cleanup"]

        self.mgr_cluster.admin_remote.run(args=args, wait=True)

    def _read_some_data(self, t, pool_name):
        """
        Write some data to the pool to simulate a workload.
        """

        args = [
            "rados", "-p", pool_name, "bench", str(t), "rand", "-t", "16"]

        self.mgr_cluster.admin_remote.run(args=args, wait=True)

    def _get_pg_stats(self):
        """
        Dump the cluster and get pg stats
        """
        out = self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'pg', 'dump', '--format=json')
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
            if pg['state'].count('active') and not pg['state'].count('stale'):
                num_active += 1
        return num_active

    def _get_active_clean_pg(self, pgs):
        """
        Get the number of active+clean PGs
        """
        num_active_clean = 0
        for pg in pgs:
            if (pg['state'].count('active') and
                pg['state'].count('clean') and
                    not pg['state'].count('stale')):
                num_active_clean += 1
        return num_active_clean

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

    def _pg_all_unavailable(self):
        """
        Check if all pgs are unavailable.
        """
        pgs = self._get_pg_stats()
        return self._get_active_pg(pgs) == 0

    def _pg_partial_active(self):
        """
        Check if some pgs are active.
        """
        pgs = self._get_pg_stats()
        return 0 < self._get_active_pg(pgs) <= len(pgs)

    def _kill_osd(self, osd):
        """
        Kill the osd.
        """
        try:
            self.ctx.daemons.get_daemon('osd', osd, self.CLUSTER).stop()
        except Exception:
            log.error("Failed to stop osd.{}".format(str(osd)))
            pass

    def _get_osds_by_dc(self, dc):
        """
        Get osds by datacenter.
        """
        return [osd for _, osd in self.DC_OSDS[dc].items()]

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

    def _get_host(self, osd):
        """
        Get the host of the osd.
        """
        for dc, nodes in self.DC_OSDS.items():
            for node, osd_id in nodes.items():
                if osd_id == osd:
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

    def _bring_back_one_osds_from_dc(self, dc):
        """
        Bring back one random OSD from the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_OSDS:
            raise ValueError("dc must be one of the following: %s" %
                             self.DC_OSDS.keys())
        log.debug("Bringing back one random OSD from %s", dc)
        # filter out failed osds
        osds_data = self._get_osds_data(self._get_osds_by_dc(dc))
        osds = [int(osd['osd']) for osd in osds_data if int(osd['up']) == 0]
        # fail over one random OSD in the DC
        osd_id = random.choice(osds)
        self._bring_back_osd(osd_id)
        # wait until the osd is down
        self.wait_until_true(
            lambda: int(self._get_osds_data([osd_id])[0]['up']) == 1,
            timeout=self.RECOVERY_PERIOD
        )

    def _bring_back_osd(self, osd):
        """
        Bring back the osd.
        """
        try:
            self.ctx.daemons.get_daemon('osd', osd, self.CLUSTER).restart()
        except Exception:
            log.error("Failed to bring back osd.{}".format(str(osd)))
            pass

    def _bring_back_all_osds_in_dc(self, dc):
        """
        Bring back all osds in the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_OSDS:
            raise ValueError("dc must be one of the following: %s" %
                             self.DC_OSDS.keys())
        log.debug("Bringing back %s", dc)
        osds = self._get_osds_by_dc(dc)
        # Bring back all the osds in the DC and move it back to the host.
        for osd_id in osds:
            # Bring back the osd
            self._bring_back_osd(osd_id)
            # Wait until the osd is up since we need it to be up before we can
            # move it back to the host
            self.wait_until_true(
                lambda: all([int(osd['up']) == 1
                            for osd in self._get_osds_data([osd_id])]),
                timeout=self.RECOVERY_PERIOD
            )
            # Move the osd back to the host
            self._move_osd_back_to_host(osd_id)

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
        log.debug("Failing over %s", dc)
        osds = self._get_osds_by_dc(dc)
        # fail over all the OSDs in the DC
        for osd_id in osds:
            self._kill_osd(osd_id)
        # wait until all the osds are down
        self.wait_until_true(
            lambda: all([int(osd['up']) == 0
                        for osd in self._get_osds_data(osds)]),
            timeout=self.RECOVERY_PERIOD
        )

    def _fail_over_one_osd_from_dc(self, dc):
        """
        Fail over one random OSD from the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_OSDS:
            raise ValueError("dc must be one of the following: %s" %
                             self.DC_OSDS.keys())
        log.debug("Failing over one random OSD from %s", dc)
        # filter out failed osds
        osds_data = self._get_osds_data(self._get_osds_by_dc(dc))
        osds = [int(osd['osd']) for osd in osds_data if int(osd['up']) == 1]
        # fail over one random OSD in the DC
        osd_id = random.choice(osds)
        self._kill_osd(osd_id)
        # wait until the osd is down
        self.wait_until_true(
            lambda: int(self._get_osds_data([osd_id])[0]['up']) == 0,
            timeout=self.RECOVERY_PERIOD
        )

    def _fail_over_all_mons_in_dc(self, dc):
        """
        Fail over all mons in the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_MONS:
            raise ValueError("dc must be one of the following: %s" %
                             ", ".join(self.DC_MONS.keys()))
        log.debug("Failing over %s", dc)
        mons = self._get_mons_by_dc(dc)
        for mon in mons:
            self._kill_mon(mon)
        # wait until all the mons are out of quorum
        self.wait_until_true(
            lambda: self._check_mons_out_of_quorum(mons),
            timeout=self.RECOVERY_PERIOD
        )

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
        return [mon for _, mon in self.DC_MONS[dc].items()]

    def _get_all_mons_from_all_dc(self):
        """
        Get all mons from all datacenters.
        """
        return [mon for nodes in self.DC_MONS.values()
                for mon in nodes.values()]

    def _check_mons_out_of_quorum(self, want_mons):
        """
        Check if the mons are not in quorum.
        """
        quorum_names = self.mgr_cluster.mon_manager.get_mon_quorum_names()
        return all([mon not in quorum_names for mon in want_mons])

    def _check_mons_in_quorum(self, want_mons):
        """
        Check if the mons are in quorum.
        """
        quorum_names = self.mgr_cluster.mon_manager.get_mon_quorum_names()
        return all([mon in quorum_names for mon in want_mons])

    def _bring_back_mon(self, mon):
        """
        Bring back the mon.
        """
        try:
            self.ctx.daemons.get_daemon('mon', mon, self.CLUSTER).restart()
        except Exception:
            log.error("Failed to bring back mon.{}".format(str(mon)))
            pass

    def _bring_back_all_mons_in_dc(self, dc):
        """
        Bring back all mons in the specified <datacenter>
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_MONS:
            raise ValueError("dc must be one of the following: %s" %
                             ", ".join(self.DC_MONS.keys()))
        log.debug("Bringing back %s", dc)
        mons = self._get_mons_by_dc(dc)
        for mon in mons:
            self._bring_back_mon(mon)
        # wait until all the mons are up
        self.wait_until_true(
            lambda: self._check_mons_in_quorum(mons),
            timeout=self.RECOVERY_PERIOD
        )

    # ------------------------------------------------------------------ #
    # Stretch mode state validators (OSDMap + MonMap)
    # ------------------------------------------------------------------ #

    def _get_stretch_mode_osdmap(self):
        """Return the stretch_mode sub-dict from the OSDMap."""
        return self.mgr_cluster.mon_manager.get_osd_dump_json().get(
            'stretch_mode', {})
    
    def _assert_recovering_stretch_mode(self, expected=True):
        """
        Assert that the OSDMap reflects (or does not reflect) recovering
        stretch mode.  recovering_stretch_mode == 1 means the cluster is
        healing after a site failure.
        """
        stretch = self._get_stretch_mode_osdmap()
        self.assertEqual(
            1 if expected else 0,
            stretch.get('recovering_stretch_mode', 0),
            "recovering_stretch_mode expected to be %d but got %d"
            % (1 if expected else 0,
               stretch.get('recovering_stretch_mode', 0))
        )
        log.debug("recovering_stretch_mode == %d (expected)", 1 if expected else 0)

    def _stretch_mode_enabled_correctly(self):
        """
        Evaluate whether the stretch EC pool stretch mode is enabled correctly
        by checking the OSDMap and MonMap.
        """
        # Checking the OSDMap
        osdmap = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for pool in osdmap['pools']:
            # expects crush_rule to be the rule generated for the pool
            self.assertEqual(
                self.mgr_cluster.mon_manager.get_crush_rule_id(
                    pool['pool_name']),
                pool['crush_rule']
            )
            # expects pool size to match SIZE (PEERING_CRUSH_BUCKET_COUNT * (K + M))
            self.assertEqual(
                self.SIZE,
                pool['size']
            )
            # expects pool min_size to match MIN_SIZE (K)
            self.assertEqual(
                self.MIN_SIZE,
                pool['min_size']
            )
            # expects pool is_stretch_pool flag to be true
            self.assertEqual(
                True,
                pool['is_stretch_pool']
            )
            # expects peering_crush_bucket_count = 2 (always this value for stretch mode)
            self.assertEqual(
                self.PEERING_CRUSH_BUCKET_COUNT,
                pool['peering_crush_bucket_count']
            )
            # expects peering_crush_bucket_target = 2 (always this value for stretch mode)
            self.assertEqual(
                self.PEERING_CRUSH_BUCKET_TARGET,
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
        # expects stretch_bucket_count to be 2
        self.assertEqual(
            self.PEERING_CRUSH_BUCKET_COUNT,
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

    def _wait_for_degraded_stretch_mode(self):
        """Wait until the cluster enters degraded stretch mode."""
        self.wait_until_true_and_hold(
            lambda: self.mgr_cluster.mon_manager.is_degraded_stretch_mode(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

    def _wait_for_recovering_stretch_mode(self):
        """Wait until the cluster enters recovering stretch mode."""
        def _is_recovering():
            stretch = self._get_stretch_mode_osdmap()
            return stretch.get('recovering_stretch_mode', 0) == 1
        self.wait_until_true_and_hold(
            _is_recovering,
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

    def _wait_for_normal_stretch_mode(self):
        """Wait until the cluster leaves degraded/recovering stretch mode."""
        def _is_normal():
            stretch = self._get_stretch_mode_osdmap()
            return (stretch.get('degraded_stretch_mode', 0) == 0 and
                    stretch.get('recovering_stretch_mode', 0) == 0)
        self.wait_until_true_and_hold(
            _is_normal,
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

    # ------------------------------------------------------------------ #
    # Data-integrity helpers
    # ------------------------------------------------------------------ #

    def _write_objects_for_validation(self, pool_name, obj_prefix, count=10):
        """
        Write <count> deterministic objects to <pool_name> using
        ``rados put``.  Each object's content is its own name so that we
        can verify it later without external state.

        Returns the list of object names written.
        """
        obj_names = ['%s_%d' % (obj_prefix, i) for i in range(count)]
        for obj in obj_names:
            self.mgr_cluster.mon_manager.do_rados(
                ['put', obj, '-'],
                pool=pool_name,
                stdin=StringIO(obj),
            )
            log.debug("Wrote object %s to pool %s", obj, pool_name)
        return obj_names

    def _validate_objects(self, pool_name, obj_names):
        """
        Read back every object in <obj_names> from <pool_name> and verify
        that the content matches the object name (as written by
        ``_write_objects_for_validation``).  Asserts on any mismatch or
        missing object.
        """
        for obj in obj_names:
            stdout = StringIO()
            ret = self.mgr_cluster.mon_manager.do_rados(
                ['get', obj, '-'],
                pool=pool_name,
                stdout=stdout,
                check_status=False,
            ).exitstatus
            self.assertEqual(
                0, ret,
                "rados get of object '%s' failed (exit %d)" % (obj, ret)
            )
            got = stdout.getvalue().strip()
            self.assertEqual(
                obj, got,
                "Data mismatch for object '%s': expected '%s', got '%s'"
                % (obj, obj, got)
            )
            log.debug("Validated object %s in pool %s", obj, pool_name)

    # ------------------------------------------------------------------ #
    # Parallel-workload helper
    # ------------------------------------------------------------------ #

    def _run_parallel_workloads(self, pool_name, duration):
        """
        Run write and read bench workloads in parallel threads and join them.
        Any exception raised inside a thread is re-raised in the caller.
        """
        errors = []

        def _write():
            try:
                self._write_some_data(duration, pool_name)
            except Exception as e:
                errors.append(('write', e))

        def _read():
            try:
                self._read_some_data(duration, pool_name)
            except Exception as e:
                errors.append(('read', e))

        tw = threading.Thread(target=_write)
        tr = threading.Thread(target=_read)
        tw.start()
        tr.start()
        tw.join()
        tr.join()

        if errors:
            raise AssertionError(
                "Parallel workload failures: %s" % errors)

    def test_stretch_ec_pool_no_active_pgs_2_sites(self):
        """
        Test setting a pool to stretch cluster and checks whether
        it prevents PGs from the going active when there is not
        enough buckets available in the acting set of PGs to
        go active.
        """
        self._setup_stretch_ec_pool(self.EC_POOL, min_size=self.K)
        self._write_some_data(self.WRITE_PERIOD, self.EC_POOL)

        # Wait for PGs to remap to the new stretch CRUSH rule
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # Fail over osds in DC1 expects PGs to be 100% active
        self._fail_over_all_osds_in_dc('dc1')
        self._fail_over_all_mons_in_dc('dc1')
        self.wait_until_true_and_hold(lambda: self._pg_all_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # Fail over 1 random OSD from DC2 expects PGs to be 100% active
        self._fail_over_one_osd_from_dc('dc2')
        self.wait_until_true_and_hold(lambda: self._pg_all_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # Fail over osds in DC2 completely expects PGs to be 100% inactive
        self._fail_over_all_osds_in_dc('dc2')
        self.wait_until_true_and_hold(lambda: self._pg_all_unavailable(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # Bring back osds in DC1 expects PGs to be 100% active
        self._bring_back_all_mons_in_dc('dc1')
        self._bring_back_all_osds_in_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME)

        # Bring back osds in DC2 expects PGs to be 100% active+clean
        self._bring_back_all_osds_in_dc('dc2')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

    def test_stretch_ec_pool_io_when_down(self):
        """
        Test setting a pool to stretch cluster and checks whether
        it prevents PGs from the going active when:
            1. write I/O occurs when a datacenter 1 is down
            2. Datacenter 1 is brought up but OSDs in datacenter 2 are
               brought down so there are less than min_size OSDs.
        """
        self._setup_stretch_ec_pool(self.EC_POOL, min_size=self.K)
        self._write_some_data(self.WRITE_PERIOD, self.EC_POOL)
        # 1. We test the case where we didn't make the pool stretch
        #   and we expect the PGs to go active even when there is only
        #   one bucket available in the acting set of PGs.

        # Wait for PGs to remap to the new stretch CRUSH rule
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # Fail DC1 and enter degraded mode Expect to be 100% active
        self._fail_over_all_osds_in_dc('dc1')
        self._fail_over_all_mons_in_dc('dc1')
        self.wait_until_true_and_hold(lambda: self._pg_all_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # Do some write I/O while DC1 is down - DC2 receives the new data
        self._write_some_data(self.WRITE_PERIOD, self.EC_POOL)

        # PGs should still be active since DC2 is up and has the latest data
        self.wait_until_true_and_hold(lambda: self._pg_all_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        self._read_some_data(self.WRITE_PERIOD, self.EC_POOL)

        # Fail over one OSD in DC2. Should all be active since above min size
        self._fail_over_one_osd_from_dc(dc='dc2')
        self.wait_until_true_and_hold(lambda: self._pg_all_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        self._read_some_data(self.WRITE_PERIOD, self.EC_POOL)

    
        # Fail over one OSD in DC2. Should all be partially inactive since below min size
        # There is a case where one OSD holds multiple shards, so some PGs still have acting
        # above min size
        self._fail_over_one_osd_from_dc(dc='dc2')
        self.wait_until_true_and_hold(lambda: self._pg_partial_active(),
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # Bring back DC1. Expect PGs to be 100% inactive since we have stale data and below min size
        self._bring_back_all_mons_in_dc('dc1')
        self._bring_back_all_osds_in_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_unavailable(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME)

        self._bring_back_one_osds_from_dc('dc2')
        # Bring back one osd in DC2 and expect PGs to be 100% active since above min size
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME)
        
        self._read_some_data(self.WRITE_PERIOD, self.EC_POOL)
        self._write_some_data(self.WRITE_PERIOD, self.EC_POOL)

        # Let the degraded-site writes settle (PGs stay active) before
        # restoring the rest of DC2.
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        sleep(30)
 
        # Bring back osds in DC2 expects PGs to be 100% active+clean
        self._bring_back_all_osds_in_dc('dc2')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=5*self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

    def test_stretch_ec_degraded_and_recovery_mode(self):
        """
        Verify that the OSDMap and MonMap correctly reflect degraded stretch
        mode when a datacenter fails, and then recovering stretch mode as the
        cluster heals after the site is restored.

        Steps:
          1. Set up the EC pool in stretch mode.
          2. Fail all OSDs + MONs in DC1 → cluster should enter degraded
             stretch mode (degraded_stretch_mode == 1).
          3. Bring DC1 back → cluster should transition through recovering
             stretch mode (recovering_stretch_mode == 1) and then return to
             normal (both flags == 0) once PGs are active+clean.
        """
        self._setup_stretch_ec_pool(self.EC_POOL, min_size=self.K)

        self._write_some_data(self.WRITE_PERIOD, self.EC_POOL)

        # Baseline: no degraded / recovering flags set
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )

        self._stretch_mode_enabled_correctly()

        # --- Bring DC1 down → expect degraded stretch mode ---
        log.debug("Failing over all OSDs and MONs in dc1")
        self._fail_over_all_osds_in_dc('dc1')
        self._fail_over_all_mons_in_dc('dc1')

        # PGs should stay active on DC2 alone
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )
        
        # OSDMap must reflect degraded (still-enabled) stretch mode, i.e.
        # degraded_stretch_mode == 1 -- not stretch mode fully disabled.
        self._wait_for_degraded_stretch_mode()
        self._assert_recovering_stretch_mode(expected=False)

        # I/O should continue on the surviving site
        self._write_some_data(self.WRITE_PERIOD, self.EC_POOL)
        self._read_some_data(self.WRITE_PERIOD, self.EC_POOL)

        # --- Restore DC1 → expect recovering then normal stretch mode ---
        log.debug("Restoring dc1 OSDs and MONs")
        self._bring_back_all_mons_in_dc('dc1')
        self._bring_back_all_osds_in_dc('dc1')

        # The cluster should move through recovering stretch mode …
        self._wait_for_recovering_stretch_mode()
        self._assert_recovering_stretch_mode(expected=True)

        # settle back to fully healthy
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=5 * self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )
        self._wait_for_normal_stretch_mode()
        self._stretch_mode_enabled_correctly()

    def test_stretch_ec_parallel_workloads_with_site_failure(self):
        """
        Run parallel write and read workloads while inducing a site failure
        and then recovering, verifying the cluster remains available on the
        surviving site throughout.

        Steps:
          1. Set up the EC pool in stretch mode, seed initial data.
          2. Start parallel read+write bench workloads.
          3. Mid-flight: fail all OSDs in DC1 (network-partition simulation).
          4. Wait for degraded stretch mode.
          5. Join the workload threads (reads/writes on DC2 must succeed).
          6. Restore DC1, verify full recovery.
        """
        self._setup_stretch_ec_pool(self.EC_POOL, min_size=self.K)

        self._write_some_data(self.WRITE_PERIOD, self.EC_POOL)

        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )

        # Launch parallel workloads in the background while we kill DC1
        errors = []

        def _background_io():
            try:
                # Long enough that I/O is still in flight while the site kill
                # sequence (OSDs + MONs) completes.
                self._run_parallel_workloads(self.EC_POOL, self.RECOVERY_PERIOD)
            except Exception as e:
                errors.append(e)

        io_thread = threading.Thread(target=_background_io)
        io_thread.start()

        # Give the workload a moment to get going, then kill DC1
        sleep(self.WRITE_PERIOD)
        log.debug("Failing over all OSDs in dc1 during active I/O")
        self._fail_over_all_osds_in_dc('dc1')
        self._fail_over_all_mons_in_dc('dc1')

        # PGs should stay active on DC2
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )
        self._wait_for_degraded_stretch_mode()

        # Wait for the background I/O to finish (it targets DC2 which is up)
        io_thread.join(timeout=self.RECOVERY_PERIOD)
        self.assertFalse(
            io_thread.is_alive(),
            "Background I/O thread did not finish within RECOVERY_PERIOD")
        if errors:
            raise AssertionError(
                "Parallel workload failed during site failure: %s" % errors)

        # Restore DC1 and verify full recovery
        self._bring_back_all_mons_in_dc('dc1')
        self._bring_back_all_osds_in_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=5 * self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )
        self._wait_for_normal_stretch_mode()

    def test_stretch_ec_data_integrity(self):
        """
        Write a set of named objects before and during a site failure then
        validate that every object can be read back with correct content
        after full recovery.

        Steps:
          1. Set up the EC pool in stretch mode.
          2. Write "pre-failure" objects and validate them immediately.
          3. Fail DC1; write "during-failure" objects on DC2.
          4. Restore DC1; wait for full recovery.
          5. Validate ALL objects (pre-failure + during-failure) are intact.
        """
        self._setup_stretch_ec_pool(self.EC_POOL, min_size=self.K)

        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )

        # Write objects while both sites are healthy
        pre_failure_objs = self._write_objects_for_validation(
            self.EC_POOL, 'pre_fail', count=10)
        self._validate_objects(self.EC_POOL, pre_failure_objs)
        log.debug("Pre-failure objects validated OK")

        # Fail DC1; write additional objects on the surviving DC2
        self._fail_over_all_osds_in_dc('dc1')
        self._fail_over_all_mons_in_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )
        self._wait_for_degraded_stretch_mode()

        # Pre-failure objects must still be readable from DC2 alone via EC recovery
        self._validate_objects(self.EC_POOL, pre_failure_objs)
        log.debug("Pre-failure objects validated OK in degraded mode")

        during_failure_objs = self._write_objects_for_validation(
            self.EC_POOL, 'during_fail', count=10)
        # Validate both sets are readable on the surviving site
        all_objs = pre_failure_objs + during_failure_objs
        self._validate_objects(self.EC_POOL, all_objs)
        log.debug("All objects validated OK in degraded mode on surviving site")

        # Restore DC1 and wait for full recovery
        self._bring_back_all_mons_in_dc('dc1')
        self._bring_back_all_osds_in_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=5 * self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME,
        )
        self._wait_for_normal_stretch_mode()

        # Validate ALL objects — both pre- and during-failure — after recovery
        self._validate_objects(self.EC_POOL, all_objs)
        log.debug("All %d objects validated OK after full recovery", len(all_objs))
