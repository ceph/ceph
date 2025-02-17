import json
import logging
import random
from tasks.mgr.mgr_test_case import MgrTestCase
from time import sleep

log = logging.getLogger(__name__)


class TestStretchCluster(MgrTestCase):
    """
    Test the stretch cluster feature.
    """
    # Define some constants
    POOL = 'pool_stretch'
    CLUSTER = "ceph"
    WRITE_PERIOD = 10
    RECOVERY_PERIOD = WRITE_PERIOD * 6
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
        'dc3': {
            "node-7": 6,
            "node-8": 7,
            "node-9": 8,
        }
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
        'dc3': {
            "node-7": 'g',
            "node-8": 'h',
            "node-9": 'i',
        }
    }
    PEERING_CRUSH_BUCKET_COUNT = 2
    PEERING_CRUSH_BUCKET_TARGET = 3
    PEERING_CRUSH_BUCKET_BARRIER = 'datacenter'
    CRUSH_RULE = 'replicated_rule_custom'
    DEFAULT_CRUSH_RULE = 'replicated_rule'
    SIZE = 6
    MIN_SIZE = 3
    BUCKET_MAX = SIZE // PEERING_CRUSH_BUCKET_TARGET
    if (BUCKET_MAX * PEERING_CRUSH_BUCKET_TARGET) < SIZE:
        BUCKET_MAX += 1

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

    def _setup_pool(self, size=None, min_size=None, rule=None):
        """
        Create a pool and set its size.
        """
        self.mgr_cluster.mon_manager.create_pool(self.POOL, min_size=min_size)
        if size is not None:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'pool', 'set', self.POOL, 'size', str(size))
        if rule is not None:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'pool', 'set', self.POOL, 'crush_rule', rule)

    def _osd_count(self):
        """
        Get the number of OSDs in the cluster.
        """
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return len(osd_map['osds'])

    def _write_some_data(self, t):
        """
        Write some data to the pool to simulate a workload.
        """

        args = [
            "rados", "-p", self.POOL, "bench", str(t), "write", "-t", "16"]

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

    def _get_acting_set(self, pgs):
        """
        Get the acting set of the PGs
        """
        acting_set = []
        for pg in pgs:
            acting_set.append(pg['acting'])
        return acting_set

    def _surviving_osds_in_acting_set_dont_exceed(self, n, osds):
        """
        Check if the acting set of the PGs doesn't contain more
        than n OSDs of the surviving DC.
        NOTE: Only call this function after we set the pool to stretch.
        """
        pgs = self._get_pg_stats()
        acting_set = self._get_acting_set(pgs)
        for acting in acting_set:
            log.debug("Acting set: %s", acting)
            intersect = list(set(acting) & set(osds))
            if len(intersect) > n:
                log.error(
                    "Acting set: %s contains more than %d \
                    OSDs from the same %s which are: %s",
                    acting, n, self.PEERING_CRUSH_BUCKET_BARRIER,
                    intersect
                )
                return False
        return True

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

    def _get_all_osds_from_all_dc(self):
        """
        Get all osds from all datacenters.
        """
        return [osd for nodes in self.DC_OSDS.values()
                for osd in nodes.values()]

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

    def _fail_over_one_mon_from_dc(self, dc, no_wait=False):
        """
        Fail over one random mon from the specified <datacenter>
        no_wait: if True, don't wait for the mon to be out of quorum
        """
        if not isinstance(dc, str):
            raise ValueError("dc must be a string")
        if dc not in self.DC_MONS:
            raise ValueError("dc must be one of the following: %s" %
                             ", ".join(self.DC_MONS.keys()))
        log.debug("Failing over one random mon from %s", dc)
        mons = self._get_mons_by_dc(dc)
        # filter out failed mons
        mon_quorum = self.mgr_cluster.mon_manager.get_mon_quorum_names()
        mons = [mon for mon in mons if mon in mon_quorum]
        # fail over one random mon in the DC
        mon = random.choice(mons)
        self._kill_mon(mon)
        if no_wait:
            return
        else:
            # wait until the mon is out of quorum
            self.wait_until_true(
                lambda: self._check_mons_out_of_quorum([mon]),
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

    def _check_mon_quorum_size(self, size):
        """
        Check if the mon quorum size is equal to <size>
        """
        return len(self.mgr_cluster.mon_manager.get_mon_quorum_names()) == size

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

    def _no_reply_to_mon_command(self):
        """
        Check if the cluster is inaccessible.
        """
        try:
            self.mgr_cluster.mon_manager.raw_cluster_cmd('status')
            return False
        except Exception:
            return True

    def test_mon_failures_in_stretch_pool(self):
        """
        Test mon failures in stretch pool.
        """
        self._setup_pool(
            self.SIZE,
            min_size=self.MIN_SIZE,
            rule=self.CRUSH_RULE
        )
        self._write_some_data(self.WRITE_PERIOD)
        # Set the pool to stretch
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'stretch', 'set',
            self.POOL, str(self.PEERING_CRUSH_BUCKET_COUNT),
            str(self.PEERING_CRUSH_BUCKET_TARGET),
            self.PEERING_CRUSH_BUCKET_BARRIER,
            self.CRUSH_RULE, str(self.SIZE), str(self.MIN_SIZE))

        # SCENARIO 1: MONS in DC1 down

        # Fail over mons in DC1
        self._fail_over_all_mons_in_dc('dc1')
        # Expects mons in DC2 and DC3 to be in quorum
        mons_dc2_dc3 = (
            self._get_mons_by_dc('dc2') +
            self._get_mons_by_dc('dc3')
        )
        self.wait_until_true_and_hold(
            lambda: self._check_mons_in_quorum(mons_dc2_dc3),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # SCENARIO 2: MONS in DC1 down + 1 MON in DC2 down

        # Fail over 1 random MON from DC2
        self._fail_over_one_mon_from_dc('dc2')
        # Expects quorum size to be 5
        self.wait_until_true_and_hold(
            lambda: self._check_mon_quorum_size(5),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # SCENARIO 3: MONS in DC1 down + 2 MONS in DC2 down

        # Fail over 1 random MON from DC2
        self._fail_over_one_mon_from_dc('dc2', no_wait=True)
        # sleep for 30 seconds to allow the mon to be out of quorum
        sleep(30)
        # Expects cluster to be inaccesible
        self.wait_until_true(
            lambda: self._no_reply_to_mon_command(),
            timeout=self.RECOVERY_PERIOD,
        )
        # Bring back all mons in DC2 to unblock the cluster
        self._bring_back_all_mons_in_dc('dc2')
        # Expects mons in DC2 and DC3 to be in quorum
        self.wait_until_true_and_hold(
            lambda: self._check_mons_in_quorum(mons_dc2_dc3),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # Unset the pool back to replicated rule expects PGs to be 100% active+clean
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'stretch', 'unset',
            self.POOL, self.DEFAULT_CRUSH_RULE,
            str(self.SIZE), str(self.MIN_SIZE))
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

    def test_set_stretch_pool_no_active_pgs(self):
        """
        Test setting a pool to stretch cluster and checks whether
        it prevents PGs from the going active when there is not
        enough buckets available in the acting set of PGs to
        go active.
        """
        self._setup_pool(
            self.SIZE,
            min_size=self.MIN_SIZE,
            rule=self.CRUSH_RULE
        )
        self._write_some_data(self.WRITE_PERIOD)
        # 1. We test the case where we didn't make the pool stretch
        #   and we expect the PGs to go active even when there is only
        #   one bucket available in the acting set of PGs.

        # Fail over osds in DC1 expects PGs to be 100% active
        self._fail_over_all_osds_in_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # Fail over osds in DC2 expects PGs to be partially active
        self._fail_over_all_osds_in_dc('dc2')
        self.wait_until_true_and_hold(
            lambda: self._pg_partial_active,
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )

        # Bring back osds in DC1 expects PGs to be 100% active
        self._bring_back_all_osds_in_dc('dc1')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # Bring back osds DC2 expects PGs to be 100% active+clean
        self._bring_back_all_osds_in_dc('dc2')
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )
        # 2. We test the case where we make the pool stretch
        #   and we expect the PGs to not go active even when there is only
        #   one bucket available in the acting set of PGs.

        # Set the pool to stretch
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'stretch', 'set',
            self.POOL, str(self.PEERING_CRUSH_BUCKET_COUNT),
            str(self.PEERING_CRUSH_BUCKET_TARGET),
            self.PEERING_CRUSH_BUCKET_BARRIER,
            self.CRUSH_RULE, str(self.SIZE), str(self.MIN_SIZE))

        # Fail over osds in DC1 expects PGs to be 100% active
        self._fail_over_all_osds_in_dc('dc1')
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
        self.wait_until_true_and_hold(lambda: self._pg_all_unavailable,
                                      timeout=self.RECOVERY_PERIOD,
                                      success_hold_time=self.SUCCESS_HOLD_TIME)

        # We expect that there will be no more than BUCKET_MAX osds from DC3
        # in the acting set of the PGs.
        self.wait_until_true(
            lambda: self._surviving_osds_in_acting_set_dont_exceed(
                        self.BUCKET_MAX,
                        self._get_osds_by_dc('dc3')
                    ),
            timeout=self.RECOVERY_PERIOD)

        # Bring back osds in DC1 expects PGs to be 100% active
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
        # Unset the pool back to replicated rule expects PGs to be 100% active+clean
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'stretch', 'unset',
            self.POOL, self.DEFAULT_CRUSH_RULE,
            str(self.SIZE), str(self.MIN_SIZE))
        self.wait_until_true_and_hold(
            lambda: self._pg_all_active_clean(),
            timeout=self.RECOVERY_PERIOD,
            success_hold_time=self.SUCCESS_HOLD_TIME
        )