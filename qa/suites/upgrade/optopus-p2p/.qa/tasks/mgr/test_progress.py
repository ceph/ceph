
import json
import logging
import time
from unittest import SkipTest

from mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestProgress(MgrTestCase):
    POOL = "progress_data"

    # How long we expect to wait at most between taking an OSD out
    # and seeing the progress event pop up.
    EVENT_CREATION_PERIOD = 5

    WRITE_PERIOD = 30

    # Generous period for OSD recovery, should be same order of magnitude
    # to how long it took to write the data to begin with
    RECOVERY_PERIOD = WRITE_PERIOD * 4

    def _get_progress(self):
        out = self.mgr_cluster.mon_manager.raw_cluster_cmd("progress", "json")
        return json.loads(out)

    def _all_events(self):
        """
        To avoid racing on completion, we almost always want to look
        for events in the total list of active and complete, so
        munge them into a single list.
        """
        p = self._get_progress()
        log.info(json.dumps(p, indent=2))
        return p['events'] + p['completed']

    def _setup_pool(self, size=None):
        self.mgr_cluster.mon_manager.create_pool(self.POOL)
        if size is not None:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'pool', 'set', self.POOL, 'size', str(size))

    def _write_some_data(self, t):
        """
        To adapt to test systems of varying performance, we write
        data for a defined time period, rather than to a defined
        capacity.  This will hopefully result in a similar timescale
        for PG recovery after an OSD failure.
        """

        args = [
            "rados", "-p", self.POOL, "bench", str(t), "write", "-t", "16"]

        self.mgr_cluster.admin_remote.run(args=args, wait=True)

    def _osd_count(self):
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return len(osd_map['osds'])

    def setUp(self):
        # Ensure we have at least four OSDs
        if self._osd_count() < 4:
            raise SkipTest("Not enough OSDS!")

        # Remove any filesystems so that we can remove their pools
        if self.mds_cluster:
            self.mds_cluster.mds_stop()
            self.mds_cluster.mds_fail()
            self.mds_cluster.delete_all_filesystems()

        # Remove all other pools
        for pool in self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']:
            self.mgr_cluster.mon_manager.remove_pool(pool['pool_name'])

        self._load_module("progress")
        self.mgr_cluster.mon_manager.raw_cluster_cmd('progress', 'clear')

    def _simulate_failure(self, osd_ids=None):
        """
        Common lead-in to several tests: get some data in the cluster,
        then mark an OSD out to trigger the start of a progress event.

        Return the JSON representation of the failure event.
        """

        if osd_ids is None:
            osd_ids = [0]

        self._setup_pool()
        self._write_some_data(self.WRITE_PERIOD)

        for osd_id in osd_ids:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'out', str(osd_id))

        # Wait for a progress event to pop up
        self.wait_until_equal(lambda: len(self._all_events()), 1,
                              timeout=self.EVENT_CREATION_PERIOD)
        ev = self._all_events()[0]
        log.info(json.dumps(ev, indent=1))
        self.assertIn("Rebalancing after osd.0 marked out", ev['message'])

        return ev

    def _is_quiet(self):
        """
        Whether any progress events are live.
        """
        return len(self._get_progress()['events']) == 0

    def _is_complete(self, ev_id):
        progress = self._get_progress()
        live_ids = [ev['id'] for ev in progress['events']]
        complete_ids = [ev['id'] for ev in progress['completed']]
        if ev_id in complete_ids:
            assert ev_id not in live_ids
            return True
        else:
            assert ev_id in live_ids
            return False

    def tearDown(self):
        if self.POOL in self.mgr_cluster.mon_manager.pools:
            self.mgr_cluster.mon_manager.remove_pool(self.POOL)

        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for osd in osd_map['osds']:
            if osd['weight'] == 0.0:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', str(osd['osd']))

        super(TestProgress, self).tearDown()

    def test_osd_healthy_recovery(self):
        """
        The simple recovery case: an OSD goes down, its PGs get a new
        placement, and we wait for the PG to get healthy in its new
        locations.
        """
        ev = self._simulate_failure()

        # Wait for progress event to ultimately reach completion
        self.wait_until_true(lambda: self._is_complete(ev['id']),
                             timeout=self.RECOVERY_PERIOD)
        self.assertTrue(self._is_quiet())

    def test_pool_removal(self):
        """
        That a pool removed during OSD recovery causes the
        progress event to be correctly marked complete once there
        is no more data to move.
        """
        ev = self._simulate_failure()

        self.mgr_cluster.mon_manager.remove_pool(self.POOL)

        # Event should complete promptly
        self.wait_until_true(lambda: self._is_complete(ev['id']),
                             timeout=self.EVENT_CREATION_PERIOD)
        self.assertTrue(self._is_quiet())

    def test_osd_came_back(self):
        """
        When a recovery is underway, but then the out OSD
        comes back in, such that recovery is no longer necessary.
        """
        ev = self._simulate_failure()

        self.mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'in', '0')

        # Event should complete promptly
        self.wait_until_true(lambda: self._is_complete(ev['id']),
                             timeout=self.EVENT_CREATION_PERIOD)
        self.assertTrue(self._is_quiet())

    def test_osd_cannot_recover(self):
        """
        When the cluster cannot recover from a lost OSD, e.g.
        because there is no suitable new placement for it.
        (a size=3 pool when there are only 2 OSDs left)
        (a size=3 pool when the remaining osds are only on 2 hosts)

        Progress event should not be created.
        """

        pool_size = 3

        self._setup_pool(size=pool_size)
        self._write_some_data(self.WRITE_PERIOD)

        # Fail enough OSDs so there are less than N_replicas OSDs
        # available.
        osd_count = self._osd_count()

        # First do some failures that will result in a normal rebalance
        # (Assumption: we're in a test environment that is configured
        #  not to require replicas be on different hosts, like teuthology)
        for osd_id in range(0, osd_count - pool_size):
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'out', str(osd_id))

        # We should see an event for each of the OSDs we took out
        self.wait_until_equal(
            lambda: len(self._all_events()),
            osd_count - pool_size,
            timeout=self.EVENT_CREATION_PERIOD)

        # Those should complete cleanly
        self.wait_until_true(
            lambda: self._is_quiet(),
            timeout=self.RECOVERY_PERIOD
        )

        # Fail one last OSD, at the point the PGs have nowhere to go
        victim_osd = osd_count - pool_size
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'out', str(victim_osd))

        # Check that no event is created
        time.sleep(self.EVENT_CREATION_PERIOD)

        self.assertEqual(len(self._all_events()), osd_count - pool_size)
