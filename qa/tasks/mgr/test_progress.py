
import json
import logging
import time
from .mgr_test_case import MgrTestCase
from contextlib import contextmanager

log = logging.getLogger(__name__)


class TestProgress(MgrTestCase):
    POOL = "progress_data"

    # How long we expect to wait at most between taking an OSD out
    # and seeing the progress event pop up.
    EVENT_CREATION_PERIOD = 60

    # We will set this in setUp once we have self.mgr_cluster available
    PG_AUTOSCALER_EVENT_CREATION_PERIOD = None
    # This is how long we expect to wait at most when a small pool scales up.
    PG_SCALE_UP_PERIOD = None

    WRITE_PERIOD = 30

    # Generous period for OSD recovery, should be same order of magnitude
    # to how long it took to write the data to begin with
    RECOVERY_PERIOD = WRITE_PERIOD * 4

    def _get_progress(self):
        """
        Get the current progress information from the cluster.
        """
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

    def _events_in_progress(self):
        """
        this function returns all events that are in progress
        """
        p = self._get_progress()
        log.info(json.dumps(p, indent=2))
        return p['events']

    def _completed_events(self):
        """
        This function returns all events that are completed
        """
        p = self._get_progress()
        log.info(json.dumps(p, indent=2))
        return p['completed']

    def is_osd_marked_out(self, ev):
        """
        Check if the event indicates an OSD has been marked out.
        """
        return ev['message'].endswith('marked out')

    def is_osd_marked_in(self, ev):
        """
        Check if the event indicates an OSD has been marked in.
        """
        return ev['message'].endswith('marked in')

    def _get_osd_in_out_events(self, marked='both'):
        """
        Return the event that deals with OSDs being
        marked in, out or both
        """

        marked_in_events = []
        marked_out_events = []

        events_in_progress = self._events_in_progress()
        for ev in events_in_progress:
            if self.is_osd_marked_out(ev):
                marked_out_events.append(ev)
            elif self.is_osd_marked_in(ev):
                marked_in_events.append(ev)

        if marked == 'both':
            return [marked_in_events] + [marked_out_events]
        elif marked == 'in':
            return marked_in_events
        else:
            return marked_out_events

    def _osd_in_out_events_count(self, marked='both'):
        """
        Count the number of on going recovery events that deals with
        OSDs being marked in, out or both.
        """
        events_in_progress = self._events_in_progress()
        marked_in_count = 0
        marked_out_count = 0

        for ev in events_in_progress:
            if self.is_osd_marked_out(ev):
                marked_out_count += 1
            elif self.is_osd_marked_in(ev):
                marked_in_count += 1

        if marked == 'both':
            return marked_in_count + marked_out_count
        elif marked == 'in':
            return marked_in_count
        else:
            return marked_out_count

    def _setup_pool(self, **kwargs):
        """
        Setup a pool with optional additional pool settings.
        Args:
            **kwargs: Additional pool settings like bulk='true', pg_autoscale_bias=4, etc.
                    These will be passed as 'osd pool set' commands
        Examples:
            self._setup_pool()  # Basic pool
            self._setup_pool(size=3)  # Pool with size 3
            self._setup_pool(bulk='true', pg_autoscale_bias=4)  # Pool with bulk=true and pg_autoscale_bias=4
            self._setup_pool(size=2, bulk='true', pg_autoscale_bias=4, min_size=1)  # Combined
        """
        self.mgr_cluster.mon_manager.create_pool(self.POOL)
        # Apply additional pool settings
        for setting, value in kwargs.items():
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                'osd', 'pool', 'set', self.POOL, setting, str(value))

    def _osd_in_out_completed_events_count(self, marked='both'):
        """
        Count the number of completed recovery events that deals with
        OSDs being marked in, out, or both.
        """

        completed_events = self._completed_events()
        marked_in_count = 0
        marked_out_count = 0

        for ev in completed_events:
            if self.is_osd_marked_out(ev):
                marked_out_count += 1
            elif self.is_osd_marked_in(ev):
                marked_in_count += 1

        if marked == 'both':
            return marked_in_count + marked_out_count
        elif marked == 'in':
            return marked_in_count
        else:
            return marked_out_count

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

    @contextmanager    
    def recovery_backfill_disabled(self):
        """
        Context manager to disable recovery and backfill temporarily.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'set', 'nobackfill')
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'set', 'norecover')
        yield
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'unset', 'nobackfill')
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'unset', 'norecover')

    def setUp(self):
        """
         Set up the test environment every time before a test is run.
        """
        super(TestProgress, self).setUp()
        # Speed up the test by reducing the sleep interval of the pg_autoscaler
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "config", "set", "mgr", "mgr/pg_autoscaler/sleep_interval", "5"
        )
        # Set PG_AUTOSCALER_EVENT_CREATION_PERIOD now that self.mgr_cluster is available
        if self.PG_AUTOSCALER_EVENT_CREATION_PERIOD is None:
            self.PG_AUTOSCALER_EVENT_CREATION_PERIOD = self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "config", "get", "mgr", "mgr/pg_autoscaler/sleep_interval"
            ) * 3
        # Ensure we have at least four OSDs
        if self._osd_count() < 4:
            self.skipTest("Not enough OSDS!")

        # Remove any filesystems so that we can remove their pools
        if self.mds_cluster:
            self.mds_cluster.mds_stop()
            self.mds_cluster.mds_fail()
            self.mds_cluster.delete_all_filesystems()

        # Remove all other pools
        for pool in self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']:
            # There might be some pools that wasn't created with this test.
            # So we would use a raw cluster command to remove them.
            pool_name = pool['pool_name']
            if pool_name in self.mgr_cluster.mon_manager.pools:
                self.mgr_cluster.mon_manager.remove_pool(pool_name)
            else:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'pool', 'rm', pool_name, pool_name,
                    "--yes-i-really-really-mean-it")

        self._load_module("progress")
        # Clear any existing progress events
        self.mgr_cluster.mon_manager.raw_cluster_cmd('progress', 'clear')
        # Turn autoscale off globally, since we don't want
        # any pg-autoscale progress events from being triggered by pool creation.
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'set', 'noautoscale')

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
        with self.recovery_backfill_disabled():
            for osd_id in osd_ids:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'out', str(osd_id))

            # Wait for a progress event to pop up
            self.wait_until_equal(lambda: self._osd_in_out_events_count('out'), 1,
                                  timeout=self.EVENT_CREATION_PERIOD,
                                  period=1)

        ev = self._get_osd_in_out_events('out')[0]
        log.info(json.dumps(ev, indent=1))
        self.assertIn("Rebalancing after osd.0 marked out", ev['message'])
        return ev

    def _simulate_back_in(self, osd_ids, initial_event):
        """
        Simulate an OSD coming back in, assuming that
        recovery is still ongoing from a previous out event.
        """
        for osd_id in osd_ids:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', str(osd_id))

        # First Event should complete promptly
        self.wait_until_true(lambda: self._is_complete(initial_event['id']),
                             timeout=self.RECOVERY_PERIOD)

        with self.recovery_backfill_disabled():

            try:
                # Wait for progress event marked in to pop up
                self.wait_until_equal(lambda: self._osd_in_out_events_count('in'), 1,
                                      timeout=self.EVENT_CREATION_PERIOD,
                                      period=1)
            except RuntimeError as ex:
                if not "Timed out after" in str(ex):
                    raise ex

                log.info("There was no PGs affected by osd being marked in")
                return None

            new_event = self._get_osd_in_out_events('in')[0]
        return new_event

    def _no_events_anywhere(self):
        """
        Check whether there are any live or completed events.
        Returns True if there are no events anywhere.
        """
        p = self._get_progress()
        total_events = len(p['events']) + len(p['completed'])
        return total_events == 0

    def _pg_autoscaler_events_count(self):
        """
        Count the number of on going recovery events that deals with
        PG autoscaler.
        """
        events_in_progress = self._events_in_progress()
        pg_autoscaler_count = 0

        for ev in events_in_progress:
            if ev['message'].startswith('PG autoscaler'):
                pg_autoscaler_count += 1

        return pg_autoscaler_count

    def _get_pg_autoscaler_events(self):
        """
        Return the event that deals with PG autoscaler
        """
        pg_autoscaler_events = []
        events_in_progress = self._events_in_progress()
        for ev in events_in_progress:
            if ev['message'].startswith('PG autoscaler'):
                pg_autoscaler_events.append(ev)

        return pg_autoscaler_events

    def _simulate_pg_autoscaler_event(self):
        """
        Simulate a PG autoscale event, assuming that
        noautoscale flag is on.

        Return the JSON representation of the pg_autoscale event.
        """
        self._setup_pool(size=3, bulk='true', pg_autoscale_bias=4) # create pool with pg_autoscale_bias 4 and --bulk flag
        self._write_some_data(self.WRITE_PERIOD)
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'unset', 'noautoscale')
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'set', self.POOL, 'pg_autoscale_mode', 'on')
        # Wait for a progress event to pop up
        self.wait_until_equal(lambda: self._pg_autoscaler_events_count(), 1,
                                  timeout=self.PG_AUTOSCALER_EVENT_CREATION_PERIOD,
                                  period=1)
        ev = self._get_pg_autoscaler_events()[0]
        log.info(json.dumps(ev, indent=1))
        self.assertIn("PG autoscaler", ev['message'])
        return ev


    def _is_quiet(self):
        """
        Whether any progress events are live.
        """
        return len(self._get_progress()['events']) == 0

    def _is_complete(self, ev_id):
        """
        Whether the event with the given ID has completed."""
        progress = self._get_progress()
        live_ids = [ev['id'] for ev in progress['events']]
        complete_ids = [ev['id'] for ev in progress['completed']]
        if ev_id in complete_ids:
            assert ev_id not in live_ids
            return True
        else:
            assert ev_id in live_ids
            return False

    def _is_inprogress_or_complete(self, ev_id):
        """
        Whether the event with the given ID is either in progress
        or complete.  This is useful for waiting on an event that
        may complete while we're waiting.
        """
        for ev in self._events_in_progress():
            if ev['id'] == ev_id:
                return ev['progress'] > 0
        # check if the event completed
        return self._is_complete(ev_id)

    def tearDown(self):
        """
        Clean up the test environment after a test is run.
        """
        if self.POOL in self.mgr_cluster.mon_manager.pools:
            self.mgr_cluster.mon_manager.remove_pool(self.POOL)

        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'unset', 'nobackfill')
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'unset', 'norecover')

        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for osd in osd_map['osds']:
            if osd['weight'] == 0.0:
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', str(osd['osd']))

        # Unset allow_pg_recovery_event in case it's set to true
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr/progress/allow_pg_recovery_event', 'false')

        super(TestProgress, self).tearDown()

    def test_osd_healthy_recovery(self):
        """
        The simple recovery case: an OSD goes down, its PGs get a new
        placement, and we wait for the PG to get healthy in its new
        locations.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr/progress/allow_pg_recovery_event', 'true')

        ev = self._simulate_failure()

        # Wait for progress event to ultimately reach completion
        self.wait_until_true(lambda: self._is_complete(ev['id']),
                             timeout=self.RECOVERY_PERIOD)
        self.assertEqual(self._osd_in_out_events_count(), 0)

    def test_pool_removal(self):
        """
        That a pool removed during OSD recovery causes the
        progress event to be correctly marked complete once there
        is no more data to move.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr/progress/allow_pg_recovery_event', 'true')

        ev = self._simulate_failure()

        self.mgr_cluster.mon_manager.remove_pool(self.POOL)

        # Event should complete promptly
        self.wait_until_true(lambda: self._is_complete(ev['id']),
                             timeout=self.RECOVERY_PERIOD)
        self.assertEqual(self._osd_in_out_events_count(), 0)

    def test_osd_came_back(self):
        """
        When a recovery is underway, but then the out OSD
        comes back in, such that recovery is no longer necessary.
        It should create another event for when osd is marked in
        and cancel the one that is still ongoing.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr/progress/allow_pg_recovery_event', 'true')

        ev1 = self._simulate_failure()

        ev2 = self._simulate_back_in([0], ev1)

        if ev2 is not None:
            # Wait for progress event to ultimately complete
            self.wait_until_true(lambda: self._is_complete(ev2['id']),
                                 timeout=self.RECOVERY_PERIOD)

        self.assertEqual(self._osd_in_out_events_count(), 0)

    def test_turn_off_module(self):
        """
        When the the module is turned off, there should not
        be any on going events or completed events.
        Also module should not accept any kind of Remote Event
        coming in from other module, however, once it is turned
        back, on creating an event should be working as it is.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr/progress/allow_pg_recovery_event', 'true')

        pool_size = 3
        self._setup_pool(size=pool_size)
        self._write_some_data(self.WRITE_PERIOD)
        self.mgr_cluster.mon_manager.raw_cluster_cmd("progress", "off")

        with self.recovery_backfill_disabled():
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'out', '0')

        time.sleep(self.EVENT_CREATION_PERIOD/2)

        with self.recovery_backfill_disabled():
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', '0')

        time.sleep(self.EVENT_CREATION_PERIOD/2)

        self.assertTrue(self._no_events_anywhere())

        self.mgr_cluster.mon_manager.raw_cluster_cmd("progress", "on")

        self._write_some_data(self.WRITE_PERIOD)

        with self.recovery_backfill_disabled():

            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'out', '0')

            # Wait for a progress event to pop up
            self.wait_until_equal(lambda: self._osd_in_out_events_count('out'), 1,
                                  timeout=self.EVENT_CREATION_PERIOD,
                                  period=1)

        ev1 = self._get_osd_in_out_events('out')[0]

        log.info(json.dumps(ev1, indent=1))

        self.wait_until_true(lambda: self._is_complete(ev1['id']),
                             check_fn=lambda: self._is_inprogress_or_complete(ev1['id']),
                             timeout=self.RECOVERY_PERIOD)
        self.assertTrue(self._is_quiet())

    def test_default_progress_test(self):
        """
        progress module disabled the event of pg recovery event
        by default, we test this to see if this holds true
        """
        pool_size = 3
        self._setup_pool(size=pool_size)
        self._write_some_data(self.WRITE_PERIOD)

        with self.recovery_backfill_disabled():
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'out', '0')

        time.sleep(self.EVENT_CREATION_PERIOD/2)

        with self.recovery_backfill_disabled():
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    'osd', 'in', '0')

        time.sleep(self.EVENT_CREATION_PERIOD/2)

        self.assertEqual(self._osd_in_out_events_count(), 0)

    def test_pg_autoscaler_event(self):
        """
        Test PG autoscaler event creation and completion.
        """
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr/progress/allow_pg_recovery_event', 'true')

        ev = self._simulate_pg_autoscaler_event()

        # Wait for progress event to ultimately complete
        self.wait_until_true(lambda: self._is_complete(ev['id']),
                                 timeout=self.PG_SCALE_UP_PERIOD)

        # There should not be any on going pg_autoscale event
        self.assertEqual(self._pg_autoscaler_events_count(), 0)
