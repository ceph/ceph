"""
Exercise the MDS phase tracker, and above all its runtime toggle.

Setting mds_enable_phase_tracker resets the counters and the mds_lock
baselines from a config thread, without mds_lock, while phase timers are in
flight on the messenger, finisher and upkeep threads.  These tests flip the
setting under a metadata workload and check that what comes back out of
`dump phase times` and the mds_phase perf counters stays sane: a phase or a
lock delta accounted across a reset shows up as an absurd total, and an
unsigned delta computed against a newer baseline shows up as a near-2^64
one.
"""

import logging
import time

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestPhaseTimes(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    # Enough metadata churn to keep every instrumented path busy: client
    # requests, cap traffic, journal completions, and the upkeep threads.
    WORKLOAD = """
        mkdir -p load
        while true; do
          for i in $(seq 200); do
            echo x > load/f$i
            cat load/f$i > /dev/null
          done
          rm -f load/f*
        done
    """

    # Nothing legitimate comes anywhere near this; a wrapped unsigned delta
    # is ~1.8e10 seconds and a total accounted across a reset is at least as
    # large as the rank's uptime.
    ABSURD_SEC = 1e6

    def setUp(self):
        super().setUp()
        # config_reset() in tearDown() puts this back
        self._set_enabled(False)
        self.workload = self.mount_a.run_shell_payload(self.WORKLOAD,
                                                       wait=False,
                                                       timeout=None,
                                                       check_status=False)

    def tearDown(self):
        # stdin-killer stops the workload when its stdin goes away
        self.workload.stdin.close()
        try:
            self.workload.wait()
        except CommandFailedError:
            pass
        super().tearDown()

    def _dump(self):
        return self.fs.rank_asok(["dump", "phase", "times"])

    def _perf(self):
        return self.fs.rank_asok(["perf", "dump", "mds_phase"])["mds_phase"]

    def _set_enabled(self, enable):
        self.config_set('mds', 'mds_enable_phase_tracker',
                        'true' if enable else 'false')
        # the config change reaches the MDS asynchronously; poll tightly so
        # that a toggle takes about as long as it takes to propagate, which
        # test_reset_on_enable() relies on
        self.wait_until_true(lambda: self._dump()["enabled"] == enable,
                             timeout=60, period=1)

    def _assert_sane(self, dump):
        """The invariants a botched reset breaks."""
        self.assertGreaterEqual(dump["elapsed_sec"], 0)
        self.assertLess(dump["elapsed_sec"], self.ABSURD_SEC)

        lock = dump["mds_lock"]
        for k in ("wait_sec", "held_sec"):
            self.assertGreaterEqual(lock[k], 0)
            self.assertLess(lock[k], self.ABSURD_SEC, f"mds_lock.{k}")
        # the lock cannot have been held for longer than the rank has been
        # tracking, give or take the resolution of elapsed_sec
        self.assertLess(lock["held_sec"], dump["elapsed_sec"] + 1)
        self.assertGreaterEqual(lock["utilization"], 0)

        self.assertLess(dump["accounted_sec"], self.ABSURD_SEC)
        for phase in dump["phases"]:
            self.assertGreaterEqual(phase["total_sec"], 0, phase["phase"])
            self.assertLess(phase["total_sec"], dump["elapsed_sec"] + 1,
                            phase["phase"])

    def _phase(self, dump, name):
        for phase in dump["phases"]:
            if phase["phase"] == name:
                return phase
        self.fail(f"no {name} phase in {dump}")

    def _wait_for_requests(self, count):
        """Wait until the tracker has seen `count` client requests."""
        self.wait_until_true(
            lambda: self._phase(self._dump(), "client_request")["count"] > count,
            timeout=120, period=1)

    def test_dump_disabled(self):
        """Disabled, the dump says so and reports nothing else."""
        dump = self._dump()
        self.assertFalse(dump["enabled"])
        self.assertIn("note", dump)
        self.assertNotIn("phases", dump)

    def test_enabled_under_load(self):
        """Enabled under load, every reported total advances and stays sane."""
        self._set_enabled(True)
        self._wait_for_requests(100)

        dump = self._dump()
        log.info(f"dump phase times: {dump}")
        self._assert_sane(dump)

        self.assertGreater(dump["elapsed_sec"], 0)
        self.assertGreater(dump["accounted_sec"], 0)
        self.assertGreater(dump["mds_lock"]["acquisitions"], 0)
        self.assertGreater(dump["mds_lock"]["held_sec"], 0)
        self.assertGreater(dump["mds_lock"]["utilization"], 0)

        requests = self._phase(dump, "client_request")
        self.assertGreater(requests["count"], 0)
        self.assertGreater(requests["total_sec"], 0)

        # the same numbers, by way of the perf counters
        perf = self._perf()
        log.info(f"perf dump mds_phase: {perf}")
        self.assertGreater(perf["lock_acquisitions"], 0)
        self.assertGreater(float(perf["lock_held"]), 0)
        self.assertLess(float(perf["lock_held"]), self.ABSURD_SEC)
        self.assertLess(float(perf["lock_wait"]), self.ABSURD_SEC)
        self.assertGreater(perf["client_request"]["avgcount"], 0)
        self.assertGreater(float(perf["client_request"]["sum"]), 0)
        for name, phase in perf.items():
            if isinstance(phase, dict):
                self.assertLess(float(phase["sum"]), self.ABSURD_SEC, name)

    def test_reset_on_enable(self):
        """Re-enabling starts a fresh window rather than continuing the old."""
        self._set_enabled(True)
        self._wait_for_requests(1000)
        # Let the first window run long enough that it is unambiguously
        # longer, and busier, than the second: the assertions below compare
        # the totals of a ~30s window against those of the window opened by
        # the toggle a couple of seconds ago.
        time.sleep(30)

        before = self._dump()
        self._assert_sane(before)
        requests_before = self._phase(before, "client_request")["count"]
        acquisitions_before = before["mds_lock"]["acquisitions"]

        self._set_enabled(False)
        self._set_enabled(True)

        # the workload is still running, so these are not zero for long; what
        # matters is that they restarted from zero and not from `before`
        after = self._dump()
        self._assert_sane(after)
        self.assertLess(after["elapsed_sec"], before["elapsed_sec"])
        self.assertLess(self._phase(after, "client_request")["count"],
                        requests_before)
        self.assertLess(after["mds_lock"]["acquisitions"], acquisitions_before)

        perf = self._perf()
        self.assertLess(perf["client_request"]["avgcount"], requests_before)
        self.assertLess(perf["lock_acquisitions"], acquisitions_before)

    def test_toggle_repeatedly_under_load(self):
        """Flip the setting under load; in-flight timers must not corrupt it.

        Each transition races the reset against timers already in flight on
        every thread that takes mds_lock, which is the code path the tracker
        has no other coverage for.
        """
        for i in range(10):
            self._set_enabled(True)
            self._assert_sane(self._dump())
            time.sleep(0.5)
            self._assert_sane(self._dump())
            self._set_enabled(False)
            log.info(f"toggle {i} done")

        # and the rank is still serving, and still accounting correctly
        self._set_enabled(True)
        self._wait_for_requests(100)
        dump = self._dump()
        self._assert_sane(dump)
        self.assertGreater(dump["accounted_sec"], 0)
        self.mount_a.run_shell(["ls", "load"])
