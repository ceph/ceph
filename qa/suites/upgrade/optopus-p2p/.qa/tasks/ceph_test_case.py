
import unittest
from unittest import case
import time
import logging

from teuthology.orchestra.run import CommandFailedError

log = logging.getLogger(__name__)


class CephTestCase(unittest.TestCase):
    """
    For test tasks that want to define a structured set of
    tests implemented in python.  Subclass this with appropriate
    helpers for the subsystem you're testing.
    """

    # Environment references
    mounts = None
    fs = None
    recovery_fs = None
    ceph_cluster = None
    mds_cluster = None
    mgr_cluster = None
    ctx = None

    mon_manager = None

    # Declarative test requirements: subclasses should override these to indicate
    # their special needs.  If not met, tests will be skipped.
    REQUIRE_MEMSTORE = False

    def setUp(self):
        self.ceph_cluster.mon_manager.raw_cluster_cmd("log",
            "Starting test {0}".format(self.id()))

        if self.REQUIRE_MEMSTORE:
            objectstore = self.ceph_cluster.get_config("osd_objectstore", "osd")
            if objectstore != "memstore":
                # You certainly *could* run this on a real OSD, but you don't want to sit
                # here for hours waiting for the test to fill up a 1TB drive!
                raise case.SkipTest("Require `memstore` OSD backend (test " \
                        "would take too long on full sized OSDs")



    def tearDown(self):
        self.ceph_cluster.mon_manager.raw_cluster_cmd("log",
            "Ended test {0}".format(self.id()))

    def assert_cluster_log(self, expected_pattern, invert_match=False,
                           timeout=10, watch_channel=None):
        """
        Context manager.  Assert that during execution, or up to 5 seconds later,
        the Ceph cluster log emits a message matching the expected pattern.

        :param expected_pattern: A string that you expect to see in the log output
        :type expected_pattern: str
        :param watch_channel: Specifies the channel to be watched. This can be
                              'cluster', 'audit', ...
        :type watch_channel: str
        """

        ceph_manager = self.ceph_cluster.mon_manager

        class ContextManager(object):
            def match(self):
                found = expected_pattern in self.watcher_process.stdout.getvalue()
                if invert_match:
                    return not found

                return found

            def __enter__(self):
                self.watcher_process = ceph_manager.run_ceph_w(watch_channel)

            def __exit__(self, exc_type, exc_val, exc_tb):
                if not self.watcher_process.finished:
                    # Check if we got an early match, wait a bit if we didn't
                    if self.match():
                        return
                    else:
                        log.debug("No log hits yet, waiting...")
                        # Default monc tick interval is 10s, so wait that long and
                        # then some grace
                        time.sleep(5 + timeout)

                self.watcher_process.stdin.close()
                try:
                    self.watcher_process.wait()
                except CommandFailedError:
                    pass

                if not self.match():
                    log.error("Log output: \n{0}\n".format(self.watcher_process.stdout.getvalue()))
                    raise AssertionError("Expected log message not found: '{0}'".format(expected_pattern))

        return ContextManager()

    def wait_for_health(self, pattern, timeout):
        """
        Wait until 'ceph health' contains messages matching the pattern
        """
        def seen_health_warning():
            health = self.ceph_cluster.mon_manager.get_mon_health()
            codes = [s for s in health['checks']]
            summary_strings = [s[1]['summary']['message'] for s in health['checks'].iteritems()]
            if len(summary_strings) == 0:
                log.debug("Not expected number of summary strings ({0})".format(summary_strings))
                return False
            else:
                for ss in summary_strings:
                    if pattern in ss:
                         return True
                if pattern in codes:
                    return True

            log.debug("Not found expected summary strings yet ({0})".format(summary_strings))
            return False

        self.wait_until_true(seen_health_warning, timeout)

    def wait_for_health_clear(self, timeout):
        """
        Wait until `ceph health` returns no messages
        """
        def is_clear():
            health = self.ceph_cluster.mon_manager.get_mon_health()
            return len(health['checks']) == 0

        self.wait_until_true(is_clear, timeout)

    def wait_until_equal(self, get_fn, expect_val, timeout, reject_fn=None):
        period = 5
        elapsed = 0
        while True:
            val = get_fn()
            if val == expect_val:
                return
            elif reject_fn and reject_fn(val):
                raise RuntimeError("wait_until_equal: forbidden value {0} seen".format(val))
            else:
                if elapsed >= timeout:
                    raise RuntimeError("Timed out after {0} seconds waiting for {1} (currently {2})".format(
                        elapsed, expect_val, val
                    ))
                else:
                    log.debug("wait_until_equal: {0} != {1}, waiting...".format(val, expect_val))
                time.sleep(period)
                elapsed += period

        log.debug("wait_until_equal: success")

    @classmethod
    def wait_until_true(cls, condition, timeout, period=5):
        elapsed = 0
        while True:
            if condition():
                log.debug("wait_until_true: success in {0}s".format(elapsed))
                return
            else:
                if elapsed >= timeout:
                    raise RuntimeError("Timed out after {0}s".format(elapsed))
                else:
                    log.debug("wait_until_true: waiting...")
                time.sleep(period)
                elapsed += period


