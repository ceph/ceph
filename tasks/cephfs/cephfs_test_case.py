import logging
import unittest
import time
from teuthology.task import interactive


log = logging.getLogger(__name__)


class CephFSTestCase(unittest.TestCase):
    """
    Test case for Ceph FS, requires caller to populate Filesystem and Mounts,
    into the fs, mount_a, mount_b class attributes (setting mount_b is optional)

    Handles resetting the cluster under test between tests.
    """
    # Environment references
    mount_a = None
    mount_b = None
    fs = None

    def setUp(self):
        self.fs.clear_firewall()

        # Unmount in order to start each test on a fresh mount, such
        # that test_barrier can have a firm expectation of what OSD
        # epoch the clients start with.
        if self.mount_a.is_mounted():
            self.mount_a.umount_wait()

        if self.mount_b:
            if self.mount_b.is_mounted():
                self.mount_b.umount_wait()

        # To avoid any issues with e.g. unlink bugs, we destroy and recreate
        # the filesystem rather than just doing a rm -rf of files
        self.fs.mds_stop()
        self.fs.mds_fail()
        self.fs.delete()
        self.fs.create()

        # In case the previous filesystem had filled up the RADOS cluster, wait for that
        # flag to pass.
        osd_mon_report_interval_max = int(self.fs.get_config("osd_mon_report_interval_max", service_type='osd'))
        self.wait_until_true(lambda: not self.fs.is_full(),
                             timeout=osd_mon_report_interval_max * 5)

        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        if not self.mount_a.is_mounted():
            self.mount_a.mount()
            self.mount_a.wait_until_mounted()

        if self.mount_b:
            if not self.mount_b.is_mounted():
                self.mount_b.mount()
                self.mount_b.wait_until_mounted()

        self.configs_set = set()

    def tearDown(self):
        self.fs.clear_firewall()
        self.mount_a.teardown()
        if self.mount_b:
            self.mount_b.teardown()

        for subsys, key in self.configs_set:
            self.fs.clear_ceph_conf(subsys, key)

    def set_conf(self, subsys, key, value):
        self.configs_set.add((subsys, key))
        self.fs.set_ceph_conf(subsys, key, value)

    def assert_session_count(self, expected, ls_data=None):
        if ls_data is None:
            ls_data = self.fs.mds_asok(['session', 'ls'])

        self.assertEqual(expected, len(ls_data), "Expected {0} sessions, found {1}".format(
            expected, len(ls_data)
        ))

    def assert_session_state(self, client_id,  expected_state):
        self.assertEqual(
            self._session_by_id(
                self.fs.mds_asok(['session', 'ls'])).get(client_id, {'state': None})['state'],
            expected_state)

    def get_session_data(self, client_id):
        return self._session_by_id(client_id)

    def _session_list(self):
        ls_data = self.fs.mds_asok(['session', 'ls'])
        ls_data = [s for s in ls_data if s['state'] not in ['stale', 'closed']]
        return ls_data

    def get_session(self, client_id, session_ls=None):
        if session_ls is None:
            session_ls = self.fs.mds_asok(['session', 'ls'])

        return self._session_by_id(session_ls)[client_id]

    def _session_by_id(self, session_ls):
        return dict([(s['id'], s) for s in session_ls])

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

    def wait_until_true(self, condition, timeout):
        period = 5
        elapsed = 0
        while True:
            if condition():
                return
            else:
                if elapsed >= timeout:
                    raise RuntimeError("Timed out after {0} seconds".format(elapsed))
                else:
                    log.debug("wait_until_true: waiting...")
                time.sleep(period)
                elapsed += period

        log.debug("wait_until_true: success")


class LogStream(object):
    def __init__(self):
        self.buffer = ""

    def write(self, data):
        self.buffer += data
        if "\n" in self.buffer:
            lines = self.buffer.split("\n")
            for line in lines[:-1]:
                log.info(line)
            self.buffer = lines[-1]

    def flush(self):
        pass


class InteractiveFailureResult(unittest.TextTestResult):
    """
    Specialization that implements interactive-on-error style
    behavior.
    """
    ctx = None

    def addFailure(self, test, err):
        log.error(self._exc_info_to_string(err, test))
        log.error("Failure in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=self.ctx, config=None)

    def addError(self, test, err):
        log.error(self._exc_info_to_string(err, test))
        log.error("Error in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=self.ctx, config=None)


def run_tests(ctx, config, test_klass, params):
    for k, v in params.items():
        setattr(test_klass, k, v)

    # Execute test suite
    # ==================
    if config and 'test_name' in config:
        # Test names like TestCase.this_test
        suite = unittest.TestLoader().loadTestsFromName(
            "{0}.{1}".format(test_klass.__module__, config['test_name']))
    else:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_klass)

    if ctx.config.get("interactive-on-error", False):
        InteractiveFailureResult.ctx = ctx
        result_class = InteractiveFailureResult
    else:
        result_class = unittest.TextTestResult

    # Unmount all clients not involved
    for mount in ctx.mounts.values():
        if mount is not params.get('mount_a') and mount is not params.get('mount_b'):
            if mount.is_mounted():
                log.info("Unmounting unneeded client {0}".format(mount.client_id))
                mount.umount_wait()

    # Execute!
    result = unittest.TextTestRunner(
        stream=LogStream(),
        resultclass=result_class,
        verbosity=2,
        failfast=True).run(suite)

    if not result.wasSuccessful():
        result.printErrors()  # duplicate output at end for convenience

        bad_tests = []
        for test, error in result.errors:
            bad_tests.append(str(test))
        for test, failure in result.failures:
            bad_tests.append(str(test))

        raise RuntimeError("Test failure: {0}".format(", ".join(bad_tests)))
