from typing import Optional, TYPE_CHECKING
import unittest
import time
import logging
from io import StringIO

from teuthology.exceptions import CommandFailedError

if TYPE_CHECKING:
    from tasks.mgr.mgr_test_case import MgrCluster

log = logging.getLogger(__name__)

class TestTimeoutError(RuntimeError):
    pass


class RunCephCmd:

    def run_ceph_cmd(self, *args, **kwargs):
        """
        *args and **kwargs must contain arguments that are accepted by
        vstart_runner.LocalRemote._do_run() or teuhology.orchestra.run.run()
        methods.
        """
        if kwargs.get('args') is None and args:
            if len(args) == 1:
                args = args[0]
            kwargs['args'] = args
        return self.mon_manager.run_cluster_cmd(**kwargs)

    def get_ceph_cmd_result(self, *args, **kwargs):
        """
        *args and **kwargs must contain arguments that are accepted by
        vstart_runner.LocalRemote._do_run() or teuhology.orchestra.run.run()
        methods.
        """
        if kwargs.get('args') is None and args:
            if len(args) == 1:
                args = args[0]
            kwargs['args'] = args
        return self.run_ceph_cmd(**kwargs).exitstatus

    def get_ceph_cmd_stdout(self, *args, **kwargs):
        """
        *args and **kwargs must contain arguments that are accepted by
        vstart_runner.LocalRemote._do_run() or teuhology.orchestra.run.run()
        methods.
        """
        if kwargs.get('args') is None and args:
            if len(args) == 1:
                args = args[0]
            kwargs['args'] = args
        kwargs['stdout'] = kwargs.pop('stdout', StringIO())
        return self.run_ceph_cmd(**kwargs).stdout.getvalue()

    def assert_retval(self, proc_retval, exp_retval):
        msg = (f'expected return value: {exp_retval}\n'
               f'received return value: {proc_retval}\n')
        assert proc_retval == exp_retval, msg

    def _verify(self, proc, exp_retval=None, exp_errmsgs=None):
        if exp_retval is None and exp_errmsgs is None:
            raise RuntimeError('Method didn\'t get enough parameters. Pass '
                               'return value or error message expected from '
                               'the command/process.')

        if exp_retval is not None:
            self.assert_retval(proc.returncode, exp_retval)
        if exp_errmsgs is None:
            return

        if isinstance(exp_errmsgs, str):
            exp_errmsgs = (exp_errmsgs, )
        exp_errmsgs = tuple([e.lower() for e in exp_errmsgs])

        proc_stderr = proc.stderr.getvalue().lower()
        msg = ('didn\'t find any of the expected string in stderr.\n'
               f'expected string -\n{exp_errmsgs}\n'
               f'received error message -\n{proc_stderr}\n'
               'note: received error message is converted to lowercase')
        for e in exp_errmsgs:
            if e in proc_stderr:
                break
        # this else is meant for the for loop above.
        else:
            assert False, msg

    def negtest_ceph_cmd(self, args, retval=None, errmsgs=None, **kwargs):
        """
        Conduct a negative test for the given Ceph command.

        retval and errmsgs are parameters to confirm the cause of command
        failure.

        *args and **kwargs must contain arguments that are accepted by
        vstart_runner.LocalRemote._do_run() or teuhology.orchestra.run.run()
        methods.

        NOTE: errmsgs is expected to be a tuple, but in case there's only one
        error message, it can also be a string. This method will add the string
        to a tuple internally.
        """
        kwargs['args'] = args
        # execution is needed to not halt on command failure because we are
        # conducting negative testing
        kwargs['check_status'] = False
        # log stdout since it may contain something useful when command fails
        kwargs['stdout'] = StringIO()
        # stderr is needed to check for expected error messages.
        kwargs['stderr'] = StringIO()

        proc = self.run_ceph_cmd(**kwargs)
        self._verify(proc, retval, errmsgs)
        return proc


class CephTestCase(unittest.TestCase, RunCephCmd):
    """
    For test tasks that want to define a structured set of
    tests implemented in python.  Subclass this with appropriate
    helpers for the subsystem you're testing.
    """

    # Environment references
    mounts = None
    fs = None
    recovery_fs = None
    backup_fs = None
    ceph_cluster = None
    mds_cluster = None
    mgr_cluster: Optional['MgrCluster'] = None
    ctx = None

    mon_manager = None

    # Declarative test requirements: subclasses should override these to indicate
    # their special needs.  If not met, tests will be skipped.
    REQUIRE_MEMSTORE = False

    def _init_mon_manager(self):
        # if vstart_runner.py has invoked this code
        if 'Local' in str(type(self.ceph_cluster)):
            from tasks.vstart_runner import LocalCephManager
            self.mon_manager = LocalCephManager(ctx=self.ctx)
        # else teuthology has invoked this code
        else:
            from tasks.ceph_manager import CephManager
            self.mon_manager = CephManager(self.ceph_cluster.admin_remote,
                ctx=self.ctx, logger=log.getChild('ceph_manager'))

    def setUp(self):
        self._mon_configs_set = set()

        self._init_mon_manager()
        self.admin_remote = self.ceph_cluster.admin_remote

        self.ceph_cluster.mon_manager.raw_cluster_cmd("log",
            "Starting test {0}".format(self.id()))

        if self.REQUIRE_MEMSTORE:
            objectstore = self.ceph_cluster.get_config("osd_objectstore", "osd")
            if objectstore != "memstore":
                # You certainly *could* run this on a real OSD, but you don't want to sit
                # here for hours waiting for the test to fill up a 1TB drive!
                raise self.skipTest("Require `memstore` OSD backend (test " \
                        "would take too long on full sized OSDs")

    def tearDown(self):
        self.config_clear()

        self.ceph_cluster.mon_manager.raw_cluster_cmd("log",
            "Ended test {0}".format(self.id()))

    def config_clear(self):
        for section, key in self._mon_configs_set:
            self.config_rm(section, key)
        self._mon_configs_set.clear()

    def _fix_key(self, key):
        return str(key).replace(' ', '_')

    def config_get(self, section, key):
       key = self._fix_key(key)
       return self.ceph_cluster.mon_manager.raw_cluster_cmd("config", "get", section, key).strip()

    def config_show(self, entity, key):
       key = self._fix_key(key)
       return self.ceph_cluster.mon_manager.raw_cluster_cmd("config", "show", entity, key).strip()

    def config_minimal(self):
       return self.ceph_cluster.mon_manager.raw_cluster_cmd("config", "generate-minimal-conf").strip()

    def config_rm(self, section, key):
       key = self._fix_key(key)
       self.ceph_cluster.mon_manager.raw_cluster_cmd("config", "rm", section, key)
       # simplification: skip removing from _mon_configs_set;
       # let tearDown clear everything again

    def config_set(self, section, key, value):
       key = self._fix_key(key)
       self._mon_configs_set.add((section, key))
       self.ceph_cluster.mon_manager.raw_cluster_cmd("config", "set", section, key, str(value))

    def cluster_cmd(self, command: str):
        assert self.ceph_cluster is not None
        return self.ceph_cluster.mon_manager.raw_cluster_cmd(*(command.split(" ")))


    def assert_cluster_log(self, expected_pattern, invert_match=False,
                           timeout=10, watch_channel=None, present=True):
        """
        Context manager.  Assert that during execution, or up to 5 seconds later,
        the Ceph cluster log emits a message matching the expected pattern.

        :param expected_pattern: A string that you expect to see in the log output
        :type expected_pattern: str
        :param watch_channel: Specifies the channel to be watched. This can be
                              'cluster', 'audit', ...
        :type watch_channel: str
        :param present: Assert the log entry is present (default: True) or not (False).
        :type present: bool
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
                fail = False
                if not self.watcher_process.finished:
                    # Check if we got an early match, wait a bit if we didn't
                    if present and self.match():
                        return
                    elif not present and self.match():
                        fail = True
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

                if present and not self.match():
                    log.error(f"Log output: \n{self.watcher_process.stdout.getvalue()}\n")
                    raise AssertionError(f"Expected log message not found: '{expected_pattern}'")
                elif fail or (not present and self.match()):
                    log.error(f"Log output: \n{self.watcher_process.stdout.getvalue()}\n")
                    raise AssertionError(f"Unexpected log message found: '{expected_pattern}'")

        return ContextManager()

    def wait_for_health(self, pattern, timeout, check_in_detail=None):
        """
        Wait until 'ceph health' contains messages matching the pattern
        Also check if @check_in_detail matches detailed health messages
        only when @pattern is a code string.
        """
        def seen_health_warning():
            health = self.ceph_cluster.mon_manager.get_mon_health(debug=False, detail=bool(check_in_detail))
            codes = [s for s in health['checks']]
            summary_strings = [s[1]['summary']['message'] for s in health['checks'].items()]
            if len(summary_strings) == 0:
                log.debug("Not expected number of summary strings ({0})".format(summary_strings))
                return False
            else:
                for ss in summary_strings:
                    if pattern in ss:
                         return True
                if pattern in codes:
                    if not check_in_detail:
                        return True
                    # check if the string is in detail list if asked
                    detail_strings = [ss['message'] for ss in \
                                      [s for s in health['checks'][pattern]['detail']]]
                    log.debug(f'detail_strings: {detail_strings}')
                    for ds in detail_strings:
                        if check_in_detail in ds:
                            return True
                    log.debug(f'detail string "{check_in_detail}" not found')

            log.debug("Not found expected summary strings yet ({0})".format(summary_strings))
            return False

        log.info(f"waiting {timeout}s for health warning matching {pattern}")
        self.wait_until_true(seen_health_warning, timeout)

    def wait_for_health_clear(self, timeout):
        """
        Wait until `ceph health` returns no messages
        """
        def is_clear():
            health = self.ceph_cluster.mon_manager.get_mon_health()
            return len(health['checks']) == 0

        self.wait_until_true(is_clear, timeout)

    def wait_until_equal(self, get_fn, expect_val, timeout, reject_fn=None, period=5):
        elapsed = 0
        while True:
            val = get_fn()
            if val == expect_val:
                return
            elif reject_fn and reject_fn(val):
                raise RuntimeError("wait_until_equal: forbidden value {0} seen".format(val))
            else:
                if elapsed >= timeout:
                    raise TestTimeoutError("Timed out after {0} seconds waiting for {1} (currently {2})".format(
                        elapsed, expect_val, val
                    ))
                else:
                    log.debug("wait_until_equal: {0} != {1}, waiting (timeout={2})...".format(val, expect_val, timeout))
                time.sleep(period)
                elapsed += period

        log.debug("wait_until_equal: success")

    @classmethod
    def wait_until_true(cls, condition, timeout, check_fn=None, period=5):
        elapsed = 0
        retry_count = 0
        while True:
            if condition():
                log.debug("wait_until_true: success in {0}s and {1} retries".format(elapsed, retry_count))
                return
            else:
                if elapsed >= timeout:
                    if check_fn and check_fn() and retry_count < 5:
                        elapsed = 0
                        retry_count += 1
                        log.debug("wait_until_true: making progress, waiting (timeout={0} retry_count={1})...".format(timeout, retry_count))
                    else:
                        raise TestTimeoutError("Timed out after {0}s and {1} retries".format(elapsed, retry_count))
                else:
                    log.debug("wait_until_true: waiting (timeout={0} retry_count={1})...".format(timeout, retry_count))
                time.sleep(period)
                elapsed += period
