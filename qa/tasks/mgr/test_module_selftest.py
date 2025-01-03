
import time
import requests
import errno
import logging

from teuthology.exceptions import CommandFailedError

from .mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestModuleSelftest(MgrTestCase):
    """
    That modules with a self-test command can be loaded and execute it
    without errors.

    This is not a substitute for really testing the modules, but it
    is quick and is designed to catch regressions that could occur
    if data structures change in a way that breaks how the modules
    touch them.
    """
    MGRS_REQUIRED = 1

    def setUp(self):
        super(TestModuleSelftest, self).setUp()
        self.setup_mgrs()

    def _selftest_plugin(self, module_name):
        self._load_module("selftest")
        self._load_module(module_name)

        # Execute the module's self_test() method
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "module", module_name)

    def test_prometheus(self):
        self._assign_ports("prometheus", "server_port", min_port=8100)
        self._selftest_plugin("prometheus")

    def test_influx(self):
        self._selftest_plugin("influx")

    def test_diskprediction_local(self):
        self._load_module("selftest")
        python_version = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "self-test", "python-version")
        if tuple(int(v) for v in python_version.split('.')) == (3, 8):
            # https://tracker.ceph.com/issues/45147
            self.skipTest(f'python {python_version} not compatible with '
                          'diskprediction_local')
        self._selftest_plugin("diskprediction_local")

    def test_telegraf(self):
        self._selftest_plugin("telegraf")

    def test_iostat(self):
        self._selftest_plugin("iostat")

    def test_devicehealth(self):
        self._selftest_plugin("devicehealth")

    def test_selftest_run(self):
        self._load_module("selftest")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "self-test", "run")

    def test_telemetry(self):
        self._selftest_plugin("telemetry")

    def test_crash(self):
        self._selftest_plugin("crash")

    def test_orchestrator(self):
        self._selftest_plugin("orchestrator")


    def test_selftest_config_update(self):
        """
        That configuration updates are seen by running mgr modules
        """
        self._load_module("selftest")

        def get_value():
            return self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "config", "get", "testkey").strip()

        self.assertEqual(get_value(), "None")
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "config", "set", "mgr", "mgr/selftest/testkey", "foo")
        self.wait_until_equal(get_value, "foo", timeout=10)

        def get_localized_value():
            return self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "config", "get_localized", "testkey").strip()

        self.assertEqual(get_localized_value(), "foo")
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "config", "set", "mgr", "mgr/selftest/{}/testkey".format(
                self.mgr_cluster.get_active_id()),
            "bar")
        self.wait_until_equal(get_localized_value, "bar", timeout=10)


    def test_selftest_command_spam(self):
        # Use the selftest module to stress the mgr daemon
        self._load_module("selftest")

        # Use the dashboard to test that the mgr is still able to do its job
        self._assign_ports("dashboard", "ssl_server_port")
        self._load_module("dashboard")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("dashboard",
                                                     "create-self-signed-cert")

        original_active = self.mgr_cluster.get_active_id()
        original_standbys = self.mgr_cluster.get_standby_ids()

        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "self-test",
                                                     "background", "start",
                                                     "command_spam")

        dashboard_uri = self._get_uri("dashboard")

        delay = 10
        periods = 10
        for i in range(0, periods):
            t1 = time.time()
            # Check that an HTTP module remains responsive
            r = requests.get(dashboard_uri, verify=False)
            self.assertEqual(r.status_code, 200)

            # Check that a native non-module command remains responsive
            self.mgr_cluster.mon_manager.raw_cluster_cmd("osd", "df")

            time.sleep(delay - (time.time() - t1))

        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "self-test",
                                                     "background", "stop")

        # Check that all mgr daemons are still running
        self.assertEqual(original_active, self.mgr_cluster.get_active_id())
        self.assertEqual(original_standbys, self.mgr_cluster.get_standby_ids())

    def test_module_commands(self):
        """
        That module-handled commands have appropriate  behavior on
        disabled/failed/recently-enabled modules.
        """

        # Calling a command on a disabled module should return the proper
        # error code.
        self._load_module("selftest")
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "module", "disable", "selftest")
        with self.assertRaises(CommandFailedError) as exc_raised:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "run")

        self.assertEqual(exc_raised.exception.exitstatus, errno.EOPNOTSUPP)

        # Calling a command that really doesn't exist should give me EINVAL.
        with self.assertRaises(CommandFailedError) as exc_raised:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "osd", "albatross")

        self.assertEqual(exc_raised.exception.exitstatus, errno.EINVAL)

        # Enabling a module and then immediately using ones of its commands
        # should work (#21683)
        self._load_module("selftest")
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "self-test", "config", "get", "testkey")

        # Calling a command for a failed module should return the proper
        # error code.
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "self-test", "background", "start", "throw_exception")
        with self.assertRaises(CommandFailedError) as exc_raised:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "run"
            )
        self.assertEqual(exc_raised.exception.exitstatus, errno.EIO)

        # A health alert should be raised for a module that has thrown
        # an exception from its serve() method
        self.wait_for_health(
            "Module 'selftest' has failed: Synthetic exception in serve",
            timeout=30)
        # prune the crash reports, so that the health report is back to
        # clean
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "crash", "prune", "0")
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "module", "disable", "selftest")

        self.wait_for_health_clear(timeout=30)

    def test_module_remote(self):
        """
        Use the selftest module to exercise inter-module communication
        """
        self._load_module("selftest")
        # The "self-test remote" operation just happens to call into
        # influx.
        self._load_module("influx")

        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "self-test", "remote")

    def test_selftest_cluster_log(self):
        """
        Use the selftest module to test the cluster/audit log interface.
        """
        priority_map = {
            "info": "INF",
            "security": "SEC",
            "warning": "WRN",
            "error": "ERR"
        }
        self._load_module("selftest")
        for priority in priority_map.keys():
            message = "foo bar {}".format(priority)
            log_message = "[{}] {}".format(priority_map[priority], message)
            # Check for cluster/audit logs:
            # 2018-09-24 09:37:10.977858 mgr.x [INF] foo bar info
            # 2018-09-24 09:37:10.977860 mgr.x [SEC] foo bar security
            # 2018-09-24 09:37:10.977863 mgr.x [WRN] foo bar warning
            # 2018-09-24 09:37:10.977866 mgr.x [ERR] foo bar error
            with self.assert_cluster_log(log_message):
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    "mgr", "self-test", "cluster-log", "cluster",
                    priority, message)
            with self.assert_cluster_log(log_message, watch_channel="audit"):
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    "mgr", "self-test", "cluster-log", "audit",
                    priority, message)

    def test_selftest_cluster_log_unknown_channel(self):
        """
        Use the selftest module to test the cluster/audit log interface.
        """
        with self.assertRaises(CommandFailedError) as exc_raised:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "cluster-log", "xyz",
                "ERR", "The channel does not exist")
        self.assertEqual(exc_raised.exception.exitstatus, errno.EOPNOTSUPP)
