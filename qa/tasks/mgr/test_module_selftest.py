
import time
import requests
import errno
from teuthology.exceptions import CommandFailedError

from tasks.mgr.mgr_test_case import MgrTestCase


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
        self.setup_mgrs()

    def _selftest_plugin(self, module_name):
        self._load_module(module_name)

        # Execute the module's self-test routine
        self.mgr_cluster.mon_manager.raw_cluster_cmd(module_name, "self-test")

    def test_zabbix(self):
        # Set these mandatory config fields so that the zabbix module
        # won't trigger health/log errors on load/serve.
        self.mgr_cluster.set_module_conf("zabbix", "zabbix_host", "localhost")
        self.mgr_cluster.set_module_conf("zabbix", "identifier", "foo")
        self._selftest_plugin("zabbix")

    def test_prometheus(self):
        self._assign_ports("prometheus", "server_port", min_port=8100)
        self._selftest_plugin("prometheus")

    def test_influx(self):
        self._selftest_plugin("influx")

    def test_selftest_run(self):
        self._load_module("selftest")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "self-test", "run")

    def test_selftest_command_spam(self):
        # Use the selftest module to stress the mgr daemon
        self._load_module("selftest")

        # Use the dashboard to test that the mgr is still able to do its job
        self._assign_ports("dashboard", "server_port")
        self._load_module("dashboard")

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
            r = requests.get(dashboard_uri)
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

        self._load_module("selftest")

        # Calling a command on a disabled module should return the proper
        # error code.
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "module", "disable", "status")
        with self.assertRaises(CommandFailedError) as exc_raised:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "osd", "status")

        self.assertEqual(exc_raised.exception.exitstatus, errno.EOPNOTSUPP)

        # Calling a command that really doesn't exist should give me EINVAL.
        with self.assertRaises(CommandFailedError) as exc_raised:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "osd", "albatross")

        self.assertEqual(exc_raised.exception.exitstatus, errno.EINVAL)

        # Enabling a module and then immediately using ones of its commands
        # should work (#21683)
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "module", "enable", "status")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("osd", "status")

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

        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "module", "disable", "selftest")

        self.wait_for_health_clear(timeout=30)
