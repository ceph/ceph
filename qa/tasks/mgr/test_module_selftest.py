
import time
import requests
import errno
import logging
from teuthology.exceptions import CommandFailedError

from tasks.mgr.mgr_test_case import MgrTestCase

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

    def test_telegraf(self):
        self._selftest_plugin("telegraf")

    def test_iostat(self):
        self._selftest_plugin("iostat")

    def test_selftest_run(self):
        self._load_module("selftest")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "self-test", "run")

    def test_telemetry(self):
        self._selftest_plugin("telemetry")

    def test_crash(self):
        self._selftest_plugin("crash")

    def test_selftest_config_update(self):
        """
        That configuration updates are seen by running mgr modules
        """
        self._load_module("selftest")

        def get_value():
            return self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    "mgr", "self-test", "config", "get", "testkey").strip()

        self.assertEqual(get_value(), "None")

        self.mgr_cluster.mon_manager.raw_cluster_cmd("config", "set",
                "mgr", "mgr/selftest/testkey", "testvalue")

        self.wait_until_equal(get_value, "testvalue",timeout=10)

        active_id = self.mgr_cluster.get_active_id()

        def get_localized_value():
            return self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    "mgr", "self-test", "config", "get_localized", "testlkey").strip()

        self.mgr_cluster.mon_manager.raw_cluster_cmd("config", "set",
                "mgr", "mgr/selftest/{0}/testlkey".format(active_id),
                "test localized value")

        self.wait_until_equal(get_localized_value, "test localized value",
                              timeout=10)

    def test_selftest_config_upgrade(self):
        """
        That pre-mimic config-key config settings are migrated into
        mimic-style config settings and visible from mgr modules.
        """
        self._load_module("selftest")

        def get_value():
            return self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    "mgr", "self-test", "config", "get", "testkey").strip()

        def get_config():
            lines = self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    "config", "dump")\
                            .strip().split("\n")
            result = []
            for line in lines[1:]:
                tokens = line.strip().split()
                log.info("tokens: {0}".format(tokens))
                subsys, key, value = tokens[0], tokens[2], tokens[3]
                result.append((subsys, key, value))

            return result

        # Stop ceph-mgr while we synthetically create a pre-mimic
        # configuration scenario
        for mgr_id in self.mgr_cluster.mgr_daemons.keys():
            self.mgr_cluster.mgr_stop(mgr_id)
            self.mgr_cluster.mgr_fail(mgr_id)

        # Blow away any modern-style mgr module config options
        # (the ceph-mgr implementation may only do the upgrade if
        #  it doesn't see new style options)
        stash = []
        for subsys, key, value in get_config():
            if subsys == "mgr" and key.startswith("mgr/"):
                log.info("Removing config key {0} ahead of upgrade".format(
                    key))
                self.mgr_cluster.mon_manager.raw_cluster_cmd(
                        "config", "rm", subsys, key)
                stash.append((subsys, key, value))

        # Inject an old-style configuration setting in config-key
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "config-key", "set", "mgr/selftest/testkey", "testvalue")

        # Inject configuration settings that looks data-ish and should
        # not be migrated to a config key
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "config-key", "set", "mgr/selftest/testnewline", "foo\nbar")

        # Inject configuration setting that does not appear in the
        # module's config schema
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "config-key", "set", "mgr/selftest/kvitem", "foo\nbar")

        # Bring mgr daemons back online, the one that goes active
        # should be doing the upgrade.
        for mgr_id in self.mgr_cluster.mgr_daemons.keys():
            self.mgr_cluster.mgr_restart(mgr_id)

        # Wait for a new active 
        self.wait_until_true(
                lambda: self.mgr_cluster.get_active_id() != "", timeout=30)

        # Check that the selftest module sees the upgraded value
        self.assertEqual(get_value(), "testvalue")

        # Check that the upgraded value is visible in the configuration
        seen_keys = [k for s,k,v in get_config()]
        self.assertIn("mgr/selftest/testkey", seen_keys)

        # ...and that the non-config-looking one isn't
        self.assertNotIn("mgr/selftest/testnewline", seen_keys)

        # ...and that the not-in-schema one isn't
        self.assertNotIn("mgr/selftest/kvitem", seen_keys)

        # Restore previous configuration
        for subsys, key, value in stash:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                    "config", "set", subsys, key, value)

    def test_selftest_command_spam(self):
        # Use the selftest module to stress the mgr daemon
        self._load_module("selftest")

        # Use the dashboard to test that the mgr is still able to do its job
        self._assign_ports("dashboard", "server_port")
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

