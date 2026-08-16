
import json
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

    def _require_mgr_module(self, module_name):
        dump = json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "dump", "--format=json-pretty"))
        mgr_map_dump = dump.get("mgrmap", dump)
        # getting the available modules and checking if the module is in the list
        # and if it can run, if not, skip the test
        for entry in mgr_map_dump["available_modules"]:
            if entry.get("name") == module_name:
                if not entry.get("can_run", True):
                    self.skipTest(entry.get("error_string") or
                                  "%s module cannot run" % module_name)
                return
        raise RuntimeError("module %r not found in mgr dump" % module_name)

    def test_prometheus(self):
        self._assign_ports("prometheus", "server_port", min_port=8100)
        self._selftest_plugin("prometheus")

    def test_influx(self):
        self._require_mgr_module("influx")
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
        self._require_mgr_module("influx")
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

    def test_serve_failure(self):
        """
        That an exception thrown from a module's serve() loop marks the
        module failed and raises a health check, with a test dedicated to
        just this failure point  (tracker #78786).
        """
        self._load_module("selftest")
        self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "self-test", "background", "start", "throw_exception")

        self.wait_for_health(
            "Module 'selftest' has failed: Synthetic exception in serve",
            timeout=30)

        self.mgr_cluster.mon_manager.raw_cluster_cmd("crash", "prune", "0")

    def test_command_handler_failure(self):
        """
        That an exception thrown from a module's command handler marks the
        module failed and raises a health check.
        """
        self._load_module("selftest")

        with self.assertRaises(CommandFailedError) as exc_raised:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "command", "throw")
        self.assertEqual(exc_raised.exception.exitstatus, errno.EINVAL)

        self.wait_for_health(
            "Module 'selftest' has failed: Synthetic exception in "
            "handle_command",
            timeout=30)

    def test_notify_failure(self):
        """
        That an exception thrown from a module's notify() marks the module
        failed and raises a health check.
        """
        self._load_module("selftest")
        self.mgr_cluster.set_module_conf("selftest", "notify_throw", "true")

        # set_module_conf only returns once the mon has the new value --
        # it still needs to propagate mon->mgr before the running module
        # will see it (see test_selftest_config_update above), which is
        # a separate race from the notify() we're about to trigger below.
        def notify_throw_armed():
            val = self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "config", "get", "notify_throw").strip()
            return val == "True"
        self.wait_until_true(notify_throw_armed, timeout=30)

        # Any notification the module is registered for will do -- an OSD
        # map change is one the suite already causes elsewhere. Always
        # unset it again: it's cluster-wide state that outlives this test
        # and will fail wait_for_health_clear() in unrelated later tests
        # otherwise (setup_mgrs() between tests restarts daemons, but
        # doesn't touch OSD flags).
        self.mgr_cluster.mon_manager.raw_cluster_cmd("osd", "set", "noout")
        try:
            self.wait_for_health(
                "Module 'selftest' has failed: Synthetic exception in notify",
                timeout=30)
        finally:
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "osd", "unset", "noout")

        self.mgr_cluster.mon_manager.raw_cluster_cmd("crash", "prune", "0")

    def test_config_notify_failure(self):
        """
        That an exception thrown from a module's config_notify() marks the
        module failed and raises a health check.
        """
        self._load_module("selftest")

        # The config-set itself triggers config_notify() on the running
        # module, no separate trigger step needed.
        self.mgr_cluster.set_module_conf(
            "selftest", "config_notify_throw", "true")

        self.wait_for_health(
            "Module 'selftest' has failed: Synthetic exception in "
            "config_notify",
            timeout=30)

        self.mgr_cluster.mon_manager.raw_cluster_cmd("crash", "prune", "0")


class TestModuleSelftestStandby(MgrTestCase):
    """
    Failure points that only manifest on a standby module: import/load
    failures at daemon startup, and shutdown() failures during
    standby->active promotion (tracker #78786).
    """
    MGRS_REQUIRED = 2

    def setUp(self):
        super(TestModuleSelftestStandby, self).setUp()
        self.setup_mgrs()

    def _get_standbys(self):
        """
        Map of standby name -> full mgrmap standby entry (gid,
        available_modules, ...).
        """
        return {s["name"]: s
                for s in self.mgr_cluster.get_mgr_map()["standbys"]}

    def test_module_load_failure(self):
        """
        That a module which fails to import on disk is recorded as
        unusable in a standby's available_modules, without disturbing the
        rest of the cluster.
        """
        module_name = "_qa_broken_module"

        standby_id = self.mgr_cluster.get_standby_ids()[0]
        original_active = self.mgr_cluster.get_active_id()
        original_gid = self._get_standbys()[standby_id]["gid"]

        module_path = self.mgr_cluster.get_config(
            "mgr_module_path", service_type="mgr")
        remote = self.mgr_cluster.mgr_daemons[standby_id].remote

        module_dir = "{0}/{1}".format(module_path, module_name)
        remote.sudo_write_file(
            "{0}/module.py".format(module_dir), "", mode="0644",
            mkdir=True)
        remote.sudo_write_file(
            "{0}/__init__.py".format(module_dir),
            "raise ImportError(\"qa synthetic module load failure\")\n",
            mode="0644", mkdir=True)

        try:
            self.mgr_cluster.mgr_restart(standby_id)

            # Wait for a genuinely new daemon instance (gid changed), not
            # just for a standby named `standby_id` to be present -- the
            # pre-restart registration can still be visible in the mgrmap
            # for a moment after mgr_restart() returns, and reading its
            # (stale) available_modules races ahead of the new process
            # actually probing the broken module on disk.
            def restarted():
                entry = self._get_standbys().get(standby_id)
                return entry is not None and entry["gid"] != original_gid
            self.wait_until_true(restarted, timeout=30)

            standby_entry = self._get_standbys()[standby_id]
            module_entry = None
            for m in standby_entry["available_modules"]:
                if m["name"] == module_name:
                    module_entry = m
                    break

            self.assertIsNotNone(module_entry)
            self.assertFalse(module_entry["can_run"])
            self.assertTrue(module_entry["error_string"])

            self.assertEqual(
                self.mgr_cluster.get_active_id(), original_active)
            self.wait_for_health_clear(timeout=30)
        finally:
            remote.run(args=["sudo", "rm", "-rf", module_dir])
            self.mgr_cluster.mgr_restart(standby_id)
            self.wait_until_true(
                lambda: standby_id in self.mgr_cluster.get_standby_ids(),
                timeout=30)

    def test_standby_shutdown_throw_marks_failed(self):
        """
        That a standby module whose shutdown() throws is marked failed
        (visible via the freshly-promoted active module), rather than
        silently ignored.
        """
        self.mgr_cluster.set_module_conf(
            "selftest", "shutdown_throw", "true")

        original_active = self.mgr_cluster.get_active_id()
        original_standby_gids = {name: entry["gid"]
                                  for name, entry
                                  in self._get_standbys().items()}
        original_standbys = set(original_standby_gids)

        self._load_module("selftest")

        # _load_module only confirms the active daemon's respawn --
        # standbys respawn independently to pick up the enabled-modules
        # change too, so wait for each one's gid to actually change, not
        # just for a standby with a matching name to reappear (which can
        # be the pre-respawn instance still lingering in the mgrmap, not
        # yet running with shutdown_throw armed).
        def standbys_respawned():
            current = self._get_standbys()
            if set(current) != original_standbys:
                return False
            return all(current[name]["gid"] != original_standby_gids[name]
                       for name in original_standbys)
        self.wait_until_true(standbys_respawned, timeout=30)

        self.mgr_cluster.mgr_fail(original_active)
        self.wait_until_true(
            lambda: self.mgr_cluster.get_active_id() in original_standbys,
            timeout=30)

        self.wait_for_health(
            "Module 'selftest' has failed: Synthetic exception in shutdown",
            timeout=30)

        self.mgr_cluster.mon_manager.raw_cluster_cmd("crash", "prune", "0")

    def test_standby_shutdown_hang_does_not_block_promotion(self):
        """
        Regression test for the daemon-availability bug motivating this
        work: a standby module whose shutdown() hangs must not block the
        standby->active promotion that calls it (tracker #78786).
        """
        self.config_set("mgr", "mgr_module_shutdown_timeout", 3)
        self.mgr_cluster.set_module_conf(
            "selftest", "standby_shutdown_hang", "true")

        original_active = self.mgr_cluster.get_active_id()
        original_standby_gids = {name: entry["gid"]
                                  for name, entry
                                  in self._get_standbys().items()}
        original_standbys = set(original_standby_gids)

        self._load_module("selftest")

        # _load_module only confirms the active daemon's respawn --
        # standbys respawn independently to pick up the enabled-modules
        # change too, and can briefly drop off the standby list or (worse)
        # still be visible under their old, pre-respawn registration, so
        # wait for each one's gid to actually change before triggering
        # promotion -- otherwise the promoted standby might not be running
        # with standby_shutdown_hang armed yet.
        def standbys_respawned():
            current = self._get_standbys()
            if set(current) != original_standbys:
                return False
            return all(current[name]["gid"] != original_standby_gids[name]
                       for name in original_standbys)
        self.wait_until_true(standbys_respawned, timeout=30)

        self.mgr_cluster.mgr_fail(original_active)

        # Deliberately tight: well under the 30s *default*
        # mgr_module_shutdown_timeout. This bound only holds because the
        # call and the join of the hung serve() thread are bounded
        # together -- a fix that only bounded the python call would still
        # hang here, blocked on thread.join().
        self.wait_until_true(
            lambda: self.mgr_cluster.get_active_id() in original_standbys,
            timeout=15)

        self.wait_for_health(
            "Module 'selftest' has failed", timeout=15)
