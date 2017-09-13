
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

    def _selftest_plugin(self, plugin_name):
        initial_gid = self.mgr_cluster.get_mgr_map()['active_gid']
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable",
                                         plugin_name)

        # Wait for the module to load
        def has_restarted():
            map = self.mgr_cluster.get_mgr_map()
            return map['active_gid'] != initial_gid and map['available']
        self.wait_until_true(has_restarted, timeout=30)

        # Execute the module's self-test routine
        self.mgr_cluster.mon_manager.raw_cluster_cmd(plugin_name, "self-test")

    def test_zabbix(self):
        self._selftest_plugin("zabbix")

    def test_prometheus(self):
        self._selftest_plugin("influx")

    def test_influx(self):
        self._selftest_plugin("prometheus")
