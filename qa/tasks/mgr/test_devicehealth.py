from io import StringIO
import logging

from .mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestDeviceHealth(MgrTestCase):
    MGRS_REQUIRED = 1

    def setUp(self):
        super(TestDeviceHealth, self).setUp()
        self.setup_mgrs()

    def tearDown(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd('mgr', 'set', 'down', 'true')
        self.mgr_cluster.mon_manager.raw_cluster_cmd('config', 'set', 'mon', 'mon_allow_pool_delete', 'true')
        self.mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'pool', 'rm', '.mgr', '.mgr', '--yes-i-really-really-mean-it-not-faking')
        self.mgr_cluster.mon_manager.raw_cluster_cmd('mgr', 'set', 'down', 'false')

    def test_legacy_upgrade_snap(self):
        """
        """

        o = "ABC_DEADB33F_FA"
        self.mon_manager.do_rados(["put", o, "-"], pool=".mgr", stdin=StringIO("junk"))
        self.mon_manager.do_rados(["mksnap", "foo"], pool=".mgr")
        self.mon_manager.do_rados(["rm", o], pool=".mgr")
        self.mgr_cluster.mgr_fail()

        with self.assert_cluster_log("Unhandled exception from module 'devicehealth' while running", present=False):
            self.wait_until_true(lambda: self.mgr_cluster.get_active_id() is not None, timeout=60)
