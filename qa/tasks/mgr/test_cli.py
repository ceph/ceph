import logging

from .mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestCLI(MgrTestCase):
    MGRS_REQUIRED = 2

    def setUp(self):
        super(TestCLI, self).setUp()
        self.setup_mgrs()

    def test_set_down(self):
        """
        That setting the down flag prevents a standby from promoting.
        """

        with self.assert_cluster_log("Activating manager daemon", present=False):
            self.mgr_cluster.mon_manager.raw_cluster_cmd('mgr', 'set', 'down', 'true')
            self.wait_until_true(lambda: self.mgr_cluster.get_active_id() == "", timeout=60)

    def test_set_down_off(self):
        """
        That removing the down flag allows a standby to promote.
        """

        with self.assert_cluster_log("Activating manager daemon"):
            self.mgr_cluster.mon_manager.raw_cluster_cmd('mgr', 'set', 'down', 'true')
            self.wait_until_true(lambda: self.mgr_cluster.get_active_id() == "", timeout=60)
            self.mgr_cluster.mon_manager.raw_cluster_cmd('mgr', 'set', 'down', 'false')
