import json
import logging

from .mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestStatus(MgrTestCase):
    MGRS_REQUIRED = 1

    def setUp(self):
        super(TestStatus, self).setUp()
        self.setup_mgrs()

    def test_osd_status(self):
        """
        That `ceph osd status` command functions.
        """

        s = self.mgr_cluster.mon_manager.raw_cluster_cmd("osd", "status")
        self.assertTrue("exists,up" in s)

        osd0 = json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd("osd", "status", "--format=json-pretty"))[0]
        self.assertTrue('id' in osd0)
        self.assertTrue('hostname' in osd0)
        self.assertTrue('used' in osd0)
        self.assertTrue('avail' in osd0)
        self.assertTrue('wr_ops' in osd0)
        self.assertTrue('wr_data' in osd0)
        self.assertTrue('rd_ops' in osd0)
        self.assertTrue('rd_data' in osd0)
        self.assertTrue('state' in osd0)
        self.assertEqual(osd0['state'], 'exists,up')

        osd0 = json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd("osd", "status", "--format=json"))[0]
        self.assertTrue('id' in osd0)
        self.assertTrue('hostname' in osd0)
        self.assertTrue('used' in osd0)
        self.assertTrue('avail' in osd0)
        self.assertTrue('wr_ops' in osd0)
        self.assertTrue('wr_data' in osd0)
        self.assertTrue('rd_ops' in osd0)
        self.assertTrue('rd_data' in osd0)
        self.assertTrue('state' in osd0)
        self.assertEqual(osd0['state'], 'exists,up')
