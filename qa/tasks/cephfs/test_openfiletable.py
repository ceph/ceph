import time
import logging
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class OpenFileTable(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _check_oft_counter(self, name, count):
        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        if perf_dump['oft'][name] == count:
            return True
        return False

    def test_max_items_per_obj(self):
        """
        The maximum number of openfiles omap objects keys are now equal to
        osd_deep_scrub_large_omap_object_key_threshold option.
        """
        self.set_conf("mds", "osd_deep_scrub_large_omap_object_key_threshold", "5")

        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        # Write some bytes to a file
        size_mb = 1

        # Hold the files open
        file_count = 8
        for i in range(0, file_count):
            filename = "open_file{}".format(i)
            self.mount_a.open_background(filename)
            self.mount_a.write_n_mb(filename, size_mb)

        time.sleep(10)

        """
        With osd_deep_scrub_large_omap_object_key_threshold value as 5 and
        opening 8 files we should have a new rados object with name
        mds0_openfiles.1 to hold the extra keys.
        """

        self.fs.radosm(["stat", "mds0_openfiles.1"])

        # Now close the files
        self.mount_a.kill_background()

    def test_perf_counters(self):
        """
        Opening a file should increment omap_total_updates by 1.
        """

        self.set_conf("mds", "osd_deep_scrub_large_omap_object_key_threshold", "1")
        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        omap_total_updates_0 = perf_dump['oft']['omap_total_updates']
        log.info("omap_total_updates_0:{}".format(omap_total_updates_0))
        
        # Open the file
        p = self.mount_a.open_background("omap_counter_test_file")
        self.wait_until_true(lambda: self._check_oft_counter('omap_total_updates', 2), timeout=120)
        
        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        omap_total_updates_1 = perf_dump['oft']['omap_total_updates']
        log.info("omap_total_updates_1:{}".format(omap_total_updates_1))
        
        self.assertTrue((omap_total_updates_1 - omap_total_updates_0) == 2)
        
        # Now close the file
        self.mount_a.kill_background(p)
        # Ensure that the file does not exist any more
        self.wait_until_true(lambda: self._check_oft_counter('omap_total_removes', 1), timeout=120)
        self.wait_until_true(lambda: self._check_oft_counter('omap_total_kv_pairs', 1), timeout=120)

        perf_dump = self.fs.mds_asok(['perf', 'dump'])
        omap_total_removes = perf_dump['oft']['omap_total_removes']
        omap_total_kv_pairs = perf_dump['oft']['omap_total_kv_pairs']
        log.info("omap_total_removes:{}".format(omap_total_removes))
        log.info("omap_total_kv_pairs:{}".format(omap_total_kv_pairs))
        self.assertTrue(omap_total_removes == 1)
        self.assertTrue(omap_total_kv_pairs == 1)
