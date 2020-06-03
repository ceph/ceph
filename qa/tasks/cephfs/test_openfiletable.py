import time
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase, for_teuthology

class OpenFileTable(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

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

        # Hold the file open
        file_count = 8
        for i in range(0, file_count):
            filename = "open_file{}".format(i)
            p = self.mount_a.open_background(filename)
            self.mount_a.write_n_mb(filename, size_mb)

        time.sleep(10)

        """
        With osd_deep_scrub_large_omap_object_key_threshold value as 5 and
        opening 8 files we should have a new rados object with name
        mds0_openfiles.1 to hold the extra keys.
        """

        stat_out = self.fs.rados(["stat", "mds0_openfiles.1"])

        # Now close the file
        self.mount_a.kill_background(p)
