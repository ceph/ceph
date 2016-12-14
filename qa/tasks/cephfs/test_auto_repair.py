
"""
Exercise the MDS's auto repair functions
"""

import logging
import time

from teuthology.orchestra.run import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase


log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60


class TestMDSAutoRepair(CephFSTestCase):
    def test_backtrace_repair(self):
        """
        MDS should verify/fix backtrace on fetch dirfrag
        """

        self.mount_a.run_shell(["mkdir", "testdir1"])
        self.mount_a.run_shell(["touch", "testdir1/testfile"])
        dir_objname = "{:x}.00000000".format(self.mount_a.path_to_ino("testdir1"))

        # drop inodes caps
        self.mount_a.umount_wait()

        # flush journal entries to dirfrag objects, and expire journal
        self.fs.mds_asok(['flush', 'journal'])

        # Restart the MDS to drop the metadata cache (because we expired the journal,
        # nothing gets replayed into cache on restart)
        self.fs.mds_stop()
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        # remove testdir1's backtrace
        self.fs.rados(["rmxattr", dir_objname, "parent"])

        # readdir (fetch dirfrag) should fix testdir1's backtrace
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        self.mount_a.run_shell(["ls", "testdir1"])

        # flush journal entries to dirfrag objects
        self.fs.mds_asok(['flush', 'journal'])

        # check if backtrace exists
        self.fs.rados(["getxattr", dir_objname, "parent"])

    def test_mds_readonly(self):
        """
        test if MDS behave correct when it's readonly
        """
        # operation should successd when MDS is not readonly
        self.mount_a.run_shell(["touch", "test_file1"])
        writer = self.mount_a.write_background(loop=True)

        time.sleep(10)
        self.assertFalse(writer.finished)

        # force MDS to read-only mode
        self.fs.mds_asok(['force_readonly'])
        time.sleep(10)

        # touching test file should fail
        try:
            self.mount_a.run_shell(["touch", "test_file1"])
        except CommandFailedError:
            pass
        else:
            self.assertTrue(False)

        # background writer also should fail
        self.assertTrue(writer.finished)

        # The MDS should report its readonly health state to the mon
        self.wait_for_health("MDS in read-only mode", timeout=30)

        # restart mds to make it writable
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        self.wait_for_health_clear(timeout=30)
