import logging
import time

log = logging.getLogger(__name__)

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import ObjectNotFound

class TestReferentInode(CephFSTestCase):
    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1

    def test_referent_link(self):
        """
        test_referent_link - Test creation of referent inode and backtrace on link
        """

        self.fs.set_allow_referent_inodes(True);
        self.mount_a.run_shell(["mkdir", "dir0"])
        self.mount_a.run_shell(["touch", "dir0/file1"])
        self.mount_a.run_shell(["ln", "dir0/file1", "dir0/hardlink_file1"])
        file_ino = self.mount_a.path_to_ino("dir0/file1")

        # write out the backtrace - this would writeout the backrace
        # of the newly introduced referent inode to the data pool.
        self.fs.mds_asok(["flush", "journal"])

        # read the primary inode
        dir_ino = self.mount_a.path_to_ino("dir0")
        file1_inode = self.fs.read_meta_inode(dir_ino, "file1")

        # read the referent inode from metadata pool
        referent_inode = self.fs.read_meta_inode(dir_ino, "hardlink_file1")

        self.assertFalse(file1_inode['ino'] == referent_inode['ino'])

        # reverse link - the real inode should track the referent inode number
        self.assertIn(referent_inode['ino'], file1_inode['referent_inodes'])
        # link - the referent inode should point to real inode
        self.assertEqual(referent_inode['remote_ino'], file_ino)

        # backtrace of referent inode from data pool
        backtrace = self.fs.read_backtrace(referent_inode['ino'])
        # path validation
        self.assertEqual(['hardlink_file1', 'dir0'], [a['dname'] for a in backtrace['ancestors']])
        # inode validation
        self.assertEqual(referent_inode['ino'], backtrace['ino'])
        self.assertEqual([dir_ino, 1], [a['dirino'] for a in backtrace['ancestors']])

    def test_hardlink_reintegration_with_referent(self):
        pass

    def test_multiple_referent_post_reintegration(self):
        pass

    def test_rename_a_referent_dentry(self):
        pass

    def test_referent_with_mds_killpoints(self):
        pass

    def test_referent_with_snapshot(self):
        pass

    def test_referent_with_mdlog_replay(self):
        pass

    def test_referent_no_caps(self):
        pass
