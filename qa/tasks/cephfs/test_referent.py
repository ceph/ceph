import logging
import time
import os
import signal
import errno

log = logging.getLogger(__name__)

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import ObjectNotFound
from teuthology.exceptions import CommandFailedError

class TestReferentInode(CephFSTestCase):
    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1

    def get_mdc_stat(self, name, mds_id=None):
        return self.get_stat("mds_cache", name, mds_id)

    def get_stat(self, subsys, name, mds_id=None):
        return self.fs.mds_asok(['perf', 'dump', subsys, name],
                                mds_id=mds_id)[subsys][name]

    def _wait_for_counter(self, subsys, counter, expect_val, timeout=60,
                          mds_id=None):
        self.wait_until_equal(
            lambda: self.get_stat(subsys, counter, mds_id),
            expect_val=expect_val, timeout=timeout,
            reject_fn=lambda x: x > expect_val
        )

    def assert_backtrace(self, ino, expected_path):
        """
        Assert that the backtrace in the data pool for an inode matches
        an expected /foo/bar path.
        """
        expected_elements = expected_path.strip("/").split("/")
        bt = self.fs.read_backtrace(ino)
        actual_elements = list(reversed([dn['dname'] for dn in bt['ancestors']]))
        self.assertListEqual(expected_elements, actual_elements)

    def get_backtrace_path(self, ino):
        bt = self.fs.read_backtrace(ino)
        elements = reversed([dn['dname'] for dn in bt['ancestors']])
        return "/".join(elements)

    def assert_purge_idle(self):
        """
        Assert that the MDS perf counters indicate no strays exist and
        no ongoing purge activity.  Sanity check for when PurgeQueue should
        be idle.
        """
        mdc_stats = self.fs.mds_asok(['perf', 'dump', "mds_cache"])['mds_cache']
        pq_stats = self.fs.mds_asok(['perf', 'dump', "purge_queue"])['purge_queue']
        self.assertEqual(mdc_stats["num_strays"], 0)
        self.assertEqual(mdc_stats["num_strays_delayed"], 0)
        self.assertEqual(pq_stats["pq_executing"], 0)
        self.assertEqual(pq_stats["pq_executing_ops"], 0)

    def verify_referent_inode_list_in_memory(self, p_file, referent_ino):
        p_file_ino = self.mount_a.path_to_ino(p_file)
        p_file_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(p_file_ino)])
        referent_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(referent_ino)])
        # referents's remote should point to primary
        self.assertEqual(p_file_inode_dump['ino'], referent_inode_dump['remote_ino'])
        self.assertIn(referent_ino, p_file_inode_dump['referent_inodes'])

    def verify_referent_inode_list_on_disk(self, p_dir, p_file, s_dir, s_file):
        p_dir_ino = self.mount_a.path_to_ino(p_dir)
        primary_inode = self.fs.read_meta_inode(p_dir_ino, p_file)

    def test_rename_primary_to_existing_referent_dentry(self):
        """
        test_rename_primary_to_existing_referent_dentry - Test rename primary to it's existing hardlink file.
        The rename should be noop as it's rename to same ino
        """

        self.mount_a.run_shell(["mkdir", "dir0"])
        self.mount_a.run_shell(["mkdir", "dir1"])
        self.mount_a.write_file('dir0/file1', 'somedata')
        self.mount_a.run_shell(["ln", "dir0/file1", "dir1/hardlink_file1"])
        primary_inode = self.mount_a.path_to_ino('dir0/file1')

        # write out the backtrace - this would writeout the backrace
        # of the newly introduced referent inode to the data pool.
        self.fs.mds_asok(["flush", "journal"])

        # rename hardlink referent dentry, noop
        srcpath = os.path.join(self.mount_a.mountpoint, "dir0", "file1")
        dstpath = os.path.join(self.mount_a.mountpoint, "dir1", "hardlink_file1")
        os.rename(srcpath, dstpath)

        primary_inode_after_rename = self.mount_a.path_to_ino('dir0/file1')
        hardlink_inode = self.mount_a.path_to_ino('dir1/hardlink_file1')
        self.assertEqual(primary_inode_after_rename, primary_inode)
        self.assertEqual(hardlink_inode, primary_inode)
