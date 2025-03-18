import logging
import time

log = logging.getLogger(__name__)

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import ObjectNotFound

class TestReferentInode(CephFSTestCase):
    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1
    ALLOW_REFERENT_INODES = True

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

    def test_referent_link(self):
        """
        test_referent_link - Test creation of referent inode and backtrace on link
        """

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

    def test_referent_unlink(self):
        """
        test_referent_unlink - Test deletion of referent inode and backtrace on unlink
        """

        self.mount_a.run_shell(["mkdir", "dir0"])
        self.mount_a.run_shell(["touch", "dir0/file1"])
        self.mount_a.run_shell(["ln", "dir0/file1", "dir0/hardlink_file1"])
        file_ino = self.mount_a.path_to_ino("dir0/file1")

        # write out the backtrace - this would writeout the backtrace
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

        # unlink
        self.mount_a.run_shell(["rm", "-f", "dir0/hardlink_file1"])
        self.fs.mds_asok(["flush", "journal"])
        # referent inode should be removed from the real inode
        # in-memory
        file1_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(file_ino)])
        self.assertNotIn(referent_inode['ino'], file1_inode_dump['referent_inodes'])
        # on-disk
        file1_inode = self.fs.read_meta_inode(dir_ino, "file1")
        self.assertNotIn(referent_inode['ino'], file1_inode['referent_inodes'])
        # referent inode object from data pool should be deleted
        with self.assertRaises(ObjectNotFound):
            self.fs.read_backtrace(referent_inode['ino'])
        # omap value of referent inode from metadata pool should be deleted
        with self.assertRaises(ObjectNotFound):
            self.fs.read_meta_inode(dir_ino, "hardlink_file1")

    def test_hardlink_reintegration_with_referent(self):
        """
        test_hardlink_reintegration_with_referent - That removal of primary dentry
        of hardlinked inode results in reintegration of inode into the previously
        referent remote dentry, rather than lingering as a stray indefinitely.
        """

        # Write some bytes to file_a
        size_mb = 8
        self.mount_a.run_shell(["mkdir", "dir_1"])
        self.mount_a.write_n_mb("dir_1/file_a", size_mb)
        ino = self.mount_a.path_to_ino("dir_1/file_a")

        # Create a hardlink named file_b
        self.mount_a.run_shell(["mkdir", "dir_2"])
        self.mount_a.run_shell(["ln", "dir_1/file_a", "dir_2/file_b"])
        self.assertEqual(self.mount_a.path_to_ino("dir_2/file_b"), ino)

        # Flush journal
        self.fs.mds_asok(['flush', 'journal'])

        # Validate referent_inodes list
        # read the primary inode
        dir_1_ino = self.mount_a.path_to_ino("dir_1")
        file_a_inode = self.fs.read_meta_inode(dir_1_ino, "file_a")
        # read the hardlink referent inode
        dir_2_ino = self.mount_a.path_to_ino("dir_2")
        file_b_inode = self.fs.read_meta_inode(dir_2_ino, "file_b")
        self.assertIn(file_b_inode['ino'], file_a_inode['referent_inodes'])
        # link - the referent inode should point to real inode
        self.assertEqual(file_b_inode['remote_ino'], ino)

        # See that backtrace for the file points to the file_a path
        pre_unlink_bt = self.fs.read_backtrace(ino)
        self.assertEqual(pre_unlink_bt['ancestors'][0]['dname'], "file_a")

        # empty mds cache. otherwise mds reintegrates stray when unlink finishes
        self.mount_a.umount_wait()
        self.fs.mds_asok(['flush', 'journal'])
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        # Unlink file_a
        self.mount_a.run_shell(["rm", "-f", "dir_1/file_a"]) #strays_created=1

        # See that a stray was created
        self.assertEqual(self.get_mdc_stat("num_strays"), 1)
        self.assertEqual(self.get_mdc_stat("strays_created"), 1)

        # Wait, see that data objects are still present (i.e. that the
        # stray did not advance to purging given time)
        time.sleep(30)
        self.assertTrue(self.fs.data_objects_present(ino, size_mb * 1024 * 1024))
        self.assertEqual(self.get_mdc_stat("strays_enqueued"), 0)

        # See that before reintegration, the inode's backtrace points to a stray dir
        self.fs.mds_asok(['flush', 'journal'])
        self.assertTrue(self.get_backtrace_path(ino).startswith("stray"))

        last_reintegrated = self.get_mdc_stat("strays_reintegrated")

        # Do a metadata operation on the remaining link (mv is heavy handed, but
        # others like touch may be satisfied from caps without poking MDS)
        self.mount_a.run_shell(["mv", "dir_2/file_b", "dir_2/file_c"])

        #strays_created=2, after reintegration

        # Stray reintegration should happen as a result of the eval_remote call
        # on responding to a client request.
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0,
            timeout=60
        )

        # See the reintegration counter increment
        curr_reintegrated = self.get_mdc_stat("strays_reintegrated")
        self.assertGreater(curr_reintegrated, last_reintegrated)
        last_reintegrated = curr_reintegrated

        # Flush the journal
        self.fs.mds_asok(['flush', 'journal'])

        # Validate for empty referent_inodes list after reintegration
        file_c_inode = self.fs.read_meta_inode(dir_2_ino, "file_c")
        self.assertEqual(file_c_inode['referent_inodes'], [])

        # See that the backtrace for the file points to the remaining link's path
        post_reint_bt = self.fs.read_backtrace(ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "file_c")

        # mds should reintegrates stray when unlink finishes
        self.mount_a.run_shell(["ln", "dir_2/file_c", "dir_2/file_d"])

        # validate in-memory referent inodes list for non-emptiness
        file_c_ino = self.mount_a.path_to_ino("dir_2/file_c")
        file_c_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(file_c_ino)])
        self.assertTrue(file_c_inode_dump['referent_inodes'])

        # mds should reintegrates stray when unlink finishes
        self.mount_a.run_shell(["rm", "-f", "dir_2/file_c"])

        #strays_created=4 (primary unlink=1, reintegration=1 referent file_d goes stray)

        # Stray reintegration should happen as a result of the notify_stray call
        # on completion of unlink
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0,
            timeout=60
        )

        # See the reintegration counter increment
        curr_reintegrated = self.get_mdc_stat("strays_reintegrated")
        self.assertGreater(curr_reintegrated, last_reintegrated)
        last_reintegrated = curr_reintegrated

        # validate in-memory referent inodes list for emptiness
        file_d_ino = self.mount_a.path_to_ino("dir_2/file_d")
        file_d_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(file_d_ino)])
        self.assertEqual(file_d_inode_dump['referent_inodes'], [])

        # Flush the journal
        self.fs.mds_asok(['flush', 'journal'])

        # See that the backtrace for the file points to the newest link's path
        post_reint_bt = self.fs.read_backtrace(ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "file_d")

        # Now really delete it
        self.mount_a.run_shell(["rm", "-f", "dir_2/file_d"])
        # strays_created=5, after unlink
        self._wait_for_counter("mds_cache", "strays_enqueued", 3)
        self._wait_for_counter("purge_queue", "pq_executed", 3)

        self.assert_purge_idle()
        self.assertTrue(self.fs.data_objects_absent(ino, size_mb * 1024 * 1024))

        # We caused the inode to go stray 5 times. Follow the comments above for the count
        self.assertEqual(self.get_mdc_stat("strays_created"), 5)
        # We purged it at the last (1 primary, 2 hardlinks with referent)
        self.assertEqual(self.get_mdc_stat("strays_enqueued"), 3)

        # Flush the journal
        self.fs.mds_asok(['flush', 'journal'])
        # All data objects include referent inodes should go away
        self.assertEqual(self.fs.list_data_objects(), [''])

    def test_mv_hardlink_cleanup_with_referent(self):
        """
        test_mv_hardlink_cleanup_with_referent - That when doing a rename from A to B, and B
        has hardlinks, then we make a stray for B which is then reintegrated into one of it's
        hardlinks with referent inodes.
        """
        # Create file_a, file_b, and a hardlink to file_b
        size_mb = 8
        self.mount_a.write_n_mb("file_a", size_mb)
        file_a_ino = self.mount_a.path_to_ino("file_a")

        self.mount_a.write_n_mb("file_b", size_mb)
        file_b_ino = self.mount_a.path_to_ino("file_b")

        # empty referent_inodes list for file_b
        file_b_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(file_b_ino)])
        self.assertEqual(file_b_inode_dump['referent_inodes'], [])

        self.mount_a.run_shell(["ln", "file_b", "linkto_b"])
        self.assertEqual(self.mount_a.path_to_ino("linkto_b"), file_b_ino)

        # linkto_b's referent inode got added to referent_inodes list of file_b
        # flush the journal
        self.fs.mds_asok(['flush', 'journal'])
        file_b_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(file_b_ino)])
        self.assertTrue(file_b_inode_dump['referent_inodes'])
        referent_ino_linkto_b = file_b_inode_dump['referent_inodes'][0]
        linkto_b_referent_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(referent_ino_linkto_b)])
        self.assertEqual(linkto_b_referent_inode_dump['remote_ino'], file_b_ino )

        # mv file_a file_b
        self.mount_a.run_shell(["mv", "file_a", "file_b"])

        # Stray reintegration should happen as a result of the notify_stray call on
        # completion of rename
        self.wait_until_equal(
            lambda: self.get_mdc_stat("num_strays"),
            expect_val=0,
            timeout=60
        )

        self.assertEqual(self.get_mdc_stat("strays_created"), 2)
        self.assertGreaterEqual(self.get_mdc_stat("strays_reintegrated"), 1)

        # No data objects should have been deleted, as both files still have linkage.
        self.assertTrue(self.fs.data_objects_present(file_a_ino, size_mb * 1024 * 1024))
        self.assertTrue(self.fs.data_objects_present(file_b_ino, size_mb * 1024 * 1024))

        # After stray reintegration, referent inode should be removed from list
        linkto_b_ino = self.mount_a.path_to_ino("linkto_b")
        linkto_b_inode_dump = self.fs.mds_asok(['dump', 'inode', hex(linkto_b_ino)])
        self.assertEqual(linkto_b_inode_dump['referent_inodes'], [])
        # referent ino should be deleted
        self.assertTrue(self.fs.data_objects_absent(referent_ino_linkto_b, size_mb * 1024 * 1024))

        self.fs.mds_asok(['flush', 'journal'])

        post_reint_bt = self.fs.read_backtrace(file_b_ino)
        self.assertEqual(post_reint_bt['ancestors'][0]['dname'], "linkto_b")

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
