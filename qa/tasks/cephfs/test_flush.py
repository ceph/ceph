
from textwrap import dedent
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import ObjectNotFound, ROOT_INO

class TestFlush(CephFSTestCase):
    def test_flush(self):
        self.mount_a.run_shell(["mkdir", "mydir"])
        self.mount_a.run_shell(["touch", "mydir/alpha"])
        dir_ino = self.mount_a.path_to_ino("mydir")
        file_ino = self.mount_a.path_to_ino("mydir/alpha")

        # Unmount the client so that it isn't still holding caps
        self.mount_a.umount_wait()

        # Before flush, the dirfrag object does not exist
        with self.assertRaises(ObjectNotFound):
            self.fs.list_dirfrag(dir_ino)

        # Before flush, the file's backtrace has not been written
        with self.assertRaises(ObjectNotFound):
            self.fs.read_backtrace(file_ino)

        # Before flush, there are no dentries in the root
        self.assertEqual(self.fs.list_dirfrag(ROOT_INO), [])

        # Execute flush
        flush_data = self.fs.mds_asok(["flush", "journal"])
        self.assertEqual(flush_data['return_code'], 0)

        # After flush, the dirfrag object has been created
        dir_list = self.fs.list_dirfrag(dir_ino)
        self.assertEqual(dir_list, ["alpha_head"])

        # And the 'mydir' dentry is in the root
        self.assertEqual(self.fs.list_dirfrag(ROOT_INO), ['mydir_head'])

        # ...and the data object has its backtrace
        backtrace = self.fs.read_backtrace(file_ino)
        self.assertEqual(['alpha', 'mydir'], [a['dname'] for a in backtrace['ancestors']])
        self.assertEqual([dir_ino, 1], [a['dirino'] for a in backtrace['ancestors']])
        self.assertEqual(file_ino, backtrace['ino'])

        # ...and the journal is truncated to just a single subtreemap from the
        # newly created segment
        self.fs.fail()
        summary_output = self.fs.journal_tool(["event", "get", "summary"], 0)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        try:
            self.assertEqual(summary_output,
                             dedent(
                                 """
                                 Events by type:
                                   SUBTREEMAP: 1
                                 Errors: 0
                                 """
                             ).strip())
        except AssertionError:
            # In some states, flushing the journal will leave you
            # an extra event from locks a client held.   This is
            # correct behaviour: the MDS is flushing the journal,
            # it's just that new events are getting added too.
            # In this case, we should nevertheless see a fully
            # empty journal after a second flush.
            self.assertEqual(summary_output,
                             dedent(
                                 """
                                 Events by type:
                                   SUBTREEMAP: 1
                                   UPDATE: 1
                                 Errors: 0
                                 """
                             ).strip())
            flush_data = self.fs.mds_asok(["flush", "journal"])
            self.assertEqual(flush_data['return_code'], 0)

            self.fs.fail()
            self.assertEqual(self.fs.journal_tool(["event", "get", "summary"], 0),
                             dedent(
                                 """
                                 Events by type:
                                   SUBTREEMAP: 1
                                 Errors: 0
                                 """
                             ).strip())
            self.fs.set_joinable()
            self.fs.wait_for_daemons()

        # Now for deletion!
        # We will count the RADOS deletions and MDS file purges, to verify that
        # the expected behaviour is happening as a result of the purge
        initial_dels = self.fs.mds_asok(['perf', 'dump', 'objecter'])['objecter']['osdop_delete']
        initial_purges = self.fs.mds_asok(['perf', 'dump', 'mds_cache'])['mds_cache']['strays_enqueued']

        # Use a client to delete a file
        self.mount_a.mount_wait()
        self.mount_a.run_shell(["rm", "-rf", "mydir"])

        # Flush the journal so that the directory inode can be purged
        flush_data = self.fs.mds_asok(["flush", "journal"])
        self.assertEqual(flush_data['return_code'], 0)

        # We expect to see a single file purge
        self.wait_until_true(
            lambda: self.fs.mds_asok(['perf', 'dump', 'mds_cache'])['mds_cache']['strays_enqueued'] - initial_purges >= 2,
            60)

        # We expect two deletions, one of the dirfrag and one of the backtrace
        self.wait_until_true(
            lambda: self.fs.mds_asok(['perf', 'dump', 'objecter'])['objecter']['osdop_delete'] - initial_dels >= 2,
            60)  # timeout is fairly long to allow for tick+rados latencies

        with self.assertRaises(ObjectNotFound):
            self.fs.list_dirfrag(dir_ino)
        with self.assertRaises(ObjectNotFound):
            self.fs.read_backtrace(file_ino)
        self.assertEqual(self.fs.list_dirfrag(ROOT_INO), [])
