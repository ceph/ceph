import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import ObjectNotFound

log = logging.getLogger(__name__)

class TestBacktrace(CephFSTestCase):
    def test_backtrace(self):
        """
        That the 'parent' 'layout' and 'symlink' xattrs on the head objects of files
        are updated correctly.
        """

        old_data_pool_name = self.fs.get_data_pool_name()
        old_pool_id = self.fs.get_data_pool_id()

        # Not enabling symlink recovery option should not store symlink xattr
        self.config_set('mds', 'mds_symlink_recovery', 'false')
        self.mount_a.run_shell(["mkdir", "sym_dir0"])
        self.mount_a.run_shell(["touch", "sym_dir0/file1"])
        self.mount_a.run_shell(["ln", "-s", "sym_dir0/file1", "sym_dir0/symlink_file1"])
        file_ino = self.mount_a.path_to_ino("sym_dir0/symlink_file1", follow_symlinks=False)

        self.fs.mds_asok(["flush", "journal"])
        with self.assertRaises(ObjectNotFound):
            self.fs.read_symlink(file_ino)

        # Enabling symlink recovery option should store symlink xattr for symlinks
        self.config_set('mds', 'mds_symlink_recovery', 'true')
        self.mount_a.run_shell(["mkdir", "sym_dir"])
        self.mount_a.run_shell(["touch", "sym_dir/file1"])
        self.mount_a.run_shell(["ln", "-s", "./file1", "sym_dir/symlink_file1"])
        file_ino = self.mount_a.path_to_ino("sym_dir/symlink_file1", follow_symlinks=False)

        self.fs.mds_asok(["flush", "journal"])
        symlink = self.fs.read_symlink(file_ino)
        self.assertEqual(symlink, {
            "s" : "./file1",
            })

        # Create a file for subsequent checks
        self.mount_a.run_shell(["mkdir", "parent_a"])
        self.mount_a.run_shell(["touch", "parent_a/alpha"])
        file_ino = self.mount_a.path_to_ino("parent_a/alpha")

        # That backtrace and layout are written after initial flush
        self.fs.mds_asok(["flush", "journal"])
        backtrace = self.fs.read_backtrace(file_ino)
        self.assertEqual(['alpha', 'parent_a'], [a['dname'] for a in backtrace['ancestors']])
        layout = self.fs.read_layout(file_ino)
        self.assertDictEqual(layout, {
            "stripe_unit": 4194304,
            "stripe_count": 1,
            "object_size": 4194304,
            "pool_id": old_pool_id,
            "pool_ns": "",
        })
        self.assertEqual(backtrace['pool'], old_pool_id)

        # That backtrace is written after parentage changes
        self.mount_a.run_shell(["mkdir", "parent_b"])
        self.mount_a.run_shell(["mv", "parent_a/alpha", "parent_b/alpha"])

        self.fs.mds_asok(["flush", "journal"])
        backtrace = self.fs.read_backtrace(file_ino)
        self.assertEqual(['alpha', 'parent_b'], [a['dname'] for a in backtrace['ancestors']])

        # Create a new data pool
        new_pool_name = "data_new"
        new_pool_id = self.fs.add_data_pool(new_pool_name)

        # That an object which has switched pools gets its backtrace updated
        self.mount_a.setfattr("./parent_b/alpha",
                              "ceph.file.layout.pool", new_pool_name)
        self.fs.mds_asok(["flush", "journal"])
        backtrace_old_pool = self.fs.read_backtrace(file_ino, pool=old_data_pool_name)
        self.assertEqual(backtrace_old_pool['pool'], new_pool_id)
        backtrace_new_pool = self.fs.read_backtrace(file_ino, pool=new_pool_name)
        self.assertEqual(backtrace_new_pool['pool'], new_pool_id)
        new_pool_layout = self.fs.read_layout(file_ino, pool=new_pool_name)
        self.assertEqual(new_pool_layout['pool_id'], new_pool_id)
        self.assertEqual(new_pool_layout['pool_ns'], '')

        # That subsequent linkage changes are only written to new pool backtrace
        self.mount_a.run_shell(["mkdir", "parent_c"])
        self.mount_a.run_shell(["mv", "parent_b/alpha", "parent_c/alpha"])
        self.fs.mds_asok(["flush", "journal"])
        backtrace_old_pool = self.fs.read_backtrace(file_ino, pool=old_data_pool_name)
        self.assertEqual(['alpha', 'parent_b'], [a['dname'] for a in backtrace_old_pool['ancestors']])
        backtrace_new_pool = self.fs.read_backtrace(file_ino, pool=new_pool_name)
        self.assertEqual(['alpha', 'parent_c'], [a['dname'] for a in backtrace_new_pool['ancestors']])

        # That layout is written to new pool after change to other field in layout
        self.mount_a.setfattr("./parent_c/alpha",
                              "ceph.file.layout.object_size", "8388608")

        self.fs.mds_asok(["flush", "journal"])
        new_pool_layout = self.fs.read_layout(file_ino, pool=new_pool_name)
        self.assertEqual(new_pool_layout['object_size'], 8388608)

        # ...but not to the old pool: the old pool's backtrace points to the new pool, and that's enough,
        # we don't update the layout in all the old pools whenever it changes
        old_pool_layout = self.fs.read_layout(file_ino, pool=old_data_pool_name)
        self.assertEqual(old_pool_layout['object_size'], 4194304)

    def test_backtrace_flush_on_deleted_data_pool(self):
        """
        that the MDS does not go read-only when handling backtrace update errors
        when backtrace updates are batched and flushed to RADOS (during journal trim)
        and some of the pool have been removed.
        """
        data_pool = self.fs.get_data_pool_name()
        extra_data_pool_name_1 = data_pool + '_extra1'
        self.fs.add_data_pool(extra_data_pool_name_1)

        self.mount_a.run_shell(["mkdir", "dir_x"])
        self.mount_a.setfattr("dir_x", "ceph.dir.layout.pool", extra_data_pool_name_1)
        self.mount_a.run_shell(["touch", "dir_x/file_x"])
        self.fs.flush()

        extra_data_pool_name_2 = data_pool + '_extra2'
        self.fs.add_data_pool(extra_data_pool_name_2)
        self.mount_a.setfattr("dir_x/file_x", "ceph.file.layout.pool", extra_data_pool_name_2)
        self.mount_a.run_shell(["setfattr", "-x", "ceph.dir.layout", "dir_x"])
        self.run_ceph_cmd("fs", "rm_data_pool", self.fs.name, extra_data_pool_name_1)
        self.fs.flush()

        # quick test to check if the mds has handled backtrace update failure
        # on the deleted data pool without going read-only.
        self.mount_a.run_shell(["mkdir", "dir_y"])

    def _mds_log(self, key):
        return self.fs.mds_asok(['perf', 'dump', 'mds_log'])['mds_log'][key]

    def _start_new_segment(self):
        segadd = self._mds_log('segadd')
        while self._mds_log('segadd') == segadd:
            self.mount_a.run_shell(["touch", f"filler/f{next(self._filler)}"])

    def _trim_journal_past(self, pos):
        """
        Start new segments until the MDS has trimmed the journal past pos.
        The MDS expires the oldest segment only while it holds more than
        mds_log_max_segments, so add one segment at a time, and only when
        none is waiting to expire.
        """
        max_segments = int(self.fs.mds_asok(
            ['config', 'get', 'mds_log_max_segments'])['mds_log_max_segments'])
        def trimmed():
            perf = self.fs.mds_asok(['perf', 'dump', 'mds_log'])['mds_log']
            if perf['expos'] > pos:
                return True
            if perf['seg'] - perf['segexg'] - perf['segexd'] <= max_segments:
                self._start_new_segment()
            return False
        self.wait_until_true(trimmed, timeout=120, period=1)

    def _setup_old_pool(self):
        data_pool = self.fs.get_data_pool_name()
        extra_data_pool_name = data_pool + '_extra'
        extra_pool_id = self.fs.add_data_pool(extra_data_pool_name)

        self.mount_a.run_shell(["mkdir", "dir_x", "dir_y", "filler"])
        self.mount_a.setfattr("dir_x", "ceph.dir.layout.pool", extra_data_pool_name)

        # Replay starts at a major segment, so the MDS trims its journal
        # only up to one.  With mds_debug_subtrees, which vstart.sh turns on
        # and the fs suites don't, every request starts a major segment, so
        # the journal can be trimmed between the create and the later
        # update.  Without early replies, a request's update is journaled
        # and applied before the client sees the reply; sync doesn't wait
        # for that with ceph-fuse.  Settings made on the running MDS apply
        # at once and override ceph.conf, where vstart_runner sets
        # mds_log_max_segments.
        self.fs.mds_asok(['config', 'set', 'mds_debug_subtrees', 'true'])
        self.fs.mds_asok(['config', 'set', 'mds_early_reply', 'false'])
        self._filler = iter(range(10000))
        return data_pool, extra_pool_id

    def _set_max_segments(self, n):
        self.fs.mds_asok(['config', 'set', 'mds_log_max_segments', str(n)])

    def _lower_max_segments(self, target):
        """
        Lower mds_log_max_segments to target in steps, so that the journal
        never holds more than twice the limit: past that the MDS warns
        MDS_TRIM, and "mds fail" refuses.
        """
        while True:
            n = max(target, self._mds_log('seg') // 2 + 1)
            self._set_max_segments(n)
            if n == target:
                return
            self.wait_until_true(lambda: self._mds_log('seg') <= n,
                                 timeout=60, period=1)

    def _old_pool_window(self, create, later_op, attempts=3):
        """
        Make a file with create(i), which returns its path, run later_op(i)
        in a later journal segment, and trim the journal past the create,
        so that only later_op's update is left for replay.  Nothing is
        trimmed until later_op is journaled, so the create's segment can't
        expire first, and a few segments in between let the trimming stop
        short of later_op's segment.  If the MDS still stores the backtrace
        before the failover, there's nothing to test: check that the inode
        is still dirtyparent and dirtypool, and start over with a new file
        if it is not.

        Return (i, ino) of the file to test.
        """
        for i in range(attempts):
            self._set_max_segments(128)
            self._start_new_segment()
            path = create(i)
            ino = self.mount_a.path_to_ino(path)
            self.mount_a.run_shell(["sync"])
            create_end = self._mds_log('wrpos')

            for _ in range(4):
                self._start_new_segment()
            later_op(i)
            self.mount_a.run_shell(["sync"])

            # 8 is the minimum since squid
            self._lower_max_segments(8)
            self._trim_journal_past(create_end)
            states = self.fs.mds_asok(['dump', 'inode', hex(ino)])['states']
            if 'dirtyparent' in states and 'dirtypool' in states:
                return i, ino
            log.info(f"backtrace of {ino:#x} stored before the failover "
                     f"(states {states}), trying again with a new file")
        self.fail(f"backtrace stored before the failover in all {attempts} attempts")

    def _cold_lookup(self, path, data):
        """
        Read path with a cold MDS cache, so that the MDS finds the inode by
        the backtrace in the first data pool, and check that nothing was
        recorded as damaged.
        """
        self.mount_a.umount_wait()
        self.fs.rank_fail()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()
        self.assertEqual(self.mount_a.read_file(path), data)
        self.assertEqual(self.fs.mds_asok(["damage", "ls"]), [])

    def test_backtrace_old_pool_after_rename_and_replay(self):
        """
        That a file created in a non-default data pool still gets its
        backtrace in the default data pool when it is renamed in a later
        journal segment and the MDS fails over after the create's segment
        is trimmed but before the rename's segment expires.
        """
        data_pool, extra_pool_id = self._setup_old_pool()

        def create(i):
            self.mount_a.run_shell(["touch", f"dir_x/file_{i}.tmp"])
            return f"dir_x/file_{i}.tmp"

        def rename(i):
            # The rename journals the backtrace update in a later segment
            self.mount_a.run_shell(["mv", f"dir_x/file_{i}.tmp", f"dir_x/file_{i}"])

        i, file_ino = self._old_pool_window(create, rename)

        self.fs.rank_fail()
        self.fs.wait_for_daemons()
        self.fs.mds_asok(["flush", "journal"])

        backtrace = self.fs.read_backtrace(file_ino, pool=data_pool)
        self.assertEqual(backtrace['pool'], extra_pool_id)
        self.assertEqual([f'file_{i}', 'dir_x'], [a['dname'] for a in backtrace['ancestors']])

    def test_backtrace_old_pool_after_rename_with_hardlink(self):
        """
        That a file in a non-default data pool, hard linked and renamed in a
        later journal segment, can still be opened through the hard link
        with a cold cache after the MDS replays that segment.
        """
        data_pool, extra_pool_id = self._setup_old_pool()

        def create(i):
            self.mount_a.write_file(f"dir_x/file_{i}.tmp", "data")
            return f"dir_x/file_{i}.tmp"

        def link_and_rename(i):
            self.mount_a.run_shell(["ln", f"dir_x/file_{i}.tmp", f"dir_y/lnk_{i}"])
            self.mount_a.run_shell(["mv", f"dir_x/file_{i}.tmp", f"dir_x/file_{i}"])

        i, file_ino = self._old_pool_window(create, link_and_rename)

        self.fs.rank_fail()
        self.fs.wait_for_daemons()
        self.fs.mds_asok(["flush", "journal"])

        backtrace = self.fs.read_backtrace(file_ino, pool=data_pool)
        self.assertEqual(backtrace['pool'], extra_pool_id)
        self.assertEqual([f'file_{i}', 'dir_x'], [a['dname'] for a in backtrace['ancestors']])
        self._cold_lookup(f"dir_y/lnk_{i}", "data")

    def test_backtrace_old_pool_after_unlink_to_stray(self):
        """
        That a hard-linked file in a non-default data pool, whose primary
        link is removed in a later journal segment, keeps its backtrace in
        the default data pool after the MDS replays that segment, so that
        the remaining hard link can be opened with a cold cache.
        """
        data_pool, extra_pool_id = self._setup_old_pool()

        def create(i):
            self.mount_a.write_file(f"dir_x/file_{i}", "data")
            self.mount_a.run_shell(["ln", f"dir_x/file_{i}", f"dir_y/lnk_{i}"])
            return f"dir_x/file_{i}"

        def unlink_primary(i):
            self.mount_a.run_shell(["rm", f"dir_x/file_{i}"])

        i, file_ino = self._old_pool_window(create, unlink_primary)

        self.fs.rank_fail()
        self.fs.wait_for_daemons()
        self.fs.mds_asok(["flush", "journal"])

        backtrace = self.fs.read_backtrace(file_ino, pool=data_pool)
        self.assertEqual(backtrace['pool'], extra_pool_id)
        self._cold_lookup(f"dir_y/lnk_{i}", "data")
