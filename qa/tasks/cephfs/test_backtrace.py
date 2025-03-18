
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import ObjectNotFound

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
