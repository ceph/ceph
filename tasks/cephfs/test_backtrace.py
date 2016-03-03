
from tasks.cephfs.cephfs_test_case import CephFSTestCase


class TestBacktrace(CephFSTestCase):
    def test_backtrace(self):
        """
        That the 'parent' and 'layout' xattrs on the head objects of files
        are updated correctly.
        """

        def get_pool_id(name):
            return self.fs.mon_manager.get_pool_dump(name)['pool']

        old_data_pool_name = self.fs.get_data_pool_name()
        old_pool_id = get_pool_id(old_data_pool_name)

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
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', new_pool_name,
                                            self.fs.get_pgs_per_fs_pool().__str__())
        self.fs.mon_manager.raw_cluster_cmd('mds', 'add_data_pool', new_pool_name)
        new_pool_id = get_pool_id(new_pool_name)

        # That an object which has switched pools gets its backtrace updated
        self.mount_a.run_shell(["setfattr", "-n", "ceph.file.layout.pool", "-v", new_pool_name, "./parent_b/alpha"])
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
        self.mount_a.run_shell(["setfattr", "-n", "ceph.file.layout.object_size", "-v", "8388608", "./parent_c/alpha"])

        self.fs.mds_asok(["flush", "journal"])
        new_pool_layout = self.fs.read_layout(file_ino, pool=new_pool_name)
        self.assertEqual(new_pool_layout['object_size'], 8388608)

        # ...but not to the old pool: the old pool's backtrace points to the new pool, and that's enough,
        # we don't update the layout in all the old pools whenever it changes
        old_pool_layout = self.fs.read_layout(file_ino, pool=old_data_pool_name)
        self.assertEqual(old_pool_layout['object_size'], 4194304)
