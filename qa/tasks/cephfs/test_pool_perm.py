from textwrap import dedent
from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase
import os


class TestPoolPerm(CephFSTestCase):
    def test_pool_perm(self):
        self.mount_a.run_shell(["touch", "test_file"])

        file_path = os.path.join(self.mount_a.mountpoint, "test_file")

        remote_script = dedent("""
            import os
            import errno

            fd = os.open("{path}", os.O_RDWR)
            try:
                if {check_read}:
                    ret = os.read(fd, 1024)
                else:
                    os.write(fd, 'content')
            except OSError, e:
                if e.errno != errno.EPERM:
                    raise
            else:
                raise RuntimeError("client does not check permission of data pool")
            """)

        client_name = "client.{0}".format(self.mount_a.client_id)

        # set data pool read only
        self.fs.mon_manager.raw_cluster_cmd_result(
            'auth', 'caps', client_name, 'mds', 'allow', 'mon', 'allow r', 'osd',
            'allow r pool={0}'.format(self.fs.get_data_pool_name()))

        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # write should fail
        self.mount_a.run_python(remote_script.format(path=file_path, check_read=str(False)))

        # set data pool write only
        self.fs.mon_manager.raw_cluster_cmd_result(
            'auth', 'caps', client_name, 'mds', 'allow', 'mon', 'allow r', 'osd',
            'allow w pool={0}'.format(self.fs.get_data_pool_name()))

        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # read should fail
        self.mount_a.run_python(remote_script.format(path=file_path, check_read=str(True)))

    def test_forbidden_modification(self):
        """
        That a client who does not have the capability for setting
        layout pools is prevented from doing so.
        """

        # Set up
        client_name = "client.{0}".format(self.mount_a.client_id)
        new_pool_name = "data_new"
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', new_pool_name,
                                            self.fs.get_pgs_per_fs_pool().__str__())
        self.fs.mon_manager.raw_cluster_cmd('mds', 'add_data_pool', new_pool_name)

        self.mount_a.run_shell(["touch", "layoutfile"])
        self.mount_a.run_shell(["mkdir", "layoutdir"])

        # Set MDS 'rw' perms: missing 'p' means no setting pool layouts
        self.fs.mon_manager.raw_cluster_cmd_result(
            'auth', 'caps', client_name, 'mds', 'allow rw', 'mon', 'allow r',
            'osd',
            'allow rw pool={0},allow rw pool={1}'.format(
                self.fs.get_data_pool_names()[0],
                self.fs.get_data_pool_names()[1],
            ))

        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr("layoutfile", "ceph.file.layout.pool",
                                  new_pool_name)
        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr("layoutdir", "ceph.dir.layout.pool",
                                  new_pool_name)
        self.mount_a.umount_wait()

        # Set MDS 'rwp' perms: should now be able to set layouts
        self.fs.mon_manager.raw_cluster_cmd_result(
            'auth', 'caps', client_name, 'mds', 'allow rwp', 'mon', 'allow r',
            'osd',
            'allow rw pool={0},allow rw pool={1}'.format(
                self.fs.get_data_pool_names()[0],
                self.fs.get_data_pool_names()[1],
            ))
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        self.mount_a.setfattr("layoutfile", "ceph.file.layout.pool",
                              new_pool_name)
        self.mount_a.setfattr("layoutdir", "ceph.dir.layout.pool",
                              new_pool_name)
        self.mount_a.umount_wait()

    def tearDown(self):
        self.fs.mon_manager.raw_cluster_cmd_result(
            'auth', 'caps', "client.{0}".format(self.mount_a.client_id),
            'mds', 'allow', 'mon', 'allow r', 'osd',
            'allow rw pool={0}'.format(self.fs.get_data_pool_names()[0]))
        super(TestPoolPerm, self).tearDown()

