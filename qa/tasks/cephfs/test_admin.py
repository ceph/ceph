import json

from teuthology.orchestra.run import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

from tasks.cephfs.filesystem import FileLayout


class TestAdminCommands(CephFSTestCase):
    """
    Tests for administration command.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def test_fs_status(self):
        """
        That `ceph fs status` command functions.
        """

        s = self.fs.mon_manager.raw_cluster_cmd("fs", "status")
        self.assertTrue("active" in s)

        mdsmap = json.loads(self.fs.mon_manager.raw_cluster_cmd("fs", "status", "--format=json-pretty"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")

        mdsmap = json.loads(self.fs.mon_manager.raw_cluster_cmd("fs", "status", "--format=json"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")

    def _setup_ec_pools(self, n, metadata=True, overwrites=True):
        if metadata:
            self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', n+"-meta", "8")
        cmd = ['osd', 'erasure-code-profile', 'set', n+"-profile", "m=2", "k=2", "crush-failure-domain=osd"]
        self.fs.mon_manager.raw_cluster_cmd(*cmd)
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', n+"-data", "8", "erasure", n+"-profile")
        if overwrites:
            self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'set', n+"-data", 'allow_ec_overwrites', 'true')

    def _check_pool_application_metadata_key_value(self, pool, app, key, value):
        output = self.fs.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'application', 'get', pool, app, key)
        self.assertEqual(str(output.strip()), value)

    def test_add_data_pool_root(self):
        """
        That a new data pool can be added and used for the root directory.
        """

        p = self.fs.add_data_pool("foo")
        self.fs.set_dir_layout(self.mount_a, ".", FileLayout(pool=p))

    def test_add_data_pool_application_metadata(self):
        """
        That the application metadata set on a newly added data pool is as expected.
        """
        pool_name = "foo"
        mon_cmd = self.fs.mon_manager.raw_cluster_cmd
        mon_cmd('osd', 'pool', 'create', pool_name, str(self.fs.pgs_per_fs_pool))
        # Check whether https://tracker.ceph.com/issues/43061 is fixed
        mon_cmd('osd', 'pool', 'application', 'enable', pool_name, 'cephfs')
        self.fs.add_data_pool(pool_name, create=False)
        self._check_pool_application_metadata_key_value(
            pool_name, 'cephfs', 'data', self.fs.name)

    def test_add_data_pool_subdir(self):
        """
        That a new data pool can be added and used for a sub-directory.
        """

        p = self.fs.add_data_pool("foo")
        self.mount_a.run_shell("mkdir subdir")
        self.fs.set_dir_layout(self.mount_a, "subdir", FileLayout(pool=p))

    def test_add_data_pool_non_alphamueric_name_as_subdir(self):
        """
        That a new data pool with non-alphanumeric name can be added and used for a sub-directory.
        """
        p = self.fs.add_data_pool("I-am-data_pool00.")
        self.mount_a.run_shell("mkdir subdir")
        self.fs.set_dir_layout(self.mount_a, "subdir", FileLayout(pool=p))

    def test_add_data_pool_ec(self):
        """
        That a new EC data pool can be added.
        """

        n = "test_add_data_pool_ec"
        self._setup_ec_pools(n, metadata=False)
        self.fs.add_data_pool(n+"-data", create=False)

    def test_new_default_ec(self):
        """
        That a new file system warns/fails with an EC default data pool.
        """

        self.fs.delete_all_filesystems()
        n = "test_new_default_ec"
        self._setup_ec_pools(n)
        try:
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")

    def test_new_default_ec_force(self):
        """
        That a new file system succeeds with an EC default data pool with --force.
        """

        self.fs.delete_all_filesystems()
        n = "test_new_default_ec_force"
        self._setup_ec_pools(n)
        self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")

    def test_new_default_ec_no_overwrite(self):
        """
        That a new file system fails with an EC default data pool without overwrite.
        """

        self.fs.delete_all_filesystems()
        n = "test_new_default_ec_no_overwrite"
        self._setup_ec_pools(n, overwrites=False)
        try:
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")
        # and even with --force !
        try:
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")

    def test_fs_new_pool_application_metadata(self):
        """
        That the application metadata set on the pools of a newly created filesystem are as expected.
        """
        self.fs.delete_all_filesystems()
        fs_name = "test_fs_new_pool_application"
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        mon_cmd = self.fs.mon_manager.raw_cluster_cmd
        for p in pool_names:
            mon_cmd('osd', 'pool', 'create', p, str(self.fs.pgs_per_fs_pool))
            mon_cmd('osd', 'pool', 'application', 'enable', p, 'cephfs')
        mon_cmd('fs', 'new', fs_name, pool_names[0], pool_names[1])
        for i in range(2):
            self._check_pool_application_metadata_key_value(
                pool_names[i], 'cephfs', keys[i], fs_name)


class TestConfigCommands(CephFSTestCase):
    """
    Test that daemons and clients respond to the otherwise rarely-used
    runtime config modification operations.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def test_ceph_config_show(self):
        """
        That I can successfully show MDS configuration.
        """

        names = self.fs.get_rank_names()
        for n in names:
            s = self.fs.mon_manager.raw_cluster_cmd("config", "show", "mds."+n)
            self.assertTrue("NAME" in s)
            self.assertTrue("mon_host" in s)

    def test_client_config(self):
        """
        That I can successfully issue asok "config set" commands

        :return:
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Test only applies to FUSE clients")

        test_key = "client_cache_size"
        test_val = "123"
        self.mount_a.admin_socket(['config', 'set', test_key, test_val])
        out = self.mount_a.admin_socket(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)

        self.mount_a.write_n_mb("file.bin", 1);

        # Implicitly asserting that things don't have lockdep error in shutdown
        self.mount_a.umount_wait(require_clean=True)
        self.fs.mds_stop()

    def test_mds_config_asok(self):
        test_key = "mds_max_purge_ops"
        test_val = "123"
        self.fs.mds_asok(['config', 'set', test_key, test_val])
        out = self.fs.mds_asok(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)

        # Implicitly asserting that things don't have lockdep error in shutdown
        self.mount_a.umount_wait(require_clean=True)
        self.fs.mds_stop()

    def test_mds_config_tell(self):
        test_key = "mds_max_purge_ops"
        test_val = "123"

        mds_id = self.fs.get_lone_mds_id()
        self.fs.mon_manager.raw_cluster_cmd("tell", "mds.{0}".format(mds_id), "injectargs",
                                            "--{0}={1}".format(test_key, test_val))

        # Read it back with asok because there is no `tell` equivalent
        out = self.fs.mds_asok(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)

        # Implicitly asserting that things don't have lockdep error in shutdown
        self.mount_a.umount_wait(require_clean=True)
        self.fs.mds_stop()
