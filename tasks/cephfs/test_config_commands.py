
from unittest import case
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount


class TestConfigCommands(CephFSTestCase):
    """
    Test that daemons and clients respond to the otherwise rarely-used
    runtime config modification operations.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def test_client_config(self):
        """
        That I can successfully issue asok "config set" commands

        :return:
        """

        if not isinstance(self.mount_a, FuseMount):
            raise case.SkipTest("Test only applies to FUSE clients")

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
