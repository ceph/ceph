
from tasks.cephfs.cephfs_test_case import CephFSTestCase

from teuthology.exceptions import CommandFailedError

class TestQuota(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    def test_remote_update_getfattr(self):
        """
        That quota changes made from one client are visible to another
        client looking at ceph.quota xattrs
        """
        self.mount_a.run_shell(["mkdir", "subdir"])

        self.assertEqual(
            self.mount_a.getfattr("./subdir", "ceph.quota.max_files"),
            None)
        self.assertEqual(
            self.mount_b.getfattr("./subdir", "ceph.quota.max_files"),
            None)

        self.mount_a.setfattr("./subdir", "ceph.quota.max_files", "10")
        self.assertEqual(
            self.mount_a.getfattr("./subdir", "ceph.quota.max_files"),
            "10")

        # Should be visible as soon as setxattr operation completes on
        # mds (we get here sooner because setfattr gets an early reply)
        self.wait_until_equal(
            lambda: self.mount_b.getfattr("./subdir", "ceph.quota.max_files"),
            "10", timeout=10)

    def test_remote_update_df(self):
        """
        That when a client modifies the quota on a directory used
        as another client's root, the other client sees the change
        reflected in their statfs output.
        """

        self.mount_b.umount_wait()

        self.mount_a.run_shell(["mkdir", "subdir"])

        size_before = 1024 * 1024 * 128
        self.mount_a.setfattr("./subdir", "ceph.quota.max_bytes",
                              "%s" % size_before)

        self.mount_b.mount_wait(cephfs_mntpt="/subdir")

        self.assertDictEqual(
            self.mount_b.df(),
            {
                "total": size_before,
                "used": 0,
                "available": size_before
            })

        size_after = 1024 * 1024 * 256
        self.mount_a.setfattr("./subdir", "ceph.quota.max_bytes",
                              "%s" % size_after)

        # Should be visible as soon as setxattr operation completes on
        # mds (we get here sooner because setfattr gets an early reply)
        self.wait_until_equal(
            lambda: self.mount_b.df(),
            {
                "total": size_after,
                "used": 0,
                "available": size_after
            },
            timeout=10
        )

    def test_remote_update_write(self):
        """
        That when a client modifies the quota on a directory used
        as another client's root, the other client sees the effect
        of the change when writing data.
        """

        self.mount_a.run_shell(["mkdir", "subdir_files"])
        self.mount_a.run_shell(["mkdir", "subdir_data"])

        # Set some nice high quotas that mount_b's initial operations
        # will be well within
        self.mount_a.setfattr("./subdir_files", "ceph.quota.max_files", "100")
        self.mount_a.setfattr("./subdir_data", "ceph.quota.max_bytes", "104857600")

        # Do some writes within my quota
        self.mount_b.create_n_files("subdir_files/file", 20)
        self.mount_b.write_n_mb("subdir_data/file", 20)

        # Set quotas lower than what mount_b already wrote, it should
        # refuse to write more once it's seen them
        self.mount_a.setfattr("./subdir_files", "ceph.quota.max_files", "10")
        self.mount_a.setfattr("./subdir_data", "ceph.quota.max_bytes", "1048576")

        # Do some writes that would have been okay within the old quota,
        # but are forbidden under the new quota
        with self.assertRaises(CommandFailedError):
            self.mount_b.create_n_files("subdir_files/file", 40)
        with self.assertRaises(CommandFailedError):
            self.mount_b.write_n_mb("subdir_data/file", 40)

    def test_human_readable_quota_values(self):
        """
        test human-readable values for setting ceph.quota.max_bytes
        """
        self.mount_a.run_shell(["mkdir", "subdir"])

        self.assertEqual(self.mount_a.getfattr("./subdir",
                                               "ceph.quota.max_bytes"), None)

        readable_values = {"10K": "10240",
                           "100Ki": "102400",
                           "10M": "10485760",
                           "100Mi": "104857600",
                           "2G": "2147483648",
                           "4Gi": "4294967296",
                           "1T": "1099511627776",
                           "2Ti": "2199023255552"}
        for readable_value in readable_values:
            self.mount_a.setfattr("./subdir", "ceph.quota.max_bytes",
                                  readable_value)
            self.assertEqual(self.mount_a.getfattr(
                "./subdir", "ceph.quota.max_bytes"),
                readable_values.get(readable_value))

    def test_human_readable_quota_invalid_values(self):
        """
        test invalid values for ceph.quota.max_bytes
        """

        self.mount_a.run_shell(["mkdir", "subdir"])

        invalid_values = ["10A", "1y00Ki", "af00", "G", "", " ", "-1t", "-1"]
        for invalid_value in invalid_values:
            with self.assertRaises(CommandFailedError):
                self.mount_a.setfattr("./subdir", "ceph.quota.max_bytes",
                                      invalid_value)

    def test_disable_enable_human_readable_quota_values(self):
        """
        test:
        1) disabling ceph.quota.max_bytes using byte value.
        2) enabling it again using human readable value.
        3) disabling it again but using human readable value.
        """

        self.mount_a.run_shell(["mkdir", "subdir"])

        self.mount_a.setfattr("./subdir", "ceph.quota.max_bytes", "0")
        self.assertEqual(self.mount_a.getfattr("./subdir",
                                               "ceph.quota.max_bytes"), None)
        self.mount_a.setfattr("./subdir", "ceph.quota.max_bytes", "1K")
        self.assertEqual(self.mount_a.getfattr("./subdir",
                                               "ceph.quota.max_bytes"), "1024")
        self.mount_a.setfattr("./subdir", "ceph.quota.max_bytes", "0M")
        self.assertEqual(self.mount_a.getfattr("./subdir",
                                               "ceph.quota.max_bytes"), None)
