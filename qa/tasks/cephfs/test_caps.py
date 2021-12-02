import os
from tasks.cephfs.cephfs_test_case import CephFSTestCase


class TestCaps(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    def test_file_lazy_io_caps(self):

        """
        Validates the grant of lazy_io cap request on multiple clients.

        This capability means the clients could perform lazy io. LazyIO
        relaxes POSIX semantics. Buffered reads/writes are allowed even
        when a file is opened by multiple applications on multiple clients.
        Applications are responsible for managing cache coherency themselves
        """
        mount_b_file_path = os.path.join(self.mount_b.mountpoint, "file_Fl")
        mount_a_file_path = os.path.join(self.mount_a.mountpoint, "file_Fl")
        self.mount_a.run_shell(["touch", mount_a_file_path])

        # Usually the caps pAsLsXsFscr/0xd55 are given. Verify Fl caps are
        # given.
        mount_a_caps = self.mount_a.getfattr(mount_a_file_path, "ceph.caps")
        self.assertNotIn("l", mount_a_caps)

        mount_b_caps = self.mount_b.getfattr(mount_b_file_path, "ceph.caps")
        self.assertNotIn("l", mount_b_caps)

        # Request for Fl(32768) on client mount_a
        self.mount_a.admin_socket(['get_caps', '/file_Fl', "32768"])
        mount_a_caps = self.mount_a.getfattr(mount_a_file_path, "ceph.caps")
        # Verify Fl is not granted
        self.assertIn("l", mount_a_caps)

        # Client mount_b caps are unchanged
        mount_b_caps_1 = self.mount_b.getfattr(mount_b_file_path, "ceph.caps")
        self.assertEqual(mount_b_caps, mount_b_caps_1)

        # Request for Fl (32768) on client mount_b
        self.mount_b.admin_socket(['get_caps', '/file_Fl', "32768"])
        # Verify Fl is not granted
        mount_b_caps = self.mount_b.getfattr(mount_b_file_path, "ceph.caps")
        self.assertIn("l", mount_b_caps)

        # Verify Fl is still retained on mount_a
        mount_a_caps = self.mount_a.getfattr(mount_a_file_path, "ceph.caps")
        # Verify Fl is not granted
        self.assertIn("l", mount_a_caps)
