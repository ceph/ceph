
from unittest import SkipTest
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase

class TestMisc(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    def test_getattr_caps(self):
        """
        Check if MDS recognizes the 'mask' parameter of open request.
        The paramter allows client to request caps when opening file
        """

        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client")

        # Enable debug. Client will requests CEPH_CAP_XATTR_SHARED
        # on lookup/open
        self.mount_b.umount_wait()
        self.set_conf('client', 'client debug getattr caps', 'true')
        self.mount_b.mount()
        self.mount_b.wait_until_mounted()

        # create a file and hold it open. MDS will issue CEPH_CAP_EXCL_*
        # to mount_a
        p = self.mount_a.open_background("testfile")
        self.mount_b.wait_for_visible("testfile")

        # this tiggers a lookup request and an open request. The debug
        # code will check if lookup/open reply contains xattrs
        self.mount_b.run_shell(["cat", "testfile"])

        self.mount_a.kill_background(p)
