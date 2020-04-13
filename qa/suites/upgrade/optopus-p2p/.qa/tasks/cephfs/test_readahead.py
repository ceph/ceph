import logging
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestReadahead(CephFSTestCase):
    def test_flush(self):
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("FUSE needed for measuring op counts")

        # Create 32MB file
        self.mount_a.run_shell(["dd", "if=/dev/urandom", "of=foo", "bs=1M", "count=32"])

        # Unmount and remount the client to flush cache
        self.mount_a.umount_wait()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        initial_op_r = self.mount_a.admin_socket(['perf', 'dump', 'objecter'])['objecter']['op_r']
        self.mount_a.run_shell(["dd", "if=foo", "of=/dev/null", "bs=128k", "count=32"])
        op_r = self.mount_a.admin_socket(['perf', 'dump', 'objecter'])['objecter']['op_r']
        assert op_r >= initial_op_r
        op_r -= initial_op_r
        log.info("read operations: {0}".format(op_r))

        # with exponentially increasing readahead, we should see fewer than 10 operations
        # but this test simply checks if the client is doing a remote read for each local read
        if op_r >= 32:
            raise RuntimeError("readahead not working")
