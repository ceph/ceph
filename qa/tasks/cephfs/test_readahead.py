import logging
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestReadahead(CephFSTestCase):
    def test_flush(self):
        # Create 32MB file
        self.mount_a.run_shell(["dd", "if=/dev/urandom", "of=foo", "bs=1M", "count=32"])

        # Unmount and remount the client to flush cache
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()

        initial_op_read = self.mount_a.get_op_read_count()
        self.mount_a.run_shell(["dd", "if=foo", "of=/dev/null", "bs=128k", "count=32"])
        op_read = self.mount_a.get_op_read_count()
        self.assertGreaterEqual(op_read, initial_op_read)
        op_read -= initial_op_read
        log.info("read operations: {0}".format(op_read))

        # with exponentially increasing readahead, we should see fewer than 10 operations
        # but this test simply checks if the client is doing a remote read for each local read
        if op_read >= 32:
            raise RuntimeError("readahead not working")
