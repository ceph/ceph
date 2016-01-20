
import os
import time
from textwrap import dedent
from unittest import SkipTest
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase

class TestCapFlush(CephFSTestCase):
    def test_replay_create(self):
        """
        MDS starts to handle client caps when it enters clientreplay stage.
        When handling a client cap in clientreplay stage, it's possible that
        corresponding inode does not exist because the client request which
        creates inode hasn't been replayed.
        """

        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client to inject client release failure")

        dir_path = os.path.join(self.mount_a.mountpoint, "testdir")
        py_script = dedent("""
            import os
            os.mkdir("{0}")
            fd = os.open("{0}", os.O_RDONLY)
            os.fchmod(fd, 0777)
            os.fsync(fd)
            """).format(dir_path)
        self.mount_a.run_python(py_script)

        self.fs.mds_asok(["flush", "journal"])

        # client will only get unsafe replay
        self.fs.mds_asok(["config", "set", "mds_log_pause", "1"])

        file_name = "testfile"
        file_path = dir_path + "/" + file_name

        # Create a file and modify its mode. ceph-fuse will mark Ax cap dirty
        py_script = dedent("""
            import os
            os.chdir("{0}")
            os.setgid(65534)
            os.setuid(65534)
            fd = os.open("{1}", os.O_CREAT | os.O_RDWR, 0644)
            os.fchmod(fd, 0640)
            """).format(dir_path, file_name)
        self.mount_a.run_python(py_script)

        # Modify file mode by different user. ceph-fuse will send a setattr request
        self.mount_a.run_shell(["chmod", "600", file_path], wait=False)

        time.sleep(10)

        # Restart mds. Client will re-send the unsafe request and cap flush
        self.fs.mds_stop()
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        mode = self.mount_a.run_shell(['stat', '-c' '%a', file_path]).stdout.getvalue().strip()
        # If the cap flush get dropped, mode should be 0644.
        # (Ax cap stays in dirty state, which prevents setattr reply from updating file mode)
        self.assertEqual(mode, "600")
