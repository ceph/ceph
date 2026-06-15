"""
Kernel client quarantine blocking tests.

These tests verify that kernel clients (without quarantine support) have
their operations blocked (not failed) when accessing quarantined subvolumes,
and that blocked operations resume when quarantine is disabled.

This is separate from test_quarantine.py because kernel client tests must
not run in the same test suite as FUSE tests — the different setUp/tearDown
lifecycles interfere with each other.

These tests only run in teuthology with kclient mount configuration.
"""

import errno
import logging
import os
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.kernel_mount import KernelMount
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestQuarantineKernelBlocking(CephFSTestCase):
    """
    Test that kernel clients (without quarantine support) have their operations
    blocked (not failed) when accessing quarantined subvolumes, and that blocked
    operations resume when quarantine is disabled.

    Old clients that don't understand quarantine will have their caps revoked
    to PIN-only, causing operations to block waiting for caps that will never
    be granted while quarantined.
    """
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1
    SUBVOLUME_NAME = "quarantine_kernel_block_subvol"
    TEST_FILE = "testfile.txt"
    TEST_DATA = "Test data for quarantine."

    def setUp(self):
        super().setUp()
        if not isinstance(self.mount_a, KernelMount):
            self.skipTest("This test requires kernel client to verify blocking behavior")
        self.volname = self.fs.name
        self._fs_cmd("subvolume", "create", self.volname,
                     self.SUBVOLUME_NAME, "--mode=777")
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname,
                                  self.SUBVOLUME_NAME).strip()
        self.subvol_path = subvolpath
        parts = subvolpath.rstrip("/").rsplit("/", 1)
        self.subvol_root_path = parts[0] if len(parts) == 2 else subvolpath
        log.info("Subvolume data path: %s", self.subvol_path)
        log.info("Subvolume root path: %s", self.subvol_root_path)

    def tearDown(self):
        try:
            self._quarantine_cmd("disable")
        except Exception:
            pass
        try:
            self._fs_cmd("subvolume", "rm", self.volname,
                         self.SUBVOLUME_NAME, "--force")
        except Exception:
            pass
        super().tearDown()

    def _fs_cmd(self, *args):
        return self.get_ceph_cmd_stdout("fs", *args)

    def _quarantine_cmd(self, action):
        return self._fs_cmd("subvolume", "quarantine", action,
                            self.volname, self.SUBVOLUME_NAME)

    def test_blocked_read_resumes_after_unquarantine(self):
        """
        Verify that a blocked read operation on a quarantined subvolume
        resumes and completes after quarantine is disabled.

        Kernel clients without quarantine support will have operations block
        (waiting for caps) rather than fail with EACCES. When quarantine is
        lifted, caps are restored and the blocked operation should complete.
        """
        self.mount_a.umount_wait()
        self.mount_a.mount_wait(cephfs_mntpt=self.subvol_path)

        test_file = os.path.join(self.mount_a.hostfs_mntpt, self.TEST_FILE)
        self.mount_a.write_file(test_file, self.TEST_DATA)
        self.mount_a.run_shell(["sync"])

        self._quarantine_cmd("enable")

        proc = self.mount_a.run_shell_payload(
            f"cat {test_file}",
            wait=False,
            timeout=None
        )

        time.sleep(2)
        self.assertIsNone(proc.poll(),
                          "Read should be blocked, not completed/failed")

        self._quarantine_cmd("disable")

        try:
            proc.wait(timeout=30)
            stdout = proc.stdout.read().decode() if proc.stdout else ""
            self.assertEqual(stdout.strip(), self.TEST_DATA,
                             "Blocked read should return correct data after unquarantine")
        except Exception as e:
            proc.kill()
            self.fail(f"Blocked read did not complete after unquarantine: {e}")
