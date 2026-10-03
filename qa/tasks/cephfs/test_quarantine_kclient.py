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

import logging
import os
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.kernel_mount import KernelMount

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
            self.mount_a.kill_background()
        except Exception:
            pass
        try:
            self._safe_umount(force=True)
        except Exception:
            pass
        try:
            self._fs_cmd("subvolume", "rm", self.volname,
                         self.SUBVOLUME_NAME, "--force")
        except Exception:
            pass
        # Do not remount before super().tearDown(): vstart's gather_mount_info
        # often leaves self.addr unset, and teardown()->umount()->is_blocked()
        # crashes on None addr. Leave the client unmounted; the framework
        # restores mount details after tearDown.
        super().tearDown()

    def _fs_cmd(self, *args):
        return self.get_ceph_cmd_stdout("fs", *args)

    def _quarantine_cmd(self, action):
        return self._fs_cmd("subvolume", "quarantine", action,
                            self.volname, self.SUBVOLUME_NAME)

    def _ensure_mount_addr(self):
        """Populate mount addr when session-ls lookup failed (vstart)."""
        m = self.mount_a
        if m.addr is not None:
            return
        try:
            m.id = m._get_global_id()
            m.addr = m._global_addr
            if m.addr:
                m.inst = "client%d %s" % (int(m.id), m.addr)
        except Exception as e:
            log.warning("could not populate mount addr: %s", e)

    def _safe_umount(self, force=False):
        """
        Umount without tripping vstart_runner is_blocked() when addr is unset.
        """
        m = self.mount_a
        if not m.is_mounted():
            m.cleanup()
            return

        self._ensure_mount_addr()
        if m.addr is None:
            cmd = ['sudo', 'umount', m.hostfs_mntpt]
            if force:
                cmd.append('-f')
            try:
                m.client_remote.run(args=cmd, omit_sudo=False)
            except Exception:
                if not force:
                    raise
                m._run_umount_lf()
            m.cleanup()
            return

        m.umount_wait(force=force)

    def _setup_mount_and_file(self):
        """Mount at the subvolume data path and create the test file."""
        self._safe_umount()
        self.mount_a.mount_wait(cephfs_mntpt=self.subvol_path)
        self._ensure_mount_addr()
        test_file = os.path.join(self.mount_a.hostfs_mntpt, self.TEST_FILE)
        self.mount_a.write_file(test_file, self.TEST_DATA)
        self.mount_a.run_shell(["sync"])
        return test_file

    def _drop_caches(self):
        """Drop VFS caches so the kernel must ask MDS for metadata."""
        self.mount_a.run_shell(["sync"])
        self.mount_a.run_shell_payload(
            "echo 3 > /proc/sys/vm/drop_caches", sudo=True)

    def _get_stdout(self, proc):
        """Extract stdout string from a completed process."""
        if hasattr(proc.stdout, 'getvalue'):
            return proc.stdout.getvalue()
        elif hasattr(proc.stdout, 'read'):
            return proc.stdout.read()
        elif proc.stdout:
            return str(proc.stdout)
        return ""

    def _wait_for_proc(self, proc, timeout=30):
        """Poll-based wait since RemoteProcess.wait() has no timeout arg."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if proc.poll() is not None:
                return
            time.sleep(0.5)
        raise TimeoutError(
            f"process did not complete within {timeout}s")

    def _assert_blocked_and_resume(self, proc, label="operation"):
        """Assert proc is blocked, disable quarantine, wait for completion."""
        time.sleep(2)
        self.assertIsNone(proc.poll(),
                          f"{label} should be blocked, not completed/failed")

        self._quarantine_cmd("disable")

        try:
            self._wait_for_proc(proc, timeout=30)
        except Exception as e:
            try:
                proc.kill()
            except Exception:
                pass
            self.fail(f"Blocked {label} did not complete after "
                      f"unquarantine: {e}")

    def test_blocked_read_resumes_after_unquarantine(self):
        """
        Verify that a blocked read operation on a quarantined subvolume
        resumes and completes after quarantine is disabled.
        """
        test_file = self._setup_mount_and_file()
        self._drop_caches()

        self._quarantine_cmd("enable")

        proc = self.mount_a.run_shell_payload(
            f"cat {test_file}",
            wait=False,
            timeout=60,
            cwd="/"
        )

        self._assert_blocked_and_resume(proc, "read")
        stdout = self._get_stdout(proc)
        self.assertEqual(stdout.strip(), self.TEST_DATA,
                         "Blocked read should return correct data after unquarantine")

    def test_blocked_ls_resumes_after_unquarantine(self):
        """
        Verify that 'ls' (readdir) on a quarantined subvolume blocks for old
        kernel clients and resumes after quarantine is disabled.
        """
        self._setup_mount_and_file()
        self._drop_caches()

        self._quarantine_cmd("enable")

        proc = self.mount_a.run_shell_payload(
            f"ls -la {self.mount_a.hostfs_mntpt}",
            wait=False,
            timeout=60,
            cwd="/"
        )

        self._assert_blocked_and_resume(proc, "ls")
        stdout = self._get_stdout(proc)
        self.assertIn(self.TEST_FILE, stdout,
                      "ls should show test file after unquarantine")

    def test_blocked_stat_resumes_after_unquarantine(self):
        """
        Verify that 'stat' (getattr) on a quarantined subvolume blocks for old
        kernel clients and resumes after quarantine is disabled.
        """
        test_file = self._setup_mount_and_file()
        self._drop_caches()

        self._quarantine_cmd("enable")

        proc = self.mount_a.run_shell_payload(
            f"stat {test_file}",
            wait=False,
            timeout=60,
            cwd="/"
        )

        self._assert_blocked_and_resume(proc, "stat")

    def _wait_for_dd_in_progress(self, proc, stream_file, min_bytes, timeout=60):
        """Wait until dd has written at least min_bytes and is still running."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if proc.poll() is not None:
                self.fail(
                    f"dd finished too quickly with exit code {proc.poll()}")
            size = int(self.mount_a.run_shell_payload(
                f"stat -c %s {stream_file} 2>/dev/null || echo 0",
                cwd="/"
            ).stdout.getvalue().strip() or 0)
            if size >= min_bytes:
                return size
            time.sleep(0.2)
        self.fail(
            f"dd did not write {min_bytes} bytes within {timeout}s")

    def test_blocked_write_resumes_after_unquarantine(self):
        """
        Verify that an in-progress write blocks when quarantine is enabled
        mid-stream, and resumes to completion when quarantine is disabled.

        A long-running dd keeps FILE_WR caps while writing.  Enabling
        quarantine revokes those caps; the next write syscall blocks in the
        kernel client.  Disabling quarantine re-issues caps and the write
        finishes.
        """
        self._setup_mount_and_file()
        stream_file = os.path.join(self.mount_a.hostfs_mntpt, "stream.bin")
        total_mib = 200
        trigger_mib = 10

        # oflag=direct bypasses page cache so writes hit CephFS and stay
        # in-flight long enough to enable quarantine mid-stream.
        write_cmd = (
            f"dd if=/dev/zero of={stream_file} bs=1M count={total_mib} "
            f"conv=notrunc oflag=direct status=none"
        )

        proc = self.mount_a.run_shell_payload(
            write_cmd, wait=False, timeout=300, cwd="/")

        size_before = self._wait_for_dd_in_progress(
            proc, stream_file, trigger_mib * 1024 * 1024)

        self._quarantine_cmd("enable")

        # Allow cap revocation to propagate, then confirm dd is stuck.
        time.sleep(3)
        self.assertIsNone(proc.poll(),
                          "Write should block after quarantine is enabled")

        self._quarantine_cmd("disable")

        try:
            self._wait_for_proc(proc, timeout=180)
        except Exception as e:
            try:
                proc.kill()
            except Exception:
                pass
            self.fail(f"Blocked write did not resume after unquarantine: {e}")

        self.assertEqual(proc.poll(), 0,
                         "Write should complete successfully after unquarantine")

        size_after = int(self.mount_a.run_shell_payload(
            f"stat -c %s {stream_file}", cwd="/"
        ).stdout.getvalue().strip())
        self.assertGreater(size_after, size_before,
                          "File should grow after write resumes")
        self.assertEqual(size_after, total_mib * 1024 * 1024,
                         f"dd should have written the full {total_mib} MiB")

    def test_multiple_blocked_ops_resume(self):
        """
        Verify that multiple concurrent operations (cat, ls, stat) all block
        under quarantine and all resume when quarantine is disabled.
        """
        test_file = self._setup_mount_and_file()
        self._drop_caches()

        self._quarantine_cmd("enable")

        procs = {}
        procs["cat"] = self.mount_a.run_shell_payload(
            f"cat {test_file}", wait=False, timeout=60, cwd="/")
        procs["ls"] = self.mount_a.run_shell_payload(
            f"ls {self.mount_a.hostfs_mntpt}", wait=False, timeout=60,
            cwd="/")
        procs["stat"] = self.mount_a.run_shell_payload(
            f"stat {test_file}", wait=False, timeout=60, cwd="/")

        time.sleep(2)
        for name, proc in procs.items():
            self.assertIsNone(proc.poll(),
                              f"{name} should be blocked, not completed/failed")

        self._quarantine_cmd("disable")

        for name, proc in procs.items():
            try:
                self._wait_for_proc(proc, timeout=30)
            except Exception as e:
                for p in procs.values():
                    try:
                        p.kill()
                    except Exception:
                        pass
                self.fail(f"Blocked {name} did not complete after "
                          f"unquarantine: {e}")

        cat_stdout = self._get_stdout(procs["cat"])
        self.assertEqual(cat_stdout.strip(), self.TEST_DATA,
                         "cat should return correct data")
        ls_stdout = self._get_stdout(procs["ls"])
        self.assertIn(self.TEST_FILE, ls_stdout,
                      "ls should show test file")

    def test_mount_while_quarantined(self):
        """
        Verify quarantine mount behaviour for old kernel clients:

        1. Mounting directly at the quarantined subvolume path fails.
        2. A fresh mount at the filesystem root can reach the subvolume
           path, but access blocks until quarantine is disabled.
        """
        self._setup_mount_and_file()
        self._safe_umount()

        self._quarantine_cmd("enable")

        # Mounting the quarantined subvolume itself must fail.
        mount_err = self.mount_a.mount(check_status=False)
        self.assertIsNotNone(mount_err,
                             "mount at quarantined subvolume should fail")
        self.assertFalse(self.mount_a.is_mounted(),
                         "quarantined subvolume mount should not succeed")

        # Mount at fs root and access the quarantined path on a fresh session.
        self.mount_a.mount_wait(cephfs_mntpt="/")
        self._ensure_mount_addr()

        test_file = os.path.join(
            self.mount_a.hostfs_mntpt,
            self.subvol_path.lstrip("/"),
            self.TEST_FILE)
        proc = self.mount_a.run_shell_payload(
            f"cat {test_file}", wait=False, timeout=60, cwd="/")

        self._assert_blocked_and_resume(proc, "read on fresh mount")
        stdout = self._get_stdout(proc)
        self.assertEqual(stdout.strip(), self.TEST_DATA,
                         "Read on fresh mount should return correct data "
                         "after unquarantine")

    def test_quarantine_enable_disable_cycles(self):
        """
        Verify that repeated quarantine enable/disable cycles work correctly:
        operations block each time quarantine is enabled and resume each time
        it is disabled, with no state leaks.
        """
        test_file = self._setup_mount_and_file()

        for cycle in range(3):
            log.info("Quarantine cycle %d", cycle + 1)
            self._drop_caches()
            self._quarantine_cmd("enable")

            proc = self.mount_a.run_shell_payload(
                f"cat {test_file}", wait=False, timeout=60, cwd="/")

            time.sleep(2)
            self.assertIsNone(
                proc.poll(),
                f"Cycle {cycle + 1}: read should be blocked")

            self._quarantine_cmd("disable")

            try:
                self._wait_for_proc(proc, timeout=30)
            except Exception as e:
                try:
                    proc.kill()
                except Exception:
                    pass
                self.fail(f"Cycle {cycle + 1}: blocked read did not "
                          f"complete after unquarantine: {e}")

            stdout = self._get_stdout(proc)
            self.assertEqual(
                stdout.strip(), self.TEST_DATA,
                f"Cycle {cycle + 1}: read should return correct data")
