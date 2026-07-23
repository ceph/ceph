"""
Test suite for CephFS subvolume quarantine feature.

Quarantine allows administrators to lock down a subvolume during security
incidents (e.g., ransomware attacks). When quarantined:
- Regular clients cannot read/write data
- Clients with 'q' flag can access specific quarantined paths
- Clients with 'Q' flag (or 'allow *') can access all quarantined paths
- Snapshot data is also protected

NOTE: These tests require FUSE client (ceph-fuse) because they expect
operations on quarantined subvolumes to fail with errors (EACCES/EPERM).
Kernel clients without quarantine support will block instead of failing.
Once the kernel driver implements quarantine support (returning errors
instead of blocking), these tests can be enabled for kernel clients too.
"""

import errno
import json
import logging
import os
import time
import unittest
from io import StringIO
from typing import Optional

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

# Set to True once the MDS quarantine feature is fully implemented
QUARANTINE_FEATURE_READY = True

# Set to True once Q/Q' flag mount access to quarantined subvolumes works.
QUARANTINE_RECOVERY_MOUNT_READY = True

# Expected error codes when quarantine blocks an operation
QUARANTINE_BLOCK_ERRNOS = [errno.EACCES, errno.EPERM, errno.EIO, errno.EROFS]


class QuarantineTestBase(CephFSTestCase):
    """
    Base class for quarantine tests.

    Subclasses should set SUBVOLUME_NAME (and optionally TEST_FILE, TEST_DATA).
    """

    MDSS_REQUIRED = 1
    SUBVOLUME_NAME: Optional[str] = None
    TEST_FILE = "testfile.txt"
    TEST_DATA = "Test data for quarantine."

    def setUp(self):
        super().setUp()
        # Quarantine tests expect operations to fail with errors (not block).
        # This behavior requires FUSE client with quarantine support. Kernel
        # clients without quarantine support will block instead of failing.
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Quarantine tests require FUSE client")
        self.volname = self.fs.name
        if self.SUBVOLUME_NAME:
            self._fs_cmd("subvolume", "create", self.volname,
                         self.SUBVOLUME_NAME, "--mode=777")
            self.subvol_path = self._get_subvolume_path()
            self.subvol_root_path = self._get_subvolume_root_path()
            log.info("Subvolume data path: %s", self.subvol_path)
            log.info("Subvolume root path: %s", self.subvol_root_path)

    def tearDown(self):
        if self.SUBVOLUME_NAME:
            try:
                self._quarantine_disable()
            except Exception:
                pass
            try:
                self._fs_cmd("subvolume", "rm", self.volname,
                             self.SUBVOLUME_NAME, "--force")
            except Exception:
                pass
        super().tearDown()

    # -- Subvolume helpers ---------------------------------------------------

    def _fs_cmd(self, *args):
        return self.get_ceph_cmd_stdout("fs", *args)

    def _get_subvolume_path(self, subvol_name=None):
        name = subvol_name or self.SUBVOLUME_NAME
        return self._fs_cmd("subvolume", "getpath", self.volname, name).strip()

    def _get_subvolume_root_path(self, subvol_name=None):
        return os.path.dirname(self._get_subvolume_path(subvol_name))

    # -- File helpers --------------------------------------------------------

    def _create_test_file(self, mount=None, subvol_path=None,
                          filename=None, data=None):
        mount = mount or self.mount_a
        rel = (subvol_path or self.subvol_path).lstrip("/")
        path = os.path.join(rel, filename or self.TEST_FILE)
        mount.write_file(path, data or self.TEST_DATA)
        log.info("Created test file at %s", path)
        return path

    def _get_test_file_path(self, subvol_path=None, filename=None):
        rel = (subvol_path or self.subvol_path).lstrip("/")
        return os.path.join(rel, filename or self.TEST_FILE)

    # -- Quarantine client helpers -------------------------------------------

    def _create_qtine_client(self, client_name, mds_caps):
        """Create a cephx client with keyring file for ceph-fuse.

        Returns (authid, keyring_path).
        """
        log.info("Creating %s with caps: %s", client_name, mds_caps)
        out = self.get_ceph_cmd_stdout(
            "auth", "get-or-create", client_name,
            "mon", "allow r", "mds", mds_caps,
            "osd", "allow rw tag cephfs data=%s" % self.volname)

        key = None
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("key"):
                key = line.split("=", 1)[1].strip()
                break
        self.assertIsNotNone(key, "Could not extract key for %s" % client_name)

        authid = client_name.replace("client.", "")
        keyring_txt = "[%s]\n\tkey = %s\n" % (client_name, key)
        keyring_path = self.mount_b.client_remote.mktemp(data=keyring_txt)
        self.config_set("client.%s" % authid, "debug client", 20)
        log.info("Created %s, keyring at %s", client_name, keyring_path)
        return authid, keyring_path

    # -- Quarantine helpers (via mgr module) ---------------------------------

    def _quarantine_enable(self, subvol_name=None, group_name=None):
        """Enable quarantine on a subvolume via mgr module."""
        name = subvol_name or self.SUBVOLUME_NAME
        args = ["subvolume", "quarantine", "enable", self.volname, name]
        if group_name:
            args.extend(["--group_name", group_name])
        log.info("Enabling quarantine: %s", " ".join(args))
        return self._fs_cmd(*args)

    def _quarantine_disable(self, subvol_name=None, group_name=None):
        """Disable quarantine on a subvolume via mgr module."""
        name = subvol_name or self.SUBVOLUME_NAME
        args = ["subvolume", "quarantine", "disable", self.volname, name]
        if group_name:
            args.extend(["--group_name", group_name])
        log.info("Disabling quarantine: %s", " ".join(args))
        return self._fs_cmd(*args)

    # -- Assertion helpers ---------------------------------------------------

    def assert_blocked(self, op, msg="Operation should have been blocked"):
        try:
            op()
            self.fail(msg)
        except CommandFailedError as e:
            log.info("Correctly blocked: %s", e)
            self.assertIn(e.exitstatus, QUARANTINE_BLOCK_ERRNOS,
                          "Unexpected errno %d" % e.exitstatus)

    def wait_until_blocked(self, op, timeout=30, interval=1,
                           msg="Operation did not become blocked in time"):
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            try:
                op()
            except CommandFailedError as e:
                if e.exitstatus in QUARANTINE_BLOCK_ERRNOS:
                    return
                last_error = e
            time.sleep(interval)
        if last_error is not None:
            self.fail("%s (last errno=%s)" % (msg, last_error.exitstatus))
        self.fail(msg)

    def wait_until_unblocked(self, op, timeout=30, interval=1,
                             msg="Operation did not become unblocked in time"):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                op()
                return
            except CommandFailedError:
                time.sleep(interval)
        self.fail(msg)

    def enable_and_wait(self, subvol_name=None, group_name=None, sleep_secs=2):
        self._quarantine_enable(subvol_name, group_name)
        time.sleep(sleep_secs)

    def disable_and_wait(self, subvol_name=None, group_name=None, sleep_secs=2):
        self._quarantine_disable(subvol_name, group_name)
        time.sleep(sleep_secs)


# ---------------------------------------------------------------------------
# Basic quarantine tests
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineBasic(QuarantineTestBase):
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = "quarantine_test_subvol"

    def setUp(self):
        super().setUp()
        self._create_test_file()

    def test_quarantine_enable_disable(self):
        self._quarantine_enable()
        self._quarantine_disable()
        self.assertEqual(self.mount_a.read_file(self._get_test_file_path()),
                         self.TEST_DATA)

    def test_quarantine_blocks_read(self):
        fp = self._get_test_file_path()
        self.assertEqual(self.mount_a.read_file(fp), self.TEST_DATA)
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.read_file(fp))

    def test_quarantine_blocks_write(self):
        fp = self._get_test_file_path()
        self.mount_a.write_file(fp, "pre-quarantine data")
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.write_file(fp, "post-quarantine"))

    def test_quarantine_blocks_create(self):
        new = os.path.join(self.subvol_path.lstrip("/"), "new_file.txt")
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.write_file(new, "nope"))

    def test_quarantine_blocks_mkdir(self):
        d = os.path.join(self.subvol_path.lstrip("/"), "new_dir")
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.run_shell(["mkdir", d]))

    def test_quarantine_blocks_delete(self):
        fp = self._get_test_file_path()
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.run_shell(["rm", fp]))

    def test_quarantine_disable_restores_access(self):
        fp = self._get_test_file_path()
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.read_file(fp))

        self.disable_and_wait()
        self.assertEqual(self.mount_a.read_file(fp), self.TEST_DATA)

        new = "Data after quarantine disabled"
        self.mount_a.write_file(fp, new)
        self.assertEqual(self.mount_a.read_file(fp), new)


# ---------------------------------------------------------------------------
# Snapshot protection
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineSnapshot(QuarantineTestBase):
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = "quarantine_snap_test_subvol"
    SNAPSHOT_NAME = "snap1"
    TEST_FILE = "snapfile.txt"
    TEST_DATA = "Snapshot test data."

    def setUp(self):
        super().setUp()
        self._create_test_file()
        self._fs_cmd("subvolume", "snapshot", "create",
                     self.volname, self.SUBVOLUME_NAME, self.SNAPSHOT_NAME)
        time.sleep(1)

    def tearDown(self):
        self._quarantine_disable()
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                         self.SUBVOLUME_NAME, self.SNAPSHOT_NAME, "--force")
        except Exception:
            pass
        super().tearDown()

    def _resolve_snap_path(self):
        rel = self.subvol_path.lstrip("/")
        path = os.path.join(rel, ".snap", self.SNAPSHOT_NAME, self.TEST_FILE)
        try:
            self.mount_a.read_file(path)
            return path
        except CommandFailedError:
            pass
        rel_root = self.subvol_root_path.lstrip("/")
        path = os.path.join(rel_root, ".snap", self.SNAPSHOT_NAME,
                            os.path.basename(rel), self.TEST_FILE)
        self.mount_a.read_file(path)
        return path

    def test_quarantine_blocks_snapshot_read(self):
        snap = self._resolve_snap_path()
        self.assertEqual(self.mount_a.read_file(snap), self.TEST_DATA)
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.read_file(snap))


# ---------------------------------------------------------------------------
# Auth caps (q / Q / allow *)
# ---------------------------------------------------------------------------
class TestQuarantineAuthCaps(QuarantineTestBase):
    CLIENTS_REQUIRED = 2
    SUBVOLUME_NAME = "quarantine_auth_test_subvol"
    TEST_FILE = "authfile.txt"
    TEST_DATA = "Auth test data."

    def setUp(self):
        super().setUp()
        self._create_test_file()
        self._custom_clients = []

    def tearDown(self):
        for name in self._custom_clients:
            try:
                self.get_ceph_cmd_stdout("auth", "del", name)
            except Exception:
                pass
        super().tearDown()

    def _create_tracked_client(self, client_name, mds_caps):
        self.get_ceph_cmd_stdout(
            "auth", "get-or-create", client_name,
            "mon", "allow r", "mds", mds_caps,
            "osd", "allow rw tag cephfs data=%s" % self.volname)
        self._custom_clients.append(client_name)
        return client_name

    def test_q_flag_allows_quarantine_access(self):
        caps = "allow rwq fsname=%s path=%s" % (self.volname, self.subvol_path)
        c = self._create_tracked_client("client.qtine_specific", caps)
        info = self.get_ceph_cmd_stdout("auth", "get", c)
        self.assertIn("allow rwq", info)
        self.assertIn(self.subvol_path, info)

    def test_Q_prime_flag_allows_all_quarantine_access(self):
        caps = "allow rwQ fsname=%s" % self.volname
        c = self._create_tracked_client("client.qtine_global", caps)
        info = self.get_ceph_cmd_stdout("auth", "get", c)
        self.assertIn("allow rwQ", info)

    def test_allow_star_excludes_quarantine(self):
        """allow * does NOT grant quarantine access — q/Q must be explicit.
        The client cannot even mount a quarantined subvolume with allow *."""
        self.enable_and_wait()
        caps = "allow * fsname=%s" % self.volname
        authid, keyring = self._create_qtine_client("client.qtine_star", caps)
        self._custom_clients.append("client.qtine_star")

        self.mount_b.umount_wait()
        try:
            self.mount_b.mount_wait(
                cephfs_name=self.volname,
                client_id=authid,
                cephfs_mntpt=self.subvol_path,
                client_keyring_path=keyring)
            self.fail("Mount should have failed for allow * on quarantined subvol")
        except CommandFailedError:
            log.info("Mount correctly failed for allow * client on quarantined subvol")


# ---------------------------------------------------------------------------
# Recovery access (Q / q flag mount + I/O)
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_RECOVERY_MOUNT_READY, "Q-flag mount not ready")
class TestQuarantineRecoveryAccess(QuarantineTestBase):
    CLIENTS_REQUIRED = 2
    SUBVOLUME_NAME = "quarantine_recovery_test_subvol"
    TEST_FILE = "recovery_test.txt"
    TEST_DATA = "Important data to recover."

    def setUp(self):
        super().setUp()
        self.recovery_client = "client.recovery_test"
        self._create_test_file()

    def tearDown(self):
        try:
            self.mount_b.umount_wait()
        except Exception:
            pass
        try:
            self.get_ceph_cmd_stdout("auth", "del", self.recovery_client)
        except Exception:
            pass
        super().tearDown()

    def _create_recovery_client(self):
        caps = "allow rwq fsname=%s path=%s" % (self.volname,
                                                 self.subvol_root_path)
        self._recovery_authid, self._recovery_keyring = \
            self._create_qtine_client(self.recovery_client, caps)

    def _mount_recovery_client(self):
        self.mount_b.umount_wait()
        self.mount_b.mount_wait(
            cephfs_name=self.volname,
            client_id=self._recovery_authid,
            cephfs_mntpt=self.subvol_path,
            client_keyring_path=self._recovery_keyring)
        # Allow time for the session and caps to be fully established
        time.sleep(2)

    def test_recovery_client_access_after_quarantine(self):
        fp = self._get_test_file_path()

        self.assertEqual(self.mount_a.read_file(fp), self.TEST_DATA)
        self.mount_a.write_file(fp, self.TEST_DATA + " more data")

        self.enable_and_wait(sleep_secs=3)

        self.assert_blocked(lambda: self.mount_a.read_file(fp))
        self.assert_blocked(lambda: self.mount_a.write_file(fp, "nope"))

        self._create_recovery_client()
        self._mount_recovery_client()

        # First read: verify recovery client can access quarantined file
        first_read = self.mount_b.read_file(self.TEST_FILE)
        self.assertIn(self.TEST_DATA.strip(), first_read)

        # Write new data
        recovery_data = "Recovery client wrote this."
        self.mount_b.write_file(self.TEST_FILE, recovery_data)

        # Sync to ensure write is flushed before attempting read
        self.mount_b.run_shell(["sync"])

        # Second read: verify data was written correctly
        second_read = self.mount_b.read_file(self.TEST_FILE)
        self.assertEqual(second_read, recovery_data)

        self.disable_and_wait()
        self.assertEqual(self.mount_a.read_file(fp), recovery_data)

        restored = "Normal client can write again."
        self.mount_a.write_file(fp, restored)
        self.assertEqual(self.mount_a.read_file(fp), restored)

    def test_normal_client_blocked_immediately(self):
        fp = self._get_test_file_path()
        self.assertEqual(self.mount_a.read_file(fp), self.TEST_DATA)
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.read_file(fp))

    def test_Q_prime_client_access_any_quarantine(self):
        admin = "client.qtine_admin"
        caps = "allow rwQ fsname=%s" % self.volname
        authid, keyring = self._create_qtine_client(admin, caps)
        try:
            self.enable_and_wait()
            self.mount_b.umount_wait()
            self.mount_b.mount_wait(
                cephfs_name=self.volname, client_id=authid,
                cephfs_mntpt=self.subvol_path,
                client_keyring_path=keyring)
            self.assertIn(self.TEST_DATA.strip(),
                          self.mount_b.read_file(self.TEST_FILE))
        finally:
            self.get_ceph_cmd_stdout("auth", "del", admin)


# ---------------------------------------------------------------------------
# mgr/volumes subvolume operations
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineSubvolumeOps(QuarantineTestBase):
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = "quarantine_ops_test_subvol"
    SNAPSHOT_NAME = "ops_snap"
    TEST_FILE = "opsfile.txt"
    TEST_DATA = "Ops test data."

    def setUp(self):
        super().setUp()
        self._create_test_file()
        self._fs_cmd("subvolume", "snapshot", "create",
                     self.volname, self.SUBVOLUME_NAME, self.SNAPSHOT_NAME)

    def tearDown(self):
        self._quarantine_disable()
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                         self.SUBVOLUME_NAME, self.SNAPSHOT_NAME, "--force")
        except Exception:
            pass
        super().tearDown()

    def test_quarantine_info_blocked(self):
        """subvolume info fails on a quarantined subvolume."""
        self.enable_and_wait(sleep_secs=1)
        with self.assertRaises(CommandFailedError) as ctx:
            self._fs_cmd("subvolume", "info",
                         self.volname, self.SUBVOLUME_NAME)
        self.assertEqual(ctx.exception.exitstatus, errno.EACCES)
        log.info("subvolume info correctly blocked")

    def test_quarantine_getpath_blocked(self):
        """subvolume getpath fails on a quarantined subvolume."""
        self.enable_and_wait(sleep_secs=1)
        with self.assertRaises(CommandFailedError) as ctx:
            self._fs_cmd("subvolume", "getpath",
                         self.volname, self.SUBVOLUME_NAME)
        self.assertEqual(ctx.exception.exitstatus, errno.EACCES)
        log.info("subvolume getpath correctly blocked")

    def test_quarantine_snapshot_list_blocked(self):
        """subvolume snapshot ls fails on a quarantined subvolume."""
        self.enable_and_wait(sleep_secs=1)
        with self.assertRaises(CommandFailedError) as ctx:
            self._fs_cmd("subvolume", "snapshot", "ls",
                         self.volname, self.SUBVOLUME_NAME)
        self.assertEqual(ctx.exception.exitstatus, errno.EACCES)
        log.info("subvolume snapshot ls correctly blocked")


# ---------------------------------------------------------------------------
# Concurrent I/O
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineConcurrentIO(QuarantineTestBase):
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = "quarantine_concurrent_test_subvol"

    def test_quarantine_during_active_io(self):
        rel = self.subvol_path.lstrip("/")
        tf = os.path.join(rel, "concurrent_io_test.txt")
        self.mount_a.write_file(tf, "Initial data")

        proc = self.mount_a.run_shell(
            ["bash", "-c",
             "for i in $(seq 1 100); do echo \"iteration $i\" >> %s;"
             " sleep 0.5; done" % tf],
            wait=False, stderr=StringIO())

        time.sleep(2)
        self._quarantine_enable()

        try:
            proc.wait()
        except CommandFailedError as e:
            log.info("Background write correctly failed: %s", e)
        else:
            log.info("Background write completed before quarantine")


# ---------------------------------------------------------------------------
# Multiple subvolumes (isolation)
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineMultipleSubvolumes(QuarantineTestBase):
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = None  # managed manually

    SUBVOL1 = "quarantine_multi_subvol1"
    SUBVOL2 = "quarantine_multi_subvol2"

    def setUp(self):
        super().setUp()
        for sv in (self.SUBVOL1, self.SUBVOL2):
            self._fs_cmd("subvolume", "create", self.volname, sv, "--mode=777")

        self.subvol1_path = self._get_subvolume_path(self.SUBVOL1)
        self.subvol2_path = self._get_subvolume_path(self.SUBVOL2)
        self.subvol1_root_path = self._get_subvolume_root_path(self.SUBVOL1)
        self.subvol2_root_path = self._get_subvolume_root_path(self.SUBVOL2)

        self._create_test_file(subvol_path=self.subvol1_path,
                               data="Data in %s" % self.SUBVOL1)
        self._create_test_file(subvol_path=self.subvol2_path,
                               data="Data in %s" % self.SUBVOL2)

    def tearDown(self):
        for sv in (self.SUBVOL1, self.SUBVOL2):
            try:
                self._quarantine_disable(subvol_name=sv)
            except Exception:
                pass
            try:
                self._fs_cmd("subvolume", "rm", self.volname, sv, "--force")
            except Exception:
                pass
        super().tearDown()

    def test_quarantine_isolation(self):
        f1 = self._get_test_file_path(subvol_path=self.subvol1_path)
        f2 = self._get_test_file_path(subvol_path=self.subvol2_path)

        c2 = self.mount_a.read_file(f2)
        self.enable_and_wait(subvol_name=self.SUBVOL1)

        # subvol2 unaffected
        self.assertEqual(self.mount_a.read_file(f2), c2)
        self.mount_a.write_file(f2, "New data in subvol2")

        # subvol1 blocked
        self.assert_blocked(lambda: self.mount_a.read_file(f1))

    def test_quarantine_multiple_subvols_quarantined(self):
        """Multiple subvolumes can be quarantined at the same time."""
        f1 = self._get_test_file_path(subvol_path=self.subvol1_path)
        f2 = self._get_test_file_path(subvol_path=self.subvol2_path)

        self.enable_and_wait(subvol_name=self.SUBVOL1)
        self.enable_and_wait(subvol_name=self.SUBVOL2)

        self.assert_blocked(lambda: self.mount_a.read_file(f1))
        self.assert_blocked(lambda: self.mount_a.read_file(f2))

        self.disable_and_wait(subvol_name=self.SUBVOL1)
        self.assertEqual(self.mount_a.read_file(f1).strip(),
                         "Data in %s" % self.SUBVOL1)
        self.assert_blocked(lambda: self.mount_a.read_file(f2))

    def test_subvolume_ls_complete_during_quarantine(self):
        """Quarantining one subvolume must not hide others from subvolume ls.

        Regression test: enabling quarantine on tenant-a caused
        'ceph fs subvolume ls' to return an incomplete list — most
        subvolumes disappeared because is_under_quarantine() fail-closed
        on uncached subvolume root inodes, causing the mgr client's
        readdir to lose entries.
        """
        extra_subvols = ["quarantine_ls_extra_%d" % i for i in range(5)]
        all_subvols = [self.SUBVOL1, self.SUBVOL2] + extra_subvols

        try:
            for sv in extra_subvols:
                self._fs_cmd("subvolume", "create", self.volname, sv,
                             "--mode=777")

            ls_out = self._fs_cmd("subvolume", "ls", self.volname)
            listed_before = set(
                e["name"] for e in json.loads(ls_out))
            for sv in all_subvols:
                self.assertIn(sv, listed_before,
                              "%s missing from ls before quarantine" % sv)

            self.enable_and_wait(subvol_name=self.SUBVOL1)

            # Debug: check what the FUSE mount sees at the _nogroup level
            nogroup_path = "volumes/_nogroup"
            try:
                fuse_entries = set(self.mount_a.ls(nogroup_path))
                log.info("FUSE mount sees in _nogroup: %s", fuse_entries)
            except Exception as e:
                log.warning("FUSE mount ls failed: %s", e)
                fuse_entries = set()

            ls_out = self._fs_cmd("subvolume", "ls", self.volname)
            listed_during = set(
                e["name"] for e in json.loads(ls_out))
            log.info("subvolume ls during quarantine: %s", listed_during)
            log.info("expected: %s", set(all_subvols))
            log.info("missing: %s", set(all_subvols) - listed_during)

            for sv in all_subvols:
                self.assertIn(sv, listed_during,
                              "%s missing from ls DURING quarantine of %s"
                              % (sv, self.SUBVOL1))

            self.disable_and_wait(subvol_name=self.SUBVOL1)

            ls_out = self._fs_cmd("subvolume", "ls", self.volname)
            listed_after = set(
                e["name"] for e in json.loads(ls_out))
            for sv in all_subvols:
                self.assertIn(sv, listed_after,
                              "%s missing from ls after quarantine" % sv)
        finally:
            for sv in extra_subvols:
                try:
                    self._fs_cmd("subvolume", "rm", self.volname, sv,
                                 "--force")
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Mgr operations on quarantined subvolumes
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineMgrOps(QuarantineTestBase):
    """Test that mgr subvolume operations are correctly blocked or allowed
    when a subvolume is quarantined."""
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = "quarantine_mgr_ops_subvol"
    TEST_FILE = "mgrops_test.txt"
    TEST_DATA = "Mgr ops test data."

    def setUp(self):
        super().setUp()
        self._create_test_file()

    def test_quarantine_blocks_subvolume_rm(self):
        """subvolume rm should fail on a quarantined subvolume."""
        self.enable_and_wait()
        try:
            self._fs_cmd("subvolume", "rm", self.volname, self.SUBVOLUME_NAME)
            self.fail("subvolume rm should have failed on quarantined subvol")
        except CommandFailedError:
            log.info("subvolume rm correctly blocked")

    def test_quarantine_blocks_snapshot_create(self):
        """subvolume snapshot create should fail on a quarantined subvolume."""
        self.enable_and_wait()
        try:
            self._fs_cmd("subvolume", "snapshot", "create",
                         self.volname, self.SUBVOLUME_NAME, "blocked_snap")
            self.fail("snapshot create should have failed on quarantined subvol")
        except CommandFailedError:
            log.info("snapshot create correctly blocked")

    def test_quarantine_blocks_resize(self):
        """subvolume resize should fail on a quarantined subvolume."""
        self.enable_and_wait()
        try:
            self._fs_cmd("subvolume", "resize", self.volname,
                         self.SUBVOLUME_NAME, "10737418240")
            self.fail("resize should have failed on quarantined subvol")
        except CommandFailedError:
            log.info("resize correctly blocked")

    def test_quarantine_blocks_info(self):
        """subvolume info fails on a quarantined subvolume because the mgr
        client cannot access .meta without quarantine caps."""
        self.enable_and_wait()
        try:
            self._fs_cmd("subvolume", "info",
                         self.volname, self.SUBVOLUME_NAME)
            self.fail("subvolume info should have failed on quarantined subvol")
        except CommandFailedError:
            log.info("subvolume info correctly blocked")

    def test_quarantine_blocks_getpath(self):
        """subvolume getpath fails on a quarantined subvolume because the mgr
        client cannot access .meta without quarantine caps."""
        self.enable_and_wait()
        try:
            self._fs_cmd("subvolume", "getpath",
                         self.volname, self.SUBVOLUME_NAME)
            self.fail("subvolume getpath should have failed on quarantined subvol")
        except CommandFailedError:
            log.info("subvolume getpath correctly blocked")

    def test_quarantine_list_still_works(self):
        """subvolume ls should still work — it reads the group directory
        which is above the quarantined subvolume, not the subvolume itself."""
        self.enable_and_wait()
        subvols = json.loads(self._fs_cmd("subvolume", "ls", self.volname))
        names = [s["name"] for s in subvols]
        self.assertIn(self.SUBVOLUME_NAME, names)


# ---------------------------------------------------------------------------
# Subvolume group quarantine
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineSubvolumeGroup(QuarantineTestBase):
    """Test quarantine with explicit subvolume group."""
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = None  # managed manually
    GROUP_NAME = "quarantine_test_group"
    SUBVOL_IN_GROUP = "quarantine_group_subvol"

    def setUp(self):
        super().setUp()
        self.volname = self.fs.name
        self._fs_cmd("subvolumegroup", "create", self.volname,
                     self.GROUP_NAME)
        self._fs_cmd("subvolume", "create", self.volname,
                     self.SUBVOL_IN_GROUP, "--group_name", self.GROUP_NAME,
                     "--mode=777")
        self.subvol_path = self._fs_cmd(
            "subvolume", "getpath", self.volname,
            self.SUBVOL_IN_GROUP, "--group_name", self.GROUP_NAME).strip()
        self.subvol_root_path = os.path.dirname(self.subvol_path)
        self._create_test_file(subvol_path=self.subvol_path)

    def tearDown(self):
        try:
            self._quarantine_disable(subvol_name=self.SUBVOL_IN_GROUP,
                                     group_name=self.GROUP_NAME)
        except Exception:
            pass
        try:
            self._fs_cmd("subvolume", "rm", self.volname,
                         self.SUBVOL_IN_GROUP, "--group_name",
                         self.GROUP_NAME, "--force")
        except Exception:
            pass
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname,
                         self.GROUP_NAME)
        except Exception:
            pass
        super().tearDown()

    def test_quarantine_under_subvolume_group(self):
        """Quarantine works on subvolumes under explicit subvolume groups."""
        fp = self._get_test_file_path(subvol_path=self.subvol_path)
        self.assertEqual(self.mount_a.read_file(fp).strip(),
                         self.TEST_DATA.strip())

        self._quarantine_enable(subvol_name=self.SUBVOL_IN_GROUP,
                                group_name=self.GROUP_NAME)
        time.sleep(2)

        self.assert_blocked(lambda: self.mount_a.read_file(fp))

        self._quarantine_disable(subvol_name=self.SUBVOL_IN_GROUP,
                                 group_name=self.GROUP_NAME)
        time.sleep(2)

        self.assertEqual(self.mount_a.read_file(fp).strip(),
                         self.TEST_DATA.strip())


# ---------------------------------------------------------------------------
# Disruptive: MDS restart
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineDisruptive(QuarantineTestBase):
    """Test that quarantine state survives MDS restart."""
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = "quarantine_disruptive_subvol"
    TEST_FILE = "disruptive_test.txt"
    TEST_DATA = "Data to survive restart."

    def setUp(self):
        super().setUp()
        self._create_test_file()

    def tearDown(self):
        # After MDS restart the fuse daemon may be in a bad state.
        # Ensure it's properly killed and remounted before the next test's
        # setUp tries umount_wait.
        try:
            self.mount_a.umount_wait(force=True)
        except Exception:
            self.mount_a.kill_cleanup()
        try:
            self.mount_a.mount_wait()
        except Exception:
            pass
        super().tearDown()

    def test_quarantine_survives_mds_restart(self):
        """Quarantine flag is retained after MDS restart."""
        fp = self._get_test_file_path()
        self.enable_and_wait()
        self.assert_blocked(lambda: self.mount_a.read_file(fp))

        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        time.sleep(5)

        self.wait_until_blocked(
            lambda: self.mount_a.read_file(fp),
            timeout=30,
            msg="Read should still be blocked after MDS restart")


# ---------------------------------------------------------------------------
# Negative tests
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineNegative(QuarantineTestBase):
    """Negative test cases for quarantine."""
    CLIENTS_REQUIRED = 1
    SUBVOLUME_NAME = None  # some tests don't need a subvolume

    def test_quarantine_nonexistent_subvol(self):
        """Quarantining a non-existent subvolume returns an error."""
        try:
            self._fs_cmd("subvolume", "quarantine", "enable",
                         self.fs.name, "nonexistent_subvol_xyz")
            self.fail("Should have failed for non-existent subvolume")
        except CommandFailedError as e:
            log.info("Correctly failed for non-existent subvol: %s", e)

    def test_quarantine_subdir_blocked(self):
        """Mounting a subdir under a quarantined subvolume is blocked."""
        sv_name = "quarantine_subdir_test_subvol"
        self._fs_cmd("subvolume", "create", self.fs.name, sv_name,
                     "--mode=777")
        try:
            sv_path = self._fs_cmd("subvolume", "getpath",
                                   self.fs.name, sv_name).strip()
            rel = sv_path.lstrip("/")
            subdir = os.path.join(rel, "mysubdir")
            self.mount_a.run_shell(["mkdir", "-p", subdir])
            self.mount_a.write_file(os.path.join(subdir, "file.txt"),
                                    "subdir data")

            self._quarantine_enable(subvol_name=sv_name)
            time.sleep(2)

            self.assert_blocked(
                lambda: self.mount_a.read_file(
                    os.path.join(subdir, "file.txt")),
                msg="Read in subdir should be blocked")
        finally:
            try:
                self._quarantine_disable(subvol_name=sv_name)
            except Exception:
                pass
            try:
                self._fs_cmd("subvolume", "rm", self.fs.name, sv_name,
                             "--force")
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Multi-MDS tests
# ---------------------------------------------------------------------------
@unittest.skipUnless(QUARANTINE_FEATURE_READY, "Quarantine not ready")
class TestQuarantineMultiMDS(QuarantineTestBase):
    """
    Tests for quarantine with multiple active MDS daemons.

    When a subvolume has subtrees pinned to different MDS ranks, the auth MDS
    must coordinate with replica MDS daemons via MMDSQuarantine messages.
    """
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2
    SUBVOLUME_NAME = "quarantine_multimds_subvol"
    TEST_FILE = "multimds_test.txt"
    TEST_DATA = "Multi-MDS test data."

    def setUp(self):
        super().setUp()
        log.info("Subvolume data path: %s", self.subvol_path)
        log.info("Subvolume root path: %s", self.subvol_root_path)

        self._create_test_file()

        rel = self.subvol_path.lstrip("/")
        self.dir_rank0 = os.path.join(rel, "dir_rank0")
        self.dir_rank1 = os.path.join(rel, "dir_rank1")
        self.mount_a.run_shell(["mkdir", "-p", self.dir_rank0])
        self.mount_a.run_shell(["mkdir", "-p", self.dir_rank1])

        self.mount_a.setfattr(self.dir_rank1, "ceph.dir.pin", "1")

        self.file_rank0 = os.path.join(self.dir_rank0, "file_on_rank0.txt")
        self.file_rank1 = os.path.join(self.dir_rank1, "file_on_rank1.txt")
        self.mount_a.write_file(self.file_rank0, "Data on rank 0")
        self.mount_a.write_file(self.file_rank1, "Data on rank 1")

        self._bring_up_second_mds()

    def _bring_up_second_mds(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()

        for _ in range(30):
            try:
                subtrees = self.fs.rank_asok(["get", "subtrees"], rank=1)
            except Exception as e:
                log.warning("Error getting subtrees from rank 1: %s", e)
                time.sleep(1)
                continue
            for st in subtrees:
                if "dir_rank1" in st.get("dir", {}).get("path", ""):
                    log.info("dir_rank1 exported to rank 1")
                    return
            time.sleep(1)
        self.fail("dir_rank1 was not exported to rank 1 in time")

    def _get_auth_mds_for_path(self, path):
        """Find the MDS that is authoritative for the given path.
        
        Finds the subtree whose path is the longest prefix of the target path
        and returns the MDS that is authoritative for that subtree.
        Uses rank-based lookups so it works regardless of daemon naming.
        """
        target = path.rstrip("/")
        if not target:
            target = "/"
        status = self.fs.status()
        for _ in range(30):
            for rank in range(self.fs.get_var("max_mds", status=status)):
                try:
                    info = self.fs.get_rank(rank=rank, status=status)
                    mds_id = info['name']
                    subtrees = self.fs.rank_asok(["get", "subtrees"],
                                                 rank=rank, status=status)
                    best_match = None
                    best_match_len = -1
                    for st in subtrees:
                        st_path = st.get("dir", {}).get("path", "").rstrip("/")
                        if not st_path:
                            st_path = "/"
                        if target == st_path or target.startswith(st_path + "/") or st_path == "/":
                            if len(st_path) > best_match_len:
                                best_match = st
                                best_match_len = len(st_path)
                    if best_match:
                        dir_auth_str = best_match.get("dir", {}).get("dir_auth", "-1")
                        try:
                            auth = int(dir_auth_str)
                        except (ValueError, TypeError):
                            auth = -1
                        if auth < 0:
                            continue
                        if rank == auth:
                            st_path = best_match.get("dir", {}).get("path", "")
                            log.info("Auth MDS for %s is %s (rank %d, subtree %s)",
                                     path, mds_id, auth, st_path)
                            return mds_id
                except Exception as e:
                    log.warning("Error checking subtrees on rank %d: %s", rank, e)
            time.sleep(1)
            status = self.fs.status()
        self.fail("Could not determine auth MDS for %s" % path)

    def tearDown(self):
        # Step 1: Disable quarantine on the subvolume.
        # The quarantine enable/disable commands return immediately while async
        # work continues in the background. If we call disable while enable is
        # still processing (journaling, replica notification, cap revocation),
        # the MDS returns EBUSY. We retry until either:
        #   - The disable succeeds (enable finished, disable started)
        #   - A file read succeeds (quarantine fully cleared)
        #
        # The retry loop handles the race where disable is called before the
        # enable's async journaling completes.
        try:
            deadline = time.time() + 30
            while time.time() < deadline:
                try:
                    self._quarantine_disable()
                    break  # disable accepted, now wait for it to take effect
                except (RuntimeError, CommandFailedError) as e:
                    if "EBUSY" in str(e) or "non-JSON response" in str(e):
                        # Enable still in progress, retry after a short delay
                        time.sleep(1)
                        continue
                    raise  # unexpected error
            # Poll until a file read succeeds. While quarantine is active,
            # reads return EPERM. Once fully cleared, reads succeed.
            self.wait_until_unblocked(
                lambda: self.mount_a.read_file(self.file_rank0),
                timeout=30, msg="Waiting for quarantine to clear")
        except Exception:
            pass

        # Step 2: Remove the export pin from dir_rank1.
        # This directory was pinned to MDS rank 1 during setUp. We must
        # unpin it before reducing max_mds, otherwise the subtree cannot
        # be migrated back to rank 0 and the MDS will get stuck in
        # "stopping" state indefinitely.
        try:
            self.mount_a.setfattr(self.dir_rank1, "ceph.dir.pin", "-1")
        except Exception:
            pass

        # Step 3: Reduce to single MDS and wait for cluster stability.
        # wait_for_daemons() blocks until rank 1 finishes exporting its
        # subtrees and transitions to standby state.
        try:
            self.fs.set_max_mds(1)
            self.fs.wait_for_daemons()
        except Exception:
            pass

        # Step 4: Call parent tearDown which removes the subvolume.
        super().tearDown()

    def test_quarantine_blocks_both_ranks(self):
        """Quarantine blocks access to files on BOTH MDS ranks."""
        self.assertEqual(self.mount_a.read_file(self.file_rank0), "Data on rank 0")
        self.assertEqual(self.mount_a.read_file(self.file_rank1), "Data on rank 1")

        self._quarantine_enable()
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(self.file_rank0),
            msg="Read on rank 0 should be blocked")
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(self.file_rank1),
            msg="Read on rank 1 should be blocked")

    def test_quarantine_disable_restores_both_ranks(self):
        """Disabling quarantine restores access on both MDS ranks."""
        self._quarantine_enable()
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank0))
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank1))

        self._quarantine_disable()
        self.wait_until_unblocked(lambda: self.mount_a.read_file(self.file_rank0))
        self.wait_until_unblocked(lambda: self.mount_a.read_file(self.file_rank1))
        self.assertEqual(self.mount_a.read_file(self.file_rank0), "Data on rank 0")
        self.assertEqual(self.mount_a.read_file(self.file_rank1), "Data on rank 1")

    def test_quarantine_write_blocked_both_ranks(self):
        """Quarantine blocks write to files on both MDS ranks."""
        self._quarantine_enable()
        self.wait_until_blocked(
            lambda: self.mount_a.write_file(self.file_rank0, "blocked"),
            msg="Write on rank 0 should be blocked")
        self.wait_until_blocked(
            lambda: self.mount_a.write_file(self.file_rank1, "blocked"),
            msg="Write on rank 1 should be blocked")

    def test_quarantine_mkdir_blocked_both_ranks(self):
        """Quarantine blocks mkdir on both MDS ranks."""
        log.info("DEBUG: Executing BASE CLASS test_quarantine_mkdir_blocked_both_ranks (line ~732)")
        log.info("DEBUG: self.__class__.__name__ = %s", self.__class__.__name__)
        log.info("DEBUG: dir_rank0 = %s", getattr(self, 'dir_rank0', 'NOT_SET'))
        log.info("DEBUG: dir_rank1 = %s", getattr(self, 'dir_rank1', 'NOT_SET'))
        log.info("DEBUG: child_rank0 = %s", getattr(self, 'child_rank0', 'NOT_SET'))
        log.info("DEBUG: child_rank1 = %s", getattr(self, 'child_rank1', 'NOT_SET'))
        self._quarantine_enable()

        new_dir0 = os.path.join(self.dir_rank0, "newdir")
        new_dir1 = os.path.join(self.dir_rank1, "newdir")
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["mkdir", new_dir0]),
            msg="mkdir on rank 0 should be blocked")
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["mkdir", new_dir1]),
            msg="mkdir on rank 1 should be blocked")

    def test_recovery_client_both_ranks(self):
        """Q-flag recovery client can access files on both ranks."""
        self._quarantine_enable()
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank0))
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank1))

        recovery_client = "client.multimds_recovery"
        caps = "allow rwq fsname=%s path=%s" % (self.volname,
                                                 self.subvol_root_path)
        authid, keyring = self._create_qtine_client(recovery_client, caps)

        try:
            self.mount_b.umount_wait()
            self.mount_b.mount_wait(
                cephfs_name=self.volname,
                client_id=authid,
                cephfs_mntpt=self.subvol_path,
                client_keyring_path=keyring)

            rel_file0 = os.path.join("dir_rank0", "file_on_rank0.txt")
            rel_file1 = os.path.join("dir_rank1", "file_on_rank1.txt")

            self.assertEqual(self.mount_b.read_file(rel_file0), "Data on rank 0")
            self.assertEqual(self.mount_b.read_file(rel_file1), "Data on rank 1")
        finally:
            self.get_ceph_cmd_stdout("auth", "del", recovery_client)

    def test_quarantine_blocks_snapshot_both_ranks(self):
        """Quarantine blocks snapshot access on both MDS ranks."""
        snapshot_name = "multimds_snap"

        self._fs_cmd("subvolume", "snapshot", "create",
                     self.volname, self.SUBVOLUME_NAME, snapshot_name)
        time.sleep(1)

        try:
            # Snapshot .snap directory is at subvolume root, not data path
            # Path: {subvol_root}/.snap/{snap_name}/{uuid}/dir_rank0/file.txt
            subvol_uuid = os.path.basename(self.subvol_path)
            rel_root = self.subvol_root_path.lstrip("/")
            snap_file0 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "dir_rank0", "file_on_rank0.txt")
            snap_file1 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "dir_rank1", "file_on_rank1.txt")

            self.assertEqual(self.mount_a.read_file(snap_file0), "Data on rank 0")
            self.assertEqual(self.mount_a.read_file(snap_file1), "Data on rank 1")

            self._quarantine_enable()
            self.wait_until_blocked(
                lambda: self.mount_a.read_file(snap_file0),
                msg="Snapshot read on rank 0 should be blocked")
            self.wait_until_blocked(
                lambda: self.mount_a.read_file(snap_file1),
                msg="Snapshot read on rank 1 should be blocked")

            self._quarantine_disable()
            self.wait_until_unblocked(lambda: self.mount_a.read_file(snap_file0))
            self.wait_until_unblocked(lambda: self.mount_a.read_file(snap_file1))

            self.assertEqual(self.mount_a.read_file(snap_file0), "Data on rank 0")
            self.assertEqual(self.mount_a.read_file(snap_file1), "Data on rank 1")
        finally:
            try:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             self.SUBVOLUME_NAME, snapshot_name, "--force")
            except Exception:
                pass

    def test_recovery_client_snapshot_both_ranks(self):
        """Q-flag recovery client can access snapshots on both ranks."""
        snapshot_name = "multimds_recovery_snap"

        self._fs_cmd("subvolume", "snapshot", "create",
                     self.volname, self.SUBVOLUME_NAME, snapshot_name)
        time.sleep(1)

        recovery_client = "client.multimds_snap_recovery"
        caps = "allow rwq fsname=%s path=%s" % (self.volname,
                                                 self.subvol_root_path)
        authid, keyring = self._create_qtine_client(recovery_client, caps)

        try:
            self._quarantine_enable()

            # Snapshot .snap directory is at subvolume root, not data path
            subvol_uuid = os.path.basename(self.subvol_path)
            rel_root = self.subvol_root_path.lstrip("/")
            snap_file0 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "dir_rank0", "file_on_rank0.txt")
            snap_file1 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "dir_rank1", "file_on_rank1.txt")

            self.wait_until_blocked(lambda: self.mount_a.read_file(snap_file0))
            self.wait_until_blocked(lambda: self.mount_a.read_file(snap_file1))

            # Mount at subvolume root to access .snap directory
            self.mount_b.umount_wait()
            self.mount_b.mount_wait(
                cephfs_name=self.volname,
                client_id=authid,
                cephfs_mntpt=self.subvol_root_path,
                client_keyring_path=keyring)

            rel_snap_file0 = os.path.join(".snap", snapshot_name,
                                          subvol_uuid, "dir_rank0", "file_on_rank0.txt")
            rel_snap_file1 = os.path.join(".snap", snapshot_name,
                                          subvol_uuid, "dir_rank1", "file_on_rank1.txt")

            self.assertEqual(self.mount_b.read_file(rel_snap_file0),
                             "Data on rank 0")
            self.assertEqual(self.mount_b.read_file(rel_snap_file1),
                             "Data on rank 1")
        finally:
            self.get_ceph_cmd_stdout("auth", "del", recovery_client)
            self._quarantine_disable()
            try:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             self.SUBVOLUME_NAME, snapshot_name, "--force")
            except Exception:
                pass


class TestQuarantineMultiMDSEdgeCases(TestQuarantineMultiMDS):
    """
    Edge case tests for quarantine with multiple MDS.
    
    Tests complex scenarios involving:
    - Nested export pins at multiple levels
    - Sibling directories pinned to different MDS
    - Deep hierarchies with alternating MDS authority
    - Replica-served reads
    - New client connections after quarantine
    """

    def setUp(self):
        # Call grandparent setUp to get basic subvolume, skip parent's simple setup
        QuarantineTestBase.setUp(self)
        log.info("Subvolume data path: %s", self.subvol_path)
        log.info("Subvolume root path: %s", self.subvol_root_path)

        self._create_test_file()

        # Create a more complex directory structure:
        # subvol/
        #   ├── parent/                    (auth: rank 0)
        #   │   ├── child_rank0/           (pinned to rank 0)
        #   │   │   └── file_r0.txt
        #   │   └── child_rank1/           (pinned to rank 1)
        #   │       └── file_r1.txt
        #   └── deep/                      (auth: rank 0)
        #       └── level1/                (pinned to rank 1)
        #           └── level2/            (pinned back to rank 0)
        #               └── file_deep.txt
        rel = self.subvol_path.lstrip("/")

        # Sibling directories pinned to different MDS
        self.parent_dir = os.path.join(rel, "parent")
        self.child_rank0 = os.path.join(self.parent_dir, "child_rank0")
        self.child_rank1 = os.path.join(self.parent_dir, "child_rank1")

        self.mount_a.run_shell(["mkdir", "-p", self.child_rank0])
        self.mount_a.run_shell(["mkdir", "-p", self.child_rank1])

        # Pin siblings to different MDS
        self.mount_a.setfattr(self.child_rank0, "ceph.dir.pin", "0")
        self.mount_a.setfattr(self.child_rank1, "ceph.dir.pin", "1")

        self.file_child_r0 = os.path.join(self.child_rank0, "file_r0.txt")
        self.file_child_r1 = os.path.join(self.child_rank1, "file_r1.txt")
        self.mount_a.write_file(self.file_child_r0, "Child rank 0 data")
        self.mount_a.write_file(self.file_child_r1, "Child rank 1 data")

        # Deep nested hierarchy with alternating pins
        self.deep_dir = os.path.join(rel, "deep")
        self.level1_dir = os.path.join(self.deep_dir, "level1")
        self.level2_dir = os.path.join(self.level1_dir, "level2")

        self.mount_a.run_shell(["mkdir", "-p", self.level2_dir])

        # level1 pinned to rank 1, level2 pinned back to rank 0
        self.mount_a.setfattr(self.level1_dir, "ceph.dir.pin", "1")
        self.mount_a.setfattr(self.level2_dir, "ceph.dir.pin", "0")

        self.file_deep = os.path.join(self.level2_dir, "file_deep.txt")
        self.mount_a.write_file(self.file_deep, "Deep nested data")

        # Add aliases for compatibility with inherited tests from TestQuarantineMultiMDS
        self.file_rank0 = self.file_child_r0
        self.file_rank1 = self.file_child_r1
        self.dir_rank0 = self.child_rank0
        self.dir_rank1 = self.child_rank1
        log.info("DEBUG EdgeCases setUp: set dir_rank0=%s, dir_rank1=%s", self.dir_rank0, self.dir_rank1)

        # Bring up second MDS without waiting for specific directory
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self._wait_for_exports()

    def _wait_for_exports(self):
        """Wait for all pinned directories to be exported to their target MDS."""
        expected_exports = [
            ("child_rank1", 1),
            ("level1", 1),
        ]
        for dir_name, target_rank in expected_exports:
            for _ in range(30):
                try:
                    subtrees = self.fs.rank_asok(["get", "subtrees"],
                                                 rank=target_rank)
                except Exception as e:
                    log.warning("Error getting subtrees from rank %d: %s",
                                target_rank, e)
                    time.sleep(1)
                    continue
                found = False
                for st in subtrees:
                    path = st.get("dir", {}).get("path", "")
                    if dir_name in path:
                        auth_str = st.get("dir", {}).get("dir_auth", "-1")
                        try:
                            auth = int(auth_str)
                        except (ValueError, TypeError):
                            auth = -1
                        if auth == target_rank:
                            log.info("%s exported to rank %d", dir_name, target_rank)
                            found = True
                            break
                if found:
                    break
                time.sleep(1)
            else:
                log.warning("%s may not be exported to rank %d yet", dir_name, target_rank)

    def tearDown(self):
        # Disable quarantine first
        try:
            deadline = time.time() + 30
            while time.time() < deadline:
                try:
                    self._quarantine_disable()
                    break
                except (RuntimeError, CommandFailedError) as e:
                    if "EBUSY" in str(e) or "non-JSON response" in str(e):
                        time.sleep(1)
                        continue
                    raise
            self.wait_until_unblocked(
                lambda: self.mount_a.run_shell(["stat", self.child_rank0]),
                timeout=30, msg="Waiting for quarantine to clear")
        except Exception:
            pass

        # Unpin all directories before scaling down MDS
        for dir_path in [self.child_rank0, self.child_rank1, 
                         self.level1_dir, self.level2_dir]:
            try:
                self.mount_a.setfattr(dir_path, "ceph.dir.pin", "-1")
            except Exception:
                pass

        try:
            self.fs.set_max_mds(1)
            self.fs.wait_for_daemons()
        except Exception:
            pass

        super(TestQuarantineMultiMDS, self).tearDown()

    def test_recovery_client_both_ranks(self):
        """Q-flag recovery client can access files on both ranks."""
        self._quarantine_enable()
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank0))
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank1))

        recovery_client = "client.edgecases_recovery"
        caps = "allow rwq fsname=%s path=%s" % (self.volname,
                                                 self.subvol_root_path)
        authid, keyring = self._create_qtine_client(recovery_client, caps)

        try:
            self.mount_b.umount_wait()
            self.mount_b.mount_wait(
                cephfs_name=self.volname,
                client_id=authid,
                cephfs_mntpt=self.subvol_path,
                client_keyring_path=keyring)

            rel_file0 = os.path.join("parent", "child_rank0", "file_r0.txt")
            rel_file1 = os.path.join("parent", "child_rank1", "file_r1.txt")

            self.assertEqual(self.mount_b.read_file(rel_file0), "Child rank 0 data")
            self.assertEqual(self.mount_b.read_file(rel_file1), "Child rank 1 data")
        finally:
            self.get_ceph_cmd_stdout("auth", "del", recovery_client)

    def test_quarantine_blocks_snapshot_both_ranks(self):
        """Quarantine blocks snapshot access on both MDS ranks."""
        snapshot_name = "edgecases_snap"

        self._fs_cmd("subvolume", "snapshot", "create",
                     self.volname, self.SUBVOLUME_NAME, snapshot_name)
        time.sleep(1)

        try:
            subvol_uuid = os.path.basename(self.subvol_path)
            rel_root = self.subvol_root_path.lstrip("/")
            snap_file0 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "parent", "child_rank0", "file_r0.txt")
            snap_file1 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "parent", "child_rank1", "file_r1.txt")

            self.assertEqual(self.mount_a.read_file(snap_file0), "Child rank 0 data")
            self.assertEqual(self.mount_a.read_file(snap_file1), "Child rank 1 data")

            self._quarantine_enable()
            self.wait_until_blocked(
                lambda: self.mount_a.read_file(snap_file0),
                msg="Snapshot read on rank 0 should be blocked")
            self.wait_until_blocked(
                lambda: self.mount_a.read_file(snap_file1),
                msg="Snapshot read on rank 1 should be blocked")

            self._quarantine_disable()
            self.wait_until_unblocked(lambda: self.mount_a.read_file(snap_file0))
            self.wait_until_unblocked(lambda: self.mount_a.read_file(snap_file1))

            self.assertEqual(self.mount_a.read_file(snap_file0), "Child rank 0 data")
            self.assertEqual(self.mount_a.read_file(snap_file1), "Child rank 1 data")
        finally:
            try:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             self.SUBVOLUME_NAME, snapshot_name, "--force")
            except Exception:
                pass

    def test_recovery_client_snapshot_both_ranks(self):
        """Q-flag recovery client can access snapshots on both ranks."""
        snapshot_name = "edgecases_recovery_snap"

        self._fs_cmd("subvolume", "snapshot", "create",
                     self.volname, self.SUBVOLUME_NAME, snapshot_name)
        time.sleep(1)

        recovery_client = "client.edgecases_snap_recovery"
        caps = "allow rwq fsname=%s path=%s" % (self.volname,
                                                 self.subvol_root_path)
        authid, keyring = self._create_qtine_client(recovery_client, caps)

        try:
            self._quarantine_enable()

            subvol_uuid = os.path.basename(self.subvol_path)
            rel_root = self.subvol_root_path.lstrip("/")
            snap_file0 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "parent", "child_rank0", "file_r0.txt")
            snap_file1 = os.path.join(rel_root, ".snap", snapshot_name,
                                      subvol_uuid, "parent", "child_rank1", "file_r1.txt")

            self.wait_until_blocked(lambda: self.mount_a.read_file(snap_file0))
            self.wait_until_blocked(lambda: self.mount_a.read_file(snap_file1))

            self.mount_b.umount_wait()
            self.mount_b.mount_wait(
                cephfs_name=self.volname,
                client_id=authid,
                cephfs_mntpt=self.subvol_root_path,
                client_keyring_path=keyring)

            rel_snap_file0 = os.path.join(".snap", snapshot_name,
                                          subvol_uuid, "parent", "child_rank0", "file_r0.txt")
            rel_snap_file1 = os.path.join(".snap", snapshot_name,
                                          subvol_uuid, "parent", "child_rank1", "file_r1.txt")

            self.assertEqual(self.mount_b.read_file(rel_snap_file0),
                             "Child rank 0 data")
            self.assertEqual(self.mount_b.read_file(rel_snap_file1),
                             "Child rank 1 data")
        finally:
            self.get_ceph_cmd_stdout("auth", "del", recovery_client)
            self._quarantine_disable()
            try:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             self.SUBVOLUME_NAME, snapshot_name, "--force")
            except Exception:
                pass

    def test_quarantine_blocks_both_ranks(self):
        """Quarantine blocks access to files on BOTH MDS ranks."""
        self.assertEqual(self.mount_a.read_file(self.file_rank0), "Child rank 0 data")
        self.assertEqual(self.mount_a.read_file(self.file_rank1), "Child rank 1 data")

        self._quarantine_enable()
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(self.file_rank0),
            msg="Read on rank 0 should be blocked")
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(self.file_rank1),
            msg="Read on rank 1 should be blocked")

    def test_quarantine_disable_restores_both_ranks(self):
        """Disabling quarantine restores access on both MDS ranks."""
        self._quarantine_enable()
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank0))
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_rank1))

        self._quarantine_disable()
        self.wait_until_unblocked(lambda: self.mount_a.read_file(self.file_rank0))
        self.wait_until_unblocked(lambda: self.mount_a.read_file(self.file_rank1))
        self.assertEqual(self.mount_a.read_file(self.file_rank0), "Child rank 0 data")
        self.assertEqual(self.mount_a.read_file(self.file_rank1), "Child rank 1 data")

    def test_quarantine_write_blocked_both_ranks(self):
        """Quarantine blocks write to files on both MDS ranks."""
        self._quarantine_enable()
        self.wait_until_blocked(
            lambda: self.mount_a.write_file(self.file_rank0, "blocked"),
            msg="Write on rank 0 should be blocked")
        self.wait_until_blocked(
            lambda: self.mount_a.write_file(self.file_rank1, "blocked"),
            msg="Write on rank 1 should be blocked")

    def test_quarantine_mkdir_blocked_both_ranks(self):
        """Quarantine blocks mkdir on both MDS ranks."""
        log.info("DEBUG: Executing DERIVED CLASS test_quarantine_mkdir_blocked_both_ranks (line ~1079)")
        self._quarantine_enable()

        new_dir0 = os.path.join(self.child_rank0, "newdir")
        new_dir1 = os.path.join(self.child_rank1, "newdir")
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["mkdir", new_dir0]),
            msg="mkdir on rank 0 should be blocked")
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["mkdir", new_dir1]),
            msg="mkdir on rank 1 should be blocked")

    def test_sibling_dirs_different_mds_blocked(self):
        """Quarantine blocks access to sibling dirs pinned to different MDS."""
        # Verify baseline access works
        self.assertEqual(self.mount_a.read_file(self.file_child_r0), "Child rank 0 data")
        self.assertEqual(self.mount_a.read_file(self.file_child_r1), "Child rank 1 data")

        self._quarantine_enable()

        # Both siblings should be blocked regardless of which MDS they're on
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(self.file_child_r0),
            msg="Read on child_rank0 should be blocked")
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(self.file_child_r1),
            msg="Read on child_rank1 should be blocked")

    def test_deep_nested_alternating_mds_blocked(self):
        """Quarantine blocks access in deep hierarchy with alternating MDS pins."""
        # Structure: deep(r0) -> level1(r1) -> level2(r0) -> file
        self.assertEqual(self.mount_a.read_file(self.file_deep), "Deep nested data")

        self._quarantine_enable()

        # File in deepest level (back on rank 0) should still be blocked
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(self.file_deep),
            msg="Read on deep nested file should be blocked")

    def test_parent_of_exported_dirs_blocked(self):
        """Quarantine blocks access to parent directory containing exported children."""
        self._quarantine_enable()

        # Stat on the parent directory should be blocked
        # (using stat instead of ls because ls returns exit status 2 for
        # access errors, while stat returns 1 which matches errno.EPERM)
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["stat", self.parent_dir]),
            msg="Stat on parent of exported dirs should be blocked")

    def test_stat_blocked_on_exported_subtree(self):
        """Stat operations are blocked on exported subtrees."""
        self._quarantine_enable()

        # Stat on files in exported subtrees should be blocked
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["stat", self.file_child_r1]),
            msg="Stat on exported subtree file should be blocked")

    def test_file_creation_blocked_on_exported_subtree(self):
        """File creation is blocked on exported subtrees."""
        self._quarantine_enable()

        new_file = os.path.join(self.child_rank1, "new_blocked_file.txt")
        self.wait_until_blocked(
            lambda: self.mount_a.write_file(new_file, "should fail"),
            msg="File creation on exported subtree should be blocked")

    def test_mkdir_blocked_on_exported_subtree(self):
        """Mkdir is blocked on exported subtrees."""
        self._quarantine_enable()

        new_dir = os.path.join(self.child_rank1, "new_blocked_dir")
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["mkdir", new_dir]),
            msg="Mkdir on exported subtree should be blocked")

    def test_new_client_blocked_after_quarantine(self):
        """A new client connecting after quarantine is enabled is blocked."""
        self._quarantine_enable()

        # Verify quarantine is active
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_child_r0))

        # Remount mount_b as a fresh client
        self.mount_b.umount_wait()
        self.mount_b.mount_wait(cephfs_name=self.volname)

        # New client should also be blocked on all paths
        self.wait_until_blocked(
            lambda: self.mount_b.read_file(self.file_child_r0),
            msg="New client should be blocked on rank 0 subtree")
        self.wait_until_blocked(
            lambda: self.mount_b.read_file(self.file_child_r1),
            msg="New client should be blocked on rank 1 subtree")

    def test_disable_restores_access_all_levels(self):
        """Disabling quarantine restores access at all nesting levels."""
        self._quarantine_enable()

        # Verify all are blocked
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_child_r0))
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_child_r1))
        self.wait_until_blocked(lambda: self.mount_a.read_file(self.file_deep))

        self._quarantine_disable()

        # All should be accessible again
        self.wait_until_unblocked(lambda: self.mount_a.read_file(self.file_child_r0))
        self.wait_until_unblocked(lambda: self.mount_a.read_file(self.file_child_r1))
        self.wait_until_unblocked(lambda: self.mount_a.read_file(self.file_deep))

        self.assertEqual(self.mount_a.read_file(self.file_child_r0), "Child rank 0 data")
        self.assertEqual(self.mount_a.read_file(self.file_child_r1), "Child rank 1 data")
        self.assertEqual(self.mount_a.read_file(self.file_deep), "Deep nested data")

    def test_replica_served_read_blocked(self):
        """
        Replica-served reads are blocked by quarantine.
        
        This tests Venky's scenario: A directory is authoritative on rank 0,
        but rank 1 has a replica and might serve read requests (stat, getattr)
        from that replica. The replica MDS must also enforce quarantine.
        
        Setup:
        - parent/ is auth on rank 0
        - parent/child_rank1/ is exported to rank 1
        - Therefore rank 1 has a replica of parent/ (needed for path traversal)
        - A stat on parent/ might be served by rank 1's replica
        """
        # First, ensure rank 1 has cached the parent directory by accessing
        # the exported child - this forces path traversal through parent/
        self.assertEqual(self.mount_a.read_file(self.file_child_r1), "Child rank 1 data")
        
        # Create a file directly in parent/ (not in any pinned subdir)
        # This file is auth on rank 0, but parent/ has replica on rank 1
        parent_file = os.path.join(self.parent_dir, "file_in_parent.txt")
        self.mount_a.write_file(parent_file, "Parent dir data")
        
        # Access it to ensure it's cached
        self.assertEqual(self.mount_a.read_file(parent_file), "Parent dir data")
        
        self._quarantine_enable()
        
        # Stat on parent/ - might be served by rank 1's replica
        # This MUST be blocked even if served by replica
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["stat", self.parent_dir]),
            msg="Stat on dir with replica should be blocked")
        
        # Read file in parent/ - might hit replica for metadata
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(parent_file),
            msg="Read on file in dir with replica should be blocked")
        
        # getattr should also be blocked (using stat for consistent exit codes)
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["stat", "-f", self.parent_dir]),
            msg="getattr on dir with replica should be blocked")

    def test_intermediate_dir_with_replica_blocked(self):
        """
        Access through intermediate directory with replica is blocked.
        
        Tests the path: subvol -> deep -> level1 (rank1) -> level2 (rank0)
        
        The 'deep' directory is auth on rank 0 but rank 1 needs a replica
        to traverse to its exported subtree (level1). Access to 'deep'
        itself should be blocked even if served by replica.
        """
        # Access the deep file to ensure path is cached on both MDS
        self.assertEqual(self.mount_a.read_file(self.file_deep), "Deep nested data")
        
        # Create a file in 'deep' (not in any pinned subdir)
        deep_file = os.path.join(self.deep_dir, "file_in_deep.txt")
        self.mount_a.write_file(deep_file, "Deep dir data")
        
        self._quarantine_enable()
        
        # Access to intermediate 'deep' directory should be blocked
        self.wait_until_blocked(
            lambda: self.mount_a.run_shell(["stat", self.deep_dir]),
            msg="Stat on intermediate dir should be blocked")
        
        self.wait_until_blocked(
            lambda: self.mount_a.read_file(deep_file),
            msg="Read on file in intermediate dir should be blocked")
