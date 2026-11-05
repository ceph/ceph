"""
Tests of ceph.file.migrate.pool: moving a file's data to another data pool.

Each test works in its own directory, whose files are laid out on a replicated
pool, and migrates them to an EC pool or back.  A client can see only the file
contents, so most checks read the file, and its snapshots, from both clients.
What the pools hold is checked with rados and ceph df.
"""

import errno
import hashlib
import json
import logging
import os
import time
from io import BytesIO, StringIO
from textwrap import dedent

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

MB = 1 << 20

FAST = "migrate_fast"
EC = "migrate_ec"
EC_PROFILE = "migrate_ec_profile"

MIGRATE = "ceph.file.migrate.pool"
DIR_POOL = "ceph.dir.layout.pool"
FILE_POOL = "ceph.file.layout.pool"

# Run on a client: a process that stops at each step() until the test says go,
# so that it can hold a file open, or data unflushed, across a migration.
STEPPER = dedent("""
    import os
    import sys
    import time

    def step(name):
        open(os.path.join({ctl!r}, name + ".ready"), "w").close()
        go = os.path.join({ctl!r}, name + ".go")
        deadline = time.time() + 300
        while not os.path.exists(go):
            if time.time() > deadline:
                sys.exit(name + ": nobody said go")
            time.sleep(0.2)

    def pattern(tag, n):
        return (tag * (n // len(tag) + 1))[:n]
""")


def pattern(tag, n):
    return (tag * (n // len(tag) + 1))[:n]


class TestFileMigrate(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2  # a standby, so that a dead MDS fails over

    def setUp(self):
        super().setUp()
        self.fs.add_data_pool(FAST)
        self.run_ceph_cmd("osd", "erasure-code-profile", "set", EC_PROFILE,
                          "k=2", "m=1", "crush-failure-domain=osd", "--force")
        self.run_ceph_cmd("osd", "pool", "create", EC, "32", "32", "erasure",
                          EC_PROFILE)
        self.run_ceph_cmd("osd", "pool", "set", EC, "allow_ec_overwrites", "true")
        self.fs.add_data_pool(EC, create=False)
        self.mount_a.run_shell(["mkdir", "ctl"])

    # --- helpers ----------------------------------------------------------

    def _dir(self, name):
        self.mount_a.run_shell(["mkdir", name])
        self.mount_a.setfattr(name, DIR_POOL, FAST)
        return name

    def _write(self, path, off, data_tag, n, mount=None):
        """Write and fsync a pattern; returns the bytes written."""
        mount = mount or self.mount_a
        mount.run_python(STEPPER.format(ctl="") + dedent(f"""
            fd = os.open({os.path.join(mount.hostfs_mntpt, path)!r},
                         os.O_WRONLY | os.O_CREAT, 0o644)
            os.pwrite(fd, pattern({data_tag!r}, {n}), {off})
            os.fsync(fd)
            os.close(fd)
        """))
        return pattern(data_tag, n)

    def _truncate(self, path, size):
        self.mount_a.run_shell(["truncate", "-s", str(size), path])

    def _sum(self, path, mount):
        out = mount.run_shell(["sha256sum", path]).stdout.getvalue()
        return out.split()[0]

    def _assert_reads(self, path, want, msg=None, mounts=None):
        """Both clients read want from path, one of them often with a cache."""
        digest = hashlib.sha256(want).hexdigest()
        for mount in mounts or (self.mount_a, self.mount_b):
            self.assertEqual(self._sum(path, mount), digest,
                             "%s on client %s" % (msg or path, mount.client_id))

    def _pool_of(self, path):
        return self.mount_a.getfattr(path, FILE_POOL)

    def _ino(self, path):
        return self.mount_a.path_to_ino(path)

    def _migrate(self, path, pool, mount=None):
        (mount or self.mount_a).setfattr(path, MIGRATE, pool)

    def _objects(self, pool, ino):
        prefix = "%x." % ino
        out = self.fs.rados(["ls"], pool=pool, stdout=StringIO())
        return sorted(o for o in out.stdout.getvalue().split()
                      if o.startswith(prefix))

    def _clones(self, pool, oid):
        """Snap ids of the clones of oid, except the head."""
        out = self.fs.rados(["listsnaps", oid], pool=pool, stdout=StringIO(),
                            check_status=False)
        ids = []
        for line in out.stdout.getvalue().splitlines()[2:]:
            fields = line.split()
            if fields and fields[0] != "head":
                ids.append(fields[0])
        return ids

    def _stored(self, pool):
        df = json.loads(self.get_ceph_cmd_stdout("df", "--format=json"))
        for p in df["pools"]:
            if p["name"] == pool:
                return p["stats"]["stored"]
        raise RuntimeError("no pool " + pool)

    def _settled_stored(self, pool):
        """The pool's stored bytes, once ceph df has stopped moving."""
        last = self._stored(pool)
        for _ in range(12):
            time.sleep(5)
            cur = self._stored(pool)
            if cur == last:
                return cur
            last = cur
        return last

    def _wait_freed(self, pool, before, n, timeout=120):
        self.wait_until_true(lambda: before - self._stored(pool) >= n, timeout)

    def _wait_no_objects(self, pool, ino, timeout=120):
        self.wait_until_equal(lambda: self._objects(pool, ino), [], timeout)

    def _start(self, mount, body):
        """Run body on the client; returns its process."""
        return mount._run_python(STEPPER.format(
            ctl=os.path.join(mount.hostfs_mntpt, "ctl")) + dedent(body))

    def _wait_step(self, name):
        self.wait_until_true(
            lambda: self.mount_a.run_shell(
                ["ls", "ctl/%s.ready" % name],
                check_status=False).exitstatus == 0,
            60, period=1)

    def _go(self, name):
        self.mount_a.run_shell(["touch", "ctl/%s.go" % name])

    def _failover(self):
        self.fs.rank_fail(rank=0)
        self.fs.wait_for_daemons()

    # --- the file itself --------------------------------------------------

    def test_plain_file_and_back(self):
        """A file with no snapshots moves there and back, inode unchanged."""
        d = self._dir("plain")
        data = self._write(d + "/f", 0, b"pl-", 9 * MB)
        ino = self._ino(d + "/f")

        self._migrate(d + "/f", EC)
        self.assertEqual(self._pool_of(d + "/f"), EC)
        self.assertEqual(self._ino(d + "/f"), ino)
        self._assert_reads(d + "/f", data)
        self.assertEqual(len(self._objects(EC, ino)), 3)
        # Only the first object stays in the old pool, for the backtrace.
        first = "%x.00000000" % ino
        self.wait_until_equal(lambda: self._objects(FAST, ino), [first], 30)

        self._migrate(d + "/f", FAST)
        self.assertEqual(self._pool_of(d + "/f"), FAST)
        self._assert_reads(d + "/f", data)

    def test_same_pool_is_a_noop(self):
        d = self._dir("noop")
        data = self._write(d + "/f", 0, b"no-", MB)
        self._migrate(d + "/f", FAST)
        self.assertEqual(self._pool_of(d + "/f"), FAST)
        self._assert_reads(d + "/f", data)

    def test_empty_file(self):
        d = self._dir("empty")
        self.mount_a.run_shell(["touch", d + "/f"])
        self._migrate(d + "/f", EC)
        self.assertEqual(self._pool_of(d + "/f"), EC)
        self._assert_reads(d + "/f", b"")

    def test_refused(self):
        d = self._dir("refused")
        self._write(d + "/f", 0, b"re-", MB)
        for path, pool in ((d, EC), (d + "/f", "no_such_pool")):
            with self.assertRaises(CommandFailedError):
                self._migrate(path, pool)
        self.assertEqual(self._pool_of(d + "/f"), FAST)

    def test_buffered_writes_of_another_client(self):
        """
        Data that another client still buffers, including beyond the end of the
        file, must be flushed and reach the new pool, and its open fd must
        keep working afterwards.
        """
        d = self._dir("buffered")
        size = 10 * MB + 12345
        data = bytearray(self._write(d + "/f", 0, b"v1-", size))
        ino = self._ino(d + "/f")

        tail = pattern(b"bb-", 100)
        ext = pattern(b"ex-", 6 * MB)
        more = pattern(b"cc-", 4096)
        path = os.path.join(self.mount_b.hostfs_mntpt, d, "f")
        p = self._start(self.mount_b, f"""
            fd = os.open({path!r}, os.O_WRONLY)
            os.pwrite(fd, pattern(b"bb-", 100), {size - 100})
            os.pwrite(fd, pattern(b"ex-", {6 * MB}), {size})  # no fsync
            step("buffered")
            os.pwrite(fd, pattern(b"cc-", 4096), 0)
            os.fsync(fd)
            os.close(fd)
        """)
        self._wait_step("buffered")

        self._migrate(d + "/f", EC)

        data[size - 100:] = tail
        data += ext
        self.assertEqual(self._pool_of(d + "/f"), EC)
        self.assertEqual(self._ino(d + "/f"), ino)
        self.assertEqual(self.mount_a.stat(d + "/f")["st_size"], len(data))
        self._assert_reads(d + "/f", bytes(data))
        self.assertEqual(len(self._objects(EC, ino)), 5)

        self._go("buffered")
        p.wait()
        data[:len(more)] = more
        # Not mount_a: ceph-fuse has been seen to keep serving what it read
        # just before this write from the kernel's page cache.
        self._assert_reads(d + "/f", bytes(data), "write through a held fd",
                           mounts=[self.mount_b])

        # That last write went to the new pool and nothing arrived late in the
        # old one.
        first = "%x.00000000" % ino
        got = self.fs.rados(["get", first, "-"], pool=EC, stdout=BytesIO())
        self.assertEqual(got.stdout.getvalue()[:len(more)], more)
        self.wait_until_equal(lambda: self._objects(FAST, ino), [first], 30)

    def test_deleted_file_leaves_nothing(self):
        """Purging a file also removes the redirects it left behind."""
        d = self._dir("deleted")
        data = self._write(d + "/f", 0, b"rt-", 9 * MB)
        ino = self._ino(d + "/f")
        self.mount_a.run_shell(["mkdir", d + "/.snap/s1"])
        for pool in (EC, FAST, EC):
            self._migrate(d + "/f", pool)
            self._assert_reads(d + "/f", data)
            self._assert_reads(d + "/.snap/s1/f", data)
        self.mount_a.run_shell(["rmdir", d + "/.snap/s1"])
        self.mount_a.run_shell(["rm", d + "/f"])
        self._wait_no_objects(FAST, ino)
        self._wait_no_objects(EC, ino)

    def test_unlink_during_cleanup(self):
        """The old pool is cleared in the background; unlink can overtake it."""
        d = self._dir("unlink")
        v1 = self._write(d + "/f", 0, b"ul-", 40 * MB)
        ino = self._ino(d + "/f")
        self.mount_a.run_shell(["mkdir", d + "/.snap/s1"])
        self._write(d + "/f", 0, b"u2-", 8 * MB)
        self._migrate(d + "/f", EC)
        self.mount_a.run_shell(["rm", d + "/f"])
        self._assert_reads(d + "/.snap/s1/f", v1)
        self.mount_a.run_shell(["rmdir", d + "/.snap/s1"])
        self._wait_no_objects(FAST, ino)
        self._wait_no_objects(EC, ino)

    # --- snapshots that move --------------------------------------------

    def test_history_moves(self):
        """
        Every snapshot version differs, and all of it moves: same contents at
        every snapshot, the old pool emptied, and a reader that opened a
        snapshot before the move can carry on with the old layout.
        """
        d = self._dir("history")
        h1 = self._write(d + "/f", 0, b"h1-", 13 * MB)
        ino = self._ino(d + "/f")
        self.mount_a.run_shell(["mkdir", d + "/.snap/a1"])
        h2 = self._write(d + "/f", 0, b"h2-", MB) + h1[MB:]
        self.mount_a.run_shell(["mkdir", d + "/.snap/a2"])
        h3 = h2 + self._write(d + "/f", len(h2), b"h3-", 6 * MB)  # new objects
        self.mount_a.run_shell(["mkdir", d + "/.snap/a3"])
        h4 = h3[:5 * MB] + self._write(d + "/f", 5 * MB, b"h4-", MB) + h3[6 * MB:]
        versions = (("a1", h1), ("a2", h2), ("a3", h3))

        path = os.path.join(self.mount_b.hostfs_mntpt, d, ".snap/a1/f")
        p = self._start(self.mount_b, f"""
            fd = os.open({path!r}, os.O_RDONLY)
            want = pattern(b"h1-", {13 * MB})
            if os.pread(fd, 4096, 0) != want[:4096]:
                sys.exit("stale reader: first read")
            step("stale")
            if os.pread(fd, {2 * MB}, {5 * MB}) != want[{5 * MB}:{7 * MB}]:
                sys.exit("stale reader: same object after the move")
            if os.pread(fd, {MB}, {9 * MB}) != want[{9 * MB}:{10 * MB}]:
                sys.exit("stale reader: another object after the move")
            step("stale2")
            if os.pread(fd, {MB}, {3 * MB}) != want[{3 * MB}:{4 * MB}]:
                sys.exit("stale reader: after the way back")
        """)
        self._wait_step("stale")

        before = self._settled_stored(FAST)
        ctime = self.mount_a.stat(d + "/.snap/a1/f")["st_ctime"]
        self._migrate(d + "/f", EC)

        self.assertEqual(self._pool_of(d + "/f"), EC)
        self._assert_reads(d + "/f", h4)
        for snap, want in versions:
            path_ = d + "/.snap/%s/f" % snap
            self.assertEqual(self._pool_of(path_), EC, path_)
            self._assert_reads(path_, want)
        self.assertEqual(self.mount_a.stat(d + "/.snap/a1/f")["st_ctime"], ctime)
        first = "%x.00000000" % ino
        self.assertEqual(len(self._clones(EC, first)), 1)

        self._go("stale")
        self._wait_step("stale2")
        self._wait_freed(FAST, before, 19 * MB)

        # A write now clones in the new pool, not the old.
        self._write(d + "/f", 0, b"Z", 4096)
        self._assert_reads(d + "/.snap/a1/f", h1)
        self._assert_reads(d + "/.snap/a3/f", h3)

        # Back through the redirects.
        self._migrate(d + "/f", FAST)
        for snap, want in versions:
            path_ = d + "/.snap/%s/f" % snap
            self.assertEqual(self._pool_of(path_), FAST, path_)
            self._assert_reads(path_, want)
        self._go("stale2")
        p.wait()

        self._failover()
        self._assert_reads(d + "/.snap/a2/f", h2, "after a failover")

        for snap, _ in versions:
            self.mount_a.run_shell(["rmdir", d + "/.snap/" + snap])
        self.mount_a.run_shell(["rm", d + "/f"])
        self._wait_no_objects(FAST, ino)
        self._wait_no_objects(EC, ino)

    def test_cold_file(self):
        """A file unchanged since its snapshots: a history of one."""
        d = self._dir("cold")
        size = 10 * MB + 777
        data = self._write(d + "/f", 0, b"cd-", size)
        ino = self._ino(d + "/f")
        self.mount_a.run_shell(["mkdir", d + "/.snap/c1"])
        self.mount_a.run_shell(["mkdir", d + "/.snap/c2"])

        before = self._settled_stored(FAST)
        self._migrate(d + "/f", EC)
        for path in (d + "/f", d + "/.snap/c1/f", d + "/.snap/c2/f"):
            self.assertEqual(self._pool_of(path), EC, path)
            self._assert_reads(path, data)

        self.fs.rank_asok(["flush", "journal"], rank=0)
        bt = self.fs.rados(["getxattr", "%x.00000000" % ino, "parent"], pool=EC,
                           stdout=BytesIO())
        self.assertGreater(len(bt.stdout.getvalue()), 0, "backtrace in the new pool")
        self._wait_freed(FAST, before, 10 * MB)

        # Snapshots keep working once the head moves on.
        self._write(d + "/f", MB, b"nw-", MB)
        self._assert_reads(d + "/.snap/c1/f", data)
        first = "%x.00000000" % ino
        self.assertNotEqual(self._clones(EC, first), [])

        self._failover()
        self.assertEqual(self._pool_of(d + "/.snap/c2/f"), EC)
        self._assert_reads(d + "/.snap/c2/f", data)

        self.mount_a.run_shell(["rmdir", d + "/.snap/c1", d + "/.snap/c2"])
        self.wait_until_equal(lambda: self._clones(EC, first), [], 120)

    def test_object_written_after_the_snapshot(self):
        """A hole at the snapshot must stay a hole, whatever the head has."""
        d = self._dir("hole")
        self._write(d + "/f", 0, b"e1-", MB)
        self._truncate(d + "/f", 12 * MB)
        self.mount_a.run_shell(["mkdir", d + "/.snap/e1"])
        late = self._write(d + "/f", 9 * MB, b"e2-", MB)
        self._migrate(d + "/f", EC)

        self.assertEqual(self._pool_of(d + "/.snap/e1/f"), EC)
        want = pattern(b"e1-", MB) + b"\0" * (11 * MB)
        self._assert_reads(d + "/.snap/e1/f", want)
        self._assert_reads(d + "/f", want[:9 * MB] + late + want[10 * MB:])

    # --- snapshots that stay --------------------------------------------

    def _assert_head_moved_alone(self, path, snap_path, data):
        self.assertEqual(self._pool_of(path), EC)
        self.assertEqual(self._pool_of(snap_path), FAST)
        self._assert_reads(path, data)
        self._assert_reads(snap_path, data)

    def test_hard_link_moves_head_only(self):
        d = self._dir("hardlink")
        data = self._write(d + "/f", 0, b"h1-", MB)
        self.mount_a.run_shell(["ln", d + "/f", d + "/g"])
        self.mount_a.run_shell(["mkdir", d + "/.snap/s"])
        self._migrate(d + "/f", EC)
        self._assert_head_moved_alone(d + "/f", d + "/.snap/s/f", data)
        self._assert_reads(d + "/.snap/s/g", data)

    def test_rename_moves_head_only(self):
        d = self._dir("rename")
        data = self._write(d + "/old", 0, b"rn-", 5 * MB)
        self.mount_a.run_shell(["mkdir", d + "/.snap/s"])
        self.mount_a.run_shell(["mv", d + "/old", d + "/new"])
        self._migrate(d + "/new", EC)
        self._assert_head_moved_alone(d + "/new", d + "/.snap/s/old", data)

    def test_new_file_near_a_snapshot_moves_head_only(self):
        """
        A snapshot within an hour before a file's birth might have seen an
        older version, so the file's other snapshots stay too.
        """
        d = self._dir("margin")
        self._write(d + "/before", 0, b"xx-", 4096)
        self.mount_a.run_shell(["mkdir", d + "/.snap/s0"])
        data = self._write(d + "/f", 0, b"nn-", MB)
        self.mount_a.run_shell(["mkdir", d + "/.snap/s1"])
        self._migrate(d + "/f", EC)
        self._assert_head_moved_alone(d + "/f", d + "/.snap/s1/f", data)

    def test_snapshots_left_behind_are_trimmed(self):
        """Clones in the old pool go when their snapshots do."""
        d = self._dir("trim")
        v1 = self._write(d + "/f", 0, b"v1-", 9 * MB)
        ino = self._ino(d + "/f")
        first = "%x.00000000" % ino
        self.mount_a.run_shell(["mkdir", d + "/.snap/s0"])
        self.mount_a.run_shell(["mv", d + "/f", d + "/g"])
        v2 = self._write(d + "/g", 0, b"v2-", MB)
        self.mount_a.run_shell(["mkdir", d + "/.snap/s1"])
        self._migrate(d + "/g", EC)
        self._assert_reads(d + "/.snap/s0/f", v1)
        self._assert_reads(d + "/.snap/s1/g", v2 + v1[MB:])
        self.assertNotEqual(self._clones(FAST, first), [])

        self.mount_a.run_shell(["rmdir", d + "/.snap/s0", d + "/.snap/s1"])
        self.wait_until_equal(lambda: self._clones(FAST, first), [], 120)

    # --- fs rm_data_pool ------------------------------------------------

    def _delete_pool(self, pool):
        # It stays tagged for this file system, so the next test could not
        # add it to the one it makes.
        self.run_ceph_cmd("osd", "pool", "rm", pool, pool,
                          "--yes-i-really-really-mean-it")

    def _wait_num_objects(self, pool):
        def num_objects():
            df = json.loads(self.get_ceph_cmd_stdout("df", "--format=json"))
            return [p["stats"]["objects"] for p in df["pools"] if p["name"] == pool][0]
        self.wait_until_true(lambda: num_objects() > 0, 60)

    def test_rm_data_pool_with_redirects(self):
        """
        Migrated files leave redirects in the old pool, which readers holding
        the old layout depend on, so it cannot be dropped by accident.
        """
        d = self._dir("guard")
        data = self._write(d + "/f", 0, b"gd-", 9 * MB)
        self.mount_a.run_shell(["mkdir", d + "/.snap/s"])
        self._migrate(d + "/f", EC)
        self._wait_num_objects(FAST)

        with self.assertRaises(CommandFailedError) as cm:
            self.run_ceph_cmd("fs", "rm_data_pool", self.fs.name, FAST)
        self.assertEqual(cm.exception.exitstatus, errno.ENOTEMPTY)
        self.assertIn(FAST, self.fs.get_data_pool_names(refresh=True))

        self.run_ceph_cmd("fs", "rm_data_pool", self.fs.name, FAST,
                          "--yes-i-really-mean-it")
        self.assertNotIn(FAST, self.fs.get_data_pool_names(refresh=True))
        # Asking again is fine, as it always was.
        self.run_ceph_cmd("fs", "rm_data_pool", self.fs.name, FAST)
        self._assert_reads(d + "/f", data)
        self._delete_pool(FAST)

    def test_rm_data_pool_empty(self):
        """An empty pool needs no flag."""
        self.fs.add_data_pool("migrate_unused")
        self.run_ceph_cmd("fs", "rm_data_pool", self.fs.name, "migrate_unused")
        self.assertNotIn("migrate_unused", self.fs.get_data_pool_names(refresh=True))
        self._delete_pool("migrate_unused")

    # --- killing the MDS ------------------------------------------------

    def _test_kill_at(self, point):
        """
        The MDS dies at a point of the migration, and the client that asked
        goes with it, so nobody is left to ask again.  What is already journaled
        must be finished by the next MDS.
        """
        d = self._dir("kill%d" % point)
        v1 = self._write(d + "/f", 0, b"v1-", 13 * MB)
        self.mount_a.run_shell(["mkdir", d + "/.snap/s1"])
        v2 = self._write(d + "/f", 0, b"v2-", MB) + v1[MB:]
        before = self._settled_stored(FAST)

        status = self.fs.status()
        rank0 = self.fs.get_rank(rank=0, status=status)
        self.fs.rank_asok(["config", "set", "mds_kill_file_migrate_at",
                           str(point)], rank=0, status=status)
        self.mount_b.run_shell(["setfattr", "-n", MIGRATE, "-v", EC, d + "/f"],
                               wait=False)
        self.fs.wait_for_death()
        self.delete_mds_coredump(rank0["name"])
        self.mount_b.kill()
        self.mount_b.kill_cleanup()

        # The kill point was set at run time, so it does not come back.
        self.fs.mds_restart(rank0["name"])
        self.fs.wait_for_daemons()
        self.mount_b.mount_wait()

        # Nothing is journaled while copying, so the user must run it again.
        # Without a network namespace to cut it off, as under vstart_runner,
        # the client may have done that itself by resending its request.
        if point == 1:
            self._assert_reads(d + "/f", v2)
            if self._pool_of(d + "/f") == FAST:
                self._migrate(d + "/f", EC)
        self.assertEqual(self._pool_of(d + "/f"), EC)
        self._assert_reads(d + "/f", v2)
        self._assert_reads(d + "/.snap/s1/f", v1)

        self._wait_freed(FAST, before, 14 * MB)

        # What happened does not stand in the way of the next migration.
        self._migrate(d + "/f", FAST)
        self.assertEqual(self._pool_of(d + "/f"), FAST)
        self._assert_reads(d + "/.snap/s1/f", v1)

    def test_kill_while_copying(self):
        self._test_kill_at(1)

    def test_kill_once_journaled(self):
        self._test_kill_at(2)

    def test_kill_while_clearing(self):
        self._test_kill_at(3)

    def test_kill_before_recording_cleared(self):
        self._test_kill_at(4)
