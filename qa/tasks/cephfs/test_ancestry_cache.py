"""
Tests for the caches of CInode::authority(), CDir::authority() and
CInode::find_snaprealm() (mds_cache_ancestry).

Each cached value is dropped by bumping a global generation wherever the
answer may change for a whole subtree.  A missed place fails silently: an
MDS keeps using a stale authority or snaprealm.  So every test here runs
with mds_cache_ancestry_verify, which checks every cached value against a
walk up the parents and aborts the MDS on a difference: warm the caches
deep below a directory, change what is above them, use them again, and
check that no MDS restarted.
"""

import logging
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

DEPTH = 8
NFILES = 20


class TestAncestryCache(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2

    def setUp(self):
        super().setUp()
        self.config_set('mds', 'mds_cache_ancestry', 'true')
        self.config_set('mds', 'mds_cache_ancestry_verify', 'true')
        # no balancer moves or fragment changes but ours
        self.config_set('mds', 'mds_bal_fragment_dirs', 'false')
        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()
        self.gids = self._active_gids()

    def _active_gids(self):
        status = self.fs.status()
        return sorted(info['gid'] for info in status.get_ranks(self.fs.id))

    def _check_no_restart(self):
        self.assertEqual(self._active_gids(), self.gids,
                         "an MDS restarted: a cached ancestry value was stale")

    def _tree(self, top):
        """Make top/l0/../l<DEPTH-1>/c with NFILES files; return (deep, files)."""
        deep = top + "".join("/l%d" % i for i in range(DEPTH))
        self.mount_a.run_shell_payload(
            f"mkdir -p {deep}/c && "
            f"for i in $(seq 1 {NFILES}); do echo -n v1 > {deep}/c/f$i; done")
        return deep, ["%s/c/f%d" % (deep, i) for i in range(1, NFILES + 1)]

    def _warm(self, top, rounds=3):
        """stat() everything below top from both clients, a few times."""
        for m in (self.mount_a, self.mount_b):
            m.run_shell_payload(
                f"for r in $(seq 1 {rounds}); do "
                f"find {top} -exec stat {{}} + > /dev/null; done")

    def _read(self, path):
        return self.mount_a.run_shell(["cat", path]).stdout.getvalue().strip()

    def test_snaprealm_open_mid_path(self):
        """
        A snapshot half way down a warm path opens a realm there: files
        below must be found under it, and keep their data in the snapshot.
        """
        deep, files = self._tree("t1")
        self._warm("t1")
        mid = "t1/l0/l1/l2/l3"
        self.mount_a.run_shell(["mkdir", f"{mid}/.snap/s1"])
        self._warm("t1")
        self.mount_b.run_shell_payload(f"echo -n v2 > {files[0]}")
        rel = deep[len(mid):]
        self.assertEqual(self._read(f"{mid}/.snap/s1{rel}/c/f1"), "v1")
        self.assertEqual(self._read(files[0]), "v2")
        # the realm closes when its inode is trimmed, which needs the
        # clients gone, and opens again when the inode is loaded back
        self.mount_a.run_shell(["rmdir", f"{mid}/.snap/s1"])
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()
        for rank in (0, 1):
            self.fs.rank_tell(["cache", "drop"], rank=rank)
        self.mount_a.mount_wait()
        self.mount_b.mount_wait()
        self._warm("t1")
        self.assertEqual(self._read(files[0]), "v2")
        self._check_no_restart()

    def test_rename_dir_across_realms(self):
        """
        A directory with dirfrags in cache, renamed under another realm:
        everything below it changes realm.
        """
        deep, files = self._tree("t2")
        self.mount_a.run_shell_payload("mkdir -p t2/x && mkdir t2/x/.snap/sx")
        self._warm("t2")
        self.mount_a.run_shell(["mv", f"{deep}/c", "t2/x/c"])
        self.mount_b.run_shell_payload("echo -n v3 > t2/x/c/f1")
        self.mount_a.run_shell(["mkdir", "t2/x/.snap/sy"])
        self.mount_b.run_shell_payload("echo -n v4 > t2/x/c/f1")
        self.assertEqual(self._read("t2/x/.snap/sy/c/f1"), "v3")
        self._warm("t2")
        # and back, out of that realm
        self.mount_a.run_shell(["mv", "t2/x/c", f"{deep}/c"])
        self._warm("t2")
        self._check_no_restart()

    def test_export_and_back(self):
        """
        Export a warm subtree to rank 1 and back while a client stats it,
        then drop to one rank, which exports everything to rank 0.
        """
        deep, files = self._tree("t3")
        self._warm("t3")
        self.mount_b.run_shell_payload(
            "touch /tmp/t3-run; "
            "while [ -e /tmp/t3-run ]; do find t3 -exec stat {} + > /dev/null; done",
            wait=False)
        try:
            for rank, sub in ((1, [('/t3', 1)]), (0, []), (1, [('/t3', 1)])):
                self.mount_a.setfattr("t3", "ceph.dir.pin", str(rank))
                self._wait_subtrees(sub, status=self.status, rank=1)
                self._warm("t3", rounds=1)
            # a subtree root below the exported one
            self.mount_a.setfattr("t3/l0/l1/l2", "ceph.dir.pin", "0")
            self._wait_subtrees([('/t3', 1), ('/t3/l0/l1/l2', 0)],
                                status=self.status, rank=1)
            self._warm("t3", rounds=1)
            # back to rank 0; dropping the inner pin merges its subtree
            self.mount_a.setfattr("t3", "ceph.dir.pin", "0")
            self.mount_a.setfattr("t3/l0/l1/l2", "ceph.dir.pin", "-1")
            self._wait_subtrees([], status=self.status, rank=1)
            self._warm("t3", rounds=1)
        finally:
            self.mount_b.run_shell(["rm", "-f", "/tmp/t3-run"])
            time.sleep(2)
        self._check_no_restart()
        # shrink to one rank: rank 1 exports what it has
        self.mount_a.setfattr("t3", "ceph.dir.pin", "1")
        self._wait_subtrees([('/t3', 1)], status=self.status, rank=1)
        self._warm("t3")
        self.fs.set_max_mds(1)
        self.status = self.fs.wait_for_daemons()
        self._warm("t3")
        self.assertEqual(self._read(files[0]), "v1")

    def test_fragment_split_merge(self):
        """
        Split and merge a dirfrag whose dentries lead to warm caches: the
        dentries, and everything below them, move to another dirfrag.
        """
        self.mount_a.run_shell_payload(
            "mkdir -p t4/d && for i in $(seq 1 16); do "
            "mkdir -p t4/d/s$i/a/b && echo -n v1 > t4/d/s$i/a/b/f; done")
        self._warm("t4")
        self.fs.rank_asok(["dirfrag", "split", "/t4/d", "0/0", "2"], rank=0)
        self._warm("t4")
        self.fs.rank_asok(["dirfrag", "merge", "/t4/d", "0/0"], rank=0)
        self._warm("t4")
        self.assertEqual(self._read("t4/d/s7/a/b/f"), "v1")
        self._check_no_restart()

    def test_toggle_under_load(self):
        """
        Turning mds_cache_ancestry off and on at runtime drops every cached
        value.
        """
        self._tree("t5")
        self._warm("t5")
        for v in ("false", "true", "false", "true"):
            self.config_set('mds', 'mds_cache_ancestry', v)
            self.mount_a.setfattr("t5", "ceph.dir.pin",
                                  "1" if v == "false" else "0")
            self._warm("t5", rounds=1)
        self._check_no_restart()
