import logging
import os
import random
import time
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while, MaxWhileTries

log = logging.getLogger(__name__)

class TestExports(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2

    def test_session_race(self):
        """
        Test session creation race.

        See: https://tracker.ceph.com/issues/24072#change-113056
        """

        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        rank1 = self.fs.get_rank(rank=1, status=status)

        # Create a directory that is pre-exported to rank 1
        self.mount_a.run_shell(["mkdir", "-p", "a/aa"])
        self.mount_a.setfattr("a", "ceph.dir.pin", "1")
        self._wait_subtrees([('/a', 1)], status=status, rank=1)

        # Now set the mds config to allow the race
        self.fs.rank_asok(["config", "set", "mds_inject_migrator_session_race", "true"], rank=1)

        # Now create another directory and try to export it
        self.mount_b.run_shell(["mkdir", "-p", "b/bb"])
        self.mount_b.setfattr("b", "ceph.dir.pin", "1")

        time.sleep(5)

        # Now turn off the race so that it doesn't wait again
        self.fs.rank_asok(["config", "set", "mds_inject_migrator_session_race", "false"], rank=1)

        # Now try to create a session with rank 1 by accessing a dir known to
        # be there, if buggy, this should cause the rank 1 to crash:
        self.mount_b.run_shell(["ls", "a"])

        # Check if rank1 changed (standby tookover?)
        new_rank1 = self.fs.get_rank(rank=1)
        self.assertEqual(rank1['gid'], new_rank1['gid'])

class TestExportPin(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 1

    def setUp(self):
        CephFSTestCase.setUp(self)

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

        self.mount_a.run_shell_payload("mkdir -p 1/2/3/4")

    def test_noop(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "-1")
        time.sleep(30) # for something to not happen
        self._wait_subtrees([], status=self.status)

    def test_negative(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "-2341")
        time.sleep(30) # for something to not happen
        self._wait_subtrees([], status=self.status)

    def test_empty_pin(self):
        self.mount_a.setfattr("1/2/3/4", "ceph.dir.pin", "1")
        time.sleep(30) # for something to not happen
        self._wait_subtrees([], status=self.status)

    def test_trivial(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self._wait_subtrees([('/1', 1)], status=self.status, rank=1)

    def test_export_targets(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self._wait_subtrees([('/1', 1)], status=self.status, rank=1)
        self.status = self.fs.status()
        r0 = self.status.get_rank(self.fs.id, 0)
        self.assertTrue(sorted(r0['export_targets']) == [1])

    def test_redundant(self):
        # redundant pin /1/2 to rank 1
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self._wait_subtrees([('/1', 1)], status=self.status, rank=1)
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees([('/1', 1), ('/1/2', 1)], status=self.status, rank=1)

    def test_reassignment(self):
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees([('/1/2', 1)], status=self.status, rank=1)
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "0")
        self._wait_subtrees([('/1/2', 0)], status=self.status, rank=0)

    def test_phantom_rank(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "0")
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "10")
        time.sleep(30) # wait for nothing weird to happen
        self._wait_subtrees([('/1', 0)], status=self.status)

    def test_nested(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "0")
        self.mount_a.setfattr("1/2/3", "ceph.dir.pin", "2")
        self._wait_subtrees([('/1', 1), ('/1/2', 0), ('/1/2/3', 2)], status=self.status, rank=2)

    def test_nested_unset(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "2")
        self._wait_subtrees([('/1', 1), ('/1/2', 2)], status=self.status, rank=1)
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "-1")
        self._wait_subtrees([('/1', 1)], status=self.status, rank=1)

    def test_rename(self):
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self.mount_a.run_shell_payload("mkdir -p 9/8/7")
        self.mount_a.setfattr("9/8", "ceph.dir.pin", "0")
        self._wait_subtrees([('/1', 1), ("/9/8", 0)], status=self.status, rank=0)
        self.mount_a.run_shell_payload("mv 9/8 1/2")
        self._wait_subtrees([('/1', 1), ("/1/2/8", 0)], status=self.status, rank=0)

    def test_getfattr(self):
        # pin /1 to rank 0
        self.mount_a.setfattr("1", "ceph.dir.pin", "1")
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "0")
        self._wait_subtrees([('/1', 1), ('/1/2', 0)], status=self.status, rank=1)

        if not isinstance(self.mount_a, FuseMount):
            p = self.mount_a.client_remote.sh('uname -r', wait=True)
            dir_pin = self.mount_a.getfattr("1", "ceph.dir.pin")
            log.debug("mount.getfattr('1','ceph.dir.pin'): %s " % dir_pin)
            if str(p) < "5" and not(dir_pin):
                self.skipTest("Kernel does not support getting the extended attribute ceph.dir.pin")
        self.assertEqual(self.mount_a.getfattr("1", "ceph.dir.pin"), '1')
        self.assertEqual(self.mount_a.getfattr("1/2", "ceph.dir.pin"), '0')

    def test_export_pin_many(self):
        """
        That large numbers of export pins don't slow down the MDS in unexpected ways.
        """

        def getlrg():
            return self.fs.rank_asok(['perf', 'dump', 'mds_log'])['mds_log']['evlrg']

        # vstart.sh sets mds_debug_subtrees to True. That causes a ESubtreeMap
        # to be written out every event. Yuck!
        self.config_set('mds', 'mds_debug_subtrees', False)
        # make sure ESubtreeMap is written frequently enough:
        self.config_set('mds', 'mds_log_minor_segments_per_major_segment', '4')
        self.config_rm('mds', 'mds bal split size') # don't split /top
        self.mount_a.run_shell_payload("rm -rf 1")

        # flush everything out so ESubtreeMap is the only event in the log
        self.fs.rank_asok(["flush", "journal"], rank=0)
        lrg = getlrg()

        n = 5000
        self.mount_a.run_shell_payload(f"""
mkdir top
setfattr -n ceph.dir.pin -v 1 top
for i in `seq 0 {n-1}`; do
    path=$(printf top/%08d $i)
    mkdir "$path"
    touch "$path/file"
    setfattr -n ceph.dir.pin -v 0 "$path"
done
""")

        subtrees = []
        subtrees.append(('/top', 1))
        for i in range(0, n):
            subtrees.append((f"/top/{i:08}", 0))
        self._wait_subtrees(subtrees, status=self.status, timeout=300, rank=1)

        self.assertGreater(getlrg(), lrg)

        # flush everything out so ESubtreeMap is the only event in the log
        self.fs.rank_asok(["flush", "journal"], rank=0)

        # now do some trivial work on rank 0, verify journaling is not slowed down by thousands of subtrees
        start = time.time()
        lrg = getlrg()
        self.mount_a.run_shell_payload('cd top/00000000 && for i in `seq 1 10000`; do mkdir $i; done;')
        self.assertLessEqual(getlrg()-1, lrg) # at most one ESubtree separating events
        self.assertLess(time.time()-start, 120)

    def test_export_pin_cache_drop(self):
        """
        That the export pin does not prevent empty (nothing in cache) subtree merging.
        """

        self.mount_a.setfattr("1", "ceph.dir.pin", "0")
        self.mount_a.setfattr("1/2", "ceph.dir.pin", "1")
        self._wait_subtrees([('/1', 0), ('/1/2', 1)], status=self.status)
        self.mount_a.umount_wait() # release all caps
        def _drop():
            self.fs.ranks_tell(["cache", "drop"], status=self.status)
        # drop cache multiple times to clear replica pins
        self._wait_subtrees([], status=self.status, action=_drop)

    def test_open_file(self):
        """
        Test opening a file via a hard link that is not in the same mds as the inode.

        See https://tracker.ceph.com/issues/58411
        """

        self.mount_a.run_shell_payload("mkdir -p target link")
        self.mount_a.touch("target/test.txt")
        self.mount_a.run_shell_payload("ln target/test.txt link/test.txt")
        self.mount_a.setfattr("target", "ceph.dir.pin", "0")
        self.mount_a.setfattr("link", "ceph.dir.pin", "1")
        self._wait_subtrees([("/target", 0), ("/link", 1)], status=self.status)

        # Release client cache, otherwise the bug may not be triggered even if buggy.
        self.mount_a.remount()

        # Open the file with access mode(O_CREAT|O_WRONLY|O_TRUNC),
        # this should cause the rank 1 to crash if buggy.
        # It's OK to use 'truncate -s 0 link/test.txt' here,
        # its access mode is (O_CREAT|O_WRONLY), it can also trigger this bug.
        log.info("test open mode (O_CREAT|O_WRONLY|O_TRUNC)")
        proc = self.mount_a.open_for_writing("link/test.txt")
        time.sleep(1)
        success = proc.finished and self.fs.rank_is_running(rank=1)

        # Test other write modes too.
        if success:
            self.mount_a.remount()
            log.info("test open mode (O_WRONLY|O_TRUNC)")
            proc = self.mount_a.open_for_writing("link/test.txt", creat=False)
            time.sleep(1)
            success = proc.finished and self.fs.rank_is_running(rank=1)
        if success:
            self.mount_a.remount()
            log.info("test open mode (O_CREAT|O_WRONLY)")
            proc = self.mount_a.open_for_writing("link/test.txt", trunc=False)
            time.sleep(1)
            success = proc.finished and self.fs.rank_is_running(rank=1)

        # Test open modes too.
        if success:
            self.mount_a.remount()
            log.info("test open mode (O_RDONLY)")
            proc = self.mount_a.open_for_reading("link/test.txt")
            time.sleep(1)
            success = proc.finished and self.fs.rank_is_running(rank=1)

        if success:
            # All tests done, rank 1 didn't crash.
            return

        if not proc.finished:
            log.warning("open operation is blocked, kill it")
            proc.kill()

        if not self.fs.rank_is_running(rank=1):
            log.warning("rank 1 crashed")

        self.mount_a.umount_wait(force=True)

        self.assertTrue(success, "open operation failed")

class TestEphemeralPins(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 1

    def setUp(self):
        CephFSTestCase.setUp(self)

        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_distributed', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)

        self.mount_a.run_shell_payload("""
set -e

# Use up a random number of inode numbers so the ephemeral pinning is not the same every test.
mkdir .inode_number_thrash
count=$((RANDOM % 1024))
for ((i = 0; i < count; i++)); do touch .inode_number_thrash/$i; done
rm -rf .inode_number_thrash
""")

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

    def _setup_tree(self, path="tree", export=-1, distributed=False, random=0.0, count=100, wait=True):
        return self.mount_a.run_shell_payload(f"""
set -ex
mkdir -p {path}
{f"setfattr -n ceph.dir.pin -v {export} {path}" if export >= 0 else ""}
{f"setfattr -n ceph.dir.pin.distributed -v 1 {path}" if distributed else ""}
{f"setfattr -n ceph.dir.pin.random -v {random} {path}" if random > 0.0 else ""}
for ((i = 0; i < {count}; i++)); do
    mkdir -p "{path}/$i"
    echo file > "{path}/$i/file"
done
""", wait=wait)

    def test_ephemeral_pin_dist_override(self):
        """
        That an ephemeral distributed pin overrides a normal export pin.
        """

        self._setup_tree(distributed=True)
        subtrees = self._wait_distributed_subtrees(3 * 2, status=self.status, rank="all")
        for s in subtrees:
            path = s['dir']['path']
            if path == '/tree':
                self.assertTrue(s['distributed_ephemeral_pin'])

    def test_ephemeral_pin_dist_override_pin(self):
        """
        That an export pin overrides an ephemerally pinned directory.
        """

        self._setup_tree(distributed=True)
        subtrees = self._wait_distributed_subtrees(3 * 2, status=self.status, rank="all")
        self.mount_a.setfattr("tree", "ceph.dir.pin", "0")
        time.sleep(15)
        subtrees = self._get_subtrees(status=self.status, rank=0)
        for s in subtrees:
            path = s['dir']['path']
            if path == '/tree':
                self.assertEqual(s['auth_first'], 0)
                self.assertFalse(s['distributed_ephemeral_pin'])
        # it has been merged into /tree

    def test_ephemeral_pin_dist_off(self):
        """
        That turning off ephemeral distributed pin merges subtrees.
        """

        self._setup_tree(distributed=True)
        self._wait_distributed_subtrees(3 * 2, status=self.status, rank="all")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "0")
        time.sleep(15)
        subtrees = self._get_subtrees(status=self.status, rank=0)
        for s in subtrees:
            path = s['dir']['path']
            if path == '/tree':
                self.assertFalse(s['distributed_ephemeral_pin'])


    def test_ephemeral_pin_dist_conf_off(self):
        """
        That turning off ephemeral distributed pin config prevents distribution.
        """

        self._setup_tree()
        self.config_set('mds', 'mds_export_ephemeral_distributed', False)
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        time.sleep(15)
        subtrees = self._get_subtrees(status=self.status, rank=0)
        for s in subtrees:
            path = s['dir']['path']
            if path == '/tree':
                self.assertFalse(s['distributed_ephemeral_pin'])

    def _test_ephemeral_pin_dist_conf_off_merge(self):
        """
        That turning off ephemeral distributed pin config merges subtrees.
        FIXME: who triggers the merge?
        """

        self._setup_tree(distributed=True)
        self._wait_distributed_subtrees(3 * 2, status=self.status, rank="all")
        self.config_set('mds', 'mds_export_ephemeral_distributed', False)
        self._wait_subtrees([('/tree', 0)], timeout=60, status=self.status)

    def test_ephemeral_pin_dist_override_before(self):
        """
        That a conventional export pin overrides the distributed policy _before_ distributed policy is set.
        """

        count = 10
        self._setup_tree(count=count)
        test = []
        for i in range(count):
            path = f"tree/{i}"
            self.mount_a.setfattr(path, "ceph.dir.pin", "1")
            test.append(("/"+path, 1))
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        time.sleep(15) # for something to not happen...
        self._wait_subtrees(test, timeout=60, status=self.status, rank="all", path="/tree/")

    def test_ephemeral_pin_dist_override_after(self):
        """
        That a conventional export pin overrides the distributed policy _after_ distributed policy is set.
        """

        self._setup_tree(distributed=True)
        self._wait_distributed_subtrees(3 * 2, status=self.status, rank="all")
        test = []
        for i in range(10):
            path = f"tree/{i}"
            self.mount_a.setfattr(path, "ceph.dir.pin", "1")
            test.append(("/"+path, 1))
        self._wait_subtrees(test, timeout=60, status=self.status, rank="all", path="/tree/")

    def test_ephemeral_pin_dist_failover(self):
        """
        That MDS failover does not cause unnecessary migrations.
        """

        # pin /tree so it does not export during failover
        self._setup_tree(distributed=True)
        self._wait_distributed_subtrees(3 * 2, status=self.status, rank="all")
        #test = [(s['dir']['path'], s['auth_first']) for s in subtrees]
        before = self.fs.ranks_perf(lambda p: p['mds']['exported'])
        log.info(f"export stats: {before}")
        self.fs.rank_fail(rank=1)
        self.status = self.fs.wait_for_daemons()
        time.sleep(10) # waiting for something to not happen
        after = self.fs.ranks_perf(lambda p: p['mds']['exported'])
        log.info(f"export stats: {after}")
        self.assertEqual(before, after)

    def test_ephemeral_pin_distribution(self):
        """
        That ephemerally pinned subtrees are somewhat evenly distributed.
        """

        max_mds = 3
        frags = 128

        self.fs.set_max_mds(max_mds)
        self.status = self.fs.wait_for_daemons()

        self.config_set('mds', 'mds_export_ephemeral_distributed_factor', (frags-1) / max_mds)
        self._setup_tree(count=1000, distributed=True)

        subtrees = self._wait_distributed_subtrees(frags, status=self.status, rank="all")
        nsubtrees = len(subtrees)

        # Check if distribution is uniform
        rank0 = list(filter(lambda x: x['auth_first'] == 0, subtrees))
        rank1 = list(filter(lambda x: x['auth_first'] == 1, subtrees))
        rank2 = list(filter(lambda x: x['auth_first'] == 2, subtrees))
        self.assertGreaterEqual(len(rank0)/nsubtrees, 0.15)
        self.assertGreaterEqual(len(rank1)/nsubtrees, 0.15)
        self.assertGreaterEqual(len(rank2)/nsubtrees, 0.15)

    def _setup_big_dir(self, path="tree/a/big", count=1000, tree=None):
        """
        Create @path with @count files, small split sizes so that it gets
        fragmented, and optionally set ceph.dir.pin.distributed.tree on
        @tree first.
        """
        self.config_set('mds', 'mds_bal_split_size', 100)
        self.config_set('mds', 'mds_bal_merge_size', 1)
        self.config_set('mds', 'mds_bal_fragment_interval', 1)
        self.mount_a.run_shell_payload(f"""
set -ex
mkdir -p {path}
{f"setfattr -n ceph.dir.pin.distributed.tree -v 1 {tree}" if tree else ""}
for ((i = 0; i < {count}; i++)); do
    touch "{path}/f$i"
done
""")

    def _dirfrag_auth(self, path):
        """
        The auth rank of each dirfrag of @path that some rank has in cache,
        as {dirfrag: rank}.
        """
        auth = {}
        for info in self.fs.get_ranks(status=self.status):
            try:
                dirs = self.fs.rank_asok(["dump", "dir", path], rank=info['rank'], status=self.status)
            except CommandFailedError:
                continue
            for d in dirs or []:
                if d.get('is_auth'):
                    auth[d['dirfrag']] = info['rank']
        return auth

    def _wait_dist_tree(self, path, nranks, timeout=150):
        """
        Wait until the dirfrags of @path are distributed over @nranks ranks:
        those away from the rank of the parent directory as subtrees on
        their target ranks, the rest in the subtree of the parent.  Returns
        the auth rank of each dirfrag.
        """
        parent = os.path.dirname(path)
        try:
            with safe_while(sleep=5, tries=timeout//5) as proceed:
                while proceed():
                    prank = set(self._dirfrag_auth(parent).values())
                    auth = self._dirfrag_auth(path)
                    subtrees = self._get_subtrees(status=self.status, rank="all", path=path)
                    subtrees = [s for s in subtrees if s['dir']['path'] == path and s['is_auth']]
                    dist = [s for s in subtrees if s['distributed_ephemeral_pin'] and
                                                   s['auth_first'] == s['export_pin_target']]
                    away = [f for f, r in auth.items() if r not in prank]
                    ranks = set(auth.values())
                    log.info(f"{path}: parent on {prank}, {len(auth)} dirfrags on ranks {ranks}, "
                             f"{len(subtrees)} subtrees, {len(dist)} distributed")
                    if (len(prank) == 1 and len(ranks) >= nranks and
                            len(dist) == len(subtrees) and
                            sorted(s['dir']['dirfrag'] for s in dist) == sorted(away)):
                        return auth
        except MaxWhileTries as e:
            raise RuntimeError(f"{path} was not distributed over {nranks} ranks") from e

    @staticmethod
    def _dist_map(auth):
        return sorted(auth.items())

    def _wait_dist_stable(self, path, nranks, timeout=150):
        """
        Like _wait_dist_tree, but also wait until the dirfrags stop
        splitting and moving: the same map twice in a row, 10s apart.
        """
        prev = None
        try:
            with safe_while(sleep=10, tries=timeout//10) as proceed:
                while proceed():
                    dist = self._wait_dist_tree(path, nranks, timeout=timeout)
                    cur = self._dist_map(dist)
                    if cur == prev:
                        return dist
                    prev = cur
        except MaxWhileTries as e:
            raise RuntimeError(f"{path} distribution did not settle") from e

    def _wait_no_dist_tree(self, path, timeout=150):
        """
        Wait until no dirfrag of @path is an ephemerally distributed subtree.
        """
        try:
            with safe_while(sleep=5, tries=timeout//5) as proceed:
                while proceed():
                    subtrees = self._get_subtrees(status=self.status, rank="all", path=path)
                    dist = [s for s in subtrees if s['dir']['path'] == path and
                                                   s['distributed_ephemeral_pin']]
                    log.info(f"{path}: {len(dist)} distributed subtrees left")
                    if not dist:
                        return
        except MaxWhileTries as e:
            raise RuntimeError(f"{path} is still distributed") from e

    def test_ephemeral_pin_dist_tree(self):
        """
        That ceph.dir.pin.distributed.tree distributes the dirfrags of a
        fragmented directory two levels below it over all the ranks.
        """

        self._setup_big_dir(tree="tree")
        self.mount_a.run_shell_payload("mkdir -p tree/small && touch tree/small/f")
        self._wait_dist_tree("/tree/a/big", 3)
        # a directory that is not fragmented is not distributed
        for s in self._get_subtrees(status=self.status, rank="all", path="/tree/small"):
            self.assertFalse(s['distributed_ephemeral_pin'])

    def test_ephemeral_pin_dist_tree_getfattr(self):
        """
        That ceph.dir.pin.distributed.tree can be read back, rejects bad
        values and is only accepted on directories.
        """

        self.mount_a.run_shell_payload("mkdir -p tree && touch tree/file")
        self.assertEqual(self.mount_a.getfattr("tree", "ceph.dir.pin.distributed.tree"), "0")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed.tree", "1")
        self.assertEqual(self.mount_a.getfattr("tree", "ceph.dir.pin.distributed.tree"), "1")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed.tree", "0")
        self.assertEqual(self.mount_a.getfattr("tree", "ceph.dir.pin.distributed.tree"), "0")
        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr("tree", "ceph.dir.pin.distributed.tree", "foo")
        with self.assertRaises(CommandFailedError):
            self.mount_a.setfattr("tree/file", "ceph.dir.pin.distributed.tree", "1")

    def test_ephemeral_pin_dist_tree_set_after(self):
        """
        That setting ceph.dir.pin.distributed.tree above a directory that is
        already fragmented distributes it.
        """

        self._setup_big_dir()
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed.tree", "1")
        self._wait_dist_tree("/tree/a/big", 3)

    def test_ephemeral_pin_dist_tree_off(self):
        """
        That clearing ceph.dir.pin.distributed.tree stops distributing the
        fragmented directories below it.
        """

        self._setup_big_dir(tree="tree")
        self._wait_dist_tree("/tree/a/big", 3)
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed.tree", "0")
        self._wait_no_dist_tree("/tree/a/big")

    def test_ephemeral_pin_dist_tree_override_pin(self):
        """
        That an export pin between the tree policy and a fragmented
        directory takes precedence.
        """

        self._setup_big_dir(tree="tree")
        self._wait_dist_tree("/tree/a/big", 3)
        self.mount_a.setfattr("tree/a", "ceph.dir.pin", "1")
        self._wait_subtrees([("/tree/a", 1)], timeout=120, status=self.status, rank="all", path="/tree/a")

    def test_ephemeral_pin_dist_tree_failover(self):
        """
        That after an MDS failover the distributed dirfrags end up on the
        same ranks as before.  (The restarted rank hands the imports it has
        not loaded yet back to the inode's auth, like any empty import, and
        the policy then moves them back.)
        """

        self._setup_big_dir(tree="tree")
        before = self._dist_map(self._wait_dist_stable("/tree/a/big", 3))
        self.fs.rank_fail(rank=1)
        self.status = self.fs.wait_for_daemons()
        self.mount_a.run_shell_payload("ls -l tree/a/big > /dev/null")
        try:
            with safe_while(sleep=5, tries=30) as proceed:
                while proceed():
                    after = self._dist_map(self._wait_dist_tree("/tree/a/big", 3))
                    log.info(f"before={before}\nafter={after}")
                    if after == before:
                        break
        except MaxWhileTries as e:
            raise RuntimeError("distribution not restored after failover") from e

    def test_ephemeral_pin_dist_tree_shrink_mds(self):
        """
        That shrinking max_mds moves the distributed dirfrags off the
        stopped rank and keeps them distributed over the rest.
        """

        self._setup_big_dir(tree="tree")
        self._wait_dist_tree("/tree/a/big", 3)
        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()
        auth = self._wait_dist_tree("/tree/a/big", 2)
        self.assertTrue(all(r < 2 for r in auth.values()))

    def test_ephemeral_pin_dist_tree_min_entries(self):
        """
        That a fragmented directory smaller than
        mds_export_ephemeral_distributed_tree_min_entries stays in the
        subtree of its parent, and is distributed once it grows past it.
        """

        self.config_set('mds', 'mds_export_ephemeral_distributed_tree_min_entries', 1500)
        self._setup_big_dir(tree="tree", count=1000)
        # fragmented, all on the rank of the parent, and no subtree of its own
        for i in range(4):
            time.sleep(5)
            auth = self._dirfrag_auth("/tree/a/big")
            prank = set(self._dirfrag_auth("/tree/a").values())
            subtrees = [s for s in self._get_subtrees(status=self.status, rank="all", path="/tree/a/big")
                        if s['dir']['path'] == "/tree/a/big"]
            log.info(f"dirfrags on {auth}, parent on {prank}, {len(subtrees)} subtrees")
            self.assertGreater(len(auth), 1)
            self.assertEqual(set(auth.values()), prank)
            self.assertEqual(subtrees, [])
        self.mount_a.run_shell_payload("""
set -ex
for ((i = 1000; i < 2000; i++)); do
    touch "tree/a/big/f$i"
done
""")
        self._wait_dist_tree("/tree/a/big", 3)

    def test_ephemeral_pin_dist_tree_random_between(self):
        """
        That a directory randomly pinned by a ceph.dir.pin.random policy
        between ceph.dir.pin.distributed.tree and a fragmented directory
        takes precedence: a fragmented directory below it that was not
        randomly pinned itself stays with it, on one rank, and is not
        distributed.
        """

        n = 24
        self.config_set('mds', 'mds_bal_split_size', 100)
        self.config_set('mds', 'mds_bal_merge_size', 1)
        self.config_set('mds', 'mds_bal_fragment_interval', 1)
        self.mount_a.run_shell_payload(f"""
set -ex
mkdir -p tree/r
setfattr -n ceph.dir.pin.distributed.tree -v 1 tree
setfattr -n ceph.dir.pin.random -v 0.5 tree/r
for ((c = 0; c < {n}; c++)); do
    mkdir -p tree/r/c$c/big
    for ((i = 0; i < 400; i++)); do
        touch "tree/r/c$c/big/f$i"
    done
done
""")
        def random_pinned():
            subtrees = self._get_subtrees(status=self.status, rank="all", path="/tree/r")
            return set(s['dir']['path'] for s in subtrees if s['random_ephemeral_pin'])
        # wait until the random pins have all become subtrees
        prev = None
        with safe_while(sleep=10, tries=12) as proceed:
            while proceed():
                pinned = random_pinned()
                if pinned == prev:
                    break
                prev = pinned
        # the directories whose parent was randomly pinned but which were not
        cases = [c for c in range(n)
                 if f"/tree/r/c{c}" in pinned and f"/tree/r/c{c}/big" not in pinned]
        log.info(f"randomly pinned: {sorted(pinned)}; checking {cases}")
        self.assertTrue(cases)

        def settled():
            pinned = random_pinned()
            for c in cases:
                big = f"/tree/r/c{c}/big"
                self.assertNotIn(big, pinned)
                auth = self._dirfrag_auth(big)
                crank = set(self._dirfrag_auth(f"/tree/r/c{c}").values())
                dist = [s for s in self._get_subtrees(status=self.status, rank="all", path=big)
                        if s['dir']['path'] == big and s['distributed_ephemeral_pin']]
                log.info(f"{big}: {len(auth)} dirfrags on {set(auth.values())}, parent on {crank}, {len(dist)} distributed")
                if len(auth) < 2 or len(crank) != 1 or set(auth.values()) != crank or dist:
                    return False
            return True
        # and so for 20s in a row
        ok = 0
        with safe_while(sleep=5, tries=40) as proceed:
            while proceed():
                ok = ok + 1 if settled() else 0
                if ok >= 4:
                    break

    def test_ephemeral_random(self):
        """
        That 100% randomness causes all children to be pinned.
        """
        self._setup_tree(random=1.0)
        self._wait_random_subtrees(100, status=self.status, rank="all")

    def test_ephemeral_random_max(self):
        """
        That the config mds_export_ephemeral_random_max is not exceeded.
        """

        r = 0.5
        count = 1000
        self._setup_tree(count=count, random=r)
        subtrees = self._wait_random_subtrees(int(r*count*.75), status=self.status, rank="all")
        self.config_set('mds', 'mds_export_ephemeral_random_max', 0.01)
        self._setup_tree(path="tree/new", count=count)
        time.sleep(30) # for something not to happen...
        subtrees = self._get_subtrees(status=self.status, rank="all", path="tree/new/")
        self.assertLessEqual(len(subtrees), int(.01*count*1.25))

    def test_ephemeral_random_max_config(self):
        """
        That the config mds_export_ephemeral_random_max config rejects new OOB policies.
        """

        self.config_set('mds', 'mds_export_ephemeral_random_max', 0.01)
        try:
            p = self._setup_tree(count=1, random=0.02, wait=False)
            p.wait()
        except CommandFailedError as e:
            log.info(f"{e}")
            self.assertIn("Invalid", p.stderr.getvalue())
        else:
            raise RuntimeError("mds_export_ephemeral_random_max ignored!")

    def test_ephemeral_random_dist(self):
        """
        That ephemeral distributed pin overrides ephemeral random pin
        """

        self._setup_tree(random=1.0, distributed=True)
        self._wait_distributed_subtrees(3 * 2, status=self.status)

        time.sleep(15)
        subtrees = self._get_subtrees(status=self.status, rank=0)
        for s in subtrees:
            path = s['dir']['path']
            if path.startswith('/tree'):
                self.assertFalse(s['random_ephemeral_pin'])

    def test_ephemeral_random_pin_override_before(self):
        """
        That a conventional export pin overrides the random policy before creating new directories.
        """

        self._setup_tree(count=0, random=1.0)
        self._setup_tree(path="tree/pin", count=10, export=1)
        self._wait_subtrees([("/tree/pin", 1)], status=self.status, rank=1, path="/tree/pin")

    def test_ephemeral_random_pin_override_after(self):
        """
        That a conventional export pin overrides the random policy after creating new directories.
        """

        count = 10
        self._setup_tree(count=0, random=1.0)
        self._setup_tree(path="tree/pin", count=count)
        self._wait_random_subtrees(count+1, status=self.status, rank="all")
        self.mount_a.setfattr("tree/pin", "ceph.dir.pin", "1")
        self._wait_subtrees([("/tree/pin", 1)], status=self.status, rank=1, path="/tree/pin")

    def test_ephemeral_randomness(self):
        """
        That the randomness is reasonable.
        """

        r = random.uniform(0.25, 0.75) # ratios don't work for small r!
        count = 1000
        self._setup_tree(count=count, random=r)
        subtrees = self._wait_random_subtrees(int(r*count*.50), status=self.status, rank="all")
        time.sleep(30) # for max to not be exceeded
        subtrees = self._wait_random_subtrees(int(r*count*.50), status=self.status, rank="all")
        self.assertLessEqual(len(subtrees), int(r*count*1.50))

    def test_ephemeral_random_cache_drop(self):
        """
        That the random ephemeral pin does not prevent empty (nothing in cache) subtree merging.
        """

        count = 100
        self._setup_tree(count=count, random=1.0)
        self._wait_random_subtrees(count, status=self.status, rank="all")
        self.mount_a.umount_wait() # release all caps
        def _drop():
            self.fs.ranks_tell(["cache", "drop"], status=self.status)
        self._wait_subtrees([], status=self.status, action=_drop)

    def test_ephemeral_random_failover(self):
        """
        That the random ephemeral pins stay pinned across MDS failover.
        """

        count = 100
        r = 0.5
        self._setup_tree(count=count, random=r)
        # wait for all random subtrees to be created, not a specific count
        time.sleep(30)
        subtrees = self._wait_random_subtrees(1, status=self.status, rank=1)
        before = [(s['dir']['path'], s['auth_first']) for s in subtrees]
        before.sort();

        self.fs.rank_fail(rank=1)
        self.status = self.fs.wait_for_daemons()

        time.sleep(30) # waiting for something to not happen
        subtrees = self._wait_random_subtrees(1, status=self.status, rank=1)
        after = [(s['dir']['path'], s['auth_first']) for s in subtrees]
        after.sort();
        log.info(f"subtrees before: {before}")
        log.info(f"subtrees after: {after}")

        self.assertEqual(before, after)

    def test_ephemeral_pin_grow_mds(self):
        """
        That consistent hashing works to reduce the number of migrations.
        """

        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()

        self._setup_tree(random=1.0)
        subtrees_old = self._wait_random_subtrees(100, status=self.status, rank="all")

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()
        
        # Sleeping for a while to allow the ephemeral pin migrations to complete
        time.sleep(30)
        
        subtrees_new = self._wait_random_subtrees(100, status=self.status, rank="all")
        count = 0
        for old_subtree in subtrees_old:
            for new_subtree in subtrees_new:
                if (old_subtree['dir']['path'] == new_subtree['dir']['path']) and (old_subtree['auth_first'] != new_subtree['auth_first']):
                    count = count + 1
                    break

        log.info("{0} migrations have occured due to the cluster resizing".format(count))
        # ~50% of subtrees from the two rank will migrate to another rank
        self.assertLessEqual((count/len(subtrees_old)), (0.5)*1.25) # with 25% overbudget

    def test_ephemeral_pin_shrink_mds(self):
        """
        That consistent hashing works to reduce the number of migrations.
        """

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

        self._setup_tree(random=1.0)
        subtrees_old = self._wait_random_subtrees(100, status=self.status, rank="all")

        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()
        time.sleep(30)

        subtrees_new = self._wait_random_subtrees(100, status=self.status, rank="all")
        count = 0
        for old_subtree in subtrees_old:
            for new_subtree in subtrees_new:
                if (old_subtree['dir']['path'] == new_subtree['dir']['path']) and (old_subtree['auth_first'] != new_subtree['auth_first']):
                    count = count + 1
                    break

        log.info("{0} migrations have occured due to the cluster resizing".format(count))
        # rebalancing from 3 -> 2 may cause half of rank 0/1 to move and all of rank 2
        self.assertLessEqual((count/len(subtrees_old)), (1.0/3.0/2.0 + 1.0/3.0/2.0 + 1.0/3.0)*1.25) # aka .66 with 25% overbudget

class TestDistTreeClients(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 2

    def test_dist_tree_shared_dir_unlink(self):
        """
        That clients unlinking the files of one directory spread over two
        ranks by ceph.dir.pin.distributed.tree, while they also look it up,
        do not stall: a scatterlock gather on the directory inode, with the
        replica holding the wrlocks of unlinks that early replied, used to
        wait for the next MDS tick for every few unlinks.
        """

        self.config_set('mds', 'mds_bal_split_size', 100)
        self.config_set('mds', 'mds_bal_merge_size', 1)
        self.config_set('mds', 'mds_bal_fragment_interval', 1)
        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()
        n = 2000
        self.mount_a.run_shell_payload("mkdir -p tree/a/shared && setfattr -n ceph.dir.pin.distributed.tree -v 1 tree")
        procs = [m.run_shell_payload(f"""
set -e
for ((i = 0; i < {n}; i++)); do touch "tree/a/shared/{m.client_id}.$i"; done
""", wait=False) for m in (self.mount_a, self.mount_b)]
        for p in procs:
            p.wait()

        def ranks():
            subtrees = self._get_subtrees(status=self.status, rank="all", path="/tree/a/shared")
            return set(s['auth_first'] for s in subtrees
                       if s['dir']['path'] == "/tree/a/shared" and s['distributed_ephemeral_pin'])
        with safe_while(sleep=5, tries=30) as proceed:
            while proceed():
                r = ranks()
                log.info(f"distributed subtrees on ranks {r}")
                if r:
                    break

        start = time.time()
        procs = [m.run_shell_payload(f"""
set -e
( while :; do stat tree/a/shared > /dev/null; ls tree/a > /dev/null; sleep 0.01; done ) &
looker=$!
for ((i = 0; i < {n}; i++)); do rm "tree/a/shared/{m.client_id}.$i"; done
kill $looker
""", wait=False) for m in (self.mount_a, self.mount_b)]
        for p in procs:
            p.wait()
        elapsed = time.time() - start
        log.info(f"{2 * n} unlinks took {elapsed:.1f}s")
        self.assertLess(elapsed, 300)
        self.mount_a.run_shell_payload("rmdir tree/a/shared")


class TestDumpExportStates(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    EXPORT_STATES = ['locking', 'discovering', 'freezing', 'prepping', 'warning', 'exporting']

    def setUp(self):
        super().setUp()

        self.fs.set_max_mds(self.MDSS_REQUIRED)
        self.status = self.fs.wait_for_daemons()

        self.mount_a.run_shell_payload('mkdir -p test/export')

    def tearDown(self):
        super().tearDown()

    def _wait_for_export_target(self, source, target, sleep=2, timeout=10):
        try:
            with safe_while(sleep=sleep, tries=timeout//sleep) as proceed:
                while proceed():
                    info = self.fs.getinfo().get_rank(self.fs.id, source)
                    log.info(f'waiting for rank {target} to be added to the export target')
                    if target in info['export_targets']:
                        return
        except MaxWhileTries as e:
            raise RuntimeError(f'rank {target} has not been added to export target after {timeout}s') from e

    def _dump_export_state(self, rank):
        states = self.fs.rank_asok(['dump_export_states'], rank=rank, status=self.status)
        self.assertTrue(type(states) is list)
        self.assertEqual(len(states), 1)
        return states[0]

    def _test_base(self, path, source, target, state_index, kill):
        self.fs.rank_asok(['config', 'set', 'mds_kill_import_at', str(kill)], rank=target, status=self.status)

        self.fs.rank_asok(['export', 'dir', path, str(target)], rank=source, status=self.status)
        self._wait_for_export_target(source, target)

        target_rank = self.fs.get_rank(rank=target, status=self.status)
        self.delete_mds_coredump(target_rank['name'])

        state = self._dump_export_state(source)

        self.assertTrue(type(state['tid']) is int)
        self.assertEqual(state['path'], path)
        self.assertEqual(state['state'], self.EXPORT_STATES[state_index])
        self.assertEqual(state['peer'], target)

        return state

    def _test_state_history(self, state):
        history = state['state_history']
        self.assertTrue(type(history) is dict)
        size = 0
        for name in self.EXPORT_STATES:
            self.assertTrue(type(history[name]) is dict)
            size += 1
            if name == state['state']:
                break
        self.assertEqual(len(history), size)

    def _test_freeze_tree(self, state, waiters):
        self.assertTrue(type(state['freeze_tree_time']) is float)
        self.assertEqual(state['unfreeze_tree_waiters'], waiters)

    def test_discovering(self):
        state = self._test_base('/test', 0, 1, 1, 1)

        self._test_state_history(state)
        self._test_freeze_tree(state, 0)

        self.assertEqual(state['last_cum_auth_pins'], 0)
        self.assertEqual(state['num_remote_waiters'], 0)

    def test_prepping(self):
        client_id = self.mount_a.get_global_id()

        state = self._test_base('/test', 0, 1, 3, 3)

        self._test_state_history(state)
        self._test_freeze_tree(state, 0)

        self.assertEqual(state['flushed_clients'], [client_id])
        self.assertTrue(type(state['warning_ack_waiting']) is list)

    def test_exporting(self):
        state = self._test_base('/test', 0, 1, 5, 5)

        self._test_state_history(state)
        self._test_freeze_tree(state, 0)

        self.assertTrue(type(state['notify_ack_waiting']) is list)

class TestKillExports(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    def setUp(self):
        CephFSTestCase.setUp(self)

        self.fs.set_max_mds(self.MDSS_REQUIRED)
        self.status = self.fs.wait_for_daemons()

        self.mount_a.run_shell_payload('mkdir -p test/export')

    def tearDown(self):
        super().tearDown()

    def _kill_export_as(self, rank, kill):
        self.fs.rank_asok(['config', 'set', 'mds_kill_export_at', str(kill)], rank=rank, status=self.status)

    def _export_dir(self, path, source, target):
        self.fs.rank_asok(['export', 'dir', path, str(target)], rank=source, status=self.status)

    def _wait_failover(self):
        self.wait_until_true(lambda: self.fs.status().hadfailover(self.status), timeout=self.fs.beacon_timeout)

    def _clear_coredump(self, rank):
        crash_rank = self.fs.get_rank(rank=rank, status=self.status)
        self.delete_mds_coredump(crash_rank['name'])

    def _run_kill_export(self, kill_at, exporter_rank=0, importer_rank=1, restart=True):
        self._kill_export_as(exporter_rank, kill_at)
        self._export_dir("/test", exporter_rank, importer_rank)
        self._wait_failover()
        self._clear_coredump(exporter_rank)

        if restart:
            self.fs.rank_restart(rank=exporter_rank, status=self.status)
        self.status = self.fs.wait_for_daemons()

    def test_session_cleanup(self):
        """
        Test importer's session cleanup after an export subtree task is interrupted.
        Set 'mds_kill_export_at' to 9 or 10 so that the importer will wait for the exporter
        to restart while the state is 'acking'.

        See https://tracker.ceph.com/issues/61459
        """

        kill_export_at = [9, 10]

        exporter_rank = 0
        importer_rank = 1

        for kill in kill_export_at:
            log.info(f"kill_export_at: {kill}")
            self._run_kill_export(kill, exporter_rank, importer_rank)

            if len(self._session_list(importer_rank, self.status)) > 0:
                client_id = self.mount_a.get_global_id()
                self.fs.rank_asok(['session', 'evict', "%s" % client_id], rank=importer_rank, status=self.status)

                # timeout if buggy
                self.wait_until_evicted(client_id, importer_rank)

            # for multiple tests
            self.mount_a.remount()

    def test_client_eviction(self):
        # modify the timeout so that we don't have to wait too long
        timeout = 30
        self.fs.set_session_timeout(timeout)
        self.fs.set_session_autoclose(timeout + 5)

        kill_export_at = [9, 10]

        exporter_rank = 0
        importer_rank = 1

        for kill in kill_export_at:
            log.info(f"kill_export_at: {kill}")
            self._run_kill_export(kill, exporter_rank, importer_rank)

            client_id = self.mount_a.get_global_id()
            self.wait_until_evicted(client_id, importer_rank, timeout + 10)
            time.sleep(1)

            # failed if buggy
            self.mount_a.ls()
