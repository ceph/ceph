import logging
import random
import time
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

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
