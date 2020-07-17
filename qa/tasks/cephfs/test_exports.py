import logging
import random
import time
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.orchestra.run import CommandFailedError

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
set -e
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
        subtrees = self._wait_distributed_subtrees(100, status=self.status, rank="all")
        for s in subtrees:
            path = s['dir']['path']
            if path == '/tree':
                self.assertEqual(s['export_pin'], 0)
                self.assertEqual(s['auth_first'], 0)
            elif path.startswith('/tree/'):
                self.assertEqual(s['export_pin'], -1)
                self.assertTrue(s['distributed_ephemeral_pin'])

    def test_ephemeral_pin_dist_override_pin(self):
        """
        That an export pin overrides an ephemerally pinned directory.
        """

        self._setup_tree(distributed=True, export=0)
        subtrees = self._wait_distributed_subtrees(100, status=self.status, rank="all", path="/tree/")
        which = None
        for s in subtrees:
            if s['auth_first'] == 1:
                path = s['dir']['path']
                self.mount_a.setfattr(path[1:], "ceph.dir.pin", "0")
                which = path
                break
        self.assertIsNotNone(which)
        time.sleep(15)
        subtrees = self._get_subtrees(status=self.status, rank=0)
        for s in subtrees:
            path = s['dir']['path']
            if path == which:
                self.assertEqual(s['auth_first'], 0)
                self.assertFalse(s['distributed_ephemeral_pin'])
                return
        # it has been merged into /tree

    def test_ephemeral_pin_dist_off(self):
        """
        That turning off ephemeral distributed pin merges subtrees.
        """

        self._setup_tree(distributed=True, export=0)
        self._wait_distributed_subtrees(100, status=self.status, rank="all")
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "0")
        self._wait_subtrees([('/tree', 0)], status=self.status)

    def test_ephemeral_pin_dist_conf_off(self):
        """
        That turning off ephemeral distributed pin config prevents distribution.
        """

        self._setup_tree(export=0)
        self.config_set('mds', 'mds_export_ephemeral_distributed', False)
        self.mount_a.setfattr("tree", "ceph.dir.pin.distributed", "1")
        time.sleep(30)
        self._wait_subtrees([('/tree', 0)], status=self.status)

    def test_ephemeral_pin_dist_conf_off_merge(self):
        """
        That turning off ephemeral distributed pin config merges subtrees.
        """

        self._setup_tree(distributed=True, export=0)
        self._wait_distributed_subtrees(100, status=self.status)
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
        time.sleep(10) # for something to not happen...
        self._wait_subtrees(test, timeout=60, status=self.status, rank="all", path="/tree/")

    def test_ephemeral_pin_dist_override_after(self):
        """
        That a conventional export pin overrides the distributed policy _after_ distributed policy is set.
        """

        self._setup_tree(count=10, distributed=True)
        subtrees = self._wait_distributed_subtrees(10, status=self.status, rank="all")
        victim = None
        test = []
        for s in subtrees:
            path = s['dir']['path']
            auth = s['auth_first']
            if auth in (0, 2) and victim is None:
                victim = path
                self.mount_a.setfattr(victim[1:], "ceph.dir.pin", "1")
                test.append((victim, 1))
            else:
                test.append((path, auth))
        self.assertIsNotNone(victim)
        self._wait_subtrees(test, status=self.status, rank="all", path="/tree/")

    def test_ephemeral_pin_dist_failover(self):
        """
        That MDS failover does not cause unnecessary migrations.
        """

        # pin /tree so it does not export during failover
        self._setup_tree(distributed=True, export=0)
        self._wait_distributed_subtrees(100, status=self.status, rank="all")
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

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

        count = 1000
        self._setup_tree(count=count, distributed=True)
        subtrees = self._wait_distributed_subtrees(count, status=self.status, rank="all")
        nsubtrees = len(subtrees)

        # Check if distribution is uniform
        rank0 = list(filter(lambda x: x['auth_first'] == 0, subtrees))
        rank1 = list(filter(lambda x: x['auth_first'] == 1, subtrees))
        rank2 = list(filter(lambda x: x['auth_first'] == 2, subtrees))
        self.assertGreaterEqual(len(rank0)/nsubtrees, 0.2)
        self.assertGreaterEqual(len(rank1)/nsubtrees, 0.2)
        self.assertGreaterEqual(len(rank2)/nsubtrees, 0.2)

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
        That ephemeral random and distributed can coexist with each other.
        """

        self._setup_tree(random=1.0, distributed=True, export=0)
        self._wait_distributed_subtrees(100, status=self.status)
        self._wait_random_subtrees(100, status=self.status)

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
        self._setup_tree(count=count, random=r, export=0)
        # wait for all random subtrees to be created, not a specific count
        time.sleep(30)
        subtrees = self._wait_random_subtrees(1, status=self.status, rank=1)
        test = [(s['dir']['path'], s['auth_first']) for s in subtrees]
        before = self.fs.ranks_perf(lambda p: p['mds']['exported'])
        log.info(f"export stats: {before}")
        self.fs.rank_fail(rank=1)
        self.status = self.fs.wait_for_daemons()
        time.sleep(30) # waiting for something to not happen
        self._wait_subtrees(test, status=self.status, rank=1)
        after = self.fs.ranks_perf(lambda p: p['mds']['exported'])
        log.info(f"export stats: {after}")
        self.assertEqual(before, after)

    def test_ephemeral_pin_grow_mds(self):
        """
        That consistent hashing works to reduce the number of migrations.
        """

        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()

        self._setup_tree(distributed=True)
        subtrees_old = self._wait_distributed_subtrees(100, status=self.status, rank="all")

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()
        
        # Sleeping for a while to allow the ephemeral pin migrations to complete
        time.sleep(30)
        
        subtrees_new = self._wait_distributed_subtrees(100, status=self.status, rank="all")
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

        self._setup_tree(distributed=True)
        subtrees_old = self._wait_distributed_subtrees(100, status=self.status, rank="all")

        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()
        time.sleep(30)

        subtrees_new = self._wait_distributed_subtrees(100, status=self.status, rank="all")
        count = 0
        for old_subtree in subtrees_old:
            for new_subtree in subtrees_new:
                if (old_subtree['dir']['path'] == new_subtree['dir']['path']) and (old_subtree['auth_first'] != new_subtree['auth_first']):
                    count = count + 1
                    break

        log.info("{0} migrations have occured due to the cluster resizing".format(count))
        # rebalancing from 3 -> 2 may cause half of rank 0/1 to move and all of rank 2
        self.assertLessEqual((count/len(subtrees_old)), (1.0/3.0/2.0 + 1.0/3.0/2.0 + 1.0/3.0)*1.25) # aka .66 with 25% overbudget
