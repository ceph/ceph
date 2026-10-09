import logging
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

class TestEphemeralDistributed(CephFSTestCase):
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

# Model of how the MDS places the dirfrags of a randomly pinned directory.
# It mirrors rjhash64() (src/include/hash.h), CInode::should_random_pin_frag()
# and MDCache::hash_into_rank_bucket(), so that tests can assert the exact
# placement of every dirfrag rather than rely on statistical bounds.
_U64 = (1 << 64) - 1
_FRAG_VALUE_BITS = 24

def _rjhash64(key):
    key = ((~key) + (key << 21)) & _U64
    key ^= key >> 24
    key = (key + (key << 3) + (key << 8)) & _U64
    key ^= key >> 14
    key = (key + (key << 2) + (key << 4)) & _U64
    key ^= key >> 28
    key = (key + (key << 31)) & _U64
    return key

def _frag_hash(ino, frag_value):
    return _rjhash64((_rjhash64(ino) + _rjhash64(frag_value)) & _U64)

def _should_random_pin_frag(ino, frag_value, prob):
    if prob <= 0.0:
        return False
    if prob >= 1.0:
        return True
    h = _frag_hash(ino, frag_value)
    return (h >> 11) * (1.0 / 9007199254740992.0) < prob

def _hash_into_rank_bucket(ino, frag_value, max_mds):
    h = _frag_hash(ino, frag_value)
    b, j = -1, 0
    while j < max_mds:
        b = j
        h = (h * 2862933555777941757 + 1) & _U64
        j = int((b + 1) * (float(1 << 31) / float((h >> 33) + 1)))
    return b

def _ephemeral_frag_bits(factor, max_mds):
    want = int(factor * max_mds)
    n = 0
    while (1 << n) < want:
        n += 1
    return n

def _frag_name(frag_value, bits):
    """
    The frag as printed in a subtree's dirfrag, e.g. "101*".
    """
    return f"{frag_value >> (_FRAG_VALUE_BITS - bits):0{bits}b}*"

class TestEphemeralRandom(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 1

    # A split of the root dirfrag is never shallower than mds_bal_split_bits
    # (default 3). Keep the ephemeral split depth at or above it so that a
    # directory is split exactly to that depth and never merged below it.
    MIN_FRAG_BITS = 3

    def setUp(self):
        super().setUp()
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

    def _wait_mds_config(self, key, value, timeout=30):
        """
        Wait until every active rank has picked up a config change.
        """
        def _applied():
            for _, out in self.fs.ranks_tell(["config", "get", key], status=self.status):
                if float(out[key]) != float(value):
                    return False
            return True
        self.wait_until_true(_applied, timeout=timeout)

    def _setup_random_dir(self, path, prob, factor=2, max_mds=3, files_per_frag=16):
        """
        Create a flat directory holding enough files for every dirfrag to be
        non-empty (empty dirfrags are neither exported nor kept as subtrees)
        and set its random ephemeral pin. The directory is split to 2^bits
        dirfrags, bits being derived from mds_export_ephemeral_frag_factor
        and max_mds. Returns bits.
        """
        bits = _ephemeral_frag_bits(factor, max_mds)
        self.assertGreaterEqual(bits, self.MIN_FRAG_BITS)
        self.config_set('mds', 'mds_export_ephemeral_frag_factor', factor)
        self._wait_mds_config('mds_export_ephemeral_frag_factor', factor)
        nfiles = files_per_frag * (1 << bits)
        self.mount_a.run_shell_payload(f"""
set -e
mkdir -p {path}
cd {path}
seq -f 'file_%g' 1 {nfiles} | xargs touch
""")
        self.mount_a.setfattr(path, "ceph.dir.pin.random", str(prob))
        return bits

    def _expected_random_placement(self, path, prob, bits, max_mds=3):
        """
        Return {frag: rank} for the dirfrags of path that should be randomly
        pinned, path being split uniformly to bits.
        """
        ino = self.mount_a.path_to_ino(path)
        placement = {}
        for v in range(1 << bits):
            value = v << (_FRAG_VALUE_BITS - bits)
            if _should_random_pin_frag(ino, value, prob):
                placement[_frag_name(value, bits)] = _hash_into_rank_bucket(ino, value, max_mds)
        log.info(f"expected placement of {path} (prob={prob}, max_mds={max_mds}): "
                 f"{len(placement)}/{1 << bits} dirfrags pinned: {placement}")
        return placement

    def _get_random_placement(self, path):
        """
        Return {frag: auth rank} for the dirfrags of path that are subtree
        roots. Dirfrags that are not randomly pinned are merged into their
        parent subtree and so are not listed.
        """
        subtrees = self._get_subtrees(status=self.status, rank="all", path=f"/{path}")
        placement = {}
        for s in subtrees:
            if s['dir']['path'] != f"/{path}":
                continue
            self.assertTrue(s['random_ephemeral_pin'])
            self.assertFalse(s['distributed_ephemeral_pin'])
            frag = s['dir']['dirfrag'].split('.')[-1]
            placement[frag] = s['auth_first']
        return placement

    def _wait_random_placement(self, path, expected, timeout=300, sleep=5):
        actual = None
        try:
            with safe_while(sleep=sleep, tries=timeout//sleep) as proceed:
                while proceed():
                    actual = self._get_random_placement(path)
                    if actual == expected:
                        return actual
                    missing = {f: r for f, r in expected.items() if actual.get(f) != r}
                    unexpected = {f: r for f, r in actual.items() if expected.get(f) != r}
                    log.info(f"placement of {path}: {len(missing)} dirfrags not yet "
                             f"placed {missing}, {len(unexpected)} misplaced {unexpected}")
        except MaxWhileTries as e:
            raise RuntimeError(f"dirfrags of {path} did not reach the expected placement "
                               f"{expected}, got {actual}") from e

    def _get_dirfrags(self, path, rank=0):
        frags = self.fs.rank_asok(["dirfrag", "ls", f"/{path}"], rank=rank, status=self.status)
        return sorted(_frag_name(f['value'], f['bits']) for f in frags)

    def _setfattr_fails(self, path, key, val, error):
        p = self.mount_a.setfattr(path, key, val, wait=False)
        with self.assertRaises(CommandFailedError):
            p.wait()
        self.assertIn(error, p.stderr.getvalue())

    def test_ephemeral_random_dirfrag_100_percent(self):
        """
        That with ceph.dir.pin.random=1.0, every dirfrag of the directory is
        pinned and placed across the active ranks by consistent hashing.
        """
        bits = self._setup_random_dir("rand_100", 1.0, factor=8)  # 32 dirfrags
        expected = self._expected_random_placement("rand_100", 1.0, bits)
        self.assertEqual(len(expected), 1 << bits)
        self._wait_random_placement("rand_100", expected)
        self.assertEqual(set(expected.values()), {0, 1, 2})

    def test_ephemeral_random_dirfrag_partial(self):
        """
        That with 0 < ceph.dir.pin.random < 1.0, only the selected dirfrags are
        pinned while the rest stay with the directory's authority.
        """
        bits = self._setup_random_dir("rand_50", 0.5, factor=16)  # 64 dirfrags
        expected = self._expected_random_placement("rand_50", 0.5, bits)
        self.assertGreater(len(expected), 0)
        self.assertLess(len(expected), 1 << bits)
        self._wait_random_placement("rand_50", expected)

    def test_ephemeral_randomness(self):
        """
        That an arbitrary ceph.dir.pin.random value pins about that fraction
        of the dirfrags.
        """
        r = round(random.uniform(0.3, 0.7), 2)
        bits = self._setup_random_dir("rand_dist_tree", r, factor=16)  # 64 dirfrags
        expected = self._expected_random_placement("rand_dist_tree", r, bits)
        nfrags = 1 << bits
        sd = (nfrags * r * (1 - r)) ** 0.5
        self.assertLessEqual(abs(len(expected) - nfrags * r), 4 * sd)
        self._wait_random_placement("rand_dist_tree", expected)

    def test_ephemeral_random_max(self):
        """
        That lowering mds_export_ephemeral_random_max below a directory's
        ceph.dir.pin.random value caps the fraction of pinned dirfrags, and
        that the change applies to an already pinned directory.
        """
        bits = self._setup_random_dir("rand_max_dir", 1.0, factor=16)  # 64 dirfrags
        self._wait_random_placement("rand_max_dir",
                                    self._expected_random_placement("rand_max_dir", 1.0, bits))

        self.config_set('mds', 'mds_export_ephemeral_random_max', 0.25)
        expected = self._expected_random_placement("rand_max_dir", 0.25, bits)
        self.assertLess(len(expected), 1 << bits)
        self._wait_random_placement("rand_max_dir", expected)

        # the policy itself is left alone
        self.assertEqual(float(self.mount_a.getfattr("rand_max_dir", "ceph.dir.pin.random")), 1.0)

    def test_ephemeral_random_max_config(self):
        """
        That ceph.dir.pin.random is rejected with EINVAL when it exceeds
        mds_export_ephemeral_random_max and with EDOM when outside [0.0, 1.0].
        """
        self.mount_a.run_shell(["mkdir", "test_max_config"])

        self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "0.5")
        self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "1.0")

        self.config_set('mds', 'mds_export_ephemeral_random_max', 0.4)
        self._wait_mds_config('mds_export_ephemeral_random_max', 0.4)

        self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "0.3")
        self._setfattr_fails("test_max_config", "ceph.dir.pin.random", "0.5",
                             "Invalid argument")
        self._setfattr_fails("test_max_config", "ceph.dir.pin.random", "1.5",
                             "Numerical argument out of domain")
        self._setfattr_fails("test_max_config", "ceph.dir.pin.random", "-0.1",
                             "Numerical argument out of domain")

    def test_ephemeral_random_dirfrag_merge_floor(self):
        """
        That the dirfrags of a randomly pinned directory are not merged below
        the ephemeral split depth once they become small.
        """
        bits = self._setup_random_dir("rand_merge", 0.5, factor=4)  # 16 dirfrags
        expected = self._expected_random_placement("rand_merge", 0.5, bits)
        self._wait_random_placement("rand_merge", expected)
        frags = self._get_dirfrags("rand_merge")
        self.assertEqual(len(frags), 1 << bits)

        # Every dirfrag is now well below mds_bal_merge_size. Unlinking
        # entries makes the MDS consider merging them.
        self.mount_a.run_shell_payload("find rand_merge -name 'file_*' | tail -n +65 | xargs rm -f")
        time.sleep(30) # for merges to not happen...

        # N.B. the placement is not checked here: a pinned dirfrag that became
        # empty is sent back to the directory's authority.
        self.assertEqual(self._get_dirfrags("rand_merge"), frags)

    def test_ephemeral_random_dirfrag_failover_stability(self):
        """
        That MDS failover neither changes the dirfrag placement nor causes
        unnecessary migrations.
        """
        bits = self._setup_random_dir("rand_failover", 0.5, factor=8)  # 32 dirfrags
        expected = self._expected_random_placement("rand_failover", 0.5, bits)
        self._wait_random_placement("rand_failover", expected)

        before = self.fs.ranks_perf(lambda p: p['mds']['exported'], status=self.status)
        log.info(f"export stats: {before}")
        self.fs.rank_fail(rank=1)
        self.status = self.fs.wait_for_daemons()
        time.sleep(15) # waiting for something to not happen
        after = self.fs.ranks_perf(lambda p: p['mds']['exported'], status=self.status)
        log.info(f"export stats: {after}")

        self.assertEqual(self._get_random_placement("rand_failover"), expected)
        self.assertEqual(before, after)

    def test_ephemeral_random_dirfrag_under_export_pin(self):
        """
        That a randomly pinned directory places its dirfrags across the ranks
        even when an ancestor is export pinned.
        """
        self.mount_a.run_shell(["mkdir", "-p", "parent_pin/rand_child"])
        self.mount_a.setfattr("parent_pin", "ceph.dir.pin", "1")
        self._wait_subtrees([('/parent_pin', 1)], status=self.status, rank=1, path="/parent_pin")

        bits = self._setup_random_dir("parent_pin/rand_child", 1.0)
        expected = self._expected_random_placement("parent_pin/rand_child", 1.0, bits)
        self._wait_random_placement("parent_pin/rand_child", expected)

    def test_ephemeral_random_dirfrag_under_distributed_pin(self):
        """
        That a randomly pinned directory places its dirfrags by its own policy
        under a directory with ephemeral distributed pinning.
        """
        self.config_set('mds', 'mds_export_ephemeral_distributed', True)
        self.mount_a.run_shell(["mkdir", "-p", "dist_parent/rand_child"])
        self.mount_a.setfattr("dist_parent", "ceph.dir.pin.distributed", "1")

        bits = self._setup_random_dir("dist_parent/rand_child", 1.0)
        expected = self._expected_random_placement("dist_parent/rand_child", 1.0, bits)
        self._wait_random_placement("dist_parent/rand_child", expected)

    def test_ephemeral_random_pin_override_before(self):
        """
        That an export pin on a child directory set before it is populated
        overrides the parent's random ephemeral pin.
        """
        self.mount_a.run_shell(["mkdir", "-p", "rand_parent/pinned_child"])
        self.mount_a.setfattr("rand_parent", "ceph.dir.pin.random", "1.0")
        self.mount_a.setfattr("rand_parent/pinned_child", "ceph.dir.pin", "1")
        self.mount_a.run_shell_payload("cd rand_parent/pinned_child && seq -f 'file_%g' 1 50 | xargs touch")

        subtrees = self._wait_subtrees([("/rand_parent/pinned_child", 1)], status=self.status,
                                       rank=1, path="/rand_parent/pinned_child")
        for s in subtrees:
            self.assertEqual(s['export_pin'], 1)
            self.assertFalse(s['random_ephemeral_pin'])

    def test_ephemeral_random_pin_override_after(self):
        """
        That an export pin set on an existing child directory overrides the
        parent's random ephemeral pin and migrates the child, leaving the
        parent's dirfrags in place.
        """
        self.mount_a.run_shell_payload("""
set -e
mkdir -p rand_tree/pin_dir
cd rand_tree/pin_dir
seq -f 'file_%g' 1 50 | xargs touch
""")
        bits = self._setup_random_dir("rand_tree", 1.0)
        expected = self._expected_random_placement("rand_tree", 1.0, bits)
        self._wait_random_placement("rand_tree", expected)

        self.mount_a.setfattr("rand_tree/pin_dir", "ceph.dir.pin", "1")
        subtrees = self._wait_subtrees([("/rand_tree/pin_dir", 1)], status=self.status,
                                       rank=1, path="/rand_tree/pin_dir")
        for s in subtrees:
            self.assertEqual(s['export_pin'], 1)
            self.assertFalse(s['random_ephemeral_pin'])
            self.assertFalse(s['distributed_ephemeral_pin'])

        self.assertEqual(self._get_random_placement("rand_tree"), expected)

    # With this factor, both 2 and 3 active ranks split a directory into 64
    # dirfrags, so that the placement of the same dirfrags can be compared.
    RESIZE_FRAG_FACTOR = 20

    def test_ephemeral_pin_grow_mds(self):
        """
        That growing the number of active ranks only migrates dirfrags to the
        new rank.
        """
        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()

        factor = self.RESIZE_FRAG_FACTOR
        bits = self._setup_random_dir("grow_dir", 1.0, factor=factor, max_mds=2)
        self.assertEqual(bits, _ephemeral_frag_bits(factor, 3))
        old = self._expected_random_placement("grow_dir", 1.0, bits, max_mds=2)
        self._wait_random_placement("grow_dir", old)
        exported_before = sum(n for _, n in self.fs.ranks_perf(lambda p: p['mds']['exported'],
                                                               status=self.status))

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

        new = self._expected_random_placement("grow_dir", 1.0, bits, max_mds=3)
        moved = [f for f in old if old[f] != new[f]]
        log.info(f"{len(moved)}/{len(old)} dirfrags to migrate: {moved}")
        self.assertGreater(len(moved), 0)
        self.assertTrue(all(new[f] == 2 for f in moved))
        self._wait_random_placement("grow_dir", new)

        exported_after = sum(n for _, n in self.fs.ranks_perf(lambda p: p['mds']['exported'],
                                                              status=self.status))
        self.assertEqual(exported_after - exported_before, len(moved))

    def test_ephemeral_pin_shrink_mds(self):
        """
        That shrinking the number of active ranks only migrates the dirfrags
        of the stopped rank.
        """
        factor = self.RESIZE_FRAG_FACTOR
        bits = self._setup_random_dir("shrink_dir", 1.0, factor=factor, max_mds=3)
        self.assertEqual(bits, _ephemeral_frag_bits(factor, 2))
        old = self._expected_random_placement("shrink_dir", 1.0, bits, max_mds=3)
        self._wait_random_placement("shrink_dir", old)

        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()

        new = self._expected_random_placement("shrink_dir", 1.0, bits, max_mds=2)
        moved = [f for f in old if old[f] != new[f]]
        log.info(f"{len(moved)}/{len(old)} dirfrags to migrate: {moved}")
        self.assertEqual(sorted(moved), sorted(f for f in old if old[f] == 2))
        self.assertGreater(len(moved), 0)
        self._wait_random_placement("shrink_dir", new)

    def test_ephemeral_random_cache_drop(self):
        """
        That randomly pinned dirfrag subtrees are dropped once nothing is
        left in cache.
        """
        bits = self._setup_random_dir("rand_drop_dir", 1.0)
        expected = self._expected_random_placement("rand_drop_dir", 1.0, bits)
        self._wait_random_placement("rand_drop_dir", expected)

        self.mount_a.umount_wait() # release all caps
        def _drop():
            self.fs.ranks_tell(["cache", "drop"], status=self.status)
        self._wait_subtrees([], status=self.status, rank="all", path="/rand_drop_dir",
                            action=_drop, timeout=120)
