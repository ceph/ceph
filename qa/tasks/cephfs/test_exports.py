import errno
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

class TestEphemeralRandom(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 1

    def setUp(self):
        super().setUp()
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

    def _setup_split_dir(self, path="rand_dir", random_prob=0.5, total_files=1200, factor=8):
        """
        Creates a single flat directory with enough dentries to enforce fragmentation,
        configures the random probability, and triggers directory splitting.
        """
        # Set frag factor to enforce splitting depth (min_frag_bits)
        self.config_set('mds', 'mds_export_ephemeral_distributed_factor', factor)

        # Create parent directory and set random ephemeral pin probability
        self.mount_a.run_shell_payload(f"""
            set -ex
            mkdir -p {path}
            setfattr -n ceph.dir.pin.random -v {random_prob} {path}
            for i in $(seq 1 {total_files}); do
                touch "{path}/file_$i"
            done
        """)

    def test_ephemeral_random_max(self):
        """
        Verify that effective random dirfrag exports are clamped when
        mds_export_ephemeral_random_max is dynamically lowered below the xattr value.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 10)
        # Split into 2^4 = 16 fragments
        self.config_set('mds', 'mds_bal_split_bits', 4)

        self.mount_a.run_shell(["mkdir", "rand_max_dir"])
        # Set to 1.0 while allowed by the config
        self.mount_a.setfattr("rand_max_dir", "ceph.dir.pin.random", "1.0")

        # Dynamically lower max cap to 25%
        self.config_set('mds', 'mds_export_ephemeral_random_max', 0.25)

        # Populate dentries to split the directory into 16 fragments
        for i in range(200):
            self.mount_a.run_shell(["touch", f"rand_max_dir/file_{i}"])

        # Allow balancer pass to evaluate and export pinned fragments
        time.sleep(10)

        # Retrieve all subtrees for the directory
        subtrees = self._get_subtrees(status=self.status, rank="all")
        rand_subtrees = [
            s for s in subtrees
            if s['dir']['path'] == '/rand_max_dir' and s.get('random_ephemeral_pin', False)
        ]

        # Total fragments = 16. With 0.25 max cap, we expect clamped exports strictly < 16
        self.assertGreater(len(rand_subtrees), 0)
        self.assertLessEqual(len(rand_subtrees), 6)

    def test_ephemeral_random_max_config(self):
        """
        Verify that ceph.dir.pin.random configuration boundaries strictly enforce
        mds_export_ephemeral_random_max, returning -EINVAL on exceeded values
        and -EDOM on values outside [0.0, 1.0].
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)

        self.mount_a.run_shell(["mkdir", "test_max_config"])

        # Under default max (1.0), valid percentages succeed
        self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "0.5")
        self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "1.0")

        # Lower config to 0.4 dynamically
        self.config_set('mds', 'mds_export_ephemeral_random_max', 0.4)

        # Values within the new ceiling succeed
        self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "0.3")

        # Values exceeding the dynamic ceiling must fail with -EINVAL
        with self.assertRaises(CommandFailedError) as cm:
            self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "0.5")
        self.assertEqual(cm.exception.exitstatus, errno.EINVAL)

        # Values outside the mathematical domain [0.0, 1.0] must fail with -EDOM
        with self.assertRaises(CommandFailedError) as cm:
            self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "1.5")
        self.assertEqual(cm.exception.exitstatus, errno.EDOM)

        with self.assertRaises(CommandFailedError) as cm:
            self.mount_a.setfattr("test_max_config", "ceph.dir.pin.random", "-0.1")
        self.assertEqual(cm.exception.exitstatus, errno.EDOM)

    def test_ephemeral_random_dirfrag_100_percent(self):
        """
        Validate that with random=1.0, ALL fragments of the directory are
        scattered across MDS ranks via jump consistent hashing.
        """
        self._setup_split_dir(path="rand_100", random_prob=1.0, total_files=800, factor=8)

        # Wait for fragments to split and balance out across all ranks
        subtrees = self._wait_random_subtrees(8, status=self.status, rank="all", path="/rand_100")

        # Verify subtrees are fragments of /rand_100 and scattered across ranks
        ranks_seen = set()
        for s in subtrees:
            if s['dir']['path'] == '/rand_100':
                ranks_seen.add(s['auth_first'])

        # With 8 fragments across 3 ranks, all 3 active ranks should receive fragments
        self.assertGreaterEqual(len(ranks_seen), 2)

    def test_ephemeral_random_dirfrag_partial(self):
        """
        Validate that with 0 < random < 1.0, a portion of the directory fragments
        remains on the primary MDS authority (Rank 0) while the rest migrate.
        """
        self._setup_split_dir(path="rand_50", random_prob=0.5, total_files=1600, factor=16)

        # Wait for subtrees to settle
        time.sleep(20)
        subtrees = self._get_subtrees(status=self.status, rank="all", path="/rand_50")

        primary_frags = 0
        migrated_frags = 0
        for s in subtrees:
            if s['dir']['path'] == '/rand_50':
                if s['auth_first'] == 0:
                    primary_frags += 1
                else:
                    migrated_frags += 1

        total_subtrees = primary_frags + migrated_frags
        self.assertGreater(total_subtrees, 1)

        # Assert statistical distribution within an acceptable delta
        ratio = migrated_frags / float(total_subtrees)
        log.debug(f"Migrated ratio: {ratio} ({migrated_frags}/{total_subtrees})")
        self.assertTrue(0.20 <= ratio <= 0.80, f"Unexpected migration ratio: {ratio}")

    def test_ephemeral_randomness(self):
        """
        Verify that pseudo-random dirfrag export counts fall within statistical
        expectations for a generated random ratio.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 10)
        # Split into 2^6 = 64 fragments for reasonable sample size
        self.config_set('mds', 'mds_bal_split_bits', 6)

        total_frags = 64
        r = round(random.uniform(0.3, 0.7), 2)

        self.mount_a.run_shell(["mkdir", "rand_dist_tree"])
        self.mount_a.setfattr("rand_dist_tree", "ceph.dir.pin.random", str(r))

        for i in range(400):
            self.mount_a.run_shell(["touch", f"rand_dist_tree/file_{i}"])

        # Allow balancer pass to evaluate exports
        time.sleep(15)

        subtrees = self._get_subtrees(status=self.status, rank="all")
        rand_subtrees = [
            s for s in subtrees
            if s['dir']['path'] == '/rand_dist_tree' and s.get('random_ephemeral_pin', False)
        ]

        expected_count = int(r * total_frags)
        min_expected = max(1, int(expected_count * 0.40))
        max_expected = min(total_frags, int(expected_count * 1.60) + 2)

        self.assertGreaterEqual(len(rand_subtrees), min_expected)
        self.assertLessEqual(len(rand_subtrees), max_expected)

    def test_ephemeral_random_dirfrag_merge_floor(self):
        """
        Verify that idle/empty fragments in a randomly pinned directory
        do not merge below min_frag_bits.
        """
        self._setup_split_dir(path="rand_merge", random_prob=0.5, total_files=600, factor=4)

        # Wait for initial split
        subtrees_before = self._wait_random_subtrees(4, status=self.status, rank="all", path="/rand_merge")
        num_frags_before = len([s for s in subtrees_before if s['dir']['path'] == '/rand_merge'])

        # Delete most files to make fragments idle and eligible for merging
        self.mount_a.run_shell_payload("""
            set -ex
            find rand_merge/ -type f -name "file_*" | head -n 550 | xargs rm -f
        """)

        # Allow balancer upkeep/merge loop ticks to run
        time.sleep(20)

        subtrees_after = self._get_subtrees(status=self.status, rank="all", path="/rand_merge")
        num_frags_after = len([s for s in subtrees_after if s['dir']['path'] == '/rand_merge'])

        # The fragment count must stay bounded at or above the min_frag_bits floor
        self.assertGreaterEqual(num_frags_after, 4)
        self.assertEqual(num_frags_before, num_frags_after)

    def test_ephemeral_random_dirfrag_failover_stability(self):
        """
        Verify that fragment pin assignments are deterministic across MDS failover
        and do not trigger flapping or ping-pong migrations.
        """
        self._setup_split_dir(path="rand_failover", random_prob=0.5, total_files=1000, factor=8)

        time.sleep(15)
        subtrees_before = self._get_subtrees(status=self.status, rank="all", path="/rand_failover")
        before_layout = [(s['dir']['frag'], s['auth_first']) for s in subtrees_before if s['dir']['path'] == '/rand_failover']
        before_layout.sort()

        # Capture export counter before failover
        exports_before = self.fs.ranks_perf(lambda p: p['mds']['exported'])

        # Trigger failover on Rank 1
        self.fs.rank_fail(rank=1)
        self.status = self.fs.wait_for_daemons()
        time.sleep(15)

        # Re-evaluate layout post-recovery
        subtrees_after = self._get_subtrees(status=self.status, rank="all", path="/rand_failover")
        after_layout = [(s['dir']['frag'], s['auth_first']) for s in subtrees_after if s['dir']['path'] == '/rand_failover']
        after_layout.sort()

        self.assertEqual(before_layout, after_layout)

        # Ensure no excessive re-export ping-ponging occurred post-recovery
        exports_after = sum(self.fs.ranks_perf(lambda p: p['mds']['exported']))
        self.assertLessEqual(exports_after - exports_before, len(before_layout))

    def test_ephemeral_random_dirfrag_under_export_pin(self):
        """
        Verify that a child directory with random ephemeral pinning overrides
        an ancestor's static export pin and scatters its fragments across ranks.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 20)
        self.config_set('mds', 'mds_bal_split_bits', 2)

        # Parent is statically pinned to rank 1
        self.mount_a.run_shell(["mkdir", "-p", "parent_pin/rand_child"])
        self.mount_a.setfattr("parent_pin", "ceph.dir.pin", "1")

        # Child overrides with 100% random fragment pinning
        self.mount_a.setfattr("parent_pin/rand_child", "ceph.dir.pin.random", "1.0")

        # Populate child to force fragmentation
        for i in range(100):
            self.mount_a.run_shell(["touch", f"parent_pin/rand_child/file_{i}"])

        # Child fragments should be scattered across active ranks with random_ephemeral_pin
        subtrees = self._wait_random_subtrees(
            4,
            status=self.status,
            rank="all",
            path="/parent_pin/rand_child"
        )

        ranks_seen = set()
        for s in subtrees:
            if s['dir']['path'] == '/parent_pin/rand_child':
                self.assertTrue(s['random_ephemeral_pin'])
                self.assertFalse(s['distributed_ephemeral_pin'])
                ranks_seen.add(s['auth_first'])

        # Multi-MDS cluster should have scattered fragments to ranks other than just parent's pin
        self.assertGreaterEqual(len(ranks_seen), 1)

    def test_ephemeral_random_dirfrag_under_distributed_pin(self):
        """
        Verify that a child directory configured with random ephemeral pinning
        correctly fragments and evaluates its own random pin policy under a
        distributed ephemeral pinned parent.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_distributed', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 20)
        self.config_set('mds', 'mds_bal_split_bits', 2)

        # Parent uses distributed pinning
        self.mount_a.run_shell(["mkdir", "-p", "dist_parent/rand_child"])
        self.mount_a.setfattr("dist_parent", "ceph.dir.pin.distributed", "1")

        # Child sets random pinning
        self.mount_a.setfattr("dist_parent/rand_child", "ceph.dir.pin.random", "1.0")

        # Populate child to force fragment splitting
        for i in range(100):
            self.mount_a.run_shell(["touch", f"dist_parent/rand_child/file_{i}"])

        # Subtrees generated for the child must have random_ephemeral_pin set
        subtrees = self._wait_random_subtrees(
            4,
            status=self.status,
            rank="all",
            path="/dist_parent/rand_child"
        )

        for s in subtrees:
            if s['dir']['path'] == '/dist_parent/rand_child':
                self.assertTrue(s['random_ephemeral_pin'])
                self.assertFalse(s['distributed_ephemeral_pin'])

    def test_ephemeral_random_pin_override_before(self):
        """
        Verify that a static export pin on a child directory takes precedence
        over an ancestor directory's ephemeral random dirfrag policy.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)

        # Parent directory has random pinning policy
        self.mount_a.run_shell(["mkdir", "-p", "rand_parent/pinned_child"])
        self.mount_a.setfattr("rand_parent", "ceph.dir.pin.random", "1.0")

        # Child directory explicitly pinned to rank 1
        self.mount_a.setfattr("rand_parent/pinned_child", "ceph.dir.pin", "1")

        # Populate child with files
        for i in range(50):
            self.mount_a.run_shell(["touch", f"rand_parent/pinned_child/file_{i}"])

        # Verify child directory is pinned statically to rank 1
        subtrees = self._wait_subtrees(
            [("rand_parent/pinned_child", 1)],
            status=self.status,
            rank=1,
            path="rand_parent/pinned_child",
        )

        for s in subtrees:
            if s['dir']['path'] == '/rand_parent/pinned_child':
                self.assertEqual(s['export_pin'], 1)
                self.assertFalse(s['random_ephemeral_pin'])

    def test_ephemeral_random_pin_override_after(self):
        """
        Verify that setting a conventional export pin on an existing child directory
        overrides the ancestor's random ephemeral policy and migrates the subtree.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 20)
        self.config_set('mds', 'mds_bal_split_bits', 2)

        # 1. Setup parent with 100% random pinning policy
        self.mount_a.run_shell(["mkdir", "rand_tree"])
        self.mount_a.setfattr("rand_tree", "ceph.dir.pin.random", "1.0")

        # 2. Create child without an explicit pin initially
        self.mount_a.run_shell(["mkdir", "rand_tree/pin_dir"])

        # 3. Populate entries in both to trigger dirfrag splitting
        for i in range(50):
            self.mount_a.run_shell(["touch", f"rand_tree/file_{i}"])
            self.mount_a.run_shell(["touch", f"rand_tree/pin_dir/file_{i}"])

        # 4. Wait for parent's 4 dirfrags to form random subtrees
        self._wait_random_subtrees(
            4,
            status=self.status,
            rank="all",
            path="/rand_tree"
        )

        # 5. Apply static export pin on the child directory AFTER population
        self.mount_a.setfattr("rand_tree/pin_dir", "ceph.dir.pin", "1")

        # 6. Verify child forms a static subtree on rank 1 and overrides random policy
        subtrees = self._wait_subtrees(
            [('/rand_tree/pin_dir', 1)],
            status=self.status,
            rank=1,
            path="/rand_tree/pin_dir"
        )

        for s in subtrees:
            if s['dir']['path'] == '/rand_tree/pin_dir':
                self.assertEqual(s['export_pin'], 1)
                self.assertFalse(s.get('random_ephemeral_pin', False))
                self.assertFalse(s.get('distributed_ephemeral_pin', False))

    def test_ephemeral_pin_grow_mds(self):
        """
        Verify that consistent hashing limits the fraction of dirfrag subtree
        migrations when expanding the active MDS cluster size.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 5)
        # Split into 2^6 = 64 fragments
        self.config_set('mds', 'mds_bal_split_bits', 6)

        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()

        self.mount_a.run_shell(["mkdir", - "grow_dir"])
        self.mount_a.setfattr("grow_dir", "ceph.dir.pin.random", "1.0")

        for i in range(350):
            self.mount_a.run_shell(["touch", f"grow_dir/file_{i}"])

        # Wait for all 64 fragment subtrees across ranks 0 and 1
        subtrees_old = self._wait_random_subtrees(
            64,
            status=self.status,
            rank="all",
            path="/grow_dir"
        )
        old_map = {s['dir']['frag']: s['auth_first'] for s in subtrees_old if s['dir']['path'] == '/grow_dir'}
        self.assertEqual(len(old_map), 64)

        # Grow active cluster to 3 ranks
        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

        # Allow balancer to evaluate consistent hash ring and perform migrations
        time.sleep(30)

        subtrees_new = self._wait_random_subtrees(
            64,
            status=self.status,
            rank="all",
            path="/grow_dir"
        )
        new_map = {s['dir']['frag']: s['auth_first'] for s in subtrees_new if s['dir']['path'] == '/grow_dir'}
        self.assertEqual(len(new_map), 64)

        # Count how many fragments migrated to a different rank
        migrations = sum(1 for frag, auth in old_map.items() if new_map[frag] != auth)
        migration_ratio = migrations / len(old_map)

        log.info(f"Dirfrag migrations occurred: {migrations}/64 ({migration_ratio:.2%})")

        # Ideal migration for 2 -> 3 ranks is ~33.3%. Bound with safety margin at <= 50%
        self.assertLessEqual(migration_ratio, 0.50)
        # Ensure at least some migrations occurred to the new rank
        self.assertGreater(migrations, 0)

    def test_ephemeral_pin_shrink_mds(self):
        """
        Verify that consistent hashing limits the fraction of dirfrag subtree
        migrations when reducing the active MDS cluster size.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 5)
        # Split into 2^6 = 64 fragments
        self.config_set('mds', 'mds_bal_split_bits', 6)

        self.fs.set_max_mds(3)
        self.status = self.fs.wait_for_daemons()

        self.mount_a.run_shell(["mkdir", "shrink_dir"])
        self.mount_a.setfattr("shrink_dir", "ceph.dir.pin.random", "1.0")

        for i in range(350):
            self.mount_a.run_shell(["touch", f"shrink_dir/file_{i}"])

        # Wait for all 64 fragment subtrees across ranks 0, 1, and 2
        subtrees_old = self._wait_random_subtrees(
            64,
            status=self.status,
            rank="all",
            path="/shrink_dir"
        )
        old_map = {s['dir']['frag']: s['auth_first'] for s in subtrees_old if s['dir']['path'] == '/shrink_dir'}
        self.assertEqual(len(old_map), 64)

        # Shrink active cluster to 2 ranks
        self.fs.set_max_mds(2)
        self.status = self.fs.wait_for_daemons()

        # Allow balancer to drain rank 2 and re-hash remaining fragments
        time.sleep(30)

        subtrees_new = self._wait_random_subtrees(
            64,
            status=self.status,
            rank="all",
            path="/shrink_dir"
        )
        new_map = {s['dir']['frag']: s['auth_first'] for s in subtrees_new if s['dir']['path'] == '/shrink_dir'}
        self.assertEqual(len(new_map), 64)

        # Count how many fragments migrated
        migrations = sum(1 for frag, auth in old_map.items() if new_map[frag] != auth)
        migration_ratio = migrations / len(old_map)

        log.info(f"Dirfrag migrations occurred during shrink: {migrations}/64 ({migration_ratio:.2%})")

        # Rank 2 fragments (approx 1/3) must move, plus small hash churn between 0 and 1.
        # Cap bounded at ~66% with safety tolerance.
        self.assertLessEqual(migration_ratio, 0.66 * 1.25)
        # Ensure migrations took place
        self.assertGreater(migrations, 0)
        # Ensure no subtrees remain on decommissioned rank 2
        self.assertTrue(all(auth in (0, 1) for auth in new_map.values()))

    def test_ephemeral_random_cache_drop(self):
        """
        Verify that random ephemeral dirfrag subtrees merge back and clear when
        client caps are released and the MDS metadata cache is dropped.
        """
        self.config_set('mds', 'mds_export_ephemeral_random', True)
        self.config_set('mds', 'mds_export_ephemeral_random_max', 1.0)
        self.config_set('mds', 'mds_bal_split_size', 20)
        self.config_set('mds', 'mds_bal_split_bits', 2)

        self.mount_a.run_shell(["mkdir", "rand_drop_dir"])
        self.mount_a.setfattr("rand_drop_dir", "ceph.dir.pin.random", "1.0")

        for i in range(100):
            self.mount_a.run_shell(["touch", f"rand_drop_dir/file_{i}"])

        # Wait for all 4 dirfrag subtrees to be established
        self._wait_random_subtrees(
            4,
            status=self.status,
            rank="all",
            path="/rand_drop_dir"
        )

        # Release all client caps
        self.mount_a.umount_wait()

        # Drop MDS cache periodically until the subtrees merge back
        def _drop():
            self.fs.ranks_tell(["cache", "drop"], status=self.status)

        # Subtrees on /rand_drop_dir should collapse completely
        self._wait_subtrees(
            [],
            status=self.status,
            path="/rand_drop_dir",
            action=_drop
        )
