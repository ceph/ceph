import errno
import json
import logging
import signal
from textwrap import dedent
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.orchestra.run import Raw
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

MDS_RESTART_GRACE = 60

class TestSnapshots(CephFSTestCase):
    MDSS_REQUIRED = 3
    LOAD_SETTINGS = ["mds_max_snaps_per_dir"]

    def _check_subtree(self, rank, path, status=None):
        got_subtrees = self.fs.rank_asok(["get", "subtrees"], rank=rank, status=status)
        for s in got_subtrees:
            if s['dir']['path'] == path and s['auth_first'] == rank:
                return True
        return False

    def _get_snapclient_dump(self, rank=0, status=None):
        return self.fs.rank_asok(["dump", "snaps"], rank=rank, status=status)

    def _get_snapserver_dump(self, rank=0, status=None):
        return self.fs.rank_asok(["dump", "snaps", "--server"], rank=rank, status=status)

    def _get_last_created_snap(self, rank=0, status=None):
        return int(self._get_snapserver_dump(rank,status=status)["last_created"])

    def _get_last_destroyed_snap(self, rank=0, status=None):
        return int(self._get_snapserver_dump(rank,status=status)["last_destroyed"])

    def _get_pending_snap_update(self, rank=0, status=None):
        return self._get_snapserver_dump(rank,status=status)["pending_update"]

    def _get_pending_snap_destroy(self, rank=0, status=None):
        return self._get_snapserver_dump(rank,status=status)["pending_destroy"]

    def test_allow_new_snaps_config(self):
        """
        Check whether 'allow_new_snaps' setting works
        """
        self.mount_a.run_shell(["mkdir", "test-allow-snaps"])

        self.fs.set_allow_new_snaps(False);
        try:
            self.mount_a.run_shell(["mkdir", "test-allow-snaps/.snap/snap00"])
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM, "expected EPERM")
        else:
            self.fail("expected snap creatiion to fail")

        self.fs.set_allow_new_snaps(True);
        self.mount_a.run_shell(["mkdir", "test-allow-snaps/.snap/snap00"])
        self.mount_a.run_shell(["rmdir", "test-allow-snaps/.snap/snap00"])
        self.mount_a.run_shell(["rmdir", "test-allow-snaps"])

    def test_kill_mdstable(self):
        """
        check snaptable transcation
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        # setup subtrees
        self.mount_a.run_shell(["mkdir", "-p", "d1/dir"])
        self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
        self._wait_subtrees([("/d1", 1)], rank=1, path="/d1")

        last_created = self._get_last_created_snap(rank=0,status=status)

        # mds_kill_mdstable_at:
        #  1: MDSTableServer::handle_prepare
        #  2: MDSTableServer::_prepare_logged
        #  5: MDSTableServer::handle_commit
        #  6: MDSTableServer::_commit_logged
        for i in [1,2,5,6]:
            log.info("testing snapserver mds_kill_mdstable_at={0}".format(i))

            status = self.fs.status()
            rank0 = self.fs.get_rank(rank=0, status=status)
            self.fs.rank_freeze(True, rank=0)
            self.fs.rank_asok(['config', 'set', "mds_kill_mdstable_at", "{0}".format(i)], rank=0, status=status)
            proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s1{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=0), timeout=self.fs.beacon_timeout);
            self.delete_mds_coredump(rank0['name']);

            self.fs.rank_fail(rank=0)
            self.fs.mds_restart(rank0['name'])
            self.wait_for_daemon_start([rank0['name']])
            status = self.fs.wait_for_daemons()

            proc.wait()
            last_created += 1
            self.wait_until_true(lambda: self._get_last_created_snap(rank=0) == last_created, timeout=30)

        self.set_conf("mds", "mds_reconnect_timeout", "5")

        self.mount_a.run_shell(["rmdir", Raw("d1/dir/.snap/*")])

        # set mds_kill_mdstable_at, also kill snapclient
        for i in [2,5,6]:
            log.info("testing snapserver mds_kill_mdstable_at={0}, also kill snapclient".format(i))
            status = self.fs.status()
            last_created = self._get_last_created_snap(rank=0, status=status)

            rank0 = self.fs.get_rank(rank=0, status=status)
            rank1 = self.fs.get_rank(rank=1, status=status)
            self.fs.rank_freeze(True, rank=0) # prevent failover...
            self.fs.rank_freeze(True, rank=1) # prevent failover...
            self.fs.rank_asok(['config', 'set', "mds_kill_mdstable_at", "{0}".format(i)], rank=0, status=status)
            proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s2{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=0), timeout=self.fs.beacon_timeout);
            self.delete_mds_coredump(rank0['name']);

            self.fs.rank_signal(signal.SIGKILL, rank=1)

            self.mount_a.suspend_netns()

            self.fs.rank_fail(rank=0)
            self.fs.mds_restart(rank0['name'])
            self.wait_for_daemon_start([rank0['name']])

            self.fs.wait_for_state('up:resolve', rank=0, timeout=MDS_RESTART_GRACE)
            if i in [2,5]:
                self.assertEqual(len(self._get_pending_snap_update(rank=0)), 1)
            elif i == 6:
                self.assertEqual(len(self._get_pending_snap_update(rank=0)), 0)
                self.assertGreater(self._get_last_created_snap(rank=0), last_created)

            self.fs.rank_fail(rank=1)
            self.fs.mds_restart(rank1['name'])
            self.wait_for_daemon_start([rank1['name']])
            self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)

            if i in [2,5]:
                self.wait_until_true(lambda: len(self._get_pending_snap_update(rank=0)) == 0, timeout=30)
                if i == 2:
                    self.assertEqual(self._get_last_created_snap(rank=0), last_created)
                else:
                    self.assertGreater(self._get_last_created_snap(rank=0), last_created)

            self.mount_a.resume_netns()
            try:
                proc.wait()
            except CommandFailedError:
                pass
            self.mount_a.remount()
            self.fs.flush()

        self.mount_a.run_shell(["rmdir", Raw("d1/dir/.snap/*")])

        # mds_kill_mdstable_at:
        #  3: MDSTableClient::handle_request (got agree)
        #  4: MDSTableClient::commit
        #  7: MDSTableClient::handle_request (got ack)
        for i in [3,4,7]:
            log.info("testing snapclient mds_kill_mdstable_at={0}".format(i))
            last_created = self._get_last_created_snap(rank=0)

            status = self.fs.status()
            rank1 = self.fs.get_rank(rank=1, status=status)
            self.fs.rank_freeze(True, rank=1) # prevent failover...
            self.fs.rank_asok(['config', 'set', "mds_kill_mdstable_at", "{0}".format(i)], rank=1, status=status)
            proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s3{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=1), timeout=self.fs.beacon_timeout);
            self.delete_mds_coredump(rank1['name']);

            self.mount_a.suspend_netns()

            if i in [3,4]:
                self.assertEqual(len(self._get_pending_snap_update(rank=0)), 1)
            elif i == 7:
                self.assertEqual(len(self._get_pending_snap_update(rank=0)), 0)
                self.assertGreater(self._get_last_created_snap(rank=0), last_created)

            self.fs.rank_fail(rank=1)
            self.fs.mds_restart(rank1['name'])
            self.wait_for_daemon_start([rank1['name']])
            status = self.fs.wait_for_daemons(timeout=MDS_RESTART_GRACE)

            if i in [3,4]:
                self.wait_until_true(lambda: len(self._get_pending_snap_update(rank=0)) == 0, timeout=30)
                if i == 3:
                    self.assertEqual(self._get_last_created_snap(rank=0), last_created)
                else:
                    self.assertGreater(self._get_last_created_snap(rank=0), last_created)

            self.mount_a.resume_netns()
            try:
                proc.wait()
            except CommandFailedError:
                pass
            self.mount_a.remount()
            self.fs.flush()

        self.mount_a.run_shell(["rmdir", Raw("d1/dir/.snap/*")])

        # mds_kill_mdstable_at:
        #  3: MDSTableClient::handle_request (got agree)
        #  8: MDSTableServer::handle_rollback
        log.info("testing snapclient mds_kill_mdstable_at=3, snapserver mds_kill_mdstable_at=8")
        last_created = self._get_last_created_snap(rank=0)

        status = self.fs.status()
        rank0 = self.fs.get_rank(rank=0, status=status)
        rank1 = self.fs.get_rank(rank=1, status=status)
        self.fs.rank_freeze(True, rank=0)
        self.fs.rank_freeze(True, rank=1)
        self.fs.rank_asok(['config', 'set', "mds_kill_mdstable_at", "8"], rank=0, status=status)
        self.fs.rank_asok(['config', 'set', "mds_kill_mdstable_at", "3"], rank=1, status=status)
        proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s4"], wait=False)
        self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=1), timeout=self.fs.beacon_timeout);
        self.delete_mds_coredump(rank1['name']);

        self.mount_a.suspend_netns()

        self.assertEqual(len(self._get_pending_snap_update(rank=0)), 1)

        self.fs.rank_fail(rank=1)
        self.fs.mds_restart(rank1['name'])
        self.wait_for_daemon_start([rank1['name']])

        # rollback triggers assertion
        self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=0), timeout=self.fs.beacon_timeout);
        self.delete_mds_coredump(rank0['name']);
        self.fs.rank_fail(rank=0)
        self.fs.mds_restart(rank0['name'])
        self.wait_for_daemon_start([rank0['name']])
        self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)

        # mds.1 should re-send rollback message
        self.wait_until_true(lambda: len(self._get_pending_snap_update(rank=0)) == 0, timeout=30)
        self.assertEqual(self._get_last_created_snap(rank=0), last_created)

        self.mount_a.resume_netns()
        try:
            proc.wait()
        except CommandFailedError:
            pass
        self.mount_a.remount()
        self.fs.flush()

    def test_snapclient_cache(self):
        """
        check if snapclient cache gets synced properly
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(3)
        status = self.fs.wait_for_daemons()

        self.mount_a.run_shell(["mkdir", "-p", "d0/d1/dir"])
        self.mount_a.run_shell(["mkdir", "-p", "d0/d2/dir"])
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d0/d1", "ceph.dir.pin", "1")
        self.mount_a.setfattr("d0/d2", "ceph.dir.pin", "2")
        self._wait_subtrees([("/d0", 0), ("/d0/d1", 1), ("/d0/d2", 2)], rank="all", status=status, path="/d0")

        def _check_snapclient_cache(snaps_dump, cache_dump=None, rank=0):
            if cache_dump is None:
                cache_dump = self._get_snapclient_dump(rank=rank)
            for key, value in cache_dump.items():
                if value != snaps_dump[key]:
                    return False
            return True;

        # sync after mksnap
        last_created = self._get_last_created_snap(rank=0)
        self.mount_a.run_shell(["mkdir", "d0/d1/dir/.snap/s1", "d0/d1/dir/.snap/s2"])
        self.wait_until_true(lambda: len(self._get_pending_snap_update(rank=0)) == 0, timeout=30)
        self.assertGreater(self._get_last_created_snap(rank=0), last_created)

        snaps_dump = self._get_snapserver_dump(rank=0)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=0));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=1));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=2));

        # sync after rmsnap
        last_destroyed = self._get_last_destroyed_snap(rank=0)
        self.mount_a.run_shell(["rmdir", "d0/d1/dir/.snap/s1"])
        self.wait_until_true(lambda: len(self._get_pending_snap_destroy(rank=0)) == 0, timeout=30)
        self.assertGreater(self._get_last_destroyed_snap(rank=0), last_destroyed)

        snaps_dump = self._get_snapserver_dump(rank=0)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=0));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=1));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=2));

        # sync during mds recovers
        self.fs.rank_fail(rank=2)
        status = self.fs.wait_for_daemons(timeout=MDS_RESTART_GRACE)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=2));

        self.fs.rank_fail(rank=0)
        self.fs.rank_fail(rank=1)
        status = self.fs.wait_for_daemons()
        self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=0));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=1));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank=2));

        # kill at MDSTableClient::handle_notify_prep
        status = self.fs.status()
        rank2 = self.fs.get_rank(rank=2, status=status)
        self.fs.rank_freeze(True, rank=2)
        self.fs.rank_asok(['config', 'set', "mds_kill_mdstable_at", "9"], rank=2, status=status)
        proc = self.mount_a.run_shell(["mkdir", "d0/d1/dir/.snap/s3"], wait=False)
        self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=2), timeout=self.fs.beacon_timeout);
        self.delete_mds_coredump(rank2['name']);

        # mksnap should wait for notify ack from mds.2
        self.assertFalse(proc.finished);

        # mksnap should proceed after mds.2 fails
        self.fs.rank_fail(rank=2)
        self.wait_until_true(lambda: proc.finished, timeout=30);

        self.fs.mds_restart(rank2['name'])
        self.wait_for_daemon_start([rank2['name']])
        status = self.fs.wait_for_daemons(timeout=MDS_RESTART_GRACE)

        self.mount_a.run_shell(["rmdir", Raw("d0/d1/dir/.snap/*")])

        # kill at MDSTableClient::commit
        # the recovering mds should sync all mds' cache when it enters resolve stage
        self.set_conf("mds", "mds_reconnect_timeout", "5")
        for i in range(1, 4):
            status = self.fs.status()
            rank2 = self.fs.get_rank(rank=2, status=status)
            self.fs.rank_freeze(True, rank=2)
            self.fs.rank_asok(['config', 'set', "mds_kill_mdstable_at", "4"], rank=2, status=status)
            last_created = self._get_last_created_snap(rank=0)
            proc = self.mount_a.run_shell(["mkdir", "d0/d2/dir/.snap/s{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=2), timeout=self.fs.beacon_timeout);
            self.delete_mds_coredump(rank2['name']);

            self.mount_a.suspend_netns()

            self.assertEqual(len(self._get_pending_snap_update(rank=0)), 1)

            if i in [2,4]:
                self.fs.rank_fail(rank=0)
            if i in [3,4]:
                self.fs.rank_fail(rank=1)

            self.fs.rank_fail(rank=2)
            self.fs.mds_restart(rank2['name'])
            self.wait_for_daemon_start([rank2['name']])
            status = self.fs.wait_for_daemons(timeout=MDS_RESTART_GRACE)

            rank0_cache = self._get_snapclient_dump(rank=0)
            rank1_cache = self._get_snapclient_dump(rank=1)
            rank2_cache = self._get_snapclient_dump(rank=2)

            self.assertGreater(int(rank0_cache["last_created"]), last_created)
            self.assertEqual(rank0_cache, rank1_cache);
            self.assertEqual(rank0_cache, rank2_cache);

            self.wait_until_true(lambda: len(self._get_pending_snap_update(rank=0)) == 0, timeout=30)

            snaps_dump = self._get_snapserver_dump(rank=0)
            self.assertEqual(snaps_dump["last_created"], rank0_cache["last_created"])
            self.assertTrue(_check_snapclient_cache(snaps_dump, cache_dump=rank0_cache));

            self.mount_a.resume_netns()
            try:
                proc.wait()
            except CommandFailedError:
                pass
            self.mount_a.remount()
            self.fs.flush()

        self.mount_a.run_shell(["rmdir", Raw("d0/d2/dir/.snap/*")])

    def test_snapshot_check_access(self):
        """
        """

        self.mount_a.run_shell_payload("mkdir -p dir1/dir2")
        self.mount_a.umount_wait(require_clean=True)

        newid = 'foo'
        keyring = self.fs.authorize(newid, ('/dir1', 'rws'))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=newid, client_keyring_path=keyring_path, cephfs_mntpt='/dir1')

        self.mount_a.run_shell_payload("pushd dir2; dd if=/dev/urandom of=file bs=4k count=1;")
        self.mount_a.run_shell_payload("mkdir .snap/one")
        self.mount_a.run_shell_payload("rm -rf dir2")
        # ???
        # Session check_access path ~mds0/stray3/10000000001/file
        # 2024-07-04T02:05:07.884+0000 7f319ce86640 20 Session check_access: [inode 0x10000000002 [2,2] ~mds0/stray2/10000000001/file ...] caller_uid=1141 caller_gid=1141 caller_gid_list=[1000,1141]
        # 2024-07-04T02:05:07.884+0000 7f319ce86640 20 Session check_access path ~mds0/stray2/10000000001/file
        # should be
        # 2024-07-04T02:11:26.990+0000 7f6b14e71640 20 Session check_access: [inode 0x10000000002 [2,2] ~mds0/stray2/10000000001/file ...] caller_uid=1141 caller_gid=1141 caller_gid_list=[1000,1141]
        # 2024-07-04T02:11:26.990+0000 7f6b14e71640 20 Session check_access stray_prior_path /dir1/dir2
        # 2024-07-04T02:11:26.990+0000 7f6b14e71640 10 MDSAuthCap is_capable inode(path /dir1/dir2 owner 1141:1141 mode 0100644) by caller 1141:1141 mask 1 new 0:0 cap: MDSAuthCaps[allow rws fsname=cephfs path="/dir1"]
        self.mount_a.run_shell_payload("stat .snap/one/dir2/file")


    def test_snap_rstat_purged_stray_old_inode(self):
        """
        check that the rstat propagation of a stray dir whose cowed old inode
        has been purged does not crash the MDS when mds_snap_rstat is enabled
        """
        self.config_set('mds', 'mds_snap_rstat', 'true')
        self.fs.set_allow_new_snaps(True);
        status = self.fs.status()
        gid = self.fs.get_rank(rank=0, status=status)['gid']

        for i in range(5):
            d = f"d{i}"
            self.mount_a.run_shell(["mkdir", d])
            # bump the snap seq, so that the stray dirs get an old inode cowed
            self.mount_a.run_shell(["mkdir", f"{d}/.snap/s1"])
            self.mount_a.run_shell(["rmdir", f"{d}/.snap/s1"])
            self.mount_a.run_shell_payload(f"for j in $(seq 10); do echo data > {d}/f$j; done; sync")
            self.mount_a.run_shell_payload(f"rm -f {d}/f*; sync")
            self.fs.rank_asok(["flush", "journal"], rank=0, status=status)
            self.assertEqual(self.fs.get_rank(rank=0)['gid'], gid)

        self.mount_a.run_shell(["rm", "-rf"] + [f"d{i}" for i in range(5)])

    def test_multimds_mksnap(self):
        """
        check if snapshot takes effect across authority subtrees
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self.mount_a.run_shell(["mkdir", "-p", "d0/d1/empty"])
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d0/d1", "ceph.dir.pin", "1")
        self._wait_subtrees([("/d0", 0), ("/d0/d1", 1)], rank="all", status=status, path="/d0")

        self.mount_a.write_test_pattern("d0/d1/file_a", 8 * 1024 * 1024)
        self.mount_a.run_shell(["mkdir", "d0/.snap/s1"])
        self.mount_a.run_shell(["rm", "-f", "d0/d1/file_a"])
        self.mount_a.validate_test_pattern("d0/.snap/s1/d1/file_a", 8 * 1024 * 1024, timeout=20)

        self.mount_a.run_shell(["rmdir", "d0/.snap/s1"])
        self.mount_a.run_shell(["rm", "-rf", "d0"])

    def _test_cap_flush_before_snap_update(self, snap_rank, nsnaps):
        """
        Take snaps on a dir whose inode rank <snap_rank> is auth for (so that
        rank handles the mksnap), and right after each snap grow files in a
        subdir pinned to rank 1, with messages between MDSs delayed on rank 1.
        Check that no rank aborted, and from a fresh mount that every file in
        every snap has the size it had when the snap was taken.
        """
        nfiles = 32
        chunk = 4096
        max_mds = max(2, snap_rank + 1)

        # The delay queue is set up when a connection is established, so
        # ms_inject_delay_type has to be in place before rank 1 talks to the
        # other ranks. Messages are only delayed once the probability is
        # raised.
        for mds_id in self.mds_cluster.mds_ids:
            self.fs.mds_asok(['config', 'set', 'ms_inject_delay_type', 'mds'],
                             mds_id=mds_id)
            self.fs.mds_asok(['config', 'set', 'ms_inject_delay_max', '6'],
                             mds_id=mds_id)

        self.fs.set_allow_new_snaps(True)
        self.fs.set_max_mds(max_mds)
        status = self.fs.wait_for_daemons()

        # The inode of the snapped dir d0/d1 lives in d0's dirfrag, so pinning
        # d0 makes <snap_rank> its auth. An empty directory is never
        # exported, so create the files first.
        snapdir = "d0/d1"
        self.mount_a.run_shell(["mkdir", "-p", f"{snapdir}/d2"])
        self.mount_a.run_shell_payload(f"touch {snapdir}/d2/f{{0..{nfiles - 1}}}")

        # The first snap on the dir splits a new realm off the root realm. A
        # rank sends the split only for the inodes it has caps for, so the
        # client moves an inode into the new realm only once the rank holding
        # its caps has handled the snap update. Until then its writes still
        # go into the snap (snapshots are asynchronous), and the sizes checked
        # below would not hold. Take this snap before anything is pinned,
        # while rank 0 holds all the caps: the client then has the split
        # before the mkdir returns, and the realm moves with the inodes when
        # they are exported.
        self.mount_a.run_shell(["mkdir", f"{snapdir}/.snap/s0"])

        self.mount_a.setfattr("d0", "ceph.dir.pin", str(snap_rank))
        self.mount_a.setfattr(f"{snapdir}/d2", "ceph.dir.pin", "1")
        self._wait_subtrees([("/d0", snap_rank), (f"/{snapdir}/d2", 1)],
                            rank="all", status=status, path="/d0")

        gids = {r: self.fs.get_rank(rank=r, status=status)['gid']
                for r in range(max_mds)}
        rank1 = self.fs.get_rank(rank=1, status=status)

        # Keep the files open throughout: the writes and fsyncs after a snap
        # then need nothing from another rank. Anything that did (an open,
        # say) would be queued behind the snap update on rank 1, and rank 1
        # would learn about the snap before the cap flush.
        #
        # Each file is s*chunk bytes long when snap s<s> is taken. After the
        # writes, sleep past ms_inject_delay_max so the delayed messages to
        # rank 1 drain before the next round.
        self.fs.mds_asok(['config', 'set', 'ms_inject_delay_probability', '1'],
                         mds_id=rank1['name'])
        try:
            self.mount_a.run_python(dedent(f"""
                import os, time
                root = "{self.mount_a.mountpoint}"
                fds = [os.open(os.path.join(root, "{snapdir}/d2/f%d" % i), os.O_RDWR)
                       for i in range({nfiles})]
                for s in range(1, {nsnaps} + 1):
                    size = s * {chunk}
                    for fd in fds:
                        os.pwrite(fd, b"A" * size, 0)
                        os.fsync(fd)
                    t0 = time.time()
                    os.mkdir(os.path.join(root, "{snapdir}/.snap/s%d" % s))
                    t1 = time.time()
                    for fd in fds:
                        os.pwrite(fd, b"B" * ({chunk} // 2), size)
                        os.fsync(fd)
                    print("s%d: mksnap %.1fs, flushes %.1fs" %
                          (s, t1 - t0, time.time() - t1), flush=True)
                    time.sleep(8)
                for fd in fds:
                    os.close(fd)
                """), timeout=900)
        finally:
            self.fs.mds_asok(['config', 'set', 'ms_inject_delay_probability', '0'],
                             mds_id=rank1['name'])

        # no rank may have aborted on a newly corrupt dentry
        status = self.fs.status()
        for r, gid in gids.items():
            self.assertEqual(self.fs.get_rank(rank=r, status=status)['gid'], gid,
                             f"rank {r} was replaced")

        # check the snaps from a fresh client, not from cached snap inodes
        expected = {f"{snapdir}/.snap/s{s}/d2/f{i}": s * chunk
                    for s in range(1, nsnaps + 1) for i in range(nfiles)}
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()
        out = self.mount_a.run_python(dedent(f"""
            import json, os
            root = "{self.mount_a.mountpoint}"
            sizes = {{}}
            for p in {sorted(expected)!r}:
                try:
                    sizes[p] = os.stat(os.path.join(root, p)).st_size
                except FileNotFoundError:
                    sizes[p] = -1
            print(json.dumps(sizes))
            """))
        sizes = json.loads(out)
        bad = {p: sz for p, sz in sizes.items() if sz != expected[p]}
        self.assertEqual(bad, {}, "snapshot files missing (-1) or with wrong size")

        for s in range(0, nsnaps + 1):
            self.mount_a.run_shell(["rmdir", f"{snapdir}/.snap/s{s}"])
        self.mount_a.run_shell(["rm", "-rf", "d0"])

    def test_multimds_mksnap_cap_flush_before_snap_update(self):
        """
        check that a cap flush following a new snap is COWed into the snap on
        a rank that has not yet handled the snap update for it

        The rank taking the snap notifies the client and sends the snap update
        to the other ranks at the same time. Delay messages between MDSs on
        rank 1 so that the client flushes caps (follows=<new snap>) to rank 1
        before rank 1 learns about the snap. Rank 0 takes the snaps here.
        """
        self._test_cap_flush_before_snap_update(snap_rank=0, nsnaps=5)

    def test_multimds_mksnap_cap_flush_before_snap_update_non_tableserver(self):
        """
        same as test_multimds_mksnap_cap_flush_before_snap_update, with the
        snaps taken by a rank other than the snap tableserver (rank 0)

        Rank 1 holds the cap flush and queries the snap table on rank 0. When
        rank 2 takes the snap, the query reply (from rank 0) can reach rank 1
        before the snap update (from rank 2) does, and already shows the snap
        as committed. Rank 1 must keep holding the flush until it has handled
        the snap update. Both messages are delayed by a random amount, so this
        happens in about half of the rounds; take more snaps to hit it.
        """
        self._test_cap_flush_before_snap_update(snap_rank=2, nsnaps=8)

    def test_multimds_past_parents(self):
        """
        check if past parents are properly recorded during across authority rename
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self.mount_a.run_shell_payload("mkdir -p {d0,d1}/empty")
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
        self._wait_subtrees([("/d0", 0), ("/d1", 1)], rank=0, status=status)

        self.mount_a.run_shell(["mkdir", "d0/d3"])
        self.mount_a.run_shell(["mkdir", "d0/.snap/s1"])
        snap_name = self.mount_a.run_shell(["ls", "d0/d3/.snap"]).stdout.getvalue()

        self.mount_a.run_shell(["mv", "d0/d3", "d1/d3"])
        snap_name1 = self.mount_a.run_shell(["ls", "d1/d3/.snap"]).stdout.getvalue()
        self.assertEqual(snap_name1, snap_name);

        self.mount_a.run_shell(["rmdir", "d0/.snap/s1"])
        snap_name1 = self.mount_a.run_shell(["ls", "d1/d3/.snap"]).stdout.getvalue()
        self.assertEqual(snap_name1, "");

        self.mount_a.run_shell(["rm", "-rf", "d0", "d1"])

    def test_multimds_hardlink(self):
        """
        check if hardlink snapshot works in multimds setup
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self.mount_a.run_shell_payload("mkdir -p {d0,d1}/empty")

        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
        self._wait_subtrees([("/d0", 0), ("/d1", 1)], rank=0, status=status)

        self.mount_a.run_python(dedent("""
            import os
            open(os.path.join("{path}", "d0/file1"), 'w').write("asdf")
            open(os.path.join("{path}", "d0/file2"), 'w').write("asdf")
            """.format(path=self.mount_a.mountpoint)
        ))

        self.mount_a.run_shell(["ln", "d0/file1", "d1/file1"])
        self.mount_a.run_shell(["ln", "d0/file2", "d1/file2"])

        self.mount_a.run_shell(["mkdir", "d1/.snap/s1"])

        self.mount_a.run_python(dedent("""
            import os
            open(os.path.join("{path}", "d0/file1"), 'w').write("qwer")
            """.format(path=self.mount_a.mountpoint)
        ))

        self.mount_a.run_shell(["grep", "asdf", "d1/.snap/s1/file1"])

        self.mount_a.run_shell(["rm", "-f", "d0/file2"])
        self.mount_a.run_shell(["grep", "asdf", "d1/.snap/s1/file2"])

        self.mount_a.run_shell(["rm", "-f", "d1/file2"])
        self.mount_a.run_shell(["grep", "asdf", "d1/.snap/s1/file2"])

        self.mount_a.run_shell(["rmdir", "d1/.snap/s1"])
        self.mount_a.run_shell(["rm", "-rf", "d0", "d1"])

    class SnapLimitViolationException(Exception):
        failed_snapshot_number = -1

        def __init__(self, num):
            self.failed_snapshot_number = num

    def get_snap_name(self, dir_name, sno):
            sname = "{dir_name}/.snap/s_{sno}".format(dir_name=dir_name, sno=sno)
            return sname

    def create_snap_dir(self, sname):
        self.mount_a.run_shell(["mkdir", sname])

    def delete_dir_and_snaps(self, dir_name, snaps):
        for sno in range(1, snaps+1, 1):
            sname = self.get_snap_name(dir_name, sno)
            self.mount_a.run_shell(["rmdir", sname])
        self.mount_a.run_shell(["rmdir", dir_name])

    def create_dir_and_snaps(self, dir_name, snaps):
        self.mount_a.run_shell(["mkdir", dir_name])

        for sno in range(1, snaps+1, 1):
            sname = self.get_snap_name(dir_name, sno)
            try:
                self.create_snap_dir(sname)
            except CommandFailedError as e:
                # failing at the last mkdir beyond the limit is expected
                if sno == snaps:
                    log.info("failed while creating snap #{}: {}".format(sno, repr(e)))
                    raise TestSnapshots.SnapLimitViolationException(sno)

    def test_mds_max_snaps_per_dir_default_limit(self):
        """
        Test the newly introudced option named mds_max_snaps_per_dir
        Default snaps limit is 100
        Test if the default number of snapshot directories can be created
        """
        self.create_dir_and_snaps("accounts", int(self.mds_max_snaps_per_dir))
        self.delete_dir_and_snaps("accounts", int(self.mds_max_snaps_per_dir))

    def test_mds_max_snaps_per_dir_with_increased_limit(self):
        """
        Test the newly introudced option named mds_max_snaps_per_dir
        First create 101 directories and ensure that the 101st directory
        creation fails. Then increase the default by one and see if the
        additional directory creation succeeds
        """
        # first test the default limit
        new_limit = int(self.mds_max_snaps_per_dir)
        self.fs.rank_asok(['config', 'set', 'mds_max_snaps_per_dir', repr(new_limit)])
        try:
            self.create_dir_and_snaps("accounts", new_limit + 1)
        except TestSnapshots.SnapLimitViolationException as e:
            if e.failed_snapshot_number == (new_limit + 1):
                pass
        # then increase the limit by one and test
        new_limit = new_limit + 1
        self.fs.rank_asok(['config', 'set', 'mds_max_snaps_per_dir', repr(new_limit)])
        sname = self.get_snap_name("accounts", new_limit)
        self.create_snap_dir(sname)
        self.delete_dir_and_snaps("accounts", new_limit)

    def test_mds_max_snaps_per_dir_with_reduced_limit(self):
        """
        Test the newly introudced option named mds_max_snaps_per_dir
        First create 99 directories. Then reduce the limit to 98. Then try
        creating another directory and ensure that additional directory
        creation fails.
        """
        # first test the new limit
        new_limit = int(self.mds_max_snaps_per_dir) - 1
        self.create_dir_and_snaps("accounts", new_limit)
        sname = self.get_snap_name("accounts", new_limit + 1)
        # then reduce the limit by one and test
        new_limit = new_limit - 1
        self.fs.rank_asok(['config', 'set', 'mds_max_snaps_per_dir', repr(new_limit)])
        try:
            self.create_snap_dir(sname)
        except CommandFailedError:
            # after reducing limit we expect the new snapshot creation to fail
            pass
        self.delete_dir_and_snaps("accounts", new_limit + 1)


class TestSnapUpdateSessionOpenRace(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2

    def _client_session_state(self, mount, rank):
        sessions = mount.admin_socket(['mds_sessions'])['sessions']
        for s in sessions:
            if s['mds'] == rank:
                return s['state']
        return None

    def _mds_session_state(self, client_id, rank, status=None):
        sessions = self.fs.rank_asok(['session', 'ls'], rank=rank, status=status)
        for s in sessions:
            if s['id'] == client_id:
                return s['state']
        return None

    def test_snap_update_before_session_open(self):
        """
        Check that the client handles a snap trace pushed on a session that
        is open on the MDS but which the client has not seen open yet.
        See https://tracker.ceph.com/issues/81585
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Requires the userspace client")

        self.fs.set_allow_new_snaps(True)
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        # an empty directory is not exported, so give d1 an entry
        self.mount_b.run_shell(["mkdir", "-p", "d0", "d1/d2"])
        self.mount_b.setfattr("d1", "ceph.dir.pin", "1")
        self._wait_subtrees([("/d1", 1)], rank=1, path="/d1")

        # mount_a was mounted while max_mds was 1 and has not touched d1
        # since, so it has no session with rank 1 yet. (remounting with the
        # delay set is no good: the mount's first request goes to a random
        # rank and stalls if that is rank 1.)
        client_id = self.mount_a.get_global_id()
        self.assertIsNone(self._client_session_state(self.mount_a, 1))

        # rank 1 opens client sessions but sits on the open reply
        delay = 60
        self.fs.rank_asok(['config', 'set', 'mds_inject_session_open_reply_delay',
                           str(delay)], rank=1, status=status)

        # make mount_a open a session with rank 1
        proc = self.mount_a.run_shell(["ls", "d1"], wait=False)
        self.wait_until_true(
            lambda: self._mds_session_state(client_id, 1, status=status) == "open",
            timeout=delay // 2)
        self.assertEqual(self._client_session_state(self.mount_a, 1), "opening")

        # a new snapshot makes rank 1 push the global snaprealm update to
        # every session it has open, including mount_a's. rank 1 does so
        # right after its snapclient learns of the commit.
        def last_created(rank):
            return int(self.fs.rank_asok(["dump", "snaps"], rank=rank,
                                         status=status)["last_created"])
        snapid = last_created(1)
        self.mount_b.run_shell(["mkdir", "d0/.snap/s0"])
        self.wait_until_true(lambda: last_created(1) > snapid, timeout=30)
        self.assertEqual(self._client_session_state(self.mount_a, 1), "opening")

        proc.wait()
        self.assertEqual(self._client_session_state(self.mount_a, 1), "open")
        self.assertIn("s0", self.mount_a.ls("d0/.snap"))

        self.fs.rank_asok(['config', 'set', 'mds_inject_session_open_reply_delay',
                           '0'], rank=1, status=status)
        self.mount_b.run_shell(["rmdir", "d0/.snap/s0"])


class TestMonSnapsAndFsPools(CephFSTestCase):
    MDSS_REQUIRED = 3

    def test_disallow_monitor_managed_snaps_for_fs_pools(self):
        """
        Test that creation of monitor managed snaps fails for pools attached
        to any file-system
        """
        with self.assertRaises(CommandFailedError):
            self.fs.rados(["mksnap", "snap1"], pool=self.fs.get_data_pool_name())

        with self.assertRaises(CommandFailedError):
            self.fs.rados(["mksnap", "snap2"], pool=self.fs.get_metadata_pool_name())

        with self.assertRaises(CommandFailedError):
            test_pool_name = self.fs.get_data_pool_name()
            base_cmd = f'osd pool mksnap {test_pool_name} snap3'
            self.run_ceph_cmd(base_cmd)

        with self.assertRaises(CommandFailedError):
            test_pool_name = self.fs.get_metadata_pool_name()
            base_cmd = f'osd pool mksnap {test_pool_name} snap4'
            self.run_ceph_cmd(base_cmd)

    def test_attaching_pools_with_snaps_to_fs_fails(self):
        """
        Test that attempt to attach pool with snapshots to an fs fails
        """
        test_pool_name = 'snap-test-pool'
        base_cmd = f'osd pool create {test_pool_name}'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)
        self.assertEqual(ret, 0)

        self.fs.rados(["mksnap", "snap3"], pool=test_pool_name)

        base_cmd = f'fs add_data_pool {self.fs.name} {test_pool_name}'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)
        self.assertEqual(ret, errno.EOPNOTSUPP)

        # cleanup
        self.fs.rados(["rmsnap", "snap3"], pool=test_pool_name)
        base_cmd = f'osd pool delete {test_pool_name}'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)

    def test_using_pool_with_snap_fails_fs_creation(self):
        """
        Test that using a pool with snaps for fs creation fails
        """
        base_cmd = 'osd pool create test_data_pool'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)
        self.assertEqual(ret, 0)
        base_cmd = 'osd pool create test_metadata_pool'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)
        self.assertEqual(ret, 0)

        self.fs.rados(["mksnap", "snap4"], pool='test_data_pool')

        base_cmd = 'fs new testfs test_metadata_pool test_data_pool'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)
        self.assertEqual(ret, errno.EOPNOTSUPP)

        # cleanup
        self.fs.rados(["rmsnap", "snap4"], pool='test_data_pool')
        base_cmd = 'osd pool delete test_data_pool'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)
        base_cmd = 'osd pool delete test_metadata_pool'
        ret = self.get_ceph_cmd_result(args=base_cmd, check_status=False)
