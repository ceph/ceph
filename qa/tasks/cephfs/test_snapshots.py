import errno
import logging
import signal
from textwrap import dedent
from tasks.cephfs.cephfs_test_case import CephFSTestCase
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
