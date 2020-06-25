import logging
import signal
from textwrap import dedent
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.orchestra.run import CommandFailedError, Raw

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

    def test_kill_mdstable(self):
        """
        check snaptable transcation
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client to forcibly kill mount")

        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

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
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=0), timeout=grace*2);
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
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=0), timeout=grace*2);
            self.delete_mds_coredump(rank0['name']);

            self.fs.rank_signal(signal.SIGKILL, rank=1)

            self.mount_a.kill()
            self.mount_a.kill_cleanup()

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

            self.mount_a.mount_wait()

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
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=1), timeout=grace*2);
            self.delete_mds_coredump(rank1['name']);

            self.mount_a.kill()
            self.mount_a.kill_cleanup()

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

            self.mount_a.mount_wait()

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
        self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=1), timeout=grace*2);
        self.delete_mds_coredump(rank1['name']);

        self.mount_a.kill()
        self.mount_a.kill_cleanup()

        self.assertEqual(len(self._get_pending_snap_update(rank=0)), 1)

        self.fs.rank_fail(rank=1)
        self.fs.mds_restart(rank1['name'])
        self.wait_for_daemon_start([rank1['name']])

        # rollback triggers assertion
        self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=0), timeout=grace*2);
        self.delete_mds_coredump(rank0['name']);
        self.fs.rank_fail(rank=0)
        self.fs.mds_restart(rank0['name'])
        self.wait_for_daemon_start([rank0['name']])
        self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)

        # mds.1 should re-send rollback message
        self.wait_until_true(lambda: len(self._get_pending_snap_update(rank=0)) == 0, timeout=30)
        self.assertEqual(self._get_last_created_snap(rank=0), last_created)

        self.mount_a.mount_wait()

    def test_snapclient_cache(self):
        """
        check if snapclient cache gets synced properly
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(3)
        status = self.fs.wait_for_daemons()

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

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
        self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=2), timeout=grace*2);
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
            self.wait_until_true(lambda: "laggy_since" in self.fs.get_rank(rank=2), timeout=grace*2);
            self.delete_mds_coredump(rank2['name']);

            self.mount_a.kill()
            self.mount_a.kill_cleanup()

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

            self.mount_a.mount_wait()

        self.mount_a.run_shell(["rmdir", Raw("d0/d2/dir/.snap/*")])

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
        self.mount_a.validate_test_pattern("d0/.snap/s1/d1/file_a", 8 * 1024 * 1024)

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
