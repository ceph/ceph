import logging
import time
from textwrap import dedent
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.orchestra.run import CommandFailedError, Raw
from unittest import SkipTest

log = logging.getLogger(__name__)

MDS_RESTART_GRACE = 60

class TestSnapshots(CephFSTestCase):
    MDSS_REQUIRED = 3

    def _check_subtree(self, status, rank, path):
        got_subtrees = self.fs.mds_asok(["get", "subtrees"], mds_id=status.get_rank(self.fs.id, rank)['name'])
        for s in got_subtrees:
            if s['dir']['path'] == path and s['auth_first'] == rank:
                return True
        return False

    def _stop_standby_mds(self):
        for i in self.fs.get_standby_daemons():
            self.fs.mds_stop(i)
            self.fs.mds_fail(i)
        self.wait_until_equal(lambda: len(self.fs.get_standby_daemons()), expect_val=0, timeout=30)

    def _get_snapclient_dump(self, rank_id):
        return self.fs.mds_asok(["dump", "snaps"], rank_id)

    def _get_snapserver_dump(self, rank0_id):
        return self.fs.mds_asok(["dump", "snaps", "--server"], rank0_id)

    def _get_last_created_snap(self, rank0_id):
        return int(self._get_snapserver_dump(rank0_id)["last_created"])

    def _get_last_destroyed_snap(self, rank0_id):
        return int(self._get_snapserver_dump(rank0_id)["last_destroyed"])

    def _get_pending_snap_update(self, rank0_id):
        return self._get_snapserver_dump(rank0_id)["pending_update"]

    def _get_pending_snap_destroy(self, rank0_id):
        return self._get_snapserver_dump(rank0_id)["pending_destroy"]

    def test_kill_mdstable(self):
        """
        check snaptable transcation
        """
        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client to forcibly kill mount")

        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        self._stop_standby_mds()

        status = self.fs.status()
        rank0_id=status.get_rank(self.fs.id, 0)['name']
        rank1_id=status.get_rank(self.fs.id, 1)['name']
        self.set_conf("mds.{0}".format(rank0_id), "mds standby for rank", 0)
        self.set_conf("mds.{0}".format(rank1_id), "mds standby for rank", 1)

        # setup subtrees
        self.mount_a.run_shell(["mkdir", "-p", "d1/dir"])
        self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
        self.wait_until_true(lambda: self._check_subtree(status, 1, '/d1'), timeout=30)

        last_created = self._get_last_created_snap(rank0_id)

        # mds_kill_mdstable_at:
        #  1: MDSTableServer::handle_prepare
        #  2: MDSTableServer::_prepare_logged
        #  5: MDSTableServer::handle_commit
        #  6: MDSTableServer::_commit_logged
        for i in [1,2,5,6]:
            log.info("testing snapserver mds_kill_mdstable_at={0}".format(i))
            self.fs.mds_asok(['config', 'set', "mds_kill_mdstable_at", "{0}".format(i)], rank0_id)
            proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s1{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.status().get_mds(rank0_id), timeout=grace*2);
            self.delete_mds_coredump(rank0_id);

            self.mds_cluster.mds_fail_restart(rank0_id)
            self.wait_for_daemon_start([rank0_id])
            self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)

            proc.wait()
            last_created += 1
            self.wait_until_true(lambda: self._get_last_created_snap(rank0_id) == last_created, timeout=30)

        self.set_conf("mds", "mds_reconnect_timeout", "5")

        self.mount_a.run_shell(["rmdir", Raw("d1/dir/.snap/*")])

        # set mds_kill_mdstable_at, also kill snapclient
        for i in [2,5,6]:
            log.info("testing snapserver mds_kill_mdstable_at={0}, also kill snapclient".format(i))
            last_created = self._get_last_created_snap(rank0_id)

            self.fs.mds_asok(['config', 'set', "mds_kill_mdstable_at", "{0}".format(i)], rank0_id)
            proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s2{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.status().get_mds(rank0_id), timeout=grace*2);
            self.delete_mds_coredump(rank0_id);

            self.mds_cluster.mds_stop(rank1_id)
            self.mds_cluster.mds_fail(rank1_id);

            self.mount_a.kill()
            self.mount_a.kill_cleanup()

            self.mds_cluster.mds_fail_restart(rank0_id)
            self.wait_for_daemon_start([rank0_id])

            self.fs.wait_for_state('up:resolve', rank=0, timeout=MDS_RESTART_GRACE)
            if i in [2,5]:
                self.assertEqual(len(self._get_pending_snap_update(rank0_id)), 1)
            elif i == 6:
                self.assertEqual(len(self._get_pending_snap_update(rank0_id)), 0)
                self.assertGreater(self._get_last_created_snap(rank0_id), last_created)

            self.mds_cluster.mds_restart(rank1_id)
            self.wait_for_daemon_start([rank1_id])
            self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)

            if i in [2,5]:
                self.wait_until_true(lambda: len(self._get_pending_snap_update(rank0_id)) == 0, timeout=30)
                if i == 2:
                    self.assertEqual(self._get_last_created_snap(rank0_id), last_created)
                else:
                    self.assertGreater(self._get_last_created_snap(rank0_id), last_created)

            self.mount_a.mount()
            self.mount_a.wait_until_mounted()

        self.mount_a.run_shell(["rmdir", Raw("d1/dir/.snap/*")])

        # mds_kill_mdstable_at:
        #  3: MDSTableClient::handle_request (got agree)
        #  4: MDSTableClient::commit
        #  7: MDSTableClient::handle_request (got ack)
        for i in [3,4,7]:
            log.info("testing snapclient mds_kill_mdstable_at={0}".format(i))
            last_created = self._get_last_created_snap(rank0_id)

            self.fs.mds_asok(['config', 'set', "mds_kill_mdstable_at", "{0}".format(i)], rank1_id)
            proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s3{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.status().get_mds(rank1_id), timeout=grace*2);
            self.delete_mds_coredump(rank1_id);

            self.mount_a.kill()
            self.mount_a.kill_cleanup()

            if i in [3,4]:
                self.assertEqual(len(self._get_pending_snap_update(rank0_id)), 1)
            elif i == 7:
                self.assertEqual(len(self._get_pending_snap_update(rank0_id)), 0)
                self.assertGreater(self._get_last_created_snap(rank0_id), last_created)

            self.mds_cluster.mds_fail_restart(rank1_id)
            self.wait_for_daemon_start([rank1_id])
            self.fs.wait_for_state('up:active', rank=1, timeout=MDS_RESTART_GRACE)

            if i in [3,4]:
                self.wait_until_true(lambda: len(self._get_pending_snap_update(rank0_id)) == 0, timeout=30)
                if i == 3:
                    self.assertEqual(self._get_last_created_snap(rank0_id), last_created)
                else:
                    self.assertGreater(self._get_last_created_snap(rank0_id), last_created)

            self.mount_a.mount()
            self.mount_a.wait_until_mounted()

        self.mount_a.run_shell(["rmdir", Raw("d1/dir/.snap/*")])

        # mds_kill_mdstable_at:
        #  3: MDSTableClient::handle_request (got agree)
        #  8: MDSTableServer::handle_rollback
        log.info("testing snapclient mds_kill_mdstable_at=3, snapserver mds_kill_mdstable_at=8")
        last_created = self._get_last_created_snap(rank0_id)

        self.fs.mds_asok(['config', 'set', "mds_kill_mdstable_at", "8".format(i)], rank0_id)
        self.fs.mds_asok(['config', 'set', "mds_kill_mdstable_at", "3".format(i)], rank1_id)
        proc = self.mount_a.run_shell(["mkdir", "d1/dir/.snap/s4".format(i)], wait=False)
        self.wait_until_true(lambda: "laggy_since" in self.fs.status().get_mds(rank1_id), timeout=grace*2);
        self.delete_mds_coredump(rank1_id);

        self.mount_a.kill()
        self.mount_a.kill_cleanup()

        self.assertEqual(len(self._get_pending_snap_update(rank0_id)), 1)

        self.mds_cluster.mds_fail_restart(rank1_id)

        # rollback triggers assertion
        self.wait_until_true(lambda: "laggy_since" in self.fs.status().get_mds(rank0_id), timeout=grace*2);
        self.delete_mds_coredump(rank0_id);

        self.mds_cluster.mds_fail_restart(rank0_id)
        self.wait_for_daemon_start([rank0_id])
        self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)
        # mds.1 should re-send rollack message
        self.wait_until_true(lambda: len(self._get_pending_snap_update(rank0_id)) == 0, timeout=30)
        self.assertEqual(self._get_last_created_snap(rank0_id), last_created)

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

    def test_snapclient_cache(self):
        """
        check if snapclient cache gets synced properly
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        self._stop_standby_mds()

        status = self.fs.status()
        rank0_id=status.get_rank(self.fs.id, 0)['name']
        rank1_id=status.get_rank(self.fs.id, 1)['name']
        rank2_id=status.get_rank(self.fs.id, 2)['name']
        self.set_conf("mds.{0}".format(rank0_id), "mds standby for rank", 0)
        self.set_conf("mds.{0}".format(rank1_id), "mds standby for rank", 1)
        self.set_conf("mds.{0}".format(rank2_id), "mds standby for rank", 2)

        self.mount_a.run_shell(["mkdir", "-p", "d0/d1/dir"])
        self.mount_a.run_shell(["mkdir", "-p", "d0/d2/dir"])
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d0/d1", "ceph.dir.pin", "1")
        self.mount_a.setfattr("d0/d2", "ceph.dir.pin", "2")
        self.wait_until_true(lambda: self._check_subtree(status, 2, '/d0/d2'), timeout=30)
        self.wait_until_true(lambda: self._check_subtree(status, 1, '/d0/d1'), timeout=5)
        self.wait_until_true(lambda: self._check_subtree(status, 0, '/d0'), timeout=5)

        def _check_snapclient_cache(snaps_dump, cache_dump=None, rank_id=-1):
            if cache_dump is None:
                cache_dump = self._get_snapclient_dump(rank_id)
            for key, value in cache_dump.iteritems():
                if value != snaps_dump[key]:
                    return False
            return True;

        # sync after mksnap
        last_created = self._get_last_created_snap(rank0_id)
        self.mount_a.run_shell(["mkdir", Raw("d0/d1/dir/.snap/{s1,s2}")])
        self.wait_until_true(lambda: len(self._get_pending_snap_update(rank0_id)) == 0, timeout=30)
        self.assertGreater(self._get_last_created_snap(rank0_id), last_created)

        snaps_dump = self._get_snapserver_dump(rank0_id)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank0_id));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank1_id));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank2_id));

        # sync after rmsnap
        last_destroyed = self._get_last_destroyed_snap(rank0_id)
        self.mount_a.run_shell(["rmdir", "d0/d1/dir/.snap/s1"])
        self.wait_until_true(lambda: len(self._get_pending_snap_destroy(rank0_id)) == 0, timeout=30)
        self.assertGreater(self._get_last_destroyed_snap(rank0_id), last_destroyed)

        snaps_dump = self._get_snapserver_dump(rank0_id)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank0_id));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank1_id));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank2_id));

        # sync during mds recovers
        self.mds_cluster.mds_stop(rank2_id)
        self.mds_cluster.mds_fail_restart(rank2_id)
        self.wait_for_daemon_start([rank2_id])
        self.fs.wait_for_state('up:active', rank=2, timeout=MDS_RESTART_GRACE)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank2_id));

        self.mds_cluster.mds_stop(rank0_id)
        self.mds_cluster.mds_stop(rank1_id)
        self.mds_cluster.mds_fail_restart(rank0_id)
        self.mds_cluster.mds_fail_restart(rank1_id)
        self.wait_for_daemon_start([rank0_id, rank1_id])
        self.fs.wait_for_state('up:active', rank=0, timeout=MDS_RESTART_GRACE)
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank0_id));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank1_id));
        self.assertTrue(_check_snapclient_cache(snaps_dump, rank_id=rank2_id));

        # kill at MDSTableClient::handle_notify_prep
        self.fs.mds_asok(['config', 'set', "mds_kill_mdstable_at", "9"], rank2_id)
        proc = self.mount_a.run_shell(["mkdir", "d0/d1/dir/.snap/s3"], wait=False)
        self.wait_until_true(lambda: "laggy_since" in self.fs.status().get_mds(rank2_id), timeout=grace*2);
        self.delete_mds_coredump(rank2_id);

        # mksnap should wait for notify ack from mds.2
        self.assertFalse(proc.finished);

        # mksnap should proceed after mds.2 fails
        self.mds_cluster.mds_fail(rank2_id)
        self.wait_until_true(lambda: proc.finished, timeout=30);

        self.mds_cluster.mds_fail_restart(rank2_id)
        self.wait_for_daemon_start([rank2_id])
        self.fs.wait_for_state('up:active', rank=2, timeout=MDS_RESTART_GRACE)

        self.mount_a.run_shell(["rmdir", Raw("d0/d1/dir/.snap/*")])

        # kill at MDSTableClient::commit
        # the recovering mds should sync all mds' cache when it enters resolve stage
        self.set_conf("mds", "mds_reconnect_timeout", "5")
        for i in range(1, 4):
            self.fs.mds_asok(['config', 'set', "mds_kill_mdstable_at", "4"], rank2_id)
            last_created = self._get_last_created_snap(rank0_id)
            proc = self.mount_a.run_shell(["mkdir", "d0/d2/dir/.snap/s{0}".format(i)], wait=False)
            self.wait_until_true(lambda: "laggy_since" in self.fs.status().get_mds(rank2_id), timeout=grace*2);
            self.delete_mds_coredump(rank2_id);

            self.mount_a.kill()
            self.mount_a.kill_cleanup()

            self.assertEqual(len(self._get_pending_snap_update(rank0_id)), 1)

            if i in [2,4]:
                self.mds_cluster.mds_stop(rank0_id)
                self.mds_cluster.mds_fail_restart(rank0_id)
            if i in [3,4]:
                self.mds_cluster.mds_stop(rank1_id)
                self.mds_cluster.mds_fail_restart(rank1_id)

            self.mds_cluster.mds_fail_restart(rank2_id)
            self.wait_for_daemon_start([rank0_id, rank1_id, rank2_id])

            self.fs.wait_for_state('up:active', rank=2, timeout=MDS_RESTART_GRACE)
            rank0_cache = self._get_snapclient_dump(rank0_id)
            rank1_cache = self._get_snapclient_dump(rank1_id)
            rank2_cache = self._get_snapclient_dump(rank2_id)

            self.assertGreater(int(rank0_cache["last_created"]), last_created)
            self.assertEqual(rank0_cache, rank1_cache);
            self.assertEqual(rank0_cache, rank2_cache);

            self.wait_until_true(lambda: len(self._get_pending_snap_update(rank0_id)) == 0, timeout=30)

            snaps_dump = self._get_snapserver_dump(rank0_id)
            self.assertEqual(snaps_dump["last_created"], rank0_cache["last_created"])
            self.assertTrue(_check_snapclient_cache(snaps_dump, cache_dump=rank0_cache));

            self.mount_a.mount()
            self.mount_a.wait_until_mounted()

        self.mount_a.run_shell(["rmdir", Raw("d0/d2/dir/.snap/*")])

    def test_multimds_mksnap(self):
        """
        check if snapshot takes effect across authority subtrees
        """
        self.fs.set_allow_new_snaps(True);
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()

        status = self.fs.status()

        self.mount_a.run_shell(["mkdir", "-p", "d0/d1"])
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d0/d1", "ceph.dir.pin", "1")
        self.wait_until_true(lambda: self._check_subtree(status, 1, '/d0/d1'), timeout=30)
        self.wait_until_true(lambda: self._check_subtree(status, 0, '/d0'), timeout=5)

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
        self.fs.wait_for_daemons()

        status = self.fs.status()

        self.mount_a.run_shell(["mkdir", "d0", "d1"])
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
        self.wait_until_true(lambda: self._check_subtree(status, 1, '/d1'), timeout=30)
        self.wait_until_true(lambda: self._check_subtree(status, 0, '/d0'), timeout=5)

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
        self.fs.wait_for_daemons()

        status = self.fs.status()

        self.mount_a.run_shell(["mkdir", "d0", "d1"])

        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
        self.wait_until_true(lambda: self._check_subtree(status, 1, '/d1'), timeout=30)
        self.wait_until_true(lambda: self._check_subtree(status, 0, '/d0'), timeout=5)

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
