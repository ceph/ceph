import time
import signal
import json
import logging
from unittest import case, SkipTest

from cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology import misc as teuthology
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


class TestClusterResize(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    def grow(self, n):
        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        fscid = self.fs.id
        status = self.fs.status()
        log.info("status = {0}".format(status))

        original_ranks = set([info['gid'] for info in status.get_ranks(fscid)])
        original_standbys = set([info['gid'] for info in status.get_standbys()])

        oldmax = self.fs.get_var('max_mds')
        self.assertTrue(n > oldmax)
        self.fs.set_max_mds(n)

        log.info("Waiting for cluster to grow.")
        status = self.fs.wait_for_daemons(timeout=60+grace*2)
        ranks = set([info['gid'] for info in status.get_ranks(fscid)])
        self.assertTrue(original_ranks.issubset(ranks) and len(ranks) == n)
        return status

    def shrink(self, n):
        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        fscid = self.fs.id
        status = self.fs.status()
        log.info("status = {0}".format(status))

        original_ranks = set([info['gid'] for info in status.get_ranks(fscid)])
        original_standbys = set([info['gid'] for info in status.get_standbys()])

        oldmax = self.fs.get_var('max_mds')
        self.assertTrue(n < oldmax)
        self.fs.set_max_mds(n)

        # Wait until the monitor finishes stopping ranks >= n
        log.info("Waiting for cluster to shink.")
        status = self.fs.wait_for_daemons(timeout=60+grace*2)
        ranks = set([info['gid'] for info in status.get_ranks(fscid)])
        self.assertTrue(ranks.issubset(original_ranks) and len(ranks) == n)
        return status


    def test_grow(self):
        """
        That the MDS cluster grows after increasing max_mds.
        """

        # Need all my standbys up as well as the active daemons
        # self.wait_for_daemon_start() necessary?

        self.grow(2)
        self.grow(3)


    def test_shrink(self):
        """
        That the MDS cluster shrinks automatically after decreasing max_mds.
        """

        self.grow(3)
        self.shrink(1)

    def test_up_less_than_max(self):
        """
        That a health warning is generated when max_mds is greater than active count.
        """

        status = self.fs.status()
        mdss = [info['gid'] for info in status.get_all()]
        self.fs.set_max_mds(len(mdss)+1)
        self.wait_for_health("MDS_UP_LESS_THAN_MAX", 30)
        self.shrink(2)
        self.wait_for_health_clear(30)

    def test_down_health(self):
        """
        That marking a FS down does not generate a health warning
        """

        self.fs.set_down()
        try:
            self.wait_for_health("", 30)
            raise RuntimeError("got health warning?")
        except RuntimeError as e:
            if "Timed out after" in str(e):
                pass
            else:
                raise

    def test_down_twice(self):
        """
        That marking a FS down twice does not wipe old_max_mds.
        """

        self.grow(2)
        self.fs.set_down()
        self.fs.wait_for_daemons()
        self.fs.set_down(False)
        self.assertEqual(self.fs.get_var("max_mds"), 2)
        self.fs.wait_for_daemons(timeout=60)

    def test_down_grow(self):
        """
        That setting max_mds undoes down.
        """

        self.fs.set_down()
        self.fs.wait_for_daemons()
        self.grow(2)
        self.fs.wait_for_daemons()

    def test_down(self):
        """
        That down setting toggles and sets max_mds appropriately.
        """

        self.fs.set_down()
        self.fs.wait_for_daemons()
        self.assertEqual(self.fs.get_var("max_mds"), 0)
        self.fs.set_down(False)
        self.assertEqual(self.fs.get_var("max_mds"), 1)
        self.fs.wait_for_daemons()
        self.assertEqual(self.fs.get_var("max_mds"), 1)

    def test_hole(self):
        """
        Test that a hole cannot be created in the FS ranks.
        """

        fscid = self.fs.id

        self.grow(2)

        self.fs.set_max_mds(1)
        log.info("status = {0}".format(self.fs.status()))

        self.fs.set_max_mds(3)
        # Don't wait for rank 1 to stop

        self.fs.set_max_mds(2)
        # Prevent another MDS from taking rank 1
        # XXX This is a little racy because rank 1 may have stopped and a
        #     standby assigned to rank 1 before joinable=0 is set.
        self.fs.set_joinable(False) # XXX keep in mind changing max_mds clears this flag

        try:
            status = self.fs.wait_for_daemons(timeout=90)
            raise RuntimeError("should not be able to successfully shrink cluster!")
        except:
            # could not shrink to max_mds=2 and reach 2 actives (because joinable=False)
            status = self.fs.status()
            ranks = set([info['rank'] for info in status.get_ranks(fscid)])
            self.assertTrue(ranks == set([0]))
        finally:
            log.info("status = {0}".format(status))

    def test_thrash(self):
        """
        Test that thrashing max_mds does not fail.
        """

        max_mds = 2
        for i in range(0, 100):
            self.fs.set_max_mds(max_mds)
            max_mds = (max_mds+1)%3+1

        self.fs.wait_for_daemons(timeout=90)

class TestFailover(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    def test_simple(self):
        """
        That when the active MDS is killed, a standby MDS is promoted into
        its rank after the grace period.

        This is just a simple unit test, the harder cases are covered
        in thrashing tests.
        """

        # Need all my standbys up as well as the active daemons
        self.wait_for_daemon_start()

        (original_active, ) = self.fs.get_active_names()
        original_standbys = self.mds_cluster.get_standby_daemons()

        # Kill the rank 0 daemon's physical process
        self.fs.mds_stop(original_active)

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        # Wait until the monitor promotes his replacement
        def promoted():
            active = self.fs.get_active_names()
            return active and active[0] in original_standbys

        log.info("Waiting for promotion of one of the original standbys {0}".format(
            original_standbys))
        self.wait_until_true(
            promoted,
            timeout=grace*2)

        # Start the original rank 0 daemon up again, see that he becomes a standby
        self.fs.mds_restart(original_active)
        self.wait_until_true(
            lambda: original_active in self.mds_cluster.get_standby_daemons(),
            timeout=60  # Approximately long enough for MDS to start and mon to notice
        )

    def test_client_abort(self):
        """
        That a client will respect fuse_require_active_mds and error out
        when the cluster appears to be unavailable.
        """

        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Requires FUSE client to inject client metadata")

        require_active = self.fs.get_config("fuse_require_active_mds", service_type="mon").lower() == "true"
        if not require_active:
            raise case.SkipTest("fuse_require_active_mds is not set")

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        # Check it's not laggy to begin with
        (original_active, ) = self.fs.get_active_names()
        self.assertNotIn("laggy_since", self.fs.status().get_mds(original_active))

        self.mounts[0].umount_wait()

        # Control: that we can mount and unmount usually, while the cluster is healthy
        self.mounts[0].mount()
        self.mounts[0].wait_until_mounted()
        self.mounts[0].umount_wait()

        # Stop the daemon processes
        self.fs.mds_stop()

        # Wait for everyone to go laggy
        def laggy():
            mdsmap = self.fs.get_mds_map()
            for info in mdsmap['info'].values():
                if "laggy_since" not in info:
                    return False

            return True

        self.wait_until_true(laggy, grace * 2)
        with self.assertRaises(CommandFailedError):
            self.mounts[0].mount()

    def test_standby_count_wanted(self):
        """
        That cluster health warnings are generated by insufficient standbys available.
        """

        # Need all my standbys up as well as the active daemons
        self.wait_for_daemon_start()

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        standbys = self.mds_cluster.get_standby_daemons()
        self.assertGreaterEqual(len(standbys), 1)
        self.fs.mon_manager.raw_cluster_cmd('fs', 'set', self.fs.name, 'standby_count_wanted', str(len(standbys)))

        # Kill a standby and check for warning
        victim = standbys.pop()
        self.fs.mds_stop(victim)
        log.info("waiting for insufficient standby daemon warning")
        self.wait_for_health("MDS_INSUFFICIENT_STANDBY", grace*2)

        # restart the standby, see that he becomes a standby, check health clears
        self.fs.mds_restart(victim)
        self.wait_until_true(
            lambda: victim in self.mds_cluster.get_standby_daemons(),
            timeout=60  # Approximately long enough for MDS to start and mon to notice
        )
        self.wait_for_health_clear(timeout=30)

        # Set it one greater than standbys ever seen
        standbys = self.mds_cluster.get_standby_daemons()
        self.assertGreaterEqual(len(standbys), 1)
        self.fs.mon_manager.raw_cluster_cmd('fs', 'set', self.fs.name, 'standby_count_wanted', str(len(standbys)+1))
        log.info("waiting for insufficient standby daemon warning")
        self.wait_for_health("MDS_INSUFFICIENT_STANDBY", grace*2)

        # Set it to 0
        self.fs.mon_manager.raw_cluster_cmd('fs', 'set', self.fs.name, 'standby_count_wanted', '0')
        self.wait_for_health_clear(timeout=30)

    def test_discontinuous_mdsmap(self):
        """
        That discontinuous mdsmap does not affect failover.
        See http://tracker.ceph.com/issues/24856.
        """
        mds_ids = sorted(self.mds_cluster.mds_ids)
        mds_a, mds_b = mds_ids[0:2]
        # Assign mds to fixed ranks. To prevent standby mds from replacing frozen mds
        rank = 0;
        for mds_id in mds_ids:
            self.set_conf("mds.{0}".format(mds_id), "mds_standby_for_rank", str(rank))
            rank += 1
        self.mds_cluster.mds_restart()
        self.fs.wait_for_daemons()

        self.fs.set_max_mds(2)
        self.fs.wait_for_state('up:active', rank=1)

        self.mount_a.umount_wait()

        grace = float(self.fs.get_config("mds_beacon_grace", service_type="mon"))
        monc_timeout = float(self.fs.get_config("mon_client_ping_timeout", service_type="mds"))

        # Freeze mds_a
        self.mds_cluster.mds_signal(mds_a, signal.SIGSTOP)
        self.wait_until_true(
            lambda: "laggy_since" in self.fs.status().get_mds(mds_a),
            timeout=grace * 2
        )

        self.mds_cluster.mds_restart(mds_b)
        self.fs.wait_for_state('up:resolve', rank=1, timeout=30)

        # Make sure of mds_a's monitor connection gets reset
        time.sleep(monc_timeout * 2)

        # Unfreeze mds_a, it will get discontinuous mdsmap
        self.mds_cluster.mds_signal(mds_a, signal.SIGCONT)
        self.wait_until_true(
            lambda: "laggy_since" not in self.fs.status().get_mds(mds_a),
            timeout=grace * 2
        )

        # mds.b will be stuck at 'reconnect' state if snapserver gets confused
        # by discontinuous mdsmap
        self.fs.wait_for_state('up:active', rank=1, timeout=30)

class TestStandbyReplay(CephFSTestCase):
    MDSS_REQUIRED = 4
    REQUIRE_FILESYSTEM = False

    def set_standby_for(self, leader, follower, replay):
        self.set_conf("mds.{0}".format(follower), "mds_standby_for_name", leader)
        if replay:
            self.set_conf("mds.{0}".format(follower), "mds_standby_replay", "true")

    def get_info_by_name(self, mds_name):
        status = self.mds_cluster.status()
        info = status.get_mds(mds_name)
        if info is None:
            log.warn(str(status))
            raise RuntimeError("MDS '{0}' not found".format(mds_name))
        else:
            return info

    def test_standby_replay_unused(self):
        # Pick out exactly 3 daemons to be run during test
        use_daemons = sorted(self.mds_cluster.mds_ids[0:3])
        mds_a, mds_b, mds_c = use_daemons
        log.info("Using MDS daemons: {0}".format(use_daemons))

        # B and C should both follow A, but only one will
        # really get into standby replay state.
        self.set_standby_for(mds_a, mds_b, True)
        self.set_standby_for(mds_a, mds_c, True)

        # Create FS and start A
        fs_a = self.mds_cluster.newfs("alpha")
        self.mds_cluster.mds_restart(mds_a)
        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_a])

        # Start B, he should go into standby replay
        self.mds_cluster.mds_restart(mds_b)
        self.wait_for_daemon_start([mds_b])
        info_b = self.get_info_by_name(mds_b)
        self.assertEqual(info_b['state'], "up:standby-replay")
        self.assertEqual(info_b['standby_for_name'], mds_a)
        self.assertEqual(info_b['rank'], 0)

        # Start C, he should go into standby (*not* replay)
        self.mds_cluster.mds_restart(mds_c)
        self.wait_for_daemon_start([mds_c])
        info_c = self.get_info_by_name(mds_c)
        self.assertEqual(info_c['state'], "up:standby")
        self.assertEqual(info_c['standby_for_name'], mds_a)
        self.assertEqual(info_c['rank'], -1)

        # Kill B, C should go into standby replay
        self.mds_cluster.mds_stop(mds_b)
        self.mds_cluster.mds_fail(mds_b)
        self.wait_until_equal(
                lambda: self.get_info_by_name(mds_c)['state'],
                "up:standby-replay",
                60)
        info_c = self.get_info_by_name(mds_c)
        self.assertEqual(info_c['state'], "up:standby-replay")
        self.assertEqual(info_c['standby_for_name'], mds_a)
        self.assertEqual(info_c['rank'], 0)

    def test_standby_failure(self):
        """
        That the failure of a standby-replay daemon happens cleanly
        and doesn't interrupt anything else.
        """
        # Pick out exactly 2 daemons to be run during test
        use_daemons = sorted(self.mds_cluster.mds_ids[0:2])
        mds_a, mds_b = use_daemons
        log.info("Using MDS daemons: {0}".format(use_daemons))

        # Configure two pairs of MDSs that are standby for each other
        self.set_standby_for(mds_a, mds_b, True)
        self.set_standby_for(mds_b, mds_a, False)

        # Create FS alpha and get mds_a to come up as active
        fs_a = self.mds_cluster.newfs("alpha")
        self.mds_cluster.mds_restart(mds_a)
        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_a])

        # Start the standbys
        self.mds_cluster.mds_restart(mds_b)
        self.wait_for_daemon_start([mds_b])

        # See the standby come up as the correct rank
        info_b = self.get_info_by_name(mds_b)
        self.assertEqual(info_b['state'], "up:standby-replay")
        self.assertEqual(info_b['standby_for_name'], mds_a)
        self.assertEqual(info_b['rank'], 0)

        # Kill the standby
        self.mds_cluster.mds_stop(mds_b)
        self.mds_cluster.mds_fail(mds_b)

        # See that the standby is gone and the active remains
        self.assertEqual(fs_a.get_active_names(), [mds_a])
        mds_map = fs_a.get_mds_map()
        self.assertEqual(len(mds_map['info']), 1)
        self.assertEqual(mds_map['failed'], [])
        self.assertEqual(mds_map['damaged'], [])
        self.assertEqual(mds_map['stopped'], [])

    def test_rank_stopped(self):
        """
        That when a rank is STOPPED, standby replays for
        that rank get torn down
        """
        # Pick out exactly 2 daemons to be run during test
        use_daemons = sorted(self.mds_cluster.mds_ids[0:4])
        mds_a, mds_b, mds_a_s, mds_b_s = use_daemons
        log.info("Using MDS daemons: {0}".format(use_daemons))

        # a and b both get a standby
        self.set_standby_for(mds_a, mds_a_s, True)
        self.set_standby_for(mds_b, mds_b_s, True)

        # Create FS alpha and get mds_a to come up as active
        fs_a = self.mds_cluster.newfs("alpha")
        fs_a.set_max_mds(2)

        self.mds_cluster.mds_restart(mds_a)
        self.wait_until_equal(lambda: fs_a.get_active_names(), [mds_a], 30)
        self.mds_cluster.mds_restart(mds_b)
        fs_a.wait_for_daemons()
        self.assertEqual(sorted(fs_a.get_active_names()), [mds_a, mds_b])

        # Start the standbys
        self.mds_cluster.mds_restart(mds_b_s)
        self.wait_for_daemon_start([mds_b_s])
        self.mds_cluster.mds_restart(mds_a_s)
        self.wait_for_daemon_start([mds_a_s])
        info_b_s = self.get_info_by_name(mds_b_s)
        self.assertEqual(info_b_s['state'], "up:standby-replay")
        info_a_s = self.get_info_by_name(mds_a_s)
        self.assertEqual(info_a_s['state'], "up:standby-replay")

        # Shrink the cluster
        fs_a.set_max_mds(1)
        self.wait_until_equal(
            lambda: fs_a.get_active_names(), [mds_a],
            60
        )

        # Both 'b' and 'b_s' should go back to being standbys
        self.wait_until_equal(
            lambda: self.mds_cluster.get_standby_daemons(), {mds_b, mds_b_s},
            60
        )


class TestMultiFilesystems(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 4

    # We'll create our own filesystems and start our own daemons
    REQUIRE_FILESYSTEM = False

    def setUp(self):
        super(TestMultiFilesystems, self).setUp()
        self.mds_cluster.mon_manager.raw_cluster_cmd("fs", "flag", "set",
            "enable_multiple", "true",
            "--yes-i-really-mean-it")

    def _setup_two(self):
        fs_a = self.mds_cluster.newfs("alpha")
        fs_b = self.mds_cluster.newfs("bravo")

        self.mds_cluster.mds_restart()

        # Wait for both filesystems to go healthy
        fs_a.wait_for_daemons()
        fs_b.wait_for_daemons()

        # Reconfigure client auth caps
        for mount in self.mounts:
            self.mds_cluster.mon_manager.raw_cluster_cmd_result(
                'auth', 'caps', "client.{0}".format(mount.client_id),
                'mds', 'allow',
                'mon', 'allow r',
                'osd', 'allow rw pool={0}, allow rw pool={1}'.format(
                    fs_a.get_data_pool_name(), fs_b.get_data_pool_name()))

        return fs_a, fs_b

    def test_clients(self):
        fs_a, fs_b = self._setup_two()

        # Mount a client on fs_a
        self.mount_a.mount(mount_fs_name=fs_a.name)
        self.mount_a.write_n_mb("pad.bin", 1)
        self.mount_a.write_n_mb("test.bin", 2)
        a_created_ino = self.mount_a.path_to_ino("test.bin")
        self.mount_a.create_files()

        # Mount a client on fs_b
        self.mount_b.mount(mount_fs_name=fs_b.name)
        self.mount_b.write_n_mb("test.bin", 1)
        b_created_ino = self.mount_b.path_to_ino("test.bin")
        self.mount_b.create_files()

        # Check that a non-default filesystem mount survives an MDS
        # failover (i.e. that map subscription is continuous, not
        # just the first time), reproduces #16022
        old_fs_b_mds = fs_b.get_active_names()[0]
        self.mds_cluster.mds_stop(old_fs_b_mds)
        self.mds_cluster.mds_fail(old_fs_b_mds)
        fs_b.wait_for_daemons()
        background = self.mount_b.write_background()
        # Raise exception if the write doesn't finish (i.e. if client
        # has not kept up with MDS failure)
        try:
            self.wait_until_true(lambda: background.finished, timeout=30)
        except RuntimeError:
            # The mount is stuck, we'll have to force it to fail cleanly
            background.stdin.close()
            self.mount_b.umount_wait(force=True)
            raise

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        # See that the client's files went into the correct pool
        self.assertTrue(fs_a.data_objects_present(a_created_ino, 1024 * 1024))
        self.assertTrue(fs_b.data_objects_present(b_created_ino, 1024 * 1024))

    def test_standby(self):
        fs_a, fs_b = self._setup_two()

        # Assert that the remaining two MDS daemons are now standbys
        a_daemons = fs_a.get_active_names()
        b_daemons = fs_b.get_active_names()
        self.assertEqual(len(a_daemons), 1)
        self.assertEqual(len(b_daemons), 1)
        original_a = a_daemons[0]
        original_b = b_daemons[0]
        expect_standby_daemons = set(self.mds_cluster.mds_ids) - (set(a_daemons) | set(b_daemons))

        # Need all my standbys up as well as the active daemons
        self.wait_for_daemon_start()
        self.assertEqual(expect_standby_daemons, self.mds_cluster.get_standby_daemons())

        # Kill fs_a's active MDS, see a standby take over
        self.mds_cluster.mds_stop(original_a)
        self.mds_cluster.mon_manager.raw_cluster_cmd("mds", "fail", original_a)
        self.wait_until_equal(lambda: len(fs_a.get_active_names()), 1, 30,
                              reject_fn=lambda v: v > 1)
        # Assert that it's a *different* daemon that has now appeared in the map for fs_a
        self.assertNotEqual(fs_a.get_active_names()[0], original_a)

        # Kill fs_b's active MDS, see a standby take over
        self.mds_cluster.mds_stop(original_b)
        self.mds_cluster.mon_manager.raw_cluster_cmd("mds", "fail", original_b)
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 1, 30,
                              reject_fn=lambda v: v > 1)
        # Assert that it's a *different* daemon that has now appeared in the map for fs_a
        self.assertNotEqual(fs_b.get_active_names()[0], original_b)

        # Both of the original active daemons should be gone, and all standbys used up
        self.assertEqual(self.mds_cluster.get_standby_daemons(), set())

        # Restart the ones I killed, see them reappear as standbys
        self.mds_cluster.mds_restart(original_a)
        self.mds_cluster.mds_restart(original_b)
        self.wait_until_true(
            lambda: {original_a, original_b} == self.mds_cluster.get_standby_daemons(),
            timeout=30
        )

    def test_grow_shrink(self):
        # Usual setup...
        fs_a, fs_b = self._setup_two()

        # Increase max_mds on fs_b, see a standby take up the role
        fs_b.set_max_mds(2)
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Increase max_mds on fs_a, see a standby take up the role
        fs_a.set_max_mds(2)
        self.wait_until_equal(lambda: len(fs_a.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Shrink fs_b back to 1, see a daemon go back to standby
        fs_b.set_max_mds(1)
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 1, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Grow fs_a up to 3, see the former fs_b daemon join it.
        fs_a.set_max_mds(3)
        self.wait_until_equal(lambda: len(fs_a.get_active_names()), 3, 60,
                              reject_fn=lambda v: v > 3 or v < 2)

    def test_standby_for_name(self):
        # Pick out exactly 4 daemons to be run during test
        use_daemons = sorted(self.mds_cluster.mds_ids[0:4])
        mds_a, mds_b, mds_c, mds_d = use_daemons
        log.info("Using MDS daemons: {0}".format(use_daemons))

        def set_standby_for(leader, follower, replay):
            self.set_conf("mds.{0}".format(follower), "mds_standby_for_name", leader)
            if replay:
                self.set_conf("mds.{0}".format(follower), "mds_standby_replay", "true")

        # Configure two pairs of MDSs that are standby for each other
        set_standby_for(mds_a, mds_b, True)
        set_standby_for(mds_b, mds_a, False)
        set_standby_for(mds_c, mds_d, True)
        set_standby_for(mds_d, mds_c, False)

        # Create FS alpha and get mds_a to come up as active
        fs_a = self.mds_cluster.newfs("alpha")
        self.mds_cluster.mds_restart(mds_a)
        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_a])

        # Create FS bravo and get mds_c to come up as active
        fs_b = self.mds_cluster.newfs("bravo")
        self.mds_cluster.mds_restart(mds_c)
        fs_b.wait_for_daemons()
        self.assertEqual(fs_b.get_active_names(), [mds_c])

        # Start the standbys
        self.mds_cluster.mds_restart(mds_b)
        self.mds_cluster.mds_restart(mds_d)
        self.wait_for_daemon_start([mds_b, mds_d])

        def get_info_by_name(fs, mds_name):
            mds_map = fs.get_mds_map()
            for gid_str, info in mds_map['info'].items():
                if info['name'] == mds_name:
                    return info

            log.warn(json.dumps(mds_map, indent=2))
            raise RuntimeError("MDS '{0}' not found in filesystem MDSMap".format(mds_name))

        # See both standbys come up as standby replay for the correct ranks
        # mds_b should be in filesystem alpha following mds_a
        info_b = get_info_by_name(fs_a, mds_b)
        self.assertEqual(info_b['state'], "up:standby-replay")
        self.assertEqual(info_b['standby_for_name'], mds_a)
        self.assertEqual(info_b['rank'], 0)
        # mds_d should be in filesystem alpha following mds_c
        info_d = get_info_by_name(fs_b, mds_d)
        self.assertEqual(info_d['state'], "up:standby-replay")
        self.assertEqual(info_d['standby_for_name'], mds_c)
        self.assertEqual(info_d['rank'], 0)

        # Kill both active daemons
        self.mds_cluster.mds_stop(mds_a)
        self.mds_cluster.mds_fail(mds_a)
        self.mds_cluster.mds_stop(mds_c)
        self.mds_cluster.mds_fail(mds_c)

        # Wait for standbys to take over
        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_b])
        fs_b.wait_for_daemons()
        self.assertEqual(fs_b.get_active_names(), [mds_d])

        # Start the original active daemons up again
        self.mds_cluster.mds_restart(mds_a)
        self.mds_cluster.mds_restart(mds_c)
        self.wait_for_daemon_start([mds_a, mds_c])

        self.assertEqual(set(self.mds_cluster.get_standby_daemons()),
                         {mds_a, mds_c})

    def test_standby_for_rank(self):
        use_daemons = sorted(self.mds_cluster.mds_ids[0:4])
        mds_a, mds_b, mds_c, mds_d = use_daemons
        log.info("Using MDS daemons: {0}".format(use_daemons))

        def set_standby_for(leader_rank, leader_fs, follower_id):
            self.set_conf("mds.{0}".format(follower_id),
                          "mds_standby_for_rank", leader_rank)

            fscid = leader_fs.get_namespace_id()
            self.set_conf("mds.{0}".format(follower_id),
                          "mds_standby_for_fscid", fscid)

        fs_a = self.mds_cluster.newfs("alpha")
        fs_b = self.mds_cluster.newfs("bravo")
        set_standby_for(0, fs_a, mds_a)
        set_standby_for(0, fs_a, mds_b)
        set_standby_for(0, fs_b, mds_c)
        set_standby_for(0, fs_b, mds_d)

        self.mds_cluster.mds_restart(mds_a)
        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_a])

        self.mds_cluster.mds_restart(mds_c)
        fs_b.wait_for_daemons()
        self.assertEqual(fs_b.get_active_names(), [mds_c])

        self.mds_cluster.mds_restart(mds_b)
        self.mds_cluster.mds_restart(mds_d)
        self.wait_for_daemon_start([mds_b, mds_d])

        self.mds_cluster.mds_stop(mds_a)
        self.mds_cluster.mds_fail(mds_a)
        self.mds_cluster.mds_stop(mds_c)
        self.mds_cluster.mds_fail(mds_c)

        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_b])
        fs_b.wait_for_daemons()
        self.assertEqual(fs_b.get_active_names(), [mds_d])

    def test_standby_for_fscid(self):
        """
        That I can set a standby FSCID with no rank, and the result is
        that daemons join any rank for that filesystem.
        """
        use_daemons = sorted(self.mds_cluster.mds_ids[0:4])
        mds_a, mds_b, mds_c, mds_d = use_daemons

        log.info("Using MDS daemons: {0}".format(use_daemons))

        def set_standby_for(leader_fs, follower_id):
            fscid = leader_fs.get_namespace_id()
            self.set_conf("mds.{0}".format(follower_id),
                          "mds_standby_for_fscid", fscid)

        # Create two filesystems which should have two ranks each
        fs_a = self.mds_cluster.newfs("alpha")

        fs_b = self.mds_cluster.newfs("bravo")

        fs_a.set_max_mds(2)
        fs_b.set_max_mds(2)

        # Set all the daemons to have a FSCID assignment but no other
        # standby preferences.
        set_standby_for(fs_a, mds_a)
        set_standby_for(fs_a, mds_b)
        set_standby_for(fs_b, mds_c)
        set_standby_for(fs_b, mds_d)

        # Now when we start all daemons at once, they should fall into
        # ranks in the right filesystem
        self.mds_cluster.mds_restart(mds_a)
        self.mds_cluster.mds_restart(mds_b)
        self.mds_cluster.mds_restart(mds_c)
        self.mds_cluster.mds_restart(mds_d)
        self.wait_for_daemon_start([mds_a, mds_b, mds_c, mds_d])
        fs_a.wait_for_daemons()
        fs_b.wait_for_daemons()
        self.assertEqual(set(fs_a.get_active_names()), {mds_a, mds_b})
        self.assertEqual(set(fs_b.get_active_names()), {mds_c, mds_d})

    def test_standby_for_invalid_fscid(self):
        """
        That an invalid standby_fscid does not cause a mon crash
        """
        use_daemons = sorted(self.mds_cluster.mds_ids[0:3])
        mds_a, mds_b, mds_c = use_daemons
        log.info("Using MDS daemons: {0}".format(use_daemons))

        def set_standby_for_rank(leader_rank, follower_id):
            self.set_conf("mds.{0}".format(follower_id),
                          "mds_standby_for_rank", leader_rank)

        # Create one fs
        fs_a = self.mds_cluster.newfs("cephfs")

        # Get configured mons in the cluster, so we can see if any
        # crashed later.
        configured_mons = fs_a.mon_manager.get_mon_quorum()

        # Set all the daemons to have a rank assignment but no other
        # standby preferences.
        set_standby_for_rank(0, mds_a)
        set_standby_for_rank(0, mds_b)

        # Set third daemon to have invalid fscid assignment and no other
        # standby preferences
        invalid_fscid = 123
        self.set_conf("mds.{0}".format(mds_c), "mds_standby_for_fscid", invalid_fscid)

        #Restart all the daemons to make the standby preference applied
        self.mds_cluster.mds_restart(mds_a)
        self.mds_cluster.mds_restart(mds_b)
        self.mds_cluster.mds_restart(mds_c)
        self.wait_for_daemon_start([mds_a, mds_b, mds_c])

        #Stop active mds daemon service of fs
        if (fs_a.get_active_names(), [mds_a]):
            self.mds_cluster.mds_stop(mds_a)
            self.mds_cluster.mds_fail(mds_a)
            fs_a.wait_for_daemons()
        else:
            self.mds_cluster.mds_stop(mds_b)
            self.mds_cluster.mds_fail(mds_b)
            fs_a.wait_for_daemons()

        #Get active mons from cluster
        active_mons = fs_a.mon_manager.get_mon_quorum()

        #Check for active quorum mon status and configured mon status
        self.assertEqual(active_mons, configured_mons,
                "Not all mons are in quorum Invalid standby invalid fscid test failed!")
