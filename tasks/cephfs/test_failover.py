import json
import logging
from unittest import case
from cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


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

        grace = int(self.fs.get_config("mds_beacon_grace", service_type="mon"))

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

        require_active = self.fs.get_config("fuse_require_active_mds", service_type="mon").lower() == "true"
        if not require_active:
            raise case.SkipTest("fuse_require_active_mds is not set")

        grace = int(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        # Check it's not laggy to begin with
        (original_active, ) = self.fs.get_active_names()
        self.assertNotIn("laggy_since", self.fs.mon_manager.get_mds_status(original_active))

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


class TestStandbyReplay(CephFSTestCase):
    MDSS_REQUIRED = 2
    REQUIRE_FILESYSTEM = False

    def test_standby_failure(self):
        """
        That the failure of a standby-replay daemon happens cleanly
        and doesn't interrupt anything else.
        """
        # Pick out exactly 2 daemons to be run during test
        use_daemons = sorted(self.mds_cluster.mds_ids[0:2])
        mds_a, mds_b = use_daemons
        log.info("Using MDS daemons: {0}".format(use_daemons))

        def set_standby_for(leader, follower, replay):
            self.set_conf("mds.{0}".format(follower), "mds_standby_for_name", leader)
            if replay:
                self.set_conf("mds.{0}".format(follower), "mds_standby_replay", "true")

        # Configure two pairs of MDSs that are standby for each other
        set_standby_for(mds_a, mds_b, True)
        set_standby_for(mds_b, mds_a, False)

        # Create FS alpha and get mds_a to come up as active
        fs_a = self.mds_cluster.get_filesystem("alpha")
        fs_a.create()
        self.mds_cluster.mds_restart(mds_a)
        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_a])

        # Start the standbys
        self.mds_cluster.mds_restart(mds_b)
        self.wait_for_daemon_start([mds_b])

        def get_info_by_name(fs, mds_name):
            mds_map = fs.get_mds_map()
            for gid_str, info in mds_map['info'].items():
                if info['name'] == mds_name:
                    return info

            log.warn(json.dumps(mds_map, indent=2))
            raise RuntimeError("MDS '{0}' not found in filesystem MDSMap".format(mds_name))

        # See the standby come up as the correct rank
        info_b = get_info_by_name(fs_a, mds_b)
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

class TestMultiFilesystems(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 4

    # We'll create our own filesystems and start our own daemons
    REQUIRE_FILESYSTEM = False

    def setUp(self):
        super(TestMultiFilesystems, self).setUp()
        self.fs.mon_manager.raw_cluster_cmd("fs", "flag", "set",
                                            "enable_multiple", "true")

    def _setup_two(self):
        fs_a = self.mds_cluster.get_filesystem("alpha")
        fs_b = self.mds_cluster.get_filesystem("bravo")
        fs_a.create()
        fs_b.create()

        self.mds_cluster.mds_restart()

        # Wait for both filesystems to go healthy
        fs_a.wait_for_daemons()
        fs_b.wait_for_daemons()

        # Reconfigure client auth caps
        for mount in self.mounts:
            self.fs.mon_manager.raw_cluster_cmd_result(
                'auth', 'caps', "client.{0}".format(mount.client_id),
                'mds', 'allow',
                'mon', 'allow r',
                'osd', 'allow rw pool={0}, allow rw pool={1}'.format(
                    fs_a.get_data_pool_name(), fs_b.get_data_pool_name()))

        return fs_a, fs_b

    def test_clients(self):
        fs_a, fs_b = self._setup_two()

        # Mount a client on fs_a
        fs_a.set_ceph_conf(
            "client.{0}".format(self.mount_a.client_id), "client_mds_namespace",
            fs_a.get_namespace_id().__str__()
        )
        self.mount_a.mount()
        self.mount_a.write_n_mb("pad.bin", 1)
        self.mount_a.write_n_mb("test.bin", 2)
        a_created_ino = self.mount_a.path_to_ino("test.bin")
        self.mount_a.create_files()
        self.mount_a.umount_wait()

        # Mount a client on fs_b
        fs_b.set_ceph_conf(
            "client.{0}".format(self.mount_b.client_id), "client_mds_namespace",
            fs_b.get_namespace_id().__str__()
        )
        self.mount_b.mount()
        self.mount_b.write_n_mb("test.bin", 1)
        b_created_ino = self.mount_b.path_to_ino("test.bin")
        self.mount_b.create_files()
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
        fs_b.mon_manager.raw_cluster_cmd('fs', 'set', fs_b.name, 'max_mds', "2")
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Increase max_mds on fs_a, see a standby take up the role
        fs_a.mon_manager.raw_cluster_cmd('fs', 'set', fs_a.name, 'max_mds', "2")
        self.wait_until_equal(lambda: len(fs_a.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Shrink fs_b back to 1, see a daemon go back to standby
        fs_b.mon_manager.raw_cluster_cmd('fs', 'set', fs_b.name, 'max_mds', "1")
        fs_b.mon_manager.raw_cluster_cmd('mds', 'deactivate', "{0}:1".format(fs_b.name))
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 1, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Grow fs_a up to 3, see the former fs_b daemon join it.
        fs_a.mon_manager.raw_cluster_cmd('fs', 'set', fs_a.name, 'max_mds', "3")
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
        fs_a = self.mds_cluster.get_filesystem("alpha")
        fs_a.create()
        self.mds_cluster.mds_restart(mds_a)
        fs_a.wait_for_daemons()
        self.assertEqual(fs_a.get_active_names(), [mds_a])

        # Create FS bravo and get mds_c to come up as active
        fs_b = self.mds_cluster.get_filesystem("bravo")
        fs_b.create()
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

        # See that they take up standby replay roles
        # mds_a should be in filesystem alpha following mds_b
        info_a = get_info_by_name(fs_a, mds_a)
        self.assertEqual(info_a['state'], "up:standby-replay")
        self.assertEqual(info_a['standby_for_name'], mds_b)
        self.assertEqual(info_a['rank'], 0)
        # mds_c should be in filesystem alpha following mds_d
        info_c = get_info_by_name(fs_b, mds_c)
        self.assertEqual(info_c['state'], "up:standby-replay")
        self.assertEqual(info_c['standby_for_name'], mds_d)
        self.assertEqual(info_c['rank'], 0)

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

        fs_a = self.mds_cluster.get_filesystem("alpha")
        fs_a.create()
        fs_b = self.mds_cluster.get_filesystem("bravo")
        fs_b.create()
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
        fs_a = self.mds_cluster.get_filesystem("alpha")
        fs_a.create()
        fs_b = self.mds_cluster.get_filesystem("bravo")
        fs_b.create()
        fs_a.mon_manager.raw_cluster_cmd('fs', 'set', fs_a.name,
                                         'max_mds', "2")
        fs_b.mon_manager.raw_cluster_cmd('fs', 'set', fs_b.name,
                                         'max_mds', "2")

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
