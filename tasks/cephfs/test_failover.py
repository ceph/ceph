
from unittest import case
from cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError


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

        (original_active, ) = self.fs.get_active_names()
        original_standbys = self.fs.get_daemon_names("up:standby")

        # Kill the rank 0 daemon's physical process
        self.fs.mds_stop(original_active)

        grace = int(self.fs.get_config("mds_beacon_grace", service_type="mon"))

        # Wait until the monitor promotes his replacement
        def promoted():
            active = self.fs.get_active_names()
            return active and active[0] in original_standbys

        self.wait_until_true(
            promoted,
            timeout=grace*2)

        # Start the original rank 0 daemon up again, see that he becomes a standby
        self.fs.mds_restart(original_active)
        self.wait_until_true(
            lambda: original_active in self.fs.get_daemon_names("up:standby"),
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
            mdsmap = self.fs.mon_manager.get_mds_status_all()
            for info in mdsmap['info'].values():
                if "laggy_since" not in info:
                    return False

            return True

        self.wait_until_true(laggy, grace * 2)
        with self.assertRaises(CommandFailedError):
            self.mounts[0].mount()
