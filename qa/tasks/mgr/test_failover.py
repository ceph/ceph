
import logging

from tasks.mgr.mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestFailover(MgrTestCase):
    REQUIRE_MGRS = 2

    def test_timeout(self):
        """
        That when an active mgr stops responding, a standby is promoted
        after mon_mgr_beacon_grace.
        """

        # Query which mgr is active
        original_active = self.mgr_cluster.get_active_id()
        original_standbys = self.mgr_cluster.get_standby_ids()

        # Stop that daemon
        self.mgr_cluster.mgr_stop(original_active)

        # Assert that the other mgr becomes active
        self.wait_until_true(
            lambda: self.mgr_cluster.get_active_id() in original_standbys,
            timeout=60
        )

        self.mgr_cluster.mgr_restart(original_active)
        self.wait_until_true(
            lambda: original_active in self.mgr_cluster.get_standby_ids(),
            timeout=10
        )

    def test_explicit_fail(self):
        """
        That when a user explicitly fails a daemon, a standby immediately
        replaces it.
        :return:
        """
        # Query which mgr is active
        original_active = self.mgr_cluster.get_active_id()
        original_standbys = self.mgr_cluster.get_standby_ids()

        self.mgr_cluster.mgr_fail(original_active)

        # A standby should take over
        self.wait_until_true(
            lambda: self.mgr_cluster.get_active_id() in original_standbys,
            timeout=60
        )

        # The one we failed should come back as a standby (he isn't
        # really dead)
        self.wait_until_true(
            lambda: original_active in self.mgr_cluster.get_standby_ids(),
            timeout=10
        )

    def test_standby_timeout(self):
        """
        That when a standby daemon stops sending beacons, it is
        removed from the list of standbys
        :return:
        """
        original_active = self.mgr_cluster.get_active_id()
        original_standbys = self.mgr_cluster.get_standby_ids()

        victim = original_standbys[0]
        self.mgr_cluster.mgr_stop(victim)

        expect_standbys = set(original_standbys) - {victim}

        self.wait_until_true(
            lambda: set(self.mgr_cluster.get_standby_ids()) == expect_standbys,
            timeout=60
        )
        self.assertEqual(self.mgr_cluster.get_active_id(), original_active)
