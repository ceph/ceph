
import logging
import json

from tasks.mgr.mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestFailover(MgrTestCase):
    MGRS_REQUIRED = 2

    def setUp(self):
        self.setup_mgrs()

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

    def test_timeout_nostandby(self):
        """
        That when an active mgr stop responding, and no standby is
        available, the active mgr is removed from the map anyway.
        """
        # Query which mgr is active
        original_active = self.mgr_cluster.get_active_id()
        original_standbys = self.mgr_cluster.get_standby_ids()

        for s in original_standbys:
            self.mgr_cluster.mgr_stop(s)
            self.mgr_cluster.mgr_fail(s)

        self.assertListEqual(self.mgr_cluster.get_standby_ids(), [])
        self.assertEqual(self.mgr_cluster.get_active_id(), original_active)

        grace = int(self.mgr_cluster.get_config("mon_mgr_beacon_grace"))
        log.info("Should time out in about {0} seconds".format(grace))

        self.mgr_cluster.mgr_stop(original_active)

        # Now wait for the mon to notice the mgr is gone and remove it
        # from the map.
        self.wait_until_equal(
            lambda: self.mgr_cluster.get_active_id(),
            "",
            timeout=grace * 2
        )

        self.assertListEqual(self.mgr_cluster.get_standby_ids(), [])
        self.assertEqual(self.mgr_cluster.get_active_id(), "")

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

        # Both daemons should have fully populated metadata
        # (regression test for http://tracker.ceph.com/issues/21260)
        meta = json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "metadata"))
        id_to_meta = dict([(i['id'], i) for i in meta])
        for i in [original_active] + original_standbys:
            self.assertIn(i, id_to_meta)
            self.assertIn('ceph_version', id_to_meta[i])

        # We should be able to fail back over again: the exercises
        # our re-initialization of the python runtime within
        # a single process lifetime.

        # Get rid of any bystander standbys so that the original_active
        # will be selected as next active.
        new_active = self.mgr_cluster.get_active_id()
        for daemon in original_standbys:
            if daemon != new_active:
                self.mgr_cluster.mgr_stop(daemon)
                self.mgr_cluster.mgr_fail(daemon)

        self.assertListEqual(self.mgr_cluster.get_standby_ids(),
                             [original_active])

        self.mgr_cluster.mgr_stop(new_active)
        self.mgr_cluster.mgr_fail(new_active)

        self.assertEqual(self.mgr_cluster.get_active_id(), original_active)
        self.assertEqual(self.mgr_cluster.get_standby_ids(), [])

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
