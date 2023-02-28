
import logging
import json

from .mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestFailover(MgrTestCase):
    MGRS_REQUIRED = 2

    def setUp(self):
        super(TestFailover, self).setUp()
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
        id_to_meta = dict([(i['name'], i) for i in meta])
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

class TestLibCephSQLiteFailover(MgrTestCase):
    MGRS_REQUIRED = 1

    def setUp(self):
        super(TestLibCephSQLiteFailover, self).setUp()
        self.setup_mgrs()

    def get_libcephsqlite(self):
        mgr_map = self.mgr_cluster.get_mgr_map()
        addresses = self.mgr_cluster.get_registered_clients('libcephsqlite', mgr_map=mgr_map)
        self.assertEqual(len(addresses), 1)
        return addresses[0]

    def test_maybe_reonnect(self):
        """
        That the devicehealth module can recover after losing its libcephsqlite lock.
        """

        # make sure the database is populated and loaded by the module
        self.mgr_cluster.mon_manager.ceph("device scrape-health-metrics")

        oldaddr = self.get_libcephsqlite()
        self.mgr_cluster.mon_manager.ceph(f"osd blocklist add {oldaddr['addr']}/{oldaddr['nonce']}")

        def test():
            self.mgr_cluster.mon_manager.ceph("device scrape-health-metrics")
            newaddr = self.get_libcephsqlite()
            return oldaddr != newaddr

        self.wait_until_true(
            test,
            timeout=30
        )
