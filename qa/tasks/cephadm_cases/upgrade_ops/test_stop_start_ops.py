"""
Integration tests for the cephadm 'orch upgrade' operations:
start, stop and restart

"""

import logging

from .orch_upgrade_test_case import UpgradeOpsTestCase

log = logging.getLogger(__name__)


class TestUpgradeStopRestart(UpgradeOpsTestCase):

    POLL_MONITOR_UPGRADE_TIMEOUT_SEC = 600
    POLL_NO_VERSION_CHANGE_SEC = 180
    POLL_NO_VERSION_CHANGE_INTERVAL_SEC = 15
    POLL_NUMBER_OF_MON_UPGRADED = 1

    # ------------------------------------------------------------------
    # tests
    # ------------------------------------------------------------------
    def test_upgrade_stop_without_start(self) -> None:
        """
        Test that stopping the upgrade when no upgrade is in progress is
        a successful no-op: it reports 'No upgrade in progress' and exits
        with rc 0 rather than failing.
        """
        self.assertTrue(
            self.no_upgrade_in_progress(),
            'an upgrade is already in progress before the test started',
        )
        log.info('attempting to stop upgrade with no upgrade in progress')
        res = self.orch('upgrade', 'stop', check=False)
        self.assertEqual(
            res.rc, 0,
            'stop failed when no upgrade is in progress',
        )
        self.assertIn(
            'No upgrade in progress', res.out,
            f'stop did not report that no upgrade is in progress: {res!r}',
        )

    def test_upgrade_stop_restart(self) -> None:
        """
        Test the stop and restart (start) functionality of the cephadm upgrade process.
        """
        self.start_upgrade()

        # wait until the mon daemon group is partially upgraded: with
        # three mons, one should be on the new version while two are
        # still on the old one
        log.info('Waiting for at least 1/3 mon daemons to be upgraded')
        self.poll(
            f'{self.POLL_NUMBER_OF_MON_UPGRADED}/3 mon daemons upgraded',
            lambda: self.upgraded_daemon_count(
                self.daemon_versions('mon'), self.source_version
            ) >= self.POLL_NUMBER_OF_MON_UPGRADED,
            self.POLL_MONITOR_UPGRADE_TIMEOUT_SEC,
        )

        # stop the upgrade
        log.info('Stopping the upgrade')
        res = self.orch('upgrade', 'stop', check=False)
        self.assertEqual(res.rc, 0, 'failed to stop upgrade')

        log.info('Upgrade stopped, verifying no upgrade in progress')
        self.poll(
            'upgrade stopped',
            self.no_upgrade_in_progress,
        )

        # let any redeploy that was already underway before the pause
        # finish and its metadata update, then use that stable reading
        # as the baseline for the hold below
        log.info('waiting for daemon versions to settle after stopping')
        baseline = self.settled_daemon_versions('overall')

        # while stopped, no daemon may change version
        log.info('verifying that no daemon changes version while the upgrade is stopped')
        self.hold(
            f'daemon versions unchanged while the upgrade is stopped '
            f'(baseline {baseline})',
            lambda: self.daemon_versions('overall') == baseline,
            seconds=self.POLL_NO_VERSION_CHANGE_SEC,
            period=self.POLL_NO_VERSION_CHANGE_INTERVAL_SEC,
        )

        # restart the upgrade
        log.info('Restarting the upgrade')
        res = self.orch('upgrade', 'start', '--image', self.target_image, check=False)
        self.assertEqual(res.rc, 0, 'failed to restart upgrade')

        self.wait_upgrade_in_progress()

        self.finish_upgrade()
