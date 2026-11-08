"""
Integration tests for the cephadm 'orch upgrade' operations:
start, pause and resume

"""

import logging

from .orch_upgrade_test_case import UpgradeOpsTestCase

log = logging.getLogger(__name__)


class TestUpgradePauseResume(UpgradeOpsTestCase):

    POLL_MONITOR_UPGRADE_TIMEOUT_SEC = 600
    POLL_NO_VERSION_CHANGE_SEC = 180
    POLL_NO_VERSION_CHANGE_INTERVAL_SEC = 15
    POLL_NUMBER_OF_MON_UPGRADED = 1

    # ------------------------------------------------------------------
    # tests
    # ------------------------------------------------------------------
    def test_upgrade_pause_without_start(self) -> None:
        """
        Test that pausing the upgrade without starting it fails.
        """
        self.assertTrue(
            self.no_upgrade_in_progress(),
            'an upgrade is already in progress before the test started',
        )
        log.info('attempting to pause upgrade without starting it')
        res = self.orch('upgrade', 'pause', check=False)
        self.assertNotEqual(res.rc, 0, 'pause succeeded without starting upgrade.')

    def test_upgrade_resume_without_start(self) -> None:
        """
        Test that resuming the upgrade without starting it fails.
        """
        self.assertTrue(
            self.no_upgrade_in_progress(),
            'an upgrade is already in progress before the test started',
        )
        log.info('attempting to resume upgrade without starting it')
        res = self.orch('upgrade', 'resume', check=False)
        self.assertNotEqual(res.rc, 0, 'resume succeeded without starting upgrade.')

    def test_upgrade_pause_resume(self) -> None:
        """
        Test the pause and resume functionality of the cephadm upgrade process.
        """
        self.start_upgrade()

        # wait until the mon daemon group is partially upgraded: with
        # three mons, one should be on the new version while two are
        # still on the old one
        log.info('waiting for the mon daemon group to be partially upgraded')
        self.poll(
            f'{self.POLL_NUMBER_OF_MON_UPGRADED}/3 mon daemons upgraded',
            lambda: self.upgraded_daemon_count(
                self.daemon_versions('mon'), self.source_version
            ) >= self.POLL_NUMBER_OF_MON_UPGRADED,
            self.POLL_MONITOR_UPGRADE_TIMEOUT_SEC,
        )

        # pause the upgrade
        log.info('pausing the upgrade')
        res = self.orch('upgrade', 'pause', check=False)
        self.assertEqual(res.rc, 0, 'failed to pause upgrade')

        self.poll(
            'upgrade paused',
            lambda: (
                self.upgrade_is_paused()
                and self.upgrade_in_progress()
            ),
        )

        # let any redeploy that was already underway before the pause
        # finish and its metadata update, then use that stable reading
        # as the baseline for the hold below
        log.info('waiting for daemon versions to settle after pausing')
        baseline = self.settled_daemon_versions('overall')

        # while paused, no daemon may change version
        log.info('verifying that no daemon changes version while the upgrade is paused')
        self.hold(
            f'daemon versions unchanged while the upgrade is paused '
            f'(baseline {baseline})',
            lambda: self.daemon_versions('overall') == baseline,
            seconds=self.POLL_NO_VERSION_CHANGE_SEC,
            period=self.POLL_NO_VERSION_CHANGE_INTERVAL_SEC,
        )

        # resume the upgrade
        log.info('resuming the upgrade')
        res = self.orch('upgrade', 'resume', check=False)
        self.assertEqual(res.rc, 0, 'failed to resume upgrade')

        self.wait_upgrade_in_progress()

        self.finish_upgrade()
