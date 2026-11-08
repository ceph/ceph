"""
Integration tests for the cephadm 'orch upgrade' behavior when the
active mgr fails in the middle of an upgrade.
"""

import logging

from .orch_upgrade_test_case import UpgradeOpsTestCase

log = logging.getLogger(__name__)


class TestMgrFailMidUpgrade(UpgradeOpsTestCase):

    POLL_OSD_UPGRADE_TIMEOUT_SEC = 1200
    POLL_NUMBER_OF_OSD_UPGRADED = 2

    # ------------------------------------------------------------------
    # tests
    # ------------------------------------------------------------------
    def test_mgr_fail_mid_upgrade(self) -> None:
        """
        Test that a failover of the active mgr in the middle of an
        upgrade. A standby takes over and the upgrade runs to completion.
        """
        self.start_upgrade()

        log.info('Waiting for at least 2 osd daemons to be upgraded')
        # wait until the osd daemon group is partially upgraded: with
        # six osds, two should be on the new version while four are
        # still on the old one
        self.poll(
            f'{self.POLL_NUMBER_OF_OSD_UPGRADED} osd daemons upgraded',
            lambda: self.upgraded_daemon_count(
                self.daemon_versions('osd'), self.source_version
            ) >= self.POLL_NUMBER_OF_OSD_UPGRADED,
            self.POLL_OSD_UPGRADE_TIMEOUT_SEC,
        )

        # fail the active mgr mid-upgrade; a standby must take over
        log.info('failing active mgr %s mid-upgrade',
                 self._mgr().get_active_id())
        self.fail_mgr_and_wait()

        # confirm the upgrade is still in progress after failing the active mgr
        self.assertTrue(
            self.upgrade_in_progress(),
            'Upgrade should still be in progress after failing the active mgr'
        )

        # the upgrade must still be running (or already finished) under
        # the new active mgr; in any case it must reach completion
        self.finish_upgrade()
