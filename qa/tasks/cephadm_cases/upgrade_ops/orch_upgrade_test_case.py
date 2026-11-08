"""
Integration tests for the cephadm orch upgrade operations.

This module holds the upgrade-specific helpers shared by every facet;
the generic command execution, polling and cluster plumbing come from
tasks.cephadm_cases.base.CephadmTestCase. Test classes, one module
per suite facet:

test_pause_resume_ops.py             TestUpgradePauseResume:
                                        ceph orch upgrade pause/resume
test_stop_start_ops.py               TestUpgradeStopRestart:
                                        ceph orch upgrade stop/start
test_mgr_fail_mid_upgrade.py         TestMgrFailMidUpgrade:
                                        active mgr failover mid-upgrade
"""

import json
import time
import logging
import re
from typing import Any, Dict, Set

from tasks.cephadm_cases.base import CephadmTestCase

log = logging.getLogger(__name__)

WORKDIR = '/tmp/upgrade-ops-qa'


class UpgradeOpsTestCase(CephadmTestCase):
    """ Upgrade Operations tests shared helpers."""

    WORKDIR = WORKDIR
    MGRS_REQUIRED = 2
    POLL_UPGRADE_FINISH_TIMEOUT_SEC = 1800
    POLL_VERSION_SETTLE_INTERVAL_SEC = 60
    POLL_VERSION_SETTLE_TIMEOUT_SEC = 600

    # Annotation-only on purpose: no class-level value is created, so
    # reading either attribute before start_upgrade() raises
    # AttributeError instead of silently using an empty default.

    #: daemon versions present before the upgrade started; set by
    #: start_upgrade() and used by finish_upgrade() to tell which
    #: daemons have not been upgraded yet.
    source_version: Set[str]

    #: image the upgrade was started with; set by start_upgrade().
    target_image: str

    # ------------------------------------------------------------------
    # cluster inspection
    # ------------------------------------------------------------------
    def ceph_versions(self) -> Dict[str, Dict[str, int]]:
        """
        The 'ceph versions' report for the entire cluster: a mapping of
        daemon type (plus the aggregate 'overall') to a mapping of
        version string to the number of daemons reporting it, e.g.::

            {
                'mon': {
                    'ceph version 22.0.0-575-ga49549c7 (...) vampire (...)': 3,
                },
                'mgr': {
                    'ceph version 22.0.0-575-ga49549c7 (...) vampire (...)': 2,
                },
                'osd': {
                    'ceph version 21.1.1 (...) umbrella (...)': 3,
                },
                'overall': {
                    'ceph version 21.1.1 (...) umbrella (...)': 3,
                    'ceph version 22.0.0-575-ga49549c7 (...) vampire (...)': 5,
                },
            }
        """
        return self.ceph_json('versions')

    def daemon_versions(self, daemon_type: str) -> Dict[str, int]:
        """
        The 'ceph versions' report for a daemon type, e.g. 'mon' or
        the aggregate 'overall': a mapping of version string to the
        number of daemons reporting it.
        """
        return self.ceph_versions()[daemon_type]

    @staticmethod
    def upgraded_daemon_count(
        versions: Dict[str, int], old: Set[str]
    ) -> int:
        return sum(n for v, n in versions.items() if v not in old)

    # ------------------------------------------------------------------
    # cluster state
    # ------------------------------------------------------------------
    def health_ok(self) -> bool:
        return (
            self.ceph('health', check=False).out.strip() == 'HEALTH_OK'
        )

    # ------------------------------------------------------------------
    # Upgrade helpers
    # ------------------------------------------------------------------
    def start_upgrade(self) -> None:
        """
        Wait for HEALTH_OK, start the upgrade to the target image and
        wait for it to be in progress and not paused.

        Records ``self.source_version`` (the daemon versions present
        just before the upgrade) and ``self.target_image`` (the image
        being upgraded to) so that finish_upgrade() can verify the
        outcome without the test having to pass them around.
        """
        log.info('waiting for cluster health to be OK before starting upgrade')
        self.poll('HEALTH_OK before upgrade', self.health_ok)

        # record what we are upgrading from and to: finish_upgrade()
        # needs both to verify the outcome of this upgrade
        self.source_version = set(self.daemon_versions('overall'))
        self.target_image = self.upgrade_target_image()
        log.info('upgrading to image %s from current version %s',
                 self.target_image, self.source_version)

        # start the upgrade (async, but the command itself must succeed)
        res = self.orch('upgrade', 'start', '--image', self.target_image, check=False)
        self.assertEqual(res.rc, 0, 'failed to start upgrade')

        # whatever the test outcome, don't leave an upgrade running
        # behind for the next test
        self.addCleanup(self._cleanup_upgrade)

        self.wait_upgrade_in_progress()

    def _cleanup_upgrade(self) -> None:
        """
        addCleanup() hook registered by start_upgrade(); not meant to be
        called directly. Logs the final upgrade status and stops the
        upgrade if one is still in progress or paused.

        Best effort: never raises, so a cleanup error can neither turn a
        passing test into an error nor skip the stop.
        """
        try:
            status = self._upgrade_status()
        except Exception as e:
            log.warning('cleanup: could not read upgrade status: %s', e)
            status = None  # unknown: stop anyway

        if status is not None:
            log.info('cleanup: final ceph orch upgrade status:\n%s',
                     json.dumps(status, indent=4))

        log.info('cleanup: stopping the upgrade')
        # rc is 0 also when no upgrade is running ('No upgrade in
        # progress'), so a non-zero rc means the stop really failed
        res = self.orch('upgrade', 'stop', check=False)
        if res.rc == 0:
            log.info('cleanup: upgrade stop: %s', res.out.strip())
        else:
            log.warning('cleanup: failed to stop upgrade (rc=%s): %s',
                        res.rc, res.out)

        try:
            ceph_versions = self.ceph_versions()
            log.info('cleanup: final ceph versions report:\n%s',
                     json.dumps(ceph_versions, indent=4))
        except Exception as e:
            log.warning('cleanup: could not read ceph versions: %s', e)

    def wait_upgrade_in_progress(self) -> None:
        """
        Wait for an upgrade to be in progress and not paused, e.g.
        after it was started, resumed or restarted.
        """
        log.info('waiting for upgrade to be in progress and not paused')
        self.poll(
            'upgrade in progress and not paused',
            lambda: (
                self.upgrade_in_progress()
                and not self.upgrade_is_paused()
            ),
        )

    def finish_upgrade(self) -> None:
        """
        Wait for the upgrade started by start_upgrade() to complete and
        verify its outcome: no upgrade in progress, no daemon left on
        any of the source versions, and the cluster back to HEALTH_OK.
        """
        log.info('waiting for the upgrade to finish')
        self.poll(
            'upgrade finished',
            self.no_upgrade_in_progress,
            timeout=self.POLL_UPGRADE_FINISH_TIMEOUT_SEC,
        )

        log.info('verifying that no daemon is left on the source version %s',
                 self.source_version)
        self.poll(
            'all daemons off the source version',
            lambda: not (
                set(self.daemon_versions('overall')) & self.source_version
            ),
        )

        log.info('verifying that cluster health is OK after upgrade')
        self.poll('HEALTH_OK after upgrade', self.health_ok)

    def upgrade_target_image(self) -> str:
        """
        The image to upgrade to: the sha1 of the build under test
        (a top-level teuthology config value, set when the suite is
        scheduled against a ceph-ci build), applied to the repository
        of the image the cluster was deployed with.

        Resolution: 'sha1' is a top-level teuthology config value set
        when the suite is scheduled against a ceph-ci build.
        """
        sha1 = self.job_ctx().config.get('sha1')
        if not sha1:
            raise AssertionError(
                'no upgrade target: the sha1 of the build under test is '
                'not set in the teuthology config'
            )
        deployed = self.job_ctx().ceph[self._mgr().mon_manager.cluster].image
        repo = re.split(r'[:@]', deployed, maxsplit=1)[0]
        return f'{repo}:{sha1}'

    def settled_daemon_versions(self, daemon_type: str) -> Dict[str, int]:
        """
        Wait until the daemon versions stop changing and return them.

        Pausing the upgrade only sets a flag: the cephadm serve loop
        still finishes any redeploy it had already started, and the
        daemon metadata behind 'ceph versions' only updates once a
        restarted daemon re-registers. So a version read taken right
        after the pause is still moving and makes a poor baseline.
        Instead, wait for two consecutive reads, one settle interval
        apart, to agree and use that as the baseline.
        """
        previous = self.daemon_versions(daemon_type)
        deadline = time.time() + self.POLL_VERSION_SETTLE_TIMEOUT_SEC
        while True:
            time.sleep(self.POLL_VERSION_SETTLE_INTERVAL_SEC)
            current = self.daemon_versions(daemon_type)
            if current == previous:
                return current
            if time.time() > deadline:
                raise AssertionError(
                    f'daemon versions for {daemon_type} did not settle '
                    f'within {self.POLL_VERSION_SETTLE_TIMEOUT_SEC}s'
                )
            log.info(
                'daemon versions for %s still changing (%s -> %s)',
                daemon_type, previous, current,
            )
            previous = current

    def _upgrade_status(self) -> Dict[str, Any]:
        """
        The parsed 'ceph orch upgrade status' report, e.g.::

            {
                "in_progress": true,
                "target_image": "quay.ceph.io/ceph-ci/ceph:tentacle",
                "services_complete": [],
                "which": "Upgrading all daemon types on all hosts",
                "progress": "",
                "message": "",
                "is_paused": false
            }

        Returns an empty dict when no upgrade is running (the command
        then prints plain text, even with --format json).
        """
        out = self.orch('upgrade', 'status', '--format', 'json').out.strip()
        if 'no upgrades in progress' in out.lower():
            return {}
        return json.loads(out)

    def upgrade_in_progress(self) -> bool:
        """
        True when an upgrade is in progress.
        Note that an upgrade can be in progress but paused due to an error.
        """
        return bool(self._upgrade_status().get('in_progress'))

    def upgrade_is_paused(self) -> bool:
        """
        True when an upgrade is paused.
        Note that an upgrade can be paused due to an error.
        """
        return bool(self._upgrade_status().get('is_paused'))

    def no_upgrade_in_progress(self) -> bool:
        """True when no upgrade is running and no upgrade is paused."""
        return not self.upgrade_in_progress() and not self.upgrade_is_paused()
