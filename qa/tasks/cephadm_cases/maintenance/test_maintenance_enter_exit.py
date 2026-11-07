"""
Integration test for the cephadm 'orch host maintenance enter/exit'
commands.
"""

import logging
import time
from io import StringIO
from typing import Any, List

from teuthology import misc as teuthology

from .host_management_test_case import (
    CommandResult,
    HostManagementTestCase,
)

log = logging.getLogger(__name__)

# time to wait before attempting to reconnect after issuing a reboot
REBOOT_SETTLE_SEC = 60
# timeout for reconnecting to all hosts after a reboot
RECONNECT_TIMEOUT_SEC = 480
# poll period for waiting on cluster health
POLL_PERIOD_SEC = 10


class TestMaintenanceEnterExit(HostManagementTestCase):
    # ------------------------------------------------------------------
    # cluster inspection
    # ------------------------------------------------------------------
    def _host_status(self, hostname: str) -> str:
        for h in self.ceph_json('orch', 'host', 'ls'):
            if h['hostname'] == hostname:
                return h.get('status') or ''
        raise AssertionError(f'no such host: {hostname}')

    def _host_has_noout(self, hostname: str) -> bool:
        dump = self.ceph_json('osd', 'dump')
        flags = dump.get('crush_node_flags', {}).get(hostname, [])
        return 'noout' in flags

    def _daemons_in_state(self, hostname: str, state: str) -> bool:
        daemons = self.orch_ps(hostname=hostname)
        return bool(daemons) and all(
            d['status_desc'] == state for d in daemons
        )

    # ------------------------------------------------------------------
    # systemd state on the host itself
    # ------------------------------------------------------------------
    def _remote_for_hostname(self, hostname: str) -> Any:
        assert self.ctx is not None
        for remote, roles in self.ctx.cluster.remotes.items():
            if remote.shortname == hostname:
                return remote
        raise AssertionError(f'no remote found for host {hostname}')

    def _systemd(self, remote: Any, args: List[str]) -> CommandResult:
        out = StringIO()
        err = StringIO()
        proc = remote.run(
            args=list(args), stdout=out, stderr=err, check_status=False
        )
        res = CommandResult(
            args, proc.exitstatus, out.getvalue(), err.getvalue()
        )
        log.debug('%r', res)
        return res

    def _assert_systemd_state(self, hostname: str, maintained: bool) -> None:
        """
        On the host itself: while it is in maintenance the cluster's
        systemd target is disabled and stopped and no ceph units of
        this cluster are running; after maintenance exit it is enabled
        and active again.
        """
        remote = self._remote_for_hostname(hostname)
        fsid = self.run_cmd(['fsid']).out.strip()
        target = f'ceph-{fsid}.target'
        enabled = self._systemd(remote, ['systemctl', 'is-enabled', target])
        active = self._systemd(remote, ['systemctl', 'is-active', target])
        if maintained:
            self.assertNotEqual(enabled.rc, 0, f'{enabled!r}')
            self.assertNotEqual(active.rc, 0, f'{active!r}')
            units = self._systemd(
                remote,
                [
                    'systemctl',
                    'list-units',
                    f'ceph-{fsid}@*',
                    '--state=active',
                    '--no-legend',
                ],
            )
            self.assertEqual(units.out.strip(), '', f'{units!r}')
        else:
            self.assertEqual(enabled.rc, 0, f'{enabled!r}')
            self.assertEqual(active.rc, 0, f'{active!r}')

    def _reboot_host(self, hostname: str) -> None:
        """
        Reboot the host and wait until it is reachable again.
        
        Uses the teuthology.orchestra.remote.Remote to execute
        the reboot command on the host.
        
        Uses teuthology.misc to loop until all hosts are reachable.
        """
        remote = self._remote_for_hostname(hostname)
        log.info('rebooting %s', hostname)
        remote.run(
            args=['sudo', 'reboot'], wait=False, check_status=False
        )
        # avoid an immediate reconnect attempt while the host is still
        # going down
        time.sleep(REBOOT_SETTLE_SEC)
        assert self.ctx is not None
        teuthology.reconnect(self.ctx, RECONNECT_TIMEOUT_SEC)
        log.info('%s is back up', hostname)

    # ------------------------------------------------------------------
    # tests
    # ------------------------------------------------------------------
    def test_maintenance_enter_active_mgr_refused(self) -> None:
        # maintenance enter on the active mgr host is refused with
        # EBUSY, no failover
        active_mgr_host = self.active_mgr_host()
        res = self.run_cmd(
            ['orch', 'host', 'maintenance', 'enter', active_mgr_host],
            check=False,
        )
        self.assertNotEqual(
            res.rc,
            0,
            'maintenance enter unexpectedly succeeded on the active '
            f'mgr host: {res!r}',
        )
        self.assertIn('Cannot stop active Mgr daemon', res.text)

    def test_maintenance_enter_exit(self) -> None:
        host_without_mgr = self.host_without_mgr()
        try:
            self.run_cmd(['orch', 'host', 'maintenance', 'enter', host_without_mgr])

            # the host is flagged with maintenance in orch host ls
            self.poll(
                f'{host_without_mgr} in maintenance',
                lambda: self._host_status(host_without_mgr) == 'maintenance',
            )

            # noout is set on the host's crush node
            self.assertTrue(
                self._host_has_noout(host_without_mgr), f'noout not set on {host_without_mgr}'
            )

            # wait until all daemons on the host are inactive
            self.poll(
                f'daemons on {host_without_mgr} stopped',
                lambda: self._daemons_in_state(host_without_mgr, 'stopped'),
            )

            # the systemd target is disabled and stopped on the host,
            # and no ceph units of this cluster are running on it
            self._assert_systemd_state(host_without_mgr, maintained=True)

            # reboot the host while it is in maintenance and verify
            # that it stays in maintenance across the reboot
            self._reboot_host(host_without_mgr)

            # the host is still flagged with maintenance after the reboot
            self.poll(
                f'{host_without_mgr} in maintenance after reboot',
                lambda: self._host_status(host_without_mgr) == 'maintenance',
            )

            # all daemons on the host are still stopped
            self.poll(
                f'daemons on {host_without_mgr} stopped after reboot',
                lambda: self._daemons_in_state(host_without_mgr, 'stopped'),
            )

            # the systemd target is still disabled and stopped on the
            # host, and no ceph units of this cluster are running on it
            self._assert_systemd_state(host_without_mgr, maintained=True)

        finally:
            # leave maintenance again (also on failure, so the state
            # is clean for the other test). When entering never
            # succeeded this is a no-op that fails quietly.
            self.run_cmd(
                ['orch', 'host', 'maintenance', 'exit', host_without_mgr], check=False
            )

        # wait until the host is no longer in maintenance
        self.poll(
            f'{host_without_mgr} out of maintenance',
            lambda: self._host_status(host_without_mgr) != 'maintenance',
        )

        # wait until all daemons on the host are active
        self.poll(
            f'daemons on {host_without_mgr} running',
            lambda: self._daemons_in_state(host_without_mgr, 'running'),
        )

        # wait until the cluster health is OK
        self.poll(
            'HEALTH_OK after maintenance exit', self.health_ok, interval_sec=POLL_PERIOD_SEC
        )

        # the systemd target is enabled and active on the host again
        self._assert_systemd_state(host_without_mgr, maintained=False)
