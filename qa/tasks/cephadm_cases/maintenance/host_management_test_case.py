"""
Integration tests for the cephadm host maintenance management.

This module holds the command execution plumbing and a base class
with the cluster inspection helpers shared by the host
maintenance tests.

This module holds the helpers shared by every facet. Test classes, one module
per suite facet:

test_maintenance_enter_exit.py        TestMaintenanceEnterExit:
                                      ceph orch host maintenance enter/exit

test_ok_to_stop.py                    TestOkToStop:
                                      ceph orch host maintenance ok-to-stop
"""

import json
import logging
import time
from io import StringIO
from typing import Any, Callable, Dict, List, Optional

from tasks.cephadm import _shell
from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class CommandFailed(Exception):
    """A ceph command exited non-zero when it was expected to succeed."""


class CommandResult:
    def __init__(self, args: List[str], rc: int, out: str, err: str) -> None:
        self.args = args
        self.rc = rc
        self.out = out
        self.err = err

    def __repr__(self) -> str:
        cmd = ' '.join(self.args)
        return f'<rc={self.rc} cmd={cmd!r} err={self.err.strip()[-500:]!r}>'

    @property
    def text(self) -> str:
        # the orch CLI reports command errors on stderr, but matching
        # on both streams keeps us independent of that detail
        return self.out + self.err


class HostManagementTestCase(MgrTestCase):
    """ Host maintenance management tests shared helpers."""

    MGRS_REQUIRED = 2

    def setUp(self) -> None:
        super().setUp()
        self.poll(
            'orchestrator ready',
            lambda: self.run_cmd(['orch', 'status'], check=False).rc == 0,
        )

    # ------------------------------------------------------------------
    # command execution
    # ------------------------------------------------------------------
    def run_cmd(
        self, args: List[str], check: bool = True, timeout: int = 300
    ) -> CommandResult:
        """Run a containerized ceph command on the admin host."""
        assert self.mon_manager is not None
        proc = _shell(
            self.ctx,
            self.mon_manager.cluster,
            self.mon_manager.controller,
            ['timeout', str(timeout), 'ceph'] + list(args),
            stdout=StringIO(),
            stderr=StringIO(),
            check_status=False,
        )
        res = CommandResult(
            args,
            proc.exitstatus,
            proc.stdout.getvalue(),
            proc.stderr.getvalue(),
        )
        log.debug('%r', res)
        if check and res.rc != 0:
            raise CommandFailed(f'command failed: {res!r}')
        return res

    def ceph_json(self, *args: str) -> Any:
        return json.loads(self.run_cmd(list(args) + ['--format', 'json']).out)

    def poll(
        self,
        what: str,
        fn: Callable[[], bool],
        timeout_sec: int = 300,
        interval_sec: int = 5,
    ) -> None:
        """Wait for fn() to become true, tolerating transient errors
        (a restarting daemon, a mgr failover, partial JSON output)."""
        deadline = time.time() + timeout_sec
        while True:
            try:
                if fn():
                    log.debug('poll %s: ok', what)
                    return
                log.debug('poll %s: not yet', what)
            except (CommandFailed, KeyError, IndexError, ValueError) as e:
                log.info('poll %s: not yet (%s)', what, e)
            if time.time() > deadline:
                raise AssertionError(
                    f'timed out after {timeout_sec}s waiting for {what}'
                )
            time.sleep(interval_sec)

    # ------------------------------------------------------------------
    # cluster inspection
    # ------------------------------------------------------------------
    def orch_ps(
        self,
        hostname: Optional[str] = None,
        daemon_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        args = ['orch', 'ps', '--refresh']
        if hostname:
            args += ['--hostname', hostname]
        if daemon_type:
            args += ['--daemon-type', daemon_type]
        out = self.run_cmd(args + ['--format', 'json']).out.strip()
        # 'orch ps' prints this plain-text line even when JSON is
        # requested
        if not out or out == 'No daemons reported':
            return []
        return json.loads(out)

    def hosts(self) -> List[str]:
        return [h['hostname'] for h in self.ceph_json('orch', 'host', 'ls')]

    def host_without_mgr(self) -> str:
        with_mgr = {d['hostname'] for d in self.orch_ps(daemon_type='mgr')}
        without = [h for h in self.hosts() if h not in with_mgr]
        self.assertEqual(
            len(without),
            1,
            f'expected exactly one host without a mgr, found {without}',
        )
        return without[0]

    def active_mgr_host(self) -> str:
        active = self.ceph_json('mgr', 'dump')['active_name']
        for d in self.orch_ps(daemon_type='mgr'):
            if d['daemon_id'] == active:
                return d['hostname']
        raise AssertionError(f'could not locate active mgr {active!r}')

    # ------------------------------------------------------------------
    # cluster state
    # ------------------------------------------------------------------
    def health_ok(self) -> bool:
        return (
            self.run_cmd(['health'], check=False).out.strip() == 'HEALTH_OK'
        )
