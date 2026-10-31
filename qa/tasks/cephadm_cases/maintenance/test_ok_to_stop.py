"""
Integration test for the cephadm 'orch host ok-to-stop' command.

"""

import logging
from typing import List

from .host_management_test_case import HostManagementTestCase

log = logging.getLogger(__name__)


class TestOkToStop(HostManagementTestCase):
    POOL = 'test_pool'

    # ------------------------------------------------------------------
    # cluster state
    # ------------------------------------------------------------------
    def _pgs_in_state(self, state: str) -> int:
        summary = self.ceph_json('pg', 'stat')['pg_summary']
        return sum(
            s['num'] for s in summary['num_pg_by_state'] if s['name'] == state
        )

    def _all_pgs_active_clean(self) -> bool:
        summary = self.ceph_json('pg', 'stat')['pg_summary']
        clean = self._pgs_in_state('active+clean')
        return clean == summary['num_pgs']

    def _other_host(self, *exclude: str) -> str:
        others = [h for h in self.hosts() if h not in exclude]
        self.assertEqual(
            len(others),
            1,
            f'expected exactly one remaining host, found {others}',
        )
        return others[0]

    def _host_osds(self, hostname: str) -> List[int]:
        return [
            int(i)
            for i in self.run_cmd(['osd', 'ls-tree', hostname]).out.split()
        ]

    def _osds_down(self, hostname: str) -> bool:
        tree = self.ceph_json('osd', 'tree', 'down')
        down = {n['id'] for n in tree['nodes'] if n['type'] == 'osd'}
        return set(self._host_osds(hostname)) <= down

    # ------------------------------------------------------------------
    # tests
    # ------------------------------------------------------------------
    def test_ok_to_stop_healthy_host(self) -> None:
        host_c = self.host_without_mgr()

        # positive: healthy cluster, host.c is safe to stop
        self.assertEqual(
            self.run_cmd(
                ['orch', 'host', 'ok-to-stop', host_c], check=False
            ).rc,
            0,
        )

    def test_ok_to_stop_active_mgr_host(self) -> None:
        active_mgr_host = self.active_mgr_host()

        # negative: the host running the active mgr is refused
        res = self.run_cmd(
            ['orch', 'host', 'ok-to-stop', active_mgr_host], check=False
        )
        self.assertNotEqual(
            res.rc, 0, f'ok-to-stop unexpectedly succeeded: {res!r}'
        )
        self.assertIn('Cannot stop active Mgr daemon', res.text)

    def test_ok_to_stop_reduced_redundancy(self) -> None:
        host_without_mgr = self.host_without_mgr()
        active_mgr_host = self.active_mgr_host()
        standby_mgr_host = self._other_host(
            host_without_mgr, active_mgr_host
        )

        # size 3 pool so every PG keeps one replica per host
        self.run_cmd(['osd', 'pool', 'create', self.POOL, '32'])
        self.run_cmd(['osd', 'pool', 'set', self.POOL, 'size', '3'])
        self.run_cmd(['osd', 'pool', 'set', self.POOL, 'min_size', '2'])
        self.run_cmd(
            ['osd', 'pool', 'application', 'enable', self.POOL, 'rados']
        )

        # wait until ALL PGs across all pools reach active+clean
        self.poll('all PGs active+clean', self._all_pgs_active_clean)

        # negative: with STANDBY_MGR_HOST's OSDs down, stopping
        # host_without_mgr would leave 1/3 replicas < min_size
        self.run_cmd(['osd', 'set', 'noup'])
        try:
            for osd in self._host_osds(standby_mgr_host):
                self.run_cmd(['osd', 'down', f'osd.{osd}'])

            # wait for the OSDs to be marked as down
            self.poll(
                f'{standby_mgr_host} OSDs down',
                lambda: self._osds_down(standby_mgr_host),
            )

            # wait for the PGs to enter the
            # active+undersized+degraded state
            self.poll(
                'PGs active+undersized+degraded',
                lambda: self._pgs_in_state('active+undersized+degraded') > 0,
            )

            res = self.run_cmd(
                ['orch', 'host', 'ok-to-stop', host_without_mgr], check=False
            )
            self.assertNotEqual(
                res.rc,
                0,
                'ok-to-stop unexpectedly succeeded with reduced '
                f'redundancy: {res!r}',
            )
            self.assertIn('unsafe to stop osd', res.text)
        finally:
            # restore (also on failure, so the state is clean for the
            # other test): let the OSDs come back and wait for the
            # cluster to become healthy again
            self.run_cmd(['osd', 'unset', 'noup'])
            self.poll('HEALTH_OK after restore', self.health_ok, interval_sec=10)
            self.run_cmd(['config', 'set', 'mon', 'mon_allow_pool_delete', 'true'])
            self.run_cmd(['osd', 'pool', 'delete', self.POOL, self.POOL, '--yes-i-really-really-mean-it'])
            self.run_cmd(['config', 'set', 'mon', 'mon_allow_pool_delete', 'false'])

        # restore verified: ok-to-stop flips back to success
        self.assertEqual(
            self.run_cmd(
                ['orch', 'host', 'ok-to-stop', host_without_mgr],
                check=False,
            ).rc,
            0,
        )
