import json
from contextlib import contextmanager
import fnmatch

import pytest

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection

try:
    from typing import Any
except ImportError:
    pass

from orchestrator import ServiceDescription, DaemonDescription, InventoryNode, \
    ServiceSpec, PlacementSpec, RGWSpec, HostSpec, OrchestratorError
from tests import mock
from .fixtures import cephadm_module, wait


"""
TODOs:
    There is really room for improvement here. I just quickly assembled theses tests.
    I general, everything should be testes in Teuthology as well. Reasons for
    also testing this here is the development roundtrip time.
"""



def _run_cephadm(ret):
    def foo(*args, **kwargs):
        return ret, '', 0
    return foo

def mon_command(*args, **kwargs):
    return 0, '', ''


def match_glob(val, pat):
    ok = fnmatch.fnmatchcase(val, pat)
    if not ok:
        assert pat in val

class TestCephadm(object):

    @contextmanager
    def _with_host(self, m, name):
        wait(m, m.add_host(HostSpec(hostname=name)))
        yield
        wait(m, m.remove_host(name))

    def test_get_unique_name(self, cephadm_module):
        existing = [
            DaemonDescription(daemon_type='mon', daemon_id='a')
        ]
        new_mon = cephadm_module.get_unique_name('myhost', existing, 'mon')
        match_glob(new_mon, 'mon.myhost.*')

    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_host(self, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            assert wait(cephadm_module, cephadm_module.get_hosts()) == [InventoryNode('test')]
        c = cephadm_module.get_hosts()
        assert wait(cephadm_module, c) == []

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_service_ls(self, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            assert wait(cephadm_module, c) == []

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_device_ls(self, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.get_inventory()
            assert wait(cephadm_module, c) == [InventoryNode('test')]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='rgw.myrgw.foobar',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_daemon_action(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        cephadm_module.service_cache_timeout = 10
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            wait(cephadm_module, c)
            c = cephadm_module.daemon_action('redeploy', 'rgw', 'myrgw.foobar')
            assert wait(cephadm_module, c) == ["Deployed rgw.myrgw.foobar on host 'test'"]

            for what in ('start', 'stop', 'restart'):
                c = cephadm_module.daemon_action(what, 'rgw', 'myrgw.foobar')
                assert wait(cephadm_module, c) == [what + " rgw.myrgw.foobar from host 'test'"]


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_mon_update(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            c = cephadm_module.apply_mon(ServiceSpec(placement=ps))
            assert wait(cephadm_module, c) == ["Deployed mon.a on host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_mgr_update(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            c = cephadm_module.apply_mgr(ServiceSpec(placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed mgr.* on host 'test'")


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_create_osds(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec('test', data_devices=DeviceSelection(paths=['']))
            c = cephadm_module.create_osds([dg])
            assert wait(cephadm_module, c) == ["Created osd(s) on host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='osd.0',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_remove_osds(self, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            wait(cephadm_module, c)
            c = cephadm_module.remove_daemons(['osd.0'], False)
            out = wait(cephadm_module, c)
            assert out == ["Removed osd.0 from host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_mds(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_mds(ServiceSpec('name', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed mds.name.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_rgw(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):

        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_rgw(RGWSpec('realm', 'zone', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed rgw.realm.zone.* on host 'test'")


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_rgw_update(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):

        with self._with_host(cephadm_module, 'host1'):
            with self._with_host(cephadm_module, 'host2'):
                ps = PlacementSpec(hosts=['host1'], count=1)
                c = cephadm_module.add_rgw(RGWSpec('realm', 'zone1', placement=ps))
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed rgw.realm.zone1.host1.* on host 'host1'")

                ps = PlacementSpec(hosts=['host1', 'host2'], count=2)
                c = cephadm_module.apply_rgw(RGWSpec('realm', 'zone1', placement=ps))
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed rgw.realm.zone1.host2.* on host 'host2'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_rgw_update_fail(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):

        with self._with_host(cephadm_module, 'host1'):
            with self._with_host(cephadm_module, 'host2'):
                ps = PlacementSpec(hosts=['host1'], count=1)
                c = cephadm_module.add_rgw(RGWSpec('realm', 'zone1', placement=ps))
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed rgw.realm.zone1.host1.* on host 'host1'")

                ps = PlacementSpec(hosts=['host2'], count=1)
                c = cephadm_module.add_rgw(RGWSpec('realm', 'zone2', placement=ps))
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed rgw.realm.zone2.host2.* on host 'host2'")

                c = cephadm_module.list_daemons()
                r = wait(cephadm_module, c)
                assert len(r) == 2

                with pytest.raises(OrchestratorError):
                    ps = PlacementSpec(hosts=['host1', 'host2'], count=2)
                    c = cephadm_module.add_rgw(RGWSpec('realm', 'zone1', placement=ps))
                    [out] = wait(cephadm_module, c)


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='rgw.myrgw.myhost.myid',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_remove_daemon(self, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            wait(cephadm_module, c)
            c = cephadm_module.remove_daemons(['rgw.myrgw.myhost.myid'], False)
            out = wait(cephadm_module, c)
            assert out == ["Removed rgw.myrgw.myhost.myid from host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='rgw.myrgw.foobar',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_remove_service(self, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            wait(cephadm_module, c)
            c = cephadm_module.remove_service('rgw', 'myrgw')
            out = wait(cephadm_module, c)
            assert out == ["Removed rgw.myrgw.foobar from host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_rbd_mirror(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_rbd_mirror(ServiceSpec(name='name', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed rbd-mirror.* on host 'test'")


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_prometheus(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)

            c = cephadm_module.add_prometheus(ServiceSpec(placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed prometheus.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("cephadm.module.DaemonCache.save_host")
    @mock.patch("cephadm.module.DaemonCache.rm_host")
    def test_blink_device_light(self, _send_command, _get_connection, _save_host, _rm_host, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.blink_device_light('ident', True, [('test', '', '')])
            assert wait(cephadm_module, c) == ['Set ident light for test: on']
