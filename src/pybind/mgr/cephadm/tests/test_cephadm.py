import datetime
import json
from contextlib import contextmanager

import pytest

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from cephadm.osd import OSDRemoval

try:
    from typing import Any
except ImportError:
    pass

from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, RGWSpec
from orchestrator import ServiceDescription, DaemonDescription, InventoryHost, \
    HostSpec, OrchestratorError
from tests import mock
from .fixtures import cephadm_module, wait, _run_cephadm, mon_command, match_glob
from cephadm.module import CephadmOrchestrator


"""
TODOs:
    There is really room for improvement here. I just quickly assembled theses tests.
    I general, everything should be testes in Teuthology as well. Reasons for
    also testing this here is the development roundtrip time.
"""


class TestCephadm(object):

    @contextmanager
    def _with_host(self, m, name):
        # type: (CephadmOrchestrator, str) -> None
        wait(m, m.add_host(HostSpec(hostname=name)))
        yield
        wait(m, m.remove_host(name))

    def test_get_unique_name(self, cephadm_module):
        # type: (CephadmOrchestrator) -> None
        existing = [
            DaemonDescription(daemon_type='mon', daemon_id='a')
        ]
        new_mon = cephadm_module.get_unique_name('mon', 'myhost', existing)
        match_glob(new_mon, 'myhost')
        new_mgr = cephadm_module.get_unique_name('mgr', 'myhost', existing)
        match_glob(new_mgr, 'myhost.*')

    def test_host(self, cephadm_module):
        assert wait(cephadm_module, cephadm_module.get_hosts()) == []
        with self._with_host(cephadm_module, 'test'):
            assert wait(cephadm_module, cephadm_module.get_hosts()) == [HostSpec('test', 'test')]

            # Be careful with backward compatibility when changing things here:
            assert json.loads(cephadm_module._store['inventory']) == \
                   {"test": {"hostname": "test", "addr": "test", "labels": [], "status": ""}}

            with self._with_host(cephadm_module, 'second'):
                assert wait(cephadm_module, cephadm_module.get_hosts()) == [
                    HostSpec('test', 'test'),
                    HostSpec('second', 'second')
                ]

            assert wait(cephadm_module, cephadm_module.get_hosts()) == [HostSpec('test', 'test')]
        assert wait(cephadm_module, cephadm_module.get_hosts()) == []

    def test_service_ls(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            assert wait(cephadm_module, c) == []

    def test_device_ls(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.get_inventory()
            assert wait(cephadm_module, c) == [InventoryHost('test')]

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
    def test_daemon_action(self, cephadm_module):
        cephadm_module.service_cache_timeout = 10
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            wait(cephadm_module, c)
            c = cephadm_module.daemon_action('redeploy', 'rgw', 'myrgw.foobar')
            assert wait(cephadm_module, c) == ["Deployed rgw.myrgw.foobar on host 'test'"]

            for what in ('start', 'stop', 'restart'):
                c = cephadm_module.daemon_action(what, 'rgw', 'myrgw.foobar')
                assert wait(cephadm_module, c) == [what + " rgw.myrgw.foobar from host 'test'"]


    def test_mon_add(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            c = cephadm_module.add_mon(ServiceSpec('mon', placement=ps))
            assert wait(cephadm_module, c) == ["Deployed mon.a on host 'test'"]

            with pytest.raises(OrchestratorError, match="Must set public_network config option or specify a CIDR network,"):
                ps = PlacementSpec(hosts=['test'], count=1)
                c = cephadm_module.add_mon(ServiceSpec('mon', placement=ps))
                wait(cephadm_module, c)

    def test_mgr_update(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            r = cephadm_module._apply_service(ServiceSpec('mgr', placement=ps))
            assert r


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_create_osds(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'), data_devices=DeviceSelection(paths=['']))
            c = cephadm_module.create_osds([dg])
            assert wait(cephadm_module, c) == ["Created no osd(s) on host test; already created?"]

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
    @mock.patch("cephadm.osd.RemoveUtil.get_pg_count", lambda _, __: 0)
    def test_remove_osds(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            wait(cephadm_module, c)

            c = cephadm_module.remove_daemons(['osd.0'], False)
            out = wait(cephadm_module, c)
            assert out == ["Removed osd.0 from host 'test'"]

            osd_removal_op = OSDRemoval(0, False, False, 'test', 'osd.0', datetime.datetime.utcnow(), -1)
            cephadm_module.rm_util.queue_osds_for_removal({osd_removal_op})
            cephadm_module.rm_util._remove_osds_bg()
            assert cephadm_module.rm_util.to_remove_osds == set()

            c = cephadm_module.remove_osds_status()
            out = wait(cephadm_module, c)
            assert out == set()


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_mds(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_mds(ServiceSpec('mds', 'name', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed mds.name.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_rgw(self, cephadm_module):

        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_rgw(RGWSpec('realm', 'zone', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed rgw.realm.zone.* on host 'test'")


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_rgw_update(self, cephadm_module):
        with self._with_host(cephadm_module, 'host1'):
            with self._with_host(cephadm_module, 'host2'):
                ps = PlacementSpec(hosts=['host1'], count=1)
                c = cephadm_module.add_rgw(RGWSpec('realm', 'zone1', placement=ps))
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed rgw.realm.zone1.host1.* on host 'host1'")

                ps = PlacementSpec(hosts=['host1', 'host2'], count=2)
                r = cephadm_module._apply_service(RGWSpec('realm', 'zone1', placement=ps))
                assert r

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
    def test_remove_daemon(self, cephadm_module):
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
    def test_remove_service(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            wait(cephadm_module, c)
            c = cephadm_module.remove_service('rgw.myrgw')
            out = wait(cephadm_module, c)
            assert out == ["Removed service rgw.myrgw"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_rbd_mirror(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_rbd_mirror(ServiceSpec('rbd-mirror', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed rbd-mirror.* on host 'test'")


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_prometheus(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)

            c = cephadm_module.add_prometheus(ServiceSpec('prometheus', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed prometheus.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_node_exporter(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)

            c = cephadm_module.add_node_exporter(ServiceSpec('node-exporter', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed node-exporter.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_grafana(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)

            c = cephadm_module.add_grafana(ServiceSpec('grafana', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed grafana.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_alertmanager(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)

            c = cephadm_module.add_alertmanager(ServiceSpec('alertmanager', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed alertmanager.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_blink_device_light(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.blink_device_light('ident', True, [('test', '', '')])
            assert wait(cephadm_module, c) == ['Set ident light for test: on']

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_mgr_save(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('mgr', placement=ps)
            c = cephadm_module.apply_mgr(spec)
            _save_spec.assert_called_with(spec)
            assert wait(cephadm_module, c) == 'Scheduled mgr update...'

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_mds_save(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('mds', 'fsname', placement=ps)
            c = cephadm_module.apply_mds(spec)
            _save_spec.assert_called_with(spec)
            assert wait(cephadm_module, c) == 'Scheduled mds update...'

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_rgw_save(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('rgw', 'r.z', placement=ps)
            c = cephadm_module.apply_rgw(spec)
            _save_spec.assert_called_with(spec)
            assert wait(cephadm_module, c) == 'Scheduled rgw update...'

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_rbd_mirror_save(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('rbd-mirror', placement=ps)
            c = cephadm_module.apply_rbd_mirror(spec)
            _save_spec.assert_called_with(spec)
            assert wait(cephadm_module, c) == 'Scheduled rbd-mirror update...'

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_prometheus_save(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('prometheus', placement=ps)
            c = cephadm_module.apply_prometheus(spec)
            _save_spec.assert_called_with(spec)
            assert wait(cephadm_module, c) == 'Scheduled prometheus update...'

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_node_exporter_save(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('node_exporter', placement=ps)
            c = cephadm_module.apply_node_exporter(spec)
            _save_spec.assert_called_with(spec)
            assert wait(cephadm_module, c) == 'Scheduled node_exporter update...'

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    @mock.patch("cephadm.module.yaml.load_all", return_value=[{'service_type': 'rgw', 'placement': {'count': 1}, 'spec': {'rgw_realm': 'realm1', 'rgw_zone': 'zone1'}}])
    @mock.patch("cephadm.module.ServiceSpec")
    def test_apply_service_config(self, _sspec, _yaml, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.apply_service_config('dummy')
            _save_spec.assert_called_once()
            _sspec.from_json.assert_called_once()
            assert wait(cephadm_module, c) == 'ServiceSpecs saved'

