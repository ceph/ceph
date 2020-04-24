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

from execnet.gateway_bootstrap import HostNotFound

from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, RGWSpec, \
    NFSServiceSpec, IscsiServiceSpec
from ceph.deployment.drive_selection.selector import DriveSelection
from ceph.deployment.inventory import Devices, Device
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

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
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

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    def test_service_ls(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            assert wait(cephadm_module, c) == []

            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_mds(ServiceSpec('mds', 'name', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed mds.name.* on host 'test'")

            c = cephadm_module.list_daemons()

            def remove_id(dd):
                out = dd.to_json()
                del out['daemon_id']
                return out

            assert [remove_id(dd) for dd in wait(cephadm_module, c)] == [
                {
                    'daemon_type': 'mds',
                    'hostname': 'test',
                    'status': 1,
                    'status_desc': 'starting'}
            ]

            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('rgw', 'r.z', placement=ps)
            c = cephadm_module.apply_rgw(spec)
            assert wait(cephadm_module, c) == 'Scheduled rgw update...'

            c = cephadm_module.describe_service()
            out = [o.to_json() for o in wait(cephadm_module, c)]
            expected = [
                {
                    'placement': {'hosts': [{'hostname': 'test', 'name': '', 'network': ''}]},
                    'service_id': 'name',
                    'service_name': 'mds.name',
                    'service_type': 'mds',
                    'status': {'running': 1, 'size': 0},
                    'unmanaged': True
                },
                {
                    'placement': {
                        'count': 1,
                        'hosts': [{'hostname': 'test', 'name': '', 'network': ''}]
                    },
                    'rgw_realm': 'r',
                    'rgw_zone': 'z',
                    'service_id': 'r.z',
                    'service_name': 'rgw.r.z',
                    'service_type': 'rgw',
                    'status': {'running': 0, 'size': 1}
                }
            ]
            assert out == expected
            assert [ServiceDescription.from_json(o).to_json() for o in expected] == expected

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
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

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    def test_mon_add(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            c = cephadm_module.add_mon(ServiceSpec('mon', placement=ps))
            assert wait(cephadm_module, c) == ["Deployed mon.a on host 'test'"]

            with pytest.raises(OrchestratorError, match="Must set public_network config option or specify a CIDR network,"):
                ps = PlacementSpec(hosts=['test'], count=1)
                c = cephadm_module.add_mon(ServiceSpec('mon', placement=ps))
                wait(cephadm_module, c)

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    def test_mgr_update(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            r = cephadm_module._apply_service(ServiceSpec('mgr', placement=ps))
            assert r

    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command")
    def test_find_destroyed_osds(self, _mon_cmd, cephadm_module):
        dict_out = {
            "nodes": [
                {
                    "id": -1,
                    "name": "default",
                    "type": "root",
                    "type_id": 11,
                    "children": [
                        -3
                    ]
                },
                {
                    "id": -3,
                    "name": "host1",
                    "type": "host",
                    "type_id": 1,
                    "pool_weights": {},
                    "children": [
                        0
                    ]
                },
                {
                    "id": 0,
                    "device_class": "hdd",
                    "name": "osd.0",
                    "type": "osd",
                    "type_id": 0,
                    "crush_weight": 0.0243988037109375,
                    "depth": 2,
                    "pool_weights": {},
                    "exists": 1,
                    "status": "destroyed",
                    "reweight": 1,
                    "primary_affinity": 1
                }
            ],
            "stray": []
        }
        json_out = json.dumps(dict_out)
        _mon_cmd.return_value = (0, json_out, '')
        out = cephadm_module.find_destroyed_osds()
        assert out == {'host1': ['0']}

    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command")
    def test_find_destroyed_osds_cmd_failure(self, _mon_cmd, cephadm_module):
        _mon_cmd.return_value = (1, "", "fail_msg")
        with pytest.raises(OrchestratorError):
            out = cephadm_module.find_destroyed_osds()

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_osd_save(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            json_spec = {'service_type': 'osd', 'host_pattern': 'test', 'service_id': 'foo', 'data_devices': {'all': True}}
            spec = ServiceSpec.from_json(json_spec)
            assert isinstance(spec, DriveGroupSpec)
            c = cephadm_module.apply_drivegroups([spec])
            assert wait(cephadm_module, c) == ['Scheduled osd update...']
            _save_spec.assert_called_with(spec)

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_osd_save_placement(self, _save_spec, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            json_spec = {'service_type': 'osd', 'placement': {'host_pattern': 'test'}, 'service_id': 'foo', 'data_devices': {'all': True}}
            spec = ServiceSpec.from_json(json_spec)
            assert isinstance(spec, DriveGroupSpec)
            c = cephadm_module.apply_drivegroups([spec])
            assert wait(cephadm_module, c) == ['Scheduled osd update...']
            _save_spec.assert_called_with(spec)

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_create_osds(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'), data_devices=DeviceSelection(paths=['']))
            c = cephadm_module.create_osds(dg)
            out = wait(cephadm_module, c)
            assert out == "Created no osd(s) on host test; already created?"

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_prepare_drivegroup(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'), data_devices=DeviceSelection(paths=['']))
            out = cephadm_module.prepare_drivegroup(dg)
            assert len(out) == 1
            f1 = out[0]
            assert f1[0] == 'test'
            assert isinstance(f1[1], DriveSelection)

    @pytest.mark.parametrize(
        "devices, preview, exp_command",
        [
            # no preview and only one disk, prepare is used due the hack that is in place.
            (['/dev/sda'], False, "lvm prepare --bluestore --data /dev/sda --no-systemd"),
            # no preview and multiple disks, uses batch
            (['/dev/sda', '/dev/sdb'], False, "lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd"),
            # preview and only one disk needs to use batch again to generate the preview
            (['/dev/sda'], True, "lvm batch --no-auto /dev/sda --report --format json"),
            # preview and multiple disks work the same
            (['/dev/sda', '/dev/sdb'], True, "lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd --report --format json"),
        ]
    )
    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_driveselection_to_ceph_volume(self, cephadm_module, devices, preview, exp_command):
        with self._with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'), data_devices=DeviceSelection(paths=devices))
            ds = DriveSelection(dg, Devices([Device(path) for path in devices]))
            preview = preview
            out = cephadm_module.driveselection_to_ceph_volume(dg, ds, [], preview)
            assert out in exp_command

    @mock.patch("cephadm.module.SpecStore.find")
    @mock.patch("cephadm.module.CephadmOrchestrator.prepare_drivegroup")
    @mock.patch("cephadm.module.CephadmOrchestrator.driveselection_to_ceph_volume")
    @mock.patch("cephadm.module.CephadmOrchestrator._run_ceph_volume_command")
    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_preview_drivegroups_str(self, _run_c_v_command, _ds_to_cv, _prepare_dg, _find_store, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'), data_devices=DeviceSelection(paths=['']))
            _find_store.return_value = [dg]
            _prepare_dg.return_value = [('host1', 'ds_dummy')]
            _run_c_v_command.return_value = ("{}", '', 0)
            cephadm_module.preview_drivegroups(drive_group_name='foo')
            _find_store.assert_called_once_with(service_name='foo')
            _prepare_dg.assert_called_once_with(dg)
            _run_c_v_command.assert_called_once()

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

            c = cephadm_module.remove_daemons(['osd.0'])
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
            c = cephadm_module.add_rgw(RGWSpec(rgw_realm='realm', rgw_zone='zone', placement=ps))
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed rgw.realm.zone.* on host 'test'")


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_rgw_update(self, cephadm_module):
        with self._with_host(cephadm_module, 'host1'):
            with self._with_host(cephadm_module, 'host2'):
                ps = PlacementSpec(hosts=['host1'], count=1)
                c = cephadm_module.add_rgw(RGWSpec(rgw_realm='realm', rgw_zone='zone1', placement=ps))
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed rgw.realm.zone1.host1.* on host 'host1'")

                ps = PlacementSpec(hosts=['host1', 'host2'], count=2)
                r = cephadm_module._apply_service(RGWSpec(rgw_realm='realm', rgw_zone='zone1', placement=ps))
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
            c = cephadm_module.remove_daemons(['rgw.myrgw.myhost.myid'])
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
    @mock.patch("cephadm.module.CephadmOrchestrator.rados", mock.MagicMock())
    def test_nfs(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = NFSServiceSpec('name', pool='pool', namespace='namespace', placement=ps)
            c = cephadm_module.add_nfs(spec)
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed nfs.name.* on host 'test'")

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_iscsi(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = IscsiServiceSpec('name', pool='pool', placement=ps)
            c = cephadm_module.add_iscsi(spec)
            [out] = wait(cephadm_module, c)
            match_glob(out, "Deployed iscsi.name.* on host 'test'")

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
    def test_apply_mgr_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('mgr', placement=ps)
            c = cephadm_module.apply_mgr(spec)
            assert wait(cephadm_module, c) == 'Scheduled mgr update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_apply_mds_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('mds', 'fsname', placement=ps)
            c = cephadm_module.apply_mds(spec)
            assert wait(cephadm_module, c) == 'Scheduled mds update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_apply_rgw_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('rgw', 'r.z', placement=ps)
            c = cephadm_module.apply_rgw(spec)
            assert wait(cephadm_module, c) == 'Scheduled rgw update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_apply_rbd_mirror_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('rbd-mirror', placement=ps)
            c = cephadm_module.apply_rbd_mirror(spec)
            assert wait(cephadm_module, c) == 'Scheduled rbd-mirror update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_apply_nfs_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = NFSServiceSpec('name', pool='pool', namespace='namespace', placement=ps)
            c = cephadm_module.apply_nfs(spec)
            assert wait(cephadm_module, c) == 'Scheduled nfs update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_apply_iscsi_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = IscsiServiceSpec('name', pool='pool', placement=ps)
            c = cephadm_module.apply_iscsi(spec)
            assert wait(cephadm_module, c) == 'Scheduled iscsi update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_apply_prometheus_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('prometheus', placement=ps)
            c = cephadm_module.apply_prometheus(spec)
            assert wait(cephadm_module, c) == 'Scheduled prometheus update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    def test_apply_node_exporter_save(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = ServiceSpec('node-exporter', placement=ps, service_id='my_exporter')
            c = cephadm_module.apply_node_exporter(spec)
            assert wait(cephadm_module, c) == 'Scheduled node-exporter update...'
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service())] == [spec]
            assert [d.spec for d in wait(cephadm_module, cephadm_module.describe_service(service_name='node-exporter.my_exporter'))] == [spec]

    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("remoto.process.check")
    def test_offline(self, _check, _get_connection, cephadm_module):
        _check.return_value = '{}', '', 0
        _get_connection.return_value = mock.Mock(), mock.Mock()
        with self._with_host(cephadm_module, 'test'):
            _get_connection.side_effect = HostNotFound
            code, out, err = cephadm_module.check_host('test')
            assert out == ''
            assert 'Failed to connect to test (test)' in err

            out = wait(cephadm_module, cephadm_module.get_hosts())[0].to_json()
            assert out == HostSpec('test', 'test', status='Offline').to_json()

            _get_connection.side_effect = None
            assert cephadm_module._check_host('test') is None
            out = wait(cephadm_module, cephadm_module.get_hosts())[0].to_json()
            assert out == HostSpec('test', 'test').to_json()
