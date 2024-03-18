import asyncio
import json
import logging

from contextlib import contextmanager

import pytest

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from cephadm.serve import CephadmServe
from cephadm.inventory import HostCacheStatus, ClientKeyringSpec
from cephadm.services.osd import OSD, OSDRemovalQueue, OsdIdClaims
from cephadm.utils import SpecialHostLabels

try:
    from typing import List
except ImportError:
    pass

from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, RGWSpec, \
    NFSServiceSpec, IscsiServiceSpec, HostPlacementSpec, CustomContainerSpec, MDSSpec, \
    CustomConfig, PrometheusSpec
from ceph.deployment.drive_selection.selector import DriveSelection
from ceph.deployment.inventory import Devices, Device
from ceph.utils import datetime_to_str, datetime_now, str_to_datetime
from orchestrator import DaemonDescription, InventoryHost, \
    HostSpec, OrchestratorError, DaemonDescriptionStatus, OrchestratorEvent
from tests import mock
from .fixtures import wait, _run_cephadm, match_glob, with_host, \
    with_cephadm_module, with_service, make_daemons_running, async_side_effect
from cephadm.module import CephadmOrchestrator

"""
TODOs:
    There is really room for improvement here. I just quickly assembled theses tests.
    I general, everything should be testes in Teuthology as well. Reasons for
    also testing this here is the development roundtrip time.
"""


def assert_rm_daemon(cephadm: CephadmOrchestrator, prefix, host):
    dds: List[DaemonDescription] = wait(cephadm, cephadm.list_daemons(host=host))
    d_names = [dd.name() for dd in dds if dd.name().startswith(prefix)]
    assert d_names
    # there should only be one daemon (if not match_glob will throw mismatch)
    assert len(d_names) == 1

    c = cephadm.remove_daemons(d_names)
    [out] = wait(cephadm, c)
    # picking the 1st element is needed, rather than passing the list when the daemon
    # name contains '-' char. If not, the '-' is treated as a range i.e. cephadm-exporter
    # is treated like a m-e range which is invalid. rbd-mirror (d-m) and node-exporter (e-e)
    # are valid, so pass without incident! Also, match_gob acts on strings anyway!
    match_glob(out, f"Removed {d_names[0]}* from host '{host}'")


@contextmanager
def with_daemon(cephadm_module: CephadmOrchestrator, spec: ServiceSpec, host: str):
    spec.placement = PlacementSpec(hosts=[host], count=1)

    c = cephadm_module.add_daemon(spec)
    [out] = wait(cephadm_module, c)
    match_glob(out, f"Deployed {spec.service_name()}.* on host '{host}'")

    dds = cephadm_module.cache.get_daemons_by_service(spec.service_name())
    for dd in dds:
        if dd.hostname == host:
            yield dd.daemon_id
            assert_rm_daemon(cephadm_module, spec.service_name(), host)
            return

    assert False, 'Daemon not found'


@contextmanager
def with_osd_daemon(cephadm_module: CephadmOrchestrator, _run_cephadm, host: str, osd_id: int, ceph_volume_lvm_list=None):
    cephadm_module.mock_store_set('_ceph_get', 'osd_map', {
        'osds': [
            {
                'osd': 1,
                'up_from': 0,
                'up': True,
                'uuid': 'uuid'
            }
        ]
    })

    _run_cephadm.reset_mock(return_value=True, side_effect=True)
    if ceph_volume_lvm_list:
        _run_cephadm.side_effect = ceph_volume_lvm_list
    else:
        async def _ceph_volume_list(s, host, entity, cmd, **kwargs):
            logging.info(f'ceph-volume cmd: {cmd}')
            if 'raw' in cmd:
                return json.dumps({
                    "21a4209b-f51b-4225-81dc-d2dca5b8b2f5": {
                        "ceph_fsid": cephadm_module._cluster_fsid,
                        "device": "/dev/loop0",
                        "osd_id": 21,
                        "osd_uuid": "21a4209b-f51b-4225-81dc-d2dca5b8b2f5",
                        "type": "bluestore"
                    },
                }), '', 0
            if 'lvm' in cmd:
                return json.dumps({
                    str(osd_id): [{
                        'tags': {
                            'ceph.cluster_fsid': cephadm_module._cluster_fsid,
                            'ceph.osd_fsid': 'uuid'
                        },
                        'type': 'data'
                    }]
                }), '', 0
            return '{}', '', 0

        _run_cephadm.side_effect = _ceph_volume_list

    assert cephadm_module._osd_activate(
        [host]).stdout == f"Created osd(s) 1 on host '{host}'"
    assert _run_cephadm.mock_calls == [
        mock.call(host, 'osd', 'ceph-volume',
                  ['--', 'lvm', 'list', '--format', 'json'], no_fsid=False, error_ok=False, image='', log_output=True),
        mock.call(host, f'osd.{osd_id}', 'deploy',
                  ['--name', f'osd.{osd_id}', '--meta-json', mock.ANY,
                   '--config-json', '-', '--osd-fsid', 'uuid'],
                  stdin=mock.ANY, image=''),
        mock.call(host, 'osd', 'ceph-volume',
                  ['--', 'raw', 'list', '--format', 'json'], no_fsid=False, error_ok=False, image='', log_output=True),
    ]
    dd = cephadm_module.cache.get_daemon(f'osd.{osd_id}', host=host)
    assert dd.name() == f'osd.{osd_id}'
    yield dd
    cephadm_module._remove_daemons([(f'osd.{osd_id}', host)])


class TestCephadm(object):

    def test_get_unique_name(self, cephadm_module):
        # type: (CephadmOrchestrator) -> None
        existing = [
            DaemonDescription(daemon_type='mon', daemon_id='a')
        ]
        new_mon = cephadm_module.get_unique_name('mon', 'myhost', existing)
        match_glob(new_mon, 'myhost')
        new_mgr = cephadm_module.get_unique_name('mgr', 'myhost', existing)
        match_glob(new_mgr, 'myhost.*')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_host(self, cephadm_module):
        assert wait(cephadm_module, cephadm_module.get_hosts()) == []
        with with_host(cephadm_module, 'test'):
            assert wait(cephadm_module, cephadm_module.get_hosts()) == [HostSpec('test', '1::4')]

            # Be careful with backward compatibility when changing things here:
            assert json.loads(cephadm_module.get_store('inventory')) == \
                {"test": {"hostname": "test", "addr": "1::4", "labels": [], "status": ""}}

            with with_host(cephadm_module, 'second', '1.2.3.5'):
                assert wait(cephadm_module, cephadm_module.get_hosts()) == [
                    HostSpec('test', '1::4'),
                    HostSpec('second', '1.2.3.5')
                ]

            assert wait(cephadm_module, cephadm_module.get_hosts()) == [HostSpec('test', '1::4')]
        assert wait(cephadm_module, cephadm_module.get_hosts()) == []

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.utils.resolve_ip")
    def test_re_add_host_receive_loopback(self, resolve_ip, cephadm_module):
        resolve_ip.side_effect = ['192.168.122.1', '127.0.0.1', '127.0.0.1']
        assert wait(cephadm_module, cephadm_module.get_hosts()) == []
        cephadm_module._add_host(HostSpec('test', '192.168.122.1'))
        assert wait(cephadm_module, cephadm_module.get_hosts()) == [
            HostSpec('test', '192.168.122.1')]
        cephadm_module._add_host(HostSpec('test'))
        assert wait(cephadm_module, cephadm_module.get_hosts()) == [
            HostSpec('test', '192.168.122.1')]
        with pytest.raises(OrchestratorError):
            cephadm_module._add_host(HostSpec('test2'))

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_service_ls(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            c = cephadm_module.list_daemons(refresh=True)
            assert wait(cephadm_module, c) == []
            with with_service(cephadm_module, MDSSpec('mds', 'name', unmanaged=True)) as _, \
                    with_daemon(cephadm_module, MDSSpec('mds', 'name'), 'test') as _:

                c = cephadm_module.list_daemons()

                def remove_id_events(dd):
                    out = dd.to_json()
                    del out['daemon_id']
                    del out['events']
                    del out['daemon_name']
                    return out

                assert [remove_id_events(dd) for dd in wait(cephadm_module, c)] == [
                    {
                        'service_name': 'mds.name',
                        'daemon_type': 'mds',
                        'hostname': 'test',
                        'status': 2,
                        'status_desc': 'starting',
                        'is_active': False,
                        'ports': [],
                    }
                ]

                with with_service(cephadm_module, ServiceSpec('rgw', 'r.z'),
                                  CephadmOrchestrator.apply_rgw, 'test', status_running=True):
                    make_daemons_running(cephadm_module, 'mds.name')

                    c = cephadm_module.describe_service()
                    out = [dict(o.to_json()) for o in wait(cephadm_module, c)]
                    expected = [
                        {
                            'placement': {'count': 2},
                            'service_id': 'name',
                            'service_name': 'mds.name',
                            'service_type': 'mds',
                            'status': {'created': mock.ANY, 'running': 1, 'size': 2},
                            'unmanaged': True
                        },
                        {
                            'placement': {
                                'count': 1,
                                'hosts': ["test"]
                            },
                            'service_id': 'r.z',
                            'service_name': 'rgw.r.z',
                            'service_type': 'rgw',
                            'status': {'created': mock.ANY, 'running': 1, 'size': 1,
                                       'ports': [80]},
                        }
                    ]
                    for o in out:
                        if 'events' in o:
                            del o['events']  # delete it, as it contains a timestamp
                    assert out == expected

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_service_ls_service_type_flag(self, cephadm_module):
        with with_host(cephadm_module, 'host1'):
            with with_host(cephadm_module, 'host2'):
                with with_service(cephadm_module, ServiceSpec('mgr', placement=PlacementSpec(count=2)),
                                  CephadmOrchestrator.apply_mgr, '', status_running=True):
                    with with_service(cephadm_module, MDSSpec('mds', 'test-id', placement=PlacementSpec(count=2)),
                                      CephadmOrchestrator.apply_mds, '', status_running=True):

                        # with no service-type. Should provide info fot both services
                        c = cephadm_module.describe_service()
                        out = [dict(o.to_json()) for o in wait(cephadm_module, c)]
                        expected = [
                            {
                                'placement': {'count': 2},
                                'service_name': 'mgr',
                                'service_type': 'mgr',
                                'status': {'created': mock.ANY,
                                           'running': 2,
                                           'size': 2}
                            },
                            {
                                'placement': {'count': 2},
                                'service_id': 'test-id',
                                'service_name': 'mds.test-id',
                                'service_type': 'mds',
                                'status': {'created': mock.ANY,
                                           'running': 2,
                                           'size': 2}
                            },
                        ]

                        for o in out:
                            if 'events' in o:
                                del o['events']  # delete it, as it contains a timestamp
                        assert out == expected

                        # with service-type. Should provide info fot only mds
                        c = cephadm_module.describe_service(service_type='mds')
                        out = [dict(o.to_json()) for o in wait(cephadm_module, c)]
                        expected = [
                            {
                                'placement': {'count': 2},
                                'service_id': 'test-id',
                                'service_name': 'mds.test-id',
                                'service_type': 'mds',
                                'status': {'created': mock.ANY,
                                           'running': 2,
                                           'size': 2}
                            },
                        ]

                        for o in out:
                            if 'events' in o:
                                del o['events']  # delete it, as it contains a timestamp
                        assert out == expected

                        # service-type should not match with service names
                        c = cephadm_module.describe_service(service_type='mds.test-id')
                        out = [dict(o.to_json()) for o in wait(cephadm_module, c)]
                        assert out == []

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_device_ls(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            c = cephadm_module.get_inventory()
            assert wait(cephadm_module, c) == [InventoryHost('test')]

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='rgw.myrgw.foobar',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            ),
            dict(
                name='something.foo.bar',
                style='cephadm',
                fsid='fsid',
            ),
            dict(
                name='haproxy.test.bar',
                style='cephadm',
                fsid='fsid',
            ),

        ])
    ))
    def test_list_daemons(self, cephadm_module: CephadmOrchestrator):
        cephadm_module.service_cache_timeout = 10
        with with_host(cephadm_module, 'test'):
            CephadmServe(cephadm_module)._refresh_host_daemons('test')
            dds = wait(cephadm_module, cephadm_module.list_daemons())
            assert {d.name() for d in dds} == {'rgw.myrgw.foobar', 'haproxy.test.bar'}

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_daemon_action(self, cephadm_module: CephadmOrchestrator):
        cephadm_module.service_cache_timeout = 10
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, RGWSpec(service_id='myrgw.foobar', unmanaged=True)) as _, \
                    with_daemon(cephadm_module, RGWSpec(service_id='myrgw.foobar'), 'test') as daemon_id:

                d_name = 'rgw.' + daemon_id

                c = cephadm_module.daemon_action('redeploy', d_name)
                assert wait(cephadm_module,
                            c) == f"Scheduled to redeploy rgw.{daemon_id} on host 'test'"

                for what in ('start', 'stop', 'restart'):
                    c = cephadm_module.daemon_action(what, d_name)
                    assert wait(cephadm_module,
                                c) == F"Scheduled to {what} {d_name} on host 'test'"

                # Make sure, _check_daemons does a redeploy due to monmap change:
                cephadm_module._store['_ceph_get/mon_map'] = {
                    'modified': datetime_to_str(datetime_now()),
                    'fsid': 'foobar',
                }
                cephadm_module.notify('mon_map', None)

                CephadmServe(cephadm_module)._check_daemons()

                assert cephadm_module.events.get_for_daemon(d_name) == [
                    OrchestratorEvent(mock.ANY, 'daemon', d_name, 'INFO',
                                      f"Deployed {d_name} on host \'test\'"),
                    OrchestratorEvent(mock.ANY, 'daemon', d_name, 'INFO',
                                      f"stop {d_name} from host \'test\'"),
                ]

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_daemon_action_fail(self, cephadm_module: CephadmOrchestrator):
        cephadm_module.service_cache_timeout = 10
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, RGWSpec(service_id='myrgw.foobar', unmanaged=True)) as _, \
                    with_daemon(cephadm_module, RGWSpec(service_id='myrgw.foobar'), 'test') as daemon_id:
                with mock.patch('ceph_module.BaseMgrModule._ceph_send_command') as _ceph_send_command:

                    _ceph_send_command.side_effect = Exception("myerror")

                    # Make sure, _check_daemons does a redeploy due to monmap change:
                    cephadm_module.mock_store_set('_ceph_get', 'mon_map', {
                        'modified': datetime_to_str(datetime_now()),
                        'fsid': 'foobar',
                    })
                    cephadm_module.notify('mon_map', None)

                    CephadmServe(cephadm_module)._check_daemons()

                    evs = [e.message for e in cephadm_module.events.get_for_daemon(
                        f'rgw.{daemon_id}')]

                    assert 'myerror' in ''.join(evs)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_daemon_action_event_timestamp_update(self, cephadm_module: CephadmOrchestrator):
        # Test to make sure if a new daemon event is created with the same subject
        # and message that the timestamp of the event is updated to let users know
        # when it most recently occurred.
        cephadm_module.service_cache_timeout = 10
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, RGWSpec(service_id='myrgw.foobar', unmanaged=True)) as _, \
                    with_daemon(cephadm_module, RGWSpec(service_id='myrgw.foobar'), 'test') as daemon_id:

                d_name = 'rgw.' + daemon_id

                now = str_to_datetime('2023-10-18T22:45:29.119250Z')
                with mock.patch("cephadm.inventory.datetime_now", lambda: now):
                    c = cephadm_module.daemon_action('redeploy', d_name)
                    assert wait(cephadm_module,
                                c) == f"Scheduled to redeploy rgw.{daemon_id} on host 'test'"

                    CephadmServe(cephadm_module)._check_daemons()

                d_events = cephadm_module.events.get_for_daemon(d_name)
                assert len(d_events) == 1
                assert d_events[0].created == now

                later = str_to_datetime('2023-10-18T23:46:37.119250Z')
                with mock.patch("cephadm.inventory.datetime_now", lambda: later):
                    c = cephadm_module.daemon_action('redeploy', d_name)
                    assert wait(cephadm_module,
                                c) == f"Scheduled to redeploy rgw.{daemon_id} on host 'test'"

                    CephadmServe(cephadm_module)._check_daemons()

                d_events = cephadm_module.events.get_for_daemon(d_name)
                assert len(d_events) == 1
                assert d_events[0].created == later

    @pytest.mark.parametrize(
        "action",
        [
            'start',
            'stop',
            'restart',
            'reconfig',
            'redeploy'
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.HostCache.save_host")
    def test_daemon_check(self, _save_host, cephadm_module: CephadmOrchestrator, action):
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='grafana'), CephadmOrchestrator.apply_grafana, 'test') as d_names:
                [daemon_name] = d_names

                cephadm_module._schedule_daemon_action(daemon_name, action)

                assert cephadm_module.cache.get_scheduled_daemon_action(
                    'test', daemon_name) == action

                CephadmServe(cephadm_module)._check_daemons()

                assert _save_host.called_with('test')
                assert cephadm_module.cache.get_scheduled_daemon_action('test', daemon_name) is None

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_daemon_check_extra_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test'):

            # Also testing deploying mons without explicit network placement
            cephadm_module.check_mon_command({
                'prefix': 'config set',
                'who': 'mon',
                'name': 'public_network',
                'value': '127.0.0.0/8'
            })

            cephadm_module.cache.update_host_networks(
                'test',
                {
                    "127.0.0.0/8": [
                        "127.0.0.1"
                    ],
                }
            )

            with with_service(cephadm_module, ServiceSpec(service_type='mon'), CephadmOrchestrator.apply_mon, 'test') as d_names:
                [daemon_name] = d_names

                cephadm_module._set_extra_ceph_conf('[mon]\nk=v')

                CephadmServe(cephadm_module)._check_daemons()

                _run_cephadm.assert_called_with(
                    'test', 'mon.test', 'deploy', [
                        '--name', 'mon.test',
                        '--meta-json', ('{"service_name": "mon", "ports": [], "ip": null, "deployed_by": [], "rank": null, '
                                        '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--reconfig',
                    ],
                    stdin='{"config": "[mon]\\nk=v\\n[mon.test]\\npublic network = 127.0.0.0/8\\n", '
                    + '"keyring": "", "files": {"config": "[mon.test]\\npublic network = 127.0.0.0/8\\n"}}',
                    image='')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_mon_crush_location_deployment(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test'):
            cephadm_module.check_mon_command({
                'prefix': 'config set',
                'who': 'mon',
                'name': 'public_network',
                'value': '127.0.0.0/8'
            })

            cephadm_module.cache.update_host_networks(
                'test',
                {
                    "127.0.0.0/8": [
                        "127.0.0.1"
                    ],
                }
            )

            with with_service(cephadm_module, ServiceSpec(service_type='mon', crush_locations={'test': ['datacenter=a', 'rack=2']}), CephadmOrchestrator.apply_mon, 'test'):
                _run_cephadm.assert_called_with(
                    'test', 'mon.test', 'deploy', [
                        '--name', 'mon.test',
                        '--meta-json', '{"service_name": "mon", "ports": [], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}',
                        '--config-json', '-',
                    ],
                    stdin=('{"config": "[mon.test]\\npublic network = 127.0.0.0/8\\n", "keyring": "", '
                           '"files": {"config": "[mon.test]\\npublic network = 127.0.0.0/8\\n"}, "crush_location": "datacenter=a"}'),
                    image='',
                )

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_extra_container_args(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='crash', extra_container_args=['--cpus=2', '--quiet']), CephadmOrchestrator.apply_crash):
                _run_cephadm.assert_called_with(
                    'test', 'crash.test', 'deploy', [
                        '--name', 'crash.test',
                        '--meta-json', ('{"service_name": "crash", "ports": [], "ip": null, "deployed_by": [], "rank": null, '
                                        '"rank_generation": null, "extra_container_args": ["--cpus=2", "--quiet"], "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--extra-container-args=--cpus=2',
                        '--extra-container-args=--quiet'
                    ],
                    stdin='{"config": "", "keyring": "[client.crash.test]\\nkey = None\\n"}',
                    image='',
                )

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_extra_entrypoint_args(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='node-exporter',
                              extra_entrypoint_args=['--collector.textfile.directory=/var/lib/node_exporter/textfile_collector', '--some-other-arg']),
                              CephadmOrchestrator.apply_node_exporter):
                _run_cephadm.assert_called_with(
                    'test', 'node-exporter.test', 'deploy', [
                        '--name', 'node-exporter.test',
                        '--meta-json', ('{"service_name": "node-exporter", "ports": [9100], "ip": null, "deployed_by": [], "rank": null, '
                                        '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": '
                                        '["--collector.textfile.directory=/var/lib/node_exporter/textfile_collector", '
                                        '"--some-other-arg"]}'),
                        '--config-json', '-',
                        '--tcp-ports', '9100',
                        '--extra-entrypoint-args=--collector.textfile.directory=/var/lib/node_exporter/textfile_collector',
                        '--extra-entrypoint-args=--some-other-arg'
                    ],
                    stdin='{}',
                    image='',
                )

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_extra_entrypoint_and_container_args(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='node-exporter',
                              extra_entrypoint_args=['--collector.textfile.directory=/var/lib/node_exporter/textfile_collector', '--some-other-arg'],
                              extra_container_args=['--cpus=2', '--quiet']),
                              CephadmOrchestrator.apply_node_exporter):
                _run_cephadm.assert_called_with(
                    'test', 'node-exporter.test', 'deploy', [
                        '--name', 'node-exporter.test',
                        '--meta-json', ('{"service_name": "node-exporter", "ports": [9100], "ip": null, "deployed_by": [], "rank": null, '
                                        '"rank_generation": null, "extra_container_args": ["--cpus=2", "--quiet"], "extra_entrypoint_args": '
                                        '["--collector.textfile.directory=/var/lib/node_exporter/textfile_collector", '
                                        '"--some-other-arg"]}'),
                        '--config-json', '-',
                        '--tcp-ports', '9100',
                        '--extra-container-args=--cpus=2',
                        '--extra-container-args=--quiet',
                        '--extra-entrypoint-args=--collector.textfile.directory=/var/lib/node_exporter/textfile_collector',
                        '--extra-entrypoint-args=--some-other-arg'
                    ],
                    stdin='{}',
                    image='',
                )

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_extra_entrypoint_and_container_args_with_spaces(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='node-exporter',
                              extra_entrypoint_args=['--entrypoint-arg-with-value value', '--some-other-arg   3'],
                              extra_container_args=['--cpus    2', '--container-arg-with-value value']),
                              CephadmOrchestrator.apply_node_exporter):
                _run_cephadm.assert_called_with(
                    'test', 'node-exporter.test', 'deploy', [
                        '--name', 'node-exporter.test',
                        '--meta-json', ('{"service_name": "node-exporter", "ports": [9100], "ip": null, "deployed_by": [], "rank": null, '
                                        '"rank_generation": null, "extra_container_args": ["--cpus    2", "--container-arg-with-value value"], '
                                        '"extra_entrypoint_args": ["--entrypoint-arg-with-value value", "--some-other-arg   3"]}'),
                        '--config-json', '-',
                        '--tcp-ports', '9100',
                        '--extra-container-args=--cpus',
                        '--extra-container-args=2',
                        '--extra-container-args=--container-arg-with-value',
                        '--extra-container-args=value',
                        '--extra-entrypoint-args=--entrypoint-arg-with-value',
                        '--extra-entrypoint-args=value',
                        '--extra-entrypoint-args=--some-other-arg',
                        '--extra-entrypoint-args=3'
                    ],
                    stdin='{}',
                    image='',
                )

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_custom_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        test_cert = ['-----BEGIN PRIVATE KEY-----',
                     'YSBhbGlxdXlhbSBlcmF0LCBzZWQgZGlhbSB2b2x1cHR1YS4gQXQgdmVybyBlb3Mg',
                     'ZXQgYWNjdXNhbSBldCBqdXN0byBkdW8=',
                     '-----END PRIVATE KEY-----',
                     '-----BEGIN CERTIFICATE-----',
                     'YSBhbGlxdXlhbSBlcmF0LCBzZWQgZGlhbSB2b2x1cHR1YS4gQXQgdmVybyBlb3Mg',
                     'ZXQgYWNjdXNhbSBldCBqdXN0byBkdW8=',
                     '-----END CERTIFICATE-----']
        configs = [
            CustomConfig(content='something something something',
                         mount_path='/etc/test.conf'),
            CustomConfig(content='\n'.join(test_cert), mount_path='/usr/share/grafana/thing.crt')
        ]
        conf_outs = [json.dumps(c.to_json()) for c in configs]
        stdin_str = '{' + \
            f'"config": "", "keyring": "[client.crash.test]\\nkey = None\\n", "custom_config_files": [{conf_outs[0]}, {conf_outs[1]}]' + '}'
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='crash', custom_configs=configs), CephadmOrchestrator.apply_crash):
                _run_cephadm.assert_called_with(
                    'test', 'crash.test', 'deploy', [
                        '--name', 'crash.test',
                        '--meta-json', ('{"service_name": "crash", "ports": [], "ip": null, "deployed_by": [], "rank": null, '
                                        '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                    ],
                    stdin=stdin_str,
                    image='',
                )

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_daemon_check_post(self, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='grafana'), CephadmOrchestrator.apply_grafana, 'test'):

                # Make sure, _check_daemons does a redeploy due to monmap change:
                cephadm_module.mock_store_set('_ceph_get', 'mon_map', {
                    'modified': datetime_to_str(datetime_now()),
                    'fsid': 'foobar',
                })
                cephadm_module.notify('mon_map', None)
                cephadm_module.mock_store_set('_ceph_get', 'mgr_map', {
                    'modules': ['dashboard']
                })

                with mock.patch("cephadm.module.CephadmOrchestrator.mon_command") as _mon_cmd:
                    CephadmServe(cephadm_module)._check_daemons()
                    _mon_cmd.assert_any_call(
                        {'prefix': 'dashboard set-grafana-api-url', 'value': 'https://[1::4]:3000'},
                        None)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '1.2.3.4')
    def test_iscsi_post_actions_with_missing_daemon_in_cache(self, cephadm_module: CephadmOrchestrator):
        # https://tracker.ceph.com/issues/52866
        with with_host(cephadm_module, 'test1'):
            with with_host(cephadm_module, 'test2'):
                with with_service(cephadm_module, IscsiServiceSpec(service_id='foobar', pool='pool', placement=PlacementSpec(host_pattern='*')), CephadmOrchestrator.apply_iscsi, 'test'):

                    CephadmServe(cephadm_module)._apply_all_services()
                    assert len(cephadm_module.cache.get_daemons_by_type('iscsi')) == 2

                    # get a deamons from postaction list (ARRGH sets!!)
                    tempset = cephadm_module.requires_post_actions.copy()
                    tempdeamon1 = tempset.pop()
                    tempdeamon2 = tempset.pop()

                    # make sure post actions has 2 daemons in it
                    assert len(cephadm_module.requires_post_actions) == 2

                    # replicate a host cache that is not in sync when check_daemons is called
                    tempdd1 = cephadm_module.cache.get_daemon(tempdeamon1)
                    tempdd2 = cephadm_module.cache.get_daemon(tempdeamon2)
                    host = 'test1'
                    if 'test1' not in tempdeamon1:
                        host = 'test2'
                    cephadm_module.cache.rm_daemon(host, tempdeamon1)

                    # Make sure, _check_daemons does a redeploy due to monmap change:
                    cephadm_module.mock_store_set('_ceph_get', 'mon_map', {
                        'modified': datetime_to_str(datetime_now()),
                        'fsid': 'foobar',
                    })
                    cephadm_module.notify('mon_map', None)
                    cephadm_module.mock_store_set('_ceph_get', 'mgr_map', {
                        'modules': ['dashboard']
                    })

                    with mock.patch("cephadm.module.IscsiService.config_dashboard") as _cfg_db:
                        CephadmServe(cephadm_module)._check_daemons()
                        _cfg_db.assert_called_once_with([tempdd2])

                        # post actions still has the other deamon in it and will run next _check_deamons
                        assert len(cephadm_module.requires_post_actions) == 1

                        # post actions was missed for a daemon
                        assert tempdeamon1 in cephadm_module.requires_post_actions

                        # put the daemon back in the cache
                        cephadm_module.cache.add_daemon(host, tempdd1)

                        _cfg_db.reset_mock()
                        # replicate serve loop running again
                        CephadmServe(cephadm_module)._check_daemons()

                        # post actions should have been called again
                        _cfg_db.asset_called()

                        # post actions is now empty
                        assert len(cephadm_module.requires_post_actions) == 0

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_mon_add(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='mon', unmanaged=True)):
                ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
                c = cephadm_module.add_daemon(ServiceSpec('mon', placement=ps))
                assert wait(cephadm_module, c) == ["Deployed mon.a on host 'test'"]

                with pytest.raises(OrchestratorError, match="Must set public_network config option or specify a CIDR network,"):
                    ps = PlacementSpec(hosts=['test'], count=1)
                    c = cephadm_module.add_daemon(ServiceSpec('mon', placement=ps))
                    wait(cephadm_module, c)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_mgr_update(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            r = CephadmServe(cephadm_module)._apply_service(ServiceSpec('mgr', placement=ps))
            assert r

            assert_rm_daemon(cephadm_module, 'mgr.a', 'test')

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
        osd_claims = OsdIdClaims(cephadm_module)
        assert osd_claims.get() == {'host1': ['0']}
        assert osd_claims.filtered_by_host('host1') == ['0']
        assert osd_claims.filtered_by_host('host1.domain.com') == ['0']

    @ pytest.mark.parametrize(
        "ceph_services, cephadm_daemons, strays_expected, metadata",
        # [ ([(daemon_type, daemon_id), ... ], [...], [...]), ... ]
        [
            (
                [('mds', 'a'), ('osd', '0'), ('mgr', 'x')],
                [],
                [('mds', 'a'), ('osd', '0'), ('mgr', 'x')],
                {},
            ),
            (
                [('mds', 'a'), ('osd', '0'), ('mgr', 'x')],
                [('mds', 'a'), ('osd', '0'), ('mgr', 'x')],
                [],
                {},
            ),
            (
                [('mds', 'a'), ('osd', '0'), ('mgr', 'x')],
                [('mds', 'a'), ('osd', '0')],
                [('mgr', 'x')],
                {},
            ),
            # https://tracker.ceph.com/issues/49573
            (
                [('rgw-nfs', '14649')],
                [],
                [('nfs', 'foo-rgw.host1')],
                {'14649': {'id': 'nfs.foo-rgw.host1-rgw'}},
            ),
            (
                [('rgw-nfs', '14649'), ('rgw-nfs', '14650')],
                [('nfs', 'foo-rgw.host1'), ('nfs', 'foo2.host2')],
                [],
                {'14649': {'id': 'nfs.foo-rgw.host1-rgw'}, '14650': {'id': 'nfs.foo2.host2-rgw'}},
            ),
            (
                [('rgw-nfs', '14649'), ('rgw-nfs', '14650')],
                [('nfs', 'foo-rgw.host1')],
                [('nfs', 'foo2.host2')],
                {'14649': {'id': 'nfs.foo-rgw.host1-rgw'}, '14650': {'id': 'nfs.foo2.host2-rgw'}},
            ),
        ]
    )
    def test_check_for_stray_daemons(
            self,
            cephadm_module,
            ceph_services,
            cephadm_daemons,
            strays_expected,
            metadata
    ):
        # mock ceph service-map
        services = []
        for service in ceph_services:
            s = {'type': service[0], 'id': service[1]}
            services.append(s)
        ls = [{'hostname': 'host1', 'services': services}]

        with mock.patch.object(cephadm_module, 'list_servers', mock.MagicMock()) as list_servers:
            list_servers.return_value = ls
            list_servers.__iter__.side_effect = ls.__iter__

            # populate cephadm daemon cache
            dm = {}
            for daemon_type, daemon_id in cephadm_daemons:
                dd = DaemonDescription(daemon_type=daemon_type, daemon_id=daemon_id)
                dm[dd.name()] = dd
            cephadm_module.cache.update_host_daemons('host1', dm)

            def get_metadata_mock(svc_type, svc_id, default):
                return metadata[svc_id]

            with mock.patch.object(cephadm_module, 'get_metadata', new_callable=lambda: get_metadata_mock):

                # test
                CephadmServe(cephadm_module)._check_for_strays()

                # verify
                strays = cephadm_module.health_checks.get('CEPHADM_STRAY_DAEMON')
                if not strays:
                    assert len(strays_expected) == 0
                else:
                    for dt, di in strays_expected:
                        name = '%s.%s' % (dt, di)
                        for detail in strays['detail']:
                            if name in detail:
                                strays['detail'].remove(detail)
                                break
                        assert name in detail
                    assert len(strays['detail']) == 0
                    assert strays['count'] == len(strays_expected)

    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command")
    def test_find_destroyed_osds_cmd_failure(self, _mon_cmd, cephadm_module):
        _mon_cmd.return_value = (1, "", "fail_msg")
        with pytest.raises(OrchestratorError):
            OsdIdClaims(cephadm_module)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_apply_osd_save(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):

            spec = DriveGroupSpec(
                service_id='foo',
                placement=PlacementSpec(
                    host_pattern='*',
                ),
                data_devices=DeviceSelection(
                    all=True
                )
            )

            c = cephadm_module.apply([spec])
            assert wait(cephadm_module, c) == ['Scheduled osd.foo update...']

            inventory = Devices([
                Device(
                    '/dev/sdb',
                    available=True
                ),
            ])

            cephadm_module.cache.update_host_devices('test', inventory.devices)

            _run_cephadm.side_effect = async_side_effect((['{}'], '', 0))

            assert CephadmServe(cephadm_module)._apply_all_services() is False

            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume',
                ['--config-json', '-', '--', 'lvm', 'batch',
                    '--no-auto', '/dev/sdb', '--yes', '--no-systemd'],
                env_vars=['CEPH_VOLUME_OSDSPEC_AFFINITY=foo'], error_ok=True,
                stdin='{"config": "", "keyring": ""}')
            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume', ['--', 'lvm', 'list', '--format', 'json'], image='', no_fsid=False, error_ok=False, log_output=True)
            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume', ['--', 'raw', 'list', '--format', 'json'], image='', no_fsid=False, error_ok=False, log_output=True)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_apply_osd_save_non_collocated(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):

            spec = DriveGroupSpec(
                service_id='noncollocated',
                placement=PlacementSpec(
                    hosts=['test']
                ),
                data_devices=DeviceSelection(paths=['/dev/sdb']),
                db_devices=DeviceSelection(paths=['/dev/sdc']),
                wal_devices=DeviceSelection(paths=['/dev/sdd'])
            )

            c = cephadm_module.apply([spec])
            assert wait(cephadm_module, c) == ['Scheduled osd.noncollocated update...']

            inventory = Devices([
                Device('/dev/sdb', available=True),
                Device('/dev/sdc', available=True),
                Device('/dev/sdd', available=True)
            ])

            cephadm_module.cache.update_host_devices('test', inventory.devices)

            _run_cephadm.side_effect = async_side_effect((['{}'], '', 0))

            assert CephadmServe(cephadm_module)._apply_all_services() is False

            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume',
                ['--config-json', '-', '--', 'lvm', 'batch',
                    '--no-auto', '/dev/sdb', '--db-devices', '/dev/sdc',
                    '--wal-devices', '/dev/sdd', '--yes', '--no-systemd'],
                env_vars=['CEPH_VOLUME_OSDSPEC_AFFINITY=noncollocated'],
                error_ok=True, stdin='{"config": "", "keyring": ""}')
            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume', ['--', 'lvm', 'list', '--format', 'json'], image='', no_fsid=False, error_ok=False, log_output=True)
            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume', ['--', 'raw', 'list', '--format', 'json'], image='', no_fsid=False, error_ok=False, log_output=True)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.SpecStore.save")
    def test_apply_osd_save_placement(self, _save_spec, cephadm_module):
        with with_host(cephadm_module, 'test'):
            json_spec = {'service_type': 'osd', 'placement': {'host_pattern': 'test'},
                         'service_id': 'foo', 'data_devices': {'all': True}}
            spec = ServiceSpec.from_json(json_spec)
            assert isinstance(spec, DriveGroupSpec)
            c = cephadm_module.apply([spec])
            assert wait(cephadm_module, c) == ['Scheduled osd.foo update...']
            _save_spec.assert_called_with(spec)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_create_osds(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'),
                                data_devices=DeviceSelection(paths=['']))
            c = cephadm_module.create_osds(dg)
            out = wait(cephadm_module, c)
            assert out == "Created no osd(s) on host test; already created?"
            bad_dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='invalid_hsot'),
                                    data_devices=DeviceSelection(paths=['']))
            c = cephadm_module.create_osds(bad_dg)
            out = wait(cephadm_module, c)
            assert "Invalid 'host:device' spec: host not found in cluster" in out

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_create_noncollocated_osd(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'),
                                data_devices=DeviceSelection(paths=['']))
            c = cephadm_module.create_osds(dg)
            out = wait(cephadm_module, c)
            assert out == "Created no osd(s) on host test; already created?"

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch('cephadm.services.osd.OSDService._run_ceph_volume_command')
    @mock.patch('cephadm.services.osd.OSDService.driveselection_to_ceph_volume')
    @mock.patch('cephadm.services.osd.OsdIdClaims.refresh', lambda _: None)
    @mock.patch('cephadm.services.osd.OsdIdClaims.get', lambda _: {})
    @mock.patch('cephadm.inventory.HostCache.get_daemons_by_service')
    def test_limit_not_reached(self, _get_daemons_by_service, d_to_cv, _run_cv_cmd, cephadm_module):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'),
                                data_devices=DeviceSelection(limit=5, rotational=1),
                                service_id='not_enough')

            disks_found = [
                '[{"data": "/dev/vdb", "data_size": "50.00 GB", "encryption": "None"}, {"data": "/dev/vdc", "data_size": "50.00 GB", "encryption": "None"}]']
            d_to_cv.return_value = 'foo'
            _run_cv_cmd.side_effect = async_side_effect((disks_found, '', 0))
            _get_daemons_by_service.return_value = [DaemonDescription(daemon_type='osd', hostname='test', service_name='not_enough')]
            preview = cephadm_module.osd_service.generate_previews([dg], 'test')

            for osd in preview:
                assert 'notes' in osd
                assert osd['notes'] == [
                    ('NOTE: Did not find enough disks matching filter on host test to reach '
                     'data device limit\n(New Devices: 2 | Existing Matching Daemons: 1 | Limit: 5)')]

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_prepare_drivegroup(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'),
                                data_devices=DeviceSelection(paths=['']))
            out = cephadm_module.osd_service.prepare_drivegroup(dg)
            assert len(out) == 1
            f1 = out[0]
            assert f1[0] == 'test'
            assert isinstance(f1[1], DriveSelection)

    @pytest.mark.parametrize(
        "devices, preview, exp_commands",
        [
            # no preview and only one disk, prepare is used due the hack that is in place.
            (['/dev/sda'], False, ["lvm batch --no-auto /dev/sda --yes --no-systemd"]),
            # no preview and multiple disks, uses batch
            (['/dev/sda', '/dev/sdb'], False,
             ["CEPH_VOLUME_OSDSPEC_AFFINITY=test.spec lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd"]),
            # preview and only one disk needs to use batch again to generate the preview
            (['/dev/sda'], True, ["lvm batch --no-auto /dev/sda --yes --no-systemd --report --format json"]),
            # preview and multiple disks work the same
            (['/dev/sda', '/dev/sdb'], True,
             ["CEPH_VOLUME_OSDSPEC_AFFINITY=test.spec lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd --report --format json"]),
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_driveselection_to_ceph_volume(self, cephadm_module, devices, preview, exp_commands):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(service_id='test.spec', placement=PlacementSpec(
                host_pattern='test'), data_devices=DeviceSelection(paths=devices))
            ds = DriveSelection(dg, Devices([Device(path) for path in devices]))
            preview = preview
            out = cephadm_module.osd_service.driveselection_to_ceph_volume(ds, [], preview)
            assert all(any(cmd in exp_cmd for exp_cmd in exp_commands)
                       for cmd in out), f'Expected cmds from f{out} in {exp_commands}'

    @pytest.mark.parametrize(
        "devices, preview, exp_commands",
        [
            # one data device, no preview
            (['/dev/sda'], False, ["raw prepare --bluestore --data /dev/sda"]),
            # multiple data devices, no preview
            (['/dev/sda', '/dev/sdb'], False,
             ["raw prepare --bluestore --data /dev/sda", "raw prepare --bluestore --data /dev/sdb"]),
            # one data device, preview
            (['/dev/sda'], True, ["raw prepare --bluestore --data /dev/sda --report --format json"]),
            # multiple data devices, preview
            (['/dev/sda', '/dev/sdb'], True,
             ["raw prepare --bluestore --data /dev/sda --report --format json", "raw prepare --bluestore --data /dev/sdb --report --format json"]),
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_raw_driveselection_to_ceph_volume(self, cephadm_module, devices, preview, exp_commands):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(service_id='test.spec', method='raw', placement=PlacementSpec(
                host_pattern='test'), data_devices=DeviceSelection(paths=devices))
            ds = DriveSelection(dg, Devices([Device(path) for path in devices]))
            preview = preview
            out = cephadm_module.osd_service.driveselection_to_ceph_volume(ds, [], preview)
            assert all(any(cmd in exp_cmd for exp_cmd in exp_commands)
                       for cmd in out), f'Expected cmds from f{out} in {exp_commands}'

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm(
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
    @mock.patch("cephadm.services.osd.OSD.exists", True)
    @mock.patch("cephadm.services.osd.RemoveUtil.get_pg_count", lambda _, __: 0)
    @mock.patch("cephadm.services.osd.RemoveUtil.get_weight")
    @mock.patch("cephadm.services.osd.RemoveUtil.reweight_osd")
    def test_remove_osds(self, _reweight_osd, _get_weight, cephadm_module):
        osd_initial_weight = 2.1
        _get_weight.return_value = osd_initial_weight
        with with_host(cephadm_module, 'test'):
            CephadmServe(cephadm_module)._refresh_host_daemons('test')
            c = cephadm_module.list_daemons()
            wait(cephadm_module, c)

            c = cephadm_module.remove_daemons(['osd.0'])
            out = wait(cephadm_module, c)
            assert out == ["Removed osd.0 from host 'test'"]

            osd_0 = OSD(osd_id=0,
                        replace=False,
                        force=False,
                        hostname='test',
                        process_started_at=datetime_now(),
                        remove_util=cephadm_module.to_remove_osds.rm_util
                        )

            cephadm_module.to_remove_osds.enqueue(osd_0)
            _get_weight.assert_called()

            # test that OSD is properly reweighted on removal
            cephadm_module.stop_remove_osds([0])
            _reweight_osd.assert_called_with(mock.ANY, osd_initial_weight)

            # add OSD back to queue and test normal removal queue processing
            cephadm_module.to_remove_osds.enqueue(osd_0)
            cephadm_module.to_remove_osds.process_removal_queue()
            assert cephadm_module.to_remove_osds == OSDRemovalQueue(cephadm_module)

            c = cephadm_module.remove_osds_status()
            out = wait(cephadm_module, c)
            assert out == []

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_rgw_update(self, cephadm_module):
        with with_host(cephadm_module, 'host1'):
            with with_host(cephadm_module, 'host2'):
                with with_service(cephadm_module, RGWSpec(service_id="foo", unmanaged=True)):
                    ps = PlacementSpec(hosts=['host1'], count=1)
                    c = cephadm_module.add_daemon(
                        RGWSpec(service_id="foo", placement=ps))
                    [out] = wait(cephadm_module, c)
                    match_glob(out, "Deployed rgw.foo.* on host 'host1'")

                    ps = PlacementSpec(hosts=['host1', 'host2'], count=2)
                    r = CephadmServe(cephadm_module)._apply_service(
                        RGWSpec(service_id="foo", placement=ps))
                    assert r

                    assert_rm_daemon(cephadm_module, 'rgw.foo', 'host1')
                    assert_rm_daemon(cephadm_module, 'rgw.foo', 'host2')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm(
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
        with with_host(cephadm_module, 'test'):
            CephadmServe(cephadm_module)._refresh_host_daemons('test')
            c = cephadm_module.list_daemons()
            wait(cephadm_module, c)
            c = cephadm_module.remove_daemons(['rgw.myrgw.myhost.myid'])
            out = wait(cephadm_module, c)
            assert out == ["Removed rgw.myrgw.myhost.myid from host 'test'"]

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_remove_duplicate_osds(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'host1'):
            with with_host(cephadm_module, 'host2'):
                with with_osd_daemon(cephadm_module, _run_cephadm, 'host1', 1) as dd1:  # type: DaemonDescription
                    with with_osd_daemon(cephadm_module, _run_cephadm, 'host2', 1) as dd2:  # type: DaemonDescription
                        CephadmServe(cephadm_module)._check_for_moved_osds()
                        # both are in status "starting"
                        assert len(cephadm_module.cache.get_daemons()) == 2

                        dd1.status = DaemonDescriptionStatus.running
                        dd2.status = DaemonDescriptionStatus.error
                        cephadm_module.cache.update_host_daemons(dd1.hostname, {dd1.name(): dd1})
                        cephadm_module.cache.update_host_daemons(dd2.hostname, {dd2.name(): dd2})
                        CephadmServe(cephadm_module)._check_for_moved_osds()
                        assert len(cephadm_module.cache.get_daemons()) == 1

                        assert cephadm_module.events.get_for_daemon('osd.1') == [
                            OrchestratorEvent(mock.ANY, 'daemon', 'osd.1', 'INFO',
                                              "Deployed osd.1 on host 'host1'"),
                            OrchestratorEvent(mock.ANY, 'daemon', 'osd.1', 'INFO',
                                              "Deployed osd.1 on host 'host2'"),
                            OrchestratorEvent(mock.ANY, 'daemon', 'osd.1', 'INFO',
                                              "Removed duplicated daemon on host 'host2'"),
                        ]

                        with pytest.raises(AssertionError):
                            cephadm_module.assert_issued_mon_command({
                                'prefix': 'auth rm',
                                'entity': 'osd.1',
                            })

                cephadm_module.assert_issued_mon_command({
                    'prefix': 'auth rm',
                    'entity': 'osd.1',
                })

    @pytest.mark.parametrize(
        "spec",
        [
            ServiceSpec('crash'),
            ServiceSpec('prometheus'),
            ServiceSpec('grafana'),
            ServiceSpec('node-exporter'),
            ServiceSpec('alertmanager'),
            ServiceSpec('rbd-mirror'),
            ServiceSpec('cephfs-mirror'),
            ServiceSpec('mds', service_id='fsname'),
            RGWSpec(rgw_realm='realm', rgw_zone='zone'),
            RGWSpec(service_id="foo"),
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_daemon_add(self, spec: ServiceSpec, cephadm_module):
        unmanaged_spec = ServiceSpec.from_json(spec.to_json())
        unmanaged_spec.unmanaged = True
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, unmanaged_spec):
                with with_daemon(cephadm_module, spec, 'test'):
                    pass

    @pytest.mark.parametrize(
        "entity,success,spec",
        [
            ('mgr.x', True, ServiceSpec(
                service_type='mgr',
                placement=PlacementSpec(hosts=[HostPlacementSpec('test', '', 'x')], count=1),
                unmanaged=True)
            ),  # noqa: E124
            ('client.rgw.x', True, ServiceSpec(
                service_type='rgw',
                service_id='id',
                placement=PlacementSpec(hosts=[HostPlacementSpec('test', '', 'x')], count=1),
                unmanaged=True)
            ),  # noqa: E124
            ('client.nfs.x', True, ServiceSpec(
                service_type='nfs',
                service_id='id',
                placement=PlacementSpec(hosts=[HostPlacementSpec('test', '', 'x')], count=1),
                unmanaged=True)
            ),  # noqa: E124
            ('mon.', False, ServiceSpec(
                service_type='mon',
                placement=PlacementSpec(
                    hosts=[HostPlacementSpec('test', '127.0.0.0/24', 'x')], count=1),
                unmanaged=True)
            ),  # noqa: E124
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.services.nfs.NFSService.run_grace_tool", mock.MagicMock())
    @mock.patch("cephadm.services.nfs.NFSService.purge", mock.MagicMock())
    @mock.patch("cephadm.services.nfs.NFSService.create_rados_config_obj", mock.MagicMock())
    def test_daemon_add_fail(self, _run_cephadm, entity, success, spec, cephadm_module):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.side_effect = OrchestratorError('fail')
                with pytest.raises(OrchestratorError):
                    wait(cephadm_module, cephadm_module.add_daemon(spec))
                if success:
                    cephadm_module.assert_issued_mon_command({
                        'prefix': 'auth rm',
                        'entity': entity,
                    })
                else:
                    with pytest.raises(AssertionError):
                        cephadm_module.assert_issued_mon_command({
                            'prefix': 'auth rm',
                            'entity': entity,
                        })
                    assert cephadm_module.events.get_for_service(spec.service_name()) == [
                        OrchestratorEvent(mock.ANY, 'service', spec.service_name(), 'INFO',
                                          "service was created"),
                        OrchestratorEvent(mock.ANY, 'service', spec.service_name(), 'ERROR',
                                          "fail"),
                    ]

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_daemon_place_fail_health_warning(self, _run_cephadm, cephadm_module):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            _run_cephadm.side_effect = OrchestratorError('fail')
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            r = CephadmServe(cephadm_module)._apply_service(ServiceSpec('mgr', placement=ps))
            assert not r
            assert cephadm_module.health_checks.get('CEPHADM_DAEMON_PLACE_FAIL') is not None
            assert cephadm_module.health_checks['CEPHADM_DAEMON_PLACE_FAIL']['count'] == 1
            assert 'Failed to place 1 daemon(s)' in cephadm_module.health_checks[
                'CEPHADM_DAEMON_PLACE_FAIL']['summary']
            assert 'Failed while placing mgr.a on test: fail' in cephadm_module.health_checks[
                'CEPHADM_DAEMON_PLACE_FAIL']['detail']

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_apply_spec_fail_health_warning(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            CephadmServe(cephadm_module)._apply_all_services()
            ps = PlacementSpec(hosts=['fail'], count=1)
            r = CephadmServe(cephadm_module)._apply_service(ServiceSpec('mgr', placement=ps))
            assert not r
            assert cephadm_module.apply_spec_fails
            assert cephadm_module.health_checks.get('CEPHADM_APPLY_SPEC_FAIL') is not None
            assert cephadm_module.health_checks['CEPHADM_APPLY_SPEC_FAIL']['count'] == 1
            assert 'Failed to apply 1 service(s)' in cephadm_module.health_checks[
                'CEPHADM_APPLY_SPEC_FAIL']['summary']

    @mock.patch("cephadm.module.CephadmOrchestrator.get_foreign_ceph_option")
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.module.HostCache.save_host_devices")
    def test_invalid_config_option_health_warning(self, _save_devs, _run_cephadm, get_foreign_ceph_option, cephadm_module: CephadmOrchestrator):
        _save_devs.return_value = None
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            get_foreign_ceph_option.side_effect = KeyError
            CephadmServe(cephadm_module)._apply_service_config(
                ServiceSpec('mgr', placement=ps, config={'test': 'foo'}))
            assert cephadm_module.health_checks.get('CEPHADM_INVALID_CONFIG_OPTION') is not None
            assert cephadm_module.health_checks['CEPHADM_INVALID_CONFIG_OPTION']['count'] == 1
            assert 'Ignoring 1 invalid config option(s)' in cephadm_module.health_checks[
                'CEPHADM_INVALID_CONFIG_OPTION']['summary']
            assert 'Ignoring invalid mgr config option test' in cephadm_module.health_checks[
                'CEPHADM_INVALID_CONFIG_OPTION']['detail']

    @mock.patch("cephadm.module.CephadmOrchestrator.get_foreign_ceph_option")
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.module.CephadmOrchestrator.set_store")
    def test_save_devices(self, _set_store, _run_cephadm, _get_foreign_ceph_option, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        entry_size = 65536  # default 64k size
        _get_foreign_ceph_option.return_value = entry_size

        class FakeDev():
            def __init__(self, c: str = 'a'):
                # using 1015 here makes the serialized string exactly 1024 bytes if c is one char
                self.content = {c: c * 1015}
                self.path = 'dev/vdc'

            def to_json(self):
                return self.content

            def from_json(self, stuff):
                return json.loads(stuff)

        def byte_len(s):
            return len(s.encode('utf-8'))

        with with_host(cephadm_module, 'test'):
            fake_devices = [FakeDev()] * 100  # should be ~100k
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) > entry_size
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) < entry_size * 2
            cephadm_module.cache.update_host_devices('test', fake_devices)
            cephadm_module.cache.save_host_devices('test')
            expected_calls = [
                mock.call('host.test.devices.0', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 34], 'entries': 3})),
                mock.call('host.test.devices.1', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 34]})),
                mock.call('host.test.devices.2', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 32]})),
            ]
            _set_store.assert_has_calls(expected_calls)

            fake_devices = [FakeDev()] * 300  # should be ~300k
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) > entry_size * 4
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) < entry_size * 5
            cephadm_module.cache.update_host_devices('test', fake_devices)
            cephadm_module.cache.save_host_devices('test')
            expected_calls = [
                mock.call('host.test.devices.0', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 50], 'entries': 6})),
                mock.call('host.test.devices.1', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 50]})),
                mock.call('host.test.devices.2', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 50]})),
                mock.call('host.test.devices.3', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 50]})),
                mock.call('host.test.devices.4', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 50]})),
                mock.call('host.test.devices.5', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 50]})),
            ]
            _set_store.assert_has_calls(expected_calls)

            fake_devices = [FakeDev()] * 62  # should be ~62k, just under cache size
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) < entry_size
            cephadm_module.cache.update_host_devices('test', fake_devices)
            cephadm_module.cache.save_host_devices('test')
            expected_calls = [
                mock.call('host.test.devices.0', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 62], 'entries': 1})),
            ]
            _set_store.assert_has_calls(expected_calls)

            # should be ~64k but just over so it requires more entries
            fake_devices = [FakeDev()] * 64
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) > entry_size
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) < entry_size * 2
            cephadm_module.cache.update_host_devices('test', fake_devices)
            cephadm_module.cache.save_host_devices('test')
            expected_calls = [
                mock.call('host.test.devices.0', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 22], 'entries': 3})),
                mock.call('host.test.devices.1', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 22]})),
                mock.call('host.test.devices.2', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev()] * 20]})),
            ]
            _set_store.assert_has_calls(expected_calls)

            # test for actual content being correct using differing devices
            entry_size = 3072
            _get_foreign_ceph_option.return_value = entry_size
            fake_devices = [FakeDev('a'), FakeDev('b'), FakeDev('c'), FakeDev('d'), FakeDev('e')]
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) > entry_size
            assert byte_len(json.dumps([d.to_json() for d in fake_devices])) < entry_size * 2
            cephadm_module.cache.update_host_devices('test', fake_devices)
            cephadm_module.cache.save_host_devices('test')
            expected_calls = [
                mock.call('host.test.devices.0', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev('a'), FakeDev('b')]], 'entries': 3})),
                mock.call('host.test.devices.1', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev('c'), FakeDev('d')]]})),
                mock.call('host.test.devices.2', json.dumps(
                    {'devices': [d.to_json() for d in [FakeDev('e')]]})),
            ]
            _set_store.assert_has_calls(expected_calls)

    @mock.patch("cephadm.module.CephadmOrchestrator.get_store")
    def test_load_devices(self, _get_store, cephadm_module: CephadmOrchestrator):
        def _fake_store(key):
            if key == 'host.test.devices.0':
                return json.dumps({'devices': [d.to_json() for d in [Device('/path')] * 9], 'entries': 3})
            elif key == 'host.test.devices.1':
                return json.dumps({'devices': [d.to_json() for d in [Device('/path')] * 7]})
            elif key == 'host.test.devices.2':
                return json.dumps({'devices': [d.to_json() for d in [Device('/path')] * 4]})
            else:
                raise Exception(f'Get store with unexpected value {key}')

        _get_store.side_effect = _fake_store
        devs = cephadm_module.cache.load_host_devices('test')
        assert devs == [Device('/path')] * 20

    @mock.patch("cephadm.module.Inventory.__contains__")
    def test_check_stray_host_cache_entry(self, _contains, cephadm_module: CephadmOrchestrator):
        def _fake_inv(key):
            if key in ['host1', 'node02', 'host.something.com']:
                return True
            return False

        _contains.side_effect = _fake_inv
        assert cephadm_module.cache._get_host_cache_entry_status('host1') == HostCacheStatus.host
        assert cephadm_module.cache._get_host_cache_entry_status(
            'host.something.com') == HostCacheStatus.host
        assert cephadm_module.cache._get_host_cache_entry_status(
            'node02.devices.37') == HostCacheStatus.devices
        assert cephadm_module.cache._get_host_cache_entry_status(
            'host.something.com.devices.0') == HostCacheStatus.devices
        assert cephadm_module.cache._get_host_cache_entry_status('hostXXX') == HostCacheStatus.stray
        assert cephadm_module.cache._get_host_cache_entry_status(
            'host.nothing.com') == HostCacheStatus.stray

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.services.nfs.NFSService.run_grace_tool", mock.MagicMock())
    @mock.patch("cephadm.services.nfs.NFSService.purge", mock.MagicMock())
    @mock.patch("cephadm.services.nfs.NFSService.create_rados_config_obj", mock.MagicMock())
    def test_nfs(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = NFSServiceSpec(
                service_id='name',
                placement=ps)
            unmanaged_spec = ServiceSpec.from_json(spec.to_json())
            unmanaged_spec.unmanaged = True
            with with_service(cephadm_module, unmanaged_spec):
                c = cephadm_module.add_daemon(spec)
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed nfs.name.* on host 'test'")

                assert_rm_daemon(cephadm_module, 'nfs.name.test', 'test')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch("subprocess.run", None)
    @mock.patch("cephadm.module.CephadmOrchestrator.rados", mock.MagicMock())
    @mock.patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '1::4')
    def test_iscsi(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            spec = IscsiServiceSpec(
                service_id='name',
                pool='pool',
                api_user='user',
                api_password='password',
                placement=ps)
            unmanaged_spec = ServiceSpec.from_json(spec.to_json())
            unmanaged_spec.unmanaged = True
            with with_service(cephadm_module, unmanaged_spec):

                c = cephadm_module.add_daemon(spec)
                [out] = wait(cephadm_module, c)
                match_glob(out, "Deployed iscsi.name.* on host 'test'")

                assert_rm_daemon(cephadm_module, 'iscsi.name.test', 'test')

    @pytest.mark.parametrize(
        "on_bool",
        [
            True,
            False
        ]
    )
    @pytest.mark.parametrize(
        "fault_ident",
        [
            'fault',
            'ident'
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_blink_device_light(self, _run_cephadm, on_bool, fault_ident, cephadm_module):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            c = cephadm_module.blink_device_light(fault_ident, on_bool, [('test', '', 'dev')])
            on_off = 'on' if on_bool else 'off'
            assert wait(cephadm_module, c) == [f'Set {fault_ident} light for test: {on_off}']
            _run_cephadm.assert_called_with('test', 'osd', 'shell', [
                                            '--', 'lsmcli', f'local-disk-{fault_ident}-led-{on_off}', '--path', 'dev'], error_ok=True)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_blink_device_light_custom(self, _run_cephadm, cephadm_module):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            cephadm_module.set_store('blink_device_light_cmd', 'echo hello')
            c = cephadm_module.blink_device_light('ident', True, [('test', '', '/dev/sda')])
            assert wait(cephadm_module, c) == ['Set ident light for test: on']
            _run_cephadm.assert_called_with('test', 'osd', 'shell', [
                                            '--', 'echo', 'hello'], error_ok=True)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_blink_device_light_custom_per_host(self, _run_cephadm, cephadm_module):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'mgr0'):
            cephadm_module.set_store('mgr0/blink_device_light_cmd',
                                     'xyz --foo --{{ ident_fault }}={{\'on\' if on else \'off\'}} \'{{ path or dev }}\'')
            c = cephadm_module.blink_device_light(
                'fault', True, [('mgr0', 'SanDisk_X400_M.2_2280_512GB_162924424784', '')])
            assert wait(cephadm_module, c) == [
                'Set fault light for mgr0:SanDisk_X400_M.2_2280_512GB_162924424784 on']
            _run_cephadm.assert_called_with('mgr0', 'osd', 'shell', [
                '--', 'xyz', '--foo', '--fault=on', 'SanDisk_X400_M.2_2280_512GB_162924424784'
            ], error_ok=True)

    @pytest.mark.parametrize(
        "spec, meth",
        [
            (ServiceSpec('mgr'), CephadmOrchestrator.apply_mgr),
            (ServiceSpec('crash'), CephadmOrchestrator.apply_crash),
            (ServiceSpec('prometheus'), CephadmOrchestrator.apply_prometheus),
            (ServiceSpec('grafana'), CephadmOrchestrator.apply_grafana),
            (ServiceSpec('node-exporter'), CephadmOrchestrator.apply_node_exporter),
            (ServiceSpec('alertmanager'), CephadmOrchestrator.apply_alertmanager),
            (ServiceSpec('rbd-mirror'), CephadmOrchestrator.apply_rbd_mirror),
            (ServiceSpec('cephfs-mirror'), CephadmOrchestrator.apply_rbd_mirror),
            (ServiceSpec('mds', service_id='fsname'), CephadmOrchestrator.apply_mds),
            (ServiceSpec(
                'mds', service_id='fsname',
                placement=PlacementSpec(
                    hosts=[HostPlacementSpec(
                        hostname='test',
                        name='fsname',
                        network=''
                    )]
                )
            ), CephadmOrchestrator.apply_mds),
            (RGWSpec(service_id='foo'), CephadmOrchestrator.apply_rgw),
            (RGWSpec(
                service_id='bar',
                rgw_realm='realm', rgw_zone='zone',
                placement=PlacementSpec(
                    hosts=[HostPlacementSpec(
                        hostname='test',
                        name='bar',
                        network=''
                    )]
                )
            ), CephadmOrchestrator.apply_rgw),
            (NFSServiceSpec(
                service_id='name',
            ), CephadmOrchestrator.apply_nfs),
            (IscsiServiceSpec(
                service_id='name',
                pool='pool',
                api_user='user',
                api_password='password'
            ), CephadmOrchestrator.apply_iscsi),
            (CustomContainerSpec(
                service_id='hello-world',
                image='docker.io/library/hello-world:latest',
                uid=65534,
                gid=65534,
                dirs=['foo/bar'],
                files={
                    'foo/bar/xyz.conf': 'aaa\nbbb'
                },
                bind_mounts=[[
                    'type=bind',
                    'source=lib/modules',
                    'destination=/lib/modules',
                    'ro=true'
                ]],
                volume_mounts={
                    'foo/bar': '/foo/bar:Z'
                },
                args=['--no-healthcheck'],
                envs=['SECRET=password'],
                ports=[8080, 8443]
            ), CephadmOrchestrator.apply_container),
        ]
    )
    @mock.patch("subprocess.run", None)
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.services.nfs.NFSService.run_grace_tool", mock.MagicMock())
    @mock.patch("cephadm.services.nfs.NFSService.create_rados_config_obj", mock.MagicMock())
    @mock.patch("cephadm.services.nfs.NFSService.purge", mock.MagicMock())
    @mock.patch("subprocess.run", mock.MagicMock())
    def test_apply_save(self, spec: ServiceSpec, meth, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec, meth, 'test'):
                pass

    @pytest.mark.parametrize(
        "spec, raise_exception, msg",
        [
            # Valid retention_time values (valid units: 'y', 'w', 'd', 'h', 'm', 's')
            (PrometheusSpec(retention_time='1y'), False, ''),
            (PrometheusSpec(retention_time=' 10w '), False, ''),
            (PrometheusSpec(retention_time=' 1348d'), False, ''),
            (PrometheusSpec(retention_time='2000h '), False, ''),
            (PrometheusSpec(retention_time='173847m'), False, ''),
            (PrometheusSpec(retention_time='200s'), False, ''),
            (PrometheusSpec(retention_time='  '), False, ''),  # default value will be used

            # Invalid retention_time values
            (PrometheusSpec(retention_time='100k'), True, '^Invalid retention time'),     # invalid unit
            (PrometheusSpec(retention_time='10'), True, '^Invalid retention time'),       # no unit
            (PrometheusSpec(retention_time='100.00y'), True, '^Invalid retention time'),  # invalid value and valid unit
            (PrometheusSpec(retention_time='100.00k'), True, '^Invalid retention time'),  # invalid value and invalid unit
            (PrometheusSpec(retention_time='---'), True, '^Invalid retention time'),      # invalid value

            # Valid retention_size values (valid units: 'B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB')
            (PrometheusSpec(retention_size='123456789B'), False, ''),
            (PrometheusSpec(retention_size=' 200KB'), False, ''),
            (PrometheusSpec(retention_size='99999MB '), False, ''),
            (PrometheusSpec(retention_size=' 10GB '), False, ''),
            (PrometheusSpec(retention_size='100TB'), False, ''),
            (PrometheusSpec(retention_size='500PB'), False, ''),
            (PrometheusSpec(retention_size='200EB'), False, ''),
            (PrometheusSpec(retention_size='  '), False, ''),  # default value will be used

            # Invalid retention_size values
            (PrometheusSpec(retention_size='100b'), True, '^Invalid retention size'),      # invalid unit (case sensitive)
            (PrometheusSpec(retention_size='333kb'), True, '^Invalid retention size'),     # invalid unit (case sensitive)
            (PrometheusSpec(retention_size='2000'), True, '^Invalid retention size'),      # no unit
            (PrometheusSpec(retention_size='200.00PB'), True, '^Invalid retention size'),  # invalid value and valid unit
            (PrometheusSpec(retention_size='400.B'), True, '^Invalid retention size'),     # invalid value and invalid unit
            (PrometheusSpec(retention_size='10.000s'), True, '^Invalid retention size'),   # invalid value and invalid unit
            (PrometheusSpec(retention_size='...'), True, '^Invalid retention size'),       # invalid value

            # valid retention_size and valid retention_time
            (PrometheusSpec(retention_time='1y', retention_size='100GB'), False, ''),
            # invalid retention_time and valid retention_size
            (PrometheusSpec(retention_time='1j', retention_size='100GB'), True, '^Invalid retention time'),
            # valid retention_time and invalid retention_size
            (PrometheusSpec(retention_time='1y', retention_size='100gb'), True, '^Invalid retention size'),
            # valid retention_time and invalid retention_size
            (PrometheusSpec(retention_time='1y', retention_size='100gb'), True, '^Invalid retention size'),
            # invalid retention_time and invalid retention_size
            (PrometheusSpec(retention_time='1i', retention_size='100gb'), True, '^Invalid retention time'),
        ])
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_apply_prometheus(self, spec: PrometheusSpec, raise_exception: bool, msg: str, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'test'):
            if not raise_exception:
                cephadm_module._apply(spec)
            else:
                with pytest.raises(OrchestratorError, match=msg):
                    cephadm_module._apply(spec)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_mds_config_purge(self, cephadm_module: CephadmOrchestrator):
        spec = MDSSpec('mds', service_id='fsname', config={'test': 'foo'})
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec, host='test'):
                ret, out, err = cephadm_module.check_mon_command({
                    'prefix': 'config get',
                    'who': spec.service_name(),
                    'key': 'mds_join_fs',
                })
                assert out == 'fsname'
            ret, out, err = cephadm_module.check_mon_command({
                'prefix': 'config get',
                'who': spec.service_name(),
                'key': 'mds_join_fs',
            })
            assert not out

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.services.cephadmservice.CephadmService.ok_to_stop")
    def test_daemon_ok_to_stop(self, ok_to_stop, cephadm_module: CephadmOrchestrator):
        spec = MDSSpec(
            'mds',
            service_id='fsname',
            placement=PlacementSpec(hosts=['host1', 'host2']),
            config={'test': 'foo'}
        )
        with with_host(cephadm_module, 'host1'), with_host(cephadm_module, 'host2'):
            c = cephadm_module.apply_mds(spec)
            out = wait(cephadm_module, c)
            match_glob(out, "Scheduled mds.fsname update...")
            CephadmServe(cephadm_module)._apply_all_services()

            [daemon] = cephadm_module.cache.daemons['host1'].keys()

            spec.placement.set_hosts(['host2'])

            ok_to_stop.side_effect = False

            c = cephadm_module.apply_mds(spec)
            out = wait(cephadm_module, c)
            match_glob(out, "Scheduled mds.fsname update...")
            CephadmServe(cephadm_module)._apply_all_services()

            ok_to_stop.assert_called_with([daemon[4:]], force=True)

            assert_rm_daemon(cephadm_module, spec.service_name(), 'host1')  # verifies ok-to-stop
            assert_rm_daemon(cephadm_module, spec.service_name(), 'host2')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_dont_touch_offline_or_maintenance_host_daemons(self, cephadm_module):
        # test daemons on offline/maint hosts not removed when applying specs
        # test daemons not added to hosts in maint/offline state
        with with_host(cephadm_module, 'test1'):
            with with_host(cephadm_module, 'test2'):
                with with_host(cephadm_module, 'test3'):
                    with with_service(cephadm_module, ServiceSpec('mgr', placement=PlacementSpec(host_pattern='*'))):
                        # should get a mgr on all 3 hosts
                        # CephadmServe(cephadm_module)._apply_all_services()
                        assert len(cephadm_module.cache.get_daemons_by_type('mgr')) == 3

                        # put one host in offline state and one host in maintenance state
                        cephadm_module.offline_hosts = {'test2'}
                        cephadm_module.inventory._inventory['test3']['status'] = 'maintenance'
                        cephadm_module.inventory.save()

                        # being in offline/maint mode should disqualify hosts from being
                        # candidates for scheduling
                        assert cephadm_module.cache.is_host_schedulable('test2')
                        assert cephadm_module.cache.is_host_schedulable('test3')

                        assert cephadm_module.cache.is_host_unreachable('test2')
                        assert cephadm_module.cache.is_host_unreachable('test3')

                        with with_service(cephadm_module, ServiceSpec('crash', placement=PlacementSpec(host_pattern='*'))):
                            # re-apply services. No mgr should be removed from maint/offline hosts
                            # crash daemon should only be on host not in maint/offline mode
                            CephadmServe(cephadm_module)._apply_all_services()
                            assert len(cephadm_module.cache.get_daemons_by_type('mgr')) == 3
                            assert len(cephadm_module.cache.get_daemons_by_type('crash')) == 1

                        cephadm_module.offline_hosts = {}

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.CephadmOrchestrator._host_ok_to_stop")
    @mock.patch("cephadm.module.HostCache.get_daemon_types")
    @mock.patch("cephadm.module.HostCache.get_hosts")
    def test_maintenance_enter_success(self, _hosts, _get_daemon_types, _host_ok, _run_cephadm, cephadm_module: CephadmOrchestrator):
        hostname = 'host1'
        _run_cephadm.side_effect = async_side_effect(
            ([''], ['something\nsuccess - systemd target xxx disabled'], 0))
        _host_ok.return_value = 0, 'it is okay'
        _get_daemon_types.return_value = ['crash']
        _hosts.return_value = [hostname, 'other_host']
        cephadm_module.inventory.add_host(HostSpec(hostname))
        # should not raise an error
        retval = cephadm_module.enter_host_maintenance(hostname)
        assert retval.result_str().startswith('Daemons for Ceph cluster')
        assert not retval.exception_str
        assert cephadm_module.inventory._inventory[hostname]['status'] == 'maintenance'

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.CephadmOrchestrator._host_ok_to_stop")
    @mock.patch("cephadm.module.HostCache.get_daemon_types")
    @mock.patch("cephadm.module.HostCache.get_hosts")
    def test_maintenance_enter_failure(self, _hosts, _get_daemon_types, _host_ok, _run_cephadm, cephadm_module: CephadmOrchestrator):
        hostname = 'host1'
        _run_cephadm.side_effect = async_side_effect(
            ([''], ['something\nfailed - disable the target'], 0))
        _host_ok.return_value = 0, 'it is okay'
        _get_daemon_types.return_value = ['crash']
        _hosts.return_value = [hostname, 'other_host']
        cephadm_module.inventory.add_host(HostSpec(hostname))

        with pytest.raises(OrchestratorError, match='Failed to place host1 into maintenance for cluster fsid'):
            cephadm_module.enter_host_maintenance(hostname)

        assert not cephadm_module.inventory._inventory[hostname]['status']

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.CephadmOrchestrator._host_ok_to_stop")
    @mock.patch("cephadm.module.HostCache.get_daemon_types")
    @mock.patch("cephadm.module.HostCache.get_hosts")
    def test_maintenance_enter_i_really_mean_it(self, _hosts, _get_daemon_types, _host_ok, _run_cephadm, cephadm_module: CephadmOrchestrator):
        hostname = 'host1'
        err_str = 'some kind of error'
        _run_cephadm.side_effect = async_side_effect(
            ([''], ['something\nfailed - disable the target'], 0))
        _host_ok.return_value = 1, err_str
        _get_daemon_types.return_value = ['mon']
        _hosts.return_value = [hostname, 'other_host']
        cephadm_module.inventory.add_host(HostSpec(hostname))

        with pytest.raises(OrchestratorError, match=err_str):
            cephadm_module.enter_host_maintenance(hostname)
        assert not cephadm_module.inventory._inventory[hostname]['status']

        with pytest.raises(OrchestratorError, match=err_str):
            cephadm_module.enter_host_maintenance(hostname, force=True)
        assert not cephadm_module.inventory._inventory[hostname]['status']

        retval = cephadm_module.enter_host_maintenance(hostname, force=True, yes_i_really_mean_it=True)
        assert retval.result_str().startswith('Daemons for Ceph cluster')
        assert not retval.exception_str
        assert cephadm_module.inventory._inventory[hostname]['status'] == 'maintenance'

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.module.HostCache.get_daemon_types")
    @mock.patch("cephadm.module.HostCache.get_hosts")
    def test_maintenance_exit_success(self, _hosts, _get_daemon_types, _run_cephadm, cephadm_module: CephadmOrchestrator):
        hostname = 'host1'
        _run_cephadm.side_effect = async_side_effect(([''], [
            'something\nsuccess - systemd target xxx enabled and started'], 0))
        _get_daemon_types.return_value = ['crash']
        _hosts.return_value = [hostname, 'other_host']
        cephadm_module.inventory.add_host(HostSpec(hostname, status='maintenance'))
        # should not raise an error
        retval = cephadm_module.exit_host_maintenance(hostname)
        assert retval.result_str().startswith('Ceph cluster')
        assert not retval.exception_str
        assert not cephadm_module.inventory._inventory[hostname]['status']

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.module.HostCache.get_daemon_types")
    @mock.patch("cephadm.module.HostCache.get_hosts")
    def test_maintenance_exit_failure(self, _hosts, _get_daemon_types, _run_cephadm, cephadm_module: CephadmOrchestrator):
        hostname = 'host1'
        _run_cephadm.side_effect = async_side_effect(
            ([''], ['something\nfailed - unable to enable the target'], 0))
        _get_daemon_types.return_value = ['crash']
        _hosts.return_value = [hostname, 'other_host']
        cephadm_module.inventory.add_host(HostSpec(hostname, status='maintenance'))

        with pytest.raises(OrchestratorError, match='Failed to exit maintenance state for host host1, cluster fsid'):
            cephadm_module.exit_host_maintenance(hostname)

        assert cephadm_module.inventory._inventory[hostname]['status'] == 'maintenance'

    @mock.patch("cephadm.ssh.SSHManager._remote_connection")
    @mock.patch("cephadm.ssh.SSHManager._execute_command")
    @mock.patch("cephadm.ssh.SSHManager._check_execute_command")
    @mock.patch("cephadm.ssh.SSHManager._write_remote_file")
    def test_etc_ceph(self, _write_file, check_execute_command, execute_command, remote_connection, cephadm_module):
        _write_file.side_effect = async_side_effect(None)
        check_execute_command.side_effect = async_side_effect('')
        execute_command.side_effect = async_side_effect(('{}', '', 0))
        remote_connection.side_effect = async_side_effect(mock.Mock())

        assert cephadm_module.manage_etc_ceph_ceph_conf is False

        with with_host(cephadm_module, 'test'):
            assert '/etc/ceph/ceph.conf' not in cephadm_module.cache.get_host_client_files('test')

        with with_host(cephadm_module, 'test'):
            cephadm_module.set_module_option('manage_etc_ceph_ceph_conf', True)
            cephadm_module.config_notify()
            assert cephadm_module.manage_etc_ceph_ceph_conf is True

            CephadmServe(cephadm_module)._write_all_client_files()
            # Make sure both ceph conf locations (default and per fsid) are called
            _write_file.assert_has_calls([mock.call('test', '/etc/ceph/ceph.conf', b'',
                                          0o644, 0, 0, None),
                                         mock.call('test', '/var/lib/ceph/fsid/config/ceph.conf', b'',
                                          0o644, 0, 0, None)]
                                         )
            ceph_conf_files = cephadm_module.cache.get_host_client_files('test')
            assert len(ceph_conf_files) == 2
            assert '/etc/ceph/ceph.conf' in ceph_conf_files
            assert '/var/lib/ceph/fsid/config/ceph.conf' in ceph_conf_files

            # set extra config and expect that we deploy another ceph.conf
            cephadm_module._set_extra_ceph_conf('[mon]\nk=v')
            CephadmServe(cephadm_module)._write_all_client_files()
            _write_file.assert_has_calls([mock.call('test',
                                                    '/etc/ceph/ceph.conf',
                                                    b'[mon]\nk=v\n', 0o644, 0, 0, None),
                                          mock.call('test',
                                                    '/var/lib/ceph/fsid/config/ceph.conf',
                                                    b'[mon]\nk=v\n', 0o644, 0, 0, None)])
            # reload
            cephadm_module.cache.last_client_files = {}
            cephadm_module.cache.load()

            ceph_conf_files = cephadm_module.cache.get_host_client_files('test')
            assert len(ceph_conf_files) == 2
            assert '/etc/ceph/ceph.conf' in ceph_conf_files
            assert '/var/lib/ceph/fsid/config/ceph.conf' in ceph_conf_files

            # Make sure, _check_daemons does a redeploy due to monmap change:
            f1_before_digest = cephadm_module.cache.get_host_client_files('test')[
                '/etc/ceph/ceph.conf'][0]
            f2_before_digest = cephadm_module.cache.get_host_client_files(
                'test')['/var/lib/ceph/fsid/config/ceph.conf'][0]
            cephadm_module._set_extra_ceph_conf('[mon]\nk2=v2')
            CephadmServe(cephadm_module)._write_all_client_files()
            f1_after_digest = cephadm_module.cache.get_host_client_files('test')[
                '/etc/ceph/ceph.conf'][0]
            f2_after_digest = cephadm_module.cache.get_host_client_files(
                'test')['/var/lib/ceph/fsid/config/ceph.conf'][0]
            assert f1_before_digest != f1_after_digest
            assert f2_before_digest != f2_after_digest

    @mock.patch("cephadm.inventory.HostCache.get_host_client_files")
    def test_dont_write_client_files_to_unreachable_hosts(self, _get_client_files, cephadm_module):
        cephadm_module.inventory.add_host(HostSpec('host1', '1.2.3.1'))  # online
        cephadm_module.inventory.add_host(HostSpec('host2', '1.2.3.2'))  # maintenance
        cephadm_module.inventory.add_host(HostSpec('host3', '1.2.3.3'))  # offline

        # mark host2 as maintenance and host3 as offline
        cephadm_module.inventory._inventory['host2']['status'] = 'maintenance'
        cephadm_module.offline_hosts.add('host3')

        # verify host2 and host3 are correctly marked as unreachable but host1 is not
        assert not cephadm_module.cache.is_host_unreachable('host1')
        assert cephadm_module.cache.is_host_unreachable('host2')
        assert cephadm_module.cache.is_host_unreachable('host3')

        _get_client_files.side_effect = Exception('Called _get_client_files')

        # with the online host, should call _get_client_files which
        # we have setup to raise an Exception
        with pytest.raises(Exception, match='Called _get_client_files'):
            CephadmServe(cephadm_module)._write_client_files({}, 'host1')

        # for the maintenance and offline host, _get_client_files should
        # not be called and it should just return immediately with nothing
        # having been raised
        CephadmServe(cephadm_module)._write_client_files({}, 'host2')
        CephadmServe(cephadm_module)._write_client_files({}, 'host3')

    def test_etc_ceph_init(self):
        with with_cephadm_module({'manage_etc_ceph_ceph_conf': True}) as m:
            assert m.manage_etc_ceph_ceph_conf is True

    @mock.patch("cephadm.CephadmOrchestrator.check_mon_command")
    @mock.patch("cephadm.CephadmOrchestrator.extra_ceph_conf")
    def test_extra_ceph_conf(self, _extra_ceph_conf, _check_mon_cmd, cephadm_module: CephadmOrchestrator):
        # settings put into the [global] section in the extra conf
        # need to be appended to existing [global] section in given
        # minimal ceph conf, but anything in another section (e.g. [mon])
        # needs to continue to be its own section

        # this is the conf "ceph generate-minimal-conf" will return in this test
        _check_mon_cmd.return_value = (0, """[global]
global_k1 = global_v1
global_k2 = global_v2
[mon]
mon_k1 = mon_v1
[osd]
osd_k1 = osd_v1
osd_k2 = osd_v2
""", '')

        # test with extra ceph conf that has some of the sections from minimal conf
        _extra_ceph_conf.return_value = CephadmOrchestrator.ExtraCephConf(conf="""[mon]
mon_k2 = mon_v2
[global]
global_k3 = global_v3
""", last_modified=datetime_now())

        expected_combined_conf = """[global]
global_k1 = global_v1
global_k2 = global_v2
global_k3 = global_v3

[mon]
mon_k1 = mon_v1
mon_k2 = mon_v2

[osd]
osd_k1 = osd_v1
osd_k2 = osd_v2
"""

        assert cephadm_module.get_minimal_ceph_conf() == expected_combined_conf

    def test_client_keyrings_special_host_labels(self, cephadm_module):
        cephadm_module.inventory.add_host(HostSpec('host1', labels=['keyring1']))
        cephadm_module.inventory.add_host(HostSpec('host2', labels=['keyring1', SpecialHostLabels.DRAIN_DAEMONS]))
        cephadm_module.inventory.add_host(HostSpec('host3', labels=['keyring1', SpecialHostLabels.DRAIN_DAEMONS, SpecialHostLabels.DRAIN_CONF_KEYRING]))
        # hosts need to be marked as having had refresh to be available for placement
        # so "refresh" with empty daemon list
        cephadm_module.cache.update_host_daemons('host1', {})
        cephadm_module.cache.update_host_daemons('host2', {})
        cephadm_module.cache.update_host_daemons('host3', {})

        assert 'host1' in [h.hostname for h in cephadm_module.cache.get_conf_keyring_available_hosts()]
        assert 'host2' in [h.hostname for h in cephadm_module.cache.get_conf_keyring_available_hosts()]
        assert 'host3' not in [h.hostname for h in cephadm_module.cache.get_conf_keyring_available_hosts()]

        assert 'host1' not in [h.hostname for h in cephadm_module.cache.get_conf_keyring_draining_hosts()]
        assert 'host2' not in [h.hostname for h in cephadm_module.cache.get_conf_keyring_draining_hosts()]
        assert 'host3' in [h.hostname for h in cephadm_module.cache.get_conf_keyring_draining_hosts()]

        cephadm_module.keys.update(ClientKeyringSpec('keyring1', PlacementSpec(label='keyring1')))

        with mock.patch("cephadm.module.CephadmOrchestrator.mon_command") as _mon_cmd:
            _mon_cmd.return_value = (0, 'real-keyring', '')
            client_files = CephadmServe(cephadm_module)._calc_client_files()
            assert 'host1' in client_files.keys()
            assert '/etc/ceph/ceph.keyring1.keyring' in client_files['host1'].keys()
            assert 'host2' in client_files.keys()
            assert '/etc/ceph/ceph.keyring1.keyring' in client_files['host2'].keys()
            assert 'host3' not in client_files.keys()

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_registry_login(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        def check_registry_credentials(url, username, password):
            assert json.loads(cephadm_module.get_store('registry_credentials')) == {
                'url': url, 'username': username, 'password': password}

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            # test successful login with valid args
            code, out, err = cephadm_module.registry_login('test-url', 'test-user', 'test-password')
            assert out == 'registry login scheduled'
            assert err == ''
            check_registry_credentials('test-url', 'test-user', 'test-password')

            # test bad login attempt with invalid args
            code, out, err = cephadm_module.registry_login('bad-args')
            assert err == ("Invalid arguments. Please provide arguments <url> <username> <password> "
                           "or -i <login credentials json file>")
            check_registry_credentials('test-url', 'test-user', 'test-password')

            # test bad login using invalid json file
            code, out, err = cephadm_module.registry_login(
                None, None, None, '{"bad-json": "bad-json"}')
            assert err == ("json provided for custom registry login did not include all necessary fields. "
                           "Please setup json file as\n"
                           "{\n"
                           " \"url\": \"REGISTRY_URL\",\n"
                           " \"username\": \"REGISTRY_USERNAME\",\n"
                           " \"password\": \"REGISTRY_PASSWORD\"\n"
                           "}\n")
            check_registry_credentials('test-url', 'test-user', 'test-password')

            # test  good login using valid json file
            good_json = ("{\"url\": \"" + "json-url" + "\", \"username\": \"" + "json-user" + "\", "
                         " \"password\": \"" + "json-pass" + "\"}")
            code, out, err = cephadm_module.registry_login(None, None, None, good_json)
            assert out == 'registry login scheduled'
            assert err == ''
            check_registry_credentials('json-url', 'json-user', 'json-pass')

            # test bad login where args are valid but login command fails
            _run_cephadm.side_effect = async_side_effect(('{}', 'error', 1))
            code, out, err = cephadm_module.registry_login('fail-url', 'fail-user', 'fail-password')
            assert err == 'Host test failed to login to fail-url as fail-user with given password'
            check_registry_credentials('json-url', 'json-user', 'json-pass')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm(json.dumps({
        'image_id': 'image_id',
                    'repo_digests': ['image@repo_digest'],
    })))
    @pytest.mark.parametrize("use_repo_digest",
                             [
                                 False,
                                 True
                             ])
    def test_upgrade_run(self, use_repo_digest, cephadm_module: CephadmOrchestrator):
        cephadm_module.use_repo_digest = use_repo_digest

        with with_host(cephadm_module, 'test', refresh_hosts=False):
            cephadm_module.set_container_image('global', 'image')

            if use_repo_digest:

                CephadmServe(cephadm_module).convert_tags_to_repo_digest()

            _, image, _ = cephadm_module.check_mon_command({
                'prefix': 'config get',
                'who': 'global',
                'key': 'container_image',
            })
            if use_repo_digest:
                assert image == 'image@repo_digest'
            else:
                assert image == 'image'

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ceph_volume_no_filter_for_batch(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        error_message = """cephadm exited with an error code: 1, stderr:/usr/bin/podman:stderr usage: ceph-volume inventory [-h] [--format {plain,json,json-pretty}] [path]/usr/bin/podman:stderr ceph-volume inventory: error: unrecognized arguments: --filter-for-batch
Traceback (most recent call last):
  File "<stdin>", line 6112, in <module>
  File "<stdin>", line 1299, in _infer_fsid
  File "<stdin>", line 1382, in _infer_image
  File "<stdin>", line 3612, in command_ceph_volume
  File "<stdin>", line 1061, in call_throws"""

        with with_host(cephadm_module, 'test'):
            _run_cephadm.reset_mock()
            _run_cephadm.side_effect = OrchestratorError(error_message)

            s = CephadmServe(cephadm_module)._refresh_host_devices('test')
            assert s == 'host test `cephadm ceph-volume` failed: ' + error_message

            assert _run_cephadm.mock_calls == [
                mock.call('test', 'osd', 'ceph-volume',
                          ['--', 'inventory', '--format=json-pretty', '--filter-for-batch'], image='',
                          no_fsid=False, error_ok=False, log_output=False),
                mock.call('test', 'osd', 'ceph-volume',
                          ['--', 'inventory', '--format=json-pretty'], image='',
                          no_fsid=False, error_ok=False, log_output=False),
            ]

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_osd_activate_datadevice(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test', refresh_hosts=False):
            with with_osd_daemon(cephadm_module, _run_cephadm, 'test', 1):
                pass

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_osd_activate_datadevice_fail(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test', refresh_hosts=False):
            cephadm_module.mock_store_set('_ceph_get', 'osd_map', {
                'osds': [
                    {
                        'osd': 1,
                        'up_from': 0,
                        'uuid': 'uuid'
                    }
                ]
            })

            ceph_volume_lvm_list = {
                '1': [{
                    'tags': {
                        'ceph.cluster_fsid': cephadm_module._cluster_fsid,
                        'ceph.osd_fsid': 'uuid'
                    },
                    'type': 'data'
                }]
            }
            _run_cephadm.reset_mock(return_value=True, side_effect=True)

            async def _r_c(*args, **kwargs):
                if 'ceph-volume' in args:
                    return (json.dumps(ceph_volume_lvm_list), '', 0)
                else:
                    assert 'deploy' in args
                    raise OrchestratorError("let's fail somehow")
            _run_cephadm.side_effect = _r_c
            assert cephadm_module._osd_activate(
                ['test']).stderr == "let's fail somehow"
            with pytest.raises(AssertionError):
                cephadm_module.assert_issued_mon_command({
                    'prefix': 'auth rm',
                    'entity': 'osd.1',
                })

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_osd_activate_datadevice_dbdevice(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test', refresh_hosts=False):

            async def _ceph_volume_list(s, host, entity, cmd, **kwargs):
                logging.info(f'ceph-volume cmd: {cmd}')
                if 'raw' in cmd:
                    return json.dumps({
                        "21a4209b-f51b-4225-81dc-d2dca5b8b2f5": {
                            "ceph_fsid": "64c84f19-fe1d-452a-a731-ab19dc144aa8",
                            "device": "/dev/loop0",
                            "osd_id": 21,
                            "osd_uuid": "21a4209b-f51b-4225-81dc-d2dca5b8b2f5",
                            "type": "bluestore"
                        },
                    }), '', 0
                if 'lvm' in cmd:
                    return json.dumps({
                        '1': [{
                            'tags': {
                                'ceph.cluster_fsid': cephadm_module._cluster_fsid,
                                'ceph.osd_fsid': 'uuid'
                            },
                            'type': 'data'
                        }, {
                            'tags': {
                                'ceph.cluster_fsid': cephadm_module._cluster_fsid,
                                'ceph.osd_fsid': 'uuid'
                            },
                            'type': 'db'
                        }]
                    }), '', 0
                return '{}', '', 0

            with with_osd_daemon(cephadm_module, _run_cephadm, 'test', 1, ceph_volume_lvm_list=_ceph_volume_list):
                pass

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_osd_count(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        dg = DriveGroupSpec(service_id='', data_devices=DeviceSelection(all=True))
        with with_host(cephadm_module, 'test', refresh_hosts=False):
            with with_service(cephadm_module, dg, host='test'):
                with with_osd_daemon(cephadm_module, _run_cephadm, 'test', 1):
                    assert wait(cephadm_module, cephadm_module.describe_service())[0].size == 1

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_host_rm_last_admin(self, cephadm_module: CephadmOrchestrator):
        with pytest.raises(OrchestratorError):
            with with_host(cephadm_module, 'test', refresh_hosts=False, rm_with_force=False):
                cephadm_module.inventory.add_label('test', SpecialHostLabels.ADMIN)
                pass
            assert False
        with with_host(cephadm_module, 'test1', refresh_hosts=False, rm_with_force=True):
            with with_host(cephadm_module, 'test2', refresh_hosts=False, rm_with_force=False):
                cephadm_module.inventory.add_label('test2', SpecialHostLabels.ADMIN)

    @pytest.mark.parametrize("facts, settings, expected_value",
                             [
                                 # All options are available on all hosts
                                 (
                                     {
                                         "host1":
                                         {
                                             "sysctl_options":
                                             {
                                                 'opt1': 'val1',
                                                 'opt2': 'val2',
                                             }
                                         },
                                         "host2":
                                         {
                                             "sysctl_options":
                                             {
                                                 'opt1': '',
                                                 'opt2': '',
                                             }
                                         },
                                     },
                                     {'opt1', 'opt2'},  # settings
                                     {'host1': [], 'host2': []}  # expected_value
                                 ),
                                 # opt1 is missing on host 1, opt2 is missing on host2
                                 ({
                                     "host1":
                                     {
                                         "sysctl_options":
                                         {
                                             'opt2': '',
                                             'optX': '',
                                         }
                                     },
                                     "host2":
                                     {
                                         "sysctl_options":
                                         {
                                             'opt1': '',
                                             'opt3': '',
                                             'opt4': '',
                                         }
                                     },
                                 },
                                     {'opt1', 'opt2'},  # settings
                                     {'host1': ['opt1'], 'host2': ['opt2']}  # expected_value
                                 ),
                                 # All options are missing on all hosts
                                 ({
                                     "host1":
                                     {
                                         "sysctl_options":
                                         {
                                         }
                                     },
                                     "host2":
                                     {
                                         "sysctl_options":
                                         {
                                         }
                                     },
                                 },
                                     {'opt1', 'opt2'},  # settings
                                     {'host1': ['opt1', 'opt2'], 'host2': [
                                         'opt1', 'opt2']}  # expected_value
                                 ),
                             ]
                             )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_tuned_profiles_settings_validation(self, facts, settings, expected_value, cephadm_module):
        with with_host(cephadm_module, 'test'):
            spec = mock.Mock()
            spec.settings = sorted(settings)
            spec.placement.filter_matching_hostspecs = mock.Mock()
            spec.placement.filter_matching_hostspecs.return_value = ['host1', 'host2']
            cephadm_module.cache.facts = facts
            assert cephadm_module._validate_tunedprofile_settings(spec) == expected_value

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_tuned_profiles_validation(self, cephadm_module):
        with with_host(cephadm_module, 'test'):

            with pytest.raises(OrchestratorError, match="^Invalid placement specification.+"):
                spec = mock.Mock()
                spec.settings = {'a': 'b'}
                spec.placement = PlacementSpec(hosts=[])
                cephadm_module._validate_tuned_profile_spec(spec)

            with pytest.raises(OrchestratorError, match="Invalid spec: settings section cannot be empty."):
                spec = mock.Mock()
                spec.settings = {}
                spec.placement = PlacementSpec(hosts=['host1', 'host2'])
                cephadm_module._validate_tuned_profile_spec(spec)

            with pytest.raises(OrchestratorError, match="^Placement 'count' field is no supported .+"):
                spec = mock.Mock()
                spec.settings = {'a': 'b'}
                spec.placement = PlacementSpec(count=1)
                cephadm_module._validate_tuned_profile_spec(spec)

            with pytest.raises(OrchestratorError, match="^Placement 'count_per_host' field is no supported .+"):
                spec = mock.Mock()
                spec.settings = {'a': 'b'}
                spec.placement = PlacementSpec(count_per_host=1, label='foo')
                cephadm_module._validate_tuned_profile_spec(spec)

            with pytest.raises(OrchestratorError, match="^Found invalid host"):
                spec = mock.Mock()
                spec.settings = {'a': 'b'}
                spec.placement = PlacementSpec(hosts=['host1', 'host2'])
                cephadm_module.inventory = mock.Mock()
                cephadm_module.inventory.all_specs = mock.Mock(
                    return_value=[mock.Mock().hostname, mock.Mock().hostname])
                cephadm_module._validate_tuned_profile_spec(spec)

    def test_inventory_known_hostnames(self, cephadm_module):
        cephadm_module.inventory.add_host(HostSpec('host1', '1.2.3.1'))
        cephadm_module.inventory.add_host(HostSpec('host2', '1.2.3.2'))
        cephadm_module.inventory.add_host(HostSpec('host3.domain', '1.2.3.3'))
        cephadm_module.inventory.add_host(HostSpec('host4.domain', '1.2.3.4'))
        cephadm_module.inventory.add_host(HostSpec('host5', '1.2.3.5'))

        # update_known_hostname expects args to be <hostname, shortname, fqdn>
        # as are gathered from cephadm gather-facts. Although, passing the
        # names in the wrong order should actually have no effect on functionality
        cephadm_module.inventory.update_known_hostnames('host1', 'host1', 'host1.domain')
        cephadm_module.inventory.update_known_hostnames('host2.domain', 'host2', 'host2.domain')
        cephadm_module.inventory.update_known_hostnames('host3', 'host3', 'host3.domain')
        cephadm_module.inventory.update_known_hostnames('host4.domain', 'host4', 'host4.domain')
        cephadm_module.inventory.update_known_hostnames('host5', 'host5', 'host5')

        assert 'host1' in cephadm_module.inventory
        assert 'host1.domain' in cephadm_module.inventory
        assert cephadm_module.inventory.get_addr('host1') == '1.2.3.1'
        assert cephadm_module.inventory.get_addr('host1.domain') == '1.2.3.1'

        assert 'host2' in cephadm_module.inventory
        assert 'host2.domain' in cephadm_module.inventory
        assert cephadm_module.inventory.get_addr('host2') == '1.2.3.2'
        assert cephadm_module.inventory.get_addr('host2.domain') == '1.2.3.2'

        assert 'host3' in cephadm_module.inventory
        assert 'host3.domain' in cephadm_module.inventory
        assert cephadm_module.inventory.get_addr('host3') == '1.2.3.3'
        assert cephadm_module.inventory.get_addr('host3.domain') == '1.2.3.3'

        assert 'host4' in cephadm_module.inventory
        assert 'host4.domain' in cephadm_module.inventory
        assert cephadm_module.inventory.get_addr('host4') == '1.2.3.4'
        assert cephadm_module.inventory.get_addr('host4.domain') == '1.2.3.4'

        assert 'host4.otherdomain' not in cephadm_module.inventory
        with pytest.raises(OrchestratorError):
            cephadm_module.inventory.get_addr('host4.otherdomain')

        assert 'host5' in cephadm_module.inventory
        assert cephadm_module.inventory.get_addr('host5') == '1.2.3.5'
        with pytest.raises(OrchestratorError):
            cephadm_module.inventory.get_addr('host5.domain')

    def test_set_unmanaged(self, cephadm_module):
        cephadm_module.spec_store._specs['crash'] = ServiceSpec('crash', unmanaged=False)
        assert not cephadm_module.spec_store._specs['crash'].unmanaged
        cephadm_module.spec_store.set_unmanaged('crash', True)
        assert cephadm_module.spec_store._specs['crash'].unmanaged
        cephadm_module.spec_store.set_unmanaged('crash', False)
        assert not cephadm_module.spec_store._specs['crash'].unmanaged

    def test_async_timeout_handler(self, cephadm_module):
        cephadm_module.default_cephadm_command_timeout = 900

        async def _timeout():
            raise asyncio.TimeoutError

        with pytest.raises(OrchestratorError, match=r'Command timed out \(default 900 second timeout\)'):
            with cephadm_module.async_timeout_handler():
                cephadm_module.wait_async(_timeout())

        with pytest.raises(OrchestratorError, match=r'Command timed out on host hostA \(default 900 second timeout\)'):
            with cephadm_module.async_timeout_handler('hostA'):
                cephadm_module.wait_async(_timeout())

        with pytest.raises(OrchestratorError, match=r'Command "testing" timed out \(default 900 second timeout\)'):
            with cephadm_module.async_timeout_handler(cmd='testing'):
                cephadm_module.wait_async(_timeout())

        with pytest.raises(OrchestratorError, match=r'Command "testing" timed out on host hostB \(default 900 second timeout\)'):
            with cephadm_module.async_timeout_handler('hostB', 'testing'):
                cephadm_module.wait_async(_timeout())

        with pytest.raises(OrchestratorError, match=r'Command timed out \(non-default 111 second timeout\)'):
            with cephadm_module.async_timeout_handler(timeout=111):
                cephadm_module.wait_async(_timeout())

        with pytest.raises(OrchestratorError, match=r'Command "very slow" timed out on host hostC \(non-default 999 second timeout\)'):
            with cephadm_module.async_timeout_handler('hostC', 'very slow', 999):
                cephadm_module.wait_async(_timeout())

    @mock.patch("cephadm.CephadmOrchestrator.remove_osds")
    @mock.patch("cephadm.CephadmOrchestrator.add_host_label", lambda *a, **kw: None)
    @mock.patch("cephadm.inventory.HostCache.get_daemons_by_host", lambda *a, **kw: [])
    def test_host_drain_zap(self, _rm_osds, cephadm_module):
        # pass force=true in these tests to bypass _admin label check
        cephadm_module.drain_host('host1', force=True, zap_osd_devices=False)
        assert _rm_osds.called_with([], zap=False)

        cephadm_module.drain_host('host1', force=True, zap_osd_devices=True)
        assert _rm_osds.called_with([], zap=True)
