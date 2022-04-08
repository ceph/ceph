import json
import logging
from contextlib import contextmanager

import pytest

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from cephadm.serve import CephadmServe
from cephadm.services.osd import OSD, OSDRemovalQueue, OsdIdClaims

try:
    from typing import List
except ImportError:
    pass

from execnet.gateway_bootstrap import HostNotFound

from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, RGWSpec, \
    NFSServiceSpec, IscsiServiceSpec, HostPlacementSpec, CustomContainerSpec, MDSSpec
from ceph.deployment.drive_selection.selector import DriveSelection
from ceph.deployment.inventory import Devices, Device
from ceph.utils import datetime_to_str, datetime_now
from orchestrator import DaemonDescription, InventoryHost, \
    HostSpec, OrchestratorError, DaemonDescriptionStatus, OrchestratorEvent
from tests import mock
from .fixtures import wait, _run_cephadm, match_glob, with_host, \
    with_cephadm_module, with_service, _deploy_cephadm_binary, make_daemons_running
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
        def _ceph_volume_list(s, host, entity, cmd, **kwargs):
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
                  ['--', 'lvm', 'list', '--format', 'json'], no_fsid=False, image=''),
        mock.call(host, f'osd.{osd_id}', 'deploy',
                  ['--name', f'osd.{osd_id}', '--meta-json', mock.ANY,
                   '--config-json', '-', '--osd-fsid', 'uuid'],
                  stdin=mock.ANY, image=''),
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
    def test_daemon_check(self, cephadm_module: CephadmOrchestrator, action):
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='grafana'), CephadmOrchestrator.apply_grafana, 'test') as d_names:
                [daemon_name] = d_names

                cephadm_module._schedule_daemon_action(daemon_name, action)

                assert cephadm_module.cache.get_scheduled_daemon_action(
                    'test', daemon_name) == action

                CephadmServe(cephadm_module)._check_daemons()

                assert cephadm_module.cache.get_scheduled_daemon_action('test', daemon_name) is None

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_daemon_check_extra_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)

        with with_host(cephadm_module, 'test'):

            # Also testing deploying mons without explicit network placement
            cephadm_module.check_mon_command({
                'prefix': 'config set',
                'who': 'mon',
                'name': 'public_network',
                'value': '127.0.0.0/8'
            })

            cephadm_module.cache.update_host_devices_networks(
                'test',
                [],
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
                        '--meta-json', '{"service_name": "mon", "ports": [], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null}',
                        '--config-json', '-',
                        '--reconfig',
                    ],
                    stdin='{"config": "\\n\\n[mon]\\nk=v\\n[mon.test]\\npublic network = 127.0.0.0/8\\n", '
                    + '"keyring": "", "files": {"config": "[mon.test]\\npublic network = 127.0.0.0/8\\n"}}',
                    image='')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_extra_container_args(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec(service_type='crash', extra_container_args=['--cpus=2', '--quiet']), CephadmOrchestrator.apply_crash):
                _run_cephadm.assert_called_with(
                    'test', 'crash.test', 'deploy', [
                        '--name', 'crash.test',
                        '--meta-json', '{"service_name": "crash", "ports": [], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": ["--cpus=2", "--quiet"]}',
                        '--config-json', '-',
                        '--extra-container-args=--cpus=2',
                        '--extra-container-args=--quiet'
                    ],
                    stdin='{"config": "", "keyring": ""}',
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
        _run_cephadm.return_value = ('{}', '', 0)
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

            cephadm_module.cache.update_host_devices_networks('test', inventory.devices, {})

            _run_cephadm.return_value = (['{}'], '', 0)

            assert CephadmServe(cephadm_module)._apply_all_services() is False

            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume',
                ['--config-json', '-', '--', 'lvm', 'batch',
                    '--no-auto', '/dev/sdb', '--yes', '--no-systemd'],
                env_vars=['CEPH_VOLUME_OSDSPEC_AFFINITY=foo'], error_ok=True, stdin='{"config": "", "keyring": ""}')
            _run_cephadm.assert_called_with(
                'test', 'osd', 'ceph-volume', ['--', 'lvm', 'list', '--format', 'json'], image='', no_fsid=False)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_apply_osd_save_non_collocated(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
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

            cephadm_module.cache.update_host_devices_networks('test', inventory.devices, {})

            _run_cephadm.return_value = (['{}'], '', 0)

            assert CephadmServe(cephadm_module)._apply_all_services() is False

            _run_cephadm.assert_any_call(
                'test', 'osd', 'ceph-volume',
                ['--config-json', '-', '--', 'lvm', 'batch',
                    '--no-auto', '/dev/sdb', '--db-devices', '/dev/sdc',
                    '--wal-devices', '/dev/sdd', '--yes', '--no-systemd'],
                env_vars=['CEPH_VOLUME_OSDSPEC_AFFINITY=noncollocated'],
                error_ok=True, stdin='{"config": "", "keyring": ""}')
            _run_cephadm.assert_called_with(
                'test', 'osd', 'ceph-volume', ['--', 'lvm', 'list', '--format', 'json'], image='', no_fsid=False)

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
    def test_limit_not_reached(self, d_to_cv, _run_cv_cmd, cephadm_module):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(placement=PlacementSpec(host_pattern='test'),
                                data_devices=DeviceSelection(limit=5, rotational=1),
                                service_id='not_enough')

            disks_found = [
                '[{"data": "/dev/vdb", "data_size": "50.00 GB", "encryption": "None"}, {"data": "/dev/vdc", "data_size": "50.00 GB", "encryption": "None"}]']
            d_to_cv.return_value = 'foo'
            _run_cv_cmd.return_value = (disks_found, '', 0)
            preview = cephadm_module.osd_service.generate_previews([dg], 'test')

            for osd in preview:
                assert 'notes' in osd
                assert osd['notes'] == [
                    'NOTE: Did not find enough disks matching filter on host test to reach data device limit (Found: 2 | Limit: 5)']

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
        "devices, preview, exp_command",
        [
            # no preview and only one disk, prepare is used due the hack that is in place.
            (['/dev/sda'], False, "lvm batch --no-auto /dev/sda --yes --no-systemd"),
            # no preview and multiple disks, uses batch
            (['/dev/sda', '/dev/sdb'], False,
             "CEPH_VOLUME_OSDSPEC_AFFINITY=test.spec lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd"),
            # preview and only one disk needs to use batch again to generate the preview
            (['/dev/sda'], True, "lvm batch --no-auto /dev/sda --yes --no-systemd --report --format json"),
            # preview and multiple disks work the same
            (['/dev/sda', '/dev/sdb'], True,
             "CEPH_VOLUME_OSDSPEC_AFFINITY=test.spec lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd --report --format json"),
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_driveselection_to_ceph_volume(self, cephadm_module, devices, preview, exp_command):
        with with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec(service_id='test.spec', placement=PlacementSpec(
                host_pattern='test'), data_devices=DeviceSelection(paths=devices))
            ds = DriveSelection(dg, Devices([Device(path) for path in devices]))
            preview = preview
            out = cephadm_module.osd_service.driveselection_to_ceph_volume(ds, [], preview)
            assert out in exp_command

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
    def test_remove_osds(self, cephadm_module):
        with with_host(cephadm_module, 'test'):
            CephadmServe(cephadm_module)._refresh_host_daemons('test')
            c = cephadm_module.list_daemons()
            wait(cephadm_module, c)

            c = cephadm_module.remove_daemons(['osd.0'])
            out = wait(cephadm_module, c)
            assert out == ["Removed osd.0 from host 'test'"]

            cephadm_module.to_remove_osds.enqueue(OSD(osd_id=0,
                                                      replace=False,
                                                      force=False,
                                                      hostname='test',
                                                      process_started_at=datetime_now(),
                                                      remove_util=cephadm_module.to_remove_osds.rm_util
                                                      ))
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
        _run_cephadm.return_value = ('{}', '', 0)
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
            ServiceSpec('cephadm-exporter'),
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._deploy_cephadm_binary", _deploy_cephadm_binary('test'))
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
        _run_cephadm.return_value = '{}', '', 0
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
        _run_cephadm.return_value = ('{}', '', 0)
        with with_host(cephadm_module, 'test'):
            _run_cephadm.side_effect = OrchestratorError('fail')
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            r = CephadmServe(cephadm_module)._apply_service(ServiceSpec('mgr', placement=ps))
            assert not r
            assert cephadm_module.health_checks.get('CEPHADM_DAEMON_PLACE_FAIL') is not None
            assert cephadm_module.health_checks['CEPHADM_DAEMON_PLACE_FAIL']['count'] == 1
            assert 'Failed to place 1 daemon(s)' in cephadm_module.health_checks['CEPHADM_DAEMON_PLACE_FAIL']['summary']
            assert 'Failed while placing mgr.a on test: fail' in cephadm_module.health_checks['CEPHADM_DAEMON_PLACE_FAIL']['detail']

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_apply_spec_fail_health_warning(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
        with with_host(cephadm_module, 'test'):
            CephadmServe(cephadm_module)._apply_all_services()
            ps = PlacementSpec(hosts=['fail'], count=1)
            r = CephadmServe(cephadm_module)._apply_service(ServiceSpec('mgr', placement=ps))
            assert not r
            assert cephadm_module.apply_spec_fails
            assert cephadm_module.health_checks.get('CEPHADM_APPLY_SPEC_FAIL') is not None
            assert cephadm_module.health_checks['CEPHADM_APPLY_SPEC_FAIL']['count'] == 1
            assert 'Failed to apply 1 service(s)' in cephadm_module.health_checks['CEPHADM_APPLY_SPEC_FAIL']['summary']

    @mock.patch("cephadm.module.CephadmOrchestrator.get_foreign_ceph_option")
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_invalid_config_option_health_warning(self, _run_cephadm, get_foreign_ceph_option, cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
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
        _run_cephadm.return_value = '{}', '', 0
        with with_host(cephadm_module, 'test'):
            c = cephadm_module.blink_device_light(fault_ident, on_bool, [('test', '', 'dev')])
            on_off = 'on' if on_bool else 'off'
            assert wait(cephadm_module, c) == [f'Set {fault_ident} light for test: {on_off}']
            _run_cephadm.assert_called_with('test', 'osd', 'shell', [
                                            '--', 'lsmcli', f'local-disk-{fault_ident}-led-{on_off}', '--path', 'dev'], error_ok=True)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_blink_device_light_custom(self, _run_cephadm, cephadm_module):
        _run_cephadm.return_value = '{}', '', 0
        with with_host(cephadm_module, 'test'):
            cephadm_module.set_store('blink_device_light_cmd', 'echo hello')
            c = cephadm_module.blink_device_light('ident', True, [('test', '', '/dev/sda')])
            assert wait(cephadm_module, c) == ['Set ident light for test: on']
            _run_cephadm.assert_called_with('test', 'osd', 'shell', [
                                            '--', 'echo', 'hello'], error_ok=True)

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_blink_device_light_custom_per_host(self, _run_cephadm, cephadm_module):
        _run_cephadm.return_value = '{}', '', 0
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
            (ServiceSpec('cephadm-exporter'), CephadmOrchestrator.apply_cephadm_exporter),
        ]
    )
    @mock.patch("cephadm.serve.CephadmServe._deploy_cephadm_binary", _deploy_cephadm_binary('test'))
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

    @mock.patch("cephadm.serve.CephadmServe._deploy_cephadm_binary", _deploy_cephadm_binary('test'))
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

    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("remoto.process.check")
    def test_offline(self, _check, _get_connection, cephadm_module):
        _check.return_value = '{}', '', 0
        _get_connection.return_value = mock.Mock(), mock.Mock()
        with with_host(cephadm_module, 'test'):
            _get_connection.side_effect = HostNotFound
            code, out, err = cephadm_module.check_host('test')
            assert out == ''
            assert "Host 'test' not found" in err

            out = wait(cephadm_module, cephadm_module.get_hosts())[0].to_json()
            assert out == HostSpec('test', '1::4', status='Offline').to_json()

            _get_connection.side_effect = None
            assert CephadmServe(cephadm_module)._check_host('test') is None
            out = wait(cephadm_module, cephadm_module.get_hosts())[0].to_json()
            assert out == HostSpec('test', '1::4').to_json()

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
                        candidates = [
                            h.hostname for h in cephadm_module._schedulable_hosts()]
                        assert 'test2' in candidates
                        assert 'test3' in candidates

                        unreachable = [h.hostname for h in cephadm_module._unreachable_hosts()]
                        assert 'test2' in unreachable
                        assert 'test3' in unreachable

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
        _run_cephadm.return_value = [''], ['something\nsuccess - systemd target xxx disabled'], 0
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
        _run_cephadm.return_value = [''], ['something\nfailed - disable the target'], 0
        _host_ok.return_value = 0, 'it is okay'
        _get_daemon_types.return_value = ['crash']
        _hosts.return_value = [hostname, 'other_host']
        cephadm_module.inventory.add_host(HostSpec(hostname))

        with pytest.raises(OrchestratorError, match='Failed to place host1 into maintenance for cluster fsid'):
            cephadm_module.enter_host_maintenance(hostname)

        assert not cephadm_module.inventory._inventory[hostname]['status']

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    @mock.patch("cephadm.module.HostCache.get_daemon_types")
    @mock.patch("cephadm.module.HostCache.get_hosts")
    def test_maintenance_exit_success(self, _hosts, _get_daemon_types, _run_cephadm, cephadm_module: CephadmOrchestrator):
        hostname = 'host1'
        _run_cephadm.return_value = [''], [
            'something\nsuccess - systemd target xxx enabled and started'], 0
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
        _run_cephadm.return_value = [''], ['something\nfailed - unable to enable the target'], 0
        _get_daemon_types.return_value = ['crash']
        _hosts.return_value = [hostname, 'other_host']
        cephadm_module.inventory.add_host(HostSpec(hostname, status='maintenance'))

        with pytest.raises(OrchestratorError, match='Failed to exit maintenance state for host host1, cluster fsid'):
            cephadm_module.exit_host_maintenance(hostname)

        assert cephadm_module.inventory._inventory[hostname]['status'] == 'maintenance'

    def test_stale_connections(self, cephadm_module):
        class Connection(object):
            """
            A mocked connection class that only allows the use of the connection
            once. If you attempt to use it again via a _check, it'll explode (go
            boom!).

            The old code triggers the boom. The new code checks the has_connection
            and will recreate the connection.
            """
            fuse = False

            @ staticmethod
            def has_connection():
                return False

            def import_module(self, *args, **kargs):
                return mock.Mock()

            @ staticmethod
            def exit():
                pass

        def _check(conn, *args, **kargs):
            if conn.fuse:
                raise Exception("boom: connection is dead")
            else:
                conn.fuse = True
            return '{}', [], 0
        with mock.patch("remoto.Connection", side_effect=[Connection(), Connection(), Connection()]):
            with mock.patch("remoto.process.check", _check):
                with with_host(cephadm_module, 'test', refresh_hosts=False):
                    code, out, err = cephadm_module.check_host('test')
                    # First should succeed.
                    assert err == ''

                    # On second it should attempt to reuse the connection, where the
                    # connection is "down" so will recreate the connection. The old
                    # code will blow up here triggering the BOOM!
                    code, out, err = cephadm_module.check_host('test')
                    assert err == ''

    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    @mock.patch("remoto.process.check")
    @mock.patch("cephadm.module.CephadmServe._write_remote_file")
    def test_etc_ceph(self, _write_file, _check, _get_connection, cephadm_module):
        _get_connection.return_value = mock.Mock(), mock.Mock()
        _check.return_value = '{}', '', 0
        _write_file.return_value = None

        assert cephadm_module.manage_etc_ceph_ceph_conf is False

        with with_host(cephadm_module, 'test'):
            assert '/etc/ceph/ceph.conf' not in cephadm_module.cache.get_host_client_files('test')

        with with_host(cephadm_module, 'test'):
            cephadm_module.set_module_option('manage_etc_ceph_ceph_conf', True)
            cephadm_module.config_notify()
            assert cephadm_module.manage_etc_ceph_ceph_conf is True

            CephadmServe(cephadm_module)._refresh_hosts_and_daemons()
            _write_file.assert_called_with('test', '/etc/ceph/ceph.conf', b'',
                                           0o644, 0, 0)

            assert '/etc/ceph/ceph.conf' in cephadm_module.cache.get_host_client_files('test')

            # set extra config and expect that we deploy another ceph.conf
            cephadm_module._set_extra_ceph_conf('[mon]\nk=v')
            CephadmServe(cephadm_module)._refresh_hosts_and_daemons()
            _write_file.assert_called_with('test', '/etc/ceph/ceph.conf',
                                           b'\n\n[mon]\nk=v\n', 0o644, 0, 0)

            # reload
            cephadm_module.cache.last_client_files = {}
            cephadm_module.cache.load()

            assert '/etc/ceph/ceph.conf' in cephadm_module.cache.get_host_client_files('test')

            # Make sure, _check_daemons does a redeploy due to monmap change:
            before_digest = cephadm_module.cache.get_host_client_files('test')[
                '/etc/ceph/ceph.conf'][0]
            cephadm_module._set_extra_ceph_conf('[mon]\nk2=v2')
            CephadmServe(cephadm_module)._refresh_hosts_and_daemons()
            after_digest = cephadm_module.cache.get_host_client_files('test')[
                '/etc/ceph/ceph.conf'][0]
            assert before_digest != after_digest

    def test_etc_ceph_init(self):
        with with_cephadm_module({'manage_etc_ceph_ceph_conf': True}) as m:
            assert m.manage_etc_ceph_ceph_conf is True

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_registry_login(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        def check_registry_credentials(url, username, password):
            assert json.loads(cephadm_module.get_store('registry_credentials')) == {
                'url': url, 'username': username, 'password': password}

        _run_cephadm.return_value = '{}', '', 0
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
            _run_cephadm.return_value = '{}', 'error', 1
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
        _run_cephadm.return_value = ('{}', '', 0)

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
                          no_fsid=False),
                mock.call('test', 'osd', 'ceph-volume',
                          ['--', 'inventory', '--format=json-pretty'], image='',
                          no_fsid=False),
            ]

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_osd_activate_datadevice(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
        with with_host(cephadm_module, 'test', refresh_hosts=False):
            with with_osd_daemon(cephadm_module, _run_cephadm, 'test', 1):
                pass

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_osd_activate_datadevice_fail(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
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
            _run_cephadm.reset_mock(return_value=True)

            def _r_c(*args, **kwargs):
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
        _run_cephadm.return_value = ('{}', '', 0)
        with with_host(cephadm_module, 'test', refresh_hosts=False):

            def _ceph_volume_list(s, host, entity, cmd, **kwargs):
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
        _run_cephadm.return_value = ('{}', '', 0)
        dg = DriveGroupSpec(service_id='', data_devices=DeviceSelection(all=True))
        with with_host(cephadm_module, 'test', refresh_hosts=False):
            with with_service(cephadm_module, dg, host='test'):
                with with_osd_daemon(cephadm_module, _run_cephadm, 'test', 1):
                    assert wait(cephadm_module, cephadm_module.describe_service())[0].size == 1

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_host_rm_last_admin(self, cephadm_module: CephadmOrchestrator):
        with pytest.raises(OrchestratorError):
            with with_host(cephadm_module, 'test', refresh_hosts=False, rm_with_force=False):
                cephadm_module.inventory.add_label('test', '_admin')
                pass
            assert False
        with with_host(cephadm_module, 'test1', refresh_hosts=False, rm_with_force=True):
            with with_host(cephadm_module, 'test2', refresh_hosts=False, rm_with_force=False):
                cephadm_module.inventory.add_label('test2', '_admin')
