import json
from unittest import mock

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from ceph.deployment.service_spec import PlacementSpec, RGWSpec

from cephadm import utils
from cephadm.module import CephadmOrchestrator
from cephadm.serve import CephadmServe
from cephadm.services.oob import oob_changed, oob_container_config
from cephadm.services.osd import OSDService
from cephadm.tests.fixtures import async_side_effect, wait, with_host, with_service

GiB = 1 << 30


def _deploys(_run_cephadm):
    """(params, config_blobs) of each deploy that the mgr sent"""
    out = []
    for c in _run_cephadm.call_args_list:
        if c.args[2] == ['_orch', 'deploy']:
            d = json.loads(c.kwargs['stdin'])
            out.append((d['params'], d['config_blobs']))
    return out


def _rgw(oob=None):
    return RGWSpec(service_id='s3', placement=PlacementSpec(hosts=['test']), oob=oob)


@mock.patch("cephadm.serve.CephadmServe._run_cephadm")
def test_rgw_deploy(_run_cephadm, cephadm_module: CephadmOrchestrator):
    _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
    with with_host(cephadm_module, 'test'):
        with with_service(cephadm_module, _rgw({'rdma_devices': ['mlx5_0']})):
            [(_, blobs)] = _deploys(_run_cephadm)
            # memlock follows oob_memory, at its default of 2G
            assert blobs['oob'] == {
                'rdma_devices': ['mlx5_0'],
                'memlock': 3 * GiB,
                'raw_sockets': False,
            }


@mock.patch("cephadm.serve.CephadmServe._run_cephadm")
def test_rgw_deploy_without_oob(_run_cephadm, cephadm_module: CephadmOrchestrator):
    _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
    with with_host(cephadm_module, 'test'):
        with with_service(cephadm_module, _rgw()):
            [(_, blobs)] = _deploys(_run_cephadm)
            assert 'oob' not in blobs
            deps = cephadm_module.rgw_service.get_dependencies(cephadm_module, _rgw())
            assert not [d for d in deps if d.startswith('oob:')]


@mock.patch("cephadm.serve.CephadmServe._run_cephadm")
def test_rgw_oob_change(_run_cephadm, cephadm_module: CephadmOrchestrator):
    # a change of the oob section, or of the oob_memory that memlock follows,
    # redeploys the gateway with the new container arguments
    _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
    with with_host(cephadm_module, 'test'):
        with with_service(cephadm_module, _rgw({'rdma_devices': ['mlx5_0']})):
            _run_cephadm.reset_mock()
            CephadmServe(cephadm_module)._check_daemons()
            assert _deploys(_run_cephadm) == []

            wait(cephadm_module, cephadm_module.apply_rgw(
                _rgw({'rdma_devices': ['mlx5_0'], 'memlock': '8G'})))
            CephadmServe(cephadm_module)._check_daemons()
            [(params, blobs)] = _deploys(_run_cephadm)
            assert 'reconfig' not in params
            assert blobs['oob']['memlock'] == 8 * GiB

            wait(cephadm_module, cephadm_module.apply_rgw(
                _rgw({'rdma_devices': ['mlx5_0']})))
            cephadm_module.check_mon_command({
                'prefix': 'config set',
                'who': 'client.rgw.s3',
                'name': 'oob_memory',
                'value': str(4 * GiB),
            })
            _run_cephadm.reset_mock()
            CephadmServe(cephadm_module)._check_daemons()
            [(params, blobs)] = _deploys(_run_cephadm)
            assert 'reconfig' not in params
            assert blobs['oob']['memlock'] == 5 * GiB


def _osd_spec(oob=None):
    return DriveGroupSpec(
        service_id='rdma',
        placement=PlacementSpec(host_pattern='*'),
        data_devices=DeviceSelection(all=True),
        oob=oob,
    )


def test_osd_oob_config(cephadm_module: CephadmOrchestrator):
    spec = _osd_spec({'rdma_devices': 'all', 'raw_sockets': True})
    cephadm_module.check_mon_command({
        'prefix': 'config set',
        'who': 'osd.3',
        'name': 'oob_memory',
        'value': str(GiB),
    })
    entity = utils.name_to_config_section('osd.3')
    assert oob_container_config(cephadm_module, spec, entity) == {
        'rdma_devices': 'all',
        'memlock': 2 * GiB,
        'raw_sockets': True,
    }
    assert oob_container_config(cephadm_module, _osd_spec(), entity) is None


def test_osd_oob_dependencies(cephadm_module: CephadmOrchestrator):
    def deps(spec):
        return OSDService.get_dependencies(cephadm_module, spec, 'osd')

    assert deps(_osd_spec()) == []
    assert deps(None) == []
    base = deps(_osd_spec({'rdma_devices': ['mlx5_0']}))
    assert len(base) == 1 and base[0].startswith('oob:')
    assert deps(_osd_spec({'rdma_devices': ['mlx5_0']})) == base
    assert deps(_osd_spec({'rdma_devices': ['mlx5_1']})) != base
    cephadm_module.check_mon_command({
        'prefix': 'config set',
        'who': 'osd',
        'name': 'oob_memory',
        'value': str(GiB),
    })
    assert deps(_osd_spec({'rdma_devices': ['mlx5_0']})) != base


def test_osd_next_action(cephadm_module: CephadmOrchestrator):
    svc = cephadm_module.osd_service
    spec = _osd_spec({'rdma_devices': ['mlx5_0']})
    none = utils.Action.NO_ACTION

    def step(curr, last):
        return svc.choose_next_action(none, 'osd', spec, curr, last).action

    assert step(['oob:1'], ['oob:1']) is none
    # container arguments need a redeploy
    assert step(['oob:2'], ['oob:1']) is utils.Action.REDEPLOY
    assert step(['oob:1'], []) is utils.Action.REDEPLOY
    assert step([], ['oob:1']) is utils.Action.REDEPLOY
    assert step(['other:2'], ['other:1']) is utils.Action.RECONFIG
    assert oob_changed(['a', 'oob:1'], ['oob:1', 'a']) is False
