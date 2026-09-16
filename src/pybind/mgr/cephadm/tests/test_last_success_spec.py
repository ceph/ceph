from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from ceph.deployment.service_spec import NodeProxySpec, PlacementSpec, ServiceSpec
from ceph.utils import datetime_now

from cephadm.module import CephadmOrchestrator
from cephadm.serve import CephadmServe
from orchestrator import DaemonDescriptionStatus
from tests import mock

from .fixtures import async_side_effect, with_host, wait


def _serve_pass(cephadm_module: CephadmOrchestrator) -> bool:
    """One serve() loop pass, after simulating the host daemon refresh done
    at its start (reachable hosts refreshed, new daemons running)."""
    for host in cephadm_module.cache.get_hosts():
        if host in cephadm_module.offline_hosts:
            continue
        cephadm_module.cache.last_daemon_update[host] = datetime_now()
        for dd in cephadm_module.cache.get_daemons_by_host(host):
            if dd.status in (None, DaemonDescriptionStatus.starting):
                dd.status = DaemonDescriptionStatus.running
    serve = CephadmServe(cephadm_module)
    if serve._apply_all_services():
        return True
    serve._check_daemons()
    serve._record_last_working_specs()
    return False


def _converge(cephadm_module: CephadmOrchestrator) -> None:
    for _ in range(5):
        if not _serve_pass(cephadm_module):
            return
    raise AssertionError('serve loop did not converge')


def _last(cephadm_module: CephadmOrchestrator, name: str):
    return cephadm_module.spec_store.get_last_working_spec(name)


@mock.patch("cephadm.serve.CephadmServe._run_cephadm")
class TestLastSuccessIncompleteApply:

    def test_not_recorded_while_countless_target_unreachable(
            self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        # An OSD spec for '*' while one host is offline: nothing is deployed
        # there and no daemon of the spec exists on it, so only the
        # placement itself shows the spec was not applied everywhere.
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test1'), with_host(cephadm_module, 'test2'):
            spec = DriveGroupSpec(service_id='foo',
                                  placement=PlacementSpec(host_pattern='*'),
                                  data_devices=DeviceSelection(paths=['/dev/sdb']))
            wait(cephadm_module, cephadm_module.apply([spec]))
            with mock.patch.object(cephadm_module.osd_service,
                                   'create_from_spec', return_value=''):
                cephadm_module.offline_hosts.add('test2')
                _converge(cephadm_module)
                assert _last(cephadm_module, 'osd.foo') is None

                cephadm_module.offline_hosts.discard('test2')
                _converge(cephadm_module)
                assert _last(cephadm_module, 'osd.foo') == spec

    def test_count_placement_not_blocked_by_unreachable_host(
            self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        # count-based placements can be met elsewhere, so an unrelated
        # unreachable host must not block recording
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test1'), with_host(cephadm_module, 'test2'):
            spec = ServiceSpec('crash', placement=PlacementSpec(count=1, hosts=['test1']))
            wait(cephadm_module, cephadm_module.apply([spec]))
            _serve_pass(cephadm_module)
            cephadm_module.offline_hosts.add('test2')
            _converge(cephadm_module)
            cephadm_module.offline_hosts.discard('test2')
            assert _last(cephadm_module, 'crash') == spec

    def test_node_proxy_without_oob_details_not_recorded(
            self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        # node-proxy is not deployed on a host without out-of-band details,
        # so the spec has not been applied there
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            spec = NodeProxySpec(service_type='node-proxy',
                                 placement=PlacementSpec(hosts=['test']))
            cephadm_module.spec_store.save(spec)
            cephadm_module.node_proxy_cache.oob = {}
            _converge(cephadm_module)
            assert cephadm_module.cache.get_daemons_by_service('node-proxy') == []
            assert _last(cephadm_module, 'node-proxy') is None
