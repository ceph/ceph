from unittest import mock

from cephadm.module import CephadmOrchestrator
from cephadm.services.cephadmservice import (
    CephadmDaemonDeploySpec,
    MgrService,
)


class TestMgrService:
    def _prepare(self, cephadm_module: CephadmOrchestrator,
                 mgr_services_payload: str,
                 carried_over_ports: list) -> CephadmDaemonDeploySpec:
        """Build a daemon_spec that mimics rehydration from the persisted
        DaemonDescription (which copies `ports=dd.ports`), then call
        MgrService.prepare_create with `mgr services` returning the supplied
        payload.
        """
        svc = MgrService(cephadm_module)
        daemon_spec = CephadmDaemonDeploySpec(
            host='ceph-1',
            daemon_id='ceph-1.xyz',
            service_name='mgr',
            daemon_type='mgr',
            ports=list(carried_over_ports),
        )
        with mock.patch.object(cephadm_module, 'check_mon_command',
                               return_value=(0, mgr_services_payload, '')), \
             mock.patch.object(svc, 'get_keyring_with_caps',
                               return_value='[mgr.ceph-1.xyz]\n\tkey = X\n'), \
             mock.patch.object(svc, 'generate_config',
                               return_value=({}, [])):
            return svc.prepare_create(daemon_spec)

    def test_prepare_create_empty_mgr_services_discards_carryover(
            self, cephadm_module: CephadmOrchestrator):
        # Regression for https://tracker.ceph.com/issues/76564
        # When `mgr services` returns no endpoints (e.g. dashboard module not
        # yet listening during an upgrade), the carried-over daemon_spec.ports
        # list from the persisted DaemonDescription must be discarded; the
        # final list must contain exactly one service_discovery_port entry.
        carried = [
            cephadm_module.service_discovery_port,
            cephadm_module.service_discovery_port,
            cephadm_module.service_discovery_port,
            cephadm_module.service_discovery_port,
            cephadm_module.service_discovery_port,
        ]
        out = self._prepare(cephadm_module, '', carried)
        assert out.ports == [cephadm_module.service_discovery_port]

    def test_prepare_create_with_mgr_services_includes_module_port(
            self, cephadm_module: CephadmOrchestrator):
        # When `mgr services` reports a module endpoint (e.g. prometheus on
        # 9283), the result must include that port plus exactly one
        # service_discovery_port — with no duplicates from the carried-over
        # daemon_spec.ports.
        carried = [
            cephadm_module.service_discovery_port,
            cephadm_module.service_discovery_port,
        ]
        out = self._prepare(
            cephadm_module,
            '{"prometheus": "http://192.0.2.10:9283/"}',
            carried,
        )
        assert out.ports == [9283, cephadm_module.service_discovery_port]
