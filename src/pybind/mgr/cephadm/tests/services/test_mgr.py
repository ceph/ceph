import json
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
        MgrService.prepare_create with mgr_map returning the supplied
        services payload.
        """
        svc = MgrService(cephadm_module)
        daemon_spec = CephadmDaemonDeploySpec(
            host='ceph-1',
            daemon_id='ceph-1.xyz',
            service_name='mgr',
            daemon_type='mgr',
            ports=list(carried_over_ports),
        )
        services = json.loads(mgr_services_payload) if mgr_services_payload else {}
        cephadm_module.mock_store_set('_ceph_get', 'mgr_map', {'services': services})
        with mock.patch.object(svc, 'get_keyring_with_caps',
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

    def test_get_dependencies_changes_when_module_enabled(
            self, cephadm_module: CephadmOrchestrator):
        # Verify that get_dependencies reflects the current active modules.
        # This is the mechanism that causes the serve loop to detect port
        # changes and trigger a reconfig when modules are enabled/disabled.

        # Before: no modules enabled
        cephadm_module.mock_store_set('_ceph_get', 'mgr_map', {'services': {}})
        deps_before = MgrService.get_dependencies(cephadm_module)
        assert deps_before == [f'sd_port:{cephadm_module.service_discovery_port}']

        # After: prometheus enabled
        cephadm_module.mock_store_set('_ceph_get', 'mgr_map', {
            'services': {'prometheus': 'http://192.0.2.10:9283/'}
        })
        deps_after = MgrService.get_dependencies(cephadm_module)
        assert deps_after == ['port:9283', f'sd_port:{cephadm_module.service_discovery_port}']

        # The difference between deps_before and deps_after is what the serve
        # loop detects -- this is what triggers the reconfig
        assert deps_before != deps_after
