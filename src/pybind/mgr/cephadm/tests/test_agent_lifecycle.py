"""
Tests for the agent service lifecycle: mgr/cephadm/use_agent owns the
agent spec.
"""
from ceph.deployment.service_spec import (
    CertificateSource,
    PlacementSpec,
    ServiceSpec,
)

from cephadm.agent import CephadmAgentHelpers
from cephadm.module import CephadmOrchestrator
from cephadm.serve import CephadmServe
from tests import mock
from .fixtures import _run_cephadm, with_host

# with_cephadm_module() mocks _apply_agent(); keep the real one so these
# tests can check the spec it writes.
_real_apply_agent = CephadmAgentHelpers._apply_agent


def _use_real_apply_agent():
    return mock.patch.object(CephadmAgentHelpers, '_apply_agent', _real_apply_agent)


def _assert_canonical_agent_spec(spec: ServiceSpec) -> None:
    assert spec.service_type == 'agent'
    assert spec.placement.host_pattern is not None
    assert spec.placement.host_pattern.pattern == '*'
    assert spec.ssl
    assert spec.certificate_source == CertificateSource.CEPHADM_SIGNED.value


class TestUseAgentSetting:

    def test_use_agent_creates_spec(self, cephadm_module: CephadmOrchestrator):
        cephadm_module.use_agent = True
        with _use_real_apply_agent():
            assert cephadm_module.agent_helpers._handle_use_agent_setting()
            _assert_canonical_agent_spec(cephadm_module.spec_store.active_specs['agent'])
            # nothing left to do on the next pass
            assert not cephadm_module.agent_helpers._handle_use_agent_setting()

    def test_use_agent_revives_spec_pending_deletion(self, cephadm_module: CephadmOrchestrator):
        store = cephadm_module.spec_store
        cephadm_module.use_agent = True
        with _use_real_apply_agent():
            # a spec saved without ssl (as stored by older releases or a
            # hand-written 'orch apply'), then removed
            store.save(ServiceSpec('agent', placement=PlacementSpec(host_pattern='*')))
            store.rm('agent')
            assert 'agent' in store
            assert 'agent' not in store.active_specs

            assert cephadm_module.agent_helpers._handle_use_agent_setting()
            assert 'agent' not in store.spec_deleted
            # the revived spec is the one cephadm expects
            _assert_canonical_agent_spec(store.active_specs['agent'])
            assert not cephadm_module.agent_helpers._handle_use_agent_setting()

    def test_use_agent_off_removes_spec_once(self, cephadm_module: CephadmOrchestrator):
        store = cephadm_module.spec_store
        cephadm_module.use_agent = True
        with _use_real_apply_agent():
            cephadm_module.agent_helpers._handle_use_agent_setting()

        cephadm_module.use_agent = False
        with mock.patch.object(store, 'rm', wraps=store.rm) as rm:
            assert cephadm_module.agent_helpers._handle_use_agent_setting()
            assert 'agent' in store.spec_deleted
            # a spec already pending removal is left alone
            assert not cephadm_module.agent_helpers._handle_use_agent_setting()
            rm.assert_called_once_with('agent')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_metadata_gate_does_not_apply_deleted_agent_spec(self, cephadm_module: CephadmOrchestrator):
        store = cephadm_module.spec_store
        with with_host(cephadm_module, 'test'):
            cephadm_module.use_agent = True
            cephadm_module.cache.metadata_up_to_date['test'] = False
            store.save(ServiceSpec('agent', placement=PlacementSpec(host_pattern='*'),
                                   ssl=True,
                                   certificate_source=CertificateSource.CEPHADM_SIGNED.value))
            store.rm('agent')

            with _use_real_apply_agent(), \
                    mock.patch.object(CephadmServe, '_apply_service') as _apply_service:
                CephadmServe(cephadm_module)._apply_all_services()
                # the deleted spec is revived, not applied as it is
                _apply_service.assert_not_called()
                assert 'agent' in store.active_specs

                CephadmServe(cephadm_module)._apply_all_services()
                assert [c.args[0].service_name() for c in _apply_service.call_args_list] == ['agent']
            cephadm_module.use_agent = False
