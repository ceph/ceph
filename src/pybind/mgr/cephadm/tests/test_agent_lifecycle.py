"""
Tests for the agent service lifecycle: mgr/cephadm/use_agent owns the
agent spec.
"""
import pytest

from ceph.deployment.service_spec import (
    CertificateSource,
    PlacementSpec,
    ServiceSpec,
)

from ceph.utils import datetime_now
from orchestrator import DaemonDescription, OrchestratorError

from cephadm.agent import CephadmAgentHelpers
from cephadm.inventory import AGENT_CACHE_PREFIX, AgentCache
from cephadm.module import CephadmOrchestrator
from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import CephService
from cephadm.services.service_registry import service_registry
from tests import mock
from .fixtures import _run_cephadm, wait, with_host

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


def _seed_agent_state(m: CephadmOrchestrator, host: str) -> None:
    ac = m.agent_cache
    ac.agent_counter[host] = 7
    ac.agent_keys[host] = f'key-{host}'
    ac.agent_ports[host] = 4721
    ac.agent_timestamp[host] = datetime_now()
    ac.update_agent_config_deps(host, ['dep'], datetime_now())
    ac.sending_agent_message[host] = True
    ac.save_agent(host)


def _has_agent_state(m: CephadmOrchestrator, host: str) -> bool:
    ac = m.agent_cache
    in_memory = any(host in d for d in (
        ac.agent_counter, ac.agent_keys, ac.agent_ports, ac.agent_timestamp,
        ac.agent_config_deps, ac.sending_agent_message))
    stored = m.get_store(AGENT_CACHE_PREFIX + host) is not None
    return in_memory or stored


class TestAgentStateReset:

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_rm_agent(self, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'), with_host(cephadm_module, 'host2'):
            _seed_agent_state(cephadm_module, 'host1')
            _seed_agent_state(cephadm_module, 'host2')

            cephadm_module.agent_cache.rm_agent('host1')
            assert not _has_agent_state(cephadm_module, 'host1')
            assert _has_agent_state(cephadm_module, 'host2')

            # nothing comes back after a mgr restart
            reloaded = AgentCache(cephadm_module)
            reloaded.load()
            assert 'host1' not in reloaded.agent_counter
            assert reloaded.agent_counter['host2'] == 7

            # removing an unknown agent is a no-op
            cephadm_module.agent_cache.rm_agent('host1')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_agent_post_remove_resets_host_state(self, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'), with_host(cephadm_module, 'host2'):
            _seed_agent_state(cephadm_module, 'host1')
            _seed_agent_state(cephadm_module, 'host2')
            dd = DaemonDescription(daemon_type='agent', daemon_id='host1', hostname='host1')
            with mock.patch.object(CephService, 'post_remove') as base_post_remove:
                service_registry.get_service('agent').post_remove(dd, is_failed_deploy=False)
                base_post_remove.assert_called_once()
            assert not _has_agent_state(cephadm_module, 'host1')
            assert _has_agent_state(cephadm_module, 'host2')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_remove_host_resets_agent_state(self, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            with with_host(cephadm_module, 'host2'):
                _seed_agent_state(cephadm_module, 'host2')
            assert not _has_agent_state(cephadm_module, 'host2')

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    def test_use_agent_off_clears_stored_state(self, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            _seed_agent_state(cephadm_module, 'host1')
            cephadm_module.use_agent = False
            cephadm_module.agent_helpers._handle_use_agent_setting()
            assert not _has_agent_state(cephadm_module, 'host1')


class TestAgentApplyAndRemove:

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.CephadmOrchestrator._kick_serve_loop")
    def test_apply_refused_without_use_agent(self, _kick, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'test'):
            cephadm_module.use_agent = False
            with pytest.raises(OrchestratorError, match='use_agent'):
                cephadm_module._apply_service_spec(CephadmAgentHelpers.agent_spec())
            assert 'agent' not in cephadm_module.spec_store

    @pytest.mark.parametrize('applied', [
        # hand-written spec: no ssl, which leaves agents with empty
        # listener credentials
        ServiceSpec('agent', placement=PlacementSpec(host_pattern='*')),
        # no placement: used to fail with a KeyError on the placement defaults
        ServiceSpec('agent'),
        # subset of hosts
        ServiceSpec('agent', placement=PlacementSpec(label='foo'), ssl=True,
                    certificate_source=CertificateSource.CEPHADM_SIGNED.value),
        # unmanaged
        ServiceSpec('agent', placement=PlacementSpec(host_pattern='*'), unmanaged=True,
                    ssl=True, certificate_source=CertificateSource.CEPHADM_SIGNED.value),
    ])
    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.CephadmOrchestrator._kick_serve_loop")
    def test_apply_normalizes_spec(self, _kick, cephadm_module: CephadmOrchestrator, applied):
        with with_host(cephadm_module, 'test'):
            cephadm_module.use_agent = True
            msg = cephadm_module._apply_service_spec(applied)
            assert 'WARNING: the agent service is managed by' in msg
            spec = cephadm_module.spec_store.active_specs['agent']
            _assert_canonical_agent_spec(spec)
            assert not spec.unmanaged
            cephadm_module.use_agent = False

    @mock.patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.CephadmOrchestrator._kick_serve_loop")
    def test_apply_canonical_spec_no_warning(self, _kick, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'test'):
            cephadm_module.use_agent = True
            msg = cephadm_module._apply_service_spec(CephadmAgentHelpers.agent_spec())
            assert 'WARNING' not in msg
            _assert_canonical_agent_spec(cephadm_module.spec_store.active_specs['agent'])
            cephadm_module.use_agent = False

    @mock.patch("cephadm.module.CephadmOrchestrator._kick_serve_loop")
    def test_rm_agent_service(self, _kick, cephadm_module: CephadmOrchestrator):
        store = cephadm_module.spec_store
        store.save(CephadmAgentHelpers.agent_spec())

        cephadm_module.use_agent = True
        with pytest.raises(OrchestratorError, match='use_agent false'):
            wait(cephadm_module, cephadm_module.remove_service('agent'))
        assert 'agent' in store.active_specs

        # with use_agent=false, removing a leftover agent service is fine
        cephadm_module.use_agent = False
        assert wait(cephadm_module, cephadm_module.remove_service('agent')) == 'Removed service agent'
        assert 'agent' in store.spec_deleted
