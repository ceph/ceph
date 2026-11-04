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


class TestUseAgentWaiting:

    def _setup(self, m: CephadmOrchestrator, hosts, agent_hosts, reported_hosts):
        m.use_agent = True
        m.spec_store.save(CephadmAgentHelpers.agent_spec())
        for h in hosts:
            m.cache.metadata_up_to_date[h] = False
        for h in reported_hosts:
            m.agent_cache.agent_ports[h] = 4721
        agents = [DaemonDescription(daemon_type='agent', daemon_id=h, hostname=h)
                  for h in agent_hosts]
        m.http_server.agent = mock.MagicMock(server_port=7150)
        return (
            mock.patch.object(m.cache, 'get_hosts', return_value=list(hosts)),
            mock.patch.object(m.cache, 'get_unreachable_hosts', return_value=[]),
            mock.patch.object(m.cache, 'get_daemons_by_type', return_value=agents),
        )

    def test_not_waiting(self, cephadm_module: CephadmOrchestrator):
        cephadm_module.use_agent = False
        assert cephadm_module.agent_helpers.use_agent_waiting_reason() is None
        cephadm_module.use_agent = True
        with mock.patch.object(cephadm_module.cache, 'get_hosts', return_value=['h1']):
            cephadm_module.cache.metadata_up_to_date['h1'] = True
            assert cephadm_module.agent_helpers.use_agent_waiting_reason() is None
        cephadm_module.use_agent = False

    def test_waiting_reason_by_host(self, cephadm_module: CephadmOrchestrator):
        m = cephadm_module
        p1, p2, p3 = self._setup(m, ['h1', 'h2', 'h3'], agent_hosts=['h2', 'h3'],
                                 reported_hosts=['h3'])
        with p1, p2, p3:
            msg = m.agent_helpers.use_agent_waiting_reason()
        assert msg is not None
        assert 'Waiting for 3 host(s)' in msg
        assert '1 without an agent daemon (h1)' in msg
        assert '1 with no report from their agent yet (h2)' in msg
        assert '1 waiting for their agent to acknowledge the latest request (h3)' in msg
        assert 'Blocked' not in msg
        m.use_agent = False

    def test_waiting_reason_caps_host_list_and_shows_blockers(self, cephadm_module: CephadmOrchestrator):
        m = cephadm_module
        hosts = [f'h{i}' for i in range(8)]
        p1, p2, p3 = self._setup(m, hosts, agent_hosts=[], reported_hosts=[])
        spec = CephadmAgentHelpers.agent_spec()
        spec.unmanaged = True
        m.spec_store.save(spec)
        m.http_server.agent = None
        with p1, p2, p3:
            msg = m.agent_helpers.use_agent_waiting_reason()
        assert msg is not None
        assert '8 without an agent daemon (h0, h1, h2, h3, h4, and 3 more)' in msg
        assert 'the agent service is unmanaged' in msg
        assert 'the cephadm agent endpoint is not ready yet' in msg
        m.use_agent = False

    def test_report_logs_on_change_and_adds_event(self, cephadm_module: CephadmOrchestrator):
        m = cephadm_module
        p1, p2, p3 = self._setup(m, ['h1'], agent_hosts=[], reported_hosts=[])
        with p1, p2, p3, mock.patch.object(m.log, 'info') as info:
            m.agent_helpers.report_use_agent_waiting()
            m.agent_helpers.report_use_agent_waiting()
            assert info.call_count == 1
            events = m.events.get_for_service('agent')
            assert any('without an agent daemon (h1)' in e.message for e in events)

            m.cache.metadata_up_to_date['h1'] = True
            m.agent_helpers.report_use_agent_waiting()
            assert info.call_count == 2
            assert 'applying all services' in info.call_args.args[0]
        m.use_agent = False
