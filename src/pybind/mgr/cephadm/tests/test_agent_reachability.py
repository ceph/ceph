from unittest import mock

import pytest

from cephadm.agent import AgentMessageThread, CephadmAgentHelpers, HostData
from cephadm.inventory import (
    AGENT_MSG_BACKOFF_BASE,
    AGENT_MSG_BACKOFF_MAX,
    AGENT_UNREACHABLE_FAILURES,
)
from cephadm.module import CephadmOrchestrator
from cephadm.tests.fixtures import _run_cephadm, with_cephadm_module, with_host
from cephadm.tlsobject_types import TLSCredentials

# with_cephadm_module() mocks _request_agent_acks; keep the real one.
_real_request_agent_acks = CephadmAgentHelpers._request_agent_acks


@pytest.fixture()
def cephadm_module():
    with with_cephadm_module({}) as m:
        yield m


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture()
def clock():
    c = _Clock()
    with mock.patch('cephadm.inventory._monotonic', c):
        yield c


class TestAgentReachability:

    def test_backoff_grows_and_is_capped(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        assert ac.agent_message_allowed('host1')
        expected = AGENT_MSG_BACKOFF_BASE
        for n in range(1, 10):
            assert ac.record_agent_message_failure('host1', 'refused') == n
            assert not ac.agent_message_allowed('host1')
            r = ac.get_agent_reachability('host1')
            assert r is not None
            assert r.backoff_seconds() == min(expected, AGENT_MSG_BACKOFF_MAX)
            clock.now += r.backoff_seconds()
            assert ac.agent_message_allowed('host1')
            expected *= 2
        # other hosts are not affected
        assert ac.agent_message_allowed('host2')

    def test_unreachable_after_threshold(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        for _ in range(AGENT_UNREACHABLE_FAILURES - 1):
            ac.record_agent_message_failure('host1', 'refused')
        assert not ac.agent_unreachable('host1')
        ac.record_agent_message_failure('host1', 'refused')
        assert ac.agent_unreachable('host1')
        r = ac.get_agent_reachability('host1')
        assert r is not None
        assert r.last_error == 'refused'
        assert r.first_failure is not None

    def test_success_resets(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        for _ in range(AGENT_UNREACHABLE_FAILURES):
            ac.record_agent_message_failure('host1', 'refused')
        ac.record_agent_message_success('host1')
        assert not ac.agent_unreachable('host1')
        assert ac.agent_message_allowed('host1')
        r = ac.get_agent_reachability('host1')
        assert r is not None
        assert r.consecutive_failures == 0
        assert r.total_failures == AGENT_UNREACHABLE_FAILURES
        assert r.last_success is not None

    def test_ssh_deploy_clears_backoff(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        for _ in range(AGENT_UNREACHABLE_FAILURES):
            ac.record_agent_message_failure('host1', 'refused')
        ac.record_agent_deployed('host1')
        assert ac.agent_message_allowed('host1')
        assert not ac.agent_unreachable('host1')
        r = ac.get_agent_reachability('host1')
        assert r is not None
        assert r.total_failures == AGENT_UNREACHABLE_FAILURES

    @mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm('[]'))
    def test_reset_on_host_removal(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        with with_host(cephadm_module, 'test'):
            cephadm_module.agent_cache.record_agent_message_failure('test', 'refused')
            assert cephadm_module.agent_cache.get_agent_reachability('test') is not None
        assert cephadm_module.agent_cache.get_agent_reachability('test') is None

    @mock.patch('cephadm.agent.AgentMessageThread')
    def test_request_acks_skips_hosts_in_backoff(self, thread: mock.MagicMock,
                                                 cephadm_module: CephadmOrchestrator,
                                                 clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        ac.agent_ports = {'host1': 4721, 'host2': 4721}
        ac.record_agent_message_failure('host1', 'refused')

        _real_request_agent_acks(cephadm_module.agent_helpers, {'host1', 'host2'}, increment=True)
        assert [c.args[0] for c in thread.call_args_list] == ['host2']
        # the counter still moves and the host is still marked out of date
        assert ac.agent_counter == {'host1': 1, 'host2': 1}
        assert cephadm_module.cache.metadata_up_to_date['host1'] is False

        thread.reset_mock()
        clock.now += AGENT_MSG_BACKOFF_BASE
        _real_request_agent_acks(cephadm_module.agent_helpers, {'host1'})
        assert [c.args[0] for c in thread.call_args_list] == ['host1']

    def test_stale_reports_dont_message_agent_in_backoff(
            self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        ac.agent_counter['host1'] = 5
        ac.record_agent_message_failure('host1', 'refused')
        with mock.patch('cephadm.agent.AgentMessageThread') as thread, \
                mock.patch.object(cephadm_module.agent_helpers, '_request_agent_acks',
                                  lambda *a, **kw: _real_request_agent_acks(cephadm_module.agent_helpers, *a, **kw)), \
                mock.patch.object(cephadm_module.agent_cache, 'save_agent'):
            for _ in range(10):
                HostData(cephadm_module).handle_metadata(
                    {'host': 'host1', 'port': '4721', 'ack': '1'})
            thread.assert_not_called()


class TestAgentMessageThread:

    def _thread(self, m: CephadmOrchestrator) -> AgentMessageThread:
        m.http_server.agent = mock.MagicMock()
        return AgentMessageThread('host1', 4721, {'counter': 1}, m)

    @mock.patch('cephadm.agent.time.sleep')
    @mock.patch('cephadm.agent.ssl.create_default_context')
    def test_refused_connection_is_recorded(self, ssl_ctx: mock.MagicMock, sleep: mock.MagicMock,
                                            cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ssl_ctx.return_value.wrap_socket.return_value.connect.side_effect = ConnectionRefusedError(111, 'refused')
        with mock.patch.object(cephadm_module.cert_mgr, 'get_root_ca', return_value='ca'), \
                mock.patch.object(cephadm_module.cert_mgr, 'generate_cert',
                                  return_value=TLSCredentials('cert', 'key')):
            self._thread(cephadm_module).run()
        r = cephadm_module.agent_cache.get_agent_reachability('host1')
        assert r is not None
        assert r.consecutive_failures == 1
        assert 'refused' in r.last_error
        assert not cephadm_module.agent_cache.messaging_agent('host1')
        assert not cephadm_module.agent_cache.agent_message_allowed('host1')

    @mock.patch('cephadm.agent.ssl.create_default_context')
    def test_other_error_is_recorded(self, ssl_ctx: mock.MagicMock,
                                     cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ssl_ctx.return_value.wrap_socket.return_value.connect.side_effect = TimeoutError('timed out')
        with mock.patch.object(cephadm_module.cert_mgr, 'get_root_ca', return_value='ca'), \
                mock.patch.object(cephadm_module.cert_mgr, 'generate_cert',
                                  return_value=TLSCredentials('cert', 'key')):
            self._thread(cephadm_module).run()
        r = cephadm_module.agent_cache.get_agent_reachability('host1')
        assert r is not None
        assert r.consecutive_failures == 1
        assert 'timed out' in r.last_error

    @mock.patch('cephadm.agent.ssl.create_default_context')
    def test_success_is_recorded(self, ssl_ctx: mock.MagicMock,
                                 cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ssl_ctx.return_value.wrap_socket.return_value.recv.return_value = b'ACK'
        cephadm_module.agent_cache.record_agent_message_failure('host1', 'refused')
        with mock.patch.object(cephadm_module.cert_mgr, 'get_root_ca', return_value='ca'), \
                mock.patch.object(cephadm_module.cert_mgr, 'generate_cert',
                                  return_value=TLSCredentials('cert', 'key')):
            self._thread(cephadm_module).run()
        r = cephadm_module.agent_cache.get_agent_reachability('host1')
        assert r is not None
        assert r.consecutive_failures == 0
        assert r.last_success is not None

    def test_local_cert_error_is_not_counted(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        with mock.patch.object(cephadm_module.cert_mgr, 'get_root_ca', side_effect=Exception('no CA')):
            self._thread(cephadm_module).run()
        assert cephadm_module.agent_cache.get_agent_reachability('host1') is None
        assert not cephadm_module.agent_cache.messaging_agent('host1')
