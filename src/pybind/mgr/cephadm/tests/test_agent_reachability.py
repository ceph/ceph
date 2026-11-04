import datetime
from unittest import mock

import pytest

from ceph.utils import datetime_now
from orchestrator import DaemonDescription

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


class TestAgentUnreachableHealth:

    HC = 'CEPHADM_AGENT_UNREACHABLE'

    @pytest.fixture()
    def agent_host(self, cephadm_module: CephadmOrchestrator, clock: _Clock):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm('[]')), \
                with_host(cephadm_module, 'test', refresh_hosts=False):
            cephadm_module.use_agent = True
            cephadm_module.agent_cache.agent_ports['test'] = 4721
            cephadm_module.cache.add_daemon('test', DaemonDescription(
                'agent', 'test', 'test',
                last_deployed=datetime_now() - datetime.timedelta(hours=1)))
            yield 'test'
            cephadm_module.use_agent = False

    def _fail(self, m: CephadmOrchestrator, host: str, n: int = AGENT_UNREACHABLE_FAILURES) -> None:
        for _ in range(n):
            m.agent_cache.record_agent_message_failure(host, 'connection refused')

    def test_raised_after_threshold(self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        helpers = cephadm_module.agent_helpers
        self._fail(cephadm_module, agent_host, AGENT_UNREACHABLE_FAILURES - 1)
        assert helpers._update_agent_unreachable_healthcheck([]) == []
        assert self.HC not in cephadm_module.health_checks

        self._fail(cephadm_module, agent_host, 1)
        assert helpers._update_agent_unreachable_healthcheck([]) == [agent_host]
        hc = cephadm_module.health_checks[self.HC]
        assert hc['count'] == 1
        assert 'host test (port 4721)' in hc['detail'][0]
        assert 'connection refused' in hc['detail'][0]

        cephadm_module.agent_cache.record_agent_message_success(agent_host)
        assert helpers._update_agent_unreachable_healthcheck([]) == []
        assert self.HC not in cephadm_module.health_checks

    def test_not_pushed_again_when_unchanged(
            self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        self._fail(cephadm_module, agent_host)
        with mock.patch.object(cephadm_module, 'set_health_checks') as set_hc:
            cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([])
            self._fail(cephadm_module, agent_host)
            cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([])
            assert set_hc.call_count == 1

    def test_grace_after_deploy(self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        cephadm_module.agent_cache.record_agent_deployed(agent_host)
        self._fail(cephadm_module, agent_host)
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([]) == []

        r = cephadm_module.agent_cache.agent_reachability[agent_host]
        assert r.deployed_at is not None
        r.deployed_at -= datetime.timedelta(
            seconds=cephadm_module.agent_helpers._agent_unreachable_grace() + 1)
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([]) == [agent_host]

    def test_grace_from_daemon_last_deployed(
            self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        # e.g. after a mgr failover: no in-memory deploy time
        dd = cephadm_module.cache.get_daemons_by_type('agent', host=agent_host)[0]
        dd.last_deployed = datetime_now()
        self._fail(cephadm_module, agent_host)
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([]) == []

    def test_down_agents_are_not_reported(
            self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        self._fail(cephadm_module, agent_host)
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([agent_host]) == []

    def test_offline_and_draining_hosts_are_not_reported(
            self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        self._fail(cephadm_module, agent_host)
        cephadm_module.offline_hosts.add(agent_host)
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([]) == []
        cephadm_module.offline_hosts.discard(agent_host)
        with mock.patch.object(cephadm_module.cache, 'is_host_draining', return_value=True):
            assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([]) == []

    def test_cleared_when_agent_disabled(
            self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        self._fail(cephadm_module, agent_host)
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([])
        cephadm_module.use_agent = False
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([]) == []
        assert self.HC not in cephadm_module.health_checks
