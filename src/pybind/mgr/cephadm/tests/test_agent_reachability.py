import datetime
import json
from unittest import mock

import pytest

from ceph.utils import datetime_now
from orchestrator import DaemonDescription, DaemonDescriptionStatus
from orchestrator.module import Format

from cephadm.agent import AgentMessageThread, CephadmAgentHelpers, HostData
from cephadm.inventory import (
    AGENT_FAILURE_SUMMARY_INTERVAL,
    AGENT_MSG_BACKOFF_BASE,
    AGENT_MSG_BACKOFF_MAX,
    AGENT_UNREACHABLE_FAILURES,
)
from cephadm.module import CephadmOrchestrator
from cephadm.serve import CephadmServe
from cephadm.tests.fixtures import _run_cephadm, wait, with_cephadm_module, with_host
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

    def test_failed_listener_skips_grace(
            self, cephadm_module: CephadmOrchestrator, agent_host: str) -> None:
        cephadm_module.agent_cache.record_agent_deployed(agent_host)
        cephadm_module.agent_cache.record_agent_listener_state(agent_host, 'failed', 'PEM lib')
        assert cephadm_module.agent_helpers._update_agent_unreachable_healthcheck([]) == [agent_host]
        hc = cephadm_module.health_checks[self.HC]
        assert 'agent listener is down: PEM lib' in hc['detail'][0]

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


class TestAgentFailureSummary:

    def test_rate_limited_and_reset(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        ac.record_agent_message_failure('host1', 'refused')
        ac.record_agent_message_failure('host1', 'refused')
        ac.record_agent_message_failure('host2', 'timed out')
        s = ac.take_agent_message_summary()
        assert s is not None
        assert s[1] == {'host1': 2, 'host2': 1}
        # too soon for another summary; failures keep accumulating
        ac.record_agent_message_failure('host1', 'refused')
        assert ac.take_agent_message_summary() is None
        clock.now += AGENT_FAILURE_SUMMARY_INTERVAL
        s = ac.take_agent_message_summary()
        assert s is not None
        assert s[0] == AGENT_FAILURE_SUMMARY_INTERVAL
        assert s[1] == {'host1': 1}

    def test_recovered_hosts(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        ac.record_agent_message_success('host0')  # never failed: not "recovered"
        ac.record_agent_message_failure('host1', 'refused')
        ac.record_agent_message_success('host1')
        ac.record_agent_message_failure('host2', 'refused')
        ac.record_agent_message_success('host2')
        ac.record_agent_message_failure('host2', 'refused')
        s = ac.take_agent_message_summary()
        assert s is not None
        assert s[2] == {'host1'}

    def test_one_warning_per_pass(self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        for i in range(8):
            ac.record_agent_message_failure(f'host{i}', f'refused {i}')
        ac.record_agent_message_failure('host7', 'refused 7')
        with mock.patch.object(cephadm_module.log, 'warning') as warn, \
                mock.patch.object(cephadm_module.log, 'info') as info:
            cephadm_module.agent_helpers._log_agent_message_summary(['host7'])
            assert warn.call_count == 1
            msg = warn.call_args.args[0]
            assert msg.startswith('9 message(s) to 8 agent(s) failed since mgr start')
            assert msg.index('host7: refused 7') < msg.index('host0: refused 0')
            assert 'and 3 more' in msg
            assert '1 agent(s) are unreachable' in msg
            # nothing new and too soon: no log
            cephadm_module.agent_helpers._log_agent_message_summary(['host7'])
            assert warn.call_count == 1
            # only recoveries: one INFO line
            ac.record_agent_message_success('host7')
            clock.now += AGENT_FAILURE_SUMMARY_INTERVAL
            cephadm_module.agent_helpers._log_agent_message_summary([])
            assert warn.call_count == 1
            info.assert_called_once()
            assert 'host7' in info.call_args.args[0]

    @mock.patch('cephadm.agent.time.sleep')
    @mock.patch('cephadm.agent.ssl.create_default_context')
    def test_failed_message_is_not_logged_as_error(self, ssl_ctx: mock.MagicMock, sleep: mock.MagicMock,
                                                   cephadm_module: CephadmOrchestrator,
                                                   clock: _Clock) -> None:
        ssl_ctx.return_value.wrap_socket.return_value.connect.side_effect = ConnectionRefusedError(111, 'refused')
        cephadm_module.http_server.agent = mock.MagicMock()
        with mock.patch.object(cephadm_module.cert_mgr, 'get_root_ca', return_value='ca'), \
                mock.patch.object(cephadm_module.cert_mgr, 'generate_cert',
                                  return_value=TLSCredentials('cert', 'key')), \
                mock.patch.object(cephadm_module.log, 'error') as error, \
                mock.patch.object(cephadm_module.log, 'warning') as warning:
            AgentMessageThread('host1', 4721, {'counter': 1}, cephadm_module).run()
        error.assert_not_called()
        warning.assert_not_called()


class TestAgentListenerState:

    def _report(self, m: CephadmOrchestrator, host: str, **listener: str) -> None:
        data = {'host': host, 'port': '4721', 'ack': '1'}
        if listener:
            data['listener'] = listener  # type: ignore
        with mock.patch.object(m.agent_cache, 'save_agent'):
            HostData(m).handle_metadata(data)

    def test_failed_listener_flags_host_at_once(
            self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        ac.agent_counter['host1'] = 5  # stale ack: would normally message the agent
        with mock.patch('cephadm.agent.AgentMessageThread') as thread, \
                mock.patch.object(cephadm_module.agent_helpers, '_request_agent_acks',
                                  lambda *a, **kw: _real_request_agent_acks(cephadm_module.agent_helpers, *a, **kw)):
            self._report(cephadm_module, 'host1', state='failed', error='SSLError: PEM lib')
            thread.assert_not_called()
        assert ac.agent_unreachable('host1')
        assert not ac.agent_message_allowed('host1')
        r = ac.get_agent_reachability('host1')
        assert r is not None
        assert r.listener_state == 'failed'
        assert r.last_error == 'agent listener is down: SSLError: PEM lib'
        # not a failed message
        s = ac.take_agent_message_summary()
        assert s is not None and s[1] == {}

    def test_listener_back_allows_messages(
            self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        ac = cephadm_module.agent_cache
        ac.agent_counter['host1'] = 1
        self._report(cephadm_module, 'host1', state='failed', error='boom')
        # a back-off from an earlier failed message doesn't hold it back either
        ac.record_agent_message_failure('host1', 'refused')
        self._report(cephadm_module, 'host1', state='listening', error='')
        assert not ac.agent_unreachable('host1')
        assert ac.agent_message_allowed('host1')
        s = ac.take_agent_message_summary()
        assert s is not None and s[2] == {'host1'}

    def test_old_agent_without_listener_field(
            self, cephadm_module: CephadmOrchestrator, clock: _Clock) -> None:
        cephadm_module.agent_cache.agent_counter['host1'] = 1
        self._report(cephadm_module, 'host1')
        assert cephadm_module.agent_cache.get_agent_reachability('host1') is None
        assert cephadm_module.agent_cache.agent_message_allowed('host1')


class TestApplyWithUnreachableAgents:

    @pytest.fixture()
    def hosts(self, cephadm_module: CephadmOrchestrator, clock: _Clock):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm('[]')), \
                with_host(cephadm_module, 'host1', refresh_hosts=False), \
                with_host(cephadm_module, 'host2', refresh_hosts=False):
            cephadm_module.use_agent = True
            cephadm_module.cache.metadata_up_to_date = {'host1': True, 'host2': True}
            yield
            cephadm_module.use_agent = False

    def test_unreachable_agent_does_not_hold_back_apply(
            self, cephadm_module: CephadmOrchestrator, hosts: None) -> None:
        serve = CephadmServe(cephadm_module)
        assert not serve._wait_for_agent_metadata()
        cephadm_module.cache.metadata_up_to_date['host1'] = False
        assert serve._wait_for_agent_metadata()
        for _ in range(AGENT_UNREACHABLE_FAILURES):
            cephadm_module.agent_cache.record_agent_message_failure('host1', 'refused')
        assert not serve._wait_for_agent_metadata()
        # the upgrade still waits for every host
        assert not cephadm_module.cache.all_host_metadata_up_to_date()

    def test_wait_is_bounded(self, cephadm_module: CephadmOrchestrator, hosts: None) -> None:
        serve = CephadmServe(cephadm_module)
        cephadm_module.cache.metadata_up_to_date['host2'] = False
        assert serve._wait_for_agent_metadata()
        assert serve._agent_metadata_wait_start is not None
        serve._agent_metadata_wait_start -= datetime.timedelta(
            seconds=serve._max_agent_metadata_wait())
        with mock.patch.object(serve.log, 'warning') as warning:
            assert not serve._wait_for_agent_metadata()
            assert not serve._wait_for_agent_metadata()
            warning.assert_called_once()
            assert 'host2' in warning.call_args.args[0]
        # once metadata is up to date, the next wait starts from scratch
        cephadm_module.cache.metadata_up_to_date['host2'] = True
        assert not serve._wait_for_agent_metadata()
        cephadm_module.cache.metadata_up_to_date['host2'] = False
        assert serve._wait_for_agent_metadata()

    def test_apply_all_services_skips_non_agent_specs_while_waiting(
            self, cephadm_module: CephadmOrchestrator, hosts: None) -> None:
        cephadm_module.cache.metadata_up_to_date['host1'] = False
        serve = CephadmServe(cephadm_module)
        with mock.patch.object(serve, '_apply_service', return_value=False) as apply, \
                mock.patch.object(type(cephadm_module.spec_store), '__getitem__') as getitem:
            serve._apply_all_services()
            getitem.assert_called_with('agent')
            assert apply.call_count == 1


class TestAgentStatus:

    @pytest.fixture()
    def agents(self, cephadm_module: CephadmOrchestrator, clock: _Clock):
        with mock.patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm('[]')), \
                with_host(cephadm_module, 'host1', refresh_hosts=False), \
                with_host(cephadm_module, 'host2', refresh_hosts=False):
            ac = cephadm_module.agent_cache
            for h in ['host1', 'host2']:
                cephadm_module.cache.add_daemon(h, DaemonDescription(
                    'agent', h, h, status=DaemonDescriptionStatus.running,
                    status_desc='running', started=datetime_now()))
                ac.agent_ports[h] = 4721
                ac.agent_counter[h] = 3
                ac.agent_timestamp[h] = datetime_now()
            cephadm_module.cache.metadata_up_to_date = {'host1': True, 'host2': False}
            ac.record_agent_listener_state('host1', 'listening', '')
            ac.record_agent_message_success('host1')
            for _ in range(AGENT_UNREACHABLE_FAILURES):
                ac.record_agent_message_failure('host2', 'connection refused')
            yield

    def test_agent_status(self, cephadm_module: CephadmOrchestrator, agents: None) -> None:
        st = {e['hostname']: e for e in cephadm_module.agent_helpers.agent_status()}
        assert st['host1']['reachability'] == 'ok'
        assert st['host1']['listener_state'] == 'listening'
        assert st['host1']['metadata_up_to_date'] is True
        assert st['host1']['last_success'] is not None
        assert st['host2']['reachability'] == 'unreachable'
        assert st['host2']['listener_state'] == 'unknown'
        assert st['host2']['metadata_up_to_date'] is False
        assert st['host2']['consecutive_failures'] == AGENT_UNREACHABLE_FAILURES
        assert st['host2']['retry_in'] > 0
        assert st['host2']['last_error'] == 'connection refused'
        assert [e['hostname'] for e in cephadm_module.agent_helpers.agent_status('host2')] == ['host2']
        assert cephadm_module.agent_helpers.agent_status('nohost') == []

    def test_agent_status_command(self, cephadm_module: CephadmOrchestrator, agents: None) -> None:
        r = cephadm_module._agent_status()
        assert r.retval == 0
        lines = r.stdout.splitlines()
        assert lines[0].split()[:3] == ['HOST', 'PORT', 'LAST']
        host2 = [line for line in lines if line.startswith('host2')][0]
        assert 'pending (counter 3)' in host2
        assert f'unreachable ({AGENT_UNREACHABLE_FAILURES} failures, retry in' in host2
        assert 'connection refused' in host2
        host1 = [line for line in lines if line.startswith('host1')][0]
        assert 'up to date' in host1 and 'connection refused' not in host1

        r = cephadm_module._agent_status(hostname='host1', format=Format.json)
        assert [e['hostname'] for e in json.loads(r.stdout)] == ['host1']
        assert cephadm_module._agent_status(format=Format.xml).retval != 0
        assert 'No cephadm agent on host nohost' in cephadm_module._agent_status(hostname='nohost').stdout

    def test_orch_ps_status(self, cephadm_module: CephadmOrchestrator, agents: None) -> None:
        def desc(host: str) -> str:
            dds = wait(cephadm_module, cephadm_module.list_daemons(daemon_type='agent', host=host))
            assert dds[0].status == DaemonDescriptionStatus.running
            return dds[0].status_desc

        assert desc('host1') == 'running'
        assert desc('host2') == 'running, unreachable'
        cephadm_module.agent_cache.record_agent_listener_state('host1', 'failed', 'PEM lib')
        assert desc('host1') == 'running, listener down'
        # the cached daemon is not altered
        dd = cephadm_module.cache.get_daemons_by_type('agent', host='host1')[0]
        assert dd.status_desc == 'running'
        # no status_desc from the host: built from the status
        dd.status_desc = None
        assert desc('host1') == 'running, listener down'
