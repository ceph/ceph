import socket
import threading
import time
from typing import Dict, List, Optional
from unittest import mock

from cephadm.agent import AGENT_MESSAGE_TIMEOUT, AgentMessage, AgentMessenger


def _fake_mgr(max_concurrent: int = 2) -> mock.MagicMock:
    mgr = mock.MagicMock()
    mgr.agent_max_concurrent_messages = max_concurrent
    mgr.agent_cache.sending_agent_message = {}
    mgr.agent_cache.agent_counter = {}
    return mgr


class _RecordingMessage(AgentMessage):
    """AgentMessage whose send() records the call and can be held open."""

    def __init__(self, host: str, mgr: mock.MagicMock, sent: List['_RecordingMessage'],
                 gate: Optional[threading.Event] = None,
                 daemon_spec: Optional[mock.MagicMock] = None,
                 tracker: Optional[Dict[str, int]] = None,
                 lock: Optional[threading.Lock] = None,
                 ok: bool = True) -> None:
        super().__init__(host, 1234, mgr, daemon_spec)
        self.sent = sent
        self.gate = gate
        self.tracker = tracker
        self.lock = lock or threading.Lock()
        self.ok = ok

    def send(self) -> bool:
        # the in-flight flag is set before the message runs
        assert self.mgr.agent_cache.sending_agent_message[self.host] is True
        if self.tracker is not None:
            with self.lock:
                self.tracker['now'] += 1
                self.tracker['max'] = max(self.tracker['max'], self.tracker['now'])
        if self.gate is not None:
            assert self.gate.wait(10)
        if self.tracker is not None:
            with self.lock:
                self.tracker['now'] -= 1
        self.sent.append(self)
        return self.ok


class _FakeClock:
    """Controllable monotonic clock for back-off tests."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _wait_idle(messenger: AgentMessenger, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if messenger.stats() == {'workers': 0, 'queued': 0, 'running': 0}:
            return
        time.sleep(0.01)
    raise AssertionError(f'messenger still busy: {messenger.stats()}')


def _wait_running(messenger: AgentMessenger, count: int, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if messenger.stats()['running'] == count:
            return
        time.sleep(0.01)
    raise AssertionError(f'expected {count} running: {messenger.stats()}')


def test_concurrency_is_bounded():
    mgr = _fake_mgr(max_concurrent=2)
    messenger = AgentMessenger(mgr)
    gate = threading.Event()
    sent: List[_RecordingMessage] = []
    tracker = {'now': 0, 'max': 0}
    lock = threading.Lock()
    hosts = [f'host{i}' for i in range(6)]
    for h in hosts:
        messenger.send(_RecordingMessage(h, mgr, sent, gate, tracker=tracker, lock=lock))
    _wait_running(messenger, 2)
    stats = messenger.stats()
    assert stats['workers'] == 2
    assert stats['queued'] + stats['running'] == 6
    gate.set()
    _wait_idle(messenger)
    assert sorted(m.host for m in sent) == hosts
    assert tracker['max'] <= 2
    assert all(mgr.agent_cache.sending_agent_message[h] is False for h in hosts)


def test_host_is_marked_in_flight_before_it_runs():
    mgr = _fake_mgr(max_concurrent=1)
    messenger = AgentMessenger(mgr)
    gate = threading.Event()
    sent: List[_RecordingMessage] = []
    messenger.send(_RecordingMessage('busy', mgr, sent, gate))
    messenger.send(_RecordingMessage('waiting', mgr, sent))
    # 'waiting' has not started yet but is already reported as being messaged
    assert mgr.agent_cache.sending_agent_message['waiting'] is True
    gate.set()
    _wait_idle(messenger)
    assert mgr.agent_cache.sending_agent_message['waiting'] is False


def test_queued_messages_for_a_host_are_coalesced():
    mgr = _fake_mgr(max_concurrent=1)
    messenger = AgentMessenger(mgr)
    gate = threading.Event()
    sent: List[_RecordingMessage] = []
    config = mock.MagicMock(name='daemon_spec')
    messenger.send(_RecordingMessage('busy', mgr, sent, gate))
    _wait_running(messenger, 1)
    messenger.send(_RecordingMessage('h', mgr, sent, daemon_spec=config))
    messenger.send(_RecordingMessage('h', mgr, sent))
    messenger.send(_RecordingMessage('h', mgr, sent))
    assert messenger.stats()['queued'] == 1
    gate.set()
    _wait_idle(messenger)
    h_msgs = [m for m in sent if m.host == 'h']
    assert len(h_msgs) == 1
    # the newest message is sent and keeps the pending config push
    assert h_msgs[0].daemon_spec is config


def test_message_for_a_running_host_is_sent_after_it():
    mgr = _fake_mgr(max_concurrent=4)
    messenger = AgentMessenger(mgr)
    gate = threading.Event()
    sent: List[_RecordingMessage] = []
    messenger.send(_RecordingMessage('h', mgr, sent, gate))
    _wait_running(messenger, 1)
    messenger.send(_RecordingMessage('h', mgr, sent))
    # never two messages to the same host at the same time
    assert messenger.stats()['running'] == 1
    assert messenger.stats()['queued'] == 0
    gate.set()
    _wait_idle(messenger)
    assert [m.host for m in sent] == ['h', 'h']
    assert mgr.agent_cache.sending_agent_message['h'] is False


def test_unexpected_error_does_not_leak_the_in_flight_flag():
    mgr = _fake_mgr(max_concurrent=1)
    messenger = AgentMessenger(mgr)
    msg = AgentMessage('h', 1234, mgr)
    with mock.patch.object(msg, 'send', side_effect=RuntimeError('boom')):
        messenger.send(msg)
        _wait_idle(messenger)
    assert mgr.agent_cache.sending_agent_message['h'] is False


def test_shutdown_drops_queued_messages():
    mgr = _fake_mgr(max_concurrent=1)
    messenger = AgentMessenger(mgr)
    gate = threading.Event()
    sent: List[_RecordingMessage] = []
    messenger.send(_RecordingMessage('busy', mgr, sent, gate))
    _wait_running(messenger, 1)
    messenger.send(_RecordingMessage('queued', mgr, sent))
    messenger.shutdown()
    assert mgr.agent_cache.sending_agent_message['queued'] is False
    messenger.send(_RecordingMessage('late', mgr, sent))
    gate.set()
    _wait_idle(messenger)
    assert [m.host for m in sent] == ['busy']


def test_payload_uses_the_counter_at_send_time():
    mgr = _fake_mgr()
    mgr.agent_cache.agent_counter = {'h': 1}
    msg = AgentMessage('h', 1234, mgr)
    mgr.agent_cache.agent_counter['h'] = 3
    assert '"counter": 3' in msg._payload()


def test_failed_message_adds_backoff():
    mgr = _fake_mgr(max_concurrent=1)
    clock = _FakeClock()
    messenger = AgentMessenger(mgr, clock=clock)
    sent: List[_RecordingMessage] = []
    messenger.send(_RecordingMessage('h', mgr, sent, ok=False))
    _wait_idle(messenger)
    assert [m.ok for m in sent] == [False]
    # the host gets no new message during the 5 s window
    messenger.send(_RecordingMessage('h', mgr, sent))
    assert messenger.stats() == {'workers': 0, 'queued': 0, 'running': 0}
    assert mgr.agent_cache.sending_agent_message['h'] is False
    # after the window a message is sent again
    clock.advance(5.0)
    messenger.send(_RecordingMessage('h', mgr, sent))
    _wait_idle(messenger)
    assert [m.ok for m in sent] == [False, True]


def test_backoff_doubles_and_is_capped():
    mgr = _fake_mgr(max_concurrent=1)
    clock = _FakeClock()
    messenger = AgentMessenger(mgr, clock=clock)
    sent: List[_RecordingMessage] = []
    messenger.send(_RecordingMessage('h', mgr, sent, ok=False))
    _wait_idle(messenger)
    # after N consecutive failures the window is min(5 * 2**(N-1), 300)
    for failures, window in enumerate([5, 10, 20, 40, 80, 160, 300, 300], start=1):
        clock.advance(window - 0.1)
        messenger.send(_RecordingMessage('h', mgr, sent))
        assert len(sent) == failures, \
            f'message sent after only {window - 0.1} s of the {window} s window'
        clock.advance(0.1)
        messenger.send(_RecordingMessage('h', mgr, sent, ok=False))
        _wait_idle(messenger)
        assert len(sent) == failures + 1


def test_successful_message_ends_backoff():
    mgr = _fake_mgr(max_concurrent=1)
    clock = _FakeClock()
    messenger = AgentMessenger(mgr, clock=clock)
    sent: List[_RecordingMessage] = []
    messenger.send(_RecordingMessage('h', mgr, sent, ok=False))
    _wait_idle(messenger)
    clock.advance(5.0)
    messenger.send(_RecordingMessage('h', mgr, sent))
    _wait_idle(messenger)
    # the failure count restarted: the next failure backs off 5 s again
    messenger.send(_RecordingMessage('h', mgr, sent, ok=False))
    _wait_idle(messenger)
    clock.advance(4.9)
    messenger.send(_RecordingMessage('h', mgr, sent))
    assert [m.ok for m in sent] == [False, True, False]  # dropped: 5 s window after a single failure
    clock.advance(0.1)
    messenger.send(_RecordingMessage('h', mgr, sent))
    _wait_idle(messenger)
    assert [m.ok for m in sent] == [False, True, False, True]


def test_reset_backoff_allows_immediate_send():
    mgr = _fake_mgr(max_concurrent=1)
    clock = _FakeClock()
    messenger = AgentMessenger(mgr, clock=clock)
    sent: List[_RecordingMessage] = []
    messenger.send(_RecordingMessage('h', mgr, sent, ok=False))
    _wait_idle(messenger)
    # e.g. the agent on the host was redeployed
    messenger.reset_backoff('h')
    messenger.send(_RecordingMessage('h', mgr, sent))
    _wait_idle(messenger)
    assert [m.ok for m in sent] == [False, True]


def test_queued_message_is_dropped_while_host_backs_off():
    mgr = _fake_mgr(max_concurrent=1)
    clock = _FakeClock()
    messenger = AgentMessenger(mgr, clock=clock)
    gate = threading.Event()
    sent: List[_RecordingMessage] = []
    # the first message for 'h' is being sent; a second one waits behind it
    messenger.send(_RecordingMessage('h', mgr, sent, gate, ok=False))
    _wait_running(messenger, 1)
    messenger.send(_RecordingMessage('h', mgr, sent))
    # the running message fails and arms the 5 s back-off; the queued
    # message is dropped instead of sent
    gate.set()
    _wait_idle(messenger)
    assert [m.host for m in sent] == ['h']
    assert mgr.agent_cache.sending_agent_message['h'] is False
    # the drop did not count as a failure: a new message goes through 5 s
    # after the failure, not 10 s
    clock.advance(5.0)
    messenger.send(_RecordingMessage('h', mgr, sent))
    _wait_idle(messenger)
    assert len(sent) == 2


def test_send_returns_whether_the_agent_replied():
    mgr = _socket_mgr()
    raw, secure, ctx = _patched_socket()
    mgr.http_server.agent.get_client_ssl_context.return_value = ctx
    with mock.patch('cephadm.agent.socket.socket', return_value=raw):
        assert AgentMessage('h', 1234, mgr).send() is True
    raw, secure, ctx = _patched_socket(ConnectionRefusedError('refused'))
    mgr.http_server.agent.get_client_ssl_context.return_value = ctx
    with mock.patch('cephadm.agent.socket.socket', return_value=raw), \
            mock.patch('cephadm.agent.time.sleep'):
        assert AgentMessage('h', 1234, mgr).send() is False


def _socket_mgr() -> mock.MagicMock:
    mgr = _fake_mgr()
    mgr.agent_cache.agent_counter = {'h': 1}
    mgr.inventory.__contains__.return_value = False
    return mgr


def _patched_socket(connect_side_effect: Optional[BaseException] = None):
    raw = mock.MagicMock(name='raw_socket')
    raw.__enter__.return_value = raw
    secure = mock.MagicMock(name='tls_socket')
    secure.__enter__.return_value = secure
    secure.recv.return_value = b'ok'
    if connect_side_effect is not None:
        secure.connect.side_effect = connect_side_effect
    ctx = mock.MagicMock(name='ssl_ctx')
    ctx.wrap_socket.return_value = secure
    return raw, secure, ctx


def test_send_sets_a_timeout_and_closes_the_socket():
    mgr = _socket_mgr()
    raw, secure, ctx = _patched_socket()
    mgr.http_server.agent.get_client_ssl_context.return_value = ctx
    msg = AgentMessage('h', 1234, mgr)
    with mock.patch('cephadm.agent.socket.socket', return_value=raw):
        msg.send()
    assert msg.get_agent_response() == 'ok'
    raw.settimeout.assert_called_once_with(AGENT_MESSAGE_TIMEOUT)
    assert ctx.wrap_socket.call_args[0][0] is raw
    raw.__exit__.assert_called_once()
    secure.__exit__.assert_called_once()
    mgr.log.error.assert_not_called()


def test_refused_connection_is_retried_once():
    mgr = _socket_mgr()
    raw, secure, ctx = _patched_socket(ConnectionRefusedError('refused'))
    mgr.http_server.agent.get_client_ssl_context.return_value = ctx
    with mock.patch('cephadm.agent.socket.socket', return_value=raw), \
            mock.patch('cephadm.agent.time.sleep') as sleep:
        AgentMessage('h', 1234, mgr).send()
    assert secure.connect.call_count == 2
    sleep.assert_called_once()
    assert raw.__exit__.call_count == 2
    mgr.log.error.assert_called_once()


def test_refused_then_accepted_connection_is_delivered():
    mgr = _socket_mgr()
    raw, secure, ctx = _patched_socket()
    secure.connect.side_effect = [ConnectionRefusedError('refused'), None]
    mgr.http_server.agent.get_client_ssl_context.return_value = ctx
    msg = AgentMessage('h', 1234, mgr)
    with mock.patch('cephadm.agent.socket.socket', return_value=raw), \
            mock.patch('cephadm.agent.time.sleep'):
        msg.send()
    assert secure.connect.call_count == 2
    assert msg.get_agent_response() == 'ok'
    mgr.log.error.assert_not_called()


def test_timeout_is_not_retried():
    mgr = _socket_mgr()
    raw, secure, ctx = _patched_socket(socket.timeout('timed out'))
    mgr.http_server.agent.get_client_ssl_context.return_value = ctx
    with mock.patch('cephadm.agent.socket.socket', return_value=raw), \
            mock.patch('cephadm.agent.time.sleep') as sleep:
        AgentMessage('h', 1234, mgr).send()
    assert secure.connect.call_count == 1
    sleep.assert_not_called()
    raw.__exit__.assert_called_once()
    mgr.log.error.assert_called_once()
