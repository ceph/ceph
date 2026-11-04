import threading
import time
from typing import Dict, List, Optional
from unittest import mock

from cephadm.agent import AgentMessage, AgentMessenger


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
                 lock: Optional[threading.Lock] = None) -> None:
        super().__init__(host, 1234, mgr, daemon_spec)
        self.sent = sent
        self.gate = gate
        self.tracker = tracker
        self.lock = lock or threading.Lock()

    def send(self) -> None:
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
