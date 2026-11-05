import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import cherrypy
import pytest

from cephadm.agent import HostData, _agent_sent_at
from cephadm.agent_metrics import AgentMetadataStats
from cephadm.http_server import CephadmHttpServer


NOW = 10000.0


def _request(sent_at=None, body=b'{}'):
    headers = {}
    if sent_at is not None:
        headers['X-Cephadm-Agent-Sent-At'] = str(sent_at)
    return SimpleNamespace(
        remote=SimpleNamespace(ip='10.0.0.1'),
        headers=headers,
        body=io.BytesIO(body),
    )


def _mgr(enabled=True, queued=True, report_timeout=10):
    # drop limit: report_timeout + 5 s skew margin = 15 s by default
    mgr = MagicMock()
    mgr.agent_metadata_stats = AgentMetadataStats()
    mgr.agent_drop_stale_reports = enabled
    mgr.http_server.agent.get_report_timeout.return_value = report_timeout
    mgr.http_server.agent.STALE_REPORT_SKEW_MARGIN_SECONDS = 5
    mgr.http_server.agent_requests_queued.return_value = queued
    return mgr


def _index(mgr, request):
    with patch('cephadm.agent.cherrypy.request', request), \
            patch('cephadm.agent.time.time', return_value=NOW), \
            patch.object(HostData, 'get_data', return_value=None) as get_data:
        try:
            result = HostData(mgr).index()
        except cherrypy.HTTPError as e:
            return e, get_data
        return result, get_data


@pytest.mark.parametrize('value,expected', [
    (None, None),
    ('1000.5', 1000.5),
    ('garbage', None),
    ('nan', None),
    ('inf', None),
])
def test_agent_sent_at_header(value, expected):
    request = _request()
    if value is not None:
        request.headers['X-Cephadm-Agent-Sent-At'] = value
    with patch('cephadm.agent.cherrypy.request', request):
        assert _agent_sent_at() == expected


def test_stale_report_is_dropped_before_parsing():
    mgr = _mgr()
    result, get_data = _index(mgr, _request(sent_at=NOW - 30))

    assert isinstance(result, cherrypy.HTTPError)
    assert result.status == 503
    get_data.assert_not_called()
    # A dropped report must not count as a sign of life.
    mgr.offline_hosts_remove.assert_not_called()
    reports = mgr.agent_metadata_stats.snapshot('mgr.a')['reports']
    assert reports['stale_dropped'] == 1
    assert reports['stale_drop_max_age_s'] == pytest.approx(30.0)
    assert reports['bad_metadata'] == 0


@pytest.mark.parametrize('sent_at,enabled,queued,report_timeout', [
    (NOW - 10, True, True, 10),     # within the timeout + margin
    (NOW - 30, True, True, 76),     # agent still waiting: timeout follows the period
    (NOW - 30, True, False, 10),    # old, but nothing queued: likely clock skew
    (NOW - 30, False, True, 10),    # drop disabled
    (None, True, True, 10),         # no send time
    (NOW + 30, True, True, 10),     # agent clock ahead of the mgr
])
def test_report_is_processed(sent_at, enabled, queued, report_timeout):
    mgr = _mgr(enabled=enabled, queued=queued, report_timeout=report_timeout)
    result, get_data = _index(mgr, _request(sent_at=sent_at))

    assert result == {}
    get_data.assert_called_once_with()
    reports = mgr.agent_metadata_stats.snapshot('mgr.a')['reports']
    assert reports['stale_dropped'] == 0


def test_stale_drop_shows_in_plain_stats():
    stats = AgentMetadataStats()
    stats.record_stale_drop(20.0)
    stats.record_stale_drop(42.5)
    text = stats.format_plain('mgr.a')
    assert 'stale dropped:      2' in text
    assert 'stale max age:      42.500 s' in text
    stats.reset()
    assert stats.snapshot('mgr.a')['reports']['stale_dropped'] == 0


def _http_server(requests):
    server = CephadmHttpServer.__new__(CephadmHttpServer)
    server.mgr = MagicMock()
    server.agent_adapter = None
    if requests is not None:
        server.agent_adapter = SimpleNamespace(httpserver=SimpleNamespace(requests=requests))
    return server


@pytest.mark.parametrize('requests,expected', [
    (None, False),
    (SimpleNamespace(idle=0, qsize=3), True),
    (SimpleNamespace(idle=4, qsize=0), False),
    (SimpleNamespace(idle=lambda: 0, qsize=lambda: 1), True),
    (SimpleNamespace(idle='x', qsize='y'), False),
])
def test_agent_requests_queued(requests, expected):
    assert _http_server(requests).agent_requests_queued() is expected


def test_stale_drop_end_to_end_with_real_index():
    # Same request twice through the real get_data(): only the queue state
    # decides whether the old report is dropped.
    body = json.dumps({'host': 'unknown'}).encode('utf-8')
    for queued, dropped in ((True, True), (False, False)):
        mgr = _mgr(queued=queued)
        with patch('cephadm.agent.cherrypy.request', _request(sent_at=NOW - 60, body=body)), \
                patch('cephadm.agent.time.time', return_value=NOW):
            if dropped:
                with pytest.raises(cherrypy.HTTPError):
                    HostData(mgr).index()
            else:
                result = HostData(mgr).index()
                assert 'Bad metadata' in result['result']


@pytest.mark.parametrize('hosts,age,dropped', [
    (305, 80, False),   # refresh period 76 s: limit 81 s
    (305, 82, True),
    (1000, 124, False),  # refresh period 133 s, timeout capped at 120 s: limit 125 s
    (1000, 126, True),
])
def test_drop_limit_follows_report_timeout(hosts, age, dropped):
    from cephadm.agent import AgentEndpoint

    mgr = _mgr()
    fake = SimpleNamespace(agent_avg_concurrency=-1, agent_refresh_rate=-1,
                           agent_initial_startup_delay_max=-1, agent_jitter_seconds=-1,
                           cache=SimpleNamespace(get_hosts=lambda: [f'h{i}' for i in range(hosts)]),
                           get_mgr_ip=lambda: '')
    mgr.http_server.agent = AgentEndpoint(fake)
    with patch('cephadm.agent.time.time', return_value=NOW):
        result = HostData(mgr)._stale_report_age(NOW - age)
    assert (result is not None) is dropped
