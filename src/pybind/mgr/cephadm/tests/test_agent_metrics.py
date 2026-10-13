import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest
import io
import json

from cephadm.agent import (
    AgentEndpoint,
    _agent_stats_request_end,
    _agent_stats_request_start,
)
from cephadm.agent_metrics import AgentMetadataStats
from cephadm.http_server import CephadmHttpServer


def test_agent_request_store_attribution_and_shape() -> None:
    stats = AgentMetadataStats()

    with patch('cephadm.agent_metrics.time.monotonic', side_effect=[10.0, 10.1, 10.2, 10.3, 10.4]):
        stats.begin_agent_request()
        stats.record_report_shape({'ls': '[{}]', 'volume': '[{}]'})
        stats.record_report_state(first_contact=False, stale_ack=True)
        stats.record_store('host', 0.050)
        stats.record_store('devices', 0.120)
        stats.record_store('agent', 0.020)
        stats.finish_agent_request()

    # A cache write outside an agent request must be kept out of the per-report bucket.
    stats.record_store('host', 0.010)

    result = stats.snapshot('mgr.a')
    assert result['reports']['total'] == 1
    assert result['reports']['with_nonempty_ls'] == 1
    assert result['reports']['with_devices'] == 1
    assert result['reports']['stale_ack'] == 1
    assert result['request_latency']['histogram']['100-500ms'] == 1
    assert result['persistence']['agent_request_calls'] == 3
    assert result['persistence']['agent_request_calls_per_report'] == 3.0
    assert result['persistence']['agent_request']['host']['count'] == 1
    assert result['persistence']['agent_request']['devices']['count'] == 1
    assert result['persistence']['agent_request']['agent']['count'] == 1
    assert result['persistence']['background']['host']['count'] == 1


def test_request_hooks_count_node_proxy_and_finish_data_request() -> None:
    stats = AgentMetadataStats()
    mgr = SimpleNamespace(
        agent_metadata_stats=stats,
        http_server=SimpleNamespace(_sample_agent_pool=MagicMock(return_value=(7, 3))),
    )

    _agent_stats_request_start(mgr, 'node-proxy')
    result = stats.snapshot('mgr.a')
    assert result['http_pool']['node_proxy_requests'] == 1
    mgr.http_server._sample_agent_pool.assert_called_once_with()

    mgr.http_server._sample_agent_pool.reset_mock()
    with patch('cephadm.agent_metrics.time.monotonic', side_effect=[10.0, 10.1, 10.2]):
        _agent_stats_request_start(mgr, 'data')
        stats.record_store('agent', 0.020)
        _agent_stats_request_end(mgr, 'data')

    result = stats.snapshot('mgr.a')
    assert result['reports']['total'] == 1
    assert result['request_latency']['count'] == 1
    assert result['persistence']['agent_request']['agent']['count'] == 1
    mgr.http_server._sample_agent_pool.assert_called_once_with()


def test_agent_routes_enable_stats_tools_for_both_mounts() -> None:
    endpoint = AgentEndpoint.__new__(AgentEndpoint)
    endpoint.mgr = object()
    endpoint.host_data = object()
    endpoint.node_proxy_endpoint = object()

    mounts = endpoint.configure_routes({'/': {'tools.trailing_slash.on': False}})
    configs = {path: config for _, path, config in mounts}

    for path, endpoint_name in (('/data', 'data'), ('/node-proxy', 'node-proxy')):
        root = configs[path]['/']
        assert root['tools.cephadm_agent_stats_start.on'] is True
        assert root['tools.cephadm_agent_stats_start.endpoint'] == endpoint_name
        assert root['tools.cephadm_agent_stats_end.on'] is True
        assert root['tools.cephadm_agent_stats_end.endpoint'] == endpoint_name


def test_sample_agent_pool_property_callable_and_missing_paths() -> None:
    stats = AgentMetadataStats()
    server = CephadmHttpServer.__new__(CephadmHttpServer)
    server.mgr = SimpleNamespace(agent_metadata_stats=stats, log=MagicMock())

    server.agent_adapter = SimpleNamespace(
        httpserver=SimpleNamespace(requests=SimpleNamespace(idle=3, qsize=7))
    )
    server._sample_agent_pool()

    server.agent_adapter = SimpleNamespace(
        httpserver=SimpleNamespace(requests=SimpleNamespace(idle=lambda: 2, qsize=lambda: 11))
    )
    server._sample_agent_pool()

    server.agent_adapter = SimpleNamespace(httpserver=SimpleNamespace())
    server._sample_agent_pool()

    result = stats.snapshot('mgr.a')
    assert result['http_pool']['samples'] == 2
    assert result['http_pool']['idle_current'] == 2
    assert result['http_pool']['idle_min'] == 2
    assert result['http_pool']['queue_current'] == 11
    assert result['http_pool']['queue_max_observed'] == 11


def test_pool_fanout_and_reset() -> None:
    stats = AgentMetadataStats()
    stats.record_pool(0, 12)
    stats.record_pool(9, 0)
    stats.record_node_proxy_request()
    stats.record_ack_fanout(123, increment=True, config_push=True)

    result = stats.snapshot('mgr.a')
    assert result['http_pool']['idle_min'] == 0
    assert result['http_pool']['queue_max_observed'] == 12
    assert result['http_pool']['node_proxy_requests'] == 1
    assert result['ack_fanout']['events'] == 1
    assert result['ack_fanout']['max_hosts'] == 123
    assert result['ack_fanout']['multi_host_events'] == 1
    assert result['ack_fanout']['last_multi_host_hosts'] == 123
    assert result['ack_fanout']['increment_events'] == 1
    assert result['ack_fanout']['increment_hosts_total'] == 123
    assert result['ack_fanout']['max_increment_hosts'] == 123
    assert result['ack_fanout']['config_push_events'] == 1
    assert result['ack_fanout']['config_push_hosts_total'] == 123

    stats.reset()
    result = stats.snapshot('mgr.a')
    assert result['reports']['total'] == 0
    assert result['http_pool']['samples'] == 0
    assert result['ack_fanout']['events'] == 0


def test_recent_rates_intervals_and_pacing() -> None:
    stats = AgentMetadataStats()
    stats._since_monotonic = 99.0
    stats._since = datetime.datetime.fromtimestamp(999.0, tz=datetime.timezone.utc)

    with patch('cephadm.agent_metrics.time.monotonic', side_effect=[100.0, 100.2, 100.3, 100.4, 101.0, 101.2, 101.3, 101.4, 101.5]), \
            patch('cephadm.agent_metrics.time.time', side_effect=[1000.1, 1001.1, 1001.5]):
        stats.begin_agent_request()
        stats.record_valid_report('host1', 1000.0)
        stats.record_store('host', 0.010)
        stats.record_store('devices', 0.020)
        stats.finish_agent_request()

        stats.begin_agent_request()
        stats.record_valid_report('host1', 1001.0)
        stats.record_store('agent', 0.030)
        stats.finish_agent_request()

        # Background writes must contribute to total MON/store call rate.
        stats.record_store('host', 0.040)

        stats.record_pacing(
            host_count=305,
            avg_concurrency=8,
            refresh_period_s=76,
            initial_startup_delay_max_s=38,
            jitter_seconds=38,
        )
        result = stats.snapshot('mgr.a')

    assert result['reports']['valid'] == 2
    assert result['reports']['unique_hosts'] == 1
    assert result['reports']['sent_timestamped'] == 2
    assert result['reports']['recent_processed_rate']['total'] == 2
    assert result['reports']['send_interval']['count'] == 1
    assert result['reports']['send_interval']['avg_s'] == pytest.approx(1.0)
    assert result['reports']['send_interval']['histogram']['<10s'] == 1
    assert result['reports']['agent_to_worker_delay']['count'] == 2
    assert result['reports']['agent_to_worker_delay']['avg_ms'] == pytest.approx(100.0)
    assert result['reports']['clock_skew_samples'] == 0
    assert result['persistence']['recent_call_rate']['total'] == 4
    assert result['persistence']['recent_agent_call_rate']['total'] == 3
    assert result['persistence']['recent_background_call_rate']['total'] == 1
    assert result['pacing'] == {
        'host_count': 305,
        'avg_concurrency': 8,
        'refresh_period_s': 76,
        'initial_startup_delay_max_s': 38,
        'jitter_seconds': 38,
    }


def test_agent_to_worker_delay_uses_backlog_scale_buckets() -> None:
    stats = AgentMetadataStats()

    delays = [
        (1000.0, 1000.5, '<1s'),
        (1000.0, 1005.0, '1-10s'),
        (1000.0, 1020.0, '10-30s'),
        (1000.0, 1045.0, '30-60s'),
        (1000.0, 1120.0, '1-5min'),
        (1000.0, 1600.0, '5-15min'),
        (1000.0, 2800.0, '15-60min'),
        (1000.0, 5800.0, '>60min'),
    ]

    for sent_at, worker_at, expected_bucket in delays:
        stats._local.agent_request_start = 1.0
        stats._local.agent_request_start_wall = worker_at
        stats.record_valid_report('host1', sent_at)
        result = stats.snapshot('mgr.a')
        assert result['reports']['agent_to_worker_delay']['histogram'][expected_bucket] >= 1

    result = stats.snapshot('mgr.a')
    delay = result['reports']['agent_to_worker_delay']
    assert delay['count'] == len(delays)
    assert delay['max_ms'] == pytest.approx(4800.0 * 1000.0)


def test_host_data_store_writes_are_attributed_to_agent_request() -> None:
    # Keep this integration focused on the attribution boundary: exercise the
    # real HostData.index(), HostCache.save_host() and AgentCache.save_agent()
    # paths while avoiding the unrelated daemon-list parser.
    from orchestrator import HostSpec
    from cephadm.agent import HostData
    from .fixtures import with_cephadm_module

    with with_cephadm_module() as mgr:
        host = 'agent-stats-host'
        mgr.inventory.add_host(HostSpec(hostname=host, addr='1::4'))
        mgr.cache.prime_empty_host(host)
        mgr.agent_cache.agent_keys[host] = 'key'
        mgr.agent_cache.agent_counter[host] = 1

        data = {
            'host': host,
            'keyring': 'key',
            'port': 1234,
            'ack': '1',
            'ls': [{}],
            'networks': {},
            'facts': '',
            'volume': '',
        }

        def save_host_from_ls(hostname, _data):
            mgr.cache.save_host(hostname)

        request = SimpleNamespace(
            remote=SimpleNamespace(ip='127.0.0.1'),
            headers={'X-Cephadm-Agent-Sent-At': '1000.0'},
            body=io.BytesIO(json.dumps(data).encode('utf-8')),
        )

        with patch.object(mgr, '_process_ls_output', side_effect=save_host_from_ls), \
                patch.object(mgr, 'update_failed_daemon_health_check'), \
                patch.object(mgr, '_kick_serve_loop'), \
                patch('cephadm.agent.cherrypy.request', request):
            _agent_stats_request_start(mgr, 'data')
            try:
                HostData(mgr).index()
            finally:
                _agent_stats_request_end(mgr, 'data')

        result = mgr.agent_metadata_stats.snapshot('mgr.a')
        assert result['reports']['total'] == 1
        assert result['reports']['valid'] == 1
        assert result['reports']['unique_hosts'] == 1
        assert result['persistence']['agent_request']['host']['count'] == 1
        assert result['persistence']['agent_request']['agent']['count'] == 1
        assert result['persistence']['background']['host']['count'] == 0
        assert result['persistence']['background']['agent']['count'] == 0


def test_agent_stats_cli_plain_json_and_reset() -> None:
    import json
    from orchestrator.module import Format
    from .fixtures import with_cephadm_module

    with with_cephadm_module() as mgr:
        mgr.agent_metadata_stats.record_pool(4, 2)

        plain = mgr._agent_stats(Format.plain)
        assert plain.retval == 0
        assert 'Agent metadata statistics' in plain.stdout
        assert 'max queued observed:' in plain.stdout

        json_result = mgr._agent_stats(Format.json_pretty)
        assert json_result.retval == 0
        payload = json.loads(json_result.stdout)
        assert payload['http_pool']['queue_max_observed'] == 2
        assert payload['mgr'].startswith('mgr.')

        reset = mgr._agent_stats_reset()
        assert reset.retval == 0
        payload = mgr.agent_metadata_stats.snapshot('mgr.test')
        assert payload['http_pool']['samples'] == 0
        assert payload['reports']['total'] == 0


def test_slow_agent_request_logs_debug_per_request_store_breakdown() -> None:
    logger = MagicMock()
    stats = AgentMetadataStats(logger)

    with patch('cephadm.agent_metrics.time.monotonic', side_effect=[10.0, 10.1, 10.2, 10.3, 10.4, 11.5]):
        stats.begin_agent_request()
        stats.record_request_pool_start(0, 12)
        stats.record_report_shape({'host': 'node1', 'ls': [{}], 'volume': '[{}]'})
        stats.record_store('host', 0.400)
        stats.record_store('devices', 0.600)
        stats.record_store('devices', 0.200)
        stats.record_store('agent', 0.100)
        stats.finish_agent_request()

    logger.debug.assert_called_once()
    logger.info.assert_not_called()
    args = logger.debug.call_args.args
    assert args[1] == 'node1'
    assert args[2] == pytest.approx(1.5)
    assert args[3] == pytest.approx(1.3)
    assert args[4] == pytest.approx(0.4)
    assert args[5] == 1
    assert args[6] == pytest.approx(0.8)
    assert args[7] == 2
    assert args[8] == pytest.approx(0.1)
    assert args[9] == 1
    assert args[10] == 0
    assert args[11] == 12


def test_fast_agent_request_does_not_log_slow_breakdown() -> None:
    logger = MagicMock()
    stats = AgentMetadataStats(logger)

    with patch('cephadm.agent_metrics.time.monotonic', side_effect=[10.0, 10.1, 10.5]):
        stats.begin_agent_request()
        stats.record_report_shape({'host': 'node1'})
        stats.record_store('agent', 0.100)
        stats.finish_agent_request()

    logger.debug.assert_not_called()
    logger.info.assert_not_called()


def test_very_slow_agent_request_logs_at_info() -> None:
    logger = MagicMock()
    stats = AgentMetadataStats(logger)

    with patch('cephadm.agent_metrics.time.monotonic', side_effect=[10.0, 10.1, 15.1]):
        stats.begin_agent_request()
        stats.record_report_shape({'host': 'node1'})
        stats.record_store('agent', 5.0)
        stats.finish_agent_request()

    logger.info.assert_called_once()
    logger.debug.assert_not_called()


def test_empty_ack_fanout_is_not_recorded() -> None:
    from cephadm.agent import CephadmAgentHelpers

    stats = AgentMetadataStats()
    mgr = SimpleNamespace(
        agent_metadata_stats=stats,
        http_server=SimpleNamespace(agent=object()),
    )
    helpers = CephadmAgentHelpers(mgr)
    helpers._request_agent_acks(set())

    result = stats.snapshot('mgr.a')
    assert result['ack_fanout']['events'] == 0


class _FakeClock:
    """Controls agent_metrics' wall and thread CPU clocks."""

    def __init__(self) -> None:
        self.wall = 100.0
        self.cpu = 5.0

    def advance(self, wall: float, cpu: float) -> None:
        self.wall += wall
        self.cpu += cpu

    def monotonic(self) -> float:
        return self.wall

    def thread_time(self) -> float:
        return self.cpu

    def time(self) -> float:
        return self.wall


def _processed_report(stats: AgentMetadataStats, body: int, delta: bool,
                      ls_skipped: bool, devices_skipped: bool,
                      baseline_save_host: bool, stores: list) -> None:
    stats.begin_agent_request()
    stats.record_body_size(body)
    for category in stores:
        stats.record_store(category, 0.010)
    stats.record_delta_outcome(delta=delta, ls_skipped=ls_skipped,
                               devices_skipped=devices_skipped,
                               baseline_save_host=baseline_save_host)
    stats.finish_agent_request()


def test_delta_effectiveness_counts_writes_and_payload() -> None:
    stats = AgentMetadataStats()
    # Full report: ls sent, so save_host() wrote host + one device shard.
    _processed_report(stats, 1000, delta=False, ls_skipped=False,
                      devices_skipped=False, baseline_save_host=True,
                      stores=['devices', 'host', 'agent'])
    # Delta report: ls and volume accepted as unchanged, only save_agent().
    _processed_report(stats, 200, delta=True, ls_skipped=True,
                      devices_skipped=True, baseline_save_host=True,
                      stores=['agent'])

    result = stats.snapshot('mgr.a')
    delta = result['delta']
    assert delta['full_reports'] == 1
    assert delta['delta_reports'] == 1
    assert delta['ls_skipped'] == 1
    assert delta['ls_skipped_pct'] == 50.0
    assert delta['devices_skipped'] == 1
    assert delta['actual_writes'] == 4
    assert delta['save_host_avoided'] == 1
    # One avoided save_host() = host write + 1 device shard (observed average).
    assert delta['est_writes_avoided'] == 2
    assert delta['est_writes_avoided_pct'] == round(2 * 100.0 / 6, 2)
    assert result['payload'] == {'full_avg_bytes': 1000, 'delta_avg_bytes': 200}


def test_skipped_volume_alone_avoids_no_write() -> None:
    """volume skipped while ls was sent: save_host() still ran, nothing avoided."""
    stats = AgentMetadataStats()
    _processed_report(stats, 500, delta=True, ls_skipped=False,
                      devices_skipped=True, baseline_save_host=True,
                      stores=['devices', 'host', 'agent'])
    delta = stats.snapshot('mgr.a')['delta']
    assert delta['devices_skipped'] == 1
    assert delta['save_host_avoided'] == 0
    assert delta['est_writes_avoided'] == 0


def test_no_baseline_save_host_avoids_no_write() -> None:
    """Stale-ack or resynced ls: the old path would not have written either."""
    stats = AgentMetadataStats()
    _processed_report(stats, 300, delta=True, ls_skipped=False,
                      devices_skipped=False, baseline_save_host=False,
                      stores=['agent'])
    delta = stats.snapshot('mgr.a')['delta']
    assert delta['save_host_avoided'] == 0
    assert delta['est_writes_avoided'] == 0


def test_avoided_writes_without_observed_save_host_count_host_only() -> None:
    stats = AgentMetadataStats()
    _processed_report(stats, 300, delta=True, ls_skipped=True,
                      devices_skipped=True, baseline_save_host=True,
                      stores=['agent'])
    assert stats.snapshot('mgr.a')['delta']['est_writes_avoided'] == 1


def test_unprocessed_reports_are_not_counted() -> None:
    stats = AgentMetadataStats()
    stats.begin_agent_request()
    stats.record_body_size(100)
    with stats.phase('ls'):
        pass
    stats.finish_agent_request()

    result = stats.snapshot('mgr.a')
    assert result['delta']['full_reports'] == 0
    assert result['delta']['delta_reports'] == 0
    assert result['payload'] == {'full_avg_bytes': 0, 'delta_avg_bytes': 0}
    assert all(item['calls'] == 0 for item in result['processing'].values())


def test_phase_times_exclude_persistence_and_other_is_the_remainder() -> None:
    stats = AgentMetadataStats()
    clock = _FakeClock()
    with patch('cephadm.agent_metrics.time', clock):
        stats.begin_agent_request()
        clock.advance(0.100, 0.020)          # decode/parse -> other
        with stats.phase('ls'):
            clock.advance(0.150, 0.030)      # ls processing
            clock.advance(0.300, 0.010)      # save_host() set_store
            stats.record_store('host', 0.300, cpu_s=0.010)
            clock.advance(0.050, 0.010)      # more ls processing
        with stats.phase('scans'):
            clock.advance(0.200, 0.100)
        with stats.phase('scans'):
            clock.advance(0.100, 0.050)
        clock.advance(0.050, 0.005)          # save_agent() set_store
        stats.record_store('agent', 0.050, cpu_s=0.005)
        clock.advance(0.040, 0.004)          # response -> other
        stats.record_delta_outcome(delta=False, ls_skipped=False,
                                   devices_skipped=False, baseline_save_host=True)
        stats.finish_agent_request()

    processing = stats.snapshot('mgr.a')['processing']
    assert processing['ls'] == {'calls': 1, 'avg_wall_ms': 200.0, 'avg_cpu_ms': 40.0}
    # Two scan blocks in one report are one phase execution.
    assert processing['scans'] == {'calls': 1, 'avg_wall_ms': 300.0, 'avg_cpu_ms': 150.0}
    assert processing['devices'] == {'calls': 0, 'avg_wall_ms': 0.0, 'avg_cpu_ms': 0.0}
    # Total 0.990 s wall / 0.229 s CPU, minus phases and persistence.
    assert processing['other'] == {'calls': 1, 'avg_wall_ms': 140.0, 'avg_cpu_ms': 24.0}


def test_phase_outside_agent_request_is_a_noop() -> None:
    stats = AgentMetadataStats()
    with stats.phase('ls'):
        stats.record_store('host', 0.010)
    stats.begin_agent_request()
    with stats.phase('not-a-phase'):
        pass
    stats.record_delta_outcome(delta=False, ls_skipped=False,
                               devices_skipped=False, baseline_save_host=False)
    stats.finish_agent_request()
    processing = stats.snapshot('mgr.a')['processing']
    assert processing['ls']['calls'] == 0
    assert processing['other']['calls'] == 1


def test_reset_clears_delta_statistics() -> None:
    stats = AgentMetadataStats()
    _processed_report(stats, 200, delta=True, ls_skipped=True,
                      devices_skipped=True, baseline_save_host=True,
                      stores=['agent'])
    stats.reset()
    result = stats.snapshot('mgr.a')
    assert result['delta']['delta_reports'] == 0
    assert result['delta']['est_writes_avoided'] == 0
    assert result['processing']['other']['calls'] == 0


def test_plain_output_includes_delta_sections() -> None:
    stats = AgentMetadataStats()
    _processed_report(stats, 1000, delta=False, ls_skipped=False,
                      devices_skipped=False, baseline_save_host=True,
                      stores=['devices', 'host', 'agent'])
    out = stats.format_plain('mgr.a')
    assert 'Delta metadata:' in out
    assert '  full reports:       1' in out
    assert 'est. writes avoided:' in out
    assert 'Metadata processing:' in out
    for label in ('ls processing', 'devices', 'daemon scans', 'other'):
        assert f'  {label}' in out
    assert '  full avg bytes:     1000' in out
    assert 'ls delta' not in out
