import datetime
import enum
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional, Set

_SLOW_REQUEST_DEBUG_THRESHOLD_S = 1.0
_SLOW_REQUEST_INFO_THRESHOLD_S = 5.0

_LATENCY_BUCKETS_MS = (
    (10.0, '<10ms'),
    (50.0, '10-50ms'),
    (100.0, '50-100ms'),
    (500.0, '100-500ms'),
    (1000.0, '500ms-1s'),
    (5000.0, '1-5s'),
    (10000.0, '5-10s'),
    (float('inf'), '>10s'),
)


class DeltaReason(str, enum.Enum):
    """Why an agent sent or skipped ls, as counted in its ls_delta_stats.

    The values are the ls_delta_stats keys on the wire and must match
    DeltaReason in src/cephadm/cephadmlib/agent_delta.py.
    """
    FULL_SYNC = 'full_sync'
    UNCHANGED = 'unchanged'
    STRUCTURAL = 'structural'
    MEMORY = 'memory_usage'
    CPU = 'cpu_percentage'


_STORE_CATEGORIES = ('host', 'devices', 'agent')
_STORE_SCOPES = ('agent_request', 'background')
_REPORT_RATE_WINDOW_S = 60
_AGENT_TO_WORKER_DELAY_BUCKETS_S = (
    (1.0, '<1s'),
    (10.0, '1-10s'),
    (30.0, '10-30s'),
    (60.0, '30-60s'),
    (300.0, '1-5min'),
    (900.0, '5-15min'),
    (3600.0, '15-60min'),
    (float('inf'), '>60min'),
)
_REPORT_INTERVAL_BUCKETS_S = (
    (10.0, '<10s'),
    (20.0, '10-20s'),
    (40.0, '20-40s'),
    (60.0, '40-60s'),
    (90.0, '60-90s'),
    (120.0, '90-120s'),
    (300.0, '120-300s'),
    (float('inf'), '>300s'),
)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def _new_latency_stats() -> Dict[str, Any]:
    return {
        'count': 0,
        'errors': 0,
        'total_ms': 0.0,
        'max_ms': 0.0,
        'histogram': OrderedDict((label, 0) for _, label in _LATENCY_BUCKETS_MS),
    }


def _record_latency(stats: Dict[str, Any], duration_s: float, error: bool = False) -> None:
    duration_ms = duration_s * 1000.0
    stats['count'] += 1
    stats['total_ms'] += duration_ms
    stats['max_ms'] = max(stats['max_ms'], duration_ms)
    if error:
        stats['errors'] += 1
    for upper_ms, label in _LATENCY_BUCKETS_MS:
        if duration_ms < upper_ms:
            stats['histogram'][label] += 1
            break


def _new_agent_to_worker_delay_stats() -> Dict[str, Any]:
    return {
        'count': 0,
        'errors': 0,
        'total_ms': 0.0,
        'max_ms': 0.0,
        'histogram': OrderedDict((label, 0) for _, label in _AGENT_TO_WORKER_DELAY_BUCKETS_S),
    }


def _record_agent_to_worker_delay(stats: Dict[str, Any], duration_s: float) -> None:
    duration_ms = duration_s * 1000.0
    stats['count'] += 1
    stats['total_ms'] += duration_ms
    stats['max_ms'] = max(stats['max_ms'], duration_ms)
    for upper_s, label in _AGENT_TO_WORKER_DELAY_BUCKETS_S:
        if duration_s < upper_s:
            stats['histogram'][label] += 1
            break


def _new_interval_stats() -> Dict[str, Any]:
    return {
        'count': 0,
        'total_s': 0.0,
        'min_s': None,
        'max_s': 0.0,
        'histogram': OrderedDict((label, 0) for _, label in _REPORT_INTERVAL_BUCKETS_S),
    }


def _record_interval(stats: Dict[str, Any], duration_s: float) -> None:
    stats['count'] += 1
    stats['total_s'] += duration_s
    stats['min_s'] = duration_s if stats['min_s'] is None else min(stats['min_s'], duration_s)
    stats['max_s'] = max(stats['max_s'], duration_s)
    for upper_s, label in _REPORT_INTERVAL_BUCKETS_S:
        if duration_s < upper_s:
            stats['histogram'][label] += 1
            break


class AgentMetadataStats:
    """Small in-memory diagnostics for cephadm agent request scalability.

    The collector intentionally measures only the cache persistence calls that are
    relevant to agent metadata processing. A thread-local marker attributes calls
    made by save_host/save_host_devices/save_agent to an agent request or to other
    cephadm activity without wrapping mgr.set_store() globally.
    """

    def __init__(self, logger: Optional[Any] = None) -> None:
        self._lock = threading.Lock()
        self._local = threading.local()
        self._logger = logger
        # Per-host cumulative ls_delta_stats last seen from each agent. These are
        # baselines, not measurements, so reset() keeps them: the next report
        # after a reset then counts only what happened since.
        self._ls_delta_last_by_host: Dict[str, Dict[DeltaReason, int]] = {}
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._since = _utcnow()
            self._since_monotonic = time.monotonic()
            self._requests = _new_latency_stats()
            self._reports_total = 0
            self._valid_reports = 0
            self._unique_reporting_hosts: Set[str] = set()
            self._last_agent_send_by_host: Dict[str, float] = {}
            self._agent_send_intervals = _new_interval_stats()
            self._processed_rate_buckets: Dict[int, int] = {}
            self._agent_sent_timestamp_reports = 0
            self._agent_to_worker_delay = _new_agent_to_worker_delay_stats()
            self._agent_clock_skew_samples = 0
            self._store_rate_buckets: Dict[int, int] = {}
            self._agent_store_rate_buckets: Dict[int, int] = {}
            self._background_store_rate_buckets: Dict[int, int] = {}
            self._pacing: Dict[str, Optional[int]] = {
                'host_count': None,
                'avg_concurrency': None,
                'refresh_period_s': None,
                'initial_startup_delay_max_s': None,
                'jitter_seconds': None,
            }
            self._bad_metadata = 0
            self._handler_errors = 0
            self._ls_delta_reasons: Dict[DeltaReason, int] = {reason: 0 for reason in DeltaReason}
            self._reports_with_nonempty_ls = 0
            self._reports_with_devices = 0
            self._first_contact_reports = 0
            self._stale_ack_reports = 0
            self._node_proxy_requests = 0
            self._stores = {
                scope: {category: _new_latency_stats() for category in _STORE_CATEGORIES}
                for scope in _STORE_SCOPES
            }
            self._pool_samples = 0
            self._pool_idle_current: Optional[int] = None
            self._pool_idle_min: Optional[int] = None
            self._pool_queue_current: Optional[int] = None
            self._pool_queue_max = 0
            self._ack_fanouts = 0
            self._last_ack_fanout_at: Optional[str] = None
            self._last_ack_fanout_hosts = 0
            self._max_ack_fanout_hosts = 0
            self._multi_host_ack_fanouts = 0
            self._last_multi_host_ack_fanout_at: Optional[str] = None
            self._last_multi_host_ack_fanout_hosts = 0
            self._increment_ack_fanouts = 0
            self._increment_ack_hosts = 0
            self._max_increment_ack_hosts = 0
            self._config_pushes = 0
            self._config_push_hosts = 0

    def begin_agent_request(self) -> None:
        # The request hook runs at on_start_resource, before json_in parses the
        # body. Keep request-local attribution and breakdown state thread-local
        # so concurrent CherryPy workers cannot mix their measurements.
        self._local.in_agent_request = True
        self._local.agent_request_start = time.monotonic()
        self._local.agent_request_start_wall = time.time()
        self._local.agent_request_host = None
        self._local.agent_store_time_s = {category: 0.0 for category in _STORE_CATEGORIES}
        self._local.agent_store_calls = {category: 0 for category in _STORE_CATEGORIES}
        self._local.agent_pool_idle_start = None
        self._local.agent_pool_queue_start = None
        with self._lock:
            self._reports_total += 1

    def record_request_pool_start(self, idle: int, queued: int) -> None:
        if not getattr(self._local, 'in_agent_request', False):
            return
        self._local.agent_pool_idle_start = idle
        self._local.agent_pool_queue_start = queued

    def finish_agent_request(self) -> None:
        start = getattr(self._local, 'agent_request_start', None)
        if start is None:
            return
        duration = time.monotonic() - start
        with self._lock:
            _record_latency(self._requests, duration)

        if duration >= _SLOW_REQUEST_DEBUG_THRESHOLD_S and self._logger is not None:
            store_time = getattr(self._local, 'agent_store_time_s', {})
            store_calls = getattr(self._local, 'agent_store_calls', {})
            store_total = sum(store_time.values())
            host = getattr(self._local, 'agent_request_host', None) or '<unknown>'
            log = (self._logger.info
                   if duration >= _SLOW_REQUEST_INFO_THRESHOLD_S
                   else self._logger.debug)
            log(
                'Slow agent metadata request host=%s total=%.3fs store=%.3fs '
                'host_store=%.3fs/%d devices_store=%.3fs/%d agent_store=%.3fs/%d '
                'pool_idle_start=%s pool_queue_start=%s',
                host,
                duration,
                store_total,
                store_time.get('host', 0.0),
                store_calls.get('host', 0),
                store_time.get('devices', 0.0),
                store_calls.get('devices', 0),
                store_time.get('agent', 0.0),
                store_calls.get('agent', 0),
                getattr(self._local, 'agent_pool_idle_start', None),
                getattr(self._local, 'agent_pool_queue_start', None),
            )

        self._local.in_agent_request = False
        self._local.agent_request_start = None
        self._local.agent_request_start_wall = None
        self._local.agent_request_host = None
        self._local.agent_store_time_s = {}
        self._local.agent_store_calls = {}
        self._local.agent_pool_idle_start = None
        self._local.agent_pool_queue_start = None

    @staticmethod
    def _record_rate_bucket(buckets: Dict[int, int], timestamp: float) -> None:
        second = int(timestamp)
        buckets[second] = buckets.get(second, 0) + 1
        cutoff = second - _REPORT_RATE_WINDOW_S + 1
        for old_second in [value for value in buckets if value < cutoff]:
            del buckets[old_second]

    def record_valid_report(self, host: str, agent_sent_at: Optional[float] = None) -> None:
        # Worker-start time measures mgr processing cadence. Agent send time is
        # supplied by the agent and measures pacing before the HTTP queue.
        processed_at = getattr(self._local, 'agent_request_start', None)
        if processed_at is None:
            processed_at = time.monotonic()
        worker_start_wall = getattr(self._local, 'agent_request_start_wall', None)
        with self._lock:
            self._valid_reports += 1
            self._unique_reporting_hosts.add(host)
            self._record_rate_bucket(self._processed_rate_buckets, processed_at)
            if agent_sent_at is not None:
                self._agent_sent_timestamp_reports += 1
                previous = self._last_agent_send_by_host.get(host)
                if previous is not None and agent_sent_at >= previous:
                    _record_interval(self._agent_send_intervals, agent_sent_at - previous)
                self._last_agent_send_by_host[host] = agent_sent_at
                if worker_start_wall is not None:
                    delay = worker_start_wall - agent_sent_at
                    if delay >= 0:
                        _record_agent_to_worker_delay(self._agent_to_worker_delay, delay)
                    else:
                        self._agent_clock_skew_samples += 1

    def record_pacing(self, host_count: int, avg_concurrency: int,
                      refresh_period_s: int, initial_startup_delay_max_s: int,
                      jitter_seconds: int) -> None:
        with self._lock:
            self._pacing = {
                'host_count': host_count,
                'avg_concurrency': avg_concurrency,
                'refresh_period_s': refresh_period_s,
                'initial_startup_delay_max_s': initial_startup_delay_max_s,
                'jitter_seconds': jitter_seconds,
            }

    def record_report_shape(self, data: Dict[str, Any]) -> None:
        if getattr(self._local, 'in_agent_request', False):
            self._local.agent_request_host = data.get('host')
        with self._lock:
            if data.get('ls'):
                self._reports_with_nonempty_ls += 1
            if data.get('volume'):
                self._reports_with_devices += 1

    def record_report_state(self, first_contact: bool, stale_ack: bool) -> None:
        with self._lock:
            if first_contact:
                self._first_contact_reports += 1
            if stale_ack:
                self._stale_ack_reports += 1

    def record_bad_metadata(self) -> None:
        with self._lock:
            self._bad_metadata += 1

    def record_handler_error(self) -> None:
        with self._lock:
            self._handler_errors += 1

    def record_ls_delta_stats(self, host: str, counters: Any) -> None:
        if not isinstance(counters, dict):
            return
        try:
            current = {reason: max(0, int(counters.get(reason.value, 0))) for reason in DeltaReason}
        except (TypeError, ValueError):
            return
        with self._lock:
            previous = self._ls_delta_last_by_host.get(host)
            if previous is None:
                # The first observation establishes a baseline. Counting the whole
                # vector here would import the agent's pre-mgr/pre-reset history.
                self._ls_delta_last_by_host[host] = current
                return
            # Cumulative counters form one vector. If any component moved backwards,
            # the agent restarted/reset and the whole current vector is post-reset.
            reset = any(current[reason] < previous[reason] for reason in DeltaReason)
            for reason in DeltaReason:
                delta = current[reason] if reset else current[reason] - previous[reason]
                self._ls_delta_reasons[reason] += delta
            self._ls_delta_last_by_host[host] = current

    def forget_ls_delta_host(self, host: str) -> None:
        with self._lock:
            self._ls_delta_last_by_host.pop(host, None)

    def record_store(self, category: str, duration_s: float, error: bool = False) -> None:
        if category not in _STORE_CATEGORIES:
            return
        in_agent_request = getattr(self._local, 'in_agent_request', False)
        scope = 'agent_request' if in_agent_request else 'background'
        store_at = time.monotonic()
        with self._lock:
            _record_latency(self._stores[scope][category], duration_s, error)
            self._record_rate_bucket(self._store_rate_buckets, store_at)
            if in_agent_request:
                self._record_rate_bucket(self._agent_store_rate_buckets, store_at)
            else:
                self._record_rate_bucket(self._background_store_rate_buckets, store_at)
        if in_agent_request:
            store_time = getattr(self._local, 'agent_store_time_s', None)
            store_calls = getattr(self._local, 'agent_store_calls', None)
            if store_time is not None and store_calls is not None:
                store_time[category] += duration_s
                store_calls[category] += 1

    def record_pool(self, idle: int, queued: int) -> None:
        with self._lock:
            self._pool_samples += 1
            self._pool_idle_current = idle
            if self._pool_idle_min is None:
                self._pool_idle_min = idle
            else:
                self._pool_idle_min = min(self._pool_idle_min, idle)
            self._pool_queue_current = queued
            self._pool_queue_max = max(self._pool_queue_max, queued)

    def record_node_proxy_request(self) -> None:
        with self._lock:
            self._node_proxy_requests += 1

    def record_ack_fanout(self, hosts: int, increment: bool = False,
                          config_push: bool = False) -> None:
        with self._lock:
            self._ack_fanouts += 1
            now = _utcnow().isoformat()
            self._last_ack_fanout_at = now
            self._last_ack_fanout_hosts = hosts
            self._max_ack_fanout_hosts = max(self._max_ack_fanout_hosts, hosts)
            if hosts > 1:
                self._multi_host_ack_fanouts += 1
                self._last_multi_host_ack_fanout_at = now
                self._last_multi_host_ack_fanout_hosts = hosts
            if increment:
                self._increment_ack_fanouts += 1
                self._increment_ack_hosts += hosts
                self._max_increment_ack_hosts = max(self._max_increment_ack_hosts, hosts)
            if config_push:
                self._config_pushes += 1
                self._config_push_hosts += hosts

    @staticmethod
    def _latency_snapshot(stats: Dict[str, Any]) -> Dict[str, Any]:
        count = stats['count']
        return {
            'count': count,
            'errors': stats['errors'],
            'avg_ms': round(stats['total_ms'] / count, 3) if count else 0.0,
            'max_ms': round(stats['max_ms'], 3),
            'total_ms': round(stats['total_ms'], 3),
            'histogram': dict(stats['histogram']),
        }

    @staticmethod
    def _interval_snapshot(stats: Dict[str, Any]) -> Dict[str, Any]:
        count = stats['count']
        return {
            'count': count,
            'avg_s': round(stats['total_s'] / count, 3) if count else 0.0,
            'min_s': round(stats['min_s'], 3) if stats['min_s'] is not None else 0.0,
            'max_s': round(stats['max_s'], 3),
            'histogram': dict(stats['histogram']),
        }

    def _rate_snapshot(self, buckets: Dict[int, int], now: float, since: Optional[float] = None) -> Dict[str, Any]:
        current_second = int(now)
        cutoff = current_second - _REPORT_RATE_WINDOW_S + 1
        counts = [count for second, count in buckets.items() if second >= cutoff]
        start = self._since_monotonic if since is None else since
        elapsed = min(float(_REPORT_RATE_WINDOW_S), max(1.0, now - start))
        total = sum(counts)
        return {
            'window_s': _REPORT_RATE_WINDOW_S,
            'total': total,
            'avg_per_sec': round(total / elapsed, 3),
            'max_per_sec': max(counts) if counts else 0,
        }

    def snapshot(self, mgr_name: str) -> Dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            requests = self._latency_snapshot(self._requests)
            processed_rate = self._rate_snapshot(self._processed_rate_buckets, now)
            store_rate = self._rate_snapshot(self._store_rate_buckets, now)
            agent_store_rate = self._rate_snapshot(self._agent_store_rate_buckets, now)
            background_store_rate = self._rate_snapshot(self._background_store_rate_buckets, now)
            agent_send_intervals = self._interval_snapshot(self._agent_send_intervals)
            agent_to_worker_delay = self._latency_snapshot(self._agent_to_worker_delay)
            stores = {
                scope: {
                    category: self._latency_snapshot(self._stores[scope][category])
                    for category in _STORE_CATEGORIES
                }
                for scope in _STORE_SCOPES
            }
            agent_store_calls = sum(stores['agent_request'][c]['count'] for c in _STORE_CATEGORIES)
            agent_store_ms = sum(stores['agent_request'][c]['total_ms'] for c in _STORE_CATEGORIES)
            request_total_ms = requests['total_ms']
            return {
                'mgr': mgr_name,
                'since': self._since.isoformat(),
                'reports': {
                    'total': self._reports_total,
                    'valid': self._valid_reports,
                    'unique_hosts': len(self._unique_reporting_hosts),
                    'bad_metadata': self._bad_metadata,
                    'handler_errors': self._handler_errors,
                    'with_nonempty_ls': self._reports_with_nonempty_ls,
                    'with_devices': self._reports_with_devices,
                    'first_contact': self._first_contact_reports,
                    'stale_ack': self._stale_ack_reports,
                    'ls_delta': {reason.value: count for reason, count in self._ls_delta_reasons.items()},
                    'recent_processed_rate': processed_rate,
                    'sent_timestamped': self._agent_sent_timestamp_reports,
                    'send_interval': agent_send_intervals,
                    'agent_to_worker_delay': agent_to_worker_delay,
                    'clock_skew_samples': self._agent_clock_skew_samples,
                },
                'pacing': dict(self._pacing),
                'request_latency': requests,
                'persistence': {
                    'agent_request_calls': agent_store_calls,
                    'agent_request_calls_per_report': round(
                        agent_store_calls / self._reports_total, 3
                    ) if self._reports_total else 0.0,
                    'agent_request_store_time_pct': round(
                        agent_store_ms * 100.0 / request_total_ms, 2
                    ) if request_total_ms else 0.0,
                    'recent_call_rate': store_rate,
                    'recent_agent_call_rate': agent_store_rate,
                    'recent_background_call_rate': background_store_rate,
                    'agent_request': stores['agent_request'],
                    'background': stores['background'],
                },
                'http_pool': {
                    'samples': self._pool_samples,
                    'idle_current': self._pool_idle_current,
                    'idle_min': self._pool_idle_min,
                    'queue_current': self._pool_queue_current,
                    'queue_max_observed': self._pool_queue_max,
                    'node_proxy_requests': self._node_proxy_requests,
                },
                'ack_fanout': {
                    'events': self._ack_fanouts,
                    'last_at': self._last_ack_fanout_at,
                    'last_hosts': self._last_ack_fanout_hosts,
                    'max_hosts': self._max_ack_fanout_hosts,
                    'multi_host_events': self._multi_host_ack_fanouts,
                    'last_multi_host_at': self._last_multi_host_ack_fanout_at,
                    'last_multi_host_hosts': self._last_multi_host_ack_fanout_hosts,
                    'increment_events': self._increment_ack_fanouts,
                    'increment_hosts_total': self._increment_ack_hosts,
                    'max_increment_hosts': self._max_increment_ack_hosts,
                    'config_push_events': self._config_pushes,
                    'config_push_hosts_total': self._config_push_hosts,
                },
            }

    def format_plain(self, mgr_name: str) -> str:
        stats = self.snapshot(mgr_name)
        reports = stats['reports']
        pacing = stats['pacing']
        req = stats['request_latency']
        persistence = stats['persistence']
        pool = stats['http_pool']
        fanout = stats['ack_fanout']

        lines = [
            'Agent metadata statistics',
            f"mgr: {stats['mgr']}",
            f"since: {stats['since']}",
            '',
            'Reports:',
            f"  total:              {reports['total']}",
            f"  valid:              {reports['valid']}",
            f"  unique hosts:       {reports['unique_hosts']}",
            f"  processed avg/sec:  {reports['recent_processed_rate']['avg_per_sec']:.3f}",
            f"  processed max/sec:  {reports['recent_processed_rate']['max_per_sec']}",
            f"  sent timestamps:    {reports['sent_timestamped']}",
            f"  bad metadata:       {reports['bad_metadata']}",
            f"  handler errors:     {reports['handler_errors']}",
            f"  non-empty ls:      {reports['with_nonempty_ls']}",
            f"  with devices:       {reports['with_devices']}",
            f"  first contact:      {reports['first_contact']}",
            f"  stale ack:          {reports['stale_ack']}",
            f"  ls delta full sync: {reports['ls_delta'][DeltaReason.FULL_SYNC.value]}",
            f"  ls structural:      {reports['ls_delta'][DeltaReason.STRUCTURAL.value]}",
            f"  ls memory:          {reports['ls_delta'][DeltaReason.MEMORY.value]}",
            f"  ls cpu:             {reports['ls_delta'][DeltaReason.CPU.value]}",
            f"  ls unchanged:       {reports['ls_delta'][DeltaReason.UNCHANGED.value]}",
            '',
            'Mgr pacing policy:',
            f"  host count:         {pacing['host_count']}",
            f"  avg concurrency:    {pacing['avg_concurrency']}",
            f"  refresh period:     {pacing['refresh_period_s']} s",
            f"  startup delay max:  {pacing['initial_startup_delay_max_s']} s",
            f"  jitter:             {pacing['jitter_seconds']} s",
            '',
            'Agent send interval:',
            f"  samples:            {reports['send_interval']['count']}",
            f"  avg:                {reports['send_interval']['avg_s']:.3f} s",
            f"  min:                {reports['send_interval']['min_s']:.3f} s",
            f"  max:                {reports['send_interval']['max_s']:.3f} s",
        ]
        for bucket, count in reports['send_interval']['histogram'].items():
            lines.append(f'  {bucket:<18} {count}')

        delay = reports['agent_to_worker_delay']
        lines.extend([
            '',
            'Agent send -> mgr worker delay:',
            f"  avg:                {delay['avg_ms']:.3f} ms",
            f"  max:                {delay['max_ms']:.3f} ms",
            f"  clock skew samples: {reports['clock_skew_samples']}",
        ])
        for bucket, count in delay['histogram'].items():
            lines.append(f'  {bucket:<18} {count}')

        lines.extend([
            '',
            'Request latency:',
            f"  avg:                {req['avg_ms']:.3f} ms",
            f"  max:                {req['max_ms']:.3f} ms",
        ])
        for bucket, count in req['histogram'].items():
            lines.append(f'  {bucket:<18} {count}')

        lines.extend([
            '',
            'Agent-request persistence:',
            f"  calls:              {persistence['agent_request_calls']}",
            f"  calls/report:       {persistence['agent_request_calls_per_report']:.3f}",
            f"  total calls/sec:    {persistence['recent_call_rate']['avg_per_sec']:.3f}",
            f"  total max/sec:      {persistence['recent_call_rate']['max_per_sec']}",
            f"  agent calls/sec:    {persistence['recent_agent_call_rate']['avg_per_sec']:.3f}",
            f"  background calls/s: {persistence['recent_background_call_rate']['avg_per_sec']:.3f}",
            f"  store time/request: {persistence['agent_request_store_time_pct']:.2f}%",
            '  category       calls      avg ms      max ms',
        ])
        for category in _STORE_CATEGORIES:
            item = persistence['agent_request'][category]
            lines.append(
                f"  {category:<12} {item['count']:>7} {item['avg_ms']:>11.3f} {item['max_ms']:>11.3f}"
            )

        lines.extend([
            '',
            'Other cephadm persistence:',
            '  category       calls      avg ms      max ms',
        ])
        for category in _STORE_CATEGORIES:
            item = persistence['background'][category]
            lines.append(
                f"  {category:<12} {item['count']:>7} {item['avg_ms']:>11.3f} {item['max_ms']:>11.3f}"
            )

        lines.extend([
            '',
            'Agent HTTP pool:',
            f"  samples:            {pool['samples']}",
            f"  idle now:           {pool['idle_current']}",
            f"  minimum idle:       {pool['idle_min']}",
            f"  queued now:         {pool['queue_current']}",
            f"  max queued observed:{pool['queue_max_observed']:>7}",
            f"  node-proxy requests:{pool['node_proxy_requests']:>7}",
            '',
            'Agent ACK fanout:',
            f"  events:             {fanout['events']}",
            f"  last at:            {fanout['last_at']}",
            f"  last hosts:         {fanout['last_hosts']}",
            f"  maximum hosts:      {fanout['max_hosts']}",
            f"  multi-host events:  {fanout['multi_host_events']}",
            f"  last multi-host at: {fanout['last_multi_host_at']}",
            f"  last multi hosts:   {fanout['last_multi_host_hosts']}",
            f"  increment events:   {fanout['increment_events']}",
            f"  increment hosts:    {fanout['increment_hosts_total']}",
            f"  max increment hosts:{fanout['max_increment_hosts']:>7}",
            f"  config push events: {fanout['config_push_events']}",
            f"  config push hosts:  {fanout['config_push_hosts_total']}",
        ])
        return '\n'.join(lines) + '\n'
