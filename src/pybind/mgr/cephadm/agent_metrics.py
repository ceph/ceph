import datetime
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional

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

_STORE_CATEGORIES = ('host', 'devices', 'agent')
_STORE_SCOPES = ('agent_request', 'background')


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
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._since = _utcnow()
            self._requests = _new_latency_stats()
            self._reports_total = 0
            self._bad_metadata = 0
            self._handler_errors = 0
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

    def begin_agent_request(self) -> None:
        # The request hook runs at on_start_resource, before json_in parses the
        # body. Keep request-local attribution and breakdown state thread-local
        # so concurrent CherryPy workers cannot mix their measurements.
        self._local.in_agent_request = True
        self._local.agent_request_start = time.monotonic()
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
        self._local.agent_request_host = None
        self._local.agent_store_time_s = {}
        self._local.agent_store_calls = {}
        self._local.agent_pool_idle_start = None
        self._local.agent_pool_queue_start = None

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

    def record_store(self, category: str, duration_s: float, error: bool = False) -> None:
        if category not in _STORE_CATEGORIES:
            return
        in_agent_request = getattr(self._local, 'in_agent_request', False)
        scope = 'agent_request' if in_agent_request else 'background'
        with self._lock:
            _record_latency(self._stores[scope][category], duration_s, error)
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

    def record_ack_fanout(self, hosts: int) -> None:
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

    def snapshot(self, mgr_name: str) -> Dict[str, Any]:
        with self._lock:
            requests = self._latency_snapshot(self._requests)
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
                    'bad_metadata': self._bad_metadata,
                    'handler_errors': self._handler_errors,
                    'with_nonempty_ls': self._reports_with_nonempty_ls,
                    'with_devices': self._reports_with_devices,
                    'first_contact': self._first_contact_reports,
                    'stale_ack': self._stale_ack_reports,
                },
                'request_latency': requests,
                'persistence': {
                    'agent_request_calls': agent_store_calls,
                    'agent_request_calls_per_report': round(
                        agent_store_calls / self._reports_total, 3
                    ) if self._reports_total else 0.0,
                    'agent_request_store_time_pct': round(
                        agent_store_ms * 100.0 / request_total_ms, 2
                    ) if request_total_ms else 0.0,
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
                },
            }

    def format_plain(self, mgr_name: str) -> str:
        stats = self.snapshot(mgr_name)
        reports = stats['reports']
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
            f"  bad metadata:       {reports['bad_metadata']}",
            f"  handler errors:     {reports['handler_errors']}",
            f"  non-empty ls:      {reports['with_nonempty_ls']}",
            f"  with devices:       {reports['with_devices']}",
            f"  first contact:      {reports['first_contact']}",
            f"  stale ack:          {reports['stale_ack']}",
            '',
            'Request latency:',
            f"  avg:                {req['avg_ms']:.3f} ms",
            f"  max:                {req['max_ms']:.3f} ms",
        ]
        for bucket, count in req['histogram'].items():
            lines.append(f'  {bucket:<18} {count}')

        lines.extend([
            '',
            'Agent-request persistence:',
            f"  calls:              {persistence['agent_request_calls']}",
            f"  calls/report:       {persistence['agent_request_calls_per_report']:.3f}",
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
        ])
        return '\n'.join(lines) + '\n'
