#!/usr/bin/env python3
"""
Mock of another cluster's Prometheus for
``TestMonitoringSecure.test_prometheus_remote_write``.

Not a real Prometheus and not part of the cluster under test. It only
accepts Prometheus ``remote_write`` so ``ceph orch prometheus
set-remote-write`` has somewhere to push without a second stack.

Run it on the Prometheus host. ``set-remote-write`` must use that host's
IP, not ``127.0.0.1`` (Prometheus runs in a container). A mock on another
host may be firewalled.

Pass a *base* URL. Cephadm renders ``{{ remote_write_url }}/api/v1/write``.

Assert ``GET /status``: ``post_accepted >= 1`` is not enough;
``metric_hits`` must include the allowlisted name (default
``ceph_health_status``).

Endpoints:
  POST /api/v1/write  - 204
  GET  /status        - JSON counters
  GET  /healthz       - ok
  POST /reset         - zero counters

Example::

  # On the Prometheus host:
  python3 mock_prometheus_remote_write.py --bind 0.0.0.0 --port 19291
  ceph orch prometheus set-remote-write http://<prometheus-host-ip>:19291 ceph_health_status
  curl -s http://127.0.0.1:19291/status
  ceph orch prometheus remove-remote-write http://<prometheus-host-ip>:19291
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


log = logging.getLogger('rw_receiver')


class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.post_total = 0
        self.post_accepted = 0
        self.post_rejected = 0
        self.bytes_accepted = 0
        self.last_status = 0
        self.last_error = ''
        self.last_content_type = ''
        self.last_content_encoding = ''
        self.last_body_len = 0
        self.last_accepted_at: Optional[float] = None
        self.metric_hits: Dict[str, int] = {}
        self.snappy_available = False
        self.snappy_impl = 'stdlib'
        self.snappy_decoded = 0
        self.snappy_errors = 0

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'started_at': self.started_at,
                'uptime_s': round(time.time() - self.started_at, 1),
                'post_total': self.post_total,
                'post_accepted': self.post_accepted,
                'post_rejected': self.post_rejected,
                'bytes_accepted': self.bytes_accepted,
                'last_status': self.last_status,
                'last_error': self.last_error,
                'last_content_type': self.last_content_type,
                'last_content_encoding': self.last_content_encoding,
                'last_body_len': self.last_body_len,
                'last_accepted_at': self.last_accepted_at,
                'metric_hits': dict(self.metric_hits),
                'snappy_available': self.snappy_available,
                'snappy_impl': self.snappy_impl,
                'snappy_decoded': self.snappy_decoded,
                'snappy_errors': self.snappy_errors,
            }

    def reset(self) -> None:
        with self.lock:
            self.post_total = 0
            self.post_accepted = 0
            self.post_rejected = 0
            self.bytes_accepted = 0
            self.last_status = 0
            self.last_error = ''
            self.last_content_type = ''
            self.last_content_encoding = ''
            self.last_body_len = 0
            self.last_accepted_at = None
            self.metric_hits = {k: 0 for k in self.metric_hits}
            self.snappy_decoded = 0
            self.snappy_errors = 0


def _try_import_snappy():
    try:
        import snappy  # type: ignore
        return snappy
    except Exception:
        return None


def _snappy_varint(data: bytes, i: int) -> Tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if i >= len(data):
            raise ValueError('truncated snappy varint')
        b = data[i]
        i += 1
        result |= (b & 0x7F) << shift
        if b < 0x80:
            return result, i
        shift += 7
        if shift > 35:
            raise ValueError('snappy varint too long')


def snappy_decompress(data: bytes) -> bytes:
    """Decompress a snappy *block* (Prometheus remote_write / golang snappy)."""
    expected, i = _snappy_varint(data, 0)
    out = bytearray()
    n = len(data)
    while i < n:
        tag = data[i]
        i += 1
        kind = tag & 0x03
        if kind == 0:
            length = tag >> 2
            if length >= 60:
                nbytes = length - 59
                if i + nbytes > n:
                    raise ValueError('truncated snappy literal length')
                length = int.from_bytes(data[i:i + nbytes], 'little')
                i += nbytes
            length += 1
            if i + length > n:
                raise ValueError('truncated snappy literal')
            out.extend(data[i:i + length])
            i += length
            continue
        if kind == 1:
            length = ((tag >> 2) & 7) + 4
            if i >= n:
                raise ValueError('truncated snappy copy1')
            offset = ((tag >> 5) << 8) | data[i]
            i += 1
        elif kind == 2:
            length = (tag >> 2) + 1
            if i + 2 > n:
                raise ValueError('truncated snappy copy2')
            offset = int.from_bytes(data[i:i + 2], 'little')
            i += 2
        else:
            length = (tag >> 2) + 1
            if i + 4 > n:
                raise ValueError('truncated snappy copy4')
            offset = int.from_bytes(data[i:i + 4], 'little')
            i += 4
        if offset <= 0 or offset > len(out):
            raise ValueError(f'invalid snappy copy offset {offset}')
        start = len(out) - offset
        for _ in range(length):
            out.append(out[start])
            start += 1
    if len(out) != expected:
        raise ValueError(
            f'snappy length mismatch: expected {expected} got {len(out)}')
    return bytes(out)


def _snappy_selfcheck() -> None:
    """Fail fast if the stdlib decoder cannot read a snappy literal block."""
    hello = bytes([5, 0x10]) + b'hello'
    if snappy_decompress(hello) != b'hello':
        raise RuntimeError('snappy self-check failed on hello')
    name = b'ceph_health_status'
    blob = bytes([len(name), (len(name) - 1) << 2]) + name
    if snappy_decompress(blob) != name:
        raise RuntimeError('snappy self-check failed on ceph_health_status')


def _decompress_remote_write(body: bytes, snappy_mod: Any) -> bytes:
    if snappy_mod is not None:
        return snappy_mod.decompress(body)
    return snappy_decompress(body)


def _scan_metrics(payload: bytes, watch: List[str], stats: Stats) -> None:
    for name in watch:
        if name.encode('utf-8') in payload:
            with stats.lock:
                stats.metric_hits[name] = stats.metric_hits.get(name, 0) + 1


class Handler(BaseHTTPRequestHandler):
    server_version = 'CephRemoteWriteReceiver/1.0'
    # Filled in by main()
    stats: Stats
    watch_metrics: List[str]
    snappy = None
    require_snappy_header: bool = True

    def log_message(self, fmt: str, *args: Any) -> None:
        log.info('%s - ' + fmt, self.address_string(), *args)

    def _send(self, code: int, body: bytes = b'',
              content_type: str = 'text/plain; charset=utf-8') -> None:
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        if body and self.command != 'HEAD':
            self.wfile.write(body)

    def _send_json(self, code: int, obj: Any) -> None:
        data = (json.dumps(obj, indent=2, sort_keys=True) + '\n').encode('utf-8')
        self._send(code, data, 'application/json')

    def do_GET(self) -> None:
        path = urlparse(self.path).path.rstrip('/') or '/'
        if path in ('/healthz', '/'):
            self._send(200, b'ok\n')
            return
        if path == '/status':
            self._send_json(200, self.stats.snapshot())
            return
        if path == '/metrics':
            s = self.stats.snapshot()
            lines = [
                '# HELP rw_receiver_posts_accepted Accepted remote_write POSTs',
                '# TYPE rw_receiver_posts_accepted counter',
                f'rw_receiver_posts_accepted {s["post_accepted"]}',
                '# HELP rw_receiver_posts_rejected Rejected remote_write POSTs',
                '# TYPE rw_receiver_posts_rejected counter',
                f'rw_receiver_posts_rejected {s["post_rejected"]}',
                '# HELP rw_receiver_bytes_accepted Accepted body bytes',
                '# TYPE rw_receiver_bytes_accepted counter',
                f'rw_receiver_bytes_accepted {s["bytes_accepted"]}',
            ]
            for name, hits in sorted(s['metric_hits'].items()):
                lines.append(
                    f'rw_receiver_metric_hits{{name="{name}"}} {hits}'
                )
            self._send(200, ('\n'.join(lines) + '\n').encode('utf-8'))
            return
        self._send(404, b'not found\n')

    def do_POST(self) -> None:
        path = urlparse(self.path).path.rstrip('/') or '/'
        if path == '/reset':
            self.stats.reset()
            self._send_json(200, {'reset': True, 'status': self.stats.snapshot()})
            return
        if path != '/api/v1/write':
            self._send(404, b'not found\n')
            return
        self._handle_remote_write()

    def _handle_remote_write(self) -> None:
        ctype = (self.headers.get('Content-Type') or '').split(';')[0].strip().lower()
        cenc = (self.headers.get('Content-Encoding') or '').strip().lower()
        try:
            length = int(self.headers.get('Content-Length') or '0')
        except ValueError:
            length = -1

        with self.stats.lock:
            self.stats.post_total += 1
            self.stats.last_content_type = ctype
            self.stats.last_content_encoding = cenc
            self.stats.last_body_len = max(length, 0)

        body = b''
        if length > 0:
            body = self.rfile.read(length)
        elif length == 0:
            body = b''
        else:
            # No Content-Length: reject rather than blocking on read
            self._reject(400, 'missing or invalid Content-Length')
            return

        with self.stats.lock:
            self.stats.last_body_len = len(body)

        # Prometheus sends application/x-protobuf + snappy. Be slightly
        # tolerant of charset suffixes (already stripped) and missing
        # encoding only when --relax-headers is set.
        if ctype not in ('application/x-protobuf', 'application/protobuf'):
            self._reject(415, f'unexpected Content-Type: {ctype!r}')
            return
        if self.require_snappy_header and cenc not in ('snappy', 'x-snappy-framed'):
            self._reject(415, f'unexpected Content-Encoding: {cenc!r}')
            return
        if not body:
            self._reject(400, 'empty body')
            return

        try:
            decoded = _decompress_remote_write(body, self.snappy)
            with self.stats.lock:
                self.stats.snappy_decoded += 1
            _scan_metrics(decoded, self.watch_metrics, self.stats)
        except Exception as exc:  # noqa: BLE001 - still 204 so Prometheus does not retry
            with self.stats.lock:
                self.stats.snappy_errors += 1
            log.warning('snappy decompress failed: %s', exc)

        with self.stats.lock:
            self.stats.post_accepted += 1
            self.stats.bytes_accepted += len(body)
            self.stats.last_status = 204
            self.stats.last_error = ''
            self.stats.last_accepted_at = time.time()

        # Prometheus treats 2xx as success; 204 is conventional for RW.
        self._send(204)

    def _reject(self, code: int, reason: str) -> None:
        with self.stats.lock:
            self.stats.post_rejected += 1
            self.stats.last_status = code
            self.stats.last_error = reason
        log.warning('reject %s: %s', code, reason)
        self._send(code, (reason + '\n').encode('utf-8'))


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Fake remote-cluster Prometheus (remote_write sink) for Ceph QA')
    parser.add_argument('--bind', default='0.0.0.0',
                        help='Listen address (default 0.0.0.0)')
    parser.add_argument('--port', type=int, default=19291,
                        help='Listen port (default 19291)')
    parser.add_argument('--watch-metric', action='append', default=[],
                        dest='watch_metrics',
                        help='Metric name to look for after snappy decode '
                             '(repeatable). Default: ceph_health_status')
    parser.add_argument('--relax-headers', action='store_true',
                        help='Do not require Content-Encoding: snappy')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    _snappy_selfcheck()
    watch = args.watch_metrics or ['ceph_health_status']
    stats = Stats()
    stats.metric_hits = {m: 0 for m in watch}
    snappy = _try_import_snappy()
    stats.snappy_available = snappy is not None
    stats.snappy_impl = 'python-snappy' if snappy is not None else 'stdlib'
    log.info('snappy decoder %s; will scan decompressed writes for %s',
             stats.snappy_impl, watch)

    Handler.stats = stats
    Handler.watch_metrics = watch
    Handler.snappy = snappy
    Handler.require_snappy_header = not args.relax_headers

    httpd = ThreadingHTTPServer((args.bind, args.port), Handler)
    log.info('listening on http://%s:%d  (POST /api/v1/write, GET /status)',
             args.bind, args.port)
    log.info('configure Ceph with BASE url only, e.g.:')
    log.info('  ceph orch prometheus set-remote-write '
             'http://<this-host-ip>:%d %s',
             args.port, ' '.join(watch))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.info('shutting down')
    finally:
        httpd.server_close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
