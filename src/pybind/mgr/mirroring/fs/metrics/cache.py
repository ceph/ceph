import time
from typing import Any, Dict

from ..utils import norm_path
from .format import format_and_order_sync_stat_for_display, format_peer_status_metrics

CACHE_TTL_SECS = 15
PARTIAL_CACHE_MAX = 32


def _peer_stats_for_dir(metrics, dir_path):
    return metrics.get(dir_path, {}).get('peer', {})


def _requested_peers_have_stats(metrics, peers, dir_path):
    peer_stats = _peer_stats_for_dir(metrics, dir_path)
    return all(peer in peer_stats for peer in peers)


def _metrics_for_dir_and_peers(metrics, dir_path, peers):
    result: Dict[str, Any] = {}
    peer_stats = _peer_stats_for_dir(metrics, dir_path)
    for peer in peers:
        if peer in peer_stats:
            format_peer_status_metrics(
                result, dir_path, peer,
                format_and_order_sync_stat_for_display(peer_stats[peer]))
    return result


def _metrics_for_peer(metrics, peer_uuid):
    result: Dict[str, Any] = {}
    for dir_path, dir_entry in metrics.items():
        peer_stats = dir_entry.get('peer', {})
        if peer_uuid in peer_stats:
            format_peer_status_metrics(
                result, dir_path, peer_uuid,
                format_and_order_sync_stat_for_display(peer_stats[peer_uuid]))
    return result


class SyncStatCache:
    def __init__(self):
        self._caches = {}

    def _prune(self, cache):
        now = time.monotonic()
        complete = cache.get('complete')
        if complete and now >= complete['expires_at']:
            cache['complete'] = None
        partial = cache['partial']
        for key, entry in list(partial.items()):
            if now >= entry['expires_at']:
                partial.pop(key, None)
                if key in cache['partial_order']:
                    cache['partial_order'].remove(key)

    def get_fs_cache(self, filesystem):
        cache = self._caches.setdefault(
            filesystem, {'complete': None, 'partial': {}, 'partial_order': []})
        self._prune(cache)
        return cache

    def try_get(self, filesystem, mirrored_dir_path, peer_uuid, peers):
        cache = self.get_fs_cache(filesystem)
        now = time.monotonic()
        complete = cache.get('complete')

        if mirrored_dir_path:
            dir_path = norm_path(mirrored_dir_path)
            if complete and now < complete['expires_at']:
                if complete['peer_uuid'] is None or complete['peer_uuid'] == peer_uuid:
                    if _requested_peers_have_stats(complete['metrics'], peers, dir_path):
                        return _metrics_for_dir_and_peers(
                            complete['metrics'], dir_path, peers)
            partial = cache['partial'].get((dir_path, peer_uuid))
            if partial and now < partial['expires_at']:
                if _requested_peers_have_stats(partial['metrics'], peers, dir_path):
                    return _metrics_for_dir_and_peers(
                        partial['metrics'], dir_path, peers)
            return None

        if not complete or now >= complete['expires_at']:
            return None
        if peer_uuid is None:
            if complete['peer_uuid'] is not None:
                return None
            return dict(complete['metrics'])
        if complete['peer_uuid'] not in (None, peer_uuid):
            return None
        return _metrics_for_peer(complete['metrics'], peer_uuid)

    def store(self, filesystem, metrics, complete, peer_uuid, dir_path=None):
        cache = self.get_fs_cache(filesystem)
        expires_at = time.monotonic() + CACHE_TTL_SECS
        if complete:
            cache['complete'] = {
                'metrics': metrics,
                'expires_at': expires_at,
                'peer_uuid': peer_uuid,
            }
            return

        partial_key = (dir_path, peer_uuid)
        cache['partial'][partial_key] = {
            'metrics': metrics,
            'expires_at': expires_at,
        }
        order = cache['partial_order']
        if partial_key in order:
            order.remove(partial_key)
        order.append(partial_key)
        while len(order) > PARTIAL_CACHE_MAX:
            cache['partial'].pop(order.pop(0), None)
