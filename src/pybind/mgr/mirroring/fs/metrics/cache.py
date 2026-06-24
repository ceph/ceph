from collections import OrderedDict, namedtuple
from functools import wraps
from typing import Any, Callable, Dict

import time

from ..utils import norm_path
from .format import format_and_order_sync_stat_for_display, format_peer_status_metrics

# Two caches are used instead of one unified cache:
#
# - Complete cache holds a full omap snapshot (all dirs, all peers) per
#   filesystem. A hit guarantees the entry is complete, so full-scan queries
#   ('status <fs>', 'status <fs> <peer>') can be served without doubt.
#
# - Partial cache holds per-directory omap loads. Single-dir queries
#   ('status <fs> <dir>') peek the complete cache first; on miss they use
#   partial cache to avoid a full omap scan.
#
# A single cache keyed at directory granularity cannot cleanly answer "do I
# have every directory?" — partial entries may be present while others are
# missing, so serving a full scan from cache would require extra bookkeeping
# to prove completeness. Splitting complete vs partial makes that contract
# explicit: complete = all dirs; partial = one dir.

# LRU max entries for complete-cache lookups (one entry per filesystem per TTL
# window; each entry holds all dirs and all peers for that filesystem).
# Cache key: (time_token, filesystem).
COMPLETE_CACHE_MAX = 8
# LRU max entries for partial-cache lookups (per-directory omap batch load).
# Cache key: (time_token, filesystem, dir_path, peer_ids).
PARTIAL_CACHE_MAX = 32

CacheInfo = namedtuple('CacheInfo', ['hits', 'misses', 'maxsize', 'currsize'])


# functools.lru_cache is not sufficient here for two reasons:
#
# 1. TTL: entries must expire after snapshot_mirror_metrics_cache_ttl. lru_cache
#    has no TTL, so we bucket keys with a time_token derived from monotonic time.
#
# 2. cache_peek: single-directory status queries must read the complete cache
#    without loading on miss (otherwise a cold complete cache would trigger a
#    full omap scan). lru_cache has no peek API; calling the wrapper always
#    invokes the loader on miss. Inspecting lru_cache.cache for a hit-only
#    lookup is also not viable: that attribute is not part of the public API and
#    is unavailable on Python 3.14+ (_lru_cache_wrapper has no .cache).
#
# _TimedLRUCache therefore implements TTL-bucketed LRU storage plus peek().
class _TimedLRUCache:
    def __init__(self, func, maxsize):
        self.func = func
        self.maxsize = maxsize
        self.store = OrderedDict()
        self.hits = 0
        self.misses = 0

    def _key(self, time_token, args):
        # time_token buckets entries by snapshot_mirror_metrics_cache_ttl; args are
        # the decorated method's positional arguments (e.g. filesystem, or
        # filesystem+dir+peers).
        return (time_token,) + args

    def get(self, time_token, args, load=True):
        key = self._key(time_token, args)
        if key in self.store:
            self.hits += 1
            self.store.move_to_end(key)
            return self.store[key]
        if not load:
            return None
        self.misses += 1
        val = self.func(*args)
        self.store[key] = val
        while len(self.store) > self.maxsize:
            self.store.popitem(last=False)
        return val

    def peek(self, time_token, args):
        return self.get(time_token, args, load=False)

    def clear(self):
        self.store.clear()
        self.hits = 0
        self.misses = 0

    def info(self):
        return CacheInfo(self.hits, self.misses, self.maxsize, len(self.store))


def _resolve_ttl(ttl_getter, args, kwargs):
    try:
        return ttl_getter(*args, **kwargs)
    except TypeError:
        return ttl_getter()


def lru_cache_timeout(ttl_getter: Callable[..., int], maxsize: int = 128):
    def decorator(func):
        cache = _TimedLRUCache(func, maxsize)

        # BoundCached is returned from CachedMethod.__get__ so cache_peek,
        # cache_info, and cache_clear are bound to the instance. Attaching
        # those helpers to a plain wrapper function would drop self (e.g.
        # self.sync_stat_complete_cache.cache_peek(fs) would pass only fs),
        # breaking ttl_getter lambdas that use self.mgr.
        class BoundCached:
            def __init__(self, instance):
                self._instance = instance

            def _full_args(self, *args):
                return (self._instance,) + args

            def __call__(self, *args, **kwargs):
                full = self._full_args(*args)
                ttl = _resolve_ttl(ttl_getter, full, kwargs)
                if not ttl:
                    return func(*full, **kwargs)
                time_token = int(time.monotonic() / ttl)
                return cache.get(time_token, full)

            def cache_peek(self, *args, **kwargs):
                full = self._full_args(*args)
                ttl = _resolve_ttl(ttl_getter, full, kwargs)
                if not ttl:
                    return None
                time_token = int(time.monotonic() / ttl)
                return cache.peek(time_token, full)

            def cache_clear(self):
                cache.clear()

            def cache_info(self):
                return cache.info()

        # Descriptor: self.method(...) and self.method.cache_peek(...) share
        # the same BoundCached instance and cache keys.
        class CachedMethod:
            def __get__(self, obj, owner=None):
                if obj is None:
                    return self
                return BoundCached(obj)

            def __call__(self, *args, **kwargs):
                ttl = _resolve_ttl(ttl_getter, args, kwargs)
                if not ttl:
                    return func(*args, **kwargs)
                time_token = int(time.monotonic() / ttl)
                return cache.get(time_token, args)

        cached = CachedMethod()
        wraps(func)(cached)
        return cached
    return decorator


def _peer_stats_for_dir(metrics, dir_path):
    return metrics.get(dir_path, {}).get('peer', {})


def _requested_peers_have_stats(metrics, peers, dir_path):
    peer_stats = _peer_stats_for_dir(metrics, dir_path)
    return all(peer in peer_stats for peer in peers)


def metrics_for_dir_and_peers(metrics, dir_path, peers):
    result: Dict[str, Any] = {}
    peer_stats = _peer_stats_for_dir(metrics, dir_path)
    for peer in peers:
        if peer in peer_stats:
            format_peer_status_metrics(
                result, dir_path, peer,
                format_and_order_sync_stat_for_display(peer_stats[peer]))
    return result


def metrics_for_peer(metrics, peer_uuid):
    result: Dict[str, Any] = {}
    for dir_path, dir_entry in metrics.items():
        peer_stats = dir_entry.get('peer', {})
        if peer_uuid in peer_stats:
            format_peer_status_metrics(
                result, dir_path, peer_uuid,
                format_and_order_sync_stat_for_display(peer_stats[peer_uuid]))
    return result


def try_get_from_complete(metrics, mirrored_dir_path, peer_uuid, peers):
    """Filter complete cache (all dirs, all peers) for the CLI request.

    The complete cache always holds every peer and directory. peer_uuid and
    peers select the subset to return; they do not affect what is loaded.
    """
    if mirrored_dir_path:
        dir_path = norm_path(mirrored_dir_path)
        if _requested_peers_have_stats(metrics, peers, dir_path):
            return metrics_for_dir_and_peers(metrics, dir_path, peers)
        return None

    if peer_uuid is None:
        return dict(metrics)
    return metrics_for_peer(metrics, peer_uuid)
