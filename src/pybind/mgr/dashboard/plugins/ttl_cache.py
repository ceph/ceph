"""
This is a minimal implementation of TTL-ed lru_cache function.

Based on Python 3 functools and backports.functools_lru_cache.
"""

from collections import OrderedDict
from enum import Enum
from functools import wraps
from threading import RLock
from time import monotonic, perf_counter

from typing import Any, Tuple


class CacheStatus(Enum):
    hit = "hit"
    miss = "miss"
    expired = "expired"


class CacheMetric:
    def __init__(self):
        self.status: CacheStatus = None
        self.elapsed: int = 0
        self.currsize: int = 0

    def to_server_timing(self):
        return f'cache-{self.status.value};dur={self.elapsed*1000:.3f}'


class CacheStats:
    def __init__(self, maxsize):
        self.hits: int = 0
        self.misses: int = 0
        self.expired: int = 0
        self.maxsize: int = maxsize
        self.currsize: int = None

    def add_metric(self, metric):
        if metric.status == CacheStatus.hit:
            self.hits += 1
        elif metric.status == CacheStatus.miss:
            self.misses += 1
        else:
            self.expired += 1

    def __repr__(self):
        return ', '.join(f'{k}={v}' for k, v in self.__dict__.items())


class TTLDict(OrderedDict):
    class ExpiredKey(LookupError): pass

    def __init__(self, ttl, maxsize, *args, **kwargs):
        self.ttl = ttl
        self.maxsize = maxsize
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        (ret, ts) = super().__getitem__(key)
        if monotonic() - ts > self.ttl:
            super().__delitem__(key)
            raise TTLDict.ExpiredKey
        return ret

    def __setitem__(self, key, value):
        if super().__len__() == self.maxsize:
            super().popitem(last=False)
        super().__setitem__(key, (value, monotonic()))


def ttl_cache(ttl: int, maxsize: int = 128, return_metric: bool = False):
    def decorating_function(func):
        cache = TTLDict(ttl, maxsize)
        stats = CacheStats(maxsize)
        rlock = RLock()
        setattr(func, 'cache_info', stats.__repr__)

        @wraps(func)
        def wrapper(*args, **kwargs):
            t0 = perf_counter()
            metric = CacheMetric()
            key = args + tuple(kwargs.items())
            with rlock:
                try:
                    ret = cache[key]
                    metric.status = CacheStatus.hit
                except (KeyError, TTLDict.ExpiredKey) as exc:
                    ret = func(*args, **kwargs)
                    if isinstance(exc, KeyError):
                        metric.status = CacheStatus.miss
                    else:
                        metric.status = CacheStatus.expired
                    cache[key] = ret
                finally:
                    metric.elapsed = perf_counter() - t0
                    stats.add_metric(metric)
                    stats.currsize = len(cache)
                    return ret if not return_metric else (ret, metric)

        return wrapper
    return decorating_function
