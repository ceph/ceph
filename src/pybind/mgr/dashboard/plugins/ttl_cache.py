"""
This is a minimal implementation of TTL-ed lru_cache function.

Based on Python 3 functools and backports.functools_lru_cache.
"""

import os
from collections import OrderedDict
from functools import wraps
from threading import RLock
from time import time
from typing import Any, Dict

try:
    from typing import Tuple
except ImportError:
    pass  # For typing only


class TTLCache:
    class CachedValue:
        def __init__(self, value, timestamp):
            self.value = value
            self.timestamp = timestamp

    def __init__(self, reference, ttl, maxsize=128):
        self.reference = reference
        self.ttl: int = ttl
        self.maxsize = maxsize
        self.cache: OrderedDict[Tuple[Any], TTLCache.CachedValue] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.expired = 0
        self.rlock = RLock()

    def __getitem__(self, key):
        with self.rlock:
            if key not in self.cache:
                self.misses += 1
                raise KeyError(f'"{key}" is not set')

            cached_value = self.cache[key]
            if time() - cached_value.timestamp >= self.ttl:
                del self.cache[key]
                self.expired += 1
                self.misses += 1
                raise KeyError(f'"{key}" is not set')

            self.hits += 1
            return cached_value.value

    def __setitem__(self, key, value):
        with self.rlock:
            if key in self.cache:
                cached_value = self.cache[key]
                if time() - cached_value.timestamp >= self.ttl:
                    self.expired += 1
            if len(self.cache) == self.maxsize:
                self.cache.popitem(last=False)

            self.cache[key] = TTLCache.CachedValue(value, time())

    def clear(self):
        with self.rlock:
            self.cache.clear()

    def info(self) -> str:
        return (f'cache={self.reference} hits={self.hits}, misses={self.misses},'
                f'expired={self.expired}, maxsize={self.maxsize}, currsize={len(self.cache)}')


class CacheManager:
    caches: Dict[str, TTLCache] = {}

    @classmethod
    def get(cls, reference: str, ttl=30, maxsize=128):
        if reference in cls.caches:
            return cls.caches[reference]
        cls.caches[reference] = TTLCache(reference, ttl, maxsize)
        return cls.caches[reference]


def ttl_cache(ttl, maxsize=128, typed=False, label: str = ''):
    if typed is not False:
        raise NotImplementedError("typed caching not supported")

    # disable caching while running unit tests
    if 'UNITTEST' in os.environ:
        ttl = 0

    def decorating_function(function):
        cache_name = label
        if not cache_name:
            cache_name = function.__name__
        cache = CacheManager.get(cache_name, ttl, maxsize)

        @wraps(function)
        def wrapper(*args, **kwargs):
            key = args + tuple(kwargs.items())
            try:
                return cache[key]
            except KeyError:
                ret = function(*args, **kwargs)
                cache[key] = ret
                return ret

        return wrapper
    return decorating_function


def ttl_cache_invalidator(label: str):
    def decorating_function(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            ret = function(*args, **kwargs)
            CacheManager.get(label).clear()
            return ret
        return wrapper
    return decorating_function
