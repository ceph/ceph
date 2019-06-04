"""
This is a minimal implementation of TTL-ed lru_cache function.

Based on Python 3 functools and backports.functools_lru_cache.
"""
from __future__ import absolute_import

from functools import wraps
from collections import OrderedDict
from threading import RLock
from time import time
from sys import getsizeof
from types import MethodType
from weakref import ref

class TTLCache(OrderedDict):
    MIN_GC_INTERVAL = 10

    class Item(object):
        def __init__(self, value):
            self.value = value
            self.timestamp = time()

    class ExpiredItem(KeyError):
        pass

    def __init__(self, ttl, maxsize=None, typed=False):
        if typed is not False:
            raise NotImplementedError("typed caching not supported")
        super(TTLCache, self).__init__()
        self.ttl = ttl

        self.hits = 0
        self.misses = 0
        self.expired = 0
        self.removed = 0
        self.maxsize = maxsize
        self.last_gc = time()
        self.gc_events = 0
        self.rlock = RLock()

    def __getitem__(self, key):
        with self.rlock:
            try:
                item = super(TTLCache, self).__getitem__(key)
            except KeyError:
                self.misses += 1
                raise

            if time() - item.timestamp > self.ttl:
                self.expired += 1
                self.misses += 1
                del self[key]
                raise TTLCache.ExpiredItem

            self.hits += 1
        return item.value

    def __setitem__(self, key, value):
        with self.rlock:
            super(TTLCache, self).__setitem__(key, TTLCache.Item(value))

            """if self.maxsize and (len(self) >= self.maxsize):
                self.gc(force=True)
                if len(self) >= self.maxsize:
                    super(TTLCache, self).popitem(last=False)
                    self.removed += 1
            else:
            self.gc()"""

    def info(self):
        return "TTLCache.Info({})".format(", ".join(
            "{}={}".format(k, v) for k, v in
            [
                ("hits", self.hits),
                ("misses", self.misses),
                ("expired", self.expired),
                ("removed", self.removed),
                ("currsize", len(self)),
                ("maxsize", self.maxsize),
                ("memsize", getsizeof(self)),
                ("gc_events", self.gc_events),
            ]
        ))

    def gc(self, force=False):
        """
        As items are ordered from older to newer, the first item not expired
        indicates that no further items are expired
        """
        ###TODO: THIS GC SEEMS TO COLLECT 1 ITEM MORE THAN EXPECTED
        with self.rlock:
            now = time()
            if not force and (now - self.last_gc < self.MIN_GC_INTERVAL):
                return

            self.last_gc = now

            gc = False
            for (i, key) in enumerate(self):
                item = super(TTLCache, self).__getitem__(key)
                if now - item.timestamp < self.ttl:
                    break
                else:
                    gc = True

            if gc:
                self.gc_events += 1
                for _ in range(i):
                    super(TTLCache, self).popitem(last=False)
                    self.expired += 1

def ttl_cache(
        ttl,
        maxsize=None,
        typed=False,
        before_return=None,
):
    """
    TTL Cache: for every invocation to a function it stores the return value to
    a given set of input parameters. Should no previously cached value exist, or
    the value has become outdated (TTL), the real function will be invoked and
    its return value saved.

    For caching to work with object method, it's essential that the class
    implements __hash__ and __eq__ magic methods to ensure two instances of the
    same back-end object fall back to the same cached value.

    :param ttl: maximum time in second for a cached value to become outdated.
    :type ttl: float

    :param maxsize: maximum size of the cache. If for caching a new entry, the
    cache would exceed this value, the oldest cached value will be removed.
    :type maxsize: int

    :param typed: not implemented. Kept to maintain interface compatibility with
    functools cache decorator.

    :param before_return: callable invoked before returning the cached value
    :type before_return: callable
    """

    if typed is not False:
        raise NotImplementedError("typed caching not supported")

    def decorating_function(function):
        is_method = isinstance(function, MethodType)
        cache = TTLCache(ttl, maxsize, typed)
        rlock = RLock()
        setattr(function, 'cache_info', cache.info)

        @wraps(function)
        def wrapper(*args, **kwargs):
            if is_method:
                key = (ref(args[0])) + args[1:]
            else:
                key = args
            key += tuple(kwargs.items())

            with rlock:
                try:
                    ret = cache[key]
                except (KeyError, TTLCache.ExpiredItem):
                    ret = function(*args, **kwargs)
                    cache[key] = ret

            if before_return:
                before_return(function, key)

            return ret

        return wrapper
    return decorating_function
