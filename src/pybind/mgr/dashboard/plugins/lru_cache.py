# -*- coding: utf-8 -*-
"""
This is a minimal implementation of lru_cache function.

Based on Python 3 functools and backports.functools_lru_cache.
"""
from __future__ import absolute_import

from functools import wraps
from collections import OrderedDict
from threading import RLock


def lru_cache(maxsize=128, typed=False):
    if typed is not False:
        raise NotImplementedError("typed caching not supported")

    def decorating_function(function):
        cache = OrderedDict()
        stats = [0, 0]
        rlock = RLock()
        setattr(
            function,
            'cache_info',
            lambda:
            "hits={}, misses={}, maxsize={}, currsize={}".format(
                stats[0], stats[1], maxsize, len(cache)))

        @wraps(function)
        def wrapper(*args, **kwargs):
            key = args + tuple(kwargs.items())
            with rlock:
                if key in cache:
                    ret = cache[key]
                    del cache[key]
                    cache[key] = ret
                    stats[0] += 1
                else:
                    ret = function(*args, **kwargs)
                    if len(cache) == maxsize:
                        cache.popitem(last=False)
                    cache[key] = ret
                    stats[1] += 1
            return ret

        return wrapper
    return decorating_function
