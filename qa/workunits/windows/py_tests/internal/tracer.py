# Copyright (C) 2023 Cloudbase Solutions
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation (see LICENSE).

import collections
import prettytable
import threading
import time

from py_tests.internal import utils


class Tracer:
    data: collections.OrderedDict = collections.OrderedDict()
    lock = threading.Lock()

    @classmethod
    def trace(cls, func):
        def wrapper(*args, **kwargs):
            tstart = time.time()
            exc_str = None

            # Preserve call order
            with cls.lock:
                if func.__qualname__ not in cls.data:
                    cls.data[func.__qualname__] = list()

            try:
                return func(*args, **kwargs)
            except Exception as exc:
                exc_str = "%r: %s" % (exc, exc)
                raise
            finally:
                tend = time.time()

                with cls.lock:
                    cls.data[func.__qualname__] += [{
                        "duration": tend - tstart,
                        "error": exc_str,
                    }]

        return wrapper

    @classmethod
    def get_results(cls):
        stats = collections.OrderedDict()
        for f in cls.data.keys():
            stats[f] = utils.array_stats([i['duration'] for i in cls.data[f]])
            errors = []
            for i in cls.data[f]:
                if i['error']:
                    errors.append(i['error'])

            stats[f]['errors'] = errors
        return stats

    @classmethod
    def print_results(cls):
        r = cls.get_results()

        table = prettytable.PrettyTable(title="Duration (s)")
        table.field_names = [
            "function", "min", "max", "total",
            "mean", "median", "std_dev",
            "max 90%", "min 90%", "count", "errors"]
        table.float_format = ".4"
        for f, s in r.items():
            table.add_row([f, s['min'], s['max'], s['sum'],
                           s['mean'], s['median'], s['std_dev'],
                           s['max_90'], s['min_90'],
                           s['count'], len(s['errors'])])
        print(table)
