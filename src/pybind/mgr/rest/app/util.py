import datetime

from dateutil import tz


def now():
    """
    A tz-aware now
    """
    return datetime.datetime.utcnow().replace(tzinfo=tz.tzutc())


def memoize(function):
    def wrapper(*args):
        self = args[0]
        if not hasattr(self, "_memo"):
            self._memo = {}

        if args in self._memo:
            return self._memo[args]
        else:
            rv = function(*args)
            self._memo[args] = rv
            return rv
    return wrapper
