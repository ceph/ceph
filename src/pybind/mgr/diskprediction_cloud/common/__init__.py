from __future__ import absolute_import
import errno
from functools import wraps
import os
import signal


DP_MGR_STAT_OK = 'OK'
DP_MGR_STAT_WARNING = 'WARNING'
DP_MGR_STAT_FAILED = 'FAILED'
DP_MGR_STAT_DISABLED = 'DISABLED'
DP_MGR_STAT_ENABLED = 'ENABLED'


class DummyResonse:
    def __init__(self):
        self.resp_json = dict()
        self.content = 'DummyResponse'
        self.status_code = 404

    def json(self):
        return self.resp_json

    def __str__(self):
        return '{}'.format({'resp': self.resp_json, 'content': self.content, 'status_code': self.status_code})


class TimeoutError(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            if hasattr(args[0], '_timeout') is not None:
                seconds = args[0]._timeout
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def get_human_readable(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffix_index = 0
    while size > 1000 and suffix_index < 4:
        # increment the index of the suffix
        suffix_index += 1
        # apply the division
        size = size/1000.0
    return '%.*d %s' % (precision, size, suffixes[suffix_index])
