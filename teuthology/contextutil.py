import contextlib
import sys
import logging
import time
import itertools

log = logging.getLogger(__name__)

@contextlib.contextmanager
def nested(*managers):
    """
    Like contextlib.nested but takes callables returning context
    managers, to avoid the major reason why contextlib.nested was
    deprecated.

    This version also logs any exceptions early, much like run_tasks,
    to ease debugging. TODO combine nested and run_tasks.
    """
    exits = []
    vars = []
    exc = (None, None, None)
    try:
        for mgr_fn in managers:
            mgr = mgr_fn()
            exit = mgr.__exit__
            enter = mgr.__enter__
            vars.append(enter())
            exits.append(exit)
        yield vars
    except Exception:
        log.exception('Saw exception from nested tasks')
        exc = sys.exc_info()
    finally:
        while exits:
            exit = exits.pop()
            try:
                if exit(*exc):
                    exc = (None, None, None)
            except Exception:
                exc = sys.exc_info()
        if exc != (None, None, None):
            # Don't rely on sys.exc_info() still containing
            # the right information. Another exception may
            # have been raised and caught by an exit method
            raise exc[0], exc[1], exc[2]


class MaxWhileTries(Exception):
    pass


class safe_while(object):
    """
    A contect manager to remove boiler plate code that deals with `while` loops
    that need a given number of tries and some seconds to sleep between each
    one of those tries.

    The most simple example possible will try 5 times sleeping for 5 seconds
    and increasing the sleep time by 5 each time::

        >>> from teuthology.contexutil import safe_while
        >>> with safe_while() as bomb:
        ...    while 1:
        ...        bomb()
        ...        # repetitive code here
        ...
        Traceback (most recent call last):
        ...
        MaxWhileTries: reached maximum tries (5) after waiting for 75 seconds

    Yes, this adds yet another level of indentation but it allows you to
    implement while loops exactly the same as before with just 1 more
    indentation level and one extra call. Everything else stays the same,
    code-wise. So adding this helper to existing code is simpler.

    The defaults are to start the sleeping time in seconds at 5s and to add
    5 more seconds at every point in the loop. Setting the increment value to
    0 makes the sleep time in seconds stay the same throughout the calls.

    """

    def __init__(self, sleep=5, increment=5, tries=5, _sleeper=None):
        self.sleep = sleep
        self.increment = increment
        self.tries = tries
        self.counter = 0
        self.sleep_current = sleep
        self.sleeper = _sleeper or time.sleep

    def _make_error_msg(self):
        """
        Sum the total number of seconds we waited while providing the number
        of tries we attempted
        """
        total_seconds_waiting = sum(
            itertools.islice(
                itertools.count(self.sleep, self.increment),
                self.tries
            )
        )
        return 'reached maximum tries (%s) after waiting for %s seconds' % (
            self.tries, total_seconds_waiting
        )

    def __call__(self):
        self.counter += 1
        if self.counter > self.tries:
            error_msg = self._make_error_msg()
            raise MaxWhileTries(error_msg)
        self.sleeper(self.sleep_current)
        self.sleep_current += self.increment

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
