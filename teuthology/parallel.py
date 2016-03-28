import logging
import sys

import gevent.pool
import gevent.queue

log = logging.getLogger(__name__)

class ExceptionHolder(object):
    def __init__(self, exc_info):
        self.exc_info = exc_info

def capture_traceback(func, *args, **kwargs):
    """
    Utility function to capture tracebacks of any exception func
    raises.
    """
    try:
        return func(*args, **kwargs)
    except Exception:
        return ExceptionHolder(sys.exc_info())

def resurrect_traceback(exc):
    if isinstance(exc, ExceptionHolder):
        exc_info = exc.exc_info
    elif isinstance(exc, BaseException):
        exc_info = (type(exc), exc, None)
    else:
        return

    raise exc_info[0], exc_info[1], exc_info[2]

class parallel(object):
    """
    This class is a context manager for running functions in parallel.

    You add functions to be run with the spawn method::

        with parallel() as p:
            for foo in bar:
                p.spawn(quux, foo, baz=True)

    You can iterate over the results (which are in arbitrary order)::

        with parallel() as p:
            for foo in bar:
                p.spawn(quux, foo, baz=True)
            for result in p:
                print result

    If one of the spawned functions throws an exception, it will be thrown
    when iterating over the results, or when the with block ends.

    At the end of the with block, the main thread waits until all
    spawned functions have completed, or, if one exited with an exception,
    kills the rest and raises the exception.
    """

    def __init__(self):
        self.group = gevent.pool.Group()
        self.results = gevent.queue.Queue()
        self.count = 0
        self.any_spawned = False
        self.iteration_stopped = False

    def spawn(self, func, *args, **kwargs):
        self.count += 1
        self.any_spawned = True
        greenlet = self.group.spawn(capture_traceback, func, *args, **kwargs)
        greenlet.link(self._finish)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.group.join()

        if value is not None:
            return False

        try:
            # raises if any greenlets exited with an exception
            for result in self:
                log.debug('result is %s', repr(result))
                pass
        except Exception:
            # Emit message here because traceback gets stomped when we re-raise
            log.exception("Exception in parallel execution")
            raise
        return True

    def __iter__(self):
        return self

    def next(self):
        if not self.any_spawned or self.iteration_stopped:
            raise StopIteration()
        result = self.results.get()

        try:
            resurrect_traceback(result)
        except StopIteration:
            self.iteration_stopped = True
            raise

        return result

    def _finish(self, greenlet):
        if greenlet.successful():
            self.results.put(greenlet.value)
        else:
            self.results.put(greenlet.exception)

        self.count -= 1
        if self.count <= 0:
            self.results.put(StopIteration())
