import logging

import gevent.pool
import gevent.queue

log = logging.getLogger(__name__)

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
        greenlet = self.group.spawn(func, *args, **kwargs)
        greenlet.link(self._finish)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        if value is not None:
            self.group.kill(block=True)
            return False

        try:
            # raises if any greenlets exited with an exception
            for result in self:
                log.debug('result is %s', repr(result))
                pass
        except:
            self.group.kill(block=True)
            raise
        return True

    def __iter__(self):
        return self

    def next(self):
        if not self.any_spawned or self.iteration_stopped:
            raise StopIteration()
        result = self.results.get()
        if isinstance(result, BaseException):
            if isinstance(result, StopIteration):
                self.iteration_stopped = True
            raise result
        return result

    def _finish(self, greenlet):
        if greenlet.successful():
            self.results.put(greenlet.value)
        else:
            self.results.put(greenlet.exception)

        self.count -= 1
        if self.count <= 0:
            self.results.put(StopIteration())
