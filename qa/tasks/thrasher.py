"""
Thrasher base class
"""


from gevent.greenlet import Greenlet
from gevent.event import Event

class Thrasher(object):

    def __init__(self):
        super(Thrasher, self).__init__()
        self._exception = None

    @property
    def exception(self):
        return self._exception

    def set_thrasher_exception(self, e):
        self._exception = e

class ThrasherGreenlet(Thrasher, Greenlet):

    class Stopped(Exception): ...

    def __init__(self):
        super(ThrasherGreenlet, self).__init__()
        self._should_stop_event = Event()

    @property
    def is_stopped(self):
        return self._should_stop_event.is_set()

    def stop(self):
        self._should_stop_event.set()

    def set_thrasher_exception(self, e):
        if not isinstance(e, self.Stopped):
            super(ThrasherGreenlet, self).set_thrasher_exception(e)

    def proceed_unless_stopped(self):
        self.sleep_unless_stopped(0, raise_stopped=True)

    def sleep_unless_stopped(self, seconds, raise_stopped = True):
        self._should_stop_event.wait(seconds)
        if self.is_stopped and raise_stopped:
            raise self.Stopped()
        return not self.is_stopped
