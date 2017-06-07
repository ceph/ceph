

from threading import Thread, Event, Lock
import datetime
import time


class GetterThread(Thread):
    def __init__(self, view):
        super(GetterThread, self).__init__()
        self._view = view
        self.event = Event()

    def run(self):
        try:
            t0 = time.time()
            val = self._view._get()
            t1 = time.time()
        except:
            self._view.log.exception("Error while calling _get:")
            # TODO: separate channel for passing back error
            self._view.value = None
            self._view.value_when = None
            self._view.getter_thread = None
        else:
            with self._view.lock:
                self._view.latency = t1 - t0
                self._view.value = val
                self._view.value_when = datetime.datetime.now()
                self._view.getter_thread = None

        self.event.set()


class RemoteViewCache(object):
    """
    A cached view of something we have to fetch remotely, e.g. the output
    of a 'tell' command to a daemon, or something we read from RADOS like
    and rbd image's status.

    There are two reasons this wrapper exists:
     * To reduce load on remote entities when we frequently fetch something
       (i.e. 10 page loads in one second should not translate into 10 separate
        remote fetches)
     * To handle the blocking nature of some remote operations
       (i.e. if RADOS isn't responding, we need a predictable way to either
        return no data, or return stale data, when something from RADOS is
        requested)

    Note that relying on timeouts in underlying libraries isn't wise here:
    if something is really slow, we would like to return a 'stale' state
    to a GUI quickly, but we should let our underlying request to the cluster
    run to completion so that we get some data even if it's slow, and so that
    a polling caller doesn't end up firing off a large number of requests to
    the cluster because each one timed out.

    Subclasses may override _init and must override _get
    """

    def __init__(self, module_inst):
        self.initialized = False

        self.log = module_inst.log

        self.getter_thread = None

        # Consider data within 1s old to be sufficiently fresh
        self.stale_period = 1.0

        # Return stale data if
        self.timeout = 5

        self.event = Event()
        self.value_when = None
        self.value = None
        self.latency = 0
        self.lock = Lock()

        self._module = module_inst

    def init(self):
        self._init()
        self.initialized = True

    VALUE_OK = 0
    VALUE_STALE = 1
    VALUE_NONE = 2

    def get(self):
        """
        If data less than `stale_period` old is available, return it
        immediately.
        If an attempt to fetch data does not complete within `timeout`, then
        return the most recent data available, with a status to indicate that
        it is stale.

        Initialization does not count towards the timeout, so the first call
        on one of these objects during the process lifetime may be slower
        than subsequent calls.

        :return: 2-tuple of value status code, value
        """
        with self.lock:
            if not self.initialized:
                self.init()

            now = datetime.datetime.now()
            if self.value_when and now - self.value_when < datetime.timedelta(
                    seconds=self.stale_period):
                return self.VALUE_OK, self.value

            if self.getter_thread is None:
                self.getter_thread = GetterThread(self)
                self.getter_thread.start()

            ev = self.getter_thread.event

        success = ev.wait(timeout=self.timeout)

        with self.lock:
            if success:
                # We fetched the data within the timeout
                return self.VALUE_OK, self.value
            elif self.value_when is not None:
                # We have some data, but it doesn't meet freshness requirements
                return self.VALUE_STALE, self.value
            else:
                # We have no data, not even stale data
                return self.VALUE_NONE, None

    def _init(self):
        pass

    def _get(self):
        raise NotImplementedError()
