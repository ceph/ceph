import sys
import time
import logging
import threading
import traceback
from collections import deque

log = logging.getLogger(__name__)

class PurgeQueueBase(object):
    """
    Base class for implementing purge queue strategies.
    """

    # this is "not" configurable and there is no need for it to be
    # configurable. if a purge thread encounters an exception, we
    # retry, till it hits this many consecutive exceptions after
    # which a warning is sent to `ceph status`.
    MAX_RETRIES_ON_EXCEPTION = 10

    class PurgeThread(threading.Thread):
        def __init__(self, volume_client, name, purge_fn):
            self.vc = volume_client
            self.purge_fn = purge_fn
            # event object to cancel ongoing purge
            self.cancel_event = threading.Event()
            threading.Thread.__init__(self, name=name)

        def run(self):
            retries = 0
            thread_name = threading.currentThread().getName()
            while retries < PurgeQueueBase.MAX_RETRIES_ON_EXCEPTION:
                try:
                    self.purge_fn()
                    retries = 0
                except Exception:
                    retries += 1
                    log.warning("purge thread [{0}] encountered fatal error: (attempt#" \
                                " {1}/{2})".format(thread_name, retries,
                                                   PurgeQueueBase.MAX_RETRIES_ON_EXCEPTION))
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    log.warning("traceback: {0}".format("".join(
                        traceback.format_exception(exc_type, exc_value, exc_traceback))))
                    time.sleep(1)
            log.error("purge thread [{0}] reached exception limit, bailing out...".format(thread_name))
            self.vc.cluster_log("purge thread {0} bailing out due to exception".format(thread_name))

        def cancel_job(self):
            self.cancel_event.set()

        def should_cancel(self):
            return self.cancel_event.isSet()

        def reset_cancel(self):
            self.cancel_event.clear()

    def __init__(self, volume_client):
        self.vc = volume_client
        # volumes whose subvolumes need to be purged
        self.q = deque()
        # job tracking
        self.jobs = {}
        # lock, cv for kickstarting purge
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        # lock, cv for purge cancellation
        self.waiting = False
        self.c_lock = threading.Lock()
        self.c_cv = threading.Condition(self.c_lock)

    def queue_purge_job(self, volname):
        with self.lock:
            if not self.q.count(volname):
                self.q.append(volname)
                self.jobs[volname] = []
            self.cv.notifyAll()

    def cancel_purge_job(self, volname):
        log.info("cancelling purge jobs for volume '{0}'".format(volname))
        self.lock.acquire()
        unlock = True
        try:
            if not self.q.count(volname):
                return
            self.q.remove(volname)
            if not self.jobs.get(volname, []):
                return
            # cancel in-progress purge operation and wait until complete
            for j in self.jobs[volname]:
                j[1].cancel_job()
            # wait for cancellation to complete
            with self.c_lock:
                unlock = False
                self.waiting = True
                self.lock.release()
                while self.waiting:
                    log.debug("waiting for {0} in-progress purge jobs for volume '{1}' to " \
                             "cancel".format(len(self.jobs[volname]), volname))
                    self.c_cv.wait()
        finally:
            if unlock:
                self.lock.release()

    def register_job(self, volname, purge_dir):
        log.debug("registering purge job: {0}.{1}".format(volname, purge_dir))

        thread_id = threading.currentThread()
        self.jobs[volname].append((purge_dir, thread_id))

    def unregister_job(self, volname, purge_dir):
        log.debug("unregistering purge job: {0}.{1}".format(volname, purge_dir))

        thread_id = threading.currentThread()
        self.jobs[volname].remove((purge_dir, thread_id))

        cancelled = thread_id.should_cancel()
        thread_id.reset_cancel()

        # wake up cancellation waiters if needed
        if not self.jobs[volname] and cancelled:
            logging.info("waking up cancellation waiters")
            self.jobs.pop(volname)
            with self.c_lock:
                self.waiting = False
                self.c_cv.notifyAll()

    def get_trash_entry_for_volume(self, volname):
        log.debug("fetching trash entry for volume '{0}'".format(volname))

        exclude_entries = [v[0] for v in self.jobs[volname]]
        ret = self.vc.get_subvolume_trash_entry(
            None, vol_name=volname, exclude_entries=exclude_entries)
        if not ret[0] == 0:
            log.error("error fetching trash entry for volume '{0}': {1}".format(volname), ret[0])
            return ret[0], None
        return 0, ret[1]

    def purge_trash_entry_for_volume(self, volname, purge_dir):
        log.debug("purging trash entry '{0}' for volume '{1}'".format(purge_dir, volname))

        thread_id = threading.currentThread()
        ret = self.vc.purge_subvolume_trash_entry(
            None, vol_name=volname, purge_dir=purge_dir, should_cancel=lambda: thread_id.should_cancel())
        return ret[0]

class ThreadPoolPurgeQueueMixin(PurgeQueueBase):
    """
    Purge queue mixin class maintaining a pool of threads for purging trash entries.
    Subvolumes are chosen from volumes in a round robin fashion. If some of the purge
    entries (belonging to a set of volumes) have huge directory tree's (such as, lots
    of small files in a directory w/ deep directory trees), this model may lead to
    _all_ threads purging entries for one volume (starving other volumes).
    """
    def __init__(self, volume_client, tp_size):
        super(ThreadPoolPurgeQueueMixin, self).__init__(volume_client)
        self.threads = []
        for i in range(tp_size):
            self.threads.append(
                PurgeQueueBase.PurgeThread(volume_client, name="purgejob.{}".format(i), purge_fn=self.run))
            self.threads[-1].start()

    def pick_purge_dir_from_volume(self):
        log.debug("processing {0} purge job entries".format(len(self.q)))
        nr_vols = len(self.q)
        to_remove = []
        to_purge = None, None
        while nr_vols > 0:
            volname = self.q[0]
            # do this now so that the other thread picks up trash entry
            # for next volume.
            self.q.rotate(1)
            ret, purge_dir = self.get_trash_entry_for_volume(volname)
            if purge_dir:
                to_purge = volname, purge_dir
                break
            # this is an optimization when for a given volume there are no more
            # entries in trash and no purge operations are in progress. in such
            # a case we remove the volume from the tracking list so as to:
            #
            # a. not query the filesystem for trash entries over and over again
            # b. keep the filesystem connection idle so that it can be freed
            #    from the connection pool
            #
            # if at all there are subvolume deletes, the volume gets added again
            # to the tracking list and the purge operations kickstarts.
            # note that, we do not iterate the volume list fully if there is a
            # purge entry to process (that will take place eventually).
            if ret == 0 and not purge_dir and not self.jobs[volname]:
                to_remove.append(volname)
            nr_vols -= 1
        for vol in to_remove:
            log.debug("auto removing volume '{0}' from purge job".format(vol))
            self.q.remove(vol)
            self.jobs.pop(vol)
        return to_purge

    def get_next_trash_entry(self):
        while True:
            # wait till there's a purge job
            while not self.q:
                log.debug("purge job list empty, waiting...")
                self.cv.wait()
            volname, purge_dir = self.pick_purge_dir_from_volume()
            if purge_dir:
                return volname, purge_dir
            log.debug("no purge jobs available, waiting...")
            self.cv.wait()

    def run(self):
        while True:
            with self.lock:
                volname, purge_dir = self.get_next_trash_entry()
                self.register_job(volname, purge_dir)
            ret = self.purge_trash_entry_for_volume(volname, purge_dir)
            if ret != 0:
                log.warn("failed to purge {0}.{1}".format(volname, purge_dir))
            with self.lock:
                self.unregister_job(volname, purge_dir)
            time.sleep(1)
