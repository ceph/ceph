import sys
import time
import logging
import threading
import traceback
from collections import deque
from mgr_util import lock_timeout_log, CephfsClient

from .operations.volume import list_volumes
from .exception import NotImplementedException

log = logging.getLogger(__name__)


class JobThread(threading.Thread):
    # this is "not" configurable and there is no need for it to be
    # configurable. if a thread encounters an exception, we retry
    # until it hits this many consecutive exceptions.
    MAX_RETRIES_ON_EXCEPTION = 10

    def __init__(self, async_job, volume_client, name):
        threading.Thread.__init__(self, name=name)

        self.vc = volume_client
        self.async_job = async_job
        # event object to cancel jobs
        self.cancel_event = threading.Event()

    def run(self):
        retries = 0
        thread_id = threading.currentThread()
        assert isinstance(thread_id, JobThread)
        thread_name = thread_id.getName()
        log.debug("thread [{0}] starting".format(thread_name))

        while retries < JobThread.MAX_RETRIES_ON_EXCEPTION:
            vol_job = None
            try:
                if not self.async_job.run_event.is_set():
                    log.debug('will wait for run_event to bet set. postponing '
                              'getting jobs until then')
                    self.async_job.run_event.wait()
                    log.debug('run_event has been set, waiting is complete. '
                              'proceeding to get jobs now')

                # fetch next job to execute
                with lock_timeout_log(self.async_job.lock):
                    while True:
                        if self.should_reconfigure_num_threads():
                            log.info("thread [{0}] terminating due to reconfigure".format(thread_name))
                            self.async_job.threads.remove(self)
                            return
                        timo = self.async_job.wakeup_timeout
                        if timo is not None:
                            volnames = list_volumes(self.vc.mgr)
                            missing = set(volnames) - set(self.async_job.q)
                            for m in missing:
                                self.async_job.jobs[m] = []
                                self.async_job.q.append(m)
                        vol_job = self.async_job.get_job()
                        if vol_job:
                            break
                        self.async_job.cv.wait(timeout=timo)
                    self.async_job.register_async_job(vol_job[0], vol_job[1], thread_id)

                # execute the job (outside lock)
                self.async_job.execute_job(vol_job[0], vol_job[1], should_cancel=lambda: thread_id.should_cancel())
                retries = 0
            except NotImplementedException:
                raise
            except Exception:
                # unless the jobs fetching and execution routines are not implemented
                # retry till we hit cap limit.
                retries += 1
                log.warning("thread [{0}] encountered fatal error: (attempt#"
                            " {1}/{2})".format(thread_name, retries, JobThread.MAX_RETRIES_ON_EXCEPTION))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                log.warning("traceback: {0}".format("".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback))))
            finally:
                # when done, unregister the job
                if vol_job:
                    with lock_timeout_log(self.async_job.lock):
                        self.async_job.unregister_async_job(vol_job[0], vol_job[1], thread_id)
                time.sleep(1)
        log.error("thread [{0}] reached exception limit, bailing out...".format(thread_name))
        self.vc.cluster_log("thread {0} bailing out due to exception".format(thread_name))
        with lock_timeout_log(self.async_job.lock):
            self.async_job.threads.remove(self)

    def should_reconfigure_num_threads(self):
        # reconfigure of max_concurrent_clones
        return len(self.async_job.threads) > self.async_job.nr_concurrent_jobs

    def cancel_job(self):
        self.cancel_event.set()

    def should_cancel(self):
        return self.cancel_event.is_set()

    def reset_cancel(self):
        self.cancel_event.clear()

class AsyncJobs(threading.Thread):
    """
    Class providing asynchronous execution of jobs via worker threads.
    `jobs` are grouped by `volume`, so a `volume` can have N number of
    `jobs` executing concurrently (capped by number of concurrent jobs).

    Usability is simple: subclass this and implement the following:
      - get_next_job(volname, running_jobs)
      - execute_job(volname, job, should_cancel)

    ... and do not forget to invoke base class constructor.

    Job cancelation is for a volume as a whole, i.e., all executing jobs
    for a volume are canceled. Cancelation is poll based -- jobs need to
    periodically check if cancelation is requested, after which the job
    should return as soon as possible. Cancelation check is provided
    via `should_cancel()` lambda passed to `execute_job()`.
    """

    # not made configurable on purpose
    WAKEUP_TIMEOUT = 5.0

    def __init__(self, volume_client, name_pfx, nr_concurrent_jobs):
        threading.Thread.__init__(self, name="{0}.tick".format(name_pfx))

        self.vc = volume_client
        # self.q is a deque of names of a volumes for which async jobs needs
        # to be started.
        self.q = deque()  # type: deque

        # self.jobs is a dictionary where volume name is the key and value is
        # a tuple containing two members: the async job and an instance of
        # threading.Thread that performs that job.
        # in short, self.jobs = {volname: (async_job, thread instance)}.
        self.jobs = {}

        # lock, cv for kickstarting jobs
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)

        # Indicates whether or not entire async job machinery is being
        # shutdown/stopped.
        self.stopping = threading.Event()

        self.cancel_cv = threading.Condition(self.lock)

        self.run_event = threading.Event()
        # let async threads run by default
        self.run_event.set()

        self.nr_concurrent_jobs = nr_concurrent_jobs
        self.name_pfx = name_pfx
        # each async job group uses its own libcephfs connection (pool)
        self.fs_client = CephfsClient(self.vc.mgr)
        self.wakeup_timeout = None

        self.threads = []
        self.spawn_all_threads()
        self.start()

    def spawn_new_thread(self, suffix):
        t_name = f'{self.name_pfx}.{time.time()}.{suffix}'
        log.debug(f'spawning new thread with name {t_name}')
        t = JobThread(self, self.vc, name=t_name)
        t.start()

        self.threads.append(t)

    def spawn_all_threads(self):
        log.debug(f'spawning {self.nr_concurrent_jobs} to execute more jobs '
                  'concurrently')
        for i in range(self.nr_concurrent_jobs):
            self.spawn_new_thread(i)

    def spawn_more_threads(self):
        c = len(self.threads)
        diff = self.nr_concurrent_jobs - c
        log.debug(f'spawning {diff} threads to execute more jobs concurrently')

        for i in range(c, self.nr_concurrent_jobs):
            self.spawn_new_thread(i)

    def set_wakeup_timeout(self):
        with self.lock:
            # not made configurable on purpose
            self.wakeup_timeout = AsyncJobs.WAKEUP_TIMEOUT
            self.cv.notifyAll()

    def unset_wakeup_timeout(self):
        with self.lock:
            self.wakeup_timeout = None
            self.cv.notifyAll()

    def run(self):
        log.debug("tick thread {} starting".format(self.name))
        with lock_timeout_log(self.lock):
            while not self.stopping.is_set():
                c = len(self.threads)
                if c > self.nr_concurrent_jobs:
                    # Decrease concurrency: notify threads which are waiting for a job to terminate.
                    log.debug("waking threads to terminate due to job reduction")
                    self.cv.notifyAll()
                elif c < self.nr_concurrent_jobs:
                    # Increase concurrency: create more threads.
                    self.spawn_more_threads()
                self.cv.wait(timeout=self.wakeup_timeout)

    def pause(self):
        self.run_event.clear()

        log.debug('pause() cancelling ongoing jobs now and respective worker '
                  'threads...')
        self.cancel_all_jobs(update_queue=False)

        # XXX: cancel_all_jobs() sets jobthread.cancel_event causing all ongoing
        # jobs to cancel. But if there are no jobs (that is self.q is empty),
        # cancel_all_jobs() will return without doing anything and
        # jobthread.cancel_event won't be set. This results in future jobs to be
        # executed even when config option to pause is already set. Similarly,
        # when there's only 1 ongoing job, jobthread.cancel_event is set for it
        # but not for other threads causing rest of threads to pick new jobs
        # when they are queued.
        # Therefore, set jobthread.cancel_event explicitly.
        log.debug('pause() pausing rest of worker threads')
        for t in self.threads:
            # is_set(), although technically redundant, is called to emphasize
            # that cancel_event might be set on some threads but might not be
            # on others. this prevents removal of the call to set() below after
            # the incomplete observation that cancel_event is already set on
            # (some) threads.
            if not t.cancel_event.is_set():
                t.cancel_event.set()
        log.debug('pause() all jobs cancelled and cancel_event have been set for '
                  'all threads, queue and threads have been paused')

    def resume(self):
        if self.run_event.is_set():
            log.debug('resume() no need to resume, run_event is already set.')
            return

        log.debug('resume() enabling worker threads')
        for t in self.threads:
            t.cancel_event.clear()

        self.run_event.set()
        log.debug('resume() run_event has been set, queue and threads have been'
                  ' resumed')

    def shutdown(self):
        self.stopping.set()
        self.cancel_all_jobs()
        with lock_timeout_log(self.lock):
            self.cv.notifyAll()
        self.join()

    def reconfigure_max_async_threads(self, nr_concurrent_jobs):
        """
        reconfigure number of cloner threads
        """
        self.nr_concurrent_jobs = nr_concurrent_jobs

    def get_job(self):
        log.debug("processing {0} volume entries".format(len(self.q)))
        nr_vols = len(self.q)
        to_remove = []
        next_job = None
        while nr_vols > 0:
            volname = self.q[0]
            # do this now so that the other thread pick up jobs for other volumes
            self.q.rotate(1)
            running_jobs = [j[0] for j in self.jobs[volname]]
            (ret, job) = self.get_next_job(volname, running_jobs)
            if job:
                next_job = (volname, job)
                break
            # this is an optimization when for a given volume there are no more
            # jobs and no jobs are in progress. in such cases we remove the volume
            # from the tracking list so as to:
            #
            # a. not query the filesystem for jobs over and over again
            # b. keep the filesystem connection idle so that it can be freed
            #    from the connection pool
            #
            # if at all there are jobs for a volume, the volume gets added again
            # to the tracking list and the jobs get kickstarted.
            # note that, we do not iterate the volume list fully if there is a
            # jobs to process (that will take place eventually).
            if ret == 0 and not job and not running_jobs:
                to_remove.append(volname)
            nr_vols -= 1
        for vol in to_remove:
            log.debug("auto removing volume '{0}' from tracked volumes".format(vol))
            if vol in self.q:
                self.q.remove(vol)
            if vol in self.jobs:
                self.jobs.pop(vol)
        return next_job

    def register_async_job(self, volname, job, thread_id):
        log.debug("registering async job {0}.{1} with thread {2}".format(volname, job, thread_id))
        self.jobs[volname].append((job, thread_id))

    def unregister_async_job(self, volname, job, thread_id):
        log.debug("unregistering async job {0}.{1} from thread {2}".format(volname, job, thread_id))
        self.jobs[volname].remove((job, thread_id))

        cancelled = thread_id.should_cancel()
        # don't clear cancel_event flag if queuing and threads have been paused
        # (that is, run_event is not set).
        if self.run_event.is_set():
            thread_id.reset_cancel()

        # wake up cancellation waiters if needed
        if cancelled:
            log.info("waking up cancellation waiters")
            self.cancel_cv.notifyAll()

    def queue_job(self, volname):
        """
        queue a volume for asynchronous job execution.
        """
        log.info("queuing job for volume '{0}'".format(volname))
        with lock_timeout_log(self.lock):
            if volname not in self.q:
                self.q.append(volname)
                self.jobs[volname] = []
            self.cv.notifyAll()

    def _cancel_jobs(self, volname, update_queue=True):
        """
        cancel all jobs for the volume. do nothing is the no jobs are
        executing for the given volume. this would wait until all jobs
        get interrupted and finish execution.
        """
        log.info("cancelling jobs for volume '{0}'".format(volname))
        try:
            if volname not in self.q and volname not in self.jobs:
                return

            if update_queue:
                self.q.remove(volname)

            # cancel in-progress operation and wait until complete
            for j in self.jobs[volname]:
                j[1].cancel_job()
            # wait for cancellation to complete
            while self.jobs[volname]:
                log.debug("waiting for {0} in-progress jobs for volume '{1}' to "
                          "cancel".format(len(self.jobs[volname]), volname))
                self.cancel_cv.wait()

            if update_queue:
                self.jobs.pop(volname)
        except (KeyError, ValueError):
            pass

    def _cancel_job(self, volname, job):
        """
        cancel a executing job for a given volume. return True if canceled, False
        otherwise (volume/job not found).
        """
        canceled = False
        log.info("canceling job {0} for volume {1}".format(job, volname))
        try:
            vol_jobs = [j[0] for j in self.jobs.get(volname, [])]
            if volname not in self.q and job not in vol_jobs:
                return canceled
            for j in self.jobs[volname]:
                if j[0] == job:
                    j[1].cancel_job()
                    # be safe against _cancel_jobs() running concurrently
                    while j in self.jobs.get(volname, []):
                        self.cancel_cv.wait()
                    canceled = True
                    break
        except (KeyError, ValueError):
            pass
        return canceled

    def cancel_job(self, volname, job):
        with lock_timeout_log(self.lock):
            return self._cancel_job(volname, job)

    def cancel_jobs(self, volname):
        """
        cancel all executing jobs for a given volume.
        """
        with lock_timeout_log(self.lock):
            self._cancel_jobs(volname)

    def cancel_all_jobs(self, update_queue=True):
        """
        call all executing jobs for all volumes.
        """
        with lock_timeout_log(self.lock):
            for volname in list(self.q):
                self._cancel_jobs(volname, update_queue=update_queue)

    def get_next_job(self, volname, running_jobs):
        """
        get the next job for asynchronous execution as (retcode, job) tuple. if no
        jobs are available return (0, None) else return (0, job). on error return
        (-ret, None). called under `self.lock`.
        """
        raise NotImplementedException()

    def execute_job(self, volname, job, should_cancel):
        """
        execute a job for a volume. the job can block on I/O operations, sleep for long
        hours and do all kinds of synchronous work. called outside `self.lock`.
        """
        raise NotImplementedException()
