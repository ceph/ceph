import sys
import time
import logging
import threading
import traceback
from uuid import uuid4
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
        self.vc = volume_client
        self.async_job = async_job
        # event object to cancel jobs
        self.cancel_event = threading.Event()
        threading.Thread.__init__(self, name=name)

    def run(self):
        retries = 0
        thread_id = threading.currentThread()
        assert isinstance(thread_id, JobThread)
        thread_name = thread_id.getName()
        log.debug("thread [{0}] starting".format(thread_name))

        while retries < JobThread.MAX_RETRIES_ON_EXCEPTION:
            vol_job = None
            try:
                # fetch next job to execute
                with lock_timeout_log(self.async_job.lock):
                    while True:
                        if self.should_reconfigure_num_threads():
                            log.info("thread [{0}] terminating due to reconfigure".format(thread_name))
                            self.async_job.threads.remove(self)
                            return
                        timo = self.async_job.wakeup_timeout
                        if timo is not None:
                            vols = [e['name'] for e in list_volumes(self.vc.mgr)]
                            missing = set(vols) - set(self.async_job.q)
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
        # queue of volumes for starting async jobs
        self.q = deque()  # type: deque
        # volume => job tracking
        self.jobs = {}
        # this will store subvol we are operating as key and misc_util.Stats
        # instance as value.
        self.job_stats = {}
        # RemoteEvent ID ongoing clone jobs.
        # mgr.progress.module.RemoteEvent is what prints the progress bar.
        self.clone_ongoing_ev_id = None
        # lock, cv for kickstarting jobs
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        # cv for job cancelation
        self.waiting = False
        self.stopping = threading.Event()
        self.cancel_cv = threading.Condition(self.lock)
        self.nr_concurrent_jobs = nr_concurrent_jobs
        self.name_pfx = name_pfx
        # each async job group uses its own libcephfs connection (pool)
        self.fs_client = CephfsClient(self.vc.mgr)
        self.wakeup_timeout = None

        self.threads = []
        for i in range(self.nr_concurrent_jobs):
            self.threads.append(JobThread(self, volume_client, name="{0}.{1}".format(self.name_pfx, i)))
            self.threads[-1].start()
        self.start()

    def set_wakeup_timeout(self):
        with self.lock:
            # not made configurable on purpose
            self.wakeup_timeout = AsyncJobs.WAKEUP_TIMEOUT
            self.cv.notifyAll()

    def unset_wakeup_timeout(self):
        with self.lock:
            self.wakeup_timeout = None
            self.cv.notifyAll()

    def get_clone_job_stats(self, subvolname):
        if subvolname in self.job_stats:
            return self.job_stats[subvolname]
        else:
            return None

    def report_pending_job(self, subvolname):
        # skip marking job as pending if it is already in progress or
        # finished.
        if subvolname in self.job_stats and \
           self.job_stats[subvolname] != 'pending':
                return

        self.job_stats[subvolname] = 'pending'

    def report_ongoing_job(self, subvolname, stats):
        assert stats is not None
        self.job_stats[subvolname] = stats

        total_jobs = len(self.job_stats)
        progress_sum = 0.0
        for subvolname, subvol_stats in self.job_stats.items():
            if subvol_stats is not None:
                progress_sum += subvol_stats.progress_fraction
        progress_avg = progress_sum / total_jobs
        progress_percent = round(progress_avg * 100, 3)

        if self.clone_ongoing_ev_id is None:
            self.clone_ongoing_ev_id = str(uuid4())

        if total_jobs == 1:
            msg = (f'Subvolume "{subvolname}" has been '
                    '{progress_percent}% cloned')
        else:
            msg = (f'{total_jobs} ongoing cloning jobs; Avg progress = '
                   f'{progress_percent}%')

        self.vc.mgr.remote(
            'progress', 'update', ev_id=self.clone_ongoing_ev_id, ev_msg=msg,
            ev_progress=stats.progress_fraction, refs=['mds', 'clone'],
            add_to_ceph_s=True)

    def finish_job_reporting(self, subvolname):
        assert self.clone_ongoing_ev_id is not None

        if self.job_stats[subvolname].progress_fraction < 1.0:
            log.critical('A job is being marked as finished but it is not'
                         'complete. Progress should be 1.0 but it is only '
                         f'{self.job_stats[subvolname].progress_fraction}.')

        self.job_stats.pop(subvolname)

        total_ongoing_jobs = 0
        for subvolname, stats in self.job_stats.items():
            if stats is not None:
                total_ongoing_jobs += 1

        if total_ongoing_jobs == 0:
            self.vc.mgr.remote('progress', 'complete', self.clone_ongoing_ev_id)

    def abort_job_reporting(self, subvolname):
        assert self.clone_ongoing_ev_id is not None

        if self.job_stats[subvolname] != 'pending':
            log.critical('A job is being aborted. Progress made so far is '
                         f'{self.job_stats[subvolname].progress_fraction}.')

        self.job_stats.pop(subvolname)

        msg = f'Cloning failed for {subvolname}'
        self.vc.mgr.remote('progress', 'fail', self.clone_ongoing_ev_id, msg)

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
                    log.debug("creating new threads to job increase")
                    for i in range(c, self.nr_concurrent_jobs):
                        self.threads.append(JobThread(self, self.vc, name="{0}.{1}.{2}".format(self.name_pfx, time.time(), i)))
                        self.threads[-1].start()
                self.cv.wait(timeout=5)

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
            self.q.remove(vol)
            self.jobs.pop(vol)
        return next_job

    def register_async_job(self, volname, job, thread_id):
        log.debug("registering async job {0}.{1} with thread {2}".format(volname, job, thread_id))
        self.jobs[volname].append((job, thread_id))

    def unregister_async_job(self, volname, job, thread_id):
        log.debug("unregistering async job {0}.{1} from thread {2}".format(volname, job, thread_id))
        self.jobs[volname].remove((job, thread_id))

        cancelled = thread_id.should_cancel()
        thread_id.reset_cancel()

        # wake up cancellation waiters if needed
        if cancelled:
            logging.info("waking up cancellation waiters")
            self.cancel_cv.notifyAll()

    def queue_job(self, volname, subvolname=None):
        """
        queue a volume for asynchronous job execution.
        """
        log.info("queuing job for volume '{0}'".format(volname))
        with lock_timeout_log(self.lock):
            if volname not in self.q:
                self.q.append(volname)
                self.jobs[volname] = []
            self.cv.notifyAll()

        if subvolname is not None:
            if 'cloner' in self.__class__.__name__.lower():
                self.report_pending_job(subvolname)

    def _cancel_jobs(self, volname):
        """
        cancel all jobs for the volume. do nothing is the no jobs are
        executing for the given volume. this would wait until all jobs
        get interrupted and finish execution.
        """
        log.info("cancelling jobs for volume '{0}'".format(volname))
        try:
            if volname not in self.q and volname not in self.jobs:
                return
            self.q.remove(volname)
            # cancel in-progress operation and wait until complete
            for j in self.jobs[volname]:
                j[1].cancel_job()
            # wait for cancellation to complete
            while self.jobs[volname]:
                log.debug("waiting for {0} in-progress jobs for volume '{1}' to "
                          "cancel".format(len(self.jobs[volname]), volname))
                self.cancel_cv.wait()
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

    def cancel_all_jobs(self):
        """
        call all executing jobs for all volumes.
        """
        with lock_timeout_log(self.lock):
            for volname in list(self.q):
                self._cancel_jobs(volname)

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
