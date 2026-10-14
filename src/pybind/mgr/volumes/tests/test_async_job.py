import threading

import pytest

from tests import mock  # noqa: F401 -- sets up ceph module mocks

from volumes.fs import async_job
from volumes.fs.async_job import AsyncJobs, JobThread
from volumes.fs.exception import JobDeferred


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeJobs(AsyncJobs):
    """
    AsyncJobs without the tick thread and worker threads; jobs come from
    `self.available` ({volname: [job, ...]}).
    """
    DEFER_MIN = 5.0
    DEFER_MAX = 40.0

    def __init__(self):
        self.available = {}
        self.executed = []
        # should_cancel() as seen by each executed job
        self.cancel_seen = []
        self.execute = None
        with mock.patch.object(async_job, 'CephfsClient'):
            super().__init__(mock.MagicMock(), 'test', 0)

    def spawn_all_threads(self):
        pass

    def start(self):
        pass

    def get_next_job(self, volname, running_jobs):
        for job in self.available.get(volname, []):
            if job not in running_jobs:
                return 0, job
        return 0, None

    def execute_job(self, volname, job, should_cancel):
        self.executed.append((volname, job))
        self.cancel_seen.append(should_cancel())
        if self.execute:
            self.execute(volname, job)


@pytest.fixture
def clock():
    c = FakeClock()
    with mock.patch.object(async_job.time, 'monotonic', c):
        yield c


@pytest.fixture
def jobs():
    j = FakeJobs()
    j.q.append('vol')
    j.jobs['vol'] = []
    return j


def test_deferred_job_is_skipped(jobs, clock):
    jobs.available['vol'] = ['a', 'b']
    jobs.defer_job('vol', 'a')
    assert jobs.get_job() == ('vol', 'b')


def test_volume_with_only_deferred_jobs_is_kept(jobs, clock):
    jobs.available['vol'] = ['a']
    jobs.defer_job('vol', 'a')
    assert jobs.get_job() is None
    assert 'vol' in jobs.q
    assert 'vol' in jobs.deferred


def test_deferred_job_is_picked_after_backoff(jobs, clock):
    jobs.available['vol'] = ['a']
    jobs.defer_job('vol', 'a')
    clock.now += FakeJobs.DEFER_MIN - 0.1
    assert jobs.get_job() is None
    clock.now += 0.2
    assert jobs.get_job() == ('vol', 'a')


def test_backoff_grows_and_is_capped(jobs, clock):
    delays = []
    for _ in range(6):
        jobs.defer_job('vol', 'a')
        deadline, _ = jobs.deferred['vol']['a']
        delays.append(deadline - clock.now)
    assert delays == [5.0, 10.0, 20.0, 40.0, 40.0, 40.0]


def test_undefer_resets_backoff(jobs, clock):
    jobs.defer_job('vol', 'a')
    jobs.defer_job('vol', 'a')
    jobs.undefer_job('vol', 'a')
    assert 'vol' not in jobs.deferred
    jobs.defer_job('vol', 'a')
    assert jobs.deferred['vol']['a'] == (clock.now + FakeJobs.DEFER_MIN, 1)


def test_undefer_unknown_job_is_noop(jobs, clock):
    jobs.undefer_job('vol', 'a')
    jobs.undefer_job('novol', 'a')
    assert jobs.deferred == {}


def test_clear_deferred_makes_jobs_eligible(jobs, clock):
    jobs.available['vol'] = ['a']
    jobs.defer_job('vol', 'a')
    jobs.clear_deferred('vol')
    assert jobs.get_job() == ('vol', 'a')


def test_clear_deferred_requeues_volume(jobs, clock):
    jobs.q.clear()
    jobs.jobs.clear()
    jobs.deferred['vol'] = {'a': (clock.now + 100, 1)}
    jobs.available['vol'] = ['a']
    jobs.clear_deferred('vol')
    assert list(jobs.q) == ['vol']
    assert jobs.get_job() == ('vol', 'a')


def test_idle_volume_drops_stale_deferral(jobs, clock):
    # backoff expired and the job is gone (e.g., entry removed on disk)
    jobs.deferred['vol'] = {'a': (clock.now - 1, 3)}
    assert jobs.get_job() is None
    assert 'vol' not in jobs.q
    assert 'vol' not in jobs.deferred


def test_cancel_jobs_drops_deferral(jobs, clock):
    jobs.defer_job('vol', 'a')
    jobs.cancel_jobs('vol')
    assert 'vol' not in jobs.deferred


def test_wait_timeout(jobs, clock):
    assert jobs.get_wait_timeout() is None
    jobs.defer_job('vol', 'a')
    assert jobs.get_wait_timeout() == pytest.approx(FakeJobs.DEFER_MIN)
    jobs.wakeup_timeout = AsyncJobs.WAKEUP_TIMEOUT
    clock.now += 1
    assert jobs.get_wait_timeout() == pytest.approx(
        min(AsyncJobs.WAKEUP_TIMEOUT, FakeJobs.DEFER_MIN - 1))
    # expired deferrals do not shorten the wait
    clock.now += FakeJobs.DEFER_MIN
    assert jobs.get_wait_timeout() == AsyncJobs.WAKEUP_TIMEOUT


def test_job_thread_retries_deferred_job():
    """
    A job raising JobDeferred more often than the exception retry cap must
    neither kill the worker thread nor be retried without a backoff, and
    must run to completion once it stops deferring.
    """
    ndefers = JobThread.MAX_RETRIES_ON_EXCEPTION + 2
    done = threading.Event()

    j = FakeJobs()
    j.DEFER_MIN = 0.01
    j.DEFER_MAX = 0.02
    j.available['vol'] = ['a']

    def execute(volname, job):
        if len(j.executed) <= ndefers:
            raise JobDeferred('quarantined')
        j.available['vol'].remove(job)
        done.set()
    j.execute = execute

    with mock.patch.object(async_job.time, 'sleep'):
        t = JobThread(j, j.vc, name='test.0')
        j.threads.append(t)
        j.nr_concurrent_jobs = 1
        t.start()
        j.queue_job('vol')
        try:
            assert done.wait(timeout=10)
        finally:
            # make the worker exit via the reconfigure path
            with j.lock:
                j.nr_concurrent_jobs = 0
                j.cv.notify_all()
            t.join(timeout=10)

    assert not t.is_alive()
    assert j.executed == [('vol', 'a')] * (ndefers + 1)
    assert j.deferred == {}
    j.vc.cluster_log.assert_not_called()  # thread did not bail out


def test_job_queued_while_paused_waits_for_resume():
    """
    A job queued while paused must not be picked up by an idle worker
    thread (which would run it with its cancel event set, i.e., cancel it),
    but run normally once resumed.
    """
    done = threading.Event()
    j = FakeJobs()

    def execute(volname, job):
        j.available['vol'].remove(job)
        done.set()
    j.execute = execute

    with mock.patch.object(async_job.time, 'sleep'):
        t = JobThread(j, j.vc, name='test.0')
        j.threads.append(t)
        j.nr_concurrent_jobs = 1
        t.start()
        try:
            # let the worker go idle, waiting for a job
            threading.Event().wait(0.3)
            j.pause()
            j.available['vol'] = ['a']
            j.queue_job('vol')
            assert not done.wait(timeout=1), "job ran while paused"
            assert j.executed == []

            j.resume()
            assert done.wait(timeout=10), "job did not run after resume"
            assert j.executed == [('vol', 'a')]
            assert j.cancel_seen == [False]
        finally:
            j.resume()
            with j.lock:
                j.nr_concurrent_jobs = 0
                j.cv.notify_all()
            t.join(timeout=10)
    assert not t.is_alive()
