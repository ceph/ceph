"""
CEPH-MGR Thrash -- Simulate MGR fails and switching between Standby and Active mgrs.
"""
import contextlib
import logging
import time

import gevent
import gevent.event

from tasks import ceph_manager
from teuthology import misc as teuthology
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)


class ThrashMGRs(Thrasher):
    """
    Thrash the MGRs by randomly failing them (and then restarting them) until the
    task is ended. This loops, and every thrash_delay seconds it randomly
    chooses to fail or restart a MGR it will always keep at least one MGR active.

    The config is optional, and is a dict containing some or all of:

    cluster: (default 'ceph') the name of the cluster to thrash

    thrash_delay: (5) the length of time to sleep between changing a
       MGR's status

    duration: (0) the length of time to run the task before stopping. If 0,
       runs until __exit__ is called.

    timeout: (duration + 60) the length of time to wait for the task to stop
       before giving up and raising an exception. This should be longer than
       duration to allow for the task to stop after the duration has elapsed.

    for example, to thrash the MGRs for 10 minutes with a 30 second delay between operations:
    tasks:
    - mgr_thrash:
        thrash_delay: 30
        duration: 600
    """
    def __init__(self, ctx, manager, config, name, logger):
        super(ThrashMGRs, self).__init__()

        self.ceph_manager = manager
        self.logger = logger
        self.config = config or {}
        self.name = name
        self.stopping = gevent.event.Event()

        self.cluster = self.config.get('cluster', 'ceph')
        self.thrash_delay = self.config.get('thrash_delay', 5)
        self.duration = self.config.get('duration', 0)
        self.timeout = self.config.get('timeout', self.duration + 60)

        self.thread = gevent.spawn(self.do_thrash)

    def log(self, message):
        self.logger.info(f'{self.name}: {message}')

    def stop(self):
        self.stopping.set()

    def join(self):
        self.thread.get()

    def stop_and_join(self):
        self.stop()
        self.join()

    def do_thrash(self):
        try:
            self._do_thrash()
        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception("Exception in MGR thrash:")

    def _do_thrash(self):
        self.log('Starting MGR thrash')
        self.ceph_manager.wait_for_mgr_available(self.timeout)
        start_time = time.time()

        while not self.stopping.is_set():
            if self.duration and (time.time() - start_time) > self.duration:
                self.log('Duration exceeded, stopping MGR thrash')
                break

            mgr_stats = self.ceph_manager.get_mgr_stat()
            if not mgr_stats.get('active_name'):
                self.log('No active MGR found, waiting for an active MGR before thrashing')
                self.ceph_manager.wait_for_mgr_available(self.timeout)
                continue

            active = mgr_stats.get('active_name', '')
            self.log('Failing active MGR: {active}'.format(active=active))

            self.ceph_manager.raw_cluster_cmd('mgr', 'fail')
            self.ceph_manager.wait_for_mgr_available(self.timeout)
            gevent.sleep(self.thrash_delay)


@contextlib.contextmanager
def task(ctx, config):
    if config is None:
        config = {}

    if config.get('cluster') is None:
        config['cluster'] = 'ceph'

    logger = config.get('logger', 'mgr_thrasher')

    log.info('Starting MGR thrash task with config: {config}'.format(config=str(config)))
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    thrash_proc = ThrashMGRs(ctx, manager, config, "MGRThrasher", log.getChild(logger))
    ctx.ceph[config['cluster']].thrashers.append(thrash_proc)
    try:
        log.debug('Yielding from MGR thrash task')
        yield
    finally:
        log.info('Joining MGR thrash task')
        thrash_proc.stop_and_join()
        manager.wait_for_mgr_available(config.get('timeout', 60))
