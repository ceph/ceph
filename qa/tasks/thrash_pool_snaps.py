"""
Pool Snaps Thrash
"""
import contextlib
import logging
import gevent
import time
import random
from gevent import sleep
from gevent.greenlet import Greenlet
from gevent.event import Event
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)

class PoolSnapsThrasher(Greenlet, Thrasher):
    """
    PoolSnapsThrasher::

    "Thrash" snap creation and removal on the listed pools

    Example:

    pool_snaps_thrash:
      pools: [.rgw.buckets, .rgw.buckets.index]
      max_snaps: 10
      min_snaps: 5
      period: 10

    """
    def __init__(self, ctx, config):
        super(PoolSnapsThrasher, self).__init__()
        self.ctx = ctx
        self.config = config
        self.logger = log
        self.pools = self.config.get('pools', [])
        self.max_snaps = self.config.get('max_snaps', 10)
        self.min_snaps = self.config.get('min_snaps', 5)
        self.period = self.config.get('period', 30)
        self.snaps = []
        self.manager = self.ctx.managers['ceph']
        self.stopping = Event()

    def _run(self):
        try:
            self._do_thrash()
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.exception = e
            self.logger.exception("exception:")
            # Allow successful completion so gevent doesn't see an exception.
            # The DaemonWatchdog will observe the error and tear down the test.

    def log(self, x):
        """Write data to logger assigned to this PoolSnapsThrasher"""
        self.logger.info(x)

    def stop(self):
        self.stopping.set()

    def add_snap(self, snap):
        self.log("Adding snap %s" % (snap,))
        for pool in self.pools:
            self.manager.add_pool_snap(pool, str(snap))
        self.snaps.append(snap)

    def remove_snap(self):
        assert len(self.snaps) > 0
        snap = random.choice(self.snaps)
        self.log("Removing snap %s" % (snap,))
        for pool in self.pools:
            self.manager.remove_pool_snap(pool, str(snap))
        self.snaps.remove(snap)

    def _do_thrash(self):
        """
        Perform the random thrashing action
        """
        index = 0
        while not self.stopping.is_set():
            index += 1
            sleep(self.period)
            if len(self.snaps) <= self.min_snaps:
                self.add_snap(index)
            elif len(self.snaps) >= self.max_snaps:
                self.remove_snap()
            else:
                random.choice([lambda: self.add_snap(index), self.remove_snap])()
        self.log("Stopping")

@contextlib.contextmanager
def task(ctx, config):

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'pool_snaps_thrash task only accepts a dict for configuration'

    thrasher = PoolSnapsThrasher(ctx, config)
    thrasher.start()

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining pool_snaps_thrasher')
        thrasher.stop()
        if thrasher.exception is not None:
            raise RuntimeError('error during thrashing')
        thrasher.join()
        log.info('done joining')
