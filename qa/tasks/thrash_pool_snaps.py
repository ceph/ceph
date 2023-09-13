"""
Thrash -- Simulate random osd failures.
"""
import contextlib
import logging
import gevent
import time
import random


log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    "Thrash" snap creation and removal on the listed pools

    Example:

    thrash_pool_snaps:
      pools: [.rgw.buckets, .rgw.buckets.index]
      max_snaps: 10
      min_snaps: 5
      period: 10
    """
    stopping = False
    def do_thrash():
        pools = config.get('pools', [])
        max_snaps = config.get('max_snaps', 10)
        min_snaps = config.get('min_snaps', 5)
        period = config.get('period', 30)
        snaps = []
        manager = ctx.managers['ceph']
        def remove_snap():
            assert len(snaps) > 0
            snap = random.choice(snaps)
            log.info("Removing snap %s" % (snap,))
            for pool in pools:
                manager.remove_pool_snap(pool, str(snap))
            snaps.remove(snap)
        def add_snap(snap):
            log.info("Adding snap %s" % (snap,))
            for pool in pools:
                manager.add_pool_snap(pool, str(snap))
            snaps.append(snap)
        index = 0
        while not stopping:
            index += 1
            time.sleep(period)
            if len(snaps) <= min_snaps:
                add_snap(index)
            elif len(snaps) >= max_snaps:
                remove_snap()
            else:
                random.choice([lambda: add_snap(index), remove_snap])()
        log.info("Stopping")
    thread = gevent.spawn(do_thrash)
    yield
    stopping = True
    thread.join()

