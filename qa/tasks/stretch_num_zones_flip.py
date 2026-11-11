"""
Thrasher that moves the pools over two zones into one zone and back with
osd pool set <pool> num_zones, one pool at a time, while a workload runs on
them. The thrash-stretch and thrash-erasure-code-stretch suites run it as
their num-zones-flip thrasher.
"""
import contextlib
import json
import logging
import random
import time

import gevent
from gevent.event import Event
from teuthology import misc as teuthology
from tasks import ceph_manager
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)


class NumZonesFlipper(Thrasher):
    """
    Waits up to pool_timeout seconds for a pool over two zones other than
    .mgr. Then, every delay seconds, it picks one such pool at random and
    runs osd pool set <pool> num_zones 1 (with --yes-i-really-mean-it for
    an erasure coded pool), then num_zones 2. After each change it expects
    the PGs to be clean within clean_timeout seconds. A command that fails,
    or a wait that times out, stops the thrasher with an error.

    Options::

      seed           RNG seed (default: the time)
      delay          seconds between rounds (default: 60)
      clean_timeout  seconds to wait for clean after each change (default: 900)
      pool_timeout   seconds to wait for a pool over zones (default: 300)
    """

    def __init__(self, manager, config, logger):
        super(NumZonesFlipper, self).__init__()
        self.manager = manager
        self.logger = logger
        self.stopping = Event()
        seed = config.get('seed', int(time.time()))
        self.rng = random.Random(int(seed))
        self.delay = float(config.get('delay', 60))
        self.clean_timeout = float(config.get('clean_timeout', 900))
        self.pool_timeout = float(config.get('pool_timeout', 300))
        self.logger.info('seed {s}, delay {d}s'.format(s=seed, d=self.delay))
        self.thread = gevent.spawn(self.do_flip)

    def stop(self):
        self.stopping.set()

    def join(self):
        self.thread.get()

    def stop_and_join(self):
        self.stop()
        return self.join()

    def _pools_over_zones(self):
        pools = json.loads(self.manager.raw_cluster_cmd(
            'osd', 'pool', 'ls', 'detail', '--format=json'))
        return [p for p in pools
                if p.get('num_zones', 1) > 1 and p['pool_name'] != '.mgr']

    def _set_num_zones(self, pool, n):
        args = ['osd', 'pool', 'set', pool['pool_name'], 'num_zones', str(n)]
        if n == 1 and pool['type'] == 3:
            args.append('--yes-i-really-mean-it')
        self.logger.info('pool {p}: num_zones {n}'.format(
            p=pool['pool_name'], n=n))
        self.manager.raw_cluster_cmd(*args)
        self.manager.wait_for_clean(timeout=self.clean_timeout)

    def do_flip(self):
        try:
            self._do_flip()
        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception('exception in NumZonesFlipper:')

    def _do_flip(self):
        end = time.time() + self.pool_timeout
        while not self._pools_over_zones():
            if time.time() > end:
                raise RuntimeError('no pool over zones after {t}s'.format(
                    t=self.pool_timeout))
            if self.stopping.wait(5):
                return
        while not self.stopping.is_set():
            pools = self._pools_over_zones()
            if pools:
                pool = self.rng.choice(pools)
                self._set_num_zones(pool, 1)
                self._set_num_zones(pool, 2)
            self.stopping.wait(self.delay)


@contextlib.contextmanager
def task(ctx, config):
    """
    Run NumZonesFlipper while the tasks after it run, then wait for the PGs
    to be clean. Takes the options of NumZonesFlipper, and cluster (default:
    ceph)::

      tasks:
      - stretch_num_zones_flip:
          delay: 60
    """
    config = config or {}
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    manager = ceph_manager.CephManager(
        mon, ctx=ctx, logger=log.getChild('ceph_manager'))
    flipper = NumZonesFlipper(manager, config,
                              logger=log.getChild('num_zones_flipper'))
    ctx.ceph[config.get('cluster', 'ceph')].thrashers.append(flipper)
    try:
        yield
    finally:
        flipper.stop_and_join()
        manager.wait_for_clean(timeout=config.get('clean_timeout', 900))
