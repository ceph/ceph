import logging
import contextlib
import ceph_manager
import random
import time
import gevent
import json
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def _get_mons(ctx):
  mons = [f[len('mon.'):] for f in teuthology.get_mon_names(ctx)]
  return mons

class MonitorThrasher:
  """
  How it works::

    - pick a monitor
    - kill it
    - wait for quorum to be formed
    - sleep for 'revive_delay' seconds
    - revive monitor
    - wait for quorum to be formed
    - sleep for 'thrash_delay' seconds

  Options::

    seed                Seed to use on the RNG to reproduce a previous
                        behaviour (default: None; i.e., not set)
    revive_delay        Number of seconds to wait before reviving
                        the monitor (default: 10)
    thrash_delay        Number of seconds to wait in-between
                        test iterations (default: 0)
    store-thrash        Thrash monitor store before killing the monitor
                        being thrashed (default: False)
    store-thrash-probability  Probability of thrashing a monitor's store
                              (default: 50)

  For example::

    tasks:
    - ceph:
    - mon_thrash:
        revive_delay: 20
        thrash_delay: 1
        store-thrash: true
        store-thrash-probability: 40
        seed: 31337
    - ceph-fuse:
    - workunit:
        clients:
          all:
            - mon/workloadgen.sh
  """
  def __init__(self, ctx, manager, config, logger):
    self.ctx = ctx
    self.manager = manager
    self.manager.wait_for_clean()

    self.stopping = False
    self.logger = logger
    self.config = config

    if self.config is None:
      self.config = dict()

    """ Test reproducibility """
    self.random_seed = self.config.get('seed', None)

    if self.random_seed is None:
      self.random_seed = int(time.time())

    self.rng = random.Random()
    self.rng.seed(int(self.random_seed))

    """ Monitor thrashing """
    self.revive_delay = float(self.config.get('revive_delay', 10.0))
    self.thrash_delay = float(self.config.get('thrash_delay', 0.0))

    """ Store thrashing """
    self.store_thrash = self.config.get('store-thrash', False)
    self.store_thrash_probability = int(
        self.config.get('store-thrash-probability', 50))
    if self.store_thrash:
      assert self.store_thrash_probability > 0, \
          'store-thrash is set, probability must be > 0'

    self.thread = gevent.spawn(self.do_thrash)

  def log(self, x):
    self.logger.info(x)

  def do_join(self):
    self.stopping = True
    self.thread.get()

  def should_thrash_store(self):
    if not self.store_thrash:
      return False
    return self.rng.randrange(0,101) >= self.store_thrash_probability

  def thrash_store(self, mon):
    addr = self.ctx.ceph.conf['mon.%s' % mon]['mon addr']
    self.log('thrashing mon.{id}@{addr} store'.format(id=mon,addr=addr))
    out = self.manager.raw_cluster_cmd('-m', addr, 'sync', 'force')
    j = json.loads(out)
    assert j['ret'] == 0, \
        'error forcing store sync on mon.{id}:\n{ret}'.format(
            id=mon,ret=out)

  def kill_mon(self, mon):
    self.log('killing mon.{id}'.format(id=mon))
    self.manager.kill_mon(mon)

  def revive_mon(self, mon):
    self.log('reviving mon.{id}'.format(id=mon))
    self.manager.revive_mon(mon)


  def do_thrash(self):
    self.log('start thrashing')
    self.log('seed: {s}, revive delay: {r}, thrash delay: {t} '\
        'store thrash: {st}, probability: {stp}'.format(
          s=self.random_seed,r=self.revive_delay,t=self.thrash_delay,
          st=self.store_thrash,stp=self.store_thrash_probability))

    while not self.stopping:
      mons = _get_mons(self.ctx)
      self.manager.wait_for_mon_quorum_size(len(mons))
      self.log('making sure all monitors are in the quorum')
      for m in mons:
        s = self.manager.get_mon_status(m)
        assert s['state'] == 'leader' or s['state'] == 'peon'
        assert len(s['quorum']) == len(mons)

      self.log('pick a monitor to kill')
      to_kill = self.rng.choice(mons)

      if self.should_thrash_store():
        self.thrash_store(to_kill)

      self.kill_mon(to_kill)

      self.manager.wait_for_mon_quorum_size(len(mons)-1)
      for m in mons:
        if m == to_kill:
          continue
        s = self.manager.get_mon_status(m)
        assert s['state'] == 'leader' or s['state'] == 'peon'
        assert len(s['quorum']) == len(mons)-1

      self.log('waiting for {delay} secs before reviving mon.{id}'.format(
        delay=self.revive_delay,id=to_kill))
      time.sleep(self.revive_delay)

      self.revive_mon(to_kill)
      self.manager.wait_for_mon_quorum_size(len(mons))
      for m in mons:
        s = self.manager.get_mon_status(m)
        assert s['state'] == 'leader' or s['state'] == 'peon'
        assert len(s['quorum']) == len(mons)

      if self.thrash_delay > 0.0:
        self.log('waiting for {delay} secs before continuing thrashing'.format(
          delay=self.thrash_delay))
        time.sleep(self.thrash_delay)

@contextlib.contextmanager
def task(ctx, config):
  """
  Stress test the monitor by thrashing them while another task/workunit
  is running.

  Please refer to MonitorThrasher class for further information on the
  available options.
  """
  if config is None:
    config = {}
  assert isinstance(config, dict), \
      'mon_thrash task only accepts a dict for configuration'
  assert len(_get_mons(ctx)) > 2, \
      'mon_thrash task requires at least 3 monitors'
  log.info('Beginning mon_thrash...')
  first_mon = teuthology.get_first_mon(ctx, config)
  (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
  manager = ceph_manager.CephManager(
      mon,
      ctx=ctx,
      logger=log.getChild('ceph_manager'),
      )
  thrash_proc = MonitorThrasher(ctx,
      manager, config,
      logger=log.getChild('mon_thrasher'))
  try:
    log.debug('Yielding')
    yield
  finally:
    log.info('joining mon_thrasher')
    thrash_proc.do_join()
    mons = _get_mons(ctx)
    manager.wait_for_mon_quorum_size(len(mons))
