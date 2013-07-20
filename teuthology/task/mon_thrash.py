import logging
import contextlib
import ceph_manager
import random
import time
import gevent
import json
import math
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
    thrash_store        Thrash monitor store before killing the monitor
                        being thrashed (default: False)
    thrash_store_probability  Probability of thrashing a monitor's store
                              (default: 50)
    thrash_many         Thrash multiple monitors instead of just one. If
                        'maintain-quorum' is set to False, then we will
                        thrash up to as many monitors as there are
                        available. (default: False)
    maintain_quorum     Always maintain quorum, taking care on how many
                        monitors we kill during the thrashing. If we
                        happen to only have one or two monitors configured,
                        if this option is set to True, then we won't run
                        this task as we cannot guarantee maintenance of
                        quorum. Setting it to false however would allow the
                        task to run with as many as just one single monitor.
                        (default: True)
    freeze_mon_probability: how often to freeze the mon instead of killing it,
                        in % (default: 0)
    freeze_mon_duration: how many seconds to freeze the mon (default: 15)
    scrub               Scrub after each iteration (default: True)

    Note: if 'store-thrash' is set to True, then 'maintain-quorum' must also
          be set to True.

  For example::

    tasks:
    - ceph:
    - mon_thrash:
        revive_delay: 20
        thrash_delay: 1
        thrash_store: true
        thrash_store_probability: 40
        seed: 31337
        maintain_quorum: true
        thrash_many: true
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

    self.thrash_many = self.config.get('thrash_many', False)
    self.maintain_quorum = self.config.get('maintain_quorum', True)

    self.scrub = self.config.get('scrub', True)

    self.freeze_mon_probability = float(self.config.get('freeze_mon_probability', 10))
    self.freeze_mon_duration = float(self.config.get('freeze_mon_duration', 15.0))

    assert self.max_killable() > 0, \
        'Unable to kill at least one monitor with the current config.'

    """ Store thrashing """
    self.store_thrash = self.config.get('store_thrash', False)
    self.store_thrash_probability = int(
        self.config.get('store_thrash_probability', 50))
    if self.store_thrash:
      assert self.store_thrash_probability > 0, \
          'store_thrash is set, probability must be > 0'
      assert self.maintain_quorum, \
          'store_thrash = true must imply maintain_quorum = true'

    self.thread = gevent.spawn(self.do_thrash)

  def log(self, x):
    self.logger.info(x)

  def do_join(self):
    self.stopping = True
    self.thread.get()

  def should_thrash_store(self):
    if not self.store_thrash:
      return False
    return self.rng.randrange(0,101) < self.store_thrash_probability

  def thrash_store(self, mon):
    addr = self.ctx.ceph.conf['mon.%s' % mon]['mon addr']
    self.log('thrashing mon.{id}@{addr} store'.format(id=mon,addr=addr))
    out = self.manager.raw_cluster_cmd('-m', addr, 'sync', 'force')
    j = json.loads(out)
    assert j['ret'] == 0, \
        'error forcing store sync on mon.{id}:\n{ret}'.format(
            id=mon,ret=out)

  def should_freeze_mon(self):
    return self.rng.randrange(0,101) < self.freeze_mon_probability

  def freeze_mon(self, mon):
    log.info('Sending STOP to mon %s', mon)
    self.manager.signal_mon(mon, 19)  # STOP

  def unfreeze_mon(self, mon):
    log.info('Sending CONT to mon %s', mon)
    self.manager.signal_mon(mon, 18)  # CONT

  def kill_mon(self, mon):
    self.log('killing mon.{id}'.format(id=mon))
    self.manager.kill_mon(mon)

  def revive_mon(self, mon):
    self.log('reviving mon.{id}'.format(id=mon))
    self.manager.revive_mon(mon)

  def max_killable(self):
    m = len(_get_mons(self.ctx))
    if self.maintain_quorum:
      return max(math.ceil(m/2.0)-1,0)
    else:
      return m

  def do_thrash(self):
    self.log('start thrashing')
    self.log('seed: {s}, revive delay: {r}, thrash delay: {t} '\
               'thrash many: {tm}, maintain quorum: {mq} '\
               'store thrash: {st}, probability: {stp} '\
               'freeze mon: prob {fp} duration {fd}'.format(
          s=self.random_seed,r=self.revive_delay,t=self.thrash_delay,
          tm=self.thrash_many, mq=self.maintain_quorum,
          st=self.store_thrash,stp=self.store_thrash_probability,
          fp=self.freeze_mon_probability,fd=self.freeze_mon_duration,
          ))

    while not self.stopping:
      mons = _get_mons(self.ctx)
      self.manager.wait_for_mon_quorum_size(len(mons))
      self.log('making sure all monitors are in the quorum')
      for m in mons:
        s = self.manager.get_mon_status(m)
        assert s['state'] == 'leader' or s['state'] == 'peon'
        assert len(s['quorum']) == len(mons)

      kill_up_to = self.rng.randrange(1, self.max_killable()+1)
      mons_to_kill = self.rng.sample(mons, kill_up_to)
      self.log('monitors to thrash: {m}'.format(m=mons_to_kill))

      mons_to_freeze = []
      for mon in mons:
        if mon in mons_to_kill:
          continue
        if self.should_freeze_mon():
          mons_to_freeze.append(mon)
      self.log('monitors to freeze: {m}'.format(m=mons_to_freeze))

      for mon in mons_to_kill:
        self.log('thrashing mon.{m}'.format(m=mon))

        """ we only thrash stores if we are maintaining quorum """
        if self.should_thrash_store() and self.maintain_quorum:
          self.thrash_store(mon)

        self.kill_mon(mon)

      if mons_to_freeze:
        for mon in mons_to_freeze:
          self.freeze_mon(mon)
        self.log('waiting for {delay} secs to unfreeze mons'.format(
            delay=self.freeze_mon_duration))
        time.sleep(self.freeze_mon_duration)
        for mon in mons_to_freeze:
          self.unfreeze_mon(mon)

      if self.maintain_quorum:
        self.manager.wait_for_mon_quorum_size(len(mons)-len(mons_to_kill))
        for m in mons:
          if m in mons_to_kill:
            continue
          s = self.manager.get_mon_status(m)
          assert s['state'] == 'leader' or s['state'] == 'peon'
          assert len(s['quorum']) == len(mons)-len(mons_to_kill)

      self.log('waiting for {delay} secs before reviving monitors'.format(
        delay=self.revive_delay))
      time.sleep(self.revive_delay)

      for mon in mons_to_kill:
        self.revive_mon(mon)

      # do more freezes
      if mons_to_freeze:
        for mon in mons_to_freeze:
          self.freeze_mon(mon)
        self.log('waiting for {delay} secs to unfreeze mons'.format(
            delay=self.freeze_mon_duration))
        time.sleep(self.freeze_mon_duration)
        for mon in mons_to_freeze:
          self.unfreeze_mon(mon)

      self.manager.wait_for_mon_quorum_size(len(mons))
      for m in mons:
        s = self.manager.get_mon_status(m)
        assert s['state'] == 'leader' or s['state'] == 'peon'
        assert len(s['quorum']) == len(mons)

      if self.scrub:
        self.log('triggering scrub')
        try:
          self.manager.raw_cluster_cmd('scrub')
        except:
          pass

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
