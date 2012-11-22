import logging
import contextlib
import ceph_manager
import random
import time
import gevent
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

    revive_delay        Number of seconds to wait before reviving
                        the monitor (default: 10)
    thrash_delay        Number of seconds to wait in-between
                        test iterations (default: 0)

  For example::

    tasks:
    - ceph:
    - mon_thrash:
        revive_delay: 20
        thrash_delay: 1
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

    self.revive_delay = float(self.config.get('revive_delay', 10.0))
    self.thrash_delay = float(self.config.get('thrash_delay', 0.0))

    self.thread = gevent.spawn(self.do_thrash)

  def log(self, x):
    self.logger.info(x)

  def do_join(self):
    self.stopping = True
    self.thread.get()

  def do_thrash(self):
    self.log('start do_thrash')
    while not self.stopping:
      mons = _get_mons(self.ctx)
      self.manager.wait_for_mon_quorum_size(len(mons))
      self.log('making sure all monitors are in the quorum')
      for m in mons:
        s = self.manager.get_mon_status(m)
        assert s['state'] == 'leader' or s['state'] == 'peon'
        assert len(s['quorum']) == len(mons)
      self.log('pick a monitor to kill')
      to_kill = random.choice(mons)

      self.log('kill mon.{id}'.format(id=to_kill))
      self.manager.kill_mon(to_kill)
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

      self.log('revive mon.{id}'.format(id=to_kill))
      self.manager.revive_mon(to_kill)
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
