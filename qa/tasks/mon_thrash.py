"""
Monitor thrash
"""
import logging
import contextlib
import random
import time
import gevent
import json
import math
from teuthology import misc as teuthology
from teuthology.contextutil import safe_while
from tasks import ceph_manager
from tasks.cephfs.filesystem import MDSCluster
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)

def _get_mons(ctx):
    """
    Get monitor names from the context value.
    """
    mons = [f[len('mon.'):] for f in teuthology.get_mon_names(ctx)]
    return mons

class MonitorThrasher(Thrasher):
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
    store_thrash        Thrash monitor store before killing the monitor being thrashed (default: False)
    store_thrash_probability  Probability of thrashing a monitor's store
                              (default: 50)
    thrash_many         Thrash multiple monitors instead of just one. If
                        'maintain_quorum' is set to False, then we will
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
    check_mds_failover  Check if mds failover happened (default: False)

    Note: if 'store_thrash' is set to True, then 'maintain_quorum' must also
          be set to True.

    For example::

    tasks:
    - ceph:
    - mon_thrash:
        revive_delay: 20
        thrash_delay: 1
        store_thrash: true
        store_thrash_probability: 40
        seed: 31337
        maintain_quorum: true
        thrash_many: true
        check_mds_failover: True
    - ceph-fuse:
    - workunit:
        clients:
          all:
            - mon/workloadgen.sh
    """
    def __init__(self, ctx, manager, config, name, logger):
        super(MonitorThrasher, self).__init__()

        self.ctx = ctx
        self.manager = manager
        self.manager.wait_for_clean()

        self.stopping = False
        self.logger = logger
        self.config = config
        self.name = name

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

        #MDS failover
        self.mds_failover = self.config.get('check_mds_failover', False)

        if self.mds_failover:
            self.mds_cluster = MDSCluster(ctx)

        self.thread = gevent.spawn(self.do_thrash)

    def log(self, x):
        """
        locally log info messages
        """
        self.logger.info(x)

    def do_join(self):
        """
        Break out of this processes thrashing loop.
        """
        self.stopping = True
        self.thread.get()

    def should_thrash_store(self):
        """
        If allowed, indicate that we should thrash a certain percentage of
        the time as determined by the store_thrash_probability value.
        """
        if not self.store_thrash:
            return False
        return self.rng.randrange(0, 101) < self.store_thrash_probability

    def thrash_store(self, mon):
        """
        Thrash the monitor specified.
        :param mon: monitor to thrash
        """
        self.log('thrashing mon.{id} store'.format(id=mon))
        out = self.manager.raw_cluster_cmd(
            'tell', 'mon.%s' % mon, 'sync_force',
            '--yes-i-really-mean-it')
        j = json.loads(out)
        assert j['ret'] == 0, \
            'error forcing store sync on mon.{id}:\n{ret}'.format(
                id=mon,ret=out)

    def should_freeze_mon(self):
        """
        Indicate that we should freeze a certain percentago of the time
        as determined by the freeze_mon_probability value.
        """
        return self.rng.randrange(0, 101) < self.freeze_mon_probability

    def freeze_mon(self, mon):
        """
        Send STOP signal to freeze the monitor.
        """
        log.info('Sending STOP to mon %s', mon)
        self.manager.signal_mon(mon, 19)  # STOP

    def unfreeze_mon(self, mon):
        """
        Send CONT signal to unfreeze the monitor.
        """
        log.info('Sending CONT to mon %s', mon)
        self.manager.signal_mon(mon, 18)  # CONT

    def kill_mon(self, mon):
        """
        Kill the monitor specified
        """
        self.log('killing mon.{id}'.format(id=mon))
        self.manager.kill_mon(mon)

    def revive_mon(self, mon):
        """
        Revive the monitor specified
        """
        self.log('killing mon.{id}'.format(id=mon))
        self.log('reviving mon.{id}'.format(id=mon))
        self.manager.revive_mon(mon)

    def max_killable(self):
        """
        Return the maximum number of monitors we can kill.
        """
        m = len(_get_mons(self.ctx))
        if self.maintain_quorum:
            return max(math.ceil(m/2.0)-1, 0)
        else:
            return m

    def _wait_until_quorum(self, mon, size, timeout=300):
        """
        Wait until the monitor specified is in the quorum.
        """
        self.log('waiting for quorum size %d for mon %s' % (size, mon))
        s = {}

        with safe_while(sleep=3,
                        tries=timeout // 3,
                        action=f'wait for quorum size {size} on mon {mon}') as proceed:
            while proceed():
                s = self.manager.get_mon_status(mon)
                if len(s['quorum']) == size:
                   break
                self.log("quorum is size %d" % len(s['quorum']))

        self.log("final quorum is size %d" % len(s['quorum']))
        return s

    def do_thrash(self):
        """
        _do_thrash() wrapper.
        """
        try:
            self._do_thrash()
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # Allow successful completion so gevent doesn't see an exception.
            # The DaemonWatchdog will observe the error and tear down the test.

    def _do_thrash(self):
        """
        Continuously loop and thrash the monitors.
        """
        #status before mon thrashing
        if self.mds_failover:
            oldstatus = self.mds_cluster.status()

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
                try:
                    s = self._wait_until_quorum(m, len(mons), timeout=30)
                except Exception as e:
                    self.log('mon.{m} is not in quorum size, exception: {e}'.format(m=m,e=e))
                    self.log('mon_status: {s}'.format(s=s))
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
                    try:
                        s = self._wait_until_quorum(m, len(mons)-len(mons_to_kill), timeout=30)
                    except Exception as e:
                        self.log('mon.{m} is not in quorum size, exception: {e}'.format(m=m,e=e))
                        self.log('mon_status: {s}'.format(s=s))

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
                try:
                    s = self._wait_until_quorum(m, len(mons), timeout=30)
                except Exception as e:
                    self.log('mon.{m} is not in quorum size, exception: {e}'.format(m=m,e=e))
                    self.log('mon_status: {s}'.format(s=s))

                assert s['state'] == 'leader' or s['state'] == 'peon'
                assert len(s['quorum']) == len(mons)

            if self.scrub:
                self.log('triggering scrub')
                try:
                    self.manager.raw_cluster_cmd('mon', 'scrub')
                except Exception as e:
                    log.warning("Ignoring exception while triggering scrub: %s", e)

            if self.thrash_delay > 0.0:
                self.log('waiting for {delay} secs before continuing thrashing'.format(
                    delay=self.thrash_delay))
                time.sleep(self.thrash_delay)

        #status after thrashing
        if self.mds_failover:
            status = self.mds_cluster.status()
            assert not oldstatus.hadfailover(status), \
                'MDS Failover'


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

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    log.info('Beginning mon_thrash...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )
    thrash_proc = MonitorThrasher(ctx,
        manager, config, "MonitorThrasher",
        logger=log.getChild('mon_thrasher'))
    ctx.ceph[config['cluster']].thrashers.append(thrash_proc)
    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining mon_thrasher')
        thrash_proc.do_join()
        mons = _get_mons(ctx)
        manager.wait_for_mon_quorum_size(len(mons))
