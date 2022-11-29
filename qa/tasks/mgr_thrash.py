"""
Manager thrash
"""
import logging
import contextlib
import random
import time
import gevent
from teuthology import misc as teuthology
from tasks import ceph_manager
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)


class ManagerThrasher(Thrasher):
    """
    How it works::

    - kill the active mgr
    - sleep for 'revive_delay' seconds
    - wait for new active
    - sleep for 'thrash_delay' seconds

    Options::

    seed                Seed to use on the RNG to reproduce a previous
                        behaviour (default: None; i.e., not set)
    revive_delay        Number of seconds to wait before reviving
                        the manager (default: 10)
    thrash_delay        Number of seconds to wait in-between
                        test iterations (default: 0)

    For example::

    tasks:
    - ceph:
    - mgr_thrash:
        revive_delay: 20
        thrash_delay: 1
        seed: 31337
    - ceph-fuse:
    - workunit:
        clients:
          all:
            - mon/workloadgen.sh
    """
    def __init__(self, ctx, manager, config, name, logger):
        super(ManagerThrasher, self).__init__()

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

        """ Manager thrashing """
        self.revive_delay = float(self.config.get('revive_delay', 10.0))
        self.thrash_delay = float(self.config.get('thrash_delay', 0.0))

        self.thread = gevent.spawn(self.do_thrash)

    def do_join(self):
        """
        Break out of this processes thrashing loop.
        """
        self.stopping = True
        self.thread.get()

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
        Continuously loop and thrash the manager.
        """
        while not self.stopping:
            self.manager.wait_for_mgr_available()
            self.manager.raw_cluster_cmd('mgr', 'fail')
            time.sleep(self.revive_delay)
            self.manager.wait_for_mgr_available()
            time.sleep(self.thrash_delay)


@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the manager by thrashing them while another task/workunit
    is running.

    Please refer to ManagerThrasher class for further information on the
    available options.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'mgr_thrash task only accepts a dict for configuration'

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    log.info('Beginning mgr_thrash...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    manager = ceph_manager.CephManager(mon, ctx=ctx,
                                       logger=log.getChild('ceph_manager'))
    thrash_proc = ManagerThrasher(ctx, manager, config, "ManagerThrasher",
                                  logger=log.getChild('mgr_thrasher'))
    ctx.ceph[config['cluster']].thrashers.append(thrash_proc)
    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining mgr_thrasher')
        thrash_proc.do_join()
