"""
Handle clock skews in monitors.
"""
import logging
import contextlib
import ceph_manager
import time
import gevent
from StringIO import StringIO
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

class ClockSkewCheck:
    """
    Check if there are any clock skews among the monitors in the
    quorum.

    This task accepts the following options:

    interval     amount of seconds to wait before check. (default: 30.0)
    expect-skew  'true' or 'false', to indicate whether to expect a skew during
                 the run or not. If 'true', the test will fail if no skew is
                 found, and succeed if a skew is indeed found; if 'false', it's
                 the other way around. (default: false)

    - mon_clock_skew_check:
        expect-skew: true
    """

    def __init__(self, ctx, manager, config, logger):
        self.ctx = ctx
        self.manager = manager

        self.stopping = False
        self.logger = logger
        self.config = config

        if self.config is None:
            self.config = dict()

        self.interval = float(self.config.get('interval', 30.0))
        self.expect_skew = self.config.get('expect-skew', False)

def task(ctx, config):
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'mon_clock_skew_check task only accepts a dict for configuration'
    log.info('Beginning mon_clock_skew_check...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    quorum_size = len(teuthology.get_mon_names(self.ctx))
    self.manager.wait_for_mon_quorum_size(quorum_size)

    # wait a bit
    log.info('sleeping for {s} seconds'.format(
        s=self.interval))
    time.sleep(self.interval)

    health = self.manager.get_mon_health(True)
    log.info('got health %s' % health)
    if self.expect_skew:
        if not health['checks']['MON_CLOCK_SKEW']:
            raise RuntimeError('expected MON_CLOCK_SKEW but got none')
    else:
        if health['checks']['MON_CLOCK_SKEW']:
            raise RuntimeError('got MON_CLOCK_SKEW but expected none')

