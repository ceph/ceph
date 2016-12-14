"""
Scrub osds
"""
import contextlib
import gevent
import logging
import random
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run scrub periodically. Randomly chooses an OSD to scrub.

    The config should be as follows:

    scrub:
        frequency: <seconds between scrubs>
        deep: <bool for deepness>

    example:

    tasks:
    - ceph:
    - scrub:
        frequency: 30
        deep: 0
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'scrub task only accepts a dict for configuration'

    log.info('Beginning scrub...')

    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    while len(manager.get_osd_status()['up']) < num_osds:
        time.sleep(10)

    scrub_proc = Scrubber(
        manager,
        config,
        )
    try:
        yield
    finally:
        log.info('joining scrub')
        scrub_proc.do_join()

class Scrubber:
    """
    Scrubbing is actually performed during initialzation
    """
    def __init__(self, manager, config):
        """
        Spawn scrubbing thread upon completion.
        """
        self.ceph_manager = manager
        self.ceph_manager.wait_for_clean()

        osd_status = self.ceph_manager.get_osd_status()
        self.osds = osd_status['up']

        self.config = config
        if self.config is None:
            self.config = dict()

        else:
            def tmp(x):
                """Local display"""
                print x
            self.log = tmp

        self.stopping = False

        log.info("spawning thread")

        self.thread = gevent.spawn(self.do_scrub)

    def do_join(self):
        """Scrubbing thread finished"""
        self.stopping = True
        self.thread.get()

    def do_scrub(self):
        """Perform the scrub operation"""
        frequency = self.config.get("frequency", 30)
        deep = self.config.get("deep", 0)

        log.info("stopping %s" % self.stopping)

        while not self.stopping:
            osd = str(random.choice(self.osds))

            if deep:
                cmd = 'deep-scrub'
            else:
                cmd = 'scrub'

            log.info('%sbing %s' % (cmd, osd))
            self.ceph_manager.raw_cluster_cmd('osd', cmd, osd)

            time.sleep(frequency)
