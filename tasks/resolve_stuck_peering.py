"""
Resolve stuck peering
"""
import logging
import time

from teuthology import misc as teuthology
from util.rados import rados

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test handling resolve stuck peering

    requires 3 osds on a single test node
    """
    if config is None:
        config = {}
        assert isinstance(config, dict), \
            'Resolve stuck peering only accepts a dict for config'

    manager = ctx.managers['ceph']

    while len(manager.get_osd_status()['up']) < 3:
        time.sleep(10)


    manager.wait_for_clean()

    dummyfile = '/etc/fstab'
    dummyfile1 = '/etc/resolv.conf'

    #create 1 PG pool
    pool='foo'
    log.info('creating pool foo')
    manager.raw_cluster_cmd('osd', 'pool', 'create', '%s' % pool, '1')

    #set min_size of the pool to 1
    #so that we can continue with I/O
    #when 2 osds are down
    manager.set_pool_property(pool, "min_size", 1)

    osds = [0, 1, 2]

    primary = manager.get_pg_primary('foo', 0)
    log.info("primary osd is %d", primary)

    others = list(osds)
    others.remove(primary)

    log.info('writing initial objects')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    #create few objects
    for i in range(100):
        rados(ctx, mon, ['-p', 'foo', 'put', 'existing_%d' % i, dummyfile])

    manager.wait_for_clean()

    #kill other osds except primary
    log.info('killing other osds except primary')
    for i in others:
        manager.kill_osd(i)
    for i in others:
        manager.mark_down_osd(i)


    for i in range(100):
        rados(ctx, mon, ['-p', 'foo', 'put', 'new_%d' % i, dummyfile1])

    #kill primary osd
    manager.kill_osd(primary)
    manager.mark_down_osd(primary)

    #revive other 2 osds
    for i in others:
        manager.revive_osd(i)

    #make sure that pg is down
    #Assuming pg number for single pg pool will start from 0
    pgnum=0
    pgstr = manager.get_pgid(pool, pgnum)
    stats = manager.get_single_pg_stats(pgstr)
    print stats['state']

    timeout=60
    start=time.time()

    while 'down' not in stats['state']:
        assert time.time() - start < timeout, \
            'failed to reach down state before timeout expired'

    #mark primary as lost
    manager.raw_cluster_cmd('osd', 'lost', '%d' % primary,\
                            '--yes-i-really-mean-it')


    #expect the pg status to be active+undersized+degraded
    #pg should recover and become active+clean within timeout
    stats = manager.get_single_pg_stats(pgstr)
    print stats['state']

    timeout=10
    start=time.time()

    while manager.get_num_down():
        assert time.time() - start < timeout, \
            'failed to recover before timeout expired'
