import logging
import re
import time

import ceph_manager
from teuthology import misc as teuthology


log = logging.getLogger(__name__)

def check_stuck(manager, num_inactive, num_unclean, num_stale, timeout=10):
    inactive = manager.get_stuck_pgs('inactive', timeout)
    assert len(inactive) == num_inactive
    unclean = manager.get_stuck_pgs('unclean', timeout)
    assert len(unclean) == num_unclean
    stale = manager.get_stuck_pgs('stale', timeout)
    assert len(stale) == num_stale

    # check health output as well
    health = manager.raw_cluster_cmd('health')
    log.debug('ceph health is: %s', health)
    if num_inactive > 0:
        m = re.search('(\d+) pgs stuck inactive', health)
        assert int(m.group(1)) == num_inactive
    if num_unclean > 0:
        m = re.search('(\d+) pgs stuck unclean', health)
        assert int(m.group(1)) == num_unclean
    if num_stale > 0:
        m = re.search('(\d+) pgs stuck stale', health)
        assert int(m.group(1)) == num_stale

def task(ctx, config):
    """
    Test the dump_stuck command.

    """
    assert config is None, \
        'dump_stuck requires no configuration'
    assert teuthology.num_instances_of_type(ctx.cluster, 'osd') == 2, \
        'dump_stuck requires exactly 2 osds'

    timeout = 60
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_clean(timeout)

    manager.raw_cluster_cmd('tell', 'mon.0', 'injectargs', '--',
#                            '--mon-osd-report-timeout 90',
                            '--mon-pg-stuck-threshold 10')

    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=0,
        num_stale=0,
        )
    num_pgs = manager.get_num_pgs()

    manager.mark_out_osd(0)
    time.sleep(timeout)
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_recovery(timeout)

    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=num_pgs,
        num_stale=0,
        )

    manager.mark_in_osd(0)
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_clean(timeout)

    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=0,
        num_stale=0,
        )

    for id_ in teuthology.all_roles_of_type(ctx.cluster, 'osd'):
        manager.kill_osd(id_)
        manager.mark_down_osd(id_)

    starttime = time.time()
    done = False
    while not done:
        try:
            check_stuck(
                manager,
                num_inactive=0,
                num_unclean=0,
                num_stale=num_pgs,
                )
            done = True
        except AssertionError:
            # wait up to 15 minutes to become stale
            if time.time() - starttime > 900:
                raise

    for id_ in teuthology.all_roles_of_type(ctx.cluster, 'osd'):
        manager.revive_osd(id_)
        manager.mark_in_osd(id_)
    while True:
        try:
            manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
            manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
            break
        except:
            log.debug('osds must not be started yet, waiting...')
            time.sleep(1)
    manager.wait_for_clean(timeout)

    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=0,
        num_stale=0,
        )
