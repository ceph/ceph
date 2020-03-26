"""
Dump_stuck command
"""
import logging
import re
import time

import ceph_manager
from teuthology import misc as teuthology


log = logging.getLogger(__name__)

def check_stuck(manager, num_inactive, num_unclean, num_stale, timeout=10):
    """
    Do checks.  Make sure get_stuck_pgs return the right amout of information, then
    extract health information from the raw_cluster_cmd and compare the results with
    values passed in.  This passes if all asserts pass.
 
    :param num_manager: Ceph manager
    :param num_inactive: number of inaactive pages that are stuck
    :param num_unclean: number of unclean pages that are stuck
    :paran num_stale: number of stale pages that are stuck
    :param timeout: timeout value for get_stuck_pgs calls
    """
    inactive = manager.get_stuck_pgs('inactive', timeout)
    unclean = manager.get_stuck_pgs('unclean', timeout)
    stale = manager.get_stuck_pgs('stale', timeout)
    log.info('inactive %s / %d,  unclean %s / %d,  stale %s / %d',
             len(inactive), num_inactive,
             len(unclean), num_unclean,
             len(stale), num_stale)
    assert len(inactive) == num_inactive
    assert len(unclean) == num_unclean
    assert len(stale) == num_stale

def task(ctx, config):
    """
    Test the dump_stuck command.

    :param ctx: Context
    :param config: Configuration
    """
    assert config is None, \
        'dump_stuck requires no configuration'
    assert teuthology.num_instances_of_type(ctx.cluster, 'osd') == 2, \
        'dump_stuck requires exactly 2 osds'

    timeout = 60
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    manager.flush_pg_stats([0, 1])
    manager.wait_for_clean(timeout)

    manager.raw_cluster_cmd('tell', 'mon.0', 'injectargs', '--',
#                            '--mon-osd-report-timeout 90',
                            '--mon-pg-stuck-threshold 10')

    # all active+clean
    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=0,
        num_stale=0,
        )
    num_pgs = manager.get_num_pgs()

    manager.mark_out_osd(0)
    time.sleep(timeout)
    manager.flush_pg_stats([1])
    manager.wait_for_recovery(timeout)

    # all active+clean+remapped
    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=0,
        num_stale=0,
        )

    manager.mark_in_osd(0)
    manager.flush_pg_stats([0, 1])
    manager.wait_for_clean(timeout)

    # all active+clean
    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=0,
        num_stale=0,
        )

    log.info('stopping first osd')
    manager.kill_osd(0)
    manager.mark_down_osd(0)
    manager.wait_for_active(timeout)

    log.info('waiting for all to be unclean')
    starttime = time.time()
    done = False
    while not done:
        try:
            check_stuck(
                manager,
                num_inactive=0,
                num_unclean=num_pgs,
                num_stale=0,
                )
            done = True
        except AssertionError:
            # wait up to 15 minutes to become stale
            if time.time() - starttime > 900:
                raise


    log.info('stopping second osd')
    manager.kill_osd(1)
    manager.mark_down_osd(1)

    log.info('waiting for all to be stale')
    starttime = time.time()
    done = False
    while not done:
        try:
            check_stuck(
                manager,
                num_inactive=0,
                num_unclean=num_pgs,
                num_stale=num_pgs,
                )
            done = True
        except AssertionError:
            # wait up to 15 minutes to become stale
            if time.time() - starttime > 900:
                raise

    log.info('reviving')
    for id_ in teuthology.all_roles_of_type(ctx.cluster, 'osd'):
        manager.revive_osd(id_)
        manager.mark_in_osd(id_)
    while True:
        try:
            manager.flush_pg_stats([0, 1])
            break
        except Exception:
            log.exception('osds must not be started yet, waiting...')
            time.sleep(1)
    manager.wait_for_clean(timeout)

    check_stuck(
        manager,
        num_inactive=0,
        num_unclean=0,
        num_stale=0,
        )
