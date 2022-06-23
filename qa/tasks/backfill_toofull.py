"""
Backfill_toofull
"""
import logging
import time
from tasks import ceph_manager
from tasks.util.rados import rados
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def wait_for_pg_state(manager, pgid, state, to_osd):
    log.debug("waiting for pg %s state is %s" % (pgid, state))
    for i in range(300):
        time.sleep(5)
        manager.flush_pg_stats([0, 1, 2, 3])
        pgs = manager.get_pg_stats()
        pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
        log.info('pg=%s' % pg);
        assert pg
        status = pg['state'].split('+')
        if 'active' not in status:
            log.debug('not active')
            continue
        if state not in status:
            log.debug('not %s' % state)
            continue
        assert to_osd in pg['up']
        return
    assert False, '%s not in %s' % (pgid, state)


def task(ctx, config):
    """
    Test backfill reservation calculates "toofull" condition correctly.

    A pretty rigid cluster is brought up and tested by this task
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'backfill_toofull task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    profile = config.get('erasure_code_profile', {
        'k': '2',
        'm': '1',
        'crush-failure-domain': 'osd'
    })
    profile_name = profile.get('name', 'backfill_toofull')
    manager.create_erasure_code_profile(profile_name, profile)
    pool = manager.create_pool_with_unique_name(
        pg_num=1,
        erasure_code_profile_name=profile_name,
        min_size=2)
    manager.raw_cluster_cmd('osd', 'pool', 'set', pool,
                            'pg_autoscale_mode', 'off')

    manager.flush_pg_stats([0, 1, 2, 3])
    manager.wait_for_clean()

    pool_id = manager.get_pool_num(pool)
    pgid = '%d.0' % pool_id
    pgs = manager.get_pg_stats()
    acting = next((pg['acting'] for pg in pgs if pg['pgid'] == pgid), None)
    log.debug("acting=%s" % acting)
    assert acting
    primary = acting[0]
    target = acting[1]

    log.debug("write some data")
    rados(ctx, mon, ['-p', pool, 'bench', '120', 'write', '--no-cleanup'])
    df = manager.get_osd_df(target)
    log.debug("target osd df: %s" % df)

    total_kb = df['kb']
    used_kb = df['kb_used']

    log.debug("pause recovery")
    manager.raw_cluster_cmd('osd', 'set', 'noout')
    manager.raw_cluster_cmd('osd', 'set', 'nobackfill')
    manager.raw_cluster_cmd('osd', 'set', 'norecover')

    log.debug("stop target osd %s" % target)
    manager.kill_osd(target)
    manager.wait_till_active()

    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.debug('pg=%s' % pg)
    assert pg

    log.debug("re-write data")
    rados(ctx, mon, ['-p', pool, 'cleanup'])
    time.sleep(10)
    rados(ctx, mon, ['-p', pool, 'bench', '60', 'write', '--no-cleanup'])

    df = manager.get_osd_df(primary)
    log.debug("primary osd df: %s" % df)

    primary_used_kb = df['kb_used']

    log.info("test backfill reservation rejected with toofull")

    # We set backfillfull ratio less than new data size and expect the pg
    # entering backfill_toofull state.
    #
    # We also need to update nearfull ratio to prevent "full ratio(s) out of order".

    backfillfull = 0.9 * primary_used_kb / total_kb
    nearfull = backfillfull * 0.9

    log.debug("update nearfull ratio to %s and backfillfull ratio to %s" %
              (nearfull, backfillfull))
    manager.raw_cluster_cmd('osd', 'set-nearfull-ratio',
                            '{:.3f}'.format(nearfull + 0.001))
    manager.raw_cluster_cmd('osd', 'set-backfillfull-ratio',
                            '{:.3f}'.format(backfillfull + 0.001))

    log.debug("start target osd %s" % target)

    manager.revive_osd(target)
    manager.wait_for_active()
    manager.wait_till_osd_is_up(target)

    wait_for_pg_state(manager, pgid, 'backfill_toofull', target)

    log.info("test pg not enter backfill_toofull after restarting backfill")

    # We want to set backfillfull ratio to be big enough for the target to
    # successfully backfill new data but smaller than the sum of old and new
    # data, so if the osd backfill reservation incorrectly calculates "toofull"
    # the test will detect this (fail).
    #
    # Note, we need to operate with "uncompressed" bytes because currently
    # osd backfill reservation does not take compression into account.
    #
    # We also need to update nearfull ratio to prevent "full ratio(s) out of order".

    pdf = manager.get_pool_df(pool)
    log.debug("pool %s df: %s" % (pool, pdf))
    assert pdf
    compress_ratio = 1.0 * pdf['compress_under_bytes'] / pdf['compress_bytes_used'] \
        if pdf['compress_bytes_used'] > 0 else 1.0
    log.debug("compress_ratio: %s" % compress_ratio)

    backfillfull = (used_kb + primary_used_kb) * compress_ratio / total_kb
    assert backfillfull < 0.9
    nearfull_min = max(used_kb, primary_used_kb) * compress_ratio / total_kb
    assert nearfull_min < backfillfull
    delta = backfillfull - nearfull_min
    nearfull = nearfull_min + delta * 0.1
    backfillfull = nearfull_min + delta * 0.2

    log.debug("update nearfull ratio to %s and backfillfull ratio to %s" %
              (nearfull, backfillfull))
    manager.raw_cluster_cmd('osd', 'set-nearfull-ratio',
                            '{:.3f}'.format(nearfull + 0.001))
    manager.raw_cluster_cmd('osd', 'set-backfillfull-ratio',
                            '{:.3f}'.format(backfillfull + 0.001))

    wait_for_pg_state(manager, pgid, 'backfilling', target)

    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.debug('pg=%s' % pg)
    assert pg

    log.debug("interrupt %s backfill" % target)
    manager.mark_down_osd(target)
    # after marking the target osd down it will automatically be
    # up soon again

    log.debug("resume recovery")
    manager.raw_cluster_cmd('osd', 'unset', 'noout')
    manager.raw_cluster_cmd('osd', 'unset', 'nobackfill')
    manager.raw_cluster_cmd('osd', 'unset', 'norecover')

    # wait for everything to peer, backfill and recover
    manager.wait_for_clean()

    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.info('pg=%s' % pg)
    assert pg
    assert 'clean' in pg['state'].split('+')
