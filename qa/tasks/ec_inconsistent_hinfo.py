"""
Inconsistent_hinfo
"""
import logging
import time
from dateutil.parser import parse
from tasks import ceph_manager
from tasks.util.rados import rados
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def wait_for_deep_scrub_complete(manager, pgid, check_time_now, inconsistent):
    log.debug("waiting for pg %s deep-scrub complete (check_time_now=%s)" %
              (pgid, check_time_now))
    for i in range(300):
        time.sleep(5)
        manager.flush_pg_stats([0, 1, 2, 3])
        pgs = manager.get_pg_stats()
        pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
        log.debug('pg=%s' % pg);
        assert pg

        last_deep_scrub_time = parse(pg['last_deep_scrub_stamp']).strftime('%s')
        if last_deep_scrub_time < check_time_now:
            log.debug('not scrubbed')
            continue

        status = pg['state'].split('+')
        if inconsistent:
            assert 'inconsistent' in status
        else:
            assert 'inconsistent' not in status
        return

    assert False, 'not scrubbed'


def wait_for_backfilling_complete(manager, pgid, from_osd, to_osd):
    log.debug("waiting for pg %s backfill from osd.%s to osd.%s complete" %
              (pgid, from_osd, to_osd))
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
        if 'backfilling' in status:
            assert from_osd in pg['acting'] and to_osd in pg['up']
            log.debug('backfilling')
            continue
        if to_osd not in pg['up']:
            log.debug('backfill not started yet')
            continue
        log.debug('backfilled!')
        break

def task(ctx, config):
    """
    Test handling of objects with inconsistent hash info during backfill and deep-scrub.

    A pretty rigid cluster is brought up and tested by this task
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'ec_inconsistent_hinfo task only accepts a dict for configuration'
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
    profile_name = profile.get('name', 'backfill_unfound')
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
    log.info("acting=%s" % acting)
    assert acting
    primary = acting[0]

    # something that is always there, readable and never empty
    dummyfile = '/etc/group'

    # kludge to make sure they get a map
    rados(ctx, mon, ['-p', pool, 'put', 'dummy', dummyfile])

    manager.flush_pg_stats([0, 1])
    manager.wait_for_recovery()

    log.debug("create test object")
    obj = 'test'
    rados(ctx, mon, ['-p', pool, 'put', obj, dummyfile])

    victim = acting[1]

    log.info("remove test object hash info from osd.%s shard and test deep-scrub and repair"
             % victim)

    manager.objectstore_tool(pool, options='', args='rm-attr hinfo_key',
                             object_name=obj, osd=victim)
    check_time_now = time.strftime('%s')
    manager.raw_cluster_cmd('pg', 'deep-scrub', pgid)
    wait_for_deep_scrub_complete(manager, pgid, check_time_now, True)

    check_time_now = time.strftime('%s')
    manager.raw_cluster_cmd('pg', 'repair', pgid)
    wait_for_deep_scrub_complete(manager, pgid, check_time_now, False)

    log.info("remove test object hash info from primary osd.%s shard and test backfill"
             % primary)

    log.debug("write some data")
    rados(ctx, mon, ['-p', pool, 'bench', '30', 'write', '-b', '4096',
                     '--no-cleanup'])

    manager.objectstore_tool(pool, options='', args='rm-attr hinfo_key',
                             object_name=obj, osd=primary)

    # mark the osd out to trigger a rebalance/backfill
    source = acting[1]
    target = [x for x in [0, 1, 2, 3] if x not in acting][0]
    manager.mark_out_osd(source)

    # wait for everything to peer, backfill and recover
    wait_for_backfilling_complete(manager, pgid, source, target)
    manager.wait_for_clean()

    manager.flush_pg_stats([0, 1, 2, 3])
    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.debug('pg=%s' % pg)
    assert pg
    assert 'clean' in pg['state'].split('+')
    assert 'inconsistent' not in pg['state'].split('+')
    unfound = manager.get_num_unfound_objects()
    log.debug("there are %d unfound objects" % unfound)
    assert unfound == 0

    source, target = target, source
    log.info("remove test object hash info from non-primary osd.%s shard and test backfill"
             % source)

    manager.objectstore_tool(pool, options='', args='rm-attr hinfo_key',
                             object_name=obj, osd=source)

    # mark the osd in to trigger a rebalance/backfill
    manager.mark_in_osd(target)

    # wait for everything to peer, backfill and recover
    wait_for_backfilling_complete(manager, pgid, source, target)
    manager.wait_for_clean()

    manager.flush_pg_stats([0, 1, 2, 3])
    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.debug('pg=%s' % pg)
    assert pg
    assert 'clean' in pg['state'].split('+')
    assert 'inconsistent' not in pg['state'].split('+')
    unfound = manager.get_num_unfound_objects()
    log.debug("there are %d unfound objects" % unfound)
    assert unfound == 0

    log.info("remove hash info from two shards and test backfill")

    source = acting[2]
    target = [x for x in [0, 1, 2, 3] if x not in acting][0]
    manager.objectstore_tool(pool, options='', args='rm-attr hinfo_key',
                             object_name=obj, osd=primary)
    manager.objectstore_tool(pool, options='', args='rm-attr hinfo_key',
                             object_name=obj, osd=source)

    # mark the osd out to trigger a rebalance/backfill
    manager.mark_out_osd(source)

    # wait for everything to peer, backfill and detect unfound object
    wait_for_backfilling_complete(manager, pgid, source, target)

    # verify that there is unfound object
    manager.flush_pg_stats([0, 1, 2, 3])
    pgs = manager.get_pg_stats()
    pg = next((pg for pg in pgs if pg['pgid'] == pgid), None)
    log.debug('pg=%s' % pg)
    assert pg
    assert 'backfill_unfound' in pg['state'].split('+')
    unfound = manager.get_num_unfound_objects()
    log.debug("there are %d unfound objects" % unfound)
    assert unfound == 1
    m = manager.list_pg_unfound(pgid)
    log.debug('list_pg_unfound=%s' % m)
    assert m['num_unfound'] == pg['stat_sum']['num_objects_unfound']

    # mark stuff lost
    pgs = manager.get_pg_stats()
    manager.raw_cluster_cmd('pg', pgid, 'mark_unfound_lost', 'delete')

    # wait for everything to peer and be happy...
    manager.flush_pg_stats([0, 1, 2, 3])
    manager.wait_for_recovery()
