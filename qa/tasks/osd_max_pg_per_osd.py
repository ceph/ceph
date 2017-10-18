import logging
import random


log = logging.getLogger(__name__)


def pg_num_in_all_states(pgs, *states):
    return sum(1 for state in pgs.itervalues()
               if all(s in state for s in states))


def pg_num_in_any_state(pgs, *states):
    return sum(1 for state in pgs.itervalues()
               if any(s in state for s in states))


def test_create_from_mon(ctx, config):
    """
    osd should stop creating new pools if the number of pg it servers
    exceeds the max-pg-per-osd setting, and it should resume the previously
    suspended pg creations once the its pg number drops down below the setting
    How it works::
    1. set the hard limit of pg-per-osd to "2"
    2. create pool.a with pg_num=2
       # all pgs should be active+clean
    2. create pool.b with pg_num=2
       # new pgs belonging to this pool should be unknown (the primary osd
       reaches the limit) or creating (replica osd reaches the limit)
    3. remove pool.a
    4. all pg belonging to pool.b should be active+clean
    """
    pg_num = config.get('pg_num', 2)
    manager = ctx.managers['ceph']
    log.info('1. creating pool.a')
    pool_a = manager.create_pool_with_unique_name(pg_num)
    manager.wait_for_clean()
    assert manager.get_num_active_clean() == pg_num

    log.info('2. creating pool.b')
    pool_b = manager.create_pool_with_unique_name(pg_num)
    pg_states = manager.wait_till_pg_convergence(300)
    pg_created = pg_num_in_all_states(pg_states, 'active', 'clean')
    assert pg_created == pg_num
    pg_pending = pg_num_in_any_state(pg_states, 'unknown', 'creating')
    assert pg_pending == pg_num

    log.info('3. removing pool.a')
    manager.remove_pool(pool_a)
    pg_states = manager.wait_till_pg_convergence(300)
    assert len(pg_states) == pg_num
    pg_created = pg_num_in_all_states(pg_states, 'active', 'clean')
    assert pg_created == pg_num

    # cleanup
    manager.remove_pool(pool_b)


def test_create_from_peer(ctx, config):
    """
    osd should stop creating new pools if the number of pg it servers
    exceeds the max-pg-per-osd setting, and it should resume the previously
    suspended pg creations once the its pg number drops down below the setting

    How it works::
    0. create 4 OSDs.
    1. create pool.a with pg_num=1, size=2
       pg will be mapped to osd.0, and osd.1, and it should be active+clean
    2. create pool.b with pg_num=1, size=2.
       if the pgs stuck in creating, delete the pool since the pool and try
       again, eventually we'll get the pool to land on the other 2 osds that
       aren't occupied by pool.a. (this will also verify that pgs for deleted
       pools get cleaned out of the creating wait list.)
    3. mark an osd out. verify that some pgs get stuck stale or peering.
    4. delete a pool, verify pgs go active.
    """
    pg_num = config.get('pg_num', 1)
    pool_size = config.get('pool_size', 2)
    from_primary = config.get('from_primary', True)

    manager = ctx.managers['ceph']
    log.info('1. creating pool.a')
    pool_a = manager.create_pool_with_unique_name(pg_num)
    manager.wait_for_clean()
    assert manager.get_num_active_clean() == pg_num

    log.info('2. creating pool.b')
    while True:
        pool_b = manager.create_pool_with_unique_name(pg_num)
        pg_states = manager.wait_till_pg_convergence(300)
        pg_created = pg_num_in_all_states(pg_states, 'active', 'clean')
        assert pg_created >= pg_num
        pg_pending = pg_num_in_any_state(pg_states, 'unknown', 'creating')
        assert pg_pending == pg_num * 2 - pg_created
        if pg_created == pg_num * 2:
            break
        manager.remove_pool(pool_b)

    log.info('3. mark an osd out')
    pg_stats = manager.get_pg_stats()
    pg = random.choice(pg_stats)
    if from_primary:
        victim = pg['acting'][-1]
    else:
        victim = pg['acting'][0]
    manager.mark_out_osd(victim)
    pg_states = manager.wait_till_pg_convergence(300)
    pg_stuck = pg_num_in_any_state(pg_states, 'activating', 'stale', 'peering')
    assert pg_stuck > 0

    log.info('4. removing pool.b')
    manager.remove_pool(pool_b)
    manager.wait_for_clean(30)

    # cleanup
    manager.remove_pool(pool_a)


def task(ctx, config):
    assert isinstance(config, dict), \
        'osd_max_pg_per_osd task only accepts a dict for config'
    manager = ctx.managers['ceph']
    if config.get('test_create_from_mon', True):
        test_create_from_mon(ctx, config)
    else:
        test_create_from_peer(ctx, config)
