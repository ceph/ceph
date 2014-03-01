"""
Lost_unfound
"""
import logging
import ceph_manager
from teuthology import misc as teuthology
from teuthology.task_util.rados import rados

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test handling of lost objects.

    A pretty rigid cluseter is brought up andtested by this task
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'lost_unfound task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    mon = teuthology.get_single_remote_value(ctx, first_mon)

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    while len(manager.get_osd_status()['up']) < 3:
        manager.sleep(10)
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_for_clean()

    # something that is always there
    dummyfile = '/etc/fstab'

    # take an osd out until the very end
    manager.kill_osd(2)
    manager.mark_down_osd(2)
    manager.mark_out_osd(2)

    # kludge to make sure they get a map
    rados(ctx, mon, ['-p', 'data', 'put', 'dummy', dummyfile])

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_recovery()

    # create old objects
    for f in range(1, 10):
        rados(ctx, mon, ['-p', 'data', 'put', 'existing_%d' % f, dummyfile])
        rados(ctx, mon, ['-p', 'data', 'put', 'existed_%d' % f, dummyfile])
        rados(ctx, mon, ['-p', 'data', 'rm', 'existed_%d' % f])

    # delay recovery, and make the pg log very long (to prevent backfill)
    manager.raw_cluster_cmd(
            'tell', 'osd.1',
            'injectargs',
            '--osd-recovery-delay-start 1000 --osd-min-pg-log-entries 100000000'
            )

    manager.kill_osd(0)
    manager.mark_down_osd(0)
    
    for f in range(1, 10):
        rados(ctx, mon, ['-p', 'data', 'put', 'new_%d' % f, dummyfile])
        rados(ctx, mon, ['-p', 'data', 'put', 'existed_%d' % f, dummyfile])
        rados(ctx, mon, ['-p', 'data', 'put', 'existing_%d' % f, dummyfile])

    # bring osd.0 back up, let it peer, but don't replicate the new
    # objects...
    log.info('osd.0 command_args is %s' % 'foo')
    log.info(ctx.daemons.get_daemon('osd', 0).command_args)
    ctx.daemons.get_daemon('osd', 0).command_kwargs['args'].extend([
            '--osd-recovery-delay-start', '1000'
            ])
    manager.revive_osd(0)
    manager.mark_in_osd(0)
    manager.wait_till_osd_is_up(0)

    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.wait_till_active()

    # take out osd.1 and the only copy of those objects.
    manager.kill_osd(1)
    manager.mark_down_osd(1)
    manager.mark_out_osd(1)
    manager.raw_cluster_cmd('osd', 'lost', '1', '--yes-i-really-mean-it')

    # bring up osd.2 so that things would otherwise, in theory, recovery fully
    manager.revive_osd(2)
    manager.mark_in_osd(2)
    manager.wait_till_osd_is_up(2)

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_till_active()
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')

    # verify that there are unfound objects
    unfound = manager.get_num_unfound_objects()
    log.info("there are %d unfound objects" % unfound)
    assert unfound

    # mark stuff lost
    pgs = manager.get_pg_stats()
    for pg in pgs:
        if pg['stat_sum']['num_objects_unfound'] > 0:
            primary = 'osd.%d' % pg['acting'][0]

            # verify that i can list them direct from the osd
            log.info('listing missing/lost in %s state %s', pg['pgid'],
                     pg['state']);
            m = manager.list_pg_missing(pg['pgid'])
            #log.info('%s' % m)
            assert m['num_unfound'] == pg['stat_sum']['num_objects_unfound']
            num_unfound=0
            for o in m['objects']:
                if len(o['locations']) == 0:
                    num_unfound += 1
            assert m['num_unfound'] == num_unfound

            log.info("reverting unfound in %s on %s", pg['pgid'], primary)
            manager.raw_cluster_cmd('pg', pg['pgid'],
                                    'mark_unfound_lost', 'revert')
        else:
            log.info("no unfound in %s", pg['pgid'])

    manager.raw_cluster_cmd('tell', 'osd.0', 'debug', 'kick_recovery_wq', '5')
    manager.raw_cluster_cmd('tell', 'osd.2', 'debug', 'kick_recovery_wq', '5')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_for_recovery()

    # verify result
    for f in range(1, 10):
        err = rados(ctx, mon, ['-p', 'data', 'get', 'new_%d' % f, '-'])
        assert err
        err = rados(ctx, mon, ['-p', 'data', 'get', 'existed_%d' % f, '-'])
        assert err
        err = rados(ctx, mon, ['-p', 'data', 'get', 'existing_%d' % f, '-'])
        assert not err

    # see if osd.1 can cope
    manager.revive_osd(1)
    manager.mark_in_osd(1)
    manager.wait_till_osd_is_up(1)
    manager.wait_for_clean()
