import logging
import ceph_manager
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def rados(remote, cmd):
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
        '/tmp/cephtest/enable-coredump',
        '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
        '/tmp/cephtest/archive/coverage',
        '/tmp/cephtest/binary/usr/local/bin/rados',
        '-c', '/tmp/cephtest/ceph.conf',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        check_status=False
        )
    return proc.exitstatus

def task(ctx, config):
    """
    Test handling of lost objects.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'lost_unfound task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    while manager.get_osd_status()['up'] < 3:
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
    rados(mon, ['-p', 'data', 'put', 'dummy', dummyfile])

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_recovery()

    # create old objects
    for f in range(1, 10):
        rados(mon, ['-p', 'data', 'put', 'existing_%d' % f, dummyfile])
        rados(mon, ['-p', 'data', 'put', 'existed_%d' % f, dummyfile])
        rados(mon, ['-p', 'data', 'rm', 'existed_%d' % f])

    # delay recovery, and make the pg log very long (to prevent backfill)
    manager.raw_cluster_cmd(
            'tell', 'osd.1',
            'injectargs',
            '--osd-recovery-delay-start 1000 --osd-min-pg-log-entries 100000000'
            )

    manager.kill_osd(0)
    manager.mark_down_osd(0)
    
    for f in range(1, 10):
        rados(mon, ['-p', 'data', 'put', 'new_%d' % f, dummyfile])
        rados(mon, ['-p', 'data', 'put', 'existed_%d' % f, dummyfile])
        rados(mon, ['-p', 'data', 'put', 'existing_%d' % f, dummyfile])

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
        err = rados(mon, ['-p', 'data', 'get', 'new_%d' % f, '-'])
        assert err
        err = rados(mon, ['-p', 'data', 'get', 'existed_%d' % f, '-'])
        assert err
        err = rados(mon, ['-p', 'data', 'get', 'existing_%d' % f, '-'])
        assert not err

    # see if osd.1 can cope
    manager.revive_osd(1)
    manager.mark_in_osd(1)
    manager.wait_till_osd_is_up(1)
    manager.wait_for_clean()
