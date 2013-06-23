import logging
import ceph_manager
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def rados(testdir, remote, cmd):
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        '{tdir}/adjust-ulimits'.format(tdir=testdir),
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'rados',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        check_status=False
        )
    return proc.exitstatus

def task(ctx, config):
    """
    Test handling of object location going down
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

    while len(manager.get_osd_status()['up']) < 3:
        manager.sleep(10)
    manager.wait_for_clean()

    # something that is always there
    dummyfile = '/etc/fstab'

    # take 0, 1 out
    manager.mark_out_osd(0)
    manager.mark_out_osd(1)
    manager.wait_for_clean()

    # delay recovery, and make the pg log very long (to prevent backfill)
    manager.raw_cluster_cmd(
            'tell', 'osd.0',
            'injectargs',
            '--osd-recovery-delay-start 10000 --osd-min-pg-log-entries 100000000'
            )
    # delay recovery, and make the pg log very long (to prevent backfill)
    manager.raw_cluster_cmd(
            'tell', 'osd.1',
            'injectargs',
            '--osd-recovery-delay-start 10000 --osd-min-pg-log-entries 100000000'
            )
    # delay recovery, and make the pg log very long (to prevent backfill)
    manager.raw_cluster_cmd(
            'tell', 'osd.2',
            'injectargs',
            '--osd-recovery-delay-start 10000 --osd-min-pg-log-entries 100000000'
            )
    # delay recovery, and make the pg log very long (to prevent backfill)
    manager.raw_cluster_cmd(
            'tell', 'osd.3',
            'injectargs',
            '--osd-recovery-delay-start 10000 --osd-min-pg-log-entries 100000000'
            )

    testdir = teuthology.get_testdir(ctx)

    # kludge to make sure they get a map
    rados(testdir, mon, ['-p', 'data', 'put', 'dummy', dummyfile])

    # create old objects
    for f in range(1, 10):
        rados(testdir, mon, ['-p', 'data', 'put', 'existing_%d' % f, dummyfile])

    manager.mark_out_osd(3)
    manager.wait_till_active()

    manager.mark_in_osd(0)
    manager.wait_till_active()

    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')

    manager.mark_out_osd(2)
    manager.wait_till_active()

    # bring up 1
    manager.mark_in_osd(1)
    manager.wait_till_active()

    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    log.info("Getting unfound objects")
    unfound = manager.get_num_unfound_objects()
    assert not unfound

    manager.kill_osd(2)
    manager.mark_down_osd(2)
    manager.kill_osd(3)
    manager.mark_down_osd(3)

    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    log.info("Getting unfound objects")
    unfound = manager.get_num_unfound_objects()
    assert unfound
