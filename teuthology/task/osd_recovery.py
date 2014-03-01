"""
osd recovery
"""
import logging
import ceph_manager
import time
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def rados_start(testdir, remote, cmd):
    """
    Run a remote rados command (currently used to only write data)
    """
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'rados',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        wait=False,
        )
    return proc

def task(ctx, config):
    """
    Test (non-backfill) recovery
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'task only accepts a dict for configuration'
    testdir = teuthology.get_testdir(ctx)
    first_mon = teuthology.get_first_mon(ctx, config)
    mon = teuthology.get_single_remote_value(ctx, first_mon)
    
    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    log.info('num_osds is %s' % num_osds)
    assert num_osds == 3        

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

    # test some osdmap flags
    manager.raw_cluster_cmd('osd', 'set', 'noin')
    manager.raw_cluster_cmd('osd', 'set', 'noout')
    manager.raw_cluster_cmd('osd', 'set', 'noup')
    manager.raw_cluster_cmd('osd', 'set', 'nodown')
    manager.raw_cluster_cmd('osd', 'unset', 'noin')
    manager.raw_cluster_cmd('osd', 'unset', 'noout')
    manager.raw_cluster_cmd('osd', 'unset', 'noup')
    manager.raw_cluster_cmd('osd', 'unset', 'nodown')

    # write some new data
    p = rados_start(testdir, mon, ['-p', 'rbd', 'bench', '60', 'write', '-b', '4096',
                          '--no-cleanup'])

    time.sleep(15)

    # trigger a divergent target:
    #  blackhole + restart osd.1 (shorter log)
    manager.blackhole_kill_osd(1)
    #  kill osd.2 (longer log... we'll make it divergent below)
    manager.kill_osd(2)
    time.sleep(2)
    manager.revive_osd(1)

    # wait for our writes to complete + succeed
    err = p.exitstatus.get()
    log.info('err is %d' % err)

    # cluster must repeer
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_active_or_down()

    # write some more (make sure osd.2 really is divergent)
    p = rados_start(testdir, mon, ['-p', 'rbd', 'bench', '15', 'write', '-b', '4096'])
    p.exitstatus.get();

    # revive divergent osd
    manager.revive_osd(2)

    while len(manager.get_osd_status()['up']) < 3:
        log.info('waiting a bit...')
        time.sleep(2)
    log.info('3 are up!')

    # cluster must recover
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_for_clean()


def test_incomplete_pgs(ctx, config):
    """
    Test handling of incomplete pgs.  Requires 4 osds.
    """
    testdir = teuthology.get_testdir(ctx)
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    mon = teuthology.get_single_remote_value(ctx, first_mon)

    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    log.info('num_osds is %s' % num_osds)
    assert num_osds == 4

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    while len(manager.get_osd_status()['up']) < 4:
        time.sleep(10)

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.3', 'flush_pg_stats')
    manager.wait_for_clean()

    log.info('Testing incomplete pgs...')

    for i in range(4):
        manager.set_config(
            i,
            osd_recovery_delay_start=1000)

    # move data off of osd.0, osd.1
    manager.raw_cluster_cmd('osd', 'out', '0', '1')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.3', 'flush_pg_stats')
    manager.wait_for_clean()

    # lots of objects in rbd (no pg log, will backfill)
    p = rados_start(testdir, mon,
                    ['-p', 'rbd', 'bench', '60', 'write', '-b', '1',
                     '--no-cleanup'])
    p.exitstatus.get()

    # few objects in metadata pool (with pg log, normal recovery)
    for f in range(1, 20):
        p = rados_start(testdir, mon, ['-p', 'metadata', 'put',
                              'foo.%d' % f, '/etc/passwd'])
        p.exitstatus.get()

    # move it back
    manager.raw_cluster_cmd('osd', 'in', '0', '1')
    manager.raw_cluster_cmd('osd', 'out', '2', '3')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.3', 'flush_pg_stats')
    manager.wait_for_active()

    assert not manager.is_clean()
    assert not manager.is_recovered()

    # kill 2 + 3
    log.info('stopping 2,3')
    manager.kill_osd(2)
    manager.kill_osd(3)
    log.info('...')
    manager.raw_cluster_cmd('osd', 'down', '2', '3')
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_active_or_down()

    assert manager.get_num_down() > 0

    # revive 2 + 3
    manager.revive_osd(2)
    manager.revive_osd(3)
    while len(manager.get_osd_status()['up']) < 4:
        log.info('waiting a bit...')
        time.sleep(2)
    log.info('all are up!')

    for i in range(4):
        manager.kick_recovery_wq(i)

    # cluster must recover
    manager.wait_for_clean()
