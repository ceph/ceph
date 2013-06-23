import logging
import ceph_manager
from teuthology import misc as teuthology
import time


log = logging.getLogger(__name__)


def rados(testdir, remote, cmd, wait=True):
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
        check_status=False,
        wait=wait
        )
    if wait:
        return proc.exitstatus
    else:
        return proc

def task(ctx, config):
    """
    Test handling of divergent entries with prior_version
    prior to log_tail

    config: none

    Requires 3 osds.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'divergent_priors task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    testdir = teuthology.get_testdir(ctx)
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )
    ctx.manager = manager

    while len(manager.get_osd_status()['up']) < 3:
        time.sleep(10)
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.raw_cluster_cmd('osd', 'set', 'noout')
    manager.raw_cluster_cmd('osd', 'set', 'noin')
    manager.raw_cluster_cmd('osd', 'set', 'nodown')
    manager.wait_for_clean()

    # something that is always there
    dummyfile = '/etc/fstab'
    dummyfile2 = '/etc/resolv.conf'

    # create 1 pg pool
    log.info('creating foo')
    manager.raw_cluster_cmd('osd', 'pool', 'create', 'foo', '1')

    osds = [0, 1, 2]
    for i in osds:
        manager.set_config(i, osd_min_pg_log_entries=1)

    # determine primary
    divergent = manager.get_pg_primary('foo', 0)
    log.info("primary and soon to be divergent is %d", divergent)
    non_divergent = [0,1,2]
    non_divergent.remove(divergent)

    log.info('writing initial objects')
    # write 1000 objects
    for i in range(1000):
        rados(testdir, mon, ['-p', 'foo', 'put', 'existing_%d' % i, dummyfile])

    manager.wait_for_clean()

    # blackhole non_divergent
    log.info("blackholing osds %s", str(non_divergent))
    for i in non_divergent:
        manager.set_config(i, filestore_blackhole='')

    # write 1 (divergent) object
    log.info('writing divergent object existing_0')
    rados(
        testdir, mon, ['-p', 'foo', 'put', 'existing_0', dummyfile2],
        wait=False)
    time.sleep(10)
    mon.run(
        args=['killall', '-9', 'rados'],
        wait=True,
        check_status=False)

    # kill all the osds
    log.info('killing all the osds')
    for i in osds:
        manager.kill_osd(i)
    for i in osds:
        manager.mark_down_osd(i)
    for i in osds:
        manager.mark_out_osd(i)

    # bring up non-divergent
    log.info("bringing up non_divergent %s", str(non_divergent))
    for i in non_divergent:
        manager.revive_osd(i)
    for i in non_divergent:
        manager.mark_in_osd(i)

    log.info('making log long to prevent backfill')
    for i in non_divergent:
        manager.set_config(i, osd_min_pg_log_entries=100000)

    # write 1 non-divergent object (ensure that old divergent one is divergent)
    log.info('writing non-divergent object existing_1')
    rados(testdir, mon, ['-p', 'foo', 'put', 'existing_1', dummyfile2])

    manager.wait_for_recovery()

    # ensure no recovery
    log.info('delay recovery')
    for i in non_divergent:
        manager.set_config(i, osd_recovery_delay_start=100000)

    # bring in our divergent friend
    log.info("revive divergent %d", divergent)
    manager.revive_osd(divergent)

    while len(manager.get_osd_status()['up']) < 3:
        time.sleep(10)

    log.info('delay recovery divergent')
    manager.set_config(divergent, osd_recovery_delay_start=100000)
    log.info('mark divergent in')
    manager.mark_in_osd(divergent)

    log.info('wait for peering')
    rados(testdir, mon, ['-p', 'foo', 'put', 'foo', dummyfile])

    log.info("killing divergent %d", divergent)
    manager.kill_osd(divergent)
    log.info("reviving divergent %d", divergent)
    manager.revive_osd(divergent)

    log.info('allowing recovery')
    for i in non_divergent:
        manager.set_config(i, osd_recovery_delay_start=0)

    log.info('reading existing_0')
    exit_status = rados(testdir, mon,
                        ['-p', 'foo', 'get', 'existing_0',
                         '-o', '/tmp/existing'])
    assert exit_status is 0
    log.info("success")
