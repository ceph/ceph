import logging
import ceph_manager
import time
from teuthology import misc as teuthology


log = logging.getLogger(__name__)


def rados_start(remote, cmd):
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
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    
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
    p = rados_start(mon, ['-p', 'rbd', 'bench', '60', 'write', '-b', '4096'])

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
    p = rados_start(mon, ['-p', 'rbd', 'bench', '15', 'write', '-b', '4096'])
    err = p.exitstatus.get();

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


