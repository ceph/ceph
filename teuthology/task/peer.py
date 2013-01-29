import logging
import ceph_manager
import json
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

def normalize_state(r):
    r = r.replace('+scrubbing', '')
    r = r.replace('+deep', '')
    return r

def task(ctx, config):
    """
    Test peering.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'peer task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

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

    # take on osd down
    manager.kill_osd(2)
    manager.mark_down_osd(2)

    # kludge to make sure they get a map
    rados(mon, ['-p', 'data', 'get', 'dummy', '-'])

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.wait_for_recovery()

    # kill another and revive 2, so that some pgs can't peer.
    manager.kill_osd(1)
    manager.mark_down_osd(1)
    manager.revive_osd(2)
    manager.wait_till_osd_is_up(2)

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')

    manager.wait_for_active_or_down()

    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')

    # look for down pgs
    num_down_pgs = 0
    pgs = manager.get_pg_stats()
    for pg in pgs:
        out = manager.raw_cluster_cmd('pg', pg['pgid'], 'query')
	log.debug("out string %s",out)
        j = json.loads('\n'.join(out.split('\n')[1:]))
        log.info("pg is %s, query json is %s", pg, j)
        assert normalize_state(j['state']) == normalize_state(pg['state'])

        if pg['state'].count('down'):
            num_down_pgs += 1
            # verify that it is blocked on osd.1
            rs = j['recovery_state']
            assert len(rs) > 0
            assert rs[0]['name'] == 'Started/Primary/Peering/GetInfo'
            assert rs[1]['name'] == 'Started/Primary/Peering'
            assert rs[1]['blocked']
            assert rs[1]['down_osds_we_would_probe'] == [1]
            assert len(rs[1]['peering_blocked_by']) == 1
            assert rs[1]['peering_blocked_by'][0]['osd'] == 1

    assert num_down_pgs > 0

    # bring it all back
    manager.revive_osd(1)
    manager.wait_till_osd_is_up(1)
    manager.raw_cluster_cmd('tell', 'osd.0', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.1', 'flush_pg_stats')
    manager.raw_cluster_cmd('tell', 'osd.2', 'flush_pg_stats')
    manager.wait_for_clean()
