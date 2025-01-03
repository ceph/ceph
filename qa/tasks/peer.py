"""
Peer test (Single test, not much configurable here)
"""
import logging
import json
import time

from tasks import ceph_manager
from tasks.util.rados import rados
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test peering.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'peer task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    while len(manager.get_osd_status()['up']) < 3:
        time.sleep(10)
    manager.flush_pg_stats([0, 1, 2])
    manager.wait_for_clean()

    for i in range(3):
        manager.set_config(
            i,
            osd_recovery_delay_start=120)

    # take on osd down
    manager.kill_osd(2)
    manager.mark_down_osd(2)

    # kludge to make sure they get a map
    rados(ctx, mon, ['-p', 'data', 'get', 'dummy', '-'])

    manager.flush_pg_stats([0, 1])
    manager.wait_for_recovery()

    # kill another and revive 2, so that some pgs can't peer.
    manager.kill_osd(1)
    manager.mark_down_osd(1)
    manager.revive_osd(2)
    manager.wait_till_osd_is_up(2)

    manager.flush_pg_stats([0, 2])

    manager.wait_for_active_or_down()

    manager.flush_pg_stats([0, 2])

    # look for down pgs
    num_down_pgs = 0
    pgs = manager.get_pg_stats()
    for pg in pgs:
        out = manager.raw_cluster_cmd('pg', pg['pgid'], 'query')
        log.debug("out string %s",out)
        j = json.loads(out)
        log.info("pg is %s, query json is %s", pg, j)

        if pg['state'].count('down'):
            num_down_pgs += 1
            # verify that it is blocked on osd.1
            rs = j['recovery_state']
            assert len(rs) >= 2
            assert rs[0]['name'] == 'Started/Primary/Peering/Down'
            assert rs[1]['name'] == 'Started/Primary/Peering'
            assert rs[1]['blocked']
            assert rs[1]['down_osds_we_would_probe'] == [1]
            assert len(rs[1]['peering_blocked_by']) == 1
            assert rs[1]['peering_blocked_by'][0]['osd'] == 1

    assert num_down_pgs > 0

    # bring it all back
    manager.revive_osd(1)
    manager.wait_till_osd_is_up(1)
    manager.flush_pg_stats([0, 1, 2])
    manager.wait_for_clean()
