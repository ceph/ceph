"""
Monitor recovery
"""
import logging
import ceph_manager
from teuthology import misc as teuthology


log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test monitor recovery.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    mons = [f.split('.')[1] for f in teuthology.get_mon_names(ctx)]
    log.info("mon ids = %s" % mons)

    manager.wait_for_mon_quorum_size(len(mons))

    log.info('verifying all monitors are in the quorum')
    for m in mons:
        s = manager.get_mon_status(m)
        assert s['state'] == 'leader' or s['state'] == 'peon'
        assert len(s['quorum']) == len(mons)

    log.info('restarting each monitor in turn')
    for m in mons:
        # stop a monitor
        manager.kill_mon(m)
        manager.wait_for_mon_quorum_size(len(mons) - 1)

        # restart
        manager.revive_mon(m)
        manager.wait_for_mon_quorum_size(len(mons))

    # in forward and reverse order,
    rmons = mons
    rmons.reverse()
    for mons in mons, rmons:
        log.info('stopping all monitors')
        for m in mons:
            manager.kill_mon(m)

        log.info('forming a minimal quorum for %s, then adding monitors' % mons)
        qnum = (len(mons) / 2) + 1
        num = 0
        for m in mons:
            manager.revive_mon(m)
            num += 1
            if num >= qnum:
                manager.wait_for_mon_quorum_size(num)

    # on both leader and non-leader ranks...
    for rank in [0, 1]:
        # take one out
        log.info('removing mon %s' % mons[rank])
        manager.kill_mon(mons[rank])
        manager.wait_for_mon_quorum_size(len(mons) - 1)

        log.info('causing some monitor log activity')
        m = 30
        for n in range(1, m):
            manager.raw_cluster_cmd('log', '%d of %d' % (n, m))

        log.info('adding mon %s back in' % mons[rank])
        manager.revive_mon(mons[rank])
        manager.wait_for_mon_quorum_size(len(mons))
