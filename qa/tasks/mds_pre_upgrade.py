"""
Prepare MDS cluster for upgrade.
"""

import logging
import time

from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Prepare MDS cluster for upgrade.

    This task reduces ranks to 1 and stops all standbys.
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'snap-upgrade task only accepts a dict for configuration'

    fs = Filesystem(ctx)
    status = fs.getinfo()

    fs.set_max_mds(1)
    status = fs.getinfo()
    targets = filter(lambda r: r['rank'] >= 1, fs.get_ranks(status=status))
    if len(targets) > 0:
        # deactivate mds in decending order
        targets = sorted(targets, key=lambda r: r['rank'], reverse=True)
        for target in targets:
            self.log("deactivating rank %d" % target['rank'])
            self.fs.deactivate(target['rank'])
            status = self.wait_for_stable()[0]
        else:
            status = self.wait_for_stable()[0]

    assert(fs.get_mds_map(status=status)['max_mds'] == 1)
    assert(fs.get_mds_map(status=status)['in'] == [0])

    # Stop standbys now to minimize time rank 0 is down in subsequent:
    # tasks:
    # - ceph.stop: [mds.*]
    rank0 = fs.get_rank(rank=0, status=status)
    for daemon in ctx.daemons.iter_daemons_of_role('mds', fs.mon_manager.cluster):
        if rank0['name'] != daemon.id_:
            daemon.stop()

    for i in range(1, 10):
        time.sleep(5) # time for FSMap to update
        status = fs.getinfo()
        if len(list(status.get_standbys())) == 0:
            break
    assert(len(list(status.get_standbys())) == 0)
