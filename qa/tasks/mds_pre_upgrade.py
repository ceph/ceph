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

    fs.set_allow_standby_replay(False)
    fs.set_max_mds(1)
    fs.reach_max_mds()

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
