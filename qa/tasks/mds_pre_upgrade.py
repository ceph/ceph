"""
Prepare MDS cluster for upgrade.
"""

import logging

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
    fs.getinfo() # load name
    fs.set_allow_standby_replay(False)
    fs.set_max_mds(1)
    fs.reach_max_mds()
