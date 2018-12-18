"""
Upgrade cluster snap format.
"""

import logging
import time

from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Upgrade CephFS file system snap format.
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'snap-upgrade task only accepts a dict for configuration'

    fs = Filesystem(ctx)

    mds_map = fs.get_mds_map()
    assert(mds_map['max_mds'] == 1)

    json = fs.rank_tell(["scrub", "start", "/", "force", "recursive", "repair"])
    if not json or json['return_code'] == 0:
        log.info("scrub / completed")
    else:
        log.info("scrub / failed: {}".format(json))

    json = fs.rank_tell(["scrub", "start", "~mdsdir", "force", "recursive", "repair"])
    if not json or json['return_code'] == 0:
        log.info("scrub ~mdsdir completed")
    else:
        log.info("scrub / failed: {}".format(json))

    for i in range(0, 10):
        mds_map = fs.get_mds_map()
        if (mds_map['flags'] & (1<<1)) != 0 and (mds_map['flags'] & (1<<4)) != 0:
            break
        time.sleep(10)
    assert((mds_map['flags'] & (1<<1)) != 0) # Test CEPH_MDSMAP_ALLOW_SNAPS
    assert((mds_map['flags'] & (1<<4)) != 0) # Test CEPH_MDSMAP_ALLOW_MULTIMDS_SNAPS
