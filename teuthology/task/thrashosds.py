import contextlib
import logging
import ceph_manager
from teuthology import misc as teuthology


log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    "Thrash" the OSDs by randomly marking them out/down (and then back
    in) until the task is ended. This loops, and every op_delay
    seconds it randomly chooses to add or remove an OSD (even odds)
    unless there are fewer than min_out OSDs out of the cluster, or
    more than min_in OSDs in the cluster.

    All commands are run on mon0 and it stops when __exit__ is called.

    The config is optional, and is a dict containing some or all of:

    min_in: (default 2) the minimum number of OSDs to keep in the
       cluster

    min_out: (default 0) the minimum number of OSDs to keep out of the
       cluster

    op_delay: (5) the length of time to sleep between changing an
       OSD's status

    clean_interval: (60) the approximate length of time to loop before
       waiting until the cluster goes clean. (In reality this is used
       to probabilistically choose when to wait, and the method used
       makes it closer to -- but not identical to -- the half-life.)

    chance_down: (0) the probability that the thrasher will mark an
       OSD down rather than marking it out. (The thrasher will not
       consider that OSD out of the cluster, since presently an OSD
       wrongly marked down will mark itself back up again.) This value
       can be either an integer (eg, 75) or a float probability (eg
       0.75).

    timeout: (360) the number of seconds to wait for the cluster
       to become clean before the task exits. If this doesn't happen,
       an exception will be raised.

    example:

    tasks:
    - ceph:
    - thrashosds:
        chance_down: 10
        op_delay: 3
        min_in: 1
        timeout: 600
    - interactive:
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'thrashosds task only accepts a dict for configuration'
    log.info('Beginning thrashosds...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )
    thrash_proc = ceph_manager.Thrasher(
        manager,
        config,
        logger=log.getChild('thrasher')
        )
    try:
        yield
    finally:
        log.info('joining thrashosds')
        thrash_proc.do_join()
        manager.wait_till_clean(config.get('timeout', 360))
