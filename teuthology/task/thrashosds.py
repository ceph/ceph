import contextlib
import logging
import ceph_manager

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    "Thrash" the OSDs by randomly marking them out/down (and then back
    in) until the task is ended.

    All commands are run on mon0 and it stops when __exit__ is called.
    The config is optional, and is a dict containing some or all of:
    minIn: (default 2) the minimum number of OSDs to keep in the cluster
    minOut: (default 0) the minimum number of OSDs to keep out of the cluster
    opDelay: (5) the length of time to sleep between changing an OSD's status
    cleanInterval: (60) the approximate length of time to loop before waiting
    until the cluster goes clean. (In reality this is used to probabilistically
    choose when to wait, and the method used makes it closer to -- but not
    identical to -- the half-life.)
    chanceOut: (0) the probability that the thrasher will mark an OSD down
    rather than marking it out. (The thrasher will not consider that OSD
    out of the cluster, since presently an OSD wrongly marked down will
    mark itself back up again.) This value can be either an integer (eg, 75)
    or a float probability (eg 0.75).
    

    example:

    tasks:
    - ceph:
    - thrashosds:
        {chanceDown: 10, opDelay: 3, minIn: 1}
    - interactive:
    """
    log.info('Beginning thrashosds...')
    (mon,) = ctx.cluster.only('mon.0').remotes.iterkeys()
    manager = ceph_manager.CephManager(
        mon,
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
