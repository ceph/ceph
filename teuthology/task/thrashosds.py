import contextlib
import logging
import ceph_manager

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run thrashosds

    There is no configuration, all commands are run on mon0 and it stops when
    __exit__ is called.

    example:

    tasks:
    - ceph:
    - thrashosds: 
    - interactive:
    """
    log.info('Beginning thrashosds...')
    (mon,) = ctx.cluster.only('mon.0').remotes.iterkeys()
    manager = ceph_manager.CephManager(
        mon, 
        logger = log.getChild('ceph_manager'),
        )
    thrash_proc = ceph_manager.Thrasher(
        manager,
        logger = log.getChild('thrasher'),
        )
    try:
        yield
    finally:
        log.info('joining thrashosds')
        thrash_proc.do_join()
