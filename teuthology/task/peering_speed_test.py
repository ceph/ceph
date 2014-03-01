"""
Remotely run peering tests.
"""
import logging
import time
from teuthology import misc as teuthology
import ceph_manager

log = logging.getLogger(__name__)

from args import argify

POOLNAME = "POOLNAME"
ARGS = [
    ('num_pgs', 'number of pgs to create', 256, int),
    ('max_time', 'seconds to complete peering', 0, int),
    ('runs', 'trials to run', 10, int),
    ('num_objects', 'objects to create', 256 * 1024, int),
    ('object_size', 'size in bytes for objects', 64, int),
    ('creation_time_limit', 'time limit for pool population', 60*60, int),
    ('create_threads', 'concurrent writes for create', 256, int)
    ]

def setup(ctx, config):
    """
    Setup peering test on remotes.
    """
    first_mon = teuthology.get_first_mon(ctx, config)
    mon = teuthology.get_single_remote_value(ctx, first_mon)
    ctx.manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )
    ctx.manager.clear_pools()
    ctx.manager.create_pool(POOLNAME, config.num_pgs)
    log.info("populating pool")
    ctx.manager.rados_write_objects(
        POOLNAME,
        config.num_objects,
        config.object_size,
        config.creation_time_limit,
        config.create_threads)
    log.info("done populating pool")

def do_run(ctx, config):
    """
    Perform the test.
    """
    start = time.time()
    # mark in osd
    ctx.manager.mark_in_osd(0)
    log.info("writing out objects")
    ctx.manager.rados_write_objects(
        POOLNAME,
        config.num_pgs, # write 1 object per pg or so
        1,
        config.creation_time_limit,
        config.num_pgs, # lots of concurrency
        cleanup = True)
    peering_end = time.time()

    log.info("peering done, waiting on recovery")
    ctx.manager.wait_for_clean()

    log.info("recovery done")
    recovery_end = time.time()
    if config.max_time:
        assert(peering_end - start < config.max_time)
    ctx.manager.mark_out_osd(0)
    ctx.manager.wait_for_clean()
    return {
        'time_to_active': peering_end - start,
        'time_to_clean': recovery_end - start
        }

@argify("peering_speed_test", ARGS)
def task(ctx, config):
    """
    Peering speed test
    """
    setup(ctx, config)
    ctx.manager.mark_out_osd(0)
    ctx.manager.wait_for_clean()
    ret = []
    for i in range(config.runs):
        log.info("Run {i}".format(i = i))
        ret.append(do_run(ctx, config))

    ctx.manager.mark_in_osd(0)
    ctx.summary['recovery_times'] = {
        'runs': ret
        }
