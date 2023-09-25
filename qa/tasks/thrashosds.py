"""
Thrash -- Simulate random osd failures.
"""
import contextlib
import logging
from tasks import ceph_manager
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

    cluster: (default 'ceph') the name of the cluster to thrash

    min_in: (default 4) the minimum number of OSDs to keep in the
       cluster

    min_out: (default 0) the minimum number of OSDs to keep out of the
       cluster

    op_delay: (5) the length of time to sleep between changing an
       OSD's status

    min_dead: (0) minimum number of osds to leave down/dead.

    max_dead: (0) maximum number of osds to leave down/dead before waiting
       for clean.  This should probably be num_replicas - 1.

    clean_interval: (60) the approximate length of time to loop before
       waiting until the cluster goes clean. (In reality this is used
       to probabilistically choose when to wait, and the method used
       makes it closer to -- but not identical to -- the half-life.)

    scrub_interval: (-1) the approximate length of time to loop before
       waiting until a scrub is performed while cleaning. (In reality
       this is used to probabilistically choose when to wait, and it
       only applies to the cases where cleaning is being performed).
       -1 is used to indicate that no scrubbing will be done.

    chance_down: (0.4) the probability that the thrasher will mark an
       OSD down rather than marking it out. (The thrasher will not
       consider that OSD out of the cluster, since presently an OSD
       wrongly marked down will mark itself back up again.) This value
       can be either an integer (eg, 75) or a float probability (eg
       0.75).

    chance_test_min_size: (0) chance to run test_pool_min_size,
       which:
       - kills all but one osd
       - waits
       - kills that osd
       - revives all other osds
       - verifies that the osds fully recover
    
    test_min_size_duration: (1800) the number of seconds for
        test_pool_min_size to last.

    timeout: (360) the number of seconds to wait for the cluster
       to become clean after each cluster change. If this doesn't
       happen within the timeout, an exception will be raised.

    revive_timeout: (150) number of seconds to wait for an osd asok to
       appear after attempting to revive the osd

    thrash_primary_affinity: (true) randomly adjust primary-affinity

    chance_pgnum_grow: (0) chance to increase a pool's size
    chance_pgpnum_fix: (0) chance to adjust pgpnum to pg for a pool
    pool_grow_by: (10) amount to increase pgnum by
    chance_pgnum_shrink: (0) chance to decrease a pool's size
    pool_shrink_by: (10) amount to decrease pgnum by
    max_pgs_per_pool_osd: (1200) don't expand pools past this size per osd

    pause_short: (3) duration of short pause
    pause_long: (80) duration of long pause
    pause_check_after: (50) assert osd down after this long
    chance_inject_pause_short: (1) chance of injecting short stall
    chance_inject_pause_long: (0) chance of injecting long stall

    clean_wait: (0) duration to wait before resuming thrashing once clean

    sighup_delay: (0.1) duration to delay between sending signal.SIGHUP to a
                  random live osd

    powercycle: (false) whether to power cycle the node instead
        of just the osd process. Note that this assumes that a single
        osd is the only important process on the node.

    bdev_inject_crash: (0) seconds to delay while inducing a synthetic crash.
        the delay lets the BlockDevice "accept" more aio operations but blocks
        any flush, and then eventually crashes (losing some or all ios).  If 0,
        no bdev failure injection is enabled.

    bdev_inject_crash_probability: (.5) probability of doing a bdev failure
        injection crash vs a normal OSD kill.

    chance_test_backfill_full: (0) chance to simulate full disks stopping
        backfill

    chance_test_map_discontinuity: (0) chance to test map discontinuity
    map_discontinuity_sleep_time: (40) time to wait for map trims

    ceph_objectstore_tool: (true) whether to export/import a pg while an osd is down
    chance_move_pg: (1.0) chance of moving a pg if more than 1 osd is down (default 100%)

    optrack_toggle_delay: (2.0) duration to delay between toggling op tracker
                  enablement to all osds

    dump_ops_enable: (true) continuously dump ops on all live osds

    noscrub_toggle_delay: (2.0) duration to delay between toggling noscrub

    disable_objectstore_tool_tests: (false) disable ceph_objectstore_tool based
                                    tests

    chance_thrash_cluster_full: .05

    chance_thrash_pg_upmap: 1.0
    chance_thrash_pg_upmap_items: 1.0

    aggressive_pg_num_changes: (true)  whether we should bypass the careful throttling of pg_num and pgp_num changes in mgr's adjust_pgs() controller

    example:

    tasks:
    - ceph:
    - thrashosds:
        cluster: ceph
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
    # add default value for sighup_delay
    config['sighup_delay'] = config.get('sighup_delay', 0.1)
    # add default value for optrack_toggle_delay
    config['optrack_toggle_delay'] = config.get('optrack_toggle_delay', 2.0)
    # add default value for dump_ops_enable
    config['dump_ops_enable'] = config.get('dump_ops_enable', "true")
    # add default value for noscrub_toggle_delay
    config['noscrub_toggle_delay'] = config.get('noscrub_toggle_delay', 2.0)
    # add default value for random_eio
    config['random_eio'] = config.get('random_eio', 0.0)
    aggro = config.get('aggressive_pg_num_changes', True)

    log.info("config is {config}".format(config=str(config)))

    overrides = ctx.config.get('overrides', {})
    log.info("overrides is {overrides}".format(overrides=str(overrides)))
    teuthology.deep_merge(config, overrides.get('thrashosds', {}))
    cluster = config.get('cluster', 'ceph')

    log.info("config is {config}".format(config=str(config)))

    if 'powercycle' in config:

        # sync everyone first to avoid collateral damage to / etc.
        log.info('Doing preliminary sync to avoid collateral damage...')
        ctx.cluster.run(args=['sync'])

        if 'ipmi_user' in ctx.teuthology_config:
            for remote in ctx.cluster.remotes.keys():
                log.debug('checking console status of %s' % remote.shortname)
                if not remote.console.check_status():
                    log.warning('Failed to get console status for %s',
                             remote.shortname)

            # check that all osd remotes have a valid console
            osds = ctx.cluster.only(teuthology.is_type('osd', cluster))
            for remote in osds.remotes.keys():
                if not remote.console.has_ipmi_credentials:
                    raise Exception(
                        'IPMI console required for powercycling, '
                        'but not available on osd role: {r}'.format(
                            r=remote.name))

    cluster_manager = ctx.managers[cluster]
    for f in ['powercycle', 'bdev_inject_crash']:
        if config.get(f):
            cluster_manager.config[f] = config.get(f)

    if aggro:
        cluster_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr_debug_aggressive_pg_num_changes',
            'true')

    log.info('Beginning thrashosds...')
    thrash_proc = ceph_manager.OSDThrasher(
        cluster_manager,
        config,
        "OSDThrasher",
        logger=log.getChild('thrasher')
        )
    ctx.ceph[cluster].thrashers.append(thrash_proc)
    try:
        yield
    finally:
        log.info('joining thrashosds')
        thrash_proc.do_join()
        cluster_manager.wait_for_all_osds_up()
        cluster_manager.flush_all_pg_stats()
        cluster_manager.wait_for_recovery(config.get('timeout', 360))
        if aggro:
            cluster_manager.raw_cluster_cmd(
                'config', 'rm', 'mgr',
                'mgr_debug_aggressive_pg_num_changes')
