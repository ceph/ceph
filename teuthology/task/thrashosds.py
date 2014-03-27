"""
Thrash -- Simulate random osd failures.
"""
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

    min_in: (default 3) the minimum number of OSDs to keep in the
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

    timeout: (360) the number of seconds to wait for the cluster
       to become clean after each cluster change. If this doesn't
       happen within the timeout, an exception will be raised.

    revive_timeout: (75) number of seconds to wait for an osd asok to
       appear after attempting to revive the osd

    thrash_primary_affinity: (true) randomly adjust primary-affinity

    chance_pgnum_grow: (0) chance to increase a pool's size
    chance_pgpnum_fix: (0) chance to adjust pgpnum to pg for a pool
    pool_grow_by: (10) amount to increase pgnum by
    max_pgs_per_pool_osd: (1200) don't expand pools past this size per osd

    pause_short: (3) duration of short pause
    pause_long: (80) duration of long pause
    pause_check_after: (50) assert osd down after this long
    chance_inject_pause_short: (1) chance of injecting short stall
    chance_inject_pause_long: (0) chance of injecting long stall

    clean_wait: (0) duration to wait before resuming thrashing once clean

    powercycle: (false) whether to power cycle the node instead
        of just the osd process. Note that this assumes that a single
        osd is the only important process on the node.

    chance_test_backfill_full: (0) chance to simulate full disks stopping
        backfill

    chance_test_map_discontinuity: (0) chance to test map discontinuity
    map_discontinuity_sleep_time: (40) time to wait for map trims

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

    if 'powercycle' in config:

        # sync everyone first to avoid collateral damage to / etc.
        log.info('Doing preliminary sync to avoid collateral damage...')
        ctx.cluster.run(args=['sync'])

        if 'ipmi_user' in ctx.teuthology_config:
            for t, key in ctx.config['targets'].iteritems():
                host = t.split('@')[-1]
                shortname = host.split('.')[0]
                from ..orchestra import remote as oremote
                console = oremote.getRemoteConsole(
                    name=host,
                    ipmiuser=ctx.teuthology_config['ipmi_user'],
                    ipmipass=ctx.teuthology_config['ipmi_password'],
                    ipmidomain=ctx.teuthology_config['ipmi_domain'])
                cname = '{host}.{domain}'.format(
                    host=shortname,
                    domain=ctx.teuthology_config['ipmi_domain'])
                log.debug('checking console status of %s' % cname)
                if not console.check_status():
                    log.info(
                        'Failed to get console status for '
                        '%s, disabling console...'
                        % cname)
                    console=None
                else:
                    # find the remote for this console and add it
                    remotes = [
                        r for r in ctx.cluster.remotes.keys() if r.name == t]
                    if len(remotes) != 1:
                        raise Exception(
                            'Too many (or too few) remotes '
                            'found for target {t}'.format(t=t))
                    remotes[0].console = console
                    log.debug('console ready on %s' % cname)

            # check that all osd remotes have a valid console
            osds = ctx.cluster.only(teuthology.is_type('osd'))
            for remote, _ in osds.remotes.iteritems():
                if not remote.console:
                    raise Exception(
                        'IPMI console required for powercycling, '
                        'but not available on osd role: {r}'.format(
                            r=remote.name))

    log.info('Beginning thrashosds...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        config=config,
        logger=log.getChild('ceph_manager'),
        )
    ctx.manager = manager
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
        manager.wait_for_recovery(config.get('timeout', 360))
