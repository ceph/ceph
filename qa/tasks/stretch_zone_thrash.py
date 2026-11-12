"""
Zone thrasher for pools over two zones: takes down every OSD and monitor of
one of the two zones, picked at random, waits for degraded_stretch_mode in
the OSDMap, keeps the zone down for a while, then brings it back and waits
for degraded_stretch_mode and recovering_stretch_mode to clear. The
thrash-stretch and thrash-erasure-code-stretch suites run it as their
zone-default, zone-out and zone-slow-recovery thrashers.
"""
import contextlib
import logging
import math
import random
import time
import gevent
from gevent.event import Event
from teuthology import misc as teuthology
from teuthology.contextutil import safe_while
from tasks import ceph_manager
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)


class ZoneThrasher(Thrasher):
    """
    Waits up to pool_timeout seconds for a pool with is_stretch_pool set,
    as a pool over two zones has. Then, each round, it picks one of the two
    zones at random and stops its OSDs and monitors, without marking the
    OSDs out, and expects degraded_stretch_mode within degrade_timeout
    seconds; with mark_out, it then marks the zone's OSDs out. After
    hold_duration seconds it marks them in again if it marked them out,
    restarts the zone's monitors, waits for every monitor to be in quorum
    and restarts the OSDs. It expects degraded_stretch_mode and
    recovering_stretch_mode to be 0 within revive_timeout seconds, then
    waits thrash_delay seconds. If a wait times out, the thrasher stops
    with an error.

    Options::

      seed               RNG seed for reproducibility (default: None, uses time)
      hold_duration      Seconds to keep the zone down (default: 60)
      degrade_timeout    Seconds to wait for degraded_stretch_mode after
                         killing the zone (default: 300)
      revive_timeout     Seconds to wait for degraded_stretch_mode and
                         recovering_stretch_mode to clear after reviving
                         the zone (default: 600)
      thrash_delay       Seconds to sleep between iterations (default: 30)
      pool_timeout       Seconds to wait for a stretch pool to exist before
                         starting thrashing (default: 300)
      mark_out           Mark the zone's OSDs out once degraded_stretch_mode
                         is set, and in again before the zone is revived
                         (default: false)

    The zones are read from
    ``overrides.ceph.pool-config.crush_map_config``, which the suite's
    ``num_zones/2/2.yaml`` fragment sets. Each zone entry must have:

      - ``name``:     zone name (informational)
      - ``osds``:     list of OSD IDs belonging to this zone
      - ``monitors``: list of monitor IDs belonging to this zone

    The tiebreaker monitor (``tiebreak_monitor`` key) is never touched, so
    the monitors keep a quorum.

    Example task config::

      tasks:
      - stretch_zone_thrash:
          hold_duration: 60
          revive_timeout: 600
          degrade_timeout: 300
          thrash_delay: 30
    """

    def __init__(self, ctx, manager, config, name, logger):
        super(ZoneThrasher, self).__init__()

        self.ctx = ctx
        self.manager = manager
        self.config = config if config is not None else {}
        self.name = name
        self.logger = logger

        self.stopping = Event()

        self.random_seed = self.config.get('seed', None)
        if self.random_seed is None:
            self.random_seed = int(time.time())
        self.rng = random.Random()
        self.rng.seed(int(self.random_seed))

        self.hold_duration = float(self.config.get('hold_duration', 60))
        self.degrade_timeout = float(self.config.get('degrade_timeout', 300))
        self.revive_timeout = float(self.config.get('revive_timeout', 600))
        self.thrash_delay = float(self.config.get('thrash_delay', 30))
        self.pool_timeout = float(self.config.get('pool_timeout', 300))
        self.mark_out = bool(self.config.get('mark_out', False))

        # Parse zone topology from crush_map_config injected by 2.yaml
        pool_config = self.config.get('pool_config', {})
        crush_config = pool_config.get('crush_map_config', {})
        self.zones = crush_config.get('zones', [])
        self.tiebreak_monitor = crush_config.get('tiebreak_monitor', None)

        assert len(self.zones) >= 2, \
            'stretch_zone_thrash requires at least 2 zones in crush_map_config'

        self.log('seed: {s}, hold_duration: {h}s, degrade_timeout: {d}s, '
                 'revive_timeout: {r}s, thrash_delay: {t}s, '
                 'pool_timeout: {p}s, mark_out: {o}'.format(
                     s=self.random_seed, h=self.hold_duration,
                     d=self.degrade_timeout, r=self.revive_timeout,
                     t=self.thrash_delay, p=self.pool_timeout,
                     o=self.mark_out))
        self.log('zones: {z}'.format(z=[z['name'] for z in self.zones]))
        self.log('tiebreak monitor: {m}'.format(m=self.tiebreak_monitor))

        self.thread = gevent.spawn(self.do_thrash)

    def log(self, x):
        self.logger.info(x)

    def stop(self):
        self.stopping.set()

    def join(self):
        self.thread.get()

    def stop_and_join(self):
        self.stop()
        return self.join()

    def _kill_zone(self, zone):
        """
        Stop the zone's OSDs and monitors.  Daemons are stopped but not
        marked out so the cluster sees a sudden failure rather than a
        planned removal.
        """
        zone_name = zone['name']
        self.log('killing zone {z}: OSDs {o}, mons {m}'.format(
            z=zone_name, o=zone['osds'], m=zone['monitors']))

        for osd_id in zone['osds']:
            self.log('stopping osd.{i}'.format(i=osd_id))
            self.manager.kill_osd(osd_id)

        for mon_id in zone['monitors']:
            self.log('stopping mon.{i}'.format(i=mon_id))
            self.manager.kill_mon(mon_id)

    def _revive_zone(self, zone, total_mons=None):
        """
        Restart all zone monitors, wait for them to rejoin quorum, then restart
        all OSD daemons.

        The quorum wait between mons and OSDs is critical: if OSDs boot while
        the zone monitor is still absent the mon leader's dead_mon_buckets map
        still contains this zone.  The OSD-boot path in
        OSDMonitor::update_from_paxos skips go_recovery_stretch_mode() when
        dead_mon_buckets is non-empty, and once the mon eventually rejoins the
        OSD-up edge (prev_num_up_osd < num_up_osd) has already passed, leaving
        the cluster stuck in degraded_stretch_mode=1 / recovering_stretch_mode=0
        with all OSDs up and no further trigger to advance recovery.
        """
        zone_name = zone['name']
        self.log('reviving zone {z}: OSDs {o}, mons {m}'.format(
            z=zone_name, o=zone['osds'], m=zone['monitors']))
        if self.mark_out:
            self.manager.raw_cluster_cmd(
                'osd', 'in', *[str(osd_id) for osd_id in zone['osds']])

        for mon_id in zone['monitors']:
            self.log('reviving mon.{i}'.format(i=mon_id))
            self.manager.revive_mon(mon_id)

        # Block until the zone mon is back in quorum before starting OSDs so
        # that dead_mon_buckets is cleared on the leader first.
        if total_mons is not None:
            self.log('waiting for full quorum ({n} mons) before reviving OSDs'.format(
                n=total_mons))
            self.manager.wait_for_mon_quorum_size(total_mons)

        for osd_id in zone['osds']:
            self.log('reviving osd.{i}'.format(i=osd_id))
            self.manager.revive_osd(osd_id, skip_admin_check=True)

    def _wait_for_stretch_pool(self):
        """
        Poll until at least one pool with is_stretch_pool: true exists.
        Prevents racing with pool creation during task startup.
        Raises RuntimeError on timeout.
        """
        self.log('waiting for stretch pool to exist (timeout={t}s)'.format(
            t=self.pool_timeout))
        start = time.time()
        with safe_while(
                sleep=5,
                tries=math.ceil(self.pool_timeout / 5),
                action='wait for stretch pool',
                _raise=False) as proceed:
            while proceed():
                if self.stopping.is_set():
                    return False
                try:
                    osdmap = self.manager.get_osd_dump_json()
                    pools = osdmap.get('pools', [])
                    for pool in pools:
                        if pool.get('is_stretch_pool') is True:
                            elapsed = time.time() - start
                            self.log('stretch pool found ({p}) after {e:.1f}s'.format(
                                p=pool.get('pool_name', pool.get('pool', 'unknown')),
                                e=elapsed))
                            return True
                except Exception as e:
                    self.log('Error checking for stretch pool: {0}'.format(e))
        raise RuntimeError(
            'Timed out waiting for stretch pool to exist after {t}s'.format(
                t=self.pool_timeout))

    def _wait_for_degraded_stretch(self):
        """
        Poll until degraded_stretch_mode is set in the OSDMap.
        Returns False if the thrasher is stopped first.
        Raises RuntimeError on timeout.
        """
        self.log('waiting for degraded mode (timeout={t}s)'.format(
            t=self.degrade_timeout))
        start = time.time()
        with safe_while(
                sleep=5,
                tries=math.ceil(self.degrade_timeout / 5),
                action='wait for degraded mode',
                _raise=False) as proceed:
            while proceed():
                if self.stopping.is_set():
                    return False
                if self.manager.is_degraded_stretch_mode():
                    elapsed = time.time() - start
                    self.log('cluster entered degraded mode after '
                             '{e:.1f}s'.format(e=elapsed))
                    return True
        raise RuntimeError(
            'Timed out waiting for cluster to enter degraded mode '
            'after {t}s'.format(t=self.degrade_timeout))

    def _wait_for_healthy_stretch(self):
        """
        Poll until degraded_stretch_mode == 0 AND recovering_stretch_mode == 0.

        The monitor sets degraded_stretch_mode=0 / recovering_stretch_mode=1 as
        soon as the second site rejoins, then waits for PGs to finish recovering
        before clearing recovering_stretch_mode.  Checking only
        is_degraded_stretch_mode() exits too early and leaves PGs inactive.
        Returns early if the thrasher is stopped.
        Raises RuntimeError on timeout.
        """
        self.log('waiting for normal mode (timeout={t}s)'.format(
            t=self.revive_timeout))
        start = time.time()
        with safe_while(
                sleep=10,
                tries=math.ceil(self.revive_timeout / 10),
                action='wait for normal mode',
                _raise=False) as proceed:
            while proceed():
                if self.stopping.is_set():
                    return
                if (not self.manager.is_degraded_stretch_mode() and
                        not self.manager.is_recovering_stretch_mode()):
                    elapsed = time.time() - start
                    self.log('cluster returned to normal mode after '
                             '{e:.1f}s'.format(e=elapsed))
                    return
        raise RuntimeError(
            'Timed out waiting for cluster to return to normal mode '
            'after {t}s'.format(t=self.revive_timeout))

    def do_thrash(self):
        """
        Wrapper that catches exceptions and records them for the DaemonWatchdog.
        """
        try:
            self._do_thrash()
        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception('exception in ZoneThrasher:')

    def _do_thrash(self):
        """
        Main thrash loop.
        """
        self.log('ZoneThrasher starting')
        if not self._wait_for_stretch_pool():
            return
        total_mons = len(teuthology.get_mon_names(self.ctx))

        while not self.stopping.is_set():
            # Pick a zone at random
            zone = self.rng.choice(self.zones)
            self.log('=== iteration start: thrashing zone {z} ==='.format(
                z=zone['name']))

            # Take down the zone
            self._kill_zone(zone)

            # Confirm that degraded_stretch_mode is set
            try:
                degraded = self._wait_for_degraded_stretch()
            except RuntimeError:
                # Always revive before propagating so teardown can succeed
                self._revive_zone(zone, total_mons=total_mons)
                raise
            if not degraded:
                self._revive_zone(zone, total_mons=total_mons)
                break
            if self.mark_out:
                self.log('marking zone {z} out'.format(z=zone['name']))
                self.manager.raw_cluster_cmd(
                    'osd', 'out', *[str(osd_id) for osd_id in zone['osds']])

            # Hold the zone down — this is the window where the IO workload
            # runs on the other zone alone
            self.log('holding zone {z} down for {d}s'.format(
                z=zone['name'], d=self.hold_duration))
            time.sleep(self.hold_duration)

            if self.stopping.is_set():
                self._revive_zone(zone, total_mons=total_mons)
                break

            # Revive the zone — mon rejoins quorum before OSDs boot
            self._revive_zone(zone, total_mons=total_mons)

            # Wait for degraded_stretch_mode and recovering_stretch_mode to
            # clear
            self._wait_for_healthy_stretch()

            self.log('=== iteration complete: zone {z} recovered ==='.format(
                z=zone['name']))

            if self.thrash_delay > 0 and not self.stopping.is_set():
                self.log('sleeping {t}s before next iteration'.format(
                    t=self.thrash_delay))
                time.sleep(self.thrash_delay)

        self.log('ZoneThrasher stopped')


@contextlib.contextmanager
def task(ctx, config):
    """
    Run ZoneThrasher while the tasks after it run. Afterwards, wait for
    every monitor to be in quorum and for recovery to finish within
    revive_timeout seconds.

    The zones are read from the ``crush_map_config`` that the suite's
    ``num_zones/2/2.yaml`` fragment sets (in ``overrides.ceph.pool-config``),
    so the job must have pools over two zones. Takes the options of
    ZoneThrasher, and cluster (default: ceph).

    Example::

      tasks:
      - stretch_zone_thrash:
          hold_duration: 60
          revive_timeout: 600
          degrade_timeout: 300
          thrash_delay: 30
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'stretch_zone_thrash task only accepts a dict for configuration'

    cluster = config.get('cluster', 'ceph')

    # Pull the crush_map_config from the pool-config overrides so the thrasher
    # knows which OSDs and monitors belong to each zone.
    overrides = ctx.config.get('overrides', {})
    pool_config = overrides.get('ceph', {}).get('pool-config', {})
    config['pool_config'] = pool_config

    crush_config = pool_config.get('crush_map_config', {})
    zones = crush_config.get('zones', [])
    assert len(zones) >= 2, (
        'stretch_zone_thrash requires crush_map_config.zones with at least '
        '2 entries in overrides.ceph.pool-config; got: {z}'.format(z=zones))

    log.info('Beginning stretch_zone_thrash...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
    )

    thrash_proc = ZoneThrasher(
        ctx,
        manager,
        config,
        'ZoneThrasher',
        logger=log.getChild('zone_thrasher'),
    )
    ctx.ceph[cluster].thrashers.append(thrash_proc)
    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining stretch_zone_thrash')
        thrash_proc.stop_and_join()
        # Ensure the cluster is fully recovered before suite teardown
        total_mons = len(teuthology.get_mon_names(ctx))
        manager.wait_for_mon_quorum_size(total_mons)
        manager.wait_for_recovery(config.get('revive_timeout', 600))
