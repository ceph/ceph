"""
Manager thrash
"""
import logging
import contextlib
import json
import random
import time
from teuthology import misc as teuthology
from teuthology.contextutil import safe_while
from tasks import ceph_manager
from tasks.thrasher import ThrasherGreenlet

log = logging.getLogger(__name__)

def _get_mgrs(ctx, cluster='ceph'):
    """
    Get mgr daemon ids from the context value.
    """
    is_mgr = teuthology.is_type('mgr', cluster)
    mgrs = ctx.cluster.only(is_mgr)
    ids = []
    for _, roles in mgrs.remotes.items():
        for role in roles:
            if not is_mgr(role):
                continue
            _, _, mgr_id = teuthology.split_role(role)
            ids.append(mgr_id)
    return ids

class MgrThrasher(ThrasherGreenlet):
    """
    How it works::

    - pick a manager (biased towards the active one)
    - either kill it, or tell the cluster to fail it over
    - wait for the cluster to report a manager available again
    - sleep for 'revive_delay' seconds
    - revive it, if it was killed
    - sleep for 'thrash_delay' seconds

    Options::

    seed                 Seed to use on the RNG to reproduce a previous
                         behaviour (default: None; i.e., not set)
    revive_delay         Number of seconds to wait before reviving
                         a killed manager (default: 10)
    thrash_delay         Number of seconds to wait in-between
                         test iterations (default: 0)
    thrash_many          Kill more than one manager per iteration, up to
                         as many as can be killed while still honoring
                         'maintain_availability' (default: False)
    maintain_availability  Always leave at least one manager running, so
                         the cluster never goes without an active mgr
                         (default: True)
    kill_active_probability  How often, in %, to target the active
                         manager rather than a standby (default: 80)
    fail_probability     How often, in %, to use 'ceph mgr fail' instead
                         of killing the daemon process (default: 20)
    check_enabled_modules  After each iteration, assert that the set of
                         mgr modules enabled before thrashing began is
                         still enabled (default: True)
    selftest_modules     List of mgr module names to run
                         'ceph mgr self-test module <name>' against
                         after every iteration, to confirm they still
                         work on whichever mgr is now active. The
                         modules (and the 'selftest' module itself)
                         are enabled once, up front. (default: [])
    test_remote_dispatch  After every iteration, run 'ceph mgr self-test
                         remote', which exercises MgrModule.remote()
                         including its error paths (nonexistent module,
                         nonexistent method) -- code that has a history
                         of crashing the mgr daemon. Enables the
                         'selftest' and 'influx' modules up front.
                         (default: False)
    module_load_delay_ms  If set > 0, inject this many milliseconds of
                         startup delay into module_load_delay_name via
                         mgr_module_load_delay, on every mgr startup
                         for the rest of the run -- deliberately
                         stressing the module-not-started-yet race a
                         newly active mgr can hit. (default: 0)
    module_load_delay_name  Which module to delay via
                         module_load_delay_ms. Defaults to the first
                         entry of selftest_modules, or 'devicehealth'
                         if that's empty.
    check_admin_socket    After every iteration, query the current
                         active mgr's own admin socket (perf dump), to
                         catch a hung or unresponsive mgr process on
                         whichever one just took over. (default: True)
    check_module_option   Set the selftest module's rwoption6 to a
                         non-default value up front, then after every
                         iteration assert the current active mgr still
                         reads it, so module options survive failover.
                         Enables the 'selftest' module. (default: True)
    check_module_errors   After every iteration, assert there is no
                         MGR_MODULE_ERROR health check, i.e. no module
                         failed to load or crashed on the new active
                         mgr. (default: True)
    check_osd_reports     After every iteration, wait until every up
                         OSD has sent stats to the current active mgr,
                         to catch OSDs that never reconnect to a new
                         active mgr. (default: True)

    For example::

    tasks:
    - ceph:
    - mgr_thrash:
        revive_delay: 20
        thrash_delay: 1
        kill_active_probability: 90
        fail_probability: 30
        selftest_modules: [crash, devicehealth, prometheus]
        seed: 31337
    - workunit:
        clients:
          all:
            - rados/test.sh
    """
    def __init__(self, ctx, manager, config, name, logger):
        super().__init__()

        self.ctx = ctx
        self.manager = manager
        self.manager.wait_for_mgr_available()

        self.logger = logger
        self.config = config
        self.name = name
        self.cluster = self.config['cluster']

        # Test reproducibility
        self.random_seed = self.config.get('seed', None)

        if self.random_seed is None:
            self.random_seed = int(time.time())

        self.rng = random.Random()
        self.rng.seed(int(self.random_seed))

        self.revive_delay = float(self.config.get('revive_delay', 10.0))
        self.thrash_delay = float(self.config.get('thrash_delay', 0.0))
        self.thrash_many = self.config.get('thrash_many', False)
        self.maintain_availability = self.config.get('maintain_availability', True)
        self.kill_active_probability = float(
            self.config.get('kill_active_probability', 80))
        self.fail_probability = float(self.config.get('fail_probability', 20))
        self.check_enabled_modules = self.config.get('check_enabled_modules', True)
        self.selftest_modules = self.config.get('selftest_modules', [])
        self.test_remote_dispatch = self.config.get('test_remote_dispatch', False)
        self.module_load_delay_ms = int(self.config.get('module_load_delay_ms', 0))
        self.module_load_delay_name = self.config.get(
            'module_load_delay_name',
            self.selftest_modules[0] if self.selftest_modules else 'devicehealth')
        self.check_admin_socket = self.config.get('check_admin_socket', True)
        self.check_module_option = self.config.get('check_module_option', True)
        self.check_module_errors = self.config.get('check_module_errors', True)
        self.check_osd_reports = self.config.get('check_osd_reports', True)
        self.failover_timeout = self._get_beacon_grace() * 2

        assert self.max_killable() > 0, \
            'Unable to kill at least one manager with the current config.'

        if (self.selftest_modules or self.test_remote_dispatch or
                self.check_module_option):
            self._load_module('selftest')
            for module_name in self.selftest_modules:
                self._load_module(module_name)
            if self.test_remote_dispatch:
                self._load_module('influx')

        if self.check_enabled_modules:
            self.baseline_modules = set(self._get_enabled_modules())
            self.log('baseline enabled mgr modules: {m}'.format(
                m=sorted(self.baseline_modules)))

        if self.module_load_delay_ms > 0:
            self._load_module(self.module_load_delay_name)
            self.manager.raw_cluster_cmd(
                'config', 'set', 'mgr', 'mgr_module_load_delay',
                str(self.module_load_delay_ms))
            self.manager.raw_cluster_cmd(
                'config', 'set', 'mgr', 'mgr_module_load_delay_name',
                self.module_load_delay_name)
            self.log('injecting {d}ms load delay into mgr module {m} on '
                      'every startup'.format(d=self.module_load_delay_ms,
                                              m=self.module_load_delay_name))

        if self.check_module_option:
            # rwoption6 defaults to True and nothing else touches it.
            self.manager.raw_cluster_cmd(
                'config', 'set', 'mgr', 'mgr/selftest/rwoption6', 'false')
            self.log('set mgr/selftest/rwoption6=false, will check it '
                     'survives failover')

    def log(self, x):
        """
        locally log info messages
        """
        self.logger.info(x)

    def max_killable(self):
        """
        Return the maximum number of managers we can kill at once.
        """
        n = len(_get_mgrs(self.ctx, self.cluster))
        return max(n - 1, 0) if self.maintain_availability else n

    def kill_mgr(self, mgr):
        """
        Kill the manager specified
        """
        self.log('killing mgr.{id}'.format(id=mgr))
        self.manager.kill_mgr(mgr)

    def fail_mgr(self, mgr):
        """
        Fail the manager specified over, without killing its process.
        """
        self.log('failing mgr.{id}'.format(id=mgr))
        self.manager.fail_mgr(mgr)

    def _get_beacon_grace(self):
        """
        Read mon_mgr_beacon_grace: how long the mon waits without a
        beacon before it considers a manager dead and fails it over.
        """
        return float(self.manager.raw_cluster_cmd(
            'config', 'get', 'mon', 'mon_mgr_beacon_grace'))

    def _wait_for_failover(self, old_active):
        """
        Wait until the mgrmap reports a different active manager than
        old_active. A killed active manager's process death is only
        detected by the mon after mon_mgr_beacon_grace, so the
        mgrmap's 'available' flag can stay stale (still pointing at
        the dead daemon) well after kill_mgr() returns; this polls
        for the actual handoff instead of trusting that flag alone.
        """
        timeout = self.failover_timeout
        self.log('waiting for failover away from mgr.{a}'.format(a=old_active))
        with safe_while(sleep=3, tries=max(int(timeout // 3), 1),
                         action='wait for failover away from mgr.{a}'.format(
                             a=old_active)) as proceed:
            while proceed():
                new_active = self.manager.get_mgr_dump()['active_name']
                if new_active and new_active != old_active:
                    self.log('mgr.{a} took over as active'.format(a=new_active))
                    break

    def revive_mgr(self, mgr):
        """
        Revive the manager specified
        """
        self.log('reviving mgr.{id}'.format(id=mgr))
        self.manager.revive_mgr(mgr)

    def _wait_until_rejoined(self, mgrs, timeout=60):
        """
        Wait until all of the given managers have rejoined the mgrmap,
        as either the active manager or a standby.
        """
        self.log('waiting for mgrs to rejoin: {m}'.format(m=mgrs))
        with safe_while(sleep=3, tries=timeout // 3,
                         action='wait for revived mgrs to rejoin') as proceed:
            while proceed():
                dump = self.manager.get_mgr_dump()
                present = set([dump['active_name']] +
                              [s['name'] for s in dump['standbys']])
                if all(m in present for m in mgrs):
                    break

    def _get_enabled_modules(self):
        """
        Return the list of currently enabled mgr modules
        """
        dump = json.loads(self.manager.raw_cluster_cmd(
            'mgr', 'module', 'ls', '--format=json-pretty'))
        return dump['enabled_modules'] + dump['always_on_modules']

    def _load_module(self, module_name):
        """
        Enable a mgr module, if it isn't already, and wait for it to
        show up as enabled.
        """
        if module_name in self._get_enabled_modules():
            return
        self.log('loading mgr module {m}'.format(m=module_name))
        self.manager.raw_cluster_cmd(
            'mgr', 'module', 'enable', module_name, '--force')
        with safe_while(sleep=3, tries=20,
                         action='wait for module {m} to load'.format(
                             m=module_name)) as proceed:
            while proceed():
                if module_name in self._get_enabled_modules():
                    break

    def _check_enabled_modules(self):
        """
        Assert that every module that was enabled before thrashing
        began is still enabled.
        """
        current = set(self._get_enabled_modules())
        missing = self.baseline_modules - current
        assert not missing, \
            'mgr modules disappeared after thrashing: {m}'.format(m=missing)

    def _run_module_selftests(self):
        """
        Run 'ceph mgr self-test module <name>' for each configured
        module, to confirm it still works on whichever mgr is active.
        """
        for module_name in self.selftest_modules:
            self.log('running self-test for mgr module {m}'.format(
                m=module_name))
            self.manager.raw_cluster_cmd(
                'mgr', 'self-test', 'module', module_name)

    def _run_remote_dispatch_selftest(self):
        """
        Run 'ceph mgr self-test remote', which exercises
        MgrModule.remote() including its error paths (nonexistent
        module, nonexistent method) -- code with a history of
        crashing the mgr daemon's dispatch_remote fast path.
        """
        self.log('running mgr self-test remote (remote() dispatch)')
        self.manager.raw_cluster_cmd('mgr', 'self-test', 'remote')

    def _check_admin_socket(self):
        """
        Query the current active mgr's own admin socket (perf dump),
        to catch a hung or unresponsive mgr process on whichever one
        just took over. 'perf dump' isn't registered on mgr's tell/
        network command interface (only the generic admin_socket
        commands like 'version' are) -- it only exists on the local
        admin socket, so go straight there instead of through tell.
        """
        active = self.manager.get_mgr_dump()['active_name']
        if not active:
            return
        proc = self.manager.admin_socket('mgr', active, ['perf', 'dump'])
        json.loads(proc.stdout.getvalue())

    def _check_module_option(self):
        """
        Assert the selftest module on the current active mgr still
        reads the non-default rwoption6 set at startup.
        """
        val = self.manager.raw_cluster_cmd(
            'mgr', 'self-test', 'config', 'get', 'rwoption6').strip()
        assert val == 'False', \
            'mgr/selftest/rwoption6 reads {v!r} after thrashing, ' \
            'expected False'.format(v=val)

    def _check_module_errors(self):
        """
        Assert no mgr module failed on the current active mgr. Modules
        still loading are left out of MGR_MODULE_ERROR by the mgr.
        """
        checks = self.manager.get_mon_health(detail=True)['checks']
        assert 'MGR_MODULE_ERROR' not in checks, \
            'mgr module failed after thrashing: {c}'.format(
                c=checks.get('MGR_MODULE_ERROR'))

    def _wait_for_osd_reports(self, timeout=120):
        """
        Wait until every up OSD shows up in the current active mgr's
        'pg dump osds', i.e. has sent it stats. A new active mgr starts
        with no OSD stats, so this catches an OSD that never reconnects.
        pg_ready is not enough: the mgr sets it after 4 *
        mgr_stats_period even if some OSDs never reported.
        """
        with safe_while(sleep=3, tries=timeout // 3,
                        action='wait for up OSDs to report to the '
                               'active mgr') as proceed:
            while proceed():
                up = {o['osd'] for o in self.manager.get_osd_dump()
                      if o['up']}
                out = self.manager.raw_cluster_cmd(
                    'pg', 'dump', 'osds', '--format=json')
                dump = json.loads(out[out.index('{'):])
                missing = up - {s['osd'] for s in dump['osd_stats']}
                if not missing:
                    break
                self.log('OSDs not reporting to the active mgr yet: '
                         '{m}'.format(m=sorted(missing)))

    def _validate_modules(self):
        if self.check_enabled_modules:
            self._check_enabled_modules()
        if self.selftest_modules:
            self._run_module_selftests()
        if self.test_remote_dispatch:
            self._run_remote_dispatch_selftest()
        if self.check_admin_socket:
            self._check_admin_socket()
        if self.check_module_option:
            self._check_module_option()
        if self.check_module_errors:
            self._check_module_errors()
        if self.check_osd_reports:
            self._wait_for_osd_reports()

    def _run(self):
        """
        _do_thrash() wrapper.
        """
        try:
            self._do_thrash()
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # Allow successful completion so gevent doesn't see an exception.
            # The DaemonWatchdog will observe the error and tear down the test.

    def _do_thrash(self):
        """
        Continuously loop and thrash the managers.
        """
        self.log('start thrashing')
        self.log('seed: {s}, revive delay: {r}, thrash delay: {t} '
                  'thrash many: {tm}, maintain availability: {ma} '
                  'kill active probability: {kap}, fail probability: {fp}'.format(
                s=self.random_seed, r=self.revive_delay, t=self.thrash_delay,
                tm=self.thrash_many, ma=self.maintain_availability,
                kap=self.kill_active_probability, fp=self.fail_probability,
                ))

        while not self.is_stopped:
            mgrs = _get_mgrs(self.ctx, self.cluster)
            self.manager.wait_for_mgr_available(timeout=60)

            dump = self.manager.get_mgr_dump()
            active = dump['active_name']
            standbys = [s['name'] for s in dump['standbys']]
            self.log('active mgr.{a}, standbys {s}'.format(a=active, s=standbys))

            if self.rng.randrange(100) < self.fail_probability and active:
                self.fail_mgr(active)
                self.manager.wait_for_mgr_available(timeout=60)
                new_dump = self.manager.get_mgr_dump()
                new_active = new_dump['active_name']
                self.log('mgr.{a} failed over to mgr.{n}'.format(
                    a=active, n=new_active))
                self._validate_modules()

                if self.thrash_delay > 0.0:
                    self.sleep_unless_stopped(self.thrash_delay)
                continue

            if self.thrash_many:
                kill_up_to = self.rng.randrange(1, self.max_killable() + 1)
                mgrs_to_kill = []
                if active and self.rng.randrange(100) < self.kill_active_probability:
                    mgrs_to_kill.append(active)
                candidates = [m for m in mgrs if m not in mgrs_to_kill]
                mgrs_to_kill += self.rng.sample(
                    candidates, kill_up_to - len(mgrs_to_kill))
            else:
                if active and self.rng.randrange(100) < self.kill_active_probability:
                    victim = active
                else:
                    victim = self.rng.choice([m for m in mgrs if m != active])
                mgrs_to_kill = [victim]

            self.log('managers to thrash: {m}'.format(m=mgrs_to_kill))

            for mgr in mgrs_to_kill:
                self.kill_mgr(mgr)

            survivors = set(mgrs) - set(mgrs_to_kill)
            if active in mgrs_to_kill and survivors:
                self._wait_for_failover(active)

            if self.maintain_availability:
                self.manager.wait_for_mgr_available(timeout=60)

            self.log('waiting for {delay} secs before reviving managers'.format(
                delay=self.revive_delay))
            # raise_stopped=False: cut the wait short on stop(), but always
            # fall through to revive_mgr() below so a stop mid-wait can't
            # leave a killed mgr dead.
            self.sleep_unless_stopped(self.revive_delay, raise_stopped=False)

            for mgr in mgrs_to_kill:
                self.revive_mgr(mgr)

            self.manager.wait_for_mgr_available(timeout=60)
            self._wait_until_rejoined(mgrs_to_kill, timeout=60)
            self._validate_modules()

            if self.thrash_delay > 0.0:
                self.log('waiting for {delay} secs before continuing thrashing'.format(
                    delay=self.thrash_delay))
                self.sleep_unless_stopped(self.thrash_delay)


@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the manager by thrashing it while another task/workunit
    is running.

    Please refer to MgrThrasher class for further information on the
    available options.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'mgr_thrash task only accepts a dict for configuration'

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    assert len(_get_mgrs(ctx, config['cluster'])) > 1, \
        'mgr_thrash task requires at least 2 manager daemons'

    logger = config.get('logger', 'mgr_thrasher')

    log.info('Beginning mgr_thrash...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )
    thrash_proc = MgrThrasher(ctx,
        manager, config, "MgrThrasher",
        logger=log.getChild(logger))
    thrash_proc.start()
    ctx.ceph[config['cluster']].thrashers.append(thrash_proc)
    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining mgr_thrasher')
        thrash_proc.stop_and_join()
        if thrash_proc.exception is not None:
            raise RuntimeError('error during mgr thrashing')
        if thrash_proc.module_load_delay_ms > 0:
            for opt in ('mgr_module_load_delay', 'mgr_module_load_delay_name'):
                manager.raw_cluster_cmd('config', 'rm', 'mgr', opt)
        if thrash_proc.check_module_option:
            manager.raw_cluster_cmd(
                'config', 'rm', 'mgr', 'mgr/selftest/rwoption6')
        manager.wait_for_mgr_available(timeout=60)
