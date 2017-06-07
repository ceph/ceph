"""
Handle clock skews in monitors.
"""
import logging
import contextlib
import ceph_manager
import time
import gevent
from StringIO import StringIO
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

class ClockSkewCheck:
    """
    Periodically check if there are any clock skews among the monitors in the
    quorum. By default, assume no skews are supposed to exist; that can be
    changed using the 'expect-skew' option. If 'fail-on-skew' is set to false,
    then we will always succeed and only report skews if any are found.

    This class does not spawn a thread. It assumes that, if that is indeed
    wanted, it should be done by a third party (for instance, the task using
    this class). We intend it as such in order to reuse this class if need be.

    This task accepts the following options:

    interval     amount of seconds to wait in-between checks. (default: 30.0)
    max-skew     maximum skew, in seconds, that is considered tolerable before
                 issuing a warning. (default: 0.05)
    expect-skew  'true' or 'false', to indicate whether to expect a skew during
                 the run or not. If 'true', the test will fail if no skew is
                 found, and succeed if a skew is indeed found; if 'false', it's
                 the other way around. (default: false)
    never-fail   Don't fail the run if a skew is detected and we weren't
                 expecting it, or if no skew is detected and we were expecting
                 it. (default: False)

    at-least-once          Runs at least once, even if we are told to stop.
                           (default: True)
    at-least-once-timeout  If we were told to stop but we are attempting to
                           run at least once, timeout after this many seconds.
                           (default: 600)

    Example:
        Expect a skew higher than 0.05 seconds, but only report it without
        failing the teuthology run.

    - mon_clock_skew_check:
        interval: 30
        max-skew: 0.05
        expect_skew: true
        never-fail: true
    """

    def __init__(self, ctx, manager, config, logger):
        self.ctx = ctx
        self.manager = manager

        self.stopping = False
        self.logger = logger
        self.config = config

        if self.config is None:
            self.config = dict()

        self.check_interval = float(self.config.get('interval', 30.0))

        first_mon = teuthology.get_first_mon(ctx, config)
        remote = ctx.cluster.only(first_mon).remotes.keys()[0]
        proc = remote.run(
            args=[
                'sudo',
                'ceph-mon',
                '-i', first_mon[4:],
                '--show-config-value', 'mon_clock_drift_allowed'
                ], stdout=StringIO(), wait=True
                )
        self.max_skew = self.config.get('max-skew', float(proc.stdout.getvalue()))

        self.expect_skew = self.config.get('expect-skew', False)
        self.never_fail = self.config.get('never-fail', False)
        self.at_least_once = self.config.get('at-least-once', True)
        self.at_least_once_timeout = self.config.get('at-least-once-timeout', 600.0)

    def info(self, x):
        """
        locally define logger for info messages
        """
        self.logger.info(x)

    def warn(self, x):
        """
        locally define logger for warnings
        """
        self.logger.warn(x)

    def debug(self, x):
        """
        locally define logger for debug messages
        """
        self.logger.info(x)
        self.logger.debug(x)

    def finish(self):
        """
        Break out of the do_check loop.
        """
        self.stopping = True

    def sleep_interval(self):
        """
        If a sleep interval is set, sleep for that amount of time.
        """
        if self.check_interval > 0.0:
            self.debug('sleeping for {s} seconds'.format(
                s=self.check_interval))
            time.sleep(self.check_interval)

    def print_skews(self, skews):
        """
        Display skew values.
        """
        total = len(skews)
        if total > 0:
            self.info('---------- found {n} skews ----------'.format(n=total))
            for mon_id, values in skews.iteritems():
                self.info('mon.{id}: {v}'.format(id=mon_id, v=values))
            self.info('-------------------------------------')
        else:
            self.info('---------- no skews were found ----------')

    def do_check(self):
        """
        Clock skew checker.  Loops until finish() is called.
        """
        self.info('start checking for clock skews')
        skews = dict()
        ran_once = False
        
        started_on = None

        while not self.stopping or (self.at_least_once and not ran_once):

            if self.at_least_once and not ran_once and self.stopping:
                if started_on is None:
                    self.info('kicking-off timeout (if any)')
                    started_on = time.time()
                elif self.at_least_once_timeout > 0.0:
                    assert time.time() - started_on < self.at_least_once_timeout, \
                        'failed to obtain a timecheck before timeout expired'

            quorum_size = len(teuthology.get_mon_names(self.ctx))
            self.manager.wait_for_mon_quorum_size(quorum_size)

            health = self.manager.get_mon_health(True)
            timechecks = health['timechecks']

            clean_check = False

            if timechecks['round_status'] == 'finished':
                assert (timechecks['round'] % 2) == 0, \
                    'timecheck marked as finished but round ' \
                    'disagrees (r {r})'.format(
                        r=timechecks['round'])
                clean_check = True
            else:
                assert timechecks['round_status'] == 'on-going', \
                        'timecheck status expected \'on-going\' ' \
                        'but found \'{s}\' instead'.format(
                            s=timechecks['round_status'])
                if 'mons' in timechecks.keys() and len(timechecks['mons']) > 1:
                    self.info('round still on-going, but there are available reports')
                else:
                    self.info('no timechecks available just yet')
                    self.sleep_interval()
                    continue

            assert len(timechecks['mons']) > 1, \
                'there are not enough reported timechecks; ' \
                'expected > 1 found {n}'.format(n=len(timechecks['mons']))

            for check in timechecks['mons']:
                mon_skew = float(check['skew'])
                mon_health = check['health']
                mon_id = check['name']
                if abs(mon_skew) > self.max_skew:
                    assert mon_health == 'HEALTH_WARN', \
                        'mon.{id} health is \'{health}\' but skew {s} > max {ms}'.format(
                            id=mon_id,health=mon_health,s=abs(mon_skew),ms=self.max_skew)

                    log_str = 'mon.{id} with skew {s} > max {ms}'.format(
                        id=mon_id,s=abs(mon_skew),ms=self.max_skew)

                    """ add to skew list """
                    details = check['details']
                    skews[mon_id] = {'skew': mon_skew, 'details': details}

                    if self.expect_skew:
                        self.info('expected skew: {str}'.format(str=log_str))
                    else:
                        self.warn('unexpected skew: {str}'.format(str=log_str))

            if clean_check or (self.expect_skew and len(skews) > 0):
                ran_once = True
                self.print_skews(skews)
            self.sleep_interval()

        total = len(skews)
        self.print_skews(skews)

        error_str = ''
        found_error = False

        if self.expect_skew:
            if total == 0:
                error_str = 'We were expecting a skew, but none was found!'
                found_error = True
        else:
            if total > 0:
                error_str = 'We were not expecting a skew, but we did find it!'
                found_error = True

        if found_error:
            self.info(error_str)
            if not self.never_fail:
                assert False, error_str

@contextlib.contextmanager
def task(ctx, config):
    """
    Use clas ClockSkewCheck to check for clock skews on the monitors.
    This task will spawn a thread running ClockSkewCheck's do_check().

    All the configuration will be directly handled by ClockSkewCheck,
    so please refer to the class documentation for further information.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'mon_clock_skew_check task only accepts a dict for configuration'
    log.info('Beginning mon_clock_skew_check...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    skew_check = ClockSkewCheck(ctx,
        manager, config,
        logger=log.getChild('mon_clock_skew_check'))
    skew_check_thread = gevent.spawn(skew_check.do_check)
    try:
        yield
    finally:
        log.info('joining mon_clock_skew_check')
        skew_check.finish()
        skew_check_thread.get()


