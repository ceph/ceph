"""
Thrash mds by simulating failures
"""
import logging
import contextlib
import ceph_manager
import random
import time

from gevent.greenlet import Greenlet
from gevent.event import Event
from teuthology import misc as teuthology

from tasks.cephfs.filesystem import MDSCluster, Filesystem

log = logging.getLogger(__name__)


class MDSThrasher(Greenlet):
    """
    MDSThrasher::

    The MDSThrasher thrashes MDSs during execution of other tasks (workunits, etc).

    The config is optional.  Many of the config parameters are a a maximum value
    to use when selecting a random value from a range.  To always use the maximum
    value, set no_random to true.  The config is a dict containing some or all of:

    max_thrash: [default: 1] the maximum number of active MDSs per FS that will be thrashed at
      any given time.

    max_thrash_delay: [default: 30] maximum number of seconds to delay before
      thrashing again.

    max_replay_thrash_delay: [default: 4] maximum number of seconds to delay while in
      the replay state before thrashing.

    max_revive_delay: [default: 10] maximum number of seconds to delay before
      bringing back a thrashed MDS.

    randomize: [default: true] enables randomization and use the max/min values

    seed: [no default] seed the random number generator

    thrash_in_replay: [default: 0.0] likelihood that the MDS will be thrashed
      during replay.  Value should be between 0.0 and 1.0.

    thrash_max_mds: [default: 0.25] likelihood that the max_mds of the mds
      cluster will be modified to a value [1, current) or (current, starting
      max_mds]. When reduced, randomly selected MDSs other than rank 0 will be
      deactivated to reach the new max_mds.  Value should be between 0.0 and 1.0.

    thrash_weights: allows specific MDSs to be thrashed more/less frequently.
      This option overrides anything specified by max_thrash.  This option is a
      dict containing mds.x: weight pairs.  For example, [mds.a: 0.7, mds.b:
      0.3, mds.c: 0.0].  Each weight is a value from 0.0 to 1.0.  Any MDSs not
      specified will be automatically given a weight of 0.0 (not thrashed).
      For a given MDS, by default the trasher delays for up to
      max_thrash_delay, trashes, waits for the MDS to recover, and iterates.
      If a non-zero weight is specified for an MDS, for each iteration the
      thrasher chooses whether to thrash during that iteration based on a
      random value [0-1] not exceeding the weight of that MDS.

    Examples::


      The following example sets the likelihood that mds.a will be thrashed
      to 80%, mds.b to 20%, and other MDSs will not be thrashed.  It also sets the
      likelihood that an MDS will be thrashed in replay to 40%.
      Thrash weights do not have to sum to 1.

      tasks:
      - ceph:
      - mds_thrash:
          thrash_weights:
            - mds.a: 0.8
            - mds.b: 0.2
          thrash_in_replay: 0.4
      - ceph-fuse:
      - workunit:
          clients:
            all: [suites/fsx.sh]

      The following example disables randomization, and uses the max delay values:

      tasks:
      - ceph:
      - mds_thrash:
          max_thrash_delay: 10
          max_revive_delay: 1
          max_replay_thrash_delay: 4

    """

    def __init__(self, ctx, manager, config, logger, fs, max_mds):
        super(MDSThrasher, self).__init__()

        self.ctx = ctx
        self.manager = manager
        assert self.manager.is_clean()
        self.config = config
        self.logger = logger
        self.fs = fs
        self.max_mds = max_mds

        self.stopping = Event()

        self.randomize = bool(self.config.get('randomize', True))
        self.thrash_max_mds = float(self.config.get('thrash_max_mds', 0.25))
        self.max_thrash = int(self.config.get('max_thrash', 1))
        self.max_thrash_delay = float(self.config.get('thrash_delay', 120.0))
        self.thrash_in_replay = float(self.config.get('thrash_in_replay', False))
        assert self.thrash_in_replay >= 0.0 and self.thrash_in_replay <= 1.0, 'thrash_in_replay ({v}) must be between [0.0, 1.0]'.format(
            v=self.thrash_in_replay)
        self.max_replay_thrash_delay = float(self.config.get('max_replay_thrash_delay', 4.0))
        self.max_revive_delay = float(self.config.get('max_revive_delay', 10.0))

    def _run(self):
        try:
            self.do_thrash()
        except:
            # Log exceptions here so we get the full backtrace (it's lost
            # by the time someone does a .get() on this greenlet)
            self.logger.exception("Exception in do_thrash:")
            raise

    def log(self, x):
        """Write data to logger assigned to this MDThrasher"""
        self.logger.info(x)

    def stop(self):
        self.stopping.set()

    def kill_mds(self, mds):
        if self.config.get('powercycle'):
            (remote,) = (self.ctx.cluster.only('mds.{m}'.format(m=mds)).
                         remotes.iterkeys())
            self.log('kill_mds on mds.{m} doing powercycle of {s}'.
                     format(m=mds, s=remote.name))
            self._assert_ipmi(remote)
            remote.console.power_off()
        else:
            self.ctx.daemons.get_daemon('mds', mds).stop()

    @staticmethod
    def _assert_ipmi(remote):
        assert remote.console.has_ipmi_credentials, (
            "powercycling requested but RemoteConsole is not "
            "initialized.  Check ipmi config.")

    def revive_mds(self, mds, standby_for_rank=None):
        """
        Revive mds -- do an ipmpi powercycle (if indicated by the config)
        and then restart (using --hot-standby if specified.
        """
        if self.config.get('powercycle'):
            (remote,) = (self.ctx.cluster.only('mds.{m}'.format(m=mds)).
                         remotes.iterkeys())
            self.log('revive_mds on mds.{m} doing powercycle of {s}'.
                     format(m=mds, s=remote.name))
            self._assert_ipmi(remote)
            remote.console.power_on()
            self.manager.make_admin_daemon_dir(self.ctx, remote)
        args = []
        if standby_for_rank:
            args.extend(['--hot-standby', standby_for_rank])
        self.ctx.daemons.get_daemon('mds', mds).restart(*args)

    def wait_for_stable(self, rank = None, gid = None):
        self.log('waiting for mds cluster to stabilize...')
        status = self.fs.status()
        itercount = 0
        while True:
            max_mds = status.get_fsmap(self.fs.id)['mdsmap']['max_mds']
            if rank is not None:
                try:
                    info = status.get_rank(self.fs.id, rank)
                    if info['gid'] != gid:
                        self.log('mds.{name} has gained rank={rank}, replacing gid={gid}'.format(name = info['name'], rank = rank, gid = gid))
                        return status, info['name']
                except:
                    pass # no rank present
            else:
                ranks = filter(lambda info: "up:active" == info['state'] and "laggy_since" not in info, list(status.get_ranks(self.fs.id)))
                count = len(ranks)
                if count >= max_mds:
                    self.log('mds cluster has {count} alive and active, now stable!'.format(count = count))
                    return status, None
            itercount = itercount + 1
            if itercount > 10:
                self.log('mds map: {status}'.format(status=self.fs.status()))
            time.sleep(2)
            status = self.fs.status()

    def do_thrash(self):
        """
        Perform the random thrashing action
        """

        self.log('starting mds_do_thrash for fs {fs}'.format(fs = self.fs.name))
        stats = {
            "max_mds": 0,
            "deactivate": 0,
            "kill": 0,
        }

        while not self.stopping.is_set():
            delay = self.max_thrash_delay
            if self.randomize:
                delay = random.randrange(0.0, self.max_thrash_delay)

            if delay > 0.0:
                self.log('waiting for {delay} secs before thrashing'.format(delay=delay))
                self.stopping.wait(delay)
                if self.stopping.is_set():
                    continue

            status = self.fs.status()

            if random.randrange(0.0, 1.0) <= self.thrash_max_mds:
                max_mds = status.get_fsmap(self.fs.id)['mdsmap']['max_mds']
                options = range(1, max_mds)+range(max_mds+1, self.max_mds+1)
                if len(options) > 0:
                    sample = random.sample(options, 1)
                    new_max_mds = sample[0]
                    self.log('thrashing max_mds: %d -> %d' % (max_mds, new_max_mds))
                    self.fs.set_max_mds(new_max_mds)
                    stats['max_mds'] += 1

                    # Now randomly deactivate mds if we shrank
                    for rank in random.sample(range(1, max_mds), max(0, max_mds-new_max_mds)):
                        self.fs.deactivate(rank)
                        stats['deactivate'] += 1

                    status = self.wait_for_stable()[0]

            count = 0
            for info in status.get_ranks(self.fs.id):
                name = info['name']
                label = 'mds.' + name
                rank = info['rank']
                gid = info['gid']

                # if thrash_weights isn't specified and we've reached max_thrash,
                # we're done
                count = count + 1
                if 'thrash_weights' not in self.config and count > self.max_thrash:
                    break

                weight = 1.0
                if 'thrash_weights' in self.config:
                    weight = self.config['thrash_weights'].get(label, '0.0')
                skip = random.randrange(0.0, 1.0)
                if weight <= skip:
                    self.log('skipping thrash iteration with skip ({skip}) > weight ({weight})'.format(skip=skip, weight=weight))
                    continue

                self.log('kill {label} (rank={rank})'.format(label=label, rank=rank))
                self.kill_mds(name)
                stats['kill'] += 1

                # wait for mon to report killed mds as crashed
                last_laggy_since = None
                itercount = 0
                while True:
                    status = self.fs.status()
                    info = status.get_mds(name)
                    if not info:
                        break
                    if 'laggy_since' in info:
                        last_laggy_since = info['laggy_since']
                        break
                    if any([(f == name) for f in status.get_fsmap(self.fs.id)['mdsmap']['failed']]):
                        break
                    self.log(
                        'waiting till mds map indicates {label} is laggy/crashed, in failed state, or {label} is removed from mdsmap'.format(
                            label=label))
                    itercount = itercount + 1
                    if itercount > 10:
                        self.log('mds map: {status}'.format(status=status))
                    time.sleep(2)

                if last_laggy_since:
                    self.log(
                        '{label} reported laggy/crashed since: {since}'.format(label=label, since=last_laggy_since))
                else:
                    self.log('{label} down, removed from mdsmap'.format(label=label, since=last_laggy_since))

                # wait for a standby mds to takeover and become active
                status, takeover_mds = self.wait_for_stable(rank, gid)
                self.log('New active mds is mds.{_id}'.format(_id=takeover_mds))

                # wait for a while before restarting old active to become new
                # standby
                delay = self.max_revive_delay
                if self.randomize:
                    delay = random.randrange(0.0, self.max_revive_delay)

                self.log('waiting for {delay} secs before reviving {label}'.format(
                    delay=delay, label=label))
                time.sleep(delay)

                self.log('reviving {label}'.format(label=label))
                self.revive_mds(name)

                while True:
                    status = self.fs.status()
                    info = status.get_mds(name)
                    if info and info['state'] in ('up:standby', 'up:standby-replay'):
                        self.log('{label} reported in {state} state'.format(label=label, state=info['state']))
                        break
                    self.log(
                        'waiting till mds map indicates {label} is in standby or standby-replay'.format(label=label))
                    time.sleep(2)

        for stat in stats:
            self.log("stat['{key}'] = {value}".format(key = stat, value = stats[stat]))

             # don't do replay thrashing right now
#            for info in status.get_replays(self.fs.id):
#                # this might race with replay -> active transition...
#                if status['state'] == 'up:replay' and random.randrange(0.0, 1.0) < self.thrash_in_replay:
#                    delay = self.max_replay_thrash_delay
#                    if self.randomize:
#                        delay = random.randrange(0.0, self.max_replay_thrash_delay)
#                time.sleep(delay)
#                self.log('kill replaying mds.{id}'.format(id=self.to_kill))
#                self.kill_mds(self.to_kill)
#
#                delay = self.max_revive_delay
#                if self.randomize:
#                    delay = random.randrange(0.0, self.max_revive_delay)
#
#                self.log('waiting for {delay} secs before reviving mds.{id}'.format(
#                    delay=delay, id=self.to_kill))
#                time.sleep(delay)
#
#                self.log('revive mds.{id}'.format(id=self.to_kill))
#                self.revive_mds(self.to_kill)


@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the mds by thrashing while another task/workunit
    is running.

    Please refer to MDSThrasher class for further information on the
    available options.
    """

    mds_cluster = MDSCluster(ctx)

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'mds_thrash task only accepts a dict for configuration'
    mdslist = list(teuthology.all_roles_of_type(ctx.cluster, 'mds'))
    assert len(mdslist) > 1, \
        'mds_thrash task requires at least 2 metadata servers'

    # choose random seed
    if 'seed' in config:
        seed = int(config['seed'])
    else:
        seed = int(time.time())
    log.info('mds thrasher using random seed: {seed}'.format(seed=seed))
    random.seed(seed)

    (first,) = ctx.cluster.only('mds.{_id}'.format(_id=mdslist[0])).remotes.iterkeys()
    manager = ceph_manager.CephManager(
        first, ctx=ctx, logger=log.getChild('ceph_manager'),
    )

    # make sure everyone is in active, standby, or standby-replay
    log.info('Wait for all MDSs to reach steady state...')
    status = mds_cluster.status()
    while True:
        steady = True
        for info in status.get_all():
            state = info['state']
            if state not in ('up:active', 'up:standby', 'up:standby-replay'):
                steady = False
                break
        if steady:
            break
        time.sleep(2)
        status = mds_cluster.status()
    log.info('Ready to start thrashing')

    manager.wait_for_clean()
    thrashers = {}
    for fs in status.get_filesystems():
        name = fs['mdsmap']['fs_name']
        log.info('Running thrasher against FS {f}'.format(f = name))
        thrasher = MDSThrasher(
            ctx, manager, config,
            log.getChild('fs.[{f}]'.format(f = name)),
            Filesystem(ctx, fs['id']), fs['mdsmap']['max_mds']
            )
        thrasher.start()
        thrashers[name] = thrasher

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining mds_thrashers')
        for name in thrashers:
            log.info('join thrasher mds_thrasher.fs.[{f}]'.format(f=name))
            thrashers[name].stop()
            thrashers[name].get()  # Raise any exception from _run()
            thrashers[name].join()
        log.info('done joining')
