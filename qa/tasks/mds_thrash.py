"""
Thrash mds by simulating failures
"""
import logging
import contextlib
import itertools
import random
import time

from gevent import sleep
from gevent.greenlet import Greenlet
from gevent.event import Event
from teuthology import misc as teuthology

from tasks import ceph_manager
from tasks.cephfs.filesystem import MDSCluster, Filesystem
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)

class MDSThrasher(Thrasher, Greenlet):
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

    thrash_max_mds: [default: 0.05] likelihood that the max_mds of the mds
      cluster will be modified to a value [1, current) or (current, starting
      max_mds]. Value should be between 0.0 and 1.0.

    thrash_while_stopping: [default: false] thrash an MDS while there
      are MDS in up:stopping (because max_mds was changed and some
      MDS were deactivated).

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

    def __init__(self, ctx, manager, config, fs, max_mds):
        super(MDSThrasher, self).__init__()

        self.config = config
        self.ctx = ctx
        self.logger = log.getChild('fs.[{f}]'.format(f = fs.name))
        self.fs = fs
        self.manager = manager
        self.max_mds = max_mds
        self.name = 'thrasher.fs.[{f}]'.format(f = fs.name)
        self.stopping = Event()

        self.randomize = bool(self.config.get('randomize', True))
        self.thrash_max_mds = float(self.config.get('thrash_max_mds', 0.05))
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
        except Exception as e:
            # Log exceptions here so we get the full backtrace (gevent loses them).
            # Also allow successful completion as gevent exception handling is a broken mess:
            #
            # 2017-02-03T14:34:01.259 CRITICAL:root:  File "gevent.libev.corecext.pyx", line 367, in gevent.libev.corecext.loop.handle_error (src/gevent/libev/gevent.corecext.c:5051)
            #   File "/home/teuthworker/src/git.ceph.com_git_teuthology_master/virtualenv/local/lib/python2.7/site-packages/gevent/hub.py", line 558, in handle_error
            #     self.print_exception(context, type, value, tb)
            #   File "/home/teuthworker/src/git.ceph.com_git_teuthology_master/virtualenv/local/lib/python2.7/site-packages/gevent/hub.py", line 605, in print_exception
            #     traceback.print_exception(type, value, tb, file=errstream)
            #   File "/usr/lib/python2.7/traceback.py", line 124, in print_exception
            #     _print(file, 'Traceback (most recent call last):')
            #   File "/usr/lib/python2.7/traceback.py", line 13, in _print
            #     file.write(str+terminator)
            # 2017-02-03T14:34:01.261 CRITICAL:root:IOError
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...

    def log(self, x):
        """Write data to the logger assigned to MDSThrasher"""
        self.logger.info(x)

    def stop(self):
        self.stopping.set()

    def kill_mds(self, mds):
        if self.config.get('powercycle'):
            (remote,) = (self.ctx.cluster.only('mds.{m}'.format(m=mds)).
                         remotes.keys())
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

    def revive_mds(self, mds):
        """
        Revive mds -- do an ipmpi powercycle (if indicated by the config)
        and then restart.
        """
        if self.config.get('powercycle'):
            (remote,) = (self.ctx.cluster.only('mds.{m}'.format(m=mds)).
                         remotes.keys())
            self.log('revive_mds on mds.{m} doing powercycle of {s}'.
                     format(m=mds, s=remote.name))
            self._assert_ipmi(remote)
            remote.console.power_on()
            self.manager.make_admin_daemon_dir(self.ctx, remote)
        args = []
        self.ctx.daemons.get_daemon('mds', mds).restart(*args)

    def wait_for_stable(self, rank = None, gid = None):
        self.log('waiting for mds cluster to stabilize...')
        for itercount in itertools.count():
            status = self.fs.status()
            max_mds = status.get_fsmap(self.fs.id)['mdsmap']['max_mds']
            ranks = list(status.get_ranks(self.fs.id))
            stopping = sum(1 for _ in ranks if "up:stopping" == _['state'])
            actives = sum(1 for _ in ranks
                          if "up:active" == _['state'] and "laggy_since" not in _)

            if not bool(self.config.get('thrash_while_stopping', False)) and stopping > 0:
                if itercount % 5 == 0:
                    self.log('cluster is considered unstable while MDS are in up:stopping (!thrash_while_stopping)')
            else:
                if rank is not None:
                    try:
                        info = status.get_rank(self.fs.id, rank)
                        if info['gid'] != gid and "up:active" == info['state']:
                            self.log('mds.{name} has gained rank={rank}, replacing gid={gid}'.format(name = info['name'], rank = rank, gid = gid))
                            return status
                    except:
                        pass # no rank present
                    if actives >= max_mds:
                        # no replacement can occur!
                        self.log("cluster has {actives} actives (max_mds is {max_mds}), no MDS can replace rank {rank}".format(
                            actives=actives, max_mds=max_mds, rank=rank))
                        return status
                else:
                    if actives == max_mds:
                        self.log('mds cluster has {count} alive and active, now stable!'.format(count = actives))
                        return status, None
            if itercount > 300/2: # 5 minutes
                 raise RuntimeError('timeout waiting for cluster to stabilize')
            elif itercount % 5 == 0:
                self.log('mds map: {status}'.format(status=status))
            else:
                self.log('no change')
            sleep(2)

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

            if random.random() <= self.thrash_max_mds:
                max_mds = status.get_fsmap(self.fs.id)['mdsmap']['max_mds']
                options = [i for i in range(1, self.max_mds + 1) if i != max_mds]
                if len(options) > 0:
                    new_max_mds = random.choice(options)
                    self.log('thrashing max_mds: %d -> %d' % (max_mds, new_max_mds))
                    self.fs.set_max_mds(new_max_mds)
                    stats['max_mds'] += 1
                    self.wait_for_stable()

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
                    sleep(2)

                if last_laggy_since:
                    self.log(
                        '{label} reported laggy/crashed since: {since}'.format(label=label, since=last_laggy_since))
                else:
                    self.log('{label} down, removed from mdsmap'.format(label=label))

                # wait for a standby mds to takeover and become active
                status = self.wait_for_stable(rank, gid)

                # wait for a while before restarting old active to become new
                # standby
                delay = self.max_revive_delay
                if self.randomize:
                    delay = random.randrange(0.0, self.max_revive_delay)

                self.log('waiting for {delay} secs before reviving {label}'.format(
                    delay=delay, label=label))
                sleep(delay)

                self.log('reviving {label}'.format(label=label))
                self.revive_mds(name)

                for itercount in itertools.count():
                    if itercount > 300/2: # 5 minutes
                        raise RuntimeError('timeout waiting for MDS to revive')
                    status = self.fs.status()
                    info = status.get_mds(name)
                    if info and info['state'] in ('up:standby', 'up:standby-replay', 'up:active'):
                        self.log('{label} reported in {state} state'.format(label=label, state=info['state']))
                        break
                    self.log(
                        'waiting till mds map indicates {label} is in active, standby or standby-replay'.format(label=label))
                    sleep(2)

        for stat in stats:
            self.log("stat['{key}'] = {value}".format(key = stat, value = stats[stat]))

             # don't do replay thrashing right now
#            for info in status.get_replays(self.fs.id):
#                # this might race with replay -> active transition...
#                if status['state'] == 'up:replay' and random.randrange(0.0, 1.0) < self.thrash_in_replay:
#                    delay = self.max_replay_thrash_delay
#                    if self.randomize:
#                        delay = random.randrange(0.0, self.max_replay_thrash_delay)
#                sleep(delay)
#                self.log('kill replaying mds.{id}'.format(id=self.to_kill))
#                self.kill_mds(self.to_kill)
#
#                delay = self.max_revive_delay
#                if self.randomize:
#                    delay = random.randrange(0.0, self.max_revive_delay)
#
#                self.log('waiting for {delay} secs before reviving mds.{id}'.format(
#                    delay=delay, id=self.to_kill))
#                sleep(delay)
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

    (first,) = ctx.cluster.only('mds.{_id}'.format(_id=mdslist[0])).remotes.keys()
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
        sleep(2)
        status = mds_cluster.status()
    log.info('Ready to start thrashing')

    manager.wait_for_clean()
    assert manager.is_clean()

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    for fs in status.get_filesystems():
        thrasher = MDSThrasher(ctx, manager, config, Filesystem(ctx, fs['id']), fs['mdsmap']['max_mds'])
        thrasher.start()
        ctx.ceph[config['cluster']].thrashers.append(thrasher)

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining mds_thrasher')
        thrasher.stop()
        if thrasher.exception is not None:
            raise RuntimeError('error during thrashing')
        thrasher.join()
        log.info('done joining')
