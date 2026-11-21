"""
Thrash the nfs-ganesha daemon(s) of an nfs cluster deployed by cephadm
"""

import contextlib
import json
import logging
import random
import time
from io import StringIO

from teuthology import misc
from teuthology.contextutil import safe_while
from tasks.thrasher import ThrasherGreenlet

log = logging.getLogger(__name__)


class GaneshaDaemon(object):
    def __init__(self, fsid, daemon, remote):
        self.name = daemon['daemon_name']
        self.remote = remote
        self.unit = f'ceph-{fsid}@{self.name}.service'
        self.container = f'ceph-{fsid}-{self.name}'.replace('.', '-')

    def _systemctl(self, *args, check_status=True):
        return self.remote.run(args=['sudo', 'systemctl', *args, self.unit],
                               stdout=StringIO(), check_status=check_status)

    def kill(self):
        # SIGKILL: the libcephfs session(s) and the NFS client state are left
        # behind for the next instance to reclaim
        self.remote.run(args=[
            'sudo', 'bash', '-c',
            'ENGINE=$(command -v podman || command -v docker) && '
            f'"$ENGINE" kill --signal KILL {self.container}'])

    def stop(self):
        # after a kill this cancels the automatic restart of the unit
        self._systemctl('stop')

    def start(self):
        # repeated kills trip the unit's start rate limit
        self._systemctl('reset-failed', check_status=False)
        self._systemctl('start')

    def is_active(self):
        proc = self._systemctl('is-active', check_status=False)
        return proc.stdout.getvalue().strip() == 'active'

    def is_listening(self, port):
        proc = self.remote.run(args=['sudo', 'ss', '-Hltn', f'sport = :{port}'],
                               stdout=StringIO())
        return bool(proc.stdout.getvalue().strip())


class GaneshaThrasher(ThrasherGreenlet):
    """
    GaneshaThrasher::

    Restarts the ganesha daemon(s) of an nfs cluster while other tasks
    (workunits, etc.) run against an NFS mount of it. Each round picks
    a daemon, stops it, either cleanly (SIGTERM through its systemd unit)
    or abruptly (SIGKILL to its container), keeps it down for a while
    and starts it again. The new instance has to reclaim its libcephfs
    session(s) from the MDS and the NFS clients reclaim their state from
    it during the grace period.

      - ganesha_thrash:
          cluster_id: <nfs cluster id>   # required
          actions: [kill, stop]          # chosen from at random
          min_thrash_delay: 15           # seconds between rounds
          max_thrash_delay: 45
          max_revive_delay: 30           # seconds a daemon is kept down
          revive_timeout: 300            # seconds for it to come back
          randomize: true                # false: always use the max delays
          seed: <int>                    # random seed

    Overrides can be set through overrides.ganesha_thrash.
    """

    ACTIONS = ('kill', 'stop')

    def __init__(self, ctx, config, cluster, daemons, port):
        super(GaneshaThrasher, self).__init__()

        self.ctx = ctx
        self.config = config
        self.cluster = cluster
        self.daemons = daemons
        self.port = port

        self.logger = log
        self.name = f'thrasher.ganesha.[{config["cluster_id"]}]'

        self.actions = list(config.get('actions', self.ACTIONS))
        for action in self.actions:
            assert action in self.ACTIONS, f'unknown ganesha_thrash action {action}'
        self.randomize = bool(config.get('randomize', True))
        self.min_thrash_delay = float(config.get('min_thrash_delay', 15))
        self.max_thrash_delay = float(config.get('max_thrash_delay', 45))
        self.max_revive_delay = float(config.get('max_revive_delay', 30))
        self.revive_timeout = int(config.get('revive_timeout', 300))
        self.stats = {action: 0 for action in self.ACTIONS}

    def log(self, x):
        self.logger.info(x)

    def _run(self):
        try:
            self.do_thrash()
        except self.Stopped:
            # stopped while waiting for the next round: not an error
            pass
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # Allow successful completion so gevent doesn't see an exception.
            # The DaemonWatchdog will observe the error and tear down the test.

    def _delay(self, low, high):
        return random.uniform(low, high) if self.randomize else high

    def _wait_for_daemon(self, daemon):
        with safe_while(sleep=5, tries=self.revive_timeout // 5,
                        action=f'wait for {daemon.name} to come back') as proceed:
            while proceed():
                if daemon.is_active() and daemon.is_listening(self.port):
                    return

    def _thrash_one(self, daemon, action):
        start = time.time()
        self.log(f'{action} {daemon.name}')
        if action == 'kill':
            daemon.kill()
        daemon.stop()
        self.stats[action] += 1
        try:
            delay = self._delay(0.0, self.max_revive_delay)
            self.log(f'waiting for {delay:.1f} secs before reviving {daemon.name}')
            # always revive, so that the workload can finish
            self.sleep_unless_stopped(delay, raise_stopped=False)
        finally:
            self.log(f'reviving {daemon.name}')
            daemon.start()
            self._wait_for_daemon(daemon)
        self.log(f'{daemon.name} is back after {time.time() - start:.1f} secs')

    def do_thrash(self):
        self.log(f'starting thrash of {[d.name for d in self.daemons]}')
        try:
            while not self.is_stopped:
                delay = self._delay(self.min_thrash_delay, self.max_thrash_delay)
                self.log(f'waiting for {delay:.1f} secs before thrashing')
                self.sleep_unless_stopped(delay)

                daemon = random.choice(self.daemons)
                action = random.choice(self.actions)
                self._thrash_one(daemon, action)
        finally:
            for action, count in self.stats.items():
                self.log(f"stat['{action}'] = {count}")


def _nfs_daemons(ctx, mon0_remote, cluster_id):
    proc = mon0_remote.run(args=['ceph', 'orch', 'ps', '--service_name',
                                 f'nfs.{cluster_id}', '--refresh', '--format', 'json'],
                           stdout=StringIO())
    return json.loads(proc.stdout.getvalue())


def _nfs_port(mon0_remote, cluster_id):
    proc = mon0_remote.run(args=['ceph', 'nfs', 'cluster', 'info', cluster_id],
                           stdout=StringIO())
    info = json.loads(proc.stdout.getvalue())
    return info[cluster_id]['backend'][0]['port']


def _host_remote(ctx, hostname):
    for remote in ctx.cluster.remotes.keys():
        if remote.shortname == hostname or remote.hostname == hostname:
            return remote
    raise RuntimeError(f'no remote for host {hostname}')


@contextlib.contextmanager
def task(ctx, config):
    """
    Thrash the ganesha daemon(s) of an nfs cluster while the nested tasks
    run. See GaneshaThrasher for the options.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'ganesha_thrash task only accepts a dict for configuration'
    overrides = ctx.config.get('overrides', {}).get('ganesha_thrash', {})
    misc.deep_merge(config, overrides)
    log.info(f'ganesha_thrash config is {config}')

    cluster = config.get('cluster', 'ceph')
    cluster_id = config['cluster_id']
    fsid = ctx.ceph[cluster].fsid

    first_mon = misc.get_first_mon(ctx, None, cluster)
    (mon0_remote,) = ctx.cluster.only(first_mon).remotes.keys()

    daemons = [GaneshaDaemon(fsid, d, _host_remote(ctx, d['hostname']))
               for d in _nfs_daemons(ctx, mon0_remote, cluster_id)]
    assert daemons, f'ganesha_thrash found no daemons for nfs cluster {cluster_id}'
    port = _nfs_port(mon0_remote, cluster_id)

    if 'seed' in config:
        seed = int(config['seed'])
    else:
        seed = int(time.time())
    log.info(f'ganesha_thrash using random seed: {seed}')
    random.seed(seed)

    thrasher = GaneshaThrasher(ctx, config, cluster, daemons, port)
    thrasher.start()
    ctx.ceph[cluster].thrashers.append(thrasher)

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining ganesha_thrash')
        thrasher.stop_and_join()
        if thrasher.exception is not None:
            raise RuntimeError('error during ganesha thrashing') from thrasher.exception
        log.info('done joining')
