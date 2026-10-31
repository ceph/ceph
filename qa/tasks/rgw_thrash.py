"""
Kill and restart radosgw, and kill the S3 clients, while other tasks run.
"""
import contextlib
import logging
import random
import signal
import socket
import time

from teuthology import misc as teuthology
from teuthology.exceptions import CommandFailedError, ConfigError
from teuthology.orchestra import run
from tasks.thrasher import ThrasherGreenlet
from tasks.util.rgw import wait_for_radosgw

log = logging.getLogger(__name__)

# the out-of-band S3 test clients, as pkill -f matches them; the brackets
# keep the pattern from matching the command line that holds it
CLIENT_PATTERN = '[c]eph_test_rgw_ofi_'


def pick(actions, value):
    """
    The action that value, from 0 to the sum of the weights, falls on, of
    a list of (action, weight)
    """
    for action, weight in actions:
        if value < weight:
            return action
        value -= weight
    return None


class RGWThrasher(ThrasherGreenlet):
    """
    Kills a radosgw at random with SIGKILL, waits up to max_revive_delay
    seconds, starts it again and waits until it answers, or kills the
    clients on the client hosts with SIGKILL, and waits from
    min_thrash_delay to max_thrash_delay seconds before the next one.
    """

    def __init__(self, ctx, config, cluster, daemons):
        super(RGWThrasher, self).__init__()
        self.ctx = ctx
        self.config = config
        self.cluster = cluster
        self.daemons = daemons
        self.logger = log
        self.name = 'thrasher.rgw.[{cluster}]'.format(cluster=cluster)
        self.min_thrash_delay = float(config.get('min_thrash_delay', 30))
        self.max_thrash_delay = float(config.get('max_thrash_delay', 90))
        self.max_revive_delay = float(config.get('max_revive_delay', 10))
        self.chance_kill_rgw = float(config.get('chance_kill_rgw', 1))
        self.chance_kill_clients = float(config.get('chance_kill_clients', 1))
        self.client_pattern = config.get('client_pattern', CLIENT_PATTERN)
        roles = config.get('kill_clients_on')
        if roles is None:
            # the rgw task names each gateway after its client role
            roles = [d.id_ for d in daemons]
        self.client_remotes = set()
        for role in roles:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            self.client_remotes.add(remote)
        self.stats = {'kill_rgw': 0, 'kill_clients': 0}

    def log(self, x):
        self.logger.info(x)

    def _run(self):
        try:
            self.do_thrash()
        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # the DaemonWatchdog sees the error and ends the job

    def kill_rgw(self):
        daemon = random.choice(self.daemons)
        role = daemon.id_
        self.log('kill {role}'.format(role=role))
        try:
            daemon.signal(signal.SIGKILL)
        except socket.error:
            pass
        try:
            run.wait([daemon.proc], timeout=600)
        except CommandFailedError:
            # daemon-helper reports the kill as a failure
            pass
        finally:
            daemon.reset()
        self.stats['kill_rgw'] += 1
        delay = random.uniform(0, self.max_revive_delay)
        self.log('reviving {role} in {delay:.1f} s'.format(role=role,
                                                           delay=delay))
        self.sleep_unless_stopped(delay, raise_stopped=False)
        daemon.start()
        endpoint = self.ctx.rgw.role_endpoints.get(role)
        if endpoint is not None:
            (remote,) = self.ctx.cluster.only(role).remotes.keys()
            wait_for_radosgw(endpoint.url(), remote)
        self.log('{role} is back'.format(role=role))

    def kill_clients(self):
        for remote in sorted(self.client_remotes, key=lambda r: r.name):
            self.log('kill the clients on {host}'.format(host=remote.shortname))
            remote.run(args=['sudo', 'pkill', '-KILL', '-f',
                             self.client_pattern], check_status=False)
        self.stats['kill_clients'] += 1

    def do_thrash(self):
        self.log('starting rgw thrash for cluster {cluster}'.format(
            cluster=self.cluster))
        actions = [(self.kill_rgw, self.chance_kill_rgw),
                   (self.kill_clients, self.chance_kill_clients)]
        total = sum(weight for _, weight in actions)
        while not self.is_stopped:
            delay = random.uniform(self.min_thrash_delay,
                                   self.max_thrash_delay)
            self.log('waiting {delay:.1f} s'.format(delay=delay))
            if not self.sleep_unless_stopped(delay, raise_stopped=False):
                break
            action = pick(actions, random.uniform(0, total))
            if action is not None:
                action()
        for stat, value in self.stats.items():
            self.log("stat['{key}'] = {value}".format(key=stat, value=value))


@contextlib.contextmanager
def task(ctx, config):
    """
    Kill and restart the gateways that the rgw task started, and kill the
    out-of-band S3 clients, while the tasks after this one run. Put it
    after the rgw task. A radosgw that the thrasher killed answers again
    before the next kill.

        tasks:
        - rgw: [client.0]
        - rgw_thrash:
            # the gateways to kill (default: every one)
            clients: [client.0]
            # the roles whose hosts get their clients killed (default:
            # those of the gateways)
            kill_clients_on: [client.0]
            # what pkill -f matches (default '[c]eph_test_rgw_ofi_')
            client_pattern: '[c]eph_test_rgw_ofi_'
            min_thrash_delay: 30
            max_thrash_delay: 90
            # the longest wait before a killed radosgw starts again
            max_revive_delay: 10
            # the weights of the two actions (default 1 each; 0 for never)
            chance_kill_rgw: 1
            chance_kill_clients: 1
            seed: 1
        - workunit:
            clients:
              client.0: [rgw/run-s3-oob-soak.sh]

    A workload must tolerate both: the soak workunit starts a killed
    client again, and takes GETs that fail while the gateway is down.
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ConfigError('rgw_thrash takes a dict')
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('rgw_thrash', {}))
    cluster = config.get('cluster', 'ceph')
    daemons = list(ctx.daemons.iter_daemons_of_role('rgw', cluster))
    clients = config.get('clients')
    if clients:
        daemons = [d for d in daemons if d.id_ in clients]
    if not daemons:
        raise ConfigError('rgw_thrash finds no radosgw to thrash')

    seed = int(config.get('seed', time.time()))
    log.info('rgw_thrash using random seed: {seed}'.format(seed=seed))
    random.seed(seed)

    thrasher = RGWThrasher(ctx, config, cluster, daemons)
    thrasher.start()
    ctx.ceph[cluster].thrashers.append(thrasher)
    try:
        yield
    finally:
        log.info('joining rgw_thrash')
        thrasher.stop_and_join()
        if thrasher.exception is not None:
            raise RuntimeError('error during rgw thrashing')
        log.info('done joining')
