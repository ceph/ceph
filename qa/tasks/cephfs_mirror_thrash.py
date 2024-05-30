"""
Task for thrashing cephfs-mirror daemons
"""

import contextlib
import logging
import random
import signal
import socket
import time

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run
from tasks.thrasher import ThrasherGreenlet

log = logging.getLogger(__name__)


class CephFSMirrorThrasher(ThrasherGreenlet):
    """
    CephFSMirrorThrasher::

    The CephFSMirrorThrasher thrashes cephfs-mirror daemons during execution of other
    tasks (workunits, etc).

    The config is optional.  Many of the config parameters are a maximum value
    to use when selecting a random value from a range.  The config is a dict
    containing some or all of:

    cluster: [default: ceph] cluster to thrash

    max_thrash: [default: 1] the maximum number of active cephfs-mirror daemons per
      cluster will be thrashed at any given time.

    min_thrash_delay: [default: 60] minimum number of seconds to delay before
      thrashing again.

    max_thrash_delay: [default: 120] maximum number of seconds to delay before
      thrashing again.

    max_revive_delay: [default: 10] maximum number of seconds to delay before
      bringing back a thrashed cephfs-mirror daemon.

    randomize: [default: true] enables randomization and use the max/min values

    seed: [no default] seed the random number generator

    Examples::

      The following example disables randomization, and uses the max delay
      values:

      tasks:
      - ceph:
      - cephfs_mirror_thrash:
          randomize: False
          max_thrash_delay: 10
    """

    def __init__(self, ctx, config, cluster, daemons):
        super(CephFSMirrorThrasher, self).__init__()

        self.ctx = ctx
        self.config = config
        self.cluster = cluster
        self.daemons = daemons

        self.logger = log
        self.name = 'thrasher.cephfs_mirror.[{cluster}]'.format(cluster = cluster)

        self.randomize = bool(self.config.get('randomize', True))
        self.max_thrash = int(self.config.get('max_thrash', 1))
        self.min_thrash_delay = float(self.config.get('min_thrash_delay', 5.0))
        self.max_thrash_delay = float(self.config.get('max_thrash_delay', 10))
        self.max_revive_delay = float(self.config.get('max_revive_delay', 15.0))

    def _run(self):
        try:
            self.do_thrash()
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # Allow successful completion so gevent doesn't see an exception.
            # The DaemonWatchdog will observe the error and tear down the test.

    def log(self, x):
        """Write data to logger assigned to this CephFSMirrorThrasher"""
        self.logger.info(x)

    def do_thrash(self):
        """
        Perform the random thrashing action
        """

        self.log('starting thrash for cluster {cluster}'.format(cluster=self.cluster))
        stats = {
            "kill": 0,
        }

        while not self.is_stopped:
            delay = self.max_thrash_delay
            if self.randomize:
                delay = random.randrange(self.min_thrash_delay, self.max_thrash_delay)

            if delay > 0.0:
                self.log('waiting for {delay} secs before thrashing'.format(delay=delay))
                self.sleep_unless_stopped(delay)

            killed_daemons = []

            weight = 1.0 / len(self.daemons)
            count = 0
            for daemon in self.daemons:
                skip = random.uniform(0.0, 1.0)
                if weight <= skip:
                    self.log('skipping daemon {label} with skip ({skip}) > weight ({weight})'.format(
                        label=daemon.id_, skip=skip, weight=weight))
                    continue

                self.log('kill {label}'.format(label=daemon.id_))
                try:
                    daemon.signal(signal.SIGTERM)
                except Exception as e:
                    self.log(f'exception when stopping mirror daemon: {e}')
                else:
                    killed_daemons.append(daemon)
                    stats['kill'] += 1

                # if we've reached max_thrash, we're done
                count += 1
                if count >= self.max_thrash:
                    break

            if killed_daemons:
                # wait for a while before restarting
                delay = self.max_revive_delay
                if self.randomize:
                    delay = random.randrange(0.0, self.max_revive_delay)

                self.log('waiting for {delay} secs before reviving daemons'.format(delay=delay))
                self.sleep_unless_stopped(delay)

                for daemon in killed_daemons:
                    self.log('waiting for {label}'.format(label=daemon.id_))
                    try:
                        run.wait([daemon.proc], timeout=600)
                    except CommandFailedError:
                        pass
                    except:
                        self.log('Failed to stop {label}'.format(label=daemon.id_))

                        try:
                            # try to capture a core dump
                            daemon.signal(signal.SIGABRT)
                        except socket.error:
                            pass
                        raise
                    finally:
                        daemon.reset()

                for daemon in killed_daemons:
                    self.log('reviving {label}'.format(label=daemon.id_))
                    daemon.start()

        for stat in stats:
            self.log("stat['{key}'] = {value}".format(key = stat, value = stats[stat]))

@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the cephfs-mirror by thrashing while another task/workunit
    is running.

    Please refer to CephFSMirrorThrasher class for further information on the
    available options.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'cephfs_mirror_thrash task only accepts a dict for configuration'

    cluster = config.get('cluster', 'ceph')
    daemons = list(ctx.daemons.iter_daemons_of_role('cephfs-mirror', cluster))
    assert len(daemons) > 0, \
        'cephfs_mirror_thrash task requires at least 1 cephfs-mirror daemon'

    # choose random seed
    if 'seed' in config:
        seed = int(config['seed'])
    else:
        seed = int(time.time())
    log.info('cephfs_mirror_thrash using random seed: {seed}'.format(seed=seed))
    random.seed(seed)

    thrasher = CephFSMirrorThrasher(ctx, config, cluster, daemons)
    thrasher.start()
    ctx.ceph[cluster].thrashers.append(thrasher)

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining cephfs_mirror_thrash')
        thrasher.stop()
        if thrasher.exception is not None:
            raise RuntimeError('error during thrashing')
        thrasher.join()
        log.info('done joining')
