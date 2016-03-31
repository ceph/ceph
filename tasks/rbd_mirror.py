"""
Task for running rbd mirroring daemons and configuring mirroring
"""

import logging

from teuthology.orchestra import run
from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from util import get_remote_for_role

log = logging.getLogger(__name__)


class RBDMirror(Task):
    """
    Run an rbd-mirror daemon to sync rbd images between clusters.

    This requires two clients (one from each cluster) on the same host
    to connect with. The pool configuration should be adjusted by later
    test scripts to include the remote client and cluster name. This task
    just needs to know how to connect to the local cluster.

    For example:

        roles:
        - [primary.mon.a, primary.osd.0, primary.osd.1, primary.osd.2]
        - [secondary.mon.a, secondary.osd.0, secondary.osd.1, secondary.osd.2]
        - [primary.client.mirror, secondary.client.mirror]
        tasks:
        - ceph:
            cluster: primary
        - ceph:
            cluster: secondary
        - rbd-mirror:
            client: primary.client.mirror

    To mirror back to the primary cluster as well, add another
    rbd_mirror instance:

        - rbd-mirror:
            client: secondary.client.mirror

    Possible options for this task are:

        client: role - ceph client to connect as
        valgrind: [--tool=<valgrind tool>] - none by default
        coverage: bool - whether this run may be collecting coverage data
    """
    def __init__(self, ctx, config):
        super(RBDMirror, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(RBDMirror, self).setup()
        try:
            self.client = self.config['client']
        except KeyError:
            raise ConfigError('rbd-mirror requires a client to connect with')

        self.cluster_name, type_, self.client_id = misc.split_role(self.client)

        if type_ != 'client':
            msg = 'client role ({0}) must be a client'.format(self.client)
            raise ConfigError(msg)

        self.remote = get_remote_for_role(self.ctx, self.client)

    def begin(self):
        super(RBDMirror, self).begin()
        testdir = misc.get_testdir(self.ctx)
        daemon_signal = 'kill'
        if 'coverage' in self.config or 'valgrind' in self.config:
            daemon_signal = 'term'

        args = [
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'daemon-helper',
            daemon_signal,
            ]

        if 'valgrind' in self.config:
            args = misc.get_valgrind_args(
                testdir,
                'rbd-mirror-{id}'.format(id=self.client),
                args,
                self.config.get('valgrind')
            )

        args.extend([
            'rbd-mirror',
            '--cluster',
            self.cluster_name,
            '--id',
            self.client_id,
            ])

        self.ctx.daemons.add_daemon(
            self.remote, 'rbd-mirror', self.client,
            cluster=self.cluster_name,
            args=args,
            logger=self.log.getChild(self.client),
            stdin=run.PIPE,
            wait=False,
        )

    def end(self):
        mirror_daemon = self.ctx.daemons.get_daemon('rbd-mirror',
                                                    self.client,
                                                    self.cluster_name)
        mirror_daemon.stop()
        super(RBDMirror, self).end()

task = RBDMirror
