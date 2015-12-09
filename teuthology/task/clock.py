"""
Clock synchronizer
"""
import logging
import contextlib
import os

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Sync or skew clock

    This will initially sync the clocks.  Eventually it should let us also
    skew by some number of seconds.

    example:

    tasks:
    - clock:
    - ceph:
    - interactive:

    to sync.

    :param ctx: Context
    :param config: Configuration
    """

    log.info('Syncing clocks and checking initial clock skew...')
    for rem in ctx.cluster.remotes.iterkeys():
        ntpconf = rem.get_file('/etc/ntp.conf')
        servers = [
            l.strip().split()[1] for l in open(ntpconf, 'r').readlines()
            if l.startswith('server')
        ]
        os.remove(ntpconf)
        args = [
            'sudo',
            'service', 'ntp', 'stop',
            run.Raw(';'),
            'sudo',
            'ntpdate',
        ]
        args.extend(servers)
        args.extend([
            run.Raw(';'),
            'sudo',
            'service', 'ntp', 'start',
            run.Raw(';'),
            'PATH=/usr/bin:/usr/sbin',
            'ntpdc', '-p',
        ])
        rem.run(args)

    try:
        yield

    finally:
        log.info('Checking final clock skew...')
        for rem in ctx.cluster.remotes.iterkeys():
            rem.run(
                args=[
                    'PATH=/usr/bin:/usr/sbin',
                    'ntpdc', '-p',
                    ],
                )


@contextlib.contextmanager
def check(ctx, config):
    """
    Run ntpdc at the start and the end of the task.

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Checking initial clock skew...')
    for rem in ctx.cluster.remotes.iterkeys():
        rem.run(
            args=[
                'PATH=/usr/bin:/usr/sbin',
                'ntpdc', '-p',
                ],
            )

    try:
        yield

    finally:
        log.info('Checking final clock skew...')
        for rem in ctx.cluster.remotes.iterkeys():
            rem.run(
                args=[
                    'PATH=/usr/bin:/usr/sbin',
                    'ntpdc', '-p',
                    ],
                )
