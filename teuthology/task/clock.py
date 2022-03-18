"""
Clock synchronizer
"""
import logging
import contextlib

from teuthology.orchestra import run

log = logging.getLogger(__name__)

def filter_out_containers(cluster):
    """
    Returns a cluster that excludes remotes which should skip this task.
    Currently, only skips containerized remotes.
    """
    return cluster.filter(lambda r: not r.is_container)

@contextlib.contextmanager
def task(ctx, config):
    """
    Sync or skew clock

    This will initially sync the clocks.  Eventually it should let us also
    skew by some number of seconds.

    example::

        tasks:
        - clock:
        - ceph:
        - interactive:

    to sync.

    :param ctx: Context
    :param config: Configuration
    """

    log.info('Syncing clocks and checking initial clock skew...')
    cluster = filter_out_containers(ctx.cluster)
    run.wait(
        cluster.run(
            args = [
                'sudo', 'systemctl', 'stop', 'ntp.service', run.Raw('||'),
                'sudo', 'systemctl', 'stop', 'ntpd.service', run.Raw('||'),
                'sudo', 'systemctl', 'stop', 'chronyd.service',
                run.Raw(';'),
                'sudo', 'ntpd', '-gq', run.Raw('||'),
                'sudo', 'chronyc', 'makestep',
                run.Raw(';'),
                'sudo', 'systemctl', 'start', 'ntp.service', run.Raw('||'),
                'sudo', 'systemctl', 'start', 'ntpd.service', run.Raw('||'),
                'sudo', 'systemctl', 'start', 'chronyd.service',
                run.Raw(';'),
                'PATH=/usr/bin:/usr/sbin', 'ntpq', '-p', run.Raw('||'),
                'PATH=/usr/bin:/usr/sbin', 'chronyc', 'sources',
                run.Raw('||'),
                'true'
            ],
            timeout = 360,
            wait=False,
        )
    )

    try:
        yield

    finally:
        log.info('Checking final clock skew...')
        cluster = filter_out_containers(ctx.cluster)
        run.wait(
            cluster.run(
                args=[
                    'PATH=/usr/bin:/usr/sbin', 'ntpq', '-p', run.Raw('||'),
                    'PATH=/usr/bin:/usr/sbin', 'chronyc', 'sources',
                    run.Raw('||'),
                    'true'
                ],
                wait=False,
            )
        )


@contextlib.contextmanager
def check(ctx, config):
    """
    Run ntpq at the start and the end of the task.

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Checking initial clock skew...')
    cluster = filter_out_containers(ctx.cluster)
    run.wait(
        cluster.run(
            args=[
                'PATH=/usr/bin:/usr/sbin', 'ntpq', '-p', run.Raw('||'),
                'PATH=/usr/bin:/usr/sbin', 'chronyc', 'sources',
                run.Raw('||'),
                'true'
            ],
            wait=False,
        )
    )

    try:
        yield

    finally:
        log.info('Checking final clock skew...')
        cluster = filter_out_containers(ctx.cluster)
        run.wait(
            cluster.run(
                args=[
                    'PATH=/usr/bin:/usr/sbin', 'ntpq', '-p', run.Raw('||'),
                    'PATH=/usr/bin:/usr/sbin', 'chronyc', 'sources',
                    run.Raw('||'),
                    'true'
                ],
                wait=False,
            )
        )
