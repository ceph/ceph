"""
Run blktrace program through teuthology
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)
blktrace = '/usr/sbin/blktrace'
daemon_signal = 'term'

@contextlib.contextmanager
def setup(ctx, config):
    """
    Setup all the remotes
    """
    osds = ctx.cluster.only(teuthology.is_type('osd', config['cluster']))
    log_dir = '{tdir}/archive/performance/blktrace'.format(tdir=teuthology.get_testdir(ctx))

    for remote, roles_for_host in osds.remotes.items():
        log.info('Creating %s on %s' % (log_dir, remote.name))
        remote.run(
            args=['mkdir', '-p', '-m0755', '--', log_dir],
            wait=False,
            )
    yield

@contextlib.contextmanager
def execute(ctx, config):
    """
    Run the blktrace program on remote machines.
    """
    procs = []
    testdir = teuthology.get_testdir(ctx)
    log_dir = '{tdir}/archive/performance/blktrace'.format(tdir=testdir)

    osds = ctx.cluster.only(teuthology.is_type('osd'))
    for remote, roles_for_host in osds.remotes.items():
        roles_to_devs = ctx.disk_config.remote_to_roles_to_dev[remote]
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'osd',
                                                     config['cluster']):
            if roles_to_devs.get(role):
                dev = roles_to_devs[role]
                log.info("running blktrace on %s: %s" % (remote.name, dev))

                proc = remote.run(
                    args=[
                        'cd',
                        log_dir,
                        run.Raw(';'),
                        'daemon-helper',
                        daemon_signal,
                        'sudo',
                        blktrace,
                        '-o',
                        dev.rsplit("/", 1)[1],
                        '-d',
                        dev,
                        ],
                    wait=False,
                    stdin=run.PIPE,
                    )
                procs.append(proc)
    try:
        yield
    finally:
        osds = ctx.cluster.only(teuthology.is_type('osd'))
        log.info('stopping blktrace processs')
        for proc in procs:
            proc.stdin.close()

@contextlib.contextmanager
def task(ctx, config):
    """
    Usage:
        blktrace:

    or:
        blktrace:
          cluster: backup

    Runs blktrace on all osds in the specified cluster (the 'ceph' cluster by
    default).
    """
    if config is None:
        config = {}
    config['cluster'] = config.get('cluster', 'ceph')

    with contextutil.nested(
        lambda: setup(ctx=ctx, config=config),
        lambda: execute(ctx=ctx, config=config),
        ):
        yield
