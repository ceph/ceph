import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run 

log = logging.getLogger(__name__)
blktrace = '/usr/sbin/blktrace'
daemon_signal = 'term'

@contextlib.contextmanager
def setup(ctx, config):
    osds = ctx.cluster.only(teuthology.is_type('osd'))
    log_dir = '{tdir}/archive/performance/blktrace'.format(tdir=teuthology.get_testdir(ctx))

    for remote, roles_for_host in osds.remotes.iteritems():
        log.info('Creating %s on %s' % (log_dir,remote.name))
        remote.run(
            args=['mkdir', '-p', '-m0755', '--', log_dir],
            wait=False,
            )
    yield

@contextlib.contextmanager
def execute(ctx, config):
    procs = []
    testdir=teuthology.get_testdir(ctx)
    log_dir = '{tdir}/archive/performance/blktrace'.format(tdir=testdir)

    osds = ctx.cluster.only(teuthology.is_type('osd'))
    for remote, roles_for_host in osds.remotes.iteritems():
        roles_to_devs = ctx.disk_config.remote_to_roles_to_dev[remote]
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            if roles_to_devs.get(id_):
                dev = roles_to_devs[id_]
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
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict.fromkeys(config)

    with contextutil.nested(
        lambda: setup(ctx=ctx, config=config),
        lambda: execute(ctx=ctx, config=config),
        ):
        yield

