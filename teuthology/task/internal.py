from cStringIO import StringIO
import contextlib
import gevent
import logging
import os
import tarfile
import time

from teuthology import lock
from teuthology import misc as teuthology
from teuthology import safepath
from orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def base(ctx, config):
    log.info('Creating base directory...')
    run.wait(
        ctx.cluster.run(
            args=[
                'mkdir', '-m0755', '--',
                '/tmp/cephtest',
                ],
            wait=False,
            )
        )

    try:
        yield
    finally:
        log.info('Tidying up after the test...')
        # if this fails, one of the earlier cleanups is flawed; don't
        # just cram an rm -rf here
        run.wait(
            ctx.cluster.run(
                args=[
                    'rmdir',
                    '--',
                    '/tmp/cephtest',
                    ],
                wait=False,
                ),
            )


@contextlib.contextmanager
def lock_machines(ctx, config):
    log.info('Locking machines...')
    assert isinstance(config, int), 'config must be an integer'

    while True:
        newly_locked = lock.lock_many(ctx, config, ctx.owner)
        if len(newly_locked) == config:
            ctx.config['targets'] = newly_locked
            log.info('\n  '.join(['Locked targets:', ] + newly_locked))
            break
        elif not ctx.block:
            assert 0, 'not enough machines are available'

        # make sure there are enough machines up
        machines = lock.list_locks(ctx)
        assert machines is not None, 'error listing machines'
        num_up = len(filter(lambda machine: machine['up'], machines))
        assert num_up >= config, 'not enough machines are up'

        log.warn('Could not lock enough machines, waiting...')
        time.sleep(10)
    try:
        yield
    finally:
        log.info('Unlocking machines...')
        for machine in ctx.config['targets']:
            lock.unlock(ctx, machine, ctx.owner)

def check_lock(ctx, config):
    log.info('Checking locks...')
    for machine in ctx.config['targets']:
        status = lock.get_status(ctx, machine)
        log.debug('machine status is %s', repr(status))
        assert status is not None, \
            'could not read lock status for {name}'.format(name=machine)
        assert status['up'], 'machine {name} is marked down'.format(name=machine)
        assert status['locked'], \
            'machine {name} is not locked'.format(name=machine)
        assert status['locked_by'] == ctx.owner, \
            'machine {name} is locked by {user}, not {owner}'.format(
            name=machine,
            user=status['locked_by'],
            owner=ctx.owner,
            )

def connect(ctx, config):
    log.info('Opening connections...')
    from orchestra import connection, remote
    import orchestra.cluster

    remotes = [remote.Remote(name=t, ssh=connection.connect(t))
               for t in ctx.config['targets']]
    ctx.cluster = orchestra.cluster.Cluster()
    if 'roles' in ctx.config:
        for rem, roles in zip(remotes, ctx.config['roles']):
            ctx.cluster.add(rem, roles)
    else:
        for rem in remotes:
            ctx.cluster.add(rem, rem.name)
        

def check_conflict(ctx, config):
    log.info('Checking for old test directory...')
    processes = ctx.cluster.run(
        args=[
            'test', '!', '-e', '/tmp/cephtest',
            ],
        wait=False,
        )
    failed = False
    for proc in processes:
        assert isinstance(proc.exitstatus, gevent.event.AsyncResult)
        try:
            proc.exitstatus.get()
        except run.CommandFailedError:
            log.error('Host %s has stale cephtest directory, check your lock and reboot to clean up.', proc.remote.shortname)
            failed = True
    if failed:
        raise RuntimeError('Stale jobs detected, aborting.')

@contextlib.contextmanager
def archive(ctx, config):
    log.info('Creating archive directory...')
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '/tmp/cephtest/archive',
                ],
            wait=False,
            )
        )

    try:
        yield
    finally:
        if ctx.archive is not None:

            log.info('Transferring archived files...')
            logdir = os.path.join(ctx.archive, 'remote')
            os.mkdir(logdir)
            for remote in ctx.cluster.remotes.iterkeys():
                path = os.path.join(logdir, remote.shortname)
                os.mkdir(path)
                log.debug('Transferring archived files from %s to %s', remote.shortname, path)
                proc = remote.run(
                    args=[
                        'tar',
                        'c',
                        '-f', '-',
                        '-C', '/tmp/cephtest/archive',
                        '--',
                        '.',
                        ],
                    stdout=run.PIPE,
                    wait=False,
                    )
                tar = tarfile.open(mode='r|', fileobj=proc.stdout)
                while True:
                    ti = tar.next()
                    if ti is None:
                        break

                    if ti.isdir():
                        # ignore silently; easier to just create leading dirs below
                        pass
                    elif ti.isfile():
                        sub = safepath.munge(ti.name)
                        safepath.makedirs(root=path, path=os.path.dirname(sub))
                        tar.makefile(ti, targetpath=os.path.join(path, sub))
                    else:
                        if ti.isdev():
                            type_ = 'device'
                        elif ti.issym():
                            type_ = 'symlink'
                        elif ti.islnk():
                            type_ = 'hard link'
                        else:
                            type_ = 'unknown'
                        log.info('Ignoring tar entry: %r type %r', ti.name, type_)
                        continue
                proc.exitstatus.get()

        log.info('Removing archive directory...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '--',
                    '/tmp/cephtest/archive',
                    ],
                wait=False,
                ),
            )

@contextlib.contextmanager
def coredump(ctx, config):
    log.info('Enabling coredump saving...')
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '/tmp/cephtest/archive/coredump',
                run.Raw('&&'),
                'sudo', 'sysctl', '-w', 'kernel.core_pattern=/tmp/cephtest/archive/coredump/%t.%p.core',
                ],
            wait=False,
            )
        )

    try:
        yield
    finally:
        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo', 'sysctl', '-w', 'kernel.core_pattern=core',
                    run.Raw('&&'),
                    # don't litter the archive dir if there were no cores dumped
                    'rmdir',
                    '--ignore-fail-on-non-empty',
                    '--',
                    '/tmp/cephtest/archive/coredump',
                    ],
                wait=False,
                )
            )

        # set success=false if the dir is still there = coredumps were
        # seen
        for remote in ctx.cluster.remotes.iterkeys():
            r = remote.run(
                args=[
                    'if', 'test', '!', '-e', '/tmp/cephtest/archive/coredump', run.Raw(';'), 'then',
                    'echo', 'OK', run.Raw(';'),
                    'fi',
                    ],
                stdout=StringIO(),
                )
            if r.stdout.getvalue() != 'OK\n':
                log.warning('Found coredumps on %s, flagging run as failed', remote)
                ctx.summary['success'] = False

@contextlib.contextmanager
def syslog(ctx, config):
    if ctx.archive is None:
        # disable this whole feature if we're not going to archive the data anyway
        yield
        return

    log.info('Starting syslog monitoring...')

    run.wait(
        ctx.cluster.run(
            args=[
                'mkdir', '-m0755', '--',
                '/tmp/cephtest/archive/syslog',
                ],
            wait=False,
            )
        )

    CONF = '/etc/rsyslog.d/80-cephtest.conf'
    conf_fp = StringIO("""
kern.* -/tmp/cephtest/archive/syslog/kern.log;RSYSLOG_FileFormat
*.*;kern.none -/tmp/cephtest/archive/syslog/misc.log;RSYSLOG_FileFormat
""")
    try:
        for rem in ctx.cluster.remotes.iterkeys():
            teuthology.sudo_write_file(
                remote=rem,
                path=CONF,
                data=conf_fp,
                )
            conf_fp.seek(0)
        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo',
                    'initctl',
                    # a mere reload (SIGHUP) doesn't seem to make
                    # rsyslog open the files
                    'restart',
                    'rsyslog',
                    ],
                wait=False,
                ),
            )

        yield
    finally:
        log.info('Shutting down syslog monitoring...')

        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo',
                    'rm',
                    '-f',
                    '--',
                    CONF,
                    run.Raw('&&'),
                    'sudo',
                    'initctl',
                    'restart',
                    'rsyslog',
                    ],
                wait=False,
                ),
            )
        # race condition: nothing actually says rsyslog had time to
        # flush the file fully. oh well.

        log.info('Compressing syslogs...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'find',
                    '/tmp/cephtest/archive/syslog',
                    '-name',
                    '*.log',
                    '-print0',
                    run.Raw('|'),
                    'xargs',
                    '-0',
                    '--no-run-if-empty',
                    '--',
                    'bzip2',
                    '-9',
                    '--',
                    ],
                wait=False,
                ),
            )
