from cStringIO import StringIO
import contextlib
import gevent
import logging
import os
import time
import yaml

from teuthology import lock
from teuthology import misc as teuthology
from ..orchestra import run

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
        # make sure there are enough machines up
        machines = lock.list_locks(ctx)
        if machines is None:
            if ctx.block:
                log.warn('error listing machines, trying again')
                time.sleep(20)
                continue
            else:
                assert 0, 'error listing machines'
        num_up = len(filter(lambda machine: machine['up'], machines))
        assert num_up >= config, 'not enough machines are up'

        # make sure there are machines for non-automated jobs to run
        num_free = len(filter(
                lambda machine: machine['up'] and machine['locked'] == 0,
                machines
                ))
        if num_free < 6 and ctx.owner.startswith('scheduled'):
            if ctx.block:
                log.info('waiting for more machines to be free...')
                time.sleep(10)
                continue
            else:
                assert 0, 'not enough machines free'

        newly_locked = lock.lock_many(ctx, config, ctx.owner, ctx.archive)
        if len(newly_locked) == config:
            ctx.config['targets'] = newly_locked
            log.info('\n  '.join(['Locked targets:', ] + yaml.safe_dump(ctx.config['targets'], default_flow_style=False).splitlines()))
            break
        elif not ctx.block:
            assert 0, 'not enough machines are available'

        log.warn('Could not lock enough machines, waiting...')
        time.sleep(10)
    try:
        yield
    finally:
        if ctx.summary.get('success', False):
            log.info('Unlocking machines...')
            for machine in ctx.config['targets'].iterkeys():
                lock.unlock(ctx, machine, ctx.owner)

def save_config(ctx, config):
    log.info('Saving configuration')
    if ctx.archive is not None:
        with file(os.path.join(ctx.archive, 'config.yaml'), 'w') as f:
            yaml.safe_dump(ctx.config, f, default_flow_style=False)

def check_lock(ctx, config):
    if ctx.config.get('check-locks') == False:
        log.info('Lock checking disabled.')
        return
    log.info('Checking locks...')
    for machine in ctx.config['targets'].iterkeys():
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

@contextlib.contextmanager
def timer(ctx, config):
    log.info('Starting timer...')
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        log.info('Duration was %f seconds', duration)
        ctx.summary['duration'] = duration

def connect(ctx, config):
    log.info('Opening connections...')
    from ..orchestra import connection, remote
    from ..orchestra import cluster
    remotes = []
    for t, key in ctx.config['targets'].iteritems():
        log.debug('connecting to %s', t)
        remotes.append(
            remote.Remote(name=t,
                          ssh=connection.connect(user_at_host=t,
                                                 host_key=key,
                                                 keep_alive=True)))
    ctx.cluster = cluster.Cluster()
    if 'roles' in ctx.config:
        for rem, roles in zip(remotes, ctx.config['roles']):
            assert all(isinstance(role, str) for role in roles), \
                "Roles in config must be strings: %r" % roles
            ctx.cluster.add(rem, roles)
            log.info('roles: %s - %s' % (rem, roles))
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
                teuthology.pull_directory(remote, '/tmp/cephtest/archive', path)

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
                if 'failure_reason' not in ctx.summary:
                    ctx.summary['failure_reason'] = \
                        'Found coredumps on {remote}'.format(remote=remote)

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

        log.info('Checking logs for errors...')
        for remote in ctx.cluster.remotes.iterkeys():
            log.debug('Checking %s', remote.name)
            r = remote.run(
                args=[
                    'egrep',
                    '\\bBUG\\b|\\bINFO\\b|\\bDEADLOCK\\b',
                    run.Raw('/tmp/cephtest/archive/syslog/*.log'),
                    run.Raw('|'),
                    'grep', '-v', 'task .* blocked for more than .* seconds',
                    run.Raw('|'),
                    'grep', '-v', 'lockdep is turned off',
                    run.Raw('|'),
                    'grep', '-v', 'trying to register non-static key',
                    run.Raw('|'),
                    'grep', '-v', 'DEBUG: fsize',  # xfs_fsr
                    run.Raw('|'),
                    'grep', '-v', 'CRON',  # ignore cron noise
                    run.Raw('|'),
                    'grep', '-v', 'inconsistent lock state', # FIXME see #2523
                    run.Raw('|'),
                    'grep', '-v', '*** DEADLOCK ***', # part of lockdep output
                    run.Raw('|'),
                    'grep', '-v', 'INFO: possible irq lock inversion dependency detected', # FIXME see #2590 and #147
                    run.Raw('|'),
                    'grep', '-v', 'INFO: possible recursive locking detected', # FIXME see #3040
                    run.Raw('|'),
                    'grep', '-v', 'BUG: lock held when returning to user space', # REMOVE ME when btrfs sb_internal crap is fixed
                    run.Raw('|'),
                    'head', '-n', '1',
                    ],
                stdout=StringIO(),
                )
            stdout = r.stdout.getvalue()
            if stdout != '':
                log.error('Error in syslog on %s: %s', remote.name, stdout)
                ctx.summary['success'] = False
                if 'failure_reason' not in ctx.summary:
                    ctx.summary['failure_reason'] = \
                        "'{error}' in syslog".format(error=stdout)

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
                    'gzip',
                    '--',
                    ],
                wait=False,
                ),
            )
