from cStringIO import StringIO
import contextlib
import gevent
import logging
import os
import time
import yaml
import re
import subprocess

from teuthology import lockstatus
from teuthology import lock
from teuthology import misc as teuthology
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def base(ctx, config):
    log.info('Creating base directory...')
    test_basedir = teuthology.get_testdir_base(ctx)
    testdir = teuthology.get_testdir(ctx)
    # make base dir if it doesn't exist
    run.wait(
        ctx.cluster.run(
            args=[
                'mkdir', '-m0755', '-p', '--',
                test_basedir,
                ],
                wait=False,
                )
        )
    # only create testdir if its not set to basedir
    if test_basedir != testdir:
        run.wait(
            ctx.cluster.run(
                args=[
                    'mkdir', '-m0755', '--',
                    testdir,
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
                    testdir,
                    ],
                wait=False,
                ),
            )


@contextlib.contextmanager
def lock_machines(ctx, config):
    log.info('Locking machines...')
    assert isinstance(config[0], int), 'config must be an integer'
    machine_type = config[1]
    config = config[0]

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
        num_up = len(filter(lambda machine: machine['up'] and machine['type'] == machine_type, machines))
        assert num_up >= config, 'not enough machines are up'

        # make sure there are machines for non-automated jobs to run
        num_free = len(filter(
                lambda machine: machine['up'] and machine['locked'] == 0 and machine['type'] == machine_type,
                machines
                ))
        if num_free < 6 and ctx.owner.startswith('scheduled'):
            if ctx.block:
                log.info('waiting for more machines to be free...')
                time.sleep(10)
                continue
            else:
                assert 0, 'not enough machines free'

        newly_locked = lock.lock_many(ctx, config, machine_type, ctx.owner, ctx.archive)
        if len(newly_locked) == config:
            vmlist = []
            for lmach in newly_locked:
                if lock.create_if_vm(ctx,lmach):
                    vmlist.append(lmach)
            if vmlist:
                log.info('Waiting for virtual machines to come up')
                keyscan_out = ''
                loopcount=0
                while len(keyscan_out.splitlines()) != len(vmlist):
                    loopcount += 1
                    time.sleep(10)
                    keyscan_out, current_locks = lock.keyscan_check(ctx, vmlist)
                    log.info('virtual machine is stil unavailable')
                    if loopcount == 40:
                        loopcount = 0
                        log.info('virtual machine(s) still not up, recreating unresponsive ones.')
                        for guest in vmlist:
                            if guest not in keyscan_out:
                                log.info('recreating: ' + guest)
                                lock.destroy_if_vm(ctx, 'ubuntu@' + guest)
                                lock.create_if_vm(ctx, 'ubuntu@' + guest)
                if lock.update_keys(ctx, keyscan_out, current_locks):
                    log.info("Error in virtual machine keys")
                newscandict = {}
                for dkey in newly_locked.iterkeys():
                    stats = lockstatus.get_status(ctx, dkey)
                    newscandict[dkey] = stats['sshpubkey']
                ctx.config['targets'] = newscandict
            else:
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
        status = lockstatus.get_status(ctx, machine)
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
    machs = []
    for name in ctx.config['targets'].iterkeys():
        machs.append(name)
    for t, key in ctx.config['targets'].iteritems():
        log.debug('connecting to %s', t)
        try:
            if ctx.config['sshkeys'] == 'ignore':
                key = None
        except (AttributeError, KeyError):
            pass
        for machine in ctx.config['targets'].iterkeys():
            if teuthology.is_vm(machine):
                key = None
                break
        remotes.append(
            remote.Remote(name=t,
                          ssh=connection.connect(user_at_host=t,
                                                 host_key=key,
                                                 keep_alive=True),
                          console=None))
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

def check_ceph_data(ctx, config):
    log.info('Checking for old /var/lib/ceph...')
    processes = ctx.cluster.run(
        args=[
            'test', '!', '-e', '/var/lib/ceph',
            ],
        wait=False,
        )
    failed = False
    for proc in processes:
        assert isinstance(proc.exitstatus, gevent.event.AsyncResult)
        try:
            proc.exitstatus.get()
        except run.CommandFailedError:
            log.error('Host %s has stale /var/lib/ceph, check lock and nuke/cleanup.', proc.remote.shortname)
            failed = True
    if failed:
        raise RuntimeError('Stale /var/lib/ceph detected, aborting.')

def check_conflict(ctx, config):
    log.info('Checking for old test directory...')
    test_basedir = teuthology.get_testdir_base(ctx)
    processes = ctx.cluster.run(
        args=[
            'test', '!', '-e', test_basedir,
            ],
        wait=False,
        )
    for proc in processes:
        assert isinstance(proc.exitstatus, gevent.event.AsyncResult)
        try:
            proc.exitstatus.get()
        except run.CommandFailedError:
            # base dir exists
            r = proc.remote.run(
                args=[
                    'ls', test_basedir, run.Raw('|'), 'wc', '-l'
                    ],
                stdout=StringIO(),
                )

            if int(r.stdout.getvalue()) > 0:
                log.error('WARNING: Host %s has stale test directories, these need to be investigated and cleaned up!',
                          proc.remote.shortname)

    # testdir might be the same as base dir (if test_path is set)
    # need to bail out in that case if the testdir exists
    testdir = teuthology.get_testdir(ctx)
    processes = ctx.cluster.run(
        args=[
            'test', '!', '-e', testdir,
            ],
        wait=False,
        )
    failed = False
    for proc in processes:
        assert isinstance(proc.exitstatus, gevent.event.AsyncResult)
        try:
            proc.exitstatus.get()
        except run.CommandFailedError:
            log.error('Host %s has stale test directory %s, check lock and cleanup.', proc.remote.shortname, testdir)
            failed = True
    if failed:
        raise RuntimeError('Stale jobs detected, aborting.')

@contextlib.contextmanager
def archive(ctx, config):
    log.info('Creating archive directory...')
    testdir = teuthology.get_testdir(ctx)
    archive_dir = '{tdir}/archive'.format(tdir=testdir)
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--', archive_dir,
                ],
            wait=False,
            )
        )

    try:
        yield
    except:
        # we need to know this below
        ctx.summary['success'] = False
        raise
    finally:
        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and ctx.summary['success']):
            log.info('Transferring archived files...')
            logdir = os.path.join(ctx.archive, 'remote')
            if (not os.path.exists(logdir)):
                os.mkdir(logdir)
            for remote in ctx.cluster.remotes.iterkeys():
                path = os.path.join(logdir, remote.shortname)
                teuthology.pull_directory(remote, archive_dir, path)

        log.info('Removing archive directory...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '--',
                    archive_dir,
                    ],
                wait=False,
                ),
            )

@contextlib.contextmanager
def coredump(ctx, config):
    log.info('Enabling coredump saving...')
    archive_dir = '{tdir}/archive'.format(tdir=teuthology.get_testdir(ctx))
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '{adir}/coredump'.format(adir=archive_dir),
                run.Raw('&&'),
                'sudo', 'sysctl', '-w', 'kernel.core_pattern={adir}/coredump/%t.%p.core'.format(adir=archive_dir),
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
                    '{adir}/coredump'.format(adir=archive_dir),
                    ],
                wait=False,
                )
            )

        # set success=false if the dir is still there = coredumps were
        # seen
        for remote in ctx.cluster.remotes.iterkeys():
            r = remote.run(
                args=[
                    'if', 'test', '!', '-e', '{adir}/coredump'.format(adir=archive_dir), run.Raw(';'), 'then',
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

    archive_dir = '{tdir}/archive'.format(tdir=teuthology.get_testdir(ctx))
    run.wait(
        ctx.cluster.run(
            args=[
                'mkdir', '-m0755', '--',
                '{adir}/syslog'.format(adir=archive_dir),
                ],
            wait=False,
            )
        )

    CONF = '/etc/rsyslog.d/80-cephtest.conf'
    conf_fp = StringIO("""
kern.* -{adir}/syslog/kern.log;RSYSLOG_FileFormat
*.*;kern.none -{adir}/syslog/misc.log;RSYSLOG_FileFormat
""".format(adir=archive_dir))
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
                    'service',
                    # a mere reload (SIGHUP) doesn't seem to make
                    # rsyslog open the files
                    'rsyslog',
                    'restart',
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
                    'service',
                    'rsyslog',
                    'restart',
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
                    run.Raw('{adir}/archive/syslog/*.log'.format(adir=archive_dir)),
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
                    'grep', '-v', 'INFO: possible circular locking dependency detected',  # FIXME remove when xfs stops being noisy and lame.
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
                    '{adir}/archive/syslog'.format(adir=archive_dir),
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

def vm_setup(ctx, config):
    """
    Look for virtual machines and handle their initialization
    """
    with parallel() as p:
        editinfo = os.path.join(os.path.dirname(__file__),'edit_sudoers.sh')
        for remote in ctx.cluster.remotes.iterkeys():
            mname = re.match(".*@([^\.]*)\.?.*", str(remote)).group(1)
            if teuthology.is_vm(mname):
                r = remote.run(args=['test', '-e', '/ceph-qa-ready',],
                        stdout=StringIO(),
                        check_status=False,)
                if r.exitstatus != 0:
                    p1 = subprocess.Popen(['cat', editinfo], stdout=subprocess.PIPE)
                    p2 = subprocess.Popen(['ssh','-t','-t',str(remote), 'sudo', 'sh'],stdin=p1.stdout, stdout=subprocess.PIPE)
                    _,err = p2.communicate()
                    if err:
                        log.info("Edit of /etc/sudoers failed: %s",err)
                    p.spawn(_handle_vm_init, remote)

def _handle_vm_init(remote):
    log.info('Running ceph_qa_chef on ', remote)
    remote.run(args=['wget','-q','-O-',
            'http://ceph.com/git/?p=ceph-qa-chef.git;a=blob_plain;f=solo/solo-from-scratch;hb=HEAD',
            run.Raw('|'),
            'sh',
        ])

