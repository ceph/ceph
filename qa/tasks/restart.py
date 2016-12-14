"""
Daemon restart
"""
import logging
import pipes

from teuthology import misc as teuthology
from teuthology.orchestra import run as tor

from teuthology.orchestra import run
log = logging.getLogger(__name__)

def restart_daemon(ctx, config, role, id_, *args):
    """
    Handle restart (including the execution of the command parameters passed)
    """
    log.info('Restarting {r}.{i} daemon...'.format(r=role, i=id_))
    daemon = ctx.daemons.get_daemon(role, id_)
    log.debug('Waiting for exit of {r}.{i} daemon...'.format(r=role, i=id_))
    try:
        daemon.wait_for_exit()
    except tor.CommandFailedError as e:
        log.debug('Command Failed: {e}'.format(e=e))
    if len(args) > 0:
        confargs = ['--{k}={v}'.format(k=k, v=v) for k,v in zip(args[0::2], args[1::2])]
        log.debug('Doing restart of {r}.{i} daemon with args: {a}...'.format(r=role, i=id_, a=confargs))
        daemon.restart_with_args(confargs)
    else:
        log.debug('Doing restart of {r}.{i} daemon...'.format(r=role, i=id_))
        daemon.restart()

def get_tests(ctx, config, role, remote, testdir):
    """Download restart tests"""
    srcdir = '{tdir}/restart.{role}'.format(tdir=testdir, role=role)

    refspec = config.get('branch')
    if refspec is None:
        refspec = config.get('sha1')
    if refspec is None:
        refspec = config.get('tag')
    if refspec is None:
        refspec = 'HEAD'
    log.info('Pulling restart qa/workunits from ref %s', refspec)

    remote.run(
        logger=log.getChild(role),
        args=[
            'mkdir', '--', srcdir,
            run.Raw('&&'),
            'git',
            'archive',
            '--remote=git://git.ceph.com/ceph.git',
            '%s:qa/workunits' % refspec,
            run.Raw('|'),
            'tar',
            '-C', srcdir,
            '-x',
            '-f-',
            run.Raw('&&'),
            'cd', '--', srcdir,
            run.Raw('&&'),
            'if', 'test', '-e', 'Makefile', run.Raw(';'), 'then', 'make', run.Raw(';'), 'fi',
            run.Raw('&&'),
            'find', '-executable', '-type', 'f', '-printf', r'%P\0'.format(srcdir=srcdir),
            run.Raw('>{tdir}/restarts.list'.format(tdir=testdir)),
            ],
        )
    restarts = sorted(teuthology.get_file(
                        remote,
                        '{tdir}/restarts.list'.format(tdir=testdir)).split('\0'))
    return (srcdir, restarts)

def task(ctx, config):
    """
    Execute commands and allow daemon restart with config options.
    Each process executed can output to stdout restart commands of the form:
        restart <role> <id> <conf_key1> <conf_value1> <conf_key2> <conf_value2>
    This will restart the daemon <role>.<id> with the specified config values once
    by modifying the conf file with those values, and then replacing the old conf file
    once the daemon is restarted.
    This task does not kill a running daemon, it assumes the daemon will abort on an
    assert specified in the config.

        tasks:
        - install:
        - ceph:
        - restart:
            exec:
              client.0:
                - test_backtraces.py

    """
    assert isinstance(config, dict), "task kill got invalid config"

    testdir = teuthology.get_testdir(ctx)

    try:
        assert 'exec' in config, "config requires exec key with <role>: <command> entries"
        for role, task in config['exec'].iteritems():
            log.info('restart for role {r}'.format(r=role))
            (remote,) = ctx.cluster.only(role).remotes.iterkeys()
            srcdir, restarts = get_tests(ctx, config, role, remote, testdir)
            log.info('Running command on role %s host %s', role, remote.name)
            spec = '{spec}'.format(spec=task[0])
            log.info('Restarts list: %s', restarts)
            log.info('Spec is %s', spec)
            to_run = [w for w in restarts if w == task or w.find(spec) != -1]
            log.info('To run: %s', to_run)
            for c in to_run:
                log.info('Running restart script %s...', c)
                args = [
                    run.Raw('TESTDIR="{tdir}"'.format(tdir=testdir)),
                    ]
                env = config.get('env')
                if env is not None:
                    for var, val in env.iteritems():
                        quoted_val = pipes.quote(val)
                        env_arg = '{var}={val}'.format(var=var, val=quoted_val)
                        args.append(run.Raw(env_arg))
                args.extend([
                            'adjust-ulimits',
                            'ceph-coverage',
                            '{tdir}/archive/coverage'.format(tdir=testdir),
                            '{srcdir}/{c}'.format(
                                srcdir=srcdir,
                                c=c,
                                ),
                            ])
                proc = remote.run(
                    args=args,
                    stdout=tor.PIPE,
                    stdin=tor.PIPE,
                    stderr=log,
                    wait=False,
                    )
                log.info('waiting for a command from script')
                while True:
                    l = proc.stdout.readline()
                    if not l or l == '':
                        break
                    log.debug('script command: {c}'.format(c=l))
                    ll = l.strip()
                    cmd = ll.split(' ')
                    if cmd[0] == "done":
                        break
                    assert cmd[0] == 'restart', "script sent invalid command request to kill task"
                    # cmd should be: restart <role> <id> <conf_key1> <conf_value1> <conf_key2> <conf_value2>
                    # or to clear, just: restart <role> <id>
                    restart_daemon(ctx, config, cmd[1], cmd[2], *cmd[3:])
                    proc.stdin.writelines(['restarted\n'])
                    proc.stdin.flush()
                try:
                    proc.wait()
                except tor.CommandFailedError:
                    raise Exception('restart task got non-zero exit status from script: {s}'.format(s=c))
    finally:
        log.info('Finishing %s on %s...', task, role)
        remote.run(
            logger=log.getChild(role),
            args=[
                'rm', '-rf', '--', '{tdir}/restarts.list'.format(tdir=testdir), srcdir,
                ],
            )
