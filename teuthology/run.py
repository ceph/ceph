import argparse
import os
import yaml

def config_file(string):
    config = {}
    try:
        with file(string) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                config.update(new)
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))
    return config

class MergeConfig(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        config = getattr(namespace, self.dest)
        for new in values:
            config.update(new)

def parse_args():
    parser = argparse.ArgumentParser(description='Run ceph integration tests')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
        )
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='+',
        type=config_file,
        action=MergeConfig,
        default={},
        help='config file to read',
        )
    parser.add_argument(
        '--archive',
        metavar='DIR',
        help='path to archive results in',
        )
    parser.add_argument(
        '--description',
        help='job description',
        )
    parser.add_argument(
        '--owner',
        help='job owner',
        )
    parser.add_argument(
        '--lock',
        action='store_true',
        default=False,
        help='lock machines for the duration of the run',
        )
    parser.add_argument(
        '--block',
        action='store_true',
        default=False,
        help='block until locking machines succeeds (use with --lock)',
        )
    parser.add_argument(
        '--keep-locked-on-error',
        action='store_true',
        default=False,
        help='unlock machines only if the test succeeds (use with --lock)',
        )

    args = parser.parse_args()
    return args

def main():
    from gevent import monkey; monkey.patch_all()
    from orchestra import monkey; monkey.patch_all()

    import logging

    log = logging.getLogger(__name__)
    ctx = parse_args()

    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    if ctx.block:
        assert ctx.lock, \
            'the --block option is only supported with the --lock option'
    if ctx.keep_locked_on_error:
        assert ctx.lock, \
            'the --keep_locked_on_error option is only supported with the --lock option'

    from teuthology.misc import read_config
    read_config(ctx)

    if ctx.archive is not None:
        os.mkdir(ctx.archive)

        handler = logging.FileHandler(
            filename=os.path.join(ctx.archive, 'teuthology.log'),
            )
        formatter = logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S',
            )
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

        with file(os.path.join(ctx.archive, 'config.yaml'), 'w') as f:
            yaml.safe_dump(ctx.config, f, default_flow_style=False)

    log.debug('\n  '.join(['Config:', ] + yaml.safe_dump(ctx.config, default_flow_style=False).splitlines()))

    ctx.summary = dict(success=True)

    if ctx.owner is None:
        from teuthology.misc import get_user
        ctx.owner = get_user()
    ctx.summary['owner'] = ctx.owner

    if ctx.description is not None:
        ctx.summary['description'] = ctx.description

    for task in ctx.config['tasks']:
        assert 'kernel' not in task, \
            'kernel installation shouldn be a base-level item, not part of the tasks list'

    init_tasks = []
    if ctx.lock:
        assert 'targets' not in ctx.config, \
            'You cannot specify targets in a config file when using the --lock option'
        init_tasks.append({'internal.lock_machines': len(ctx.config['roles'])})

    init_tasks.extend([
            {'internal.check_lock': None},
            {'internal.connect': None},
            {'internal.check_conflict': None},
            ])
    if 'kernel' in ctx.config:
        init_tasks.append({'kernel': ctx.config['kernel']})
    init_tasks.extend([
            {'internal.base': None},
            {'internal.archive': None},
            {'internal.coredump': None},
            {'internal.syslog': None},
            ])

    ctx.config['tasks'][:0] = init_tasks

    from teuthology.run_tasks import run_tasks
    try:
        run_tasks(tasks=ctx.config['tasks'], ctx=ctx)
    finally:
        if ctx.archive is not None:
            with file(os.path.join(ctx.archive, 'summary.yaml'), 'w') as f:
                yaml.safe_dump(ctx.summary, f, default_flow_style=False)


def nuke():
    from gevent import monkey; monkey.patch_all()
    from orchestra import monkey; monkey.patch_all()

    import logging
    import time

    log = logging.getLogger(__name__)
    ctx = parse_args()

    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    from teuthology.misc import read_config
    read_config(ctx)

    log.info('\n  '.join(['targets:', ] + yaml.safe_dump(ctx.config['targets'], default_flow_style=False).splitlines()))

    if ctx.owner is None:
        from teuthology.misc import get_user
        ctx.owner = get_user()

    from teuthology.task.internal import check_lock, connect
    check_lock(ctx, None)
    connect(ctx, None)

    log.info('Unmount cfuse and killing daemons...')

    from orchestra import run
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'if', 'grep', '-q', 'cfuse', '/etc/mtab', run.Raw(';'),
                'then',
                'grep', 'cfuse', '/etc/mtab', run.Raw('|'),
                'grep', '-o', " /.* fuse", run.Raw('|'),
                'grep', '-o', "/.* ", run.Raw('|'),
                'xargs', 'sudo', 'fusermount', '-u', run.Raw(';'),
                'fi',
                run.Raw(';'),
                'killall',
                '--quiet',
                '/tmp/cephtest/binary/usr/local/bin/cmon',
                '/tmp/cephtest/binary/usr/local/bin/cosd',
                '/tmp/cephtest/binary/usr/local/bin/cmds',
                '/tmp/cephtest/binary/usr/local/bin/cfuse',
                run.Raw(';'),
                'if', 'test', '-e', '/etc/rsyslog.d/80-cephtest.conf',
                run.Raw(';'),
                'then',
                'sudo', 'rm', '-f', '--', '/etc/rsyslog.d/80-cephtest.conf',
                run.Raw('&&'),
                'sudo', 'initctl', 'restart', 'rsyslog',
                run.Raw(';'),
                'fi',
                run.Raw(';'),
                ],
            wait=False,
            )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to finish shutdowns...', name)
        proc.exitstatus.get()
    log.info('Shutdowns Done.')

    nodes = {}
    log.info('Looking for kernel mounts to handle...')
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'grep', '-q', " ceph " , '/etc/mtab'
                ],
            wait=False,
            )
        nodes[remote] = proc
    kernel_mounts = list()
    for remote, proc in nodes.iteritems():
        try:
            proc.exitstatus.get()
            log.debug('kernel mount exists on %s', remote.name)
            kernel_mounts.append(remote)
        except run.CommandFailedError: # no mounts!
            log.debug('no kernel mount on %s', remote.name)
    """
    properly we should be able to just do a forced unmount,
    but that doesn't seem to be working, so we'll reboot instead 
    nodes = {}
    for remote in kernel_mounts:
        log.info('clearing kernel mount from %s', remote.name)
        proc = remote.run(
            args=[
                'grep', 'ceph', '/etc/mtab', run.Raw('|'),
                'grep', '-o', "on /.* type", run.Raw('|'),
                'grep', '-o', "/.* ", run.Raw('|'),
                'xargs', 'sudo', 'umount', '-f', run.Raw(';')
                'fi'
                ]
            wait=False
            )
        nodes[remote] = proc
    """
    nodes = {}
    
    for remote in kernel_mounts:
        log.info('rebooting %s', remote.name)
        proc = remote.run( # note use of -n to force a no-sync reboot
            args=['sudo', 'reboot', '-f', '-n'],
            wait=False
            )
        nodes[remote] = proc
        # we just ignore these procs because reboot -f doesn't actually
        # send anything back to the ssh client!
        #for remote, proc in nodes.iteritems():
        #proc.exitstatus.get()
    from teuthology.misc import reconnect
    if kernel_mounts:
        log.info('waiting for nodes to reboot')
        time.sleep(5) #if we try and reconnect too quickly, it succeeds!
        reconnect(ctx, 300)     #allow 5 minutes for the reboots


    nodes = {}
    log.info('Clearing filesystem of test data...')
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'sudo', 'rm', '-rf', '/tmp/cephtest',
                ],
            wait=False,
            )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to clear filesystem...', name)
        proc.exitstatus.get()
    log.info('Filesystem Cleared.')


def schedule():
    parser = argparse.ArgumentParser(description='Schedule ceph integration tests')
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='+',
        type=config_file,
        action=MergeConfig,
        default={},
        help='config file to read',
        )
    parser.add_argument(
        '--name',
        required=True,
        help='job name',
        )
    parser.add_argument(
        '--description',
        help='job description',
        )
    parser.add_argument(
        '--owner',
        help='job owner',
        )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
        )

    ctx = parser.parse_args()

    from teuthology.misc import read_config, get_user
    if ctx.owner is None:
        ctx.owner = 'scheduled_{user}'.format(user=get_user())
    read_config(ctx)

    import teuthology.queue
    beanstalk = teuthology.queue.connect(ctx)

    beanstalk.use('teuthology')
    job = yaml.safe_dump(dict(
            config=ctx.config,
            name=ctx.name,
            description=ctx.description,
            owner=ctx.owner,
            verbose=ctx.verbose,
            ))
    jid = beanstalk.put(job, ttr=60*60*24)
    print 'Job scheduled with ID {jid}'.format(jid=jid)
