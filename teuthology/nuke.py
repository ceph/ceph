import argparse
import yaml

def parse_args():
    from teuthology.run import config_file
    from teuthology.run import MergeConfig

    parser = argparse.ArgumentParser(description='Reset test machines')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose'
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
        '--owner',
        help='job owner',
        )
    parser.add_argument(
        '-r', '--reboot-all',
        action='store_true',
        default=False,
        help='reboot all machines',
        )
    args = parser.parse_args()
    return args

def shutdown_daemons(ctx, log):
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

def find_kernel_mounts(ctx, log):
    from orchestra import run
    nodes = {}
    log.info('Looking for kernel mounts to handle...')
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'grep', '-q', ' ceph ' , '/etc/mtab',
                run.Raw('||'),
                'grep', '-q', '^/dev/rbd' , '/etc/mtab',
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
    
    return kernel_mounts

def remove_kernel_mounts(ctx, kernel_mounts, log):
    """
    properly we should be able to just do a forced unmount,
    but that doesn't seem to be working, so you should reboot instead 
    """
    from orchestra import run
    nodes = {}
    for remote in kernel_mounts:
        log.info('clearing kernel mount from %s', remote.name)
        proc = remote.run(
            args=[
                'grep', 'ceph', '/etc/mtab', run.Raw('|'),
                'grep', '-o', "on /.* type", run.Raw('|'),
                'grep', '-o', "/.* ", run.Raw('|'),
                'xargs', 'sudo', 'umount', '-f', run.Raw(';'),
                'fi'
                ],
            wait=False
            )
        nodes[remote] = proc

    for remote, proc in nodes:
        proc.exitstatus.get()

def reboot(ctx, remotes, log):
    import time
    nodes = {}
    for remote in remotes:
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
    if remotes:
        log.info('waiting for nodes to reboot')
        time.sleep(5) #if we try and reconnect too quickly, it succeeds!
        reconnect(ctx, 300)     #allow 5 minutes for the reboots

def remove_testing_tree(ctx, log):
    from orchestra import run
    nodes = {}
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
    shutdown_daemons(ctx, log)
    log.info('All daemons killed.')

    log.info('Dealing with any kernel mounts...')
    kernel_mounts = find_kernel_mounts(ctx, log)
    #remove_kernel_mounts(ctx, kernel_mounts, log)
    need_reboot = kernel_mounts
    if ctx.reboot_all:
        need_reboot = ctx.cluster.remotes.keys()
    reboot(ctx, need_reboot, log)
    log.info('All kernel mounts gone.')

    log.info('Clearing filesystem of test data...')
    remove_testing_tree(ctx, log)
    log.info('Filesystem Cleared.')
