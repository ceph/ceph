import argparse
import logging
import os
import subprocess
import time
import yaml

import teuthology
from . import orchestra
import orchestra.remote
from .orchestra import run
from .lock import list_locks
from .lock import unlock_one
from .misc import config_file
from .misc import get_testdir
from .misc import get_user
from .misc import read_config
from .misc import reconnect
from .parallel import parallel
from .task import install as install_task
from .task.internal import check_lock
from .task.internal import connect

log = logging.getLogger(__name__)


def shutdown_daemons(ctx):
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'if', 'grep', '-q', 'ceph-fuse', '/etc/mtab', run.Raw(';'),
                'then',
                'grep', 'ceph-fuse', '/etc/mtab', run.Raw('|'),
                'grep', '-o', " /.* fuse", run.Raw('|'),
                'grep', '-o', "/.* ", run.Raw('|'),
                'xargs', 'sudo', 'fusermount', '-u', run.Raw(';'),
                'fi',
                run.Raw(';'),
                'sudo',
                'killall',
                '--quiet',
                'ceph-mon',
                'ceph-osd',
                'ceph-mds',
                'ceph-fuse',
                'ceph-disk',
                'radosgw',
                'ceph_test_rados',
                'rados',
                'apache2',
                run.Raw('||'),
                'true',  # ignore errors from ceph binaries not being found
            ],
            wait=False,
        )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to finish shutdowns...', name)
        proc.exitstatus.get()


def find_kernel_mounts(ctx):
    nodes = {}
    log.info('Looking for kernel mounts to handle...')
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'grep', '-q', ' ceph ', '/etc/mtab',
                run.Raw('||'),
                'grep', '-q', '^/dev/rbd', '/etc/mtab',
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
        except run.CommandFailedError:  # no mounts!
            log.debug('no kernel mount on %s', remote.name)

    return kernel_mounts


def remove_kernel_mounts(ctx, kernel_mounts):
    """
    properly we should be able to just do a forced unmount,
    but that doesn't seem to be working, so you should reboot instead
    """
    nodes = {}
    for remote in kernel_mounts:
        log.info('clearing kernel mount from %s', remote.name)
        proc = remote.run(
            args=[
                'grep', 'ceph', '/etc/mtab', run.Raw('|'),
                'grep', '-o', "on /.* type", run.Raw('|'),
                'grep', '-o', "/.* ", run.Raw('|'),
                'xargs', '-r',
                'sudo', 'umount', '-f', run.Raw(';'),
                'fi'
            ],
            wait=False
        )
        nodes[remote] = proc

    for remote, proc in nodes:
        proc.exitstatus.get()


def remove_osd_mounts(ctx):
    """
    unmount any osd data mounts (scratch disks)
    """
    ctx.cluster.run(
        args=[
            'grep',
            '/var/lib/ceph/osd/',
            '/etc/mtab',
            run.Raw('|'),
            'awk', '{print $2}', run.Raw('|'),
            'xargs', '-r',
            'sudo', 'umount', run.Raw(';'),
            'true'
        ],
    )


def remove_osd_tmpfs(ctx):
    """
    unmount tmpfs mounts
    """
    ctx.cluster.run(
        args=[
            'egrep', 'tmpfs\s+/mnt', '/etc/mtab', run.Raw('|'),
            'awk', '{print $2}', run.Raw('|'),
            'xargs', '-r',
            'sudo', 'umount', run.Raw(';'),
            'true'
        ],
    )


def reboot(ctx, remotes):
    nodes = {}
    for remote in remotes:
        log.info('rebooting %s', remote.name)
        try:
            proc = remote.run(  # note use of -n to force a no-sync reboot
                args=[
                    'sync',
                    run.Raw('&'),
                    'sleep', '5',
                    run.Raw(';'),
                    'sudo', 'reboot', '-f', '-n'
                    ],
                wait=False
                )
        except Exception:
            log.exception('ignoring exception during reboot command')
        nodes[remote] = proc
        # we just ignore these procs because reboot -f doesn't actually
        # send anything back to the ssh client!
        # for remote, proc in nodes.iteritems():
        # proc.exitstatus.get()
    if remotes:
        log.info('waiting for nodes to reboot')
        time.sleep(8)  # if we try and reconnect too quickly, it succeeds!
        reconnect(ctx, 480)  # allow 8 minutes for the reboots


def reset_syslog_dir(ctx):
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'if', 'test', '-e', '/etc/rsyslog.d/80-cephtest.conf',
                run.Raw(';'),
                'then',
                'sudo', 'rm', '-f', '--', '/etc/rsyslog.d/80-cephtest.conf',
                run.Raw('&&'),
                'sudo', 'service', 'rsyslog', 'restart',
                run.Raw(';'),
                'fi',
                run.Raw(';'),
            ],
            wait=False,
        )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to restart syslog...', name)
        proc.exitstatus.get()


def dpkg_configure(ctx):
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'sudo', 'dpkg', '--configure', '-a',
                run.Raw('&&'),
                'sudo', 'apt-get', '-f', 'install',
                run.Raw('||'),
                ':',
            ],
            wait=False,
        )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info(
            'Waiting for %s to dpkg --configure -a and apt-get -f install...',
            name)
        proc.exitstatus.get()


def remove_installed_packages(ctx):

    dpkg_configure(ctx)
    config = {'project': 'ceph'}
    install_task.remove_packages(
        ctx,
        config,
        {"deb": install_task.deb_packages['ceph'] + ['salt-common', 'salt-minion'],
         "rpm": install_task.rpm_packages['ceph']})
    install_task.remove_sources(ctx, config)
    install_task.purge_data(ctx)


def remove_testing_tree(ctx):
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'sudo', 'rm', '-rf', get_testdir(ctx),
                # just for old time's sake
                run.Raw('&&'),
                'sudo', 'rm', '-rf', '/tmp/cephtest',
                run.Raw('&&'),
                'sudo', 'rm', '-rf', '/home/ubuntu/cephtest',
                run.Raw('&&'),
                'sudo', 'rm', '-rf', '/etc/ceph',
            ],
            wait=False,
        )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to clear filesystem...', name)
        proc.exitstatus.get()


def synch_clocks(remotes):
    nodes = {}
    for remote in remotes:
        proc = remote.run(
            args=[
                'sudo', 'service', 'ntp', 'stop',
                run.Raw('&&'),
                'sudo', 'ntpdate-debian',
                run.Raw('&&'),
                'sudo', 'hwclock', '--systohc', '--utc',
                run.Raw('&&'),
                'sudo', 'service', 'ntp', 'start',
                run.Raw('||'),
                'true',    # ignore errors; we may be racing with ntpd startup
            ],
            wait=False,
        )
        nodes[remote.name] = proc
    for name, proc in nodes.iteritems():
        log.info('Waiting for clock to synchronize on %s...', name)
        proc.exitstatus.get()


def main(ctx):
    if ctx.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    info = {}
    if ctx.archive:
        ctx.config = config_file(ctx.archive + '/config.yaml')
        ifn = os.path.join(ctx.archive, 'info.yaml')
        if os.path.exists(ifn):
            with file(ifn, 'r') as fd:
                info = yaml.load(fd.read())
        if not ctx.pid:
            ctx.pid = info.get('pid')
            if not ctx.pid:
                ctx.pid = int(open(ctx.archive + '/pid').read().rstrip('\n'))
        if not ctx.owner:
            ctx.owner = info.get('owner')
            if not ctx.owner:
                ctx.owner = open(ctx.archive + '/owner').read().rstrip('\n')

    read_config(ctx)

    log.info(
        '\n  '.join(
            ['targets:', ] + yaml.safe_dump(
                ctx.config['targets'],
                default_flow_style=False).splitlines()))

    if ctx.owner is None:
        ctx.owner = get_user()

    if ctx.pid:
        if ctx.archive:
            log.info('Killing teuthology process at pid %d', ctx.pid)
            os.system('grep -q %s /proc/%d/cmdline && sudo kill %d' % (
                ctx.archive,
                ctx.pid,
                ctx.pid))
        else:
            subprocess.check_call(["kill", "-9", str(ctx.pid)])

    nuke(ctx, ctx.unlock, ctx.synch_clocks, ctx.reboot_all, ctx.noipmi)


def nuke(ctx, should_unlock, sync_clocks=True, reboot_all=True, noipmi=False):
    total_unnuked = {}
    targets = dict(ctx.config['targets'])
    if ctx.name:
        log.info('Checking targets against current locks')
        locks = list_locks()
        # Remove targets who's description doesn't match archive name.
        for lock in locks:
            for target in targets:
                if target == lock['name']:
                    if ctx.name not in lock['description']:
                        del ctx.config['targets'][lock['name']]
                        log.info(
                            "Not nuking %s because description doesn't match",
                            lock['name'])
    with parallel() as p:
        for target, hostkey in ctx.config['targets'].iteritems():
            p.spawn(
                nuke_one,
                ctx,
                {target: hostkey},
                should_unlock,
                sync_clocks,
                reboot_all,
                ctx.config.get('check-locks', True),
                noipmi,
            )
        for unnuked in p:
            if unnuked:
                total_unnuked.update(unnuked)
    if total_unnuked:
        log.error('Could not nuke the following targets:\n' +
                  '\n  '.join(['targets:', ] +
                              yaml.safe_dump(
                                  total_unnuked,
                                  default_flow_style=False).splitlines()))


def nuke_one(ctx, targets, should_unlock, synch_clocks, reboot_all,
             check_locks, noipmi):
    ret = None
    ctx = argparse.Namespace(
        config=dict(targets=targets),
        owner=ctx.owner,
        check_locks=check_locks,
        synch_clocks=synch_clocks,
        reboot_all=reboot_all,
        teuthology_config=ctx.teuthology_config,
        name=ctx.name,
        noipmi=noipmi,
    )
    try:
        nuke_helper(ctx)
    except Exception:
        log.exception('Could not nuke all targets in %s' % targets)
        # not re-raising the so that parallel calls aren't killed
        ret = targets
    else:
        if should_unlock:
            for target in targets.keys():
                unlock_one(ctx, target, ctx.owner)
    return ret


def nuke_helper(ctx):
    # ensure node is up with ipmi

    (target,) = ctx.config['targets'].keys()
    host = target.split('@')[-1]
    shortname = host.split('.')[0]
    if 'vpm' in shortname:
        return
    log.debug('shortname: %s' % shortname)
    log.debug('{ctx}'.format(ctx=ctx))
    if not ctx.noipmi and 'ipmi_user' in ctx.teuthology_config:
        console = orchestra.remote.getRemoteConsole(
            name=host,
            ipmiuser=ctx.teuthology_config['ipmi_user'],
            ipmipass=ctx.teuthology_config['ipmi_password'],
            ipmidomain=ctx.teuthology_config['ipmi_domain'])
        cname = '{host}.{domain}'.format(
            host=shortname,
            domain=ctx.teuthology_config['ipmi_domain'])
        log.info('checking console status of %s' % cname)
        if not console.check_status():
            # not powered on or can't get IPMI status.  Try to power on
            console.power_on()
            # try to get status again, waiting for login prompt this time
            log.info('checking console status of %s' % cname)
            if not console.check_status(100):
                log.error('Failed to get console status for %s, ' +
                          'disabling console...' % cname)
            log.info('console ready on %s' % cname)
        else:
            log.info('console ready on %s' % cname)

    if ctx.check_locks:
        check_lock(ctx, None)
    connect(ctx, None)

    log.info('Unmount ceph-fuse and killing daemons...')
    shutdown_daemons(ctx)
    log.info('All daemons killed.')

    need_reboot = find_kernel_mounts(ctx)

    # no need to unmount anything if we're rebooting
    if ctx.reboot_all:
        need_reboot = ctx.cluster.remotes.keys()
    else:
        log.info('Unmount any osd data directories...')
        remove_osd_mounts(ctx)
        log.info('Unmount any osd tmpfs dirs...')
        remove_osd_tmpfs(ctx)
        # log.info('Dealing with any kernel mounts...')
        # remove_kernel_mounts(ctx, need_reboot)

    if need_reboot:
        reboot(ctx, need_reboot)
    log.info('All kernel mounts gone.')

    log.info('Synchronizing clocks...')
    if ctx.synch_clocks:
        need_reboot = ctx.cluster.remotes.keys()
    synch_clocks(need_reboot)

    log.info('Making sure firmware.git is not locked...')
    ctx.cluster.run(args=['sudo', 'rm', '-f',
                          '/lib/firmware/updates/.git/index.lock', ])

    log.info('Reseting syslog output locations...')
    reset_syslog_dir(ctx)
    log.info('Clearing filesystem of test data...')
    remove_testing_tree(ctx)
    log.info('Filesystem Cleared.')
    remove_installed_packages(ctx)
    log.info('Installed packages removed.')
