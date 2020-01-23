import logging
import time

from teuthology.misc import get_testdir, reconnect
from teuthology.orchestra import run
from teuthology.orchestra.remote import Remote
from teuthology.task import install as install_task


log = logging.getLogger(__name__)


def clear_firewall(ctx):
    """
    Remove any iptables rules created by teuthology.  These rules are
    identified by containing a comment with 'teuthology' in it.  Non-teuthology
    firewall rules are unaffected.
    """
    log.info("Clearing teuthology firewall rules...")
    ctx.cluster.run(
        args=[
            "sudo", "sh", "-c",
            "iptables-save | grep -v teuthology | iptables-restore"
        ],
    )
    log.info("Cleared teuthology firewall rules.")


def shutdown_daemons(ctx):
    log.info('Unmounting ceph-fuse and killing daemons...')
    ctx.cluster.run(args=['sudo', 'stop', 'ceph-all', run.Raw('||'),
                          'sudo', 'service', 'ceph', 'stop', run.Raw('||'),
                          'sudo', 'systemctl', 'stop', 'ceph.target'],
                    check_status=False, timeout=180)
    ctx.cluster.run(
        args=[
            'if', 'grep', '-q', 'ceph-fuse', '/etc/mtab', run.Raw(';'),
            'then',
            'grep', 'ceph-fuse', '/etc/mtab', run.Raw('|'),
            'grep', '-o', " /.* fuse", run.Raw('|'),
            'grep', '-o', "/.* ", run.Raw('|'),
            'xargs', '-n', '1', 'sudo', 'fusermount', '-u', run.Raw(';'),
            'fi',
            run.Raw(';'),
            'if', 'grep', '-q', 'rbd-fuse', '/etc/mtab', run.Raw(';'),
            'then',
            'grep', 'rbd-fuse', '/etc/mtab', run.Raw('|'),
            'grep', '-o', " /.* fuse", run.Raw('|'),
            'grep', '-o', "/.* ", run.Raw('|'),
            'xargs', '-n', '1', 'sudo', 'fusermount', '-u', run.Raw(';'),
            'fi',
            run.Raw(';'),
            'sudo',
            'killall',
            '--quiet',
            'ceph-mon',
            'ceph-osd',
            'ceph-mds',
            'ceph-mgr',
            'ceph-fuse',
            'ceph-disk',
            'radosgw',
            'ceph_test_rados',
            'rados',
            'rbd-fuse',
            'apache2',
            run.Raw('||'),
            'true',  # ignore errors from ceph binaries not being found
        ],
        timeout=120,
    )
    log.info('All daemons killed.')


def kill_hadoop(ctx):
    log.info("Terminating Hadoop services...")
    ctx.cluster.run(args=[
        "pkill", "-f", "-KILL", "java.*hadoop",
        ],
        check_status=False,
        timeout=60
    )


def kill_valgrind(ctx):
    # http://tracker.ceph.com/issues/17084
    ctx.cluster.run(
        args=['sudo', 'pkill', '-f', '-9', 'valgrind.bin'],
        check_status=False,
        timeout=20,
    )


def remove_osd_mounts(ctx):
    """
    unmount any osd data mounts (scratch disks)
    """
    log.info('Unmount any osd data directories...')
    ctx.cluster.run(
        args=[
            'grep',
            '/var/lib/ceph/osd/',
            '/etc/mtab',
            run.Raw('|'),
            'awk', '{print $2}', run.Raw('|'),
            'xargs', '-r',
            'sudo', 'umount', '-l', run.Raw(';'),
            'true'
        ],
        timeout=120
    )


def remove_osd_tmpfs(ctx):
    """
    unmount tmpfs mounts
    """
    log.info('Unmount any osd tmpfs dirs...')
    ctx.cluster.run(
        args=[
            'egrep', 'tmpfs\s+/mnt', '/etc/mtab', run.Raw('|'),
            'awk', '{print $2}', run.Raw('|'),
            'xargs', '-r',
            'sudo', 'umount', run.Raw(';'),
            'true'
        ],
        timeout=120
    )


def stale_kernel_mount(remote):
    proc = remote.run(
        args=[
            'sudo', 'find',
            '/sys/kernel/debug/ceph',
            '-mindepth', '1',
            '-type', 'd',
            run.Raw('|'),
            'read'
        ],
        check_status=False
    )
    return proc.exitstatus == 0


def reboot(ctx, remotes):
    for remote in remotes:
        if stale_kernel_mount(remote):
            log.warn('Stale kernel mount on %s!', remote.name)
            log.info('force/no-sync rebooting %s', remote.name)
            # -n is ignored in systemd versions through v229, which means this
            # only works on trusty -- on 7.3 (v219) and xenial (v229) reboot -n
            # still calls sync().
            # args = ['sync', run.Raw('&'),
            #         'sleep', '5', run.Raw(';'),
            #         'sudo', 'reboot', '-f', '-n']
            args = ['for', 'sysrq', 'in', 's', 'u', 'b', run.Raw(';'),
                    'do', 'echo', run.Raw('$sysrq'), run.Raw('|'),
                    'sudo', 'tee', '/proc/sysrq-trigger', run.Raw(';'),
                    'done']
        else:
            log.info('rebooting %s', remote.name)
            args = ['sudo', 'reboot']
        try:
            remote.run(args=args, wait=False)
        except Exception:
            log.exception('ignoring exception during reboot command')
        # we just ignore these procs because reboot -f doesn't actually
        # send anything back to the ssh client!
    if remotes:
        log.info('waiting for nodes to reboot')
        time.sleep(8)  # if we try and reconnect too quickly, it succeeds!
        reconnect(ctx, 480)  # allow 8 minutes for the reboots


def reset_syslog_dir(ctx):
    log.info('Resetting syslog output locations...')
    nodes = {}
    for remote in ctx.cluster.remotes.keys():
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
            timeout=60,
        )
        nodes[remote.name] = proc

    for name, proc in nodes.items():
        log.info('Waiting for %s to restart syslog...', name)
        proc.wait()


def dpkg_configure(ctx):
    for remote in ctx.cluster.remotes.keys():
        if remote.os.package_type != 'deb':
            continue
        log.info(
            'Waiting for dpkg --configure -a and apt-get -f install...')
        remote.run(
            args=[
                'sudo', 'dpkg', '--configure', '-a',
                run.Raw(';'),
                'sudo', 'DEBIAN_FRONTEND=noninteractive',
                'apt-get', '-y', '--force-yes', '-f', 'install',
                run.Raw('||'),
                ':',
            ],
            timeout=180,
            check_status=False,
        )


def remove_yum_timedhosts(ctx):
    # Workaround for https://bugzilla.redhat.com/show_bug.cgi?id=1233329
    log.info("Removing yum timedhosts files...")
    for remote in ctx.cluster.remotes.keys():
        if remote.os.package_type != 'rpm':
            continue
        remote.run(
            args="sudo find /var/cache/yum -name 'timedhosts' -exec rm {} \;",
            check_status=False, timeout=180
        )


def remove_ceph_packages(ctx):
    """
    remove ceph and ceph dependent packages by force
    force is needed since the node's repo might have changed and
    in many cases autocorrect will not work due to missing packages
    due to repo changes
    """
    log.info("Force remove ceph packages")
    ceph_packages_to_remove = ['ceph-common', 'ceph-mon', 'ceph-osd',
                               'libcephfs1', 'libcephfs2',
                               'librados2', 'librgw2', 'librbd1', 'python-rgw',
                               'ceph-selinux', 'python-cephfs', 'ceph-base',
                               'python-rbd', 'python-rados', 'ceph-mds',
                               'ceph-mgr', 'libcephfs-java', 'libcephfs-jni',
                               'ceph-deploy', 'libapache2-mod-fastcgi'
                               ]
    pkgs = str.join(' ', ceph_packages_to_remove)
    for remote in ctx.cluster.remotes.keys():
        if remote.os.package_type == 'rpm':
            log.info("Remove any broken repos")
            dist_release = remote.os.name
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/yum.repos.d/*ceph*")],
                check_status=False
            )
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/yum.repos.d/*fcgi*")],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/yum.repos.d/*samba*")],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/yum.repos.d/*nfs-ganesha*")],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'rpm', '--rebuilddb']
            )
            if dist_release in ['opensuse', 'sle']:
                remote.sh('sudo zypper clean')
                log.info('Remove any ceph packages')
                remote.sh('sudo zypper remove --non-interactive',
                    check_status=False
                )
            else:
                remote.sh('sudo yum clean all')
                log.info('Remove any ceph packages')
                remote.sh('sudo yum remove -y', check_status=False)
        else:
            log.info("Remove any broken repos")
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/apt/sources.list.d/*ceph*")],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/apt/sources.list.d/*samba*")],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/apt/sources.list.d/*nfs-ganesha*")],
                check_status=False,
            )
            log.info("Autoclean")
            remote.run(
                args=['sudo', 'apt-get', 'autoclean'],
                check_status=False,
            )
            log.info('Remove any ceph packages')
            remote.run(
                args=[
                    'sudo', 'dpkg', '--remove', '--force-remove-reinstreq',
                    run.Raw(pkgs)
                ],
                check_status=False
            )
            log.info("Autoclean")
            remote.run(
                args=['sudo', 'apt-get', 'autoclean']
            )


def remove_installed_packages(ctx):
    dpkg_configure(ctx)
    conf = dict(
        project='ceph',
        debuginfo='true',
    )
    packages = install_task.get_package_list(ctx, conf)
    debs = packages['deb'] + \
        ['salt-common', 'salt-minion', 'calamari-server',
         'python-rados', 'multipath-tools']
    rpms = packages['rpm'] + \
        ['salt-common', 'salt-minion', 'calamari-server',
         'multipath-tools', 'device-mapper-multipath']
    install_task.remove_packages(
        ctx,
        conf,
        dict(
            deb=debs,
            rpm=rpms,
        )
    )
    install_task.remove_sources(ctx, conf)


def remove_ceph_data(ctx):
    log.info("Removing any stale ceph data...")
    ctx.cluster.run(
        args=[
            'sudo', 'rm', '-rf', '/etc/ceph',
            run.Raw('/var/run/ceph*'),
        ],
    )


def remove_testing_tree(ctx):
    log.info('Clearing filesystem of test data...')
    ctx.cluster.run(
        args=[
            'sudo', 'rm', '-rf', get_testdir(ctx),
            # just for old time's sake
            run.Raw('&&'),
            'sudo', 'rm', '-rf', '/tmp/cephtest',
            run.Raw('&&'),
            'sudo', 'rm', '-rf', '/home/ubuntu/cephtest',
        ],
    )


def remove_configuration_files(ctx):
    """
    Goes through a list of commonly used configuration files used for testing
    that should not be left behind.

    For example, sometimes ceph-deploy may be configured via
    ``~/.cephdeploy.conf`` to alter how it handles installation by specifying
    a default section in its config with custom locations.
    """
    ctx.cluster.run(
        args=[
            'rm', '-f', '/home/ubuntu/.cephdeploy.conf'
        ],
        timeout=30
    )


def undo_multipath(ctx):
    """
    Undo any multipath device mappings created, an
    remove the packages/daemon that manages them so they don't
    come back unless specifically requested by the test.
    """
    log.info('Removing any multipath config/pkgs...')
    for remote in ctx.cluster.remotes.keys():
        remote.run(
            args=[
                'sudo', 'multipath', '-F',
            ],
            check_status=False,
            timeout=60
        )


def synch_clocks(remotes):
    log.info('Synchronizing clocks...')
    for remote in remotes:
        remote.run(
            args=[
                'sudo', 'systemctl', 'stop', 'ntp.service', run.Raw('||'),
                'sudo', 'systemctl', 'stop', 'ntpd.service', run.Raw('||'),
                'sudo', 'systemctl', 'stop', 'chronyd.service',
                run.Raw('&&'),
                'sudo', 'ntpdate-debian', run.Raw('||'),
                'sudo', 'ntp', '-gq', run.Raw('||'),
                'sudo', 'ntpd', '-gq', run.Raw('||'),
                'sudo', 'chronyc', 'sources',
                run.Raw('&&'),
                'sudo', 'hwclock', '--systohc', '--utc',
                run.Raw('&&'),
                'sudo', 'systemctl', 'start', 'ntp.service', run.Raw('||'),
                'sudo', 'systemctl', 'start', 'ntpd.service', run.Raw('||'),
                'sudo', 'systemctl', 'start', 'chronyd.service',
                run.Raw('||'),
                'true',    # ignore errors; we may be racing with ntpd startup
            ],
            timeout=60,
        )


def unlock_firmware_repo(ctx):
    log.info('Making sure firmware.git is not locked...')
    ctx.cluster.run(args=['sudo', 'rm', '-f',
                          '/lib/firmware/updates/.git/index.lock', ])


def check_console(hostname):
    remote = Remote(hostname)
    shortname = remote.shortname
    console = remote.console
    if not console:
        return
    cname = '{host}.{domain}'.format(
        host=shortname,
        domain=console.ipmidomain,
    )
    log.info('checking console status of %s' % cname)
    if console.check_status():
        log.info('console ready on %s' % cname)
        return
    if console.check_power('on'):
        log.info('attempting to reboot %s' % cname)
        console.power_cycle()
    else:
        log.info('attempting to power on %s' % cname)
        console.power_on()
    timeout = 100
    log.info('checking console status of %s with timeout %s' %
             (cname, timeout))
    if console.check_status(timeout=timeout):
        log.info('console ready on %s' % cname)
    else:
        log.error("Failed to get console status for %s, " % cname)
