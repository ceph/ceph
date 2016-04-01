import argparse
import datetime
import json
import logging
import os
import subprocess
import time
import yaml
from StringIO import StringIO

import teuthology
from . import orchestra
import orchestra.remote
from .openstack import OpenStack, OpenStackInstance, enforce_json_dictionary
from .orchestra import run
from .config import config, FakeNamespace
from .lock import list_locks
from .lock import locked_since_seconds
from .lock import unlock_one
from .lock import find_stale_locks
from .lockstatus import get_status
from .misc import canonicalize_hostname
from .misc import config_file
from .misc import decanonicalize_hostname
from .misc import merge_configs
from .misc import get_testdir
from .misc import get_user
from .misc import reconnect
from .misc import sh
from .parallel import parallel
from .task import install as install_task
from .task.internal import check_lock, add_remotes, connect

log = logging.getLogger(__name__)


def clear_firewall(ctx):
    """
    Remove any iptables rules created by teuthology.  These rules are
    identified by containing a comment with 'teuthology' in it.  Non-teuthology
    firewall rules are unaffected.
    """
    ctx.cluster.run(
        args=[
            "sudo", "sh", "-c",
            "iptables-save | grep -v teuthology | iptables-restore"
        ],
        wait=False,
    )


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
            wait=False,
        )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to finish shutdowns...', name)
        proc.wait()


def kill_hadoop(ctx):
    for remote in ctx.cluster.remotes.iterkeys():
        pids_out = StringIO()
        ps_proc = remote.run(args=[
            "ps", "-eo", "pid,cmd",
            run.Raw("|"), "grep", "java.*hadoop",
            run.Raw("|"), "grep", "-v", "grep"
            ], stdout=pids_out, check_status=False)

        if ps_proc.exitstatus == 0:
            for line in pids_out.getvalue().strip().split("\n"):
                pid, cmdline = line.split(None, 1)
                log.info("Killing PID {0} ({1})".format(pid, cmdline))
                remote.run(args=["kill", "-9", pid], check_status=False)


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
            proc.wait()
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
        proc.wait()


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
        # proc.wait()
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
        proc.wait()


def dpkg_configure(ctx):
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        if remote.os.package_type != 'deb':
            continue
        proc = remote.run(
            args=[
                'sudo', 'dpkg', '--configure', '-a',
                run.Raw(';'),
                'sudo', 'DEBIAN_FRONTEND=noninteractive',
                'apt-get', '-y', '--force-yes', '-f', 'install',
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
        proc.wait()


def remove_yum_timedhosts(ctx):
    # Workaround for https://bugzilla.redhat.com/show_bug.cgi?id=1233329
    log.info("Removing yum timedhosts files...")
    for remote in ctx.cluster.remotes.iterkeys():
        if remote.os.package_type != 'rpm':
            continue
        remote.run(
            args="sudo find /var/cache/yum -name 'timedhosts' -exec rm {} \;",
            check_status=False,
        )


def remove_ceph_packages(ctx):
    """
    remove ceph and ceph dependent packages by force
    force is needed since the node's repo might have changed and
    in many cases autocorrect will not work due to missing packages
    due to repo changes
    """
    ceph_packages_to_remove = ['ceph-common', 'ceph-mon', 'ceph-osd',
                               'libcephfs1', 'librados2', 'librgw2', 'librbd1',
                               'ceph-selinux', 'python-cephfs', 'ceph-base',
                               'python-rbd', 'python-rados', 'ceph-mds',
                               'libcephfs-java', 'libcephfs-jni',
                               'ceph-deploy', 'libapache2-mod-fastcgi'
                               ]
    pkgs = str.join(' ', ceph_packages_to_remove)
    for remote in ctx.cluster.remotes.iterkeys():
        if remote.os.package_type == 'rpm':
            log.info("Remove any broken repos")
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/yum.repos.d/*ceph*")],
                check_status=False
            )
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/yum.repos.d/*fcgi*")],
                check_status=False,
            )
            remote.run(
                args=['sudo', 'rpm', '--rebuilddb', run.Raw('&&'), 'yum',
                      'clean', 'all']
            )
            log.info('Remove any ceph packages')
            remote.run(
                args=['sudo', 'yum', 'remove', '-y', run.Raw(pkgs)],
                check_status=False
            )
        else:
            log.info("Remove any broken repos")
            remote.run(
                args=['sudo', 'rm', run.Raw("/etc/apt/sources.list.d/*ceph*")],
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
        ['salt-common', 'salt-minion', 'calamari-server', 'python-rados']
    rpms = packages['rpm'] + \
        ['salt-common', 'salt-minion', 'calamari-server']
    install_task.remove_packages(
        ctx,
        conf,
        dict(
            deb=debs,
            rpm=rpms,
        )
    )
    install_task.remove_sources(ctx, conf)
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
        proc.wait()


def remove_configuration_files(ctx):
    """
    Goes through a list of commonly used configuration files used for testing
    that should not be left behind.

    For example, sometimes ceph-deploy may be configured via
    ``~/.cephdeploy.conf`` to alter how it handles installation by specifying
    a default section in its config with custom locations.
    """

    nodes = {}

    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'rm', '-f', '/home/ubuntu/.cephdeploy.conf'
            ],
            wait=False,
        )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('removing temporary configuration files on %s', name)
        proc.wait()


def undo_multipath(ctx):
    """
    Undo any multipath device mappings created, an
    remove the packages/daemon that manages them so they don't
    come back unless specifically requested by the test.
    """
    for remote in ctx.cluster.remotes.iterkeys():
        remote.run(
            args=[
                'sudo', 'multipath', '-F',
            ],
            check_status=False,
        )
        install_task.remove_packages(
            ctx,
            dict(),     # task config not relevant here
            {
                "rpm": ['multipath-tools', 'device-mapper-multipath'],
                "deb": ['multipath-tools'],
            }
        )


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
        proc.wait()


def stale_openstack(ctx):
    targets = dict(map(lambda i: (i['ID'], i),
                       OpenStack.list_instances()))
    nodes = list_locks(keyed_by_name=True, locked=True)
    stale_openstack_instances(ctx, targets, nodes)
    stale_openstack_nodes(ctx, targets, nodes)
    stale_openstack_volumes(ctx, OpenStack.list_volumes())
    if not ctx.dry_run:
        openstack_remove_again()

#
# A delay, in seconds, that is significantly longer than
# any kind of OpenStack server creation / deletion / etc.
#
OPENSTACK_DELAY = 30 * 60


def stale_openstack_instances(ctx, instances, locked_nodes):
    for (instance_id, instance) in instances.iteritems():
        i = OpenStackInstance(instance_id)
        if not i.exists():
            log.debug("stale-openstack: {instance} disappeared, ignored"
                      .format(instance=instance_id))
            continue
        if (i.get_created() >
            config['max_job_time'] + OPENSTACK_DELAY):
            log.info(
                "stale-openstack: destroying instance {instance}"
                " because it was created {created} seconds ago"
                " which is older than"
                " max_job_time {max_job_time} + {delay}"
                .format(instance=i['name'],
                        created=i.get_created(),
                        max_job_time=config['max_job_time'],
                        delay=OPENSTACK_DELAY))
            if not ctx.dry_run:
                i.destroy()
            continue
        name = canonicalize_hostname(i['name'], user=None)
        if i.get_created() > OPENSTACK_DELAY and name not in locked_nodes:
            log.info("stale-openstack: destroying instance {instance}"
                     " because it was created {created} seconds ago"
                     " is older than {delay}s and it is not locked"
                     .format(instance=i['name'],
                             created=i.get_created(),
                             delay=OPENSTACK_DELAY))
            if not ctx.dry_run:
                i.destroy()
            continue
        log.debug("stale-openstack: instance " + i['name'] + " OK")


def openstack_delete_volume(id):
    sh("openstack volume delete " + id + " || true")


def stale_openstack_volumes(ctx, volumes):
    now = datetime.datetime.now()
    for volume in volumes:
        volume_id = volume.get('ID') or volume['id']
        try:
            volume = json.loads(sh("openstack volume show -f json " +
                                   volume_id))
        except subprocess.CalledProcessError:
            log.debug("stale-openstack: {id} disappeared, ignored"
                      .format(id=volume_id))
            continue
        volume_name = (volume.get('Display Name') or volume.get('display_name')
                       or volume['name'])
        enforce_json_dictionary(volume)
        created_at = datetime.datetime.strptime(
            volume['created_at'], '%Y-%m-%dT%H:%M:%S.%f')
        created = (now - created_at).total_seconds()
        if created > config['max_job_time'] + OPENSTACK_DELAY:
            log.info(
                "stale-openstack: destroying volume {volume}({id})"
                " because it was created {created} seconds ago"
                " which is older than"
                " max_job_time {max_job_time} + {delay}"
                .format(volume=volume_name,
                        id=volume_id,
                        created=created,
                        max_job_time=config['max_job_time'],
                        delay=OPENSTACK_DELAY))
            if not ctx.dry_run:
                openstack_delete_volume(volume_id)
            continue
        log.debug("stale-openstack: volume " + volume_id + " OK")


def stale_openstack_nodes(ctx, instances, locked_nodes):
    names = set([ i['Name'] for i in instances.values() ])
    for (name, node) in locked_nodes.iteritems():
        name = decanonicalize_hostname(name)
        if node['machine_type'] != 'openstack':
            continue
        if (name not in names and
                locked_since_seconds(node) > OPENSTACK_DELAY):
            log.info("stale-openstack: unlocking node {name} unlocked"
                     " because it was created {created}"
                     " seconds ago which is older than {delay}"
                     " and it has no instance"
                     .format(name=name,
                             created=locked_since_seconds(node),
                             delay=OPENSTACK_DELAY))
            if not ctx.dry_run:
                unlock_one(ctx, name, node['locked_by'])
            continue
        log.debug("stale-openstack: node " + name + " OK")


def openstack_remove_again():
    """
    Volumes and servers with REMOVE-ME in the name are leftover
    that failed to be removed. It is not uncommon for a failed removal
    to succeed later on.
    """
    sh("""
    openstack server list --name REMOVE-ME --column ID --format value |
    xargs --no-run-if-empty --max-args 1 -P20 openstack server delete --wait
    true
    """)
    sh("""
    openstack volume list --name REMOVE-ME --column ID --format value |
    xargs --no-run-if-empty --max-args 1 -P20 openstack volume delete
    true
    """)


def main(args):
    ctx = FakeNamespace(args)
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

    if ctx.targets:
        ctx.config = merge_configs(ctx.targets)

    if ctx.stale:
        stale_nodes = find_stale_locks(ctx.owner)
        targets = dict()
        for node in stale_nodes:
            targets[node['name']] = node['ssh_pub_key']
        ctx.config = dict(targets=targets)

    if ctx.stale_openstack:
        stale_openstack(ctx)
        return

    log.info(
        '\n  '.join(
            ['targets:', ] + yaml.safe_dump(
                ctx.config['targets'],
                default_flow_style=False).splitlines()))

    if ctx.dry_run:
        log.info("Not actually nuking anything since --dry-run was passed")
        return

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
    if 'targets' not in ctx.config:
        return
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


def nuke_one(ctx, target, should_unlock, synch_clocks, reboot_all,
             check_locks, noipmi):
    ret = None
    ctx = argparse.Namespace(
        config=dict(targets=target),
        owner=ctx.owner,
        check_locks=check_locks,
        synch_clocks=synch_clocks,
        reboot_all=reboot_all,
        teuthology_config=config.to_dict(),
        name=ctx.name,
        noipmi=noipmi,
    )
    try:
        nuke_helper(ctx, should_unlock)
    except Exception:
        log.exception('Could not nuke %s' % target)
        # not re-raising the so that parallel calls aren't killed
        ret = target
    else:
        if should_unlock:
            unlock_one(ctx, target.keys()[0], ctx.owner)
    return ret


def nuke_helper(ctx, should_unlock):
    # ensure node is up with ipmi
    (target,) = ctx.config['targets'].keys()
    host = target.split('@')[-1]
    shortname = host.split('.')[0]
    if should_unlock:
        if 'vpm' in shortname:
            return
        status_info = get_status(host)
        if status_info['is_vm'] and status_info['machine_type'] == 'openstack':
            return
    log.debug('shortname: %s' % shortname)
    log.debug('{ctx}'.format(ctx=ctx))
    if (not ctx.noipmi and 'ipmi_user' in config and
            'vpm' not in shortname):
        console = orchestra.remote.getRemoteConsole(
            name=host,
            ipmiuser=config['ipmi_user'],
            ipmipass=config['ipmi_password'],
            ipmidomain=config['ipmi_domain'])
        cname = '{host}.{domain}'.format(
            host=shortname,
            domain=config['ipmi_domain'])
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
        # does not check to ensure if the node is 'up'
        # we want to be able to nuke a downed node
        check_lock(ctx, None, check_up=False)
    add_remotes(ctx, None)
    connect(ctx, None)

    log.info("Clearing teuthology firewall rules...")
    clear_firewall(ctx)
    log.info("Cleared teuthology firewall rules.")

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
        log.info("Terminating Hadoop services...")
        kill_hadoop(ctx)

    log.info("Force remove ceph packages")
    remove_ceph_packages(ctx)
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

    remove_configuration_files(ctx)
    log.info('Removing any multipath config/pkgs...')
    undo_multipath(ctx)
    log.info('Resetting syslog output locations...')
    reset_syslog_dir(ctx)
    log.info('Clearing filesystem of test data...')
    remove_testing_tree(ctx)
    log.info('Filesystem cleared.')
    remove_yum_timedhosts(ctx)
    remove_installed_packages(ctx)
    log.info('Installed packages removed.')
