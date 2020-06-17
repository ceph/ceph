import argparse
import datetime
import json
import logging
import os
import subprocess

import yaml

import teuthology
from teuthology import provision
from teuthology.lock.ops import unlock_one
from teuthology.lock.query import is_vm, list_locks, \
    find_stale_locks, get_status
from teuthology.lock.util import locked_since_seconds
from teuthology.nuke.actions import (
    check_console, clear_firewall, shutdown_daemons, remove_installed_packages,
    reboot, remove_osd_mounts, remove_osd_tmpfs, kill_hadoop,
    remove_ceph_packages, synch_clocks, unlock_firmware_repo,
    remove_configuration_files, undo_multipath, reset_syslog_dir,
    remove_ceph_data, remove_testing_tree, remove_yum_timedhosts,
    kill_valgrind,
)
from teuthology.config import config, FakeNamespace
from teuthology.misc import (
    canonicalize_hostname, config_file, decanonicalize_hostname, merge_configs,
    get_user, sh
)
from teuthology.openstack import OpenStack, OpenStackInstance, enforce_json_dictionary
from teuthology.orchestra.remote import Remote
from teuthology.parallel import parallel
from teuthology.task.internal import check_lock, add_remotes, connect

log = logging.getLogger(__name__)


def openstack_volume_id(volume):
    return (volume.get('ID') or volume['id'])


def openstack_volume_name(volume):
    return (volume.get('Display Name') or
            volume.get('display_name') or
            volume.get('Name') or
            volume.get('name') or "")


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
    for (instance_id, instance) in instances.items():
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
    OpenStack().run("volume delete " + id + " || true")


def stale_openstack_volumes(ctx, volumes):
    now = datetime.datetime.now()
    for volume in volumes:
        volume_id = openstack_volume_id(volume)
        try:
            volume = json.loads(OpenStack().run("volume show -f json " +
                                                volume_id))
        except subprocess.CalledProcessError:
            log.debug("stale-openstack: {id} disappeared, ignored"
                      .format(id=volume_id))
            continue
        volume_name = openstack_volume_name(volume)
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
    for (name, node) in locked_nodes.items():
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
    volumes = json.loads(OpenStack().run("volume list -f json --long"))
    remove_me = [openstack_volume_id(v) for v in volumes
                 if 'REMOVE-ME' in openstack_volume_name(v)]
    for i in remove_me:
        log.info("Trying to remove stale volume %s" % i)
        openstack_delete_volume(i)


def main(args):
    ctx = FakeNamespace(args)
    if ctx.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    info = {}
    if ctx.archive:
        ctx.config = config_file(ctx.archive + '/config.yaml')
        ifn = os.path.join(ctx.archive, 'info.yaml')
        if os.path.exists(ifn):
            with open(ifn, 'r') as fd:
                info = yaml.safe_load(fd.read())
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
        for target, hostkey in ctx.config['targets'].items():
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
            unlock_one(ctx, list(target.keys())[0], ctx.owner)
    return ret


def nuke_helper(ctx, should_unlock):
    # ensure node is up with ipmi
    (target,) = ctx.config['targets'].keys()
    host = target.split('@')[-1]
    shortname = host.split('.')[0]
    if should_unlock:
        if is_vm(shortname):
            return
    log.debug('shortname: %s' % shortname)
    if ctx.check_locks:
        # does not check to ensure if the node is 'up'
        # we want to be able to nuke a downed node
        check_lock.check_lock(ctx, None, check_up=False)
    status = get_status(host)
    if status['machine_type'] in provision.fog.get_types():
        remote = Remote(host)
        remote.console.power_off()
        return
    elif status['machine_type'] in provision.pelagos.get_types():
        provision.pelagos.park_node(host)
        return

    if (not ctx.noipmi and 'ipmi_user' in config and
            'vpm' not in shortname):
        try:
            check_console(host)
        except Exception:
            log.exception('')
            log.info("Will attempt to connect via SSH")
            remote = Remote(host)
            remote.connect()
    add_remotes(ctx, None)
    connect(ctx, None)
    clear_firewall(ctx)
    shutdown_daemons(ctx)
    kill_valgrind(ctx)
    # Try to remove packages before reboot
    remove_installed_packages(ctx)
    remotes = ctx.cluster.remotes.keys()
    reboot(ctx, remotes)
    # shutdown daemons again incase of startup
    shutdown_daemons(ctx)
    remove_osd_mounts(ctx)
    remove_osd_tmpfs(ctx)
    kill_hadoop(ctx)
    remove_ceph_packages(ctx)
    synch_clocks(remotes)
    unlock_firmware_repo(ctx)
    remove_configuration_files(ctx)
    undo_multipath(ctx)
    reset_syslog_dir(ctx)
    remove_ceph_data(ctx)
    remove_testing_tree(ctx)
    remove_yum_timedhosts(ctx)
    # Once again remove packages after reboot
    remove_installed_packages(ctx)
    log.info('Installed packages removed.')
