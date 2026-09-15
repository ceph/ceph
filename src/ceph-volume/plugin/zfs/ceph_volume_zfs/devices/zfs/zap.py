import argparse
import os
import re
from textwrap import dedent

from ceph_volume import conf, process, terminal
from ceph_volume_zfs.util.disk import (
    get_gpart_info,
    get_zpool_membership,
    get_mount_info,
    get_swap_info,
    get_zpool_ceph_properties,
    zpool_is_ceph_managed,
)


class Zap(object):

    help = 'Zap a device'

    def __init__(self, argv):
        self.argv = argv

    def build_commands(self, diskname, gpart):
        """
        Returns two lists:
          - labelclear_commands: best-effort. A failure here (e.g.
            "nothing to clear" because a previous zap attempt already
            cleared it, or there was never a label to begin with) is
            logged and does NOT stop the zap -- it can't leave the
            disk in a worse state either way.
          - destroy_commands: strict. Deletes each partition
            individually by index, THEN destroys the now-empty
            scheme, then zeroes the first few MB. If any step fails,
            the caller stops immediately.

            Deleting partitions one at a time before destroying the
            scheme is required here: `gpart destroy -F` on a scheme
            that still has partition entries reliably fails with
            "Device busy" on this system, even with -F, regardless of
            whether anything is actually mounted/imported/active on
            them. Destroying an already-empty scheme does not hit
            this.

        labelclear only targets actual freebsd-zfs partitions -- if
        there are none, labelclear_commands is empty.
        """
        zfs_partitions = [p for p in gpart.get('partitions', []) if p.get('type') == 'freebsd-zfs']
        labelclear_targets = ['/dev/{}'.format(p['name']) for p in zfs_partitions]

        labelclear_commands = [
            ['/sbin/zpool', 'labelclear', '-f', target] for target in labelclear_targets
        ]

        destroy_commands = []
        for part in gpart.get('partitions', []):
            m = re.match(r'.*p(\d+)$', part['name'])
            if not m:
                # Unexpected partition naming -- skip rather than
                # guess a wrong index; gpart destroy will fail loudly
                # afterward if this partition is still present, which
                # is safer than deleting the wrong index.
                continue
            index = m.group(1)
            destroy_commands.append(['/sbin/gpart', 'delete', '-i', index, diskname])
        destroy_commands.append(['/sbin/gpart', 'destroy', '-F', diskname])
        destroy_commands.append(
            ['/bin/dd', 'if=/dev/zero', 'of=/dev/{}'.format(diskname), 'bs=1m', 'count=10']
        )
        return labelclear_commands, destroy_commands

    def _osd_path(self, osd_id):
        return '/var/lib/ceph/osd/{}-{}'.format(conf.cluster, osd_id)

    def remove_osd_path(self, osd_id):
        """
        Unmounts and removes /var/lib/ceph/osd/<cluster>-<id>, left
        behind after the OSD's pool is destroyed. It's a tmpfs, so it
        would vanish on reboot anyway -- but until then it sits there
        with a `block` symlink pointing at a zvol that no longer
        exists, which is confusing at best.

        Best-effort: failures here are warned about, not fatal. The
        pool is already gone by this point, so aborting would leave
        things in a worse state than continuing.
        """
        path = self._osd_path(osd_id)
        if not os.path.exists(path):
            return
        if os.path.ismount(path):
            self._vlog('Unmounting {}'.format(path))
            out, err, rc = process.call(['/sbin/umount', path])
            if rc != 0:
                terminal.warning(
                    'Could not unmount {} (rc={}); leaving it in place.'.format(path, rc)
                )
                return
        try:
            os.rmdir(path)
            terminal.success('Removed {}'.format(path))
        except OSError as error:
            terminal.warning('Could not remove {}: {}'.format(path, error))

    def _vlog(self, msg):
        if getattr(conf, 'verbose', False):
            terminal.info(msg)

    def zap_device(self, diskname):
        """
        diskname: bare device name, e.g. 'ada0' (no /dev/ prefix)
        """
        gpart = get_gpart_info(diskname)
        zpool = get_zpool_membership(diskname)
        mount = get_mount_info(diskname, partitions=gpart.get('partitions'))
        swap = get_swap_info(diskname, partitions=gpart.get('partitions'))
        destroyed_pool = False

        # Always show current state first, regardless of whether we
        # go on to refuse -- a refusal without this is a black box:
        # the user has no way to see what's actually on the disk that
        # triggered it.
        terminal.info('/dev/{}: current state:'.format(diskname))
        if gpart.get('empty'):
            terminal.info('  no partition table')
        else:
            if gpart.get('scheme'):
                terminal.info('  partition scheme: {}'.format(gpart.get('scheme')))
            for part in gpart.get('partitions', []):
                terminal.info('  {}  {}  {}'.format(part['name'], part['type'], part['size_human']))
        if zpool.get('in_pool'):
            terminal.info('  zpool membership: "{}"'.format(zpool.get('pool_name')))
        if mount.get('mounted'):
            terminal.info('  mounted at: {}'.format(', '.join(mount.get('mountpoints', []))))
        if swap.get('active'):
            terminal.info('  active swap: {}'.format(', '.join(swap.get('devices', []))))

        # Hard refusals -- no --force override for any of these.
        # zap's job is to blank a disk, not to fight through an
        # actively-used one.
        if mount.get('mounted'):
            terminal.error(
                'Refusing to zap /dev/{}: has mounted filesystem(s) at {}'.format(
                    diskname, ', '.join(mount.get('mountpoints', []))
                )
            )
            return False

        if swap.get('active'):
            terminal.error(
                'Refusing to zap /dev/{}: has active swap on {}'.format(
                    diskname, ', '.join(swap.get('devices', []))
                )
            )
            return False

        if zpool.get('in_pool'):
            pool_name = zpool.get('pool_name')
            if not self.args.destroy:
                terminal.error(
                    'Refusing to zap /dev/{}: already a member of imported '
                    'zpool "{}". If this is an OSD created by "ceph-volume '
                    'zfs prepare", use --destroy to remove it.'.format(
                        diskname, pool_name
                    )
                )
                return False

            # --destroy only applies to pools THIS PLUGIN created,
            # identified by the ceph:managed_by property prepare sets.
            # Any other pool -- zroot, a user's data pool -- has no
            # such marker and is never destroyed, no matter what flags
            # are passed.
            if not zpool_is_ceph_managed(pool_name):
                terminal.error(
                    'Refusing to destroy zpool "{}": it is not marked as '
                    'created by ceph-volume-zfs (no '
                    'ceph:managed_by=ceph-volume-zfs property). --destroy '
                    'only removes pools this plugin made.'.format(pool_name)
                )
                return False

            props = get_zpool_ceph_properties(pool_name)
            terminal.warning(
                '--destroy: zpool "{}" is a ceph-volume-zfs OSD '
                '(osd_id={}, osd_fsid={}) and will be DESTROYED.'.format(
                    pool_name,
                    props.get('osd_id', 'unknown'),
                    props.get('osd_fsid', 'unknown'),
                )
            )
            terminal.warning(
                '  note: this removes local storage only -- it does not '
                'purge the OSD from the cluster ("ceph osd purge" is a '
                'separate, deliberate step).'
            )
            destroy_cmd = ['/sbin/zpool', 'destroy', pool_name]
            terminal.warning('  ' + ' '.join(destroy_cmd))
            osd_id_plan = props.get('osd_id')
            if osd_id_plan:
                terminal.warning('  ' + ' '.join(
                    ['/sbin/umount', self._osd_path(osd_id_plan)]) + '  (if mounted)')
                terminal.warning('  rmdir {}'.format(self._osd_path(osd_id_plan)))
            if not self.args.force:
                terminal.info(
                    '/dev/{}: dry run only -- pass --force to actually '
                    'execute the above.'.format(diskname)
                )
                return True
            out, err, rc = process.call(destroy_cmd)
            if rc != 0:
                terminal.error(
                    'Failed to destroy zpool "{}" (rc={}), stopping.'.format(pool_name, rc)
                )
                return False
            terminal.success('Destroyed zpool "{}"'.format(pool_name))
            destroyed_pool = True
            osd_id = props.get('osd_id')
            if osd_id:
                self.remove_osd_path(osd_id)
            # Re-read disk state: with the pool gone, the disk may now
            # be blank, or may still carry a partition table to clear
            # in the normal zap path below.
            gpart = get_gpart_info(diskname)

        if gpart.get('empty'):
            if destroyed_pool:
                terminal.success(
                    '/dev/{} is now pristine (pool destroyed, no partition '
                    'table remained).'.format(diskname)
                )
            else:
                terminal.info(
                    '/dev/{} has no existing partition table -- nothing to '
                    'zap.'.format(diskname)
                )
            return True

        commands_labelclear, commands_destroy = self.build_commands(diskname, gpart)
        all_commands = commands_labelclear + commands_destroy

        terminal.warning('/dev/{}: commands to run:'.format(diskname))
        for cmd in all_commands:
            terminal.warning('  ' + ' '.join(cmd))

        if not self.args.force:
            terminal.info(
                '/dev/{}: dry run only -- pass --force to actually execute the above.'.format(diskname)
            )
            return True

        # labelclear is best-effort: a target with no actual ZFS label
        # to clear -- because it was already cleared by a previous
        # zap attempt, or never had one -- fails with rc != 0, and
        # that's expected, not a reason to stop. It can't leave the
        # disk worse off either way.
        for cmd in commands_labelclear:
            out, err, rc = process.call(cmd)
            if rc != 0:
                terminal.warning(
                    '/dev/{}: labelclear on {} found nothing to clear (rc={}), continuing.'.format(
                        diskname, cmd[-1], rc
                    )
                )

        # gpart destroy and dd are strict: either can leave the disk
        # in a half-modified state if they fail, so a failure here
        # stops the zap immediately.
        for cmd in commands_destroy:
            out, err, rc = process.call(cmd)
            if rc != 0:
                terminal.error(
                    '/dev/{}: command failed (rc={}), stopping -- remaining commands were NOT run: {}'.format(
                        diskname, rc, ' '.join(cmd)
                    )
                )
                return False

        terminal.success('Zapped /dev/{} -- disk is now pristine (no GPT).'.format(diskname))
        return True

    def main(self):
        sub_command_help = dedent("""
        Zap one or more devices: wipes any existing partition table
        and ZFS labels, leaving a pristine disk with no GPT.

        Without --force, this only PRINTS the commands that would be
        run -- nothing is changed. Pass --force to actually execute
        them.

        Devices with a mounted filesystem, or that are members of a
        currently-imported zpool, are always refused -- there is no
        override for either.

        The one exception is --destroy: if the device holds a zpool
        that THIS PLUGIN created (marked with
        ceph:managed_by=ceph-volume-zfs by "ceph-volume zfs prepare"),
        --destroy will remove that pool before zapping. Pools without
        that marker -- zroot, your own data pools -- are still refused.
        --destroy removes local storage only; purging the OSD from the
        cluster ("ceph osd purge") remains a separate step.

        Examples:
          ceph-volume zfs zap /dev/ada0                      # dry run, prints commands
          ceph-volume zfs zap --force /dev/ada0              # actually zaps it
          ceph-volume zfs zap --destroy --force /dev/ada0    # destroys our OSD pool, then zaps
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs zap',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            'devices',
            metavar='DEVICES',
            nargs='*',
            default=[],
            help='Path to one or more devices, e.g. /dev/ada0',
        )
        parser.add_argument(
            '--destroy',
            action='store_true',
            default=False,
            help=(
                'Destroy the zpool on this device first, but ONLY if it was '
                'created by "ceph-volume zfs prepare" (identified by its '
                'ceph:managed_by property). Any other pool is refused. '
                'Removes local storage only -- does not purge the OSD from '
                'the cluster.'
            ),
        )
        parser.add_argument(
            '--force',
            action='store_true',
            default=False,
            help='Actually execute the zap. Without this, commands are only printed.',
        )
        self.args = parser.parse_args(self.argv)

        if not self.args.devices:
            parser.print_help()
            return

        for device in self.args.devices:
            diskname = device.replace('/dev/', '')
            self.zap_device(diskname)

