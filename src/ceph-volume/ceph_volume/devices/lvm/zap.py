import argparse
import os
import logging

from textwrap import dedent

from ceph_volume import decorators, terminal, process
from ceph_volume.api import lvm as api
from ceph_volume.util import system, encryption, disk, arg_validators
from ceph_volume.util.device import Device

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)


def wipefs(path):
    """
    Removes the filesystem from an lv or partition.
    """
    process.run([
        'wipefs',
        '--all',
        path
    ])


def zap_data(path):
    """
    Clears all data from the given path. Path should be
    an absolute path to an lv or partition.

    10M of data is written to the path to make sure that
    there is no trace left of any previous Filesystem.
    """
    process.run([
        'dd',
        'if=/dev/zero',
        'of={path}'.format(path=path),
        'bs=1M',
        'count=10',
    ])


class Zap(object):

    help = 'Removes all data and filesystems from a logical volume or partition.'

    def __init__(self, argv):
        self.argv = argv

    def unmount_lv(self, lv):
        if lv.tags.get('ceph.cluster_name') and lv.tags.get('ceph.osd_id'):
            lv_path = "/var/lib/ceph/osd/{}-{}".format(lv.tags['ceph.cluster_name'], lv.tags['ceph.osd_id'])
        else:
            lv_path = lv.lv_path
        dmcrypt_uuid = lv.lv_uuid
        dmcrypt = lv.encrypted
        if system.path_is_mounted(lv_path):
            mlogger.info("Unmounting %s", lv_path)
            system.unmount(lv_path)
        if dmcrypt and dmcrypt_uuid:
            self.dmcrypt_close(dmcrypt_uuid)

    def zap_lv(self, device):
        """
        Device examples: vg-name/lv-name, /dev/vg-name/lv-name
        Requirements: Must be a logical volume (LV)
        """
        lv = api.get_lv(lv_name=device.lv_name, vg_name=device.vg_name)
        self.unmount_lv(lv)

        wipefs(device.abspath)
        zap_data(device.abspath)

        if self.args.destroy:
            lvs = api.Volumes()
            lvs.filter(vg_name=device.vg_name)
            if len(lvs) <= 1:
                mlogger.info('Only 1 LV left in VG, will proceed to destroy volume group %s', device.vg_name)
                api.remove_vg(device.vg_name)
            else:
                mlogger.info('More than 1 LV left in VG, will proceed to destroy LV only')
                mlogger.info('Removing LV because --destroy was given: %s', device.abspath)
                api.remove_lv(device.abspath)
        elif lv:
            # just remove all lvm metadata, leaving the LV around
            lv.clear_tags()

    def zap_partition(self, device):
        """
        Device example: /dev/sda1
        Requirements: Must be a partition
        """
        if device.is_encrypted:
            # find the holder
            holders = [
                '/dev/%s' % holder for holder in device.sys_api.get('holders', [])
            ]
            for mapper_uuid in os.listdir('/dev/mapper'):
                mapper_path = os.path.join('/dev/mapper', mapper_uuid)
                if os.path.realpath(mapper_path) in holders:
                    self.dmcrypt_close(mapper_uuid)

        if system.device_is_mounted(device.abspath):
            mlogger.info("Unmounting %s", device.abspath)
            system.unmount(device.abspath)

        wipefs(device.abspath)
        zap_data(device.abspath)

        if self.args.destroy:
            mlogger.info("Destroying partition since --destroy was used: %s" % device.abspath)
            disk.remove_partition(device)

    def zap_lvm_member(self, device):
        """
        An LVM member may have more than one LV and or VG, for example if it is
        a raw device with multiple partitions each belonging to a different LV

        Device example: /dev/sda
        Requirements: An LV or VG present in the device, making it an LVM member
        """
        for lv in device.lvs:
            self.zap_lv(Device(lv.lv_path))


    def zap_raw_device(self, device):
        """
        Any whole (raw) device passed in as input will be processed here,
        checking for LVM membership and partitions (if any).

        Device example: /dev/sda
        Requirements: None
        """
        if not self.args.destroy:
            # the use of dd on a raw device causes the partition table to be
            # destroyed
            mlogger.warning(
                '--destroy was not specified, but zapping a whole device will remove the partition table'
            )

        # look for partitions and zap those
        for part_name in device.sys_api.get('partitions', {}).keys():
            self.zap_partition(Device('/dev/%s' % part_name))

        wipefs(device.abspath)
        zap_data(device.abspath)

    @decorators.needs_root
    def zap(self):
        for device in self.args.devices:
            mlogger.info("Zapping: %s", device.abspath)
            if device.is_mapper:
                terminal.error("Refusing to zap the mapper device: {}".format(device))
                raise SystemExit(1)
            if device.is_lvm_member:
                self.zap_lvm_member(device)
            if device.is_lv:
                self.zap_lv(device)
            if device.is_partition:
                self.zap_partition(device)
            if device.is_device:
                self.zap_raw_device(device)

        terminal.success("Zapping successful for: %s" % ", ".join([str(d) for d in self.args.devices]))

    def dmcrypt_close(self, dmcrypt_uuid):
        dmcrypt_path = "/dev/mapper/{}".format(dmcrypt_uuid)
        mlogger.info("Closing encrypted path %s", dmcrypt_path)
        encryption.dmcrypt_close(dmcrypt_path)

    def main(self):
        sub_command_help = dedent("""
        Zaps the given logical volume(s), raw device(s) or partition(s) for reuse by ceph-volume.
        If given a path to a logical volume it must be in the format of vg/lv. Any
        filesystems present on the given device, vg/lv, or partition will be removed and
        all data will be purged.

        If the logical volume, raw device or partition is being used for any ceph related
        mount points they will be unmounted.

        However, the lv or partition will be kept intact.

        Example calls for supported scenarios:

          Zapping a logical volume:

              ceph-volume lvm zap {vg name/lv name}

          Zapping a partition:

              ceph-volume lvm zap /dev/sdc1

          Zapping many raw devices:

              ceph-volume lvm zap /dev/sda /dev/sdb /db/sdc

        If the --destroy flag is given and you are zapping a raw device or partition
        then all vgs and lvs that exist on that raw device or partition will be destroyed.

        This is especially useful if a raw device or partition was used by ceph-volume lvm create
        or ceph-volume lvm prepare commands previously and now you want to reuse that device.

        For example:

          ceph-volume lvm zap /dev/sda --destroy

        If the --destroy flag is given and you are zapping an lv then the lv is still
        kept intact for reuse.

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm zap',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'devices',
            metavar='DEVICES',
            nargs='*',
            type=arg_validators.ValidDevice(gpt_ok=True),
            default=[],
            help='Path to one or many lv (as vg/lv), partition (as /dev/sda1) or device (as /dev/sda)'
        )
        parser.add_argument(
            '--destroy',
            action='store_true',
            default=False,
            help='Destroy all volume groups and logical volumes if you are zapping a raw device or partition',
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        self.args = parser.parse_args(self.argv)
        self.zap()
