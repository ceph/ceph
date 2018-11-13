import argparse
import logging

from textwrap import dedent

from ceph_volume import decorators, terminal, process
from ceph_volume.api import lvm as api
from ceph_volume.util import system, encryption, disk

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

    @decorators.needs_root
    def zap(self, args):
        for device in args.devices:
            if disk.is_mapper_device(device):
                terminal.error("Refusing to zap the mapper device: {}".format(device))
                raise SystemExit(1)
            lv = api.get_lv_from_argument(device)
            if lv:
                # we are zapping a logical volume
                path = lv.lv_path
                self.unmount_lv(lv)
            else:
                # we are zapping a partition
                #TODO: ensure device is a partition
                path = device
                # check to if it is encrypted to close
                partuuid = disk.get_partuuid(device)
                if encryption.status("/dev/mapper/{}".format(partuuid)):
                    dmcrypt_uuid = partuuid
                    self.dmcrypt_close(dmcrypt_uuid)

            mlogger.info("Zapping: %s", path)

            # check if there was a pv created with the
            # name of device
            pvs = api.PVolumes()
            pvs.filter(pv_name=device)
            vgs = set([pv.vg_name for pv in pvs])
            for pv in pvs:
                vg_name = pv.vg_name
                lv = None
                if pv.lv_uuid:
                    lv = api.get_lv(vg_name=vg_name, lv_uuid=pv.lv_uuid)

                if lv:
                    self.unmount_lv(lv)

            if args.destroy:
                for vg_name in vgs:
                    mlogger.info("Destroying volume group %s because --destroy was given", vg_name)
                    api.remove_vg(vg_name)
                if not lv:
                    mlogger.info("Destroying physical volume %s because --destroy was given", device)
                    api.remove_pv(device)

            wipefs(path)
            zap_data(path)

            if lv and not pvs:
                if args.destroy:
                    lvs = api.Volumes()
                    lvs.filter(vg_name=lv.vg_name)
                    if len(lvs) <= 1:
                        mlogger.info('Only 1 LV left in VG, will proceed to destroy volume group %s', lv.vg_name)
                        api.remove_vg(lv.vg_name)
                    else:
                        mlogger.info('More than 1 LV left in VG, will proceed to destroy LV only')
                        mlogger.info('Removing LV because --destroy was given: %s', lv)
                        api.remove_lv(lv)
                else:
                    # just remove all lvm metadata, leaving the LV around
                    lv.clear_tags()

        terminal.success("Zapping successful for: %s" % ", ".join(args.devices))

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
        args = parser.parse_args(self.argv)
        self.zap(args)
