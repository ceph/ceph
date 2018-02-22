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

    @decorators.needs_root
    def zap(self, args):
        device = args.device
        lv = api.get_lv_from_argument(device)
        if lv:
            # we are zapping a logical volume
            path = lv.lv_path
        else:
            # we are zapping a partition
            #TODO: ensure device is a partition
            path = device

        mlogger.info("Zapping: %s", path)

        # check if there was a pv created with the
        # name of device
        pv = api.get_pv(pv_name=device)
        if pv:
            vg_name = pv.vg_name
            lv = api.get_lv(vg_name=vg_name)

        dmcrypt = False
        dmcrypt_uuid = None
        if lv:
            osd_path = "/var/lib/ceph/osd/{}-{}".format(lv.tags['ceph.cluster_name'], lv.tags['ceph.osd_id'])
            dmcrypt_uuid = lv.lv_uuid
            dmcrypt = lv.encrypted
            if system.path_is_mounted(osd_path):
                mlogger.info("Unmounting %s", osd_path)
                system.unmount(osd_path)
        else:
            # we're most likely dealing with a partition here, check to
            # see if it was encrypted
            partuuid = disk.get_partuuid(device)
            if encryption.status("/dev/mapper/{}".format(partuuid)):
                dmcrypt_uuid = partuuid
                dmcrypt = True

        if dmcrypt and dmcrypt_uuid:
            dmcrypt_path = "/dev/mapper/{}".format(dmcrypt_uuid)
            mlogger.info("Closing encrypted path %s", dmcrypt_path)
            encryption.dmcrypt_close(dmcrypt_path)

        if args.destroy and pv:
            logger.info("Found a physical volume created from %s, will destroy all it's vgs and lvs", device)
            vg_name = pv.vg_name
            mlogger.info("Destroying volume group %s because --destroy was given", vg_name)
            api.remove_vg(vg_name)
            mlogger.info("Destroying physical volume %s because --destroy was given", device)
            api.remove_pv(device)
        elif args.destroy and not pv:
            mlogger.info("Skipping --destroy because no associated physical volumes are found for %s", device)

        wipefs(path)
        zap_data(path)

        if lv and not pv:
            # remove all lvm metadata
            lv.clear_tags()

        terminal.success("Zapping successful for: %s" % path)

    def main(self):
        sub_command_help = dedent("""
        Zaps the given logical volume, raw device or partition for reuse by ceph-volume.
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
            'device',
            metavar='DEVICE',
            nargs='?',
            help='Path to an lv (as vg/lv), partition (as /dev/sda1) or device (as /dev/sda)'
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
