import argparse
import logging

from textwrap import dedent

from ceph_volume import decorators, terminal, process
from ceph_volume.api import lvm as api

logger = logging.getLogger(__name__)


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

        logger.info("Zapping: %s", path)
        terminal.write("Zapping: %s" % path)

        wipefs(path)
        zap_data(path)

        if lv:
            # remove all lvm metadata
            lv.clear_tags()

        terminal.success("Zapping successful for: %s" % path)

    def main(self):
        sub_command_help = dedent("""
        Zaps the given logical volume or partition. If given a path to a logical
        volume it must be in the format of vg/lv. Any filesystems present
        on the given lv or partition will be removed and all data will be purged.

        However, the lv or partition will be kept intact.

        Example calls for supported scenarios:

          Zapping a logical volume:

              ceph-volume lvm zap {vg name/lv name}

          Zapping a partition:

              ceph-volume lvm zap /dev/sdc1

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
            help='Path to an lv (as vg/lv) or to a partition like /dev/sda1'
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        self.zap(args)
