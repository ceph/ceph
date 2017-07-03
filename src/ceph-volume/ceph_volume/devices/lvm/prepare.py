import argparse
from textwrap import dedent
from ceph_volume import process
from ceph_volume.systemd import systemctl
import api


class Prepare(object):

    help = 'Format an LVM device and associate it with an OSD'

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        sub_command_help = dedent("""
        Prepare an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, and
        finally by adding all the metadata to the logical volumes using LVM
        tags, so that it can later be discovered.

        Once the OSD is ready, an ad-hoc systemd unit will be enabled so that
        it can later get activated and the OSD daemon can get started.

        Most basic Usage looks like (journal will be collocated from the same volume group):

            ceph-volume lvm prepare --data {volume group name}


        Example calls for supported scenarios:

        Dedicated volume group for Journal(s)
        -------------------------------------

          Existing logical volume (lv) or device:

              ceph-volume lvm prepare --data {volume group} --journal /path/to/{lv}|{device}

          Or:

              ceph-volume lvm prepare --data {data volume group} --journal {journal volume group}

        Collocated (same group) for data and journal
        --------------------------------------------

              ceph-volume lvm prepare --data {volume group}

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        required_args = parser.add_argument_group('required arguments')
        parser.add_argument(
            '--journal',
            help='A logical group name, path to a logical volume, or path to a device',
        )
        required_args.add_argument(
            '--data',
            required=True,
            help='A logical group name or a path to a logical volume',
        )
        parser.add_argument(
            '--journal-size',
            default=5,
            metavar='GB',
            type=int,
            help='Size (in GB) A logical group name or a path to a logical volume',
        )
        parser.add_argument(
            '--bluestore',
            action='store_true', default=False,
            help='Use the bluestore objectstore (not currently supported)',
        )
        parser.add_argument(
            '--filestore',
            action='store_true', default=True,
            help='Use the filestore objectstore (currently the only supported object store)',
        )
        args = parser.parse_args(self.argv[1:])
        self.activate(args)
