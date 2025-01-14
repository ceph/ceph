from __future__ import print_function
import logging
from textwrap import dedent
from ceph_volume import objectstore
from .common import prepare_parser


logger = logging.getLogger(__name__)


class Prepare(object):

    help = 'Format an LVM device and associate it with an OSD'

    def __init__(self, argv, args=None):
        self.objectstore = None
        self.argv = argv
        self.args = args
        self.osd_id = None

    def main(self):
        sub_command_help = dedent("""
        Prepare an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, and
        finally by adding all the metadata to the logical volumes using LVM
        tags, so that it can later be discovered.

        Once the OSD is ready, an ad-hoc systemd unit will be enabled so that
        it can later get activated and the OSD daemon can get started.

        Encryption is supported via dmcrypt and the --dmcrypt flag.

        Existing logical volume (lv):

            ceph-volume lvm prepare --data {vg/lv}

        Existing block device (a logical volume will be created):

            ceph-volume lvm prepare --data /path/to/device

        Optionally, can consume db and wal devices, partitions or logical
        volumes. A device will get a logical volume, partitions and existing
        logical volumes will be used as is:

            ceph-volume lvm prepare --data {vg/lv} --block.wal {partition} --block.db {/path/to/device}
        """)
        parser = prepare_parser(
            prog='ceph-volume lvm prepare',
            description=sub_command_help,
        )
        if len(self.argv) == 0 and self.args is None:
            print(sub_command_help)
            return
        if self.args is None:
            self.args = parser.parse_args(self.argv)
        if self.args.bluestore:
            self.args.objectstore = 'bluestore'
        self.objectstore = objectstore.mapping['LVM'][self.args.objectstore](args=self.args)
        self.objectstore.safe_prepare()
