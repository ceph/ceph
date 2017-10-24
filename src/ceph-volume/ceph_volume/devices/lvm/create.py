from __future__ import print_function
from textwrap import dedent
from ceph_volume.util import system
from ceph_volume import decorators
from .common import create_parser
from .prepare import Prepare
from .activate import Activate


class Create(object):

    help = 'Create a new OSD from  an LVM device'

    def __init__(self, argv):
        self.argv = argv

    @decorators.needs_root
    def create(self, args):
        if not args.osd_fsid:
            args.osd_fsid = system.generate_uuid()
        Prepare([]).prepare(args)
        Activate([]).activate(args)

    def main(self):
        sub_command_help = dedent("""
        Create an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, adding
        all the metadata to the logical volumes using LVM tags, and starting
        the OSD daemon.

        Example calls for supported scenarios:

        Filestore
        ---------

          Existing logical volume (lv) or device:

              ceph-volume lvm create --filestore --data {vg name/lv name} --journal /path/to/device

          Or:

              ceph-volume lvm create --filestore --data {vg name/lv name} --journal {vg name/lv name}

        """)
        parser = create_parser(
            prog='ceph-volume lvm create',
            description=sub_command_help,
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if args.bluestore is None and args.filestore is None:
            args.bluestore = True
        self.create(args)
