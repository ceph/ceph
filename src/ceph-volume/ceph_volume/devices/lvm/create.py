from __future__ import print_function
from textwrap import dedent
from ceph_volume.util import system
from .common import create_parser
from .prepare import Prepare
from .activate import Activate


class Create(object):

    help = 'Create a new OSD from  an LVM device'

    def __init__(self, argv):
        self.argv = argv

    def create(self, args):
        if not args.osd_fsid:
            args.osd_fsid = system.generate_uuid()
        if not args.osd_id:
            args.osd_id = prepare_utils.create_id(args.osd_fsid)
        Prepare([]).prepare(args)
        Activate([]).activate(args)

    def main(self):
        sub_command_help = dedent("""
        Create an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, adding
        all the metadata to the logical volumes using LVM tags, and starting
        the OSD daemon.

        Most basic Usage looks like (journal will be collocated from the same volume group):

            ceph-volume lvm create --data {volume group name}


        Example calls for supported scenarios:

        Dedicated volume group for Journal(s)
        -------------------------------------

          Existing logical volume (lv) or device:

              ceph-volume lvm create --data {logical volume} --journal /path/to/{lv}|{device}

          Or:

              ceph-volume lvm create --data {data volume group} --journal {journal volume group}

        Collocated (same group) for data and journal
        --------------------------------------------

              ceph-volume lvm create --data {volume group}

        """)
        parser = create_parser(
            prog='ceph-volume lvm create',
            description=sub_command_help,
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        self.create(args)
