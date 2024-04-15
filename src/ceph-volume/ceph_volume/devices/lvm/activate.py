from __future__ import print_function
import argparse
import logging
from textwrap import dedent
from ceph_volume import objectstore


logger = logging.getLogger(__name__)


class Activate(object):
    help = 'Discover and mount the LVM device associated with an OSD ID and start the Ceph OSD'

    def __init__(self, argv, args=None):
        self.objectstore = None
        self.argv = argv
        self.args = args

    def main(self):
        sub_command_help = dedent("""
        Activate OSDs by discovering them with LVM and mounting them in their
        appropriate destination:

            ceph-volume lvm activate {ID} {FSID}

        The lvs associated with the OSD need to have been prepared previously,
        so that all needed tags and metadata exist.

        When migrating OSDs, or a multiple-osd activation is needed, the
        ``--all`` flag can be used instead of the individual ID and FSID:

            ceph-volume lvm activate --all

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'osd_id',
            metavar='ID',
            nargs='?',
            help='The ID of the OSD, usually an integer, like 0'
        )
        parser.add_argument(
            'osd_fsid',
            metavar='FSID',
            nargs='?',
            help='The FSID of the OSD, similar to a SHA1'
        )
        parser.add_argument(
            '--auto-detect-objectstore',
            action='store_true',
            help='Autodetect the objectstore by inspecting the OSD',
        )
        parser.add_argument(
            '--bluestore',
            action='store_true',
            help='force bluestore objectstore activation',
        )
        parser.add_argument(
            '--objectstore',
            dest='objectstore',
            help='The OSD objectstore.',
            default='bluestore',
            choices=['bluestore', 'seastore'],
            type=str,
        )
        parser.add_argument(
            '--all',
            dest='activate_all',
            action='store_true',
            help='Activate all OSDs found in the system',
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services',
        )
        parser.add_argument(
            '--no-tmpfs',
            action='store_true',
            help='Do not use a tmpfs mount for OSD data dir'
        )
        if len(self.argv) == 0 and self.args is None:
            print(sub_command_help)
            return
        if self.args is None:
            self.args = parser.parse_args(self.argv)
        if self.args.bluestore:
            self.args.objectstore = 'bluestore'
        self.objectstore = objectstore.mapping['LVM'][self.args.objectstore](args=self.args)
        if self.args.activate_all:
            self.objectstore.activate_all()
        else:
            self.objectstore.activate()
