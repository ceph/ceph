from __future__ import print_function
import argparse
import logging
from textwrap import dedent
from ceph_volume import objectstore


logger = logging.getLogger(__name__)

class Activate(object):

    help = 'Discover and prepare a data directory for a (BlueStore) OSD on a raw device'

    def __init__(self, argv, args=None):
        self.objectstore = None
        self.argv = argv
        self.args = args

    def main(self):
        sub_command_help = dedent("""
        Activate (BlueStore) OSD on a raw block device(s) based on the
        device label (normally the first block of the device).

            ceph-volume raw activate [/dev/sdb2 ...]

        or

            ceph-volume raw activate --osd-id NUM --osd-uuid UUID

        The device(s) associated with the OSD need to have been prepared
        previously, so that all needed tags and metadata exist.
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume raw activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            '--device',
            help='The device for the OSD to start'
        )
        parser.add_argument(
            '--osd-id',
            help='OSD ID to activate'
        )
        parser.add_argument(
            '--osd-uuid',
            help='OSD UUID to active'
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='This argument has no effect, this is here for backward compatibility.'
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
            '--block.db',
            dest='block_db',
            help='Path to bluestore block.db block device'
        )
        parser.add_argument(
            '--block.wal',
            dest='block_wal',
            help='Path to bluestore block.wal block device'
        )
        parser.add_argument(
            '--no-tmpfs',
            action='store_true',
            help='Do not use a tmpfs mount for OSD data dir'
            )

        if not self.argv:
            print(sub_command_help)
            return
        self.args = parser.parse_args(self.argv)

        devs = [self.args.device]
        if self.args.block_wal:
            devs.append(self.args.block_wal)
        if self.args.block_db:
            devs.append(self.args.block_db)
        self.objectstore = objectstore.mapping['RAW'][self.args.objectstore](args=self.args)
        self.objectstore.activate(devs=devs,
                                  start_osd_id=self.args.osd_id,
                                  start_osd_uuid=self.args.osd_uuid,
                                  tmpfs=not self.args.no_tmpfs)
