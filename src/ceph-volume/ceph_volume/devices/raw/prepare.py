from __future__ import print_function
import logging
import os
from textwrap import dedent
from ceph_volume import terminal, objectstore
from .common import create_parser

logger = logging.getLogger(__name__)


class Prepare(object):

    help = 'Format a raw device and associate it with a (BlueStore) OSD'

    def __init__(self, argv):
        self.argv = argv
        self.osd_id = None
        self.objectstore = None

    def main(self):
        sub_command_help = dedent("""
        Prepare an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting the volume.

        Once the OSD is ready, an ad-hoc systemd unit will be enabled so that
        it can later get activated and the OSD daemon can get started.

            ceph-volume raw prepare --bluestore --data {device}

        DB and WAL devices are supported.

            ceph-volume raw prepare --bluestore --data {device} --block.db {device} --block.wal {device}

        """)
        parser = create_parser(
            prog='ceph-volume raw prepare',
            description=sub_command_help,
        )
        if not self.argv:
            print(sub_command_help)
            return
        self.args = parser.parse_args(self.argv)
        if self.args.bluestore:
            self.args.objectstore = 'bluestore'
        if self.args.dmcrypt and not os.getenv('CEPH_VOLUME_DMCRYPT_SECRET'):
            terminal.error('encryption was requested (--dmcrypt) but environment variable ' \
                           'CEPH_VOLUME_DMCRYPT_SECRET is not set, you must set ' \
                           'this variable to provide a dmcrypt secret.')
            raise SystemExit(1)

        self.objectstore = objectstore.mapping['RAW'][self.args.objectstore](args=self.args)
        self.objectstore.safe_prepare(self.args)
