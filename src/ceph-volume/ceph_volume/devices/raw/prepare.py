from __future__ import print_function
import json
import logging
import os
from textwrap import dedent
from ceph_volume import terminal, objectstore
from ceph_volume.util import disk
from ceph_volume.util.device import Device
from .common import create_parser

logger = logging.getLogger(__name__)


def device_size(path):
    return disk.Size(b=Device(path).size)


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

        To preview the OSD without formatting the device:

            ceph-volume raw prepare --bluestore --data {device} --report --format json

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
        if self.args.report:
            self.report()
            return
        if self.args.dmcrypt:
            if not self.args.with_tpm and not os.getenv('CEPH_VOLUME_DMCRYPT_SECRET'):
                terminal.error('encryption was requested (--dmcrypt) but environment variable ' \
                               'CEPH_VOLUME_DMCRYPT_SECRET is not set, you must set ' \
                               'this variable to provide a dmcrypt secret or use --with-tpm ' \
                               'in order to enroll a tpm2 token.')
                raise SystemExit(1)

        self.objectstore = objectstore.mapping['RAW'][self.args.objectstore](args=self.args)
        self.objectstore.safe_prepare(self.args)

    def report(self):
        """Print the OSD that would be created and return."""
        plan = {
            'data': self.args.data,
            'data_size': device_size(self.args.data),
            'encryption': 'dmcrypt' if self.args.dmcrypt else None,
        }
        if self.args.block_db:
            plan['block_db'] = self.args.block_db
            plan['block_db_size'] = device_size(self.args.block_db)
        if self.args.block_wal:
            plan['block_wal'] = self.args.block_wal
            plan['block_wal_size'] = device_size(self.args.block_wal)
        if self.args.osd_id is not None:
            plan['osd_id'] = self.args.osd_id
        plan = {key: str(value) for key, value in plan.items()}
        if self.args.format == 'json':
            print(json.dumps([plan]))
        elif self.args.format == 'json-pretty':
            print(json.dumps([plan], indent=4, sort_keys=True))
        else:
            for key in ('data', 'block_db', 'block_wal'):
                if key not in plan:
                    continue
                size_key = 'data_size' if key == 'data' else '{}_size'.format(key)
                print('{}  {}  {}'.format(key, plan[key], plan[size_key]))
