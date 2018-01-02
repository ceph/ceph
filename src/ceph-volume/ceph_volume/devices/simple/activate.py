from __future__ import print_function
import argparse
import json
import logging
import os
from textwrap import dedent
from ceph_volume import process, decorators, terminal
from ceph_volume.util import system, disk
from ceph_volume.systemd import systemctl


logger = logging.getLogger(__name__)


class Activate(object):

    help = 'Enable systemd units to mount configured devices and start a Ceph OSD'

    def __init__(self, argv, systemd=False):
        self.argv = argv
        self.systemd = systemd

    @decorators.needs_root
    def activate(self, args):
        with open(args.json_config, 'r') as fp:
            osd_metadata = json.load(fp)

        osd_id = osd_metadata.get('whoami', args.osd_id)
        osd_fsid = osd_metadata.get('fsid', args.osd_fsid)

        cluster_name = osd_metadata.get('cluster_name', 'ceph')
        osd_dir = '/var/lib/ceph/osd/%s-%s' % (cluster_name, osd_id)
        data_uuid = osd_metadata.get('data', {}).get('uuid')
        if not data_uuid:
            raise RuntimeError(
                'Unable to activate OSD %s - no "uuid" key found for data' % args.osd_id
            )
        data_device = disk.get_device_from_partuuid(data_uuid)
        journal_device = disk.get_device_from_partuuid(osd_metadata.get('journal', {}).get('uuid'))
        block_device = disk.get_device_from_partuuid(osd_metadata.get('block', {}).get('uuid'))
        block_db_device = disk.get_device_from_partuuid(osd_metadata.get('block.db', {}).get('uuid'))
        block_wal_device = disk.get_device_from_partuuid(
            osd_metadata.get('block.wal', {}).get('uuid')
        )

        if not system.device_is_mounted(data_device, destination=osd_dir):
            process.run(['mount', '-v', data_device, osd_dir])

        device_map = {
            'journal': journal_device,
            'block': block_device,
            'block.db': block_db_device,
            'block.wal': block_wal_device
        }

        for name, device in device_map.items():
            if not device:
                continue
            # always re-do the symlink regardless if it exists, so that the journal
            # device path that may have changed can be mapped correctly every time
            destination = os.path.join(osd_dir, name)
            process.run(['ln', '-snf', device, destination])

            # make sure that the journal has proper permissions
            system.chown(device)

        if not self.systemd:
            # enable the ceph-volume unit for this OSD
            systemctl.enable_volume(osd_id, osd_fsid, 'simple')

            # disable any/all ceph-disk units
            systemctl.mask_ceph_disk()

        # enable the OSD
        systemctl.enable_osd(osd_id)

        # start the OSD
        systemctl.start_osd(osd_id)

        if not self.systemd:
            terminal.success('Successfully activated OSD %s with FSID %s' % (osd_id, osd_fsid))
            terminal.warning(
                ('All ceph-disk systemd units have been disabled to '
                 'prevent OSDs getting triggered by UDEV events')
            )

    def main(self):
        sub_command_help = dedent("""
        Activate OSDs by mounting devices previously configured to their
        appropriate destination::

            ceph-volume simple activate {ID} {FSID}

        Or using a JSON file directly::

            ceph-volume simple activate --file /etc/ceph/osd/{ID}-{FSID}.json

        The OSD must have been "scanned" previously (see ``ceph-volume simple
        scan``), so that all needed OSD device information and metadata exist.

        A previously scanned OSD would exist like::

            /etc/ceph/osd/{ID}-{FSID}.json


        Environment variables supported:

        CEPH_VOLUME_SIMPLE_JSON_DIR: Directory location for scanned OSD JSON configs
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume simple activate',
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
            '--file',
            help='The path to a JSON file, from a scanned OSD'
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        if not args.file:
            if not args.osd_id and not args.osd_fsid:
                terminal.error('ID and FSID are required to find the right OSD to activate')
                terminal.error('from a scanned OSD location in /etc/ceph/osd/')
                raise RuntimeError('Unable to activate without both ID and FSID')
        # don't allow a CLI flag to specify the JSON dir, because that might
        # implicitly indicate that it would be possible to activate a json file
        # at a non-default location which would not work at boot time if the
        # custom location is not exposed through an ENV var
        json_dir = os.environ.get('CEPH_VOLUME_SIMPLE_JSON_DIR', '/etc/ceph/osd/')
        if args.file:
            json_config = args.file
        else:
            json_config = os.path.join(json_dir, '%s-%s.json' % (args.osd_id, args.osd_fsid))
        if not os.path.exists(json_config):
            raise RuntimeError('Expected JSON config path not found: %s' % json_config)
        args.json_config = json_config
        self.activate(args)
