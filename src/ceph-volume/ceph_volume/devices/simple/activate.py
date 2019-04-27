from __future__ import print_function
import argparse
import base64
import glob
import json
import logging
import os
from textwrap import dedent
from ceph_volume import process, decorators, terminal, conf
from ceph_volume.util import system, disk
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.systemd import systemctl


logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)


class Activate(object):

    help = 'Enable systemd units to mount configured devices and start a Ceph OSD'

    def __init__(self, argv, from_trigger=False):
        self.argv = argv
        self.from_trigger = from_trigger
        self.skip_systemd = False

    def validate_devices(self, json_config):
        """
        ``json_config`` is the loaded dictionary coming from the JSON file. It is usually mixed with
        other non-device items, but for sakes of comparison it doesn't really matter. This method is
        just making sure that the keys needed exist
        """
        devices = json_config.keys()
        try:
            objectstore = json_config['type']
        except KeyError:
            logger.warning('"type" was not defined, will assume "bluestore"')
            objectstore = 'bluestore'

        # Go through all the device combinations that are absolutely required,
        # raise an error describing what was expected and what was found
        # otherwise.
        if objectstore == 'filestore':
            if {'data', 'journal'}.issubset(set(devices)):
                return True
            else:
                found = [i for i in devices if i in ['data', 'journal']]
                mlogger.error("Required devices (data, and journal) not present for filestore")
                mlogger.error('filestore devices found: %s', found)
                raise RuntimeError('Unable to activate filestore OSD due to missing devices')
        else:
            # This is a bit tricky, with newer bluestore we don't need data, older implementations
            # do (e.g. with ceph-disk). ceph-volume just uses a tmpfs that doesn't require data.
            if {'block', 'data'}.issubset(set(devices)):
                return True
            else:
                bluestore_devices = ['block.db', 'block.wal', 'block', 'data']
                found = [i for i in devices if i in bluestore_devices]
                mlogger.error("Required devices (block and data) not present for bluestore")
                mlogger.error('bluestore devices found: %s', found)
                raise RuntimeError('Unable to activate bluestore OSD due to missing devices')

    def get_device(self, uuid):
        """
        If a device is encrypted, it will decrypt/open and return the mapper
        path, if it isn't encrypted it will just return the device found that
        is mapped to the uuid.  This will make it easier for the caller to
        avoid if/else to check if devices need decrypting

        :param uuid: The partition uuid of the device (PARTUUID)
        """
        device = disk.get_device_from_partuuid(uuid)

        # If device is not found, it is fine to return an empty string from the
        # helper that finds `device`. If it finds anything and it is not
        # encrypted, just return what was found
        if not self.is_encrypted or not device:
            return device

        if self.encryption_type == 'luks':
            encryption_utils.luks_open(self.dmcrypt_secret, device, uuid)
        else:
            encryption_utils.plain_open(self.dmcrypt_secret, device, uuid)

        return '/dev/mapper/%s' % uuid

    def enable_systemd_units(self, osd_id, osd_fsid):
        """
        * disables the ceph-disk systemd units to prevent them from running when
          a UDEV event matches Ceph rules
        * creates the ``simple`` systemd units to handle the activation and
          startup of the OSD with ``osd_id`` and ``osd_fsid``
        * enables the OSD systemd unit and finally starts the OSD.
        """
        if not self.from_trigger and not self.skip_systemd:
            # means it was scanned and now activated directly, so ensure that
            # ceph-disk units are disabled, and that the `simple` systemd unit
            # is created and enabled

            # enable the ceph-volume unit for this OSD
            systemctl.enable_volume(osd_id, osd_fsid, 'simple')

            # disable any/all ceph-disk units
            systemctl.mask_ceph_disk()
            terminal.warning(
                ('All ceph-disk systemd units have been disabled to '
                 'prevent OSDs getting triggered by UDEV events')
            )
        else:
            terminal.info('Skipping enabling of `simple` systemd unit')
            terminal.info('Skipping masking of ceph-disk systemd units')

        if not self.skip_systemd:
            # enable the OSD
            systemctl.enable_osd(osd_id)

            # start the OSD
            systemctl.start_osd(osd_id)
        else:
            terminal.info(
                'Skipping enabling and starting OSD simple systemd unit because --no-systemd was used'
            )

    @decorators.needs_root
    def activate(self, args):
        with open(args.json_config, 'r') as fp:
            osd_metadata = json.load(fp)

        # Make sure that required devices are configured
        self.validate_devices(osd_metadata)

        osd_id = osd_metadata.get('whoami', args.osd_id)
        osd_fsid = osd_metadata.get('fsid', args.osd_fsid)
        data_uuid = osd_metadata.get('data', {}).get('uuid')
        conf.cluster = osd_metadata.get('cluster_name', 'ceph')
        if not data_uuid:
            raise RuntimeError(
                'Unable to activate OSD %s - no "uuid" key found for data' % args.osd_id
            )

        # Encryption detection, and capturing of the keys to decrypt
        self.is_encrypted = osd_metadata.get('encrypted', False)
        self.encryption_type = osd_metadata.get('encryption_type')
        if self.is_encrypted:
            lockbox_secret = osd_metadata.get('lockbox.keyring')
            # write the keyring always so that we can unlock
            encryption_utils.write_lockbox_keyring(osd_id, osd_fsid, lockbox_secret)
            # Store the secret around so that the decrypt method can reuse
            raw_dmcrypt_secret = encryption_utils.get_dmcrypt_key(osd_id, osd_fsid)
            # Note how both these calls need b64decode. For some reason, the
            # way ceph-disk creates these keys, it stores them in the monitor
            # *undecoded*, requiring this decode call again. The lvm side of
            # encryption doesn't need it, so we are assuming here that anything
            # that `simple` scans, will come from ceph-disk and will need this
            # extra decode call here
            self.dmcrypt_secret = base64.b64decode(raw_dmcrypt_secret)

        cluster_name = osd_metadata.get('cluster_name', 'ceph')
        osd_dir = '/var/lib/ceph/osd/%s-%s' % (cluster_name, osd_id)

        # XXX there is no support for LVM here
        data_device = self.get_device(data_uuid)
        journal_device = self.get_device(osd_metadata.get('journal', {}).get('uuid'))
        block_device = self.get_device(osd_metadata.get('block', {}).get('uuid'))
        block_db_device = self.get_device(osd_metadata.get('block.db', {}).get('uuid'))
        block_wal_device = self.get_device(osd_metadata.get('block.wal', {}).get('uuid'))

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

        self.enable_systemd_units(osd_id, osd_fsid)

        terminal.success('Successfully activated OSD %s with FSID %s' % (osd_id, osd_fsid))

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
            '--all',
            help='Activate all OSDs with a OSD JSON config',
            action='store_true',
            default=False,
        )
        parser.add_argument(
            '--file',
            help='The path to a JSON file, from a scanned OSD'
        )
        parser.add_argument(
            '--no-systemd',
            dest='skip_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services',
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        if not args.file and not args.all:
            if not args.osd_id and not args.osd_fsid:
                terminal.error('ID and FSID are required to find the right OSD to activate')
                terminal.error('from a scanned OSD location in /etc/ceph/osd/')
                raise RuntimeError('Unable to activate without both ID and FSID')
        # don't allow a CLI flag to specify the JSON dir, because that might
        # implicitly indicate that it would be possible to activate a json file
        # at a non-default location which would not work at boot time if the
        # custom location is not exposed through an ENV var
        self.skip_systemd = args.skip_systemd
        json_dir = os.environ.get('CEPH_VOLUME_SIMPLE_JSON_DIR', '/etc/ceph/osd/')
        if args.all:
            if args.file or args.osd_id:
                mlogger.warn('--all was passed, ignoring --file and ID/FSID arguments')
            json_configs = glob.glob('{}/*.json'.format(json_dir))
            for json_config in json_configs:
                mlogger.info('activating OSD specified in {}'.format(json_config))
                args.json_config = json_config
                self.activate(args)
        else:
            if args.file:
                json_config = args.file
            else:
                json_config = os.path.join(json_dir, '%s-%s.json' % (args.osd_id, args.osd_fsid))
            if not os.path.exists(json_config):
                raise RuntimeError('Expected JSON config path not found: %s' % json_config)
            args.json_config = json_config
            self.activate(args)
