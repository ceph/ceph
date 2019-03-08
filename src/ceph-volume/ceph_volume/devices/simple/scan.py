from __future__ import print_function
import argparse
import base64
import json
import logging
import os
from textwrap import dedent
from ceph_volume import decorators, terminal, conf
from ceph_volume.api import lvm
from ceph_volume.systemd import systemctl
from ceph_volume.util import arg_validators, system, disk, encryption
from ceph_volume.util.device import Device


logger = logging.getLogger(__name__)


def parse_keyring(file_contents):
    """
    Extract the actual key from a string. Usually from a keyring file, where
    the keyring will be in a client section. In the case of a lockbox, it is
    something like::

        [client.osd-lockbox.8d7a8ab2-5db0-4f83-a785-2809aba403d5]\n\tkey = AQDtoGha/GYJExAA7HNl7Ukhqr7AKlCpLJk6UA==\n

    From the above case, it would return::

        AQDtoGha/GYJExAA7HNl7Ukhqr7AKlCpLJk6UA==
    """
    # remove newlines that might be trailing
    keyring = file_contents.strip('\n')

    # Now split on spaces
    keyring = keyring.split(' ')[-1]

    # Split on newlines
    keyring = keyring.split('\n')[-1]

    return keyring.strip()


class Scan(object):

    help = 'Capture metadata from all running ceph-disk OSDs, OSD data partition or directory'

    def __init__(self, argv):
        self.argv = argv
        self._etc_path = '/etc/ceph/osd/'

    @property
    def etc_path(self):
        if os.path.isdir(self._etc_path):
            return self._etc_path

        if not os.path.exists(self._etc_path):
            os.mkdir(self._etc_path)
            return self._etc_path

        error = "OSD Configuration path (%s) needs to be a directory" % self._etc_path
        raise RuntimeError(error)

    def get_contents(self, path):
        with open(path, 'r') as fp:
            contents = fp.readlines()
        if len(contents) > 1:
            return ''.join(contents)
        return ''.join(contents).strip().strip('\n')

    def scan_device(self, path):
        device_metadata = {'path': None, 'uuid': None}
        if not path:
            return device_metadata
        if self.is_encrypted:
            encryption_metadata = encryption.legacy_encrypted(path)
            device_metadata['path'] = encryption_metadata['device']
            device_metadata['uuid'] = disk.get_partuuid(encryption_metadata['device'])
            return device_metadata
        # cannot read the symlink if this is tmpfs
        if os.path.islink(path):
            device = os.readlink(path)
        else:
            device = path
        lvm_device = lvm.get_lv_from_argument(device)
        if lvm_device:
            device_uuid = lvm_device.lv_uuid
        else:
            device_uuid = disk.get_partuuid(device)

        device_metadata['uuid'] = device_uuid
        device_metadata['path'] = device

        return device_metadata

    def scan_directory(self, path):
        osd_metadata = {'cluster_name': conf.cluster}
        directory_files = os.listdir(path)
        if 'keyring' not in directory_files:
            raise RuntimeError(
                'OSD files not found, required "keyring" file is not present at: %s' % path
            )
        for _file in os.listdir(path):
            file_path = os.path.join(path, _file)
            if os.path.islink(file_path):
                if os.path.exists(file_path):
                    osd_metadata[_file] = self.scan_device(file_path)
                else:
                    msg = 'broken symlink found %s -> %s' % (file_path, os.path.realpath(file_path))
                    terminal.warning(msg)
                    logger.warning(msg)

            if os.path.isdir(file_path):
                continue

            # the check for binary needs to go before the file, to avoid
            # capturing data from binary files but still be able to capture
            # contents from actual files later
            try:
                if system.is_binary(file_path):
                    logger.info('skipping binary file: %s' % file_path)
                    continue
            except IOError:
                logger.exception('skipping due to IOError on file: %s' % file_path)
                continue
            if os.path.isfile(file_path):
                content = self.get_contents(file_path)
                if 'keyring' in file_path:
                    content = parse_keyring(content)
                try:
                    osd_metadata[_file] = int(content)
                except ValueError:
                    osd_metadata[_file] = content

        # we must scan the paths again because this might be a temporary mount
        path_mounts = system.get_mounts(paths=True)
        device = path_mounts.get(path)

        # it is possible to have more than one device, pick the first one, and
        # warn that it is possible that more than one device is 'data'
        if not device:
            terminal.error('Unable to detect device mounted for path: %s' % path)
            raise RuntimeError('Cannot activate OSD')
        osd_metadata['data'] = self.scan_device(device[0] if len(device) else None)

        return osd_metadata

    def scan_encrypted(self, directory=None):
        device = self.encryption_metadata['device']
        lockbox = self.encryption_metadata['lockbox']
        encryption_type = self.encryption_metadata['type']
        osd_metadata = {}
        # Get the PARTUUID of the device to make sure have the right one and
        # that maps to the data device
        device_uuid = disk.get_partuuid(device)
        dm_path = '/dev/mapper/%s' % device_uuid
        # check if this partition is already mapped
        device_status = encryption.status(device_uuid)

        # capture all the information from the lockbox first, reusing the
        # directory scan method
        if self.device_mounts.get(lockbox):
            lockbox_path = self.device_mounts.get(lockbox)[0]
            lockbox_metadata = self.scan_directory(lockbox_path)
            # ceph-disk stores the fsid as osd-uuid in the lockbox, thanks ceph-disk
            dmcrypt_secret = encryption.get_dmcrypt_key(
                None,  # There is no ID stored in the lockbox
                lockbox_metadata['osd-uuid'],
                os.path.join(lockbox_path, 'keyring')
            )
        else:
            with system.tmp_mount(lockbox) as lockbox_path:
                lockbox_metadata = self.scan_directory(lockbox_path)
                # ceph-disk stores the fsid as osd-uuid in the lockbox, thanks ceph-disk
                dmcrypt_secret = encryption.get_dmcrypt_key(
                    None,  # There is no ID stored in the lockbox
                    lockbox_metadata['osd-uuid'],
                    os.path.join(lockbox_path, 'keyring')
                )

        if not device_status:
            # Note how both these calls need b64decode. For some reason, the
            # way ceph-disk creates these keys, it stores them in the monitor
            # *undecoded*, requiring this decode call again. The lvm side of
            # encryption doesn't need it, so we are assuming here that anything
            # that `simple` scans, will come from ceph-disk and will need this
            # extra decode call here
            dmcrypt_secret = base64.b64decode(dmcrypt_secret)
            if encryption_type == 'luks':
                encryption.luks_open(dmcrypt_secret, device, device_uuid)
            else:
                encryption.plain_open(dmcrypt_secret, device, device_uuid)

        # If we have a directory, use that instead of checking for mounts
        if directory:
            osd_metadata = self.scan_directory(directory)
        else:
            # Now check if that mapper is mounted already, to avoid remounting and
            # decrypting the device
            dm_path_mount = self.device_mounts.get(dm_path)
            if dm_path_mount:
                osd_metadata = self.scan_directory(dm_path_mount[0])
            else:
                with system.tmp_mount(dm_path, encrypted=True) as device_path:
                    osd_metadata = self.scan_directory(device_path)

        osd_metadata['encrypted'] = True
        osd_metadata['encryption_type'] = encryption_type
        osd_metadata['lockbox.keyring'] = parse_keyring(lockbox_metadata['keyring'])
        return osd_metadata

    @decorators.needs_root
    def scan(self, args):
        osd_metadata = {'cluster_name': conf.cluster}
        osd_path = None
        logger.info('detecting if argument is a device or a directory: %s', args.osd_path)
        if os.path.isdir(args.osd_path):
            logger.info('will scan directly, path is a directory')
            osd_path = args.osd_path
        else:
            # assume this is a device, check if it is mounted and use that path
            logger.info('path is not a directory, will check if mounted')
            if system.device_is_mounted(args.osd_path):
                logger.info('argument is a device, which is mounted')
                mounted_osd_paths = self.device_mounts.get(args.osd_path)
                osd_path = mounted_osd_paths[0] if len(mounted_osd_paths) else None

        # argument is not a directory, and it is not a device that is mounted
        # somewhere so temporarily mount it to poke inside, otherwise, scan
        # directly
        if not osd_path:
            # check if we have an encrypted device first, so that we can poke at
            # the lockbox instead
            if self.is_encrypted:
                if not self.encryption_metadata.get('lockbox'):
                    raise RuntimeError(
                        'Lockbox partition was not found for device: %s' % args.osd_path
                    )
                osd_metadata = self.scan_encrypted()
            else:
                logger.info('device is not mounted, will mount it temporarily to scan')
                with system.tmp_mount(args.osd_path) as osd_path:
                    osd_metadata = self.scan_directory(osd_path)
        else:
            if self.is_encrypted:
                logger.info('will scan encrypted OSD directory at path: %s', osd_path)
                osd_metadata = self.scan_encrypted(osd_path)
            else:
                logger.info('will scan OSD directory at path: %s', osd_path)
                osd_metadata = self.scan_directory(osd_path)

        osd_id = osd_metadata['whoami']
        osd_fsid = osd_metadata['fsid']
        filename = '%s-%s.json' % (osd_id, osd_fsid)
        json_path = os.path.join(self.etc_path, filename)

        if os.path.exists(json_path) and not args.stdout:
            if not args.force:
                raise RuntimeError(
                    '--force was not used and OSD metadata file exists: %s' % json_path
                )

        if args.stdout:
            print(json.dumps(osd_metadata, indent=4, sort_keys=True, ensure_ascii=False))
        else:
            with open(json_path, 'w') as fp:
                json.dump(osd_metadata, fp, indent=4, sort_keys=True, ensure_ascii=False)
                fp.write(os.linesep)
            terminal.success(
                'OSD %s got scanned and metadata persisted to file: %s' % (
                    osd_id,
                    json_path
                )
            )
            terminal.success(
                'To take over management of this scanned OSD, and disable ceph-disk and udev, run:'
            )
            terminal.success('    ceph-volume simple activate %s %s' % (osd_id, osd_fsid))

        if not osd_metadata.get('data'):
            msg = 'Unable to determine device mounted on %s' % args.osd_path
            logger.warning(msg)
            terminal.warning(msg)
            terminal.warning('OSD will not be able to start without this information:')
            terminal.warning('    "data": "/path/to/device",')
            logger.warning('Unable to determine device mounted on %s' % args.osd_path)

    def main(self):
        sub_command_help = dedent("""
        Scan running OSDs, an OSD directory (or data device) for files and configurations
        that will allow to take over the management of the OSD.

        Scanned OSDs will get their configurations stored in
        /etc/ceph/osd/<id>-<fsid>.json

        For an OSD ID of 0 with fsid of ``a9d50838-e823-43d6-b01f-2f8d0a77afc2``
        that could mean a scan command that looks like::

            ceph-volume lvm scan /var/lib/ceph/osd/ceph-0

        Which would store the metadata in a JSON file at::

            /etc/ceph/osd/0-a9d50838-e823-43d6-b01f-2f8d0a77afc2.json

        To scan all running OSDs:

            ceph-volume simple scan

        To a scan a specific running OSD:

            ceph-volume simple scan /var/lib/ceph/osd/{cluster}-{osd id}

        And to scan a device (mounted or unmounted) that has OSD data in it, for example /dev/sda1

            ceph-volume simple scan /dev/sda1

        Scanning a device or directory that belongs to an OSD not created by ceph-disk will be ingored.
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume simple scan',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            '-f', '--force',
            action='store_true',
            help='If OSD has already been scanned, the JSON file will be overwritten'
        )

        parser.add_argument(
            '--stdout',
            action='store_true',
            help='Do not save to a file, output metadata to stdout'
        )

        parser.add_argument(
            'osd_path',
            metavar='OSD_PATH',
            type=arg_validators.OSDPath(),
            nargs='?',
            default=None,
            help='Path to an existing OSD directory or OSD data partition'
        )

        args = parser.parse_args(self.argv)
        paths = []
        if args.osd_path:
            paths.append(args.osd_path)
        else:
            osd_ids = systemctl.get_running_osd_ids()
            for osd_id in osd_ids:
                paths.append("/var/lib/ceph/osd/{}-{}".format(
                    conf.cluster,
                    osd_id,
                ))

        # Capture some environment status, so that it can be reused all over
        self.device_mounts = system.get_mounts(devices=True)
        self.path_mounts = system.get_mounts(paths=True)

        for path in paths:
            args.osd_path = path
            device = Device(args.osd_path)
            if device.is_partition:
                if device.ceph_disk.type != 'data':
                    label = device.ceph_disk.partlabel
                    msg = 'Device must be the ceph data partition, but PARTLABEL reported: "%s"' % label
                    raise RuntimeError(msg)

            self.encryption_metadata = encryption.legacy_encrypted(args.osd_path)
            self.is_encrypted = self.encryption_metadata['encrypted']

            device = Device(self.encryption_metadata['device'])
            if not device.is_ceph_disk_member:
                terminal.warning("Ignoring %s because it's not a ceph-disk created osd." % path)
            else:
                self.scan(args)
