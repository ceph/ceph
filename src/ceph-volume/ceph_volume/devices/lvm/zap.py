import argparse
import os
import logging
import time

from textwrap import dedent

from ceph_volume import decorators, terminal, process, BEING_REPLACED_HEADER
from ceph_volume.api import lvm as api
from ceph_volume.util import system, encryption, disk, arg_validators, str_to_int, merge_dict
from ceph_volume.util.device import Device
from ceph_volume.systemd import systemctl
from ceph_volume.devices.raw.list import direct_report
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)


def zap_device(path: str) -> None:
    """Remove any existing filesystem signatures.

    Args:
        path (str): The path to the device to zap.
    """
    zap_bluestore(path)
    wipefs(path)
    zap_data(path)

def zap_bluestore(path: str) -> None:
    """Remove all BlueStore signature on a device.

    Args:
        path (str): The path to the device to remove BlueStore signatures from.
    """
    terminal.info(f'Removing all BlueStore signature on {path} if any...')
    process.run([
        'ceph-bluestore-tool',
        'zap-device',
        '--dev',
        path,
        '--yes-i-really-really-mean-it'
    ])

def wipefs(path):
    """
    Removes the filesystem from an lv or partition.

    Environment variables supported::

    * ``CEPH_VOLUME_WIPEFS_TRIES``: Defaults to 8
    * ``CEPH_VOLUME_WIPEFS_INTERVAL``: Defaults to 5

    """
    tries = str_to_int(
        os.environ.get('CEPH_VOLUME_WIPEFS_TRIES', 8)
    )
    interval = str_to_int(
        os.environ.get('CEPH_VOLUME_WIPEFS_INTERVAL', 5)
    )

    for trying in range(tries):
        stdout, stderr, exit_code = process.call([
            'wipefs',
            '--all',
            path
        ])
        if exit_code != 0:
            # this could narrow the retry by poking in the stderr of the output
            # to verify that 'probing initialization failed' appears, but
            # better to be broad in this retry to prevent missing on
            # a different message that needs to be retried as well
            terminal.warning(
                'failed to wipefs device, will try again to workaround probable race condition'
            )
            time.sleep(interval)
        else:
            return
    raise RuntimeError("could not complete wipefs on device: %s" % path)


def zap_data(path):
    """
    Clears all data from the given path. Path should be
    an absolute path to an lv or partition.

    10M of data is written to the path to make sure that
    there is no trace left of any previous Filesystem.
    """
    process.run([
        'dd',
        'if=/dev/zero',
        'of={path}'.format(path=path),
        'bs=1M',
        'count=10',
        'conv=fsync'
    ])


class Zap:
    help = 'Removes all data and filesystems from a logical volume or partition.'

    def __init__(self, argv: List[str]) -> None:
        self.argv = argv
        self.osd_ids_to_zap: List[str] = []

    def ensure_associated_raw(self, raw_report: Dict[str, Any]) -> List[str]:
        osd_id: str = self.args.osd_id
        osd_uuid: str = self.args.osd_fsid
        raw_devices: Set[str] = set()

        if len([details.get('osd_id') for _, details in raw_report.items() if details.get('osd_id') == osd_id]) > 1:
            if not osd_uuid:
                raise RuntimeError(f'Multiple OSDs found with id {osd_id}, pass --osd-fsid')

        if not osd_uuid:
            for _, details in raw_report.items():
                if details.get('osd_id') == int(osd_id):
                    osd_uuid = details.get('osd_uuid')
                    break

        for osd_uuid, details in raw_report.items():
            device: str = details.get('device')
            if details.get('osd_uuid') == osd_uuid:
                raw_devices.add(device)

        return list(raw_devices)
        

    def find_associated_devices(self) -> List[api.Volume]:
        """From an ``osd_id`` and/or an ``osd_fsid``, filter out all the Logical Volumes (LVs) in the
        system that match those tag values, further detect if any partitions are
        part of the OSD, and then return the set of LVs and partitions (if any).

        The function first queries the LVM-based OSDs using the provided `osd_id` or `osd_fsid`.
        If no matches are found, it then searches the system for RAW-based OSDs.

        Raises:
            SystemExit: If no OSDs are found, the function raises a `SystemExit` with an appropriate message.

        Returns:
            List[api.Volume]: A list of `api.Volume` objects corresponding to the OSD's Logical Volumes (LVs)
            or partitions that are associated with the given `osd_id` or `osd_fsid`.

        Notes:
            - If neither `osd_id` nor `osd_fsid` are provided, the function will not be able to find OSDs.
            - The search proceeds from LVM-based OSDs to RAW-based OSDs if no Logical Volumes are found.
        """
        lv_tags = {}
        lv_tags = {key: value for key, value in {
            'ceph.osd_id': self.args.osd_id,
            'ceph.osd_fsid': self.args.osd_fsid
        }.items() if value}
        devices_to_zap: List[str] = []
        lvs = api.get_lvs(tags=lv_tags)

        if lvs:
            devices_to_zap = self.ensure_associated_lvs(lvs, lv_tags)
        else:
            mlogger.debug(f'No OSD identified by "{self.args.osd_id or self.args.osd_fsid}" was found among LVM-based OSDs.')
            mlogger.debug('Proceeding to check RAW-based OSDs.')
            raw_osds: Dict[str, Any] = direct_report()
            if raw_osds:
                devices_to_zap = self.ensure_associated_raw(raw_osds)
        if not devices_to_zap:
            raise SystemExit('No OSD were found.')

        return [Device(path) for path in set(devices_to_zap) if path]

    def ensure_associated_lvs(self,
                              lvs: List[api.Volume],
                              lv_tags: Dict[str, Any] = {}) -> List[str]:
        """
        Go through each LV and ensure if backing devices (journal, wal, block)
        are LVs or partitions, so that they can be accurately reported.
        """
        # look for many LVs for each backing type, because it is possible to
        # receive a filtering for osd.1, and have multiple failed deployments
        # leaving many journals with osd.1 - usually, only a single LV will be
        # returned

        db_lvs = api.get_lvs(tags=merge_dict(lv_tags, {'ceph.type': 'db'}))
        wal_lvs = api.get_lvs(tags=merge_dict(lv_tags, {'ceph.type': 'wal'}))
        backing_devices = [(db_lvs, 'db'),
                        (wal_lvs, 'wal')]

        verified_devices = []

        for lv in lvs:
            # go through each lv and append it, otherwise query `blkid` to find
            # a physical device. Do this for each type (journal,db,wal) regardless
            # if they have been processed in the previous LV, so that bad devices
            # with the same ID can be caught
            for ceph_lvs, _type in backing_devices:
                if ceph_lvs:
                    verified_devices.extend([l.lv_path for l in ceph_lvs])
                    continue

                # must be a disk partition, by querying blkid by the uuid we are
                # ensuring that the device path is always correct
                try:
                    device_uuid = lv.tags['ceph.%s_uuid' % _type]
                except KeyError:
                    # Bluestore will not have ceph.journal_uuid, and Filestore
                    # will not not have ceph.db_uuid
                    continue

                osd_device = disk.get_device_from_partuuid(device_uuid)
                if not osd_device:
                    # if the osd_device is not found by the partuuid, then it is
                    # not possible to ensure this device exists anymore, so skip it
                    continue
                verified_devices.append(osd_device)

            verified_devices.append(lv.lv_path)

        # reduce the list from all the duplicates that were added
        return list(set(verified_devices))

    def unmount_lv(self, lv: api.Volume) -> None:
        if lv.tags.get('ceph.cluster_name') and lv.tags.get('ceph.osd_id'):
            lv_path = "/var/lib/ceph/osd/{}-{}".format(lv.tags['ceph.cluster_name'], lv.tags['ceph.osd_id'])
        else:
            lv_path = lv.lv_path
        dmcrypt_uuid = lv.lv_uuid
        dmcrypt = lv.encrypted
        if system.path_is_mounted(lv_path):
            mlogger.info("Unmounting %s", lv_path)
            system.unmount(lv_path)
        if dmcrypt and dmcrypt_uuid:
            self.dmcrypt_close(dmcrypt_uuid)

    def _write_replacement_header(self, device: str) -> None:
        """Write a replacement header to a device.

        This method writes the string defined in `BEING_REPLACED_HEADER`
        to the specified device. This header indicates that the device
        is in the process of being replaced.

        Args:
            device (str): The path to the device on which the replacement
                          header will be written.
        """
        disk._dd_write(device,
                       BEING_REPLACED_HEADER)

    def clear_replace_header(self) -> bool:
        """Safely erase the replacement header on a device if it is marked as being replaced.

        This method checks whether the given device is marked as being replaced
        (`device.is_being_replaced`). If true, it proceeds to erase the replacement header
        from the device using the `_erase_replacement_header` method. The method returns
        a boolean indicating whether any action was taken.

        Args:
            device (Device): The device object, which includes information about the device's
                            path and status (such as whether it is currently being replaced).

        Returns:
            bool: True if the replacement header was successfully erased, False if the
                device was not marked as being replaced or no action was necessary.
        """
        result: bool = False
        device: Device = self.args.clear_replace_header
        if device.is_being_replaced:
            self._erase_replacement_header(device.path)
            result = True
        return result

    def _erase_replacement_header(self, device: str) -> None:
        """Erase the replacement header on a device.

        This method writes a sequence of null bytes (`0x00`) over the area of the device
        where the replacement header is stored, effectively erasing it.

        Args:
            device (str): The path to the device from which the replacement header will be erased.
        """
        disk._dd_write(device,
                       b'\x00' * len(BEING_REPLACED_HEADER))

    def zap_lv(self, device: Device) -> None:
        """
        Device examples: vg-name/lv-name, /dev/vg-name/lv-name
        Requirements: Must be a logical volume (LV)
        """
        lv: api.Volume = device.lv_api
        self.unmount_lv(lv)
        self.parent_device: str = disk.get_parent_device_from_mapper(lv.lv_path)
        zap_device(device.path)

        if self.args.destroy:
            lvs = api.get_lvs(filters={'vg_name': device.vg_name})
            if len(lvs) <= 1:
                mlogger.info('Only 1 LV left in VG, will proceed to destroy '
                             'volume group %s', device.vg_name)
                pvs = api.get_pvs(filters={'lv_uuid': lv.lv_uuid})
                api.remove_vg(device.vg_name)
                for pv in pvs:
                    api.remove_pv(pv.pv_name)
                replacement_args: Dict[str, bool] = {
                    'block': self.args.replace_block,
                    'db': self.args.replace_db,
                    'wal': self.args.replace_wal
                }
                if replacement_args.get(lv.tags.get('ceph.type'), False):
                    mlogger.info(f'Marking {self.parent_device} as being replaced')
                    self._write_replacement_header(self.parent_device)
            else:
                mlogger.info('More than 1 LV left in VG, will proceed to '
                             'destroy LV only')
                mlogger.info('Removing LV because --destroy was given: %s',
                             device.path)
                if self.args.replace_block:
                    mlogger.info(f'--replace-block passed but the device still has {str(len(lvs))} LV(s)')
                api.remove_lv(device.path)
        elif lv:
            # just remove all lvm metadata, leaving the LV around
            lv.clear_tags()

    def zap_partition(self, device: Device) -> None:
        """
        Device example: /dev/sda1
        Requirements: Must be a partition
        """
        if device.is_encrypted:
            # find the holder
            pname = device.sys_api.get('parent')
            devname = device.sys_api.get('devname')
            parent_device = Device(f'/dev/{pname}')
            holders: List[str] = [
                f'/dev/{holder}' for holder in parent_device.sys_api['partitions'][devname]['holders']
            ]
            for mapper_uuid in os.listdir('/dev/mapper'):
                mapper_path = os.path.join('/dev/mapper', mapper_uuid)
                if os.path.realpath(mapper_path) in holders:
                    self.dmcrypt_close(mapper_uuid)

        if system.device_is_mounted(device.path):
            mlogger.info("Unmounting %s", device.path)
            system.unmount(device.path)

        zap_device(device.path)

        if self.args.destroy:
            mlogger.info("Destroying partition since --destroy was used: %s" % device.path)
            disk.remove_partition(device)

    def zap_lvm_member(self, device: Device) -> None:
        """
        An LVM member may have more than one LV and or VG, for example if it is
        a raw device with multiple partitions each belonging to a different LV

        Device example: /dev/sda
        Requirements: An LV or VG present in the device, making it an LVM member
        """
        for lv in device.lvs:
            if lv.lv_name:
                mlogger.info('Zapping lvm member {}. lv_path is {}'.format(device.path, lv.lv_path))
                self.zap_lv(Device(lv.lv_path))
            else:
                vg = api.get_single_vg(filters={'vg_name': lv.vg_name})
                if vg:
                    mlogger.info('Found empty VG {}, removing'.format(vg.vg_name))
                    api.remove_vg(vg.vg_name)



    def zap_raw_device(self, device: Device) -> None:
        """
        Any whole (raw) device passed in as input will be processed here,
        checking for LVM membership and partitions (if any).

        Device example: /dev/sda
        Requirements: None
        """
        if not self.args.destroy:
            # the use of dd on a raw device causes the partition table to be
            # destroyed
            mlogger.warning(
                '--destroy was not specified, but zapping a whole device will remove the partition table'
            )

        # look for partitions and zap those
        for part_name in device.sys_api.get('partitions', {}).keys():
            self.zap_partition(Device('/dev/%s' % part_name))

        zap_device(device.path)
        # TODO(guits): I leave this commented out, this should be part of a separate patch in order to
        # support device replacement with raw-based OSDs
        # if self.args.replace_block:
        #     disk._dd_write(device.path, 'CEPH_DEVICE_BEING_REPLACED')

    @decorators.needs_root
    def zap(self) -> None:
        """Zap a device.

        Raises:
            SystemExit: When the device is a mapper and not a mpath device.
        """
        devices = self.args.devices
        for device in devices:
            mlogger.info("Zapping: %s", device.path)
            if device.is_mapper and not device.is_mpath:
                terminal.error("Refusing to zap the mapper device: {}".format(device))
                raise SystemExit(1)
            if device.is_lvm_member:
                self.zap_lvm_member(device)
            if device.is_lv:
                self.zap_lv(device)
            if device.is_partition:
                self.zap_partition(device)
            if device.is_device:
                self.zap_raw_device(device)

        if self.args.devices:
            terminal.success(
                "Zapping successful for: %s" % ", ".join([str(d) for d in self.args.devices])
            )
        else:
            identifier = self.args.osd_id or self.args.osd_fsid
            terminal.success(
                "Zapping successful for OSD: %s" % identifier
            )

    @decorators.needs_root
    def zap_osd(self) -> None:
        if self.args.osd_id and not self.args.no_systemd:
            osd_is_running = systemctl.osd_is_active(self.args.osd_id)
            if osd_is_running:
                mlogger.error("OSD ID %s is running, stop it with:" % self.args.osd_id)
                mlogger.error("systemctl stop ceph-osd@%s" % self.args.osd_id)
                raise SystemExit("Unable to zap devices associated with OSD ID: %s" % self.args.osd_id)
        self.args.devices = self.find_associated_devices()
        self.zap()

    def dmcrypt_close(self, dmcrypt_uuid: str) -> None:
        mlogger.info("Closing encrypted volume %s", dmcrypt_uuid)
        encryption.dmcrypt_close(mapping=dmcrypt_uuid, skip_path_check=True)

    def main(self) -> None:
        sub_command_help = dedent("""
        Zaps the given logical volume(s), raw device(s) or partition(s) for reuse by ceph-volume.
        If given a path to a logical volume it must be in the format of vg/lv. Any
        filesystems present on the given device, vg/lv, or partition will be removed and
        all data will be purged.

        If the logical volume, raw device or partition is being used for any ceph related
        mount points they will be unmounted.

        However, the lv or partition will be kept intact.

        Example calls for supported scenarios:

          Zapping a logical volume:

              ceph-volume lvm zap {vg name/lv name}

          Zapping a partition:

              ceph-volume lvm zap /dev/sdc1

          Zapping many raw devices:

              ceph-volume lvm zap /dev/sda /dev/sdb /db/sdc

          Zapping devices associated with an OSD ID:

              ceph-volume lvm zap --osd-id 1

            Optionally include the OSD FSID

              ceph-volume lvm zap --osd-id 1 --osd-fsid 55BD4219-16A7-4037-BC20-0F158EFCC83D

        If the --destroy flag is given and you are zapping a raw device or partition
        then all vgs and lvs that exist on that raw device or partition will be destroyed.

        This is especially useful if a raw device or partition was used by ceph-volume lvm create
        or ceph-volume lvm prepare commands previously and now you want to reuse that device.

        For example:

          ceph-volume lvm zap /dev/sda --destroy

        If the --destroy flag is given and you are zapping an lv then the lv is still
        kept intact for reuse.

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm zap',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'devices',
            metavar='DEVICES',
            nargs='*',
            type=arg_validators.ValidZapDevice(gpt_ok=True),
            default=[],
            help='Path to one or many lv (as vg/lv), partition (as /dev/sda1) or device (as /dev/sda)'
        )

        parser.add_argument(
            '--destroy',
            action='store_true',
            default=False,
            help='Destroy all volume groups and logical volumes if you are zapping a raw device or partition',
        )

        parser.add_argument(
            '--osd-id',
            type=arg_validators.valid_osd_id,
            help='Specify an OSD ID to detect associated devices for zapping',
        )

        parser.add_argument(
            '--osd-fsid',
            help='Specify an OSD FSID to detect associated devices for zapping',
        )

        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip systemd unit checks',
        )

        parser.add_argument(
            '--replace-block',
            dest='replace_block',
            action='store_true',
            help='Mark the block device as unavailable.'
        )

        parser.add_argument(
            '--replace-db',
            dest='replace_db',
            action='store_true',
            help='Mark the db device as unavailable.'
        )

        parser.add_argument(
            '--replace-wal',
            dest='replace_wal',
            action='store_true',
            help='Mark the wal device as unavailable.'
        )

        parser.add_argument(
            '--clear-replace-header',
            dest='clear_replace_header',
            type=arg_validators.ValidClearReplaceHeaderDevice(),
            help='clear the replace header on devices.'
        )

        if len(self.argv) == 0:
            print(sub_command_help)
            return

        self.args = parser.parse_args(self.argv)

        if self.args.clear_replace_header:
            rc: bool = False
            try:
                rc = self.clear_replace_header()
            except Exception as e:
                raise SystemExit(e)
            if rc:
                mlogger.info(f'Replacement header cleared on {self.args.clear_replace_header}')
            else:
                mlogger.info(f'No replacement header detected on {self.args.clear_replace_header}, nothing to do.')
            raise SystemExit(not rc)

        if self.args.replace_block or self.args.replace_db or self.args.replace_wal:
            self.args.destroy = True
            mlogger.info('--replace-block|db|wal passed, enforcing --destroy.')

        if self.args.osd_id or self.args.osd_fsid:
            self.zap_osd()
        else:
            self.zap()
