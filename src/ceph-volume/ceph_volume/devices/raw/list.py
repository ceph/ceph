from __future__ import print_function
import argparse
import json
import logging
from textwrap import dedent
from ceph_volume import decorators, process
from ceph_volume.util import disk
from ceph_volume.util.device import Device
from typing import Any, Dict, Optional, List as _List
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


def direct_report(devices: Optional[_List[str]] = None) -> Dict[str, Any]:
    """
    Other non-cli consumers of listing information will want to consume the
    report without the need to parse arguments or other flags. This helper
    bypasses the need to deal with the class interface which is meant for cli
    handling.
    """
    _list = List([])
    return _list.generate(devices)

def _get_bluestore_info(devices: _List[str]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    command: _List[str] = ['ceph-bluestore-tool',
                           'show-label', '--bdev_aio_poll_ms=1']
    for device in devices:
        command.extend(['--dev', device])
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc:
        logger.debug(f"ceph-bluestore-tool couldn't detect any BlueStore device.\n{out}\n{err}")
    else:
        oj = json.loads(''.join(out))
        for device in devices:
            if device not in oj:
                # should be impossible, so warn
                logger.warning(f'skipping device {device} because it is not reported in ceph-bluestore-tool output: {out}')
            if oj.get(device):
                try:
                    osd_uuid = oj[device]['osd_uuid']
                    result[osd_uuid] = disk.bluestore_info(device, oj)
                except KeyError as e:
                    # this will appear for devices that have a bluestore header but aren't valid OSDs
                    # for example, due to incomplete rollback of OSDs: https://tracker.ceph.com/issues/51869
                    logger.error(f'device {device} does not have all BlueStore data needed to be a valid OSD: {out}\n{e}')
    return result


class List(object):

    help = 'list BlueStore OSDs on raw devices'

    def __init__(self, argv: _List[str]) -> None:
        self.argv = argv
        self.info_devices: _List[Dict[str, str]] = []
        self.devices_to_scan: _List[str] = []

    def exclude_atari_partitions(self) -> None:
        result: _List[str] = []
        for info_device in self.info_devices:
            path = info_device['NAME']
            parent_device = info_device.get('PKNAME')
            if parent_device:
                try:
                    if disk.has_bluestore_label(parent_device):
                        logger.warning(('ignoring child device {} whose parent {} is a BlueStore OSD.'.format(path, parent_device),
                                        'device is likely a phantom Atari partition. device info: {}'.format(info_device)))
                        continue
                except OSError as e:
                    logger.error(('ignoring child device {} to avoid reporting invalid BlueStore data from phantom Atari partitions.'.format(path),
                                'failed to determine if parent device {} is BlueStore. err: {}'.format(parent_device, e)))
                    continue
            result.append(path)
        self.devices_to_scan = result

    def exclude_lvm_osd_devices(self) -> None:
        with ThreadPoolExecutor() as pool:
            filtered_devices_to_scan = pool.map(self.filter_lvm_osd_devices, self.devices_to_scan)
            self.devices_to_scan = [device for device in filtered_devices_to_scan if device is not None]

    def filter_lvm_osd_devices(self, device: str) -> Optional[str]:
        d = Device(device)
        return d.path if not d.ceph_device_lvm else None

    def generate(self, devices: Optional[_List[str]] = None) -> Dict[str, Any]:
        logger.debug('Listing block devices via lsblk...')
        if not devices or not any(devices):
            # If no devs are given initially, we want to list ALL devices including children and
            # parents. Parent disks with child partitions may be the appropriate device to return if
            # the parent disk has a bluestore header, but children may be the most appropriate
            # devices to return if the parent disk does not have a bluestore header.
            self.info_devices = disk.lsblk_all(abspath=True)
            # Linux kernels built with CONFIG_ATARI_PARTITION enabled can falsely interpret
            # bluestore's on-disk format as an Atari partition table. These false Atari partitions
            # can be interpreted as real OSDs if a bluestore OSD was previously created on the false
            # partition. See https://tracker.ceph.com/issues/52060 for more info. If a device has a
            # parent, it is a child. If the parent is a valid bluestore OSD, the child will only
            # exist if it is a phantom Atari partition, and the child should be ignored. If the
            # parent isn't bluestore, then the child could be a valid bluestore OSD. If we fail to
            # determine whether a parent is bluestore, we should err on the side of not reporting
            # the child so as not to give a false negative.
            self.exclude_atari_partitions()
            self.exclude_lvm_osd_devices()

        else:
            self.devices_to_scan = devices

        result: Dict[str, Any] = {}
        logger.debug('inspecting devices: {}'.format(self.devices_to_scan))
        result = _get_bluestore_info(self.devices_to_scan)

        return result

    @decorators.needs_root
    def list(self, args: argparse.Namespace) -> None:
        report = self.generate(args.device)
        if args.format == 'json':
            print(json.dumps(report, indent=4, sort_keys=True))
        else:
            if not report:
                raise SystemExit('No valid Ceph devices found')
            raise RuntimeError('not implemented yet')

    def main(self) -> None:
        sub_command_help = dedent("""
        List OSDs on raw devices with raw device labels (usually the first
        block of the device).

        Full listing of all identifiable (currently, BlueStore) OSDs
        on raw devices:

            ceph-volume raw list

        List a particular device, reporting all metadata about it::

            ceph-volume raw list /dev/sda1

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume raw list',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'device',
            metavar='DEVICE',
            nargs='*',
            help='Path to a device like /dev/sda1'
        )

        parser.add_argument(
            '--format',
            help='output format, defaults to "pretty"',
            default='json',
            choices=['json', 'pretty'],
        )

        args = parser.parse_args(self.argv)
        self.list(args)
