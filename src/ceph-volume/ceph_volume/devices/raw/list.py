from __future__ import print_function
import argparse
import json
import logging
from textwrap import dedent
from ceph_volume import decorators, process
from ceph_volume.util import disk
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def direct_report(devices):
    """
    Other non-cli consumers of listing information will want to consume the
    report without the need to parse arguments or other flags. This helper
    bypasses the need to deal with the class interface which is meant for cli
    handling.
    """
    _list = List([])
    return _list.generate(devices)

def _get_bluestore_info(dev):
    out, err, rc = process.call([
        'ceph-bluestore-tool', 'show-label',
        '--dev', dev], verbose_on_failure=False)
    if rc:
        # ceph-bluestore-tool returns an error (below) if device is not bluestore OSD
        #   > unable to read label for <device>: (2) No such file or directory
        # but it's possible the error could be for a different reason (like if the disk fails)
        logger.debug('assuming device {} is not BlueStore; ceph-bluestore-tool failed to get info from device: {}\n{}'.format(dev, out, err))
        return None
    oj = json.loads(''.join(out))
    if dev not in oj:
        # should be impossible, so warn
        logger.warning('skipping device {} because it is not reported in ceph-bluestore-tool output: {}'.format(dev, out))
        return None
    try:
        r = {
            'osd_uuid': oj[dev]['osd_uuid'],
        }
        if oj[dev]['description'] == 'main':
            whoami = oj[dev]['whoami']
            r.update({
                'type': 'bluestore',
                'osd_id': int(whoami),
                'ceph_fsid': oj[dev]['ceph_fsid'],
                'device': dev,
            })
        elif oj[dev]['description'] == 'bluefs db':
            r['device_db'] = dev
        elif oj[dev]['description'] == 'bluefs wal':
            r['device_wal'] = dev
        return r
    except KeyError as e:
        # this will appear for devices that have a bluestore header but aren't valid OSDs
        # for example, due to incomplete rollback of OSDs: https://tracker.ceph.com/issues/51869
        logger.error('device {} does not have all BlueStore data needed to be a valid OSD: {}\n{}'.format(dev, out, e))
        return None


class List(object):

    help = 'list BlueStore OSDs on raw devices'

    def __init__(self, argv):
        self.argv = argv

    def is_atari_partitions(self, _lsblk: Dict[str, Any]) -> bool:
        dev = _lsblk['NAME']
        if _lsblk.get('PKNAME'):
            parent = _lsblk['PKNAME']
            try:
                if disk.has_bluestore_label(parent):
                    logger.warning(('ignoring child device {} whose parent {} is a BlueStore OSD.'.format(dev, parent),
                                    'device is likely a phantom Atari partition. device info: {}'.format(_lsblk)))
                    return True
            except OSError as e:
                logger.error(('ignoring child device {} to avoid reporting invalid BlueStore data from phantom Atari partitions.'.format(dev),
                            'failed to determine if parent device {} is BlueStore. err: {}'.format(parent, e)))
                return True
        return False

    def exclude_atari_partitions(self, _lsblk_all: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [_lsblk for _lsblk in _lsblk_all if not self.is_atari_partitions(_lsblk)]

    def generate(self, devs=None):
        logger.debug('Listing block devices via lsblk...')
        info_devices = []
        if not devs or not any(devs):
            # If no devs are given initially, we want to list ALL devices including children and
            # parents. Parent disks with child partitions may be the appropriate device to return if
            # the parent disk has a bluestore header, but children may be the most appropriate
            # devices to return if the parent disk does not have a bluestore header.
            info_devices = disk.lsblk_all(abspath=True)
            devs = [device['NAME'] for device in info_devices if device.get('NAME',)]
        else:
            for dev in devs:
                info_devices.append(disk.lsblk(dev, abspath=True))

        # Linux kernels built with CONFIG_ATARI_PARTITION enabled can falsely interpret
        # bluestore's on-disk format as an Atari partition table. These false Atari partitions
        # can be interpreted as real OSDs if a bluestore OSD was previously created on the false
        # partition. See https://tracker.ceph.com/issues/52060 for more info. If a device has a
        # parent, it is a child. If the parent is a valid bluestore OSD, the child will only
        # exist if it is a phantom Atari partition, and the child should be ignored. If the
        # parent isn't bluestore, then the child could be a valid bluestore OSD. If we fail to
        # determine whether a parent is bluestore, we should err on the side of not reporting
        # the child so as not to give a false negative.
        info_devices = self.exclude_atari_partitions(info_devices)

        result = {}
        logger.debug('inspecting devices: {}'.format(devs))
        for info_device in info_devices:
            bs_info = _get_bluestore_info(info_device['NAME'])
            if bs_info is None:
                # None is also returned in the rare event that there is an issue reading info from
                # a BlueStore disk, so be sure to log our assumption that it isn't bluestore
                logger.info('device {} does not have BlueStore information'.format(info_device['NAME']))
                continue
            uuid = bs_info['osd_uuid']
            if uuid not in result:
                result[uuid] = {}
            result[uuid].update(bs_info)

        return result

    @decorators.needs_root
    def list(self, args):
        report = self.generate(args.device)
        if args.format == 'json':
            print(json.dumps(report, indent=4, sort_keys=True))
        else:
            if not report:
                raise SystemExit('No valid Ceph devices found')
            raise RuntimeError('not implemented yet')

    def main(self):
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
