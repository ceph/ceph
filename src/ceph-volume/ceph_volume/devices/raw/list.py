from __future__ import print_function
import argparse
import json
import logging
from textwrap import dedent
from ceph_volume import decorators, process


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


class List(object):

    help = 'list BlueStore OSDs on raw devices'

    def __init__(self, argv):
        self.argv = argv

    def generate(self, devs=None):
        if not devs:
            logger.debug('Listing block devices via lsblk...')
            devs = []
            # adding '--inverse' allows us to get the mapper devices list in that command output.
            # not listing root devices containing partitions shouldn't have side effect since we are
            # in `ceph-volume raw` context.
            #
            #   example:
            #   running `lsblk --paths --nodeps --output=NAME --noheadings` doesn't allow to get the mapper list
            #   because the output is like following :
            #
            #   $ lsblk --paths --nodeps --output=NAME --noheadings
            #   /dev/sda
            #   /dev/sdb
            #   /dev/sdc
            #   /dev/sdd
            #
            #   the dmcrypt mappers are hidden because of the `--nodeps` given they are displayed as a dependency.
            #
            #   $ lsblk --paths --output=NAME --noheadings
            #   /dev/sda
            #   |-/dev/mapper/ceph-3b52c90d-6548-407d-bde1-efd31809702f-sda-block-dmcrypt
            #   `-/dev/mapper/ceph-3b52c90d-6548-407d-bde1-efd31809702f-sda-db-dmcrypt
            #   /dev/sdb
            #   /dev/sdc
            #   /dev/sdd
            #
            #   adding `--inverse` is a trick to get around this issue, the counterpart is that we can't list root devices if they contain
            #   at least one partition but this shouldn't be an issue in `ceph-volume raw` context given we only deal with raw devices.
            out, err, ret = process.call([
                'lsblk', '--paths', '--nodeps', '--output=NAME', '--noheadings', '--inverse'
            ])
            assert not ret
            devs = out
        result = {}
        for dev in devs:
            logger.debug('Examining %s' % dev)
            # bluestore?
            out, err, ret = process.call([
                'ceph-bluestore-tool', 'show-label',
                '--dev', dev], verbose_on_failure=False)
            if ret:
                logger.debug('No label on %s' % dev)
                continue
            oj = json.loads(''.join(out))
            if dev not in oj:
                continue
            if oj[dev]['description'] != 'main':
                # ignore non-main devices, for now
                continue
            whoami = oj[dev]['whoami']
            result[whoami] = {
                'type': 'bluestore',
                'osd_id': int(whoami),
            }
            for f in ['osd_uuid', 'ceph_fsid']:
                result[whoami][f] = oj[dev][f]
            result[whoami]['device'] = dev
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
