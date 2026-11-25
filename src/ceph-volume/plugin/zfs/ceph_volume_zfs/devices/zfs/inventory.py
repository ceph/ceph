import argparse
import json
from textwrap import dedent
from typing import List

from ceph_volume_zfs.util.disk import Disks


class Inventory(object):

    help = 'Generate a list of available devices'

    def __init__(self, argv: List[str]) -> None:
        self.argv = argv

    def format_report(self, inventory: Disks) -> None:
        if self.args.format == 'json':
            print(json.dumps(inventory.json_report()))
        elif self.args.format == 'json-pretty':
            print(json.dumps(inventory.json_report(), indent=4, sort_keys=True))
        elif self.args.verbose:
            print(inventory.verbose_report())
        else:
            print(inventory.pretty_report())

    def main(self) -> None:
        sub_command_help = dedent("""
        Generate an inventory of available devices

        Use --verbose for a per-disk report that also lists the
        partitions on each disk with their zpool, mount and swap state.
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs inventory',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            'path',
            nargs='?',
            default=None,
            help=('Report on specific disk'),
        )
        parser.add_argument(
            '--format',
            choices=['plain', 'json', 'json-pretty'],
            default='plain',
            help='Output format',
        )
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            default=False,
            help=('Add partition, mount and swap detail per disk. Only '
                  'applies to the plain format'),
        )

        self.args = parser.parse_args(self.argv)
        if self.args.path:
            self.format_report(Disks(self.args.path))
        else:
            self.format_report(Disks())

