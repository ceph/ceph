import argparse
import json
from textwrap import dedent

# import ceph_volume.process

from ceph_volume_zfs.util.disk import Disks

class Inventory(object):

    help = 'Generate a list of available devices'

    def __init__(self, argv):
        self.argv = argv

    def format_report(self, inventory):
        if self.args.format == 'json':
            print(json.dumps(inventory.json_report()))
        elif self.args.format == 'json-pretty':
            print(json.dumps(inventory.json_report(), indent=4, sort_keys=True))
        else:
            print(inventory.pretty_report())

    def main(self):
        sub_command_help = dedent("""
	Generate an inventory of available devices
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs inventory',
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

        self.args = parser.parse_args(self.argv)
        if self.args.path:
            self.format_report(Disks(self.args.path)) 
        else:
            self.format_report(Disks()) 

