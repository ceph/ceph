import argparse
from textwrap import dedent
# import ceph_volume.process
from ceph_volume import conf
from ceph_volume_zfs.util.disk import Disks


class Inventory(object):

    help = 'Generate a list of available devices'

    def __init__(self, argv):
        self.argv = argv

    def format_report(self, inventory):
        fmt = getattr(conf, 'format', None)
        if fmt == 'json':
            import json
            print(json.dumps(inventory.json_report()))
        elif fmt == 'json-pretty' or getattr(conf, 'json', False):
            import json
            print(json.dumps(inventory.json_report(), indent=4, sort_keys=True))
        elif fmt == 'plain':
            print(inventory.pretty_report())
        elif getattr(conf, 'verbose', False):
            print(inventory.verbose_report())
        else:
            print(inventory.pretty_report())

    def main(self):
        sub_command_help = dedent("""
        Generate an inventory of available devices

        Use -v (before or after the subcommand) for a detailed
        per-disk report including partitions and mount status, or -j
        for JSON output.
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
        self.args = parser.parse_args(self.argv)
        if self.args.path:
            self.format_report(Disks(self.args.path))
        else:
            self.format_report(Disks())

