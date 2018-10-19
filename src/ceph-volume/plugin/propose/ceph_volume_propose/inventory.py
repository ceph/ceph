# -*- coding: utf-8 -*-

import argparse

from . import devices


class Inventory(object):

    help = "Get this nodes available disk inventory"

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        self.devices = devices.Devices()
        parser = argparse.ArgumentParser(
            prog='ceph-volume-propose',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.help,
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Show all devices',
        )
        self.args = parser.parse_args(self.argv)
        print(self.devices.pretty_report(self.args.all))
