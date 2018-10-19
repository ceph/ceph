# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse

from ceph_volume import terminal
from . import inventory
from . import drive_groups


class Proposal(object):

    help_menu = "Get disk inventory and propose Drive Groups"
    _help = """
Get a nodes disk inventory and generate Drive Group proposals

{sub_help}
    """
    name = "propose"

    mapper = {
        'inventory': inventory.Inventory,
        'drive_groups': drive_groups.DriveGroups,
    }

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume propose',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()

    def print_help(self, sub_help):
        return self._help.format(sub_help=sub_help)
