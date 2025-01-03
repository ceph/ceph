# vim: expandtab smarttab shiftwidth=4 softtabstop=4

import argparse
from textwrap import dedent
from ceph_volume import terminal

from . import inventory
from . import prepare
from . import zap

class ZFSDEV(object):

    help = 'Use ZFS to deploy OSDs'

    _help = dedent("""
        Use ZFS to deploy OSDs

        {sub_help}
    """)

    def __init__(self, argv):
        self.argv = argv

    def print_help(self, sub_help):
        return self._help.format(sub_help=sub_help)

    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()
