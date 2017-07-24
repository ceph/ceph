import argparse
from textwrap import dedent
from ceph_volume import terminal
from . import activate
from . import prepare
from . import create


class LVM(object):

    help = 'Use LVM and LVM-based technologies like dmcache to deploy OSDs'

    _help = dedent("""
    Use LVM and LVM-based technologies like dmcache to deploy OSDs

    {sub_help}
    """)

    mapper = {
        'activate': activate.Activate,
        'prepare': prepare.Prepare,
        'create': create.Create,
    }

    def __init__(self, argv):
        self.argv = argv

    def print_help(self, sub_help):
        return self._help.format(sub_help=sub_help)

    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()
