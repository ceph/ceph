import argparse
from textwrap import dedent
from ceph_volume import terminal
from . import list
from . import prepare
from . import activate

class Raw(object):

    help = 'Manage single-device OSDs on raw block devices'

    _help = dedent("""
    Manage a single-device OSD on a raw block device.  Rely on
    the existing device labels to store any needed metadata.

    {sub_help}
    """)

    mapper = {
        'list': list.List,
        'prepare': prepare.Prepare,
        'activate': activate.Activate,
    }

    def __init__(self, argv):
        self.argv = argv

    def print_help(self, sub_help):
        return self._help.format(sub_help=sub_help)

    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume raw',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()
