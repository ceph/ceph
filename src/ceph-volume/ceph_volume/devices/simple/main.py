import argparse
from textwrap import dedent
from ceph_volume import terminal
from . import scan
from . import activate
from . import trigger


class Simple(object):

    help = 'Manage already deployed OSDs with ceph-volume'

    _help = dedent("""
    Take over a deployed OSD, persisting its metadata in /etc/ceph/osd/ so that it can be managed
    with ceph-volume directly. Avoids UDEV and ceph-disk handling.

    {sub_help}
    """)

    mapper = {
        'scan': scan.Scan,
        'activate': activate.Activate,
        'trigger': trigger.Trigger,
    }

    def __init__(self, argv):
        self.argv = argv

    def print_help(self, sub_help):
        return self._help.format(sub_help=sub_help)

    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume simple',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()
