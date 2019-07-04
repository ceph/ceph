# -*- coding: utf-8 -*-

import argparse

import logging
from textwrap import dedent
from ceph_volume import terminal
from .actions import DriveGroupsApply, DriveGroupsShow

mlogger = terminal.MultiLogger(__name__)
logger = logging.getLogger(__name__)


class DriveGroups(object):

    help = 'foo'

    _help = dedent("""

    show [reports the resulting changes]
    apply [applies the drivegroup, creates OSDs if needed #TODO re-write]

    {sub_help}
    """)

    mapper = {'show': DriveGroupsShow, 'apply': DriveGroupsApply}

    def __init__(self, argv):
        self.argv = argv

    def print_help(self, sub_help):
        return self._help.format(sub_help=sub_help)

    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume drivegroup',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()
