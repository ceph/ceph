from __future__ import print_function
import argparse
from textwrap import dedent
from ceph_volume.exceptions import SuffixParsingError
from ceph_volume import decorators
from .activate import Activate


def parse_osd_id(string):
    osd_id = string.split('-', 1)[0]
    if not osd_id:
        raise SuffixParsingError('OSD id', string)
    if osd_id.isdigit():
        return osd_id
    raise SuffixParsingError('OSD id', string)


def parse_osd_uuid(string):
    osd_id = '%s-' % parse_osd_id(string)
    # remove the id first
    osd_uuid = string.split(osd_id, 1)[-1]
    if not osd_uuid:
        raise SuffixParsingError('OSD uuid', string)
    return osd_uuid


class Trigger(object):

    help = 'systemd helper to activate an OSD'

    def __init__(self, argv):
        self.argv = argv

    @decorators.needs_root
    def main(self):
        sub_command_help = dedent("""
        ** DO NOT USE DIRECTLY **
        This tool is meant to help the systemd unit that knows about OSDs.

        Proxy OSD activation to ``ceph-volume lvm activate`` by parsing the
        input from systemd, detecting the UUID and ID associated with an OSD::

            ceph-volume lvm trigger {SYSTEMD-DATA}

        The systemd "data" is expected to be in the format of::

            {OSD ID}-{OSD UUID}

        The lvs associated with the OSD need to have been prepared previously,
        so that all needed tags and metadata exist.
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm trigger',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'systemd_data',
            metavar='SYSTEMD_DATA',
            nargs='?',
            help='Data from a systemd unit containing ID and UUID of the OSD, like asdf-lkjh-0'
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        osd_id = parse_osd_id(args.systemd_data)
        osd_uuid = parse_osd_uuid(args.systemd_data)
        Activate(['--auto-detect-objectstore', osd_id, osd_uuid]).main()
