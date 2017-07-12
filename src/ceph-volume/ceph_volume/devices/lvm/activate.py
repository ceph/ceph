import argparse
import os
from textwrap import dedent
from ceph_volume import process
from ceph_volume.util import activate as activate_utils
from ceph_volume.systemd import systemctl
import api


def activate_filestore(lvs):
    # find the osd
    osd_lv = lvs.get(lv_tags={'ceph.type': 'data'})
    osd_id = osd_lv.tags['ceph.osd_id']
    # it may have a volume with a journal
    osd_journal_lv = lvs.get(lv_tags={'ceph.type': 'journal'})
    if not osd_journal_lv:
        osd_journal = osd_lv.tags.get('ceph.journal_device')
    else:
        osd_journal = osd_journal.lv_path

    if not osd_journal:
        raise RuntimeError('unable to detect an lv or device journal for OSD %s' % osd_id)

    # mount the osd
    source = osd_lv.lv_path
    destination = '/var/lib/ceph/osd/ceph-%s' % osd_id
    process.call(['sudo', 'mount', '-v', source, destination])

    # ensure that the symlink for the journal is there
    if not os.path.exists(osd_journal):
        source = osd_journal
        destination = '/var/lib/ceph/osd/ceph-%s/journal' % osd_id
        process.call(['sudo', 'ln', '-s', source, destination])

    # register the osd
    activate_utils.add_osd_to_mon(osd_id)

    # start the OSD
    systemctl.start_osd(osd_id)


def activate_bluestore(lvs):
    # TODO
    pass


class Activate(object):

    help = 'Discover and mount the LVM device associated with an OSD ID and start the Ceph OSD'

    def __init__(self, argv):
        self.argv = argv

    def activate(self, args):
        lvs = api.Volumes()
        # filter them down for the OSD ID and FSID we need to activate
        lvs.filter(lv_tags={'ceph.osd_id': args.id, 'ceph.osd_fsid': args.fsid})
        if not lvs:
            raise RuntimeError('could not find osd.%s with fsid %s' % (args.id, args.fsid))
        activate_filestore(lvs)

    def main(self):
        sub_command_help = dedent("""
        Activate OSDs by discovering them with LVM and mounting them in their
        appropriate destination:

            ceph-volume lvm activate {ID} {FSID}

        The lvs associated with the OSD need to have been prepared previously,
        so that all needed tags and metadata exist.

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'id',
            metavar='ID',
            nargs='?',
            help='The ID of the OSD, usually an integer, like 0'
        )
        parser.add_argument(
            'fsid',
            metavar='FSID',
            nargs='?',
            help='The FSID of the OSD, similar to a SHA1'
        )
        parser.add_argument(
            '--bluestore',
            action='store_true', default=False,
            help='filestore objectstore (not yet implemented)',
        )
        parser.add_argument(
            '--filestore',
            action='store_true', default=True,
            help='filestore objectstore (current default)',
        )
        args = parser.parse_args(self.argv[1:])
        if len(self.argv) <= 1:
            return parser.print_help()
        self.activate(args)
