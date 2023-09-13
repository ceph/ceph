import argparse
import logging
import sys
from textwrap import dedent
from ceph_volume import conf
from ceph_volume.util import encryption, system
from ceph_volume.api.lvm import get_lvs_by_tag

logger = logging.getLogger(__name__)


def deactivate_osd(osd_id=None, osd_uuid=None):

    lvs = []
    if osd_uuid is not None:
        lvs = get_lvs_by_tag('ceph.osd_fsid={}'.format(osd_uuid))
        osd_id = next(lv.tags['ceph.osd_id'] for lv in lvs)
    else:
        lvs = get_lvs_by_tag('ceph.osd_id={}'.format(osd_id))

    data_lv = next(lv for lv in lvs if lv.tags['ceph.type'] in ['data', 'block'])

    conf.cluster = data_lv.tags['ceph.cluster_name']
    logger.debug('Found cluster name {}'.format(conf.cluster))

    tmpfs_path = '/var/lib/ceph/osd/{}-{}'.format(conf.cluster, osd_id)
    system.unmount_tmpfs(tmpfs_path)

    for lv in lvs:
        if lv.tags.get('ceph.encrypted', '0') == '1':
            encryption.dmcrypt_close(mapping=lv.lv_uuid, skip_path_check=True)


class Deactivate(object):

    help = 'Deactivate OSDs'

    def deactivate(self, args=None):
        if args:
            self.args = args
        try:
            deactivate_osd(self.args.osd_id, self.args.osd_uuid)
        except StopIteration:
            logger.error(('No data or block LV found for OSD'
                          '{}').format(self.args.osd_id))
            sys.exit(1)

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        sub_command_help = dedent("""
        Deactivate unmounts and OSDs tmpfs and closes any crypt devices.

            ceph-volume lvm deactivate {ID} {FSID}

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm deactivate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'osd_id',
            nargs='?',
            help='The ID of the OSD'
        )
        parser.add_argument(
            'osd_uuid',
            nargs='?',
            help='The UUID of the OSD, similar to a SHA1, takes precedence over osd_id'
        )
        # parser.add_argument(
        #     '--all',
        #     action='store_true',
        #     help='Deactivate all OSD volumes found in the system',
        # )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if not args.osd_id and not args.osd_uuid:
            raise ValueError(('Can not identify OSD, pass either all or'
                             'osd_id or osd_uuid'))
        self.deactivate(args)
