from __future__ import print_function
import os
from textwrap import dedent
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system
from ceph_volume import conf
from . import api
from .common import prepare_parser


def canonical_device_path(device):
    """
    Ensure that a device is canonical (full path) and that it exists so that
    it can be used throughout the prepare/activate process
    """
    # FIXME: this is obviously super naive
    inferred = os.path.join('/dev', device)
    if os.path.exists(os.path.abspath(device)):
        return device
    elif os.path.exists(inferred):
        return inferred
    raise RuntimeError('Selected device does not exist: %s' % device)


def prepare_filestore(device, journal, id_=None, fsid=None):
    # allow re-using an existing fsid, in case prepare failed
    fsid = fsid or system.generate_uuid()
    # allow re-using an id, in case a prepare failed
    osd_id = id_ or prepare_utils.create_id(fsid)
    # create the directory
    prepare_utils.create_path(osd_id)
    # format the device
    prepare_utils.format_device(device)
    # mount the data device
    prepare_utils.mount_osd(device, osd_id)
    # symlink the journal
    prepare_utils.link_journal(journal, osd_id)
    # get the latest monmap
    prepare_utils.get_monmap(osd_id)
    # prepare the osd filesystem
    prepare_utils.osd_mkfs(osd_id, fsid)


def prepare_bluestore():
    raise NotImplemented()


class Prepare(object):

    help = 'Format an LVM device and associate it with an OSD'

    def __init__(self, argv):
        self.argv = argv

    def prepare(self, args):
        cluster_fsid = conf.ceph.get('global', 'fsid')
        fsid = args.osd_fsid or system.generate_uuid()
        osd_id = args.osd_id or prepare_utils.create_id(fsid)
        journal_name = "journal_%s" % fsid
        osd_name = "osd_%s" % fsid
        if args.filestore:
            data_vg = api.get_vg(vg_name=args.data)
            data_lv = api.get_lv(lv_name=args.data)
            journal_vg = api.get_vg(vg_name=args.journal)
            journal_lv = api.get_lv(lv_name=args.journal)
            journal_device = None
            # it is possible to pass a device as a journal that is not
            # an actual logical volume (or group)
            if not args.journal:
                if data_lv:
                    raise RuntimeError('--journal is required when not using a vg for OSD data')
                # collocated: carve out the journal from the data vg
                if data_vg:
                    journal_lv = api.create_lv(
                        name=journal_name,
                        group=data_vg.name,
                        size=args.journal_size,
                        osd_fsid=fsid,
                        osd_id=osd_id,
                        type='journal',
                        cluster_fsid=cluster_fsid
                    )

            # if a volume group was defined for the journal create that first
            if journal_vg:
                journal_lv = api.create_lv(
                    name=journal_name,
                    group=args.journal,
                    size=args.journal_size,
                    osd_fsid=fsid,
                    osd_id=osd_id,
                    type='journal',
                    cluster_fsid=cluster_fsid
                )
            if journal_lv:
                journal_device = journal_lv.lv_path
            # The journal is probably a device, not in LVM
            elif args.journal:
                journal_device = canonical_device_path(args.journal)
            # At this point we must have a journal_lv or a journal device
            # now create the osd from the group if that was found
            if data_vg:
                # XXX make sure that a there aren't more OSDs than physical
                # devices from this volume group
                data_lv = api.create_lv(
                    name=osd_name,
                    group=args.data,
                    osd_fsid=fsid,
                    osd_id=osd_id,
                    type='data',
                    journal_device=journal_device,
                    cluster_fsid=cluster_fsid
                )
            # we must have either an existing data_lv or a newly created, so lets make
            # sure that the tags are correct
            data_lv.set_tags({
                'ceph.type': 'data',
                'ceph.osd_fsid': fsid,
                'ceph.osd_id': osd_id,
                'ceph.cluster_fsid': cluster_fsid,
                'ceph.journal_device': journal_device,
                'ceph.data_device': data_lv.lv_path,
            })
            if not data_lv:
                raise RuntimeError('no data logical volume found with: %s' % args.data)

            prepare_filestore(
                data_lv.lv_path,
                journal_device,
                id_=osd_id,
                fsid=fsid,
            )
        elif args.bluestore:
            prepare_bluestore(args)

    def main(self):
        sub_command_help = dedent("""
        Prepare an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, and
        finally by adding all the metadata to the logical volumes using LVM
        tags, so that it can later be discovered.

        Once the OSD is ready, an ad-hoc systemd unit will be enabled so that
        it can later get activated and the OSD daemon can get started.

        Most basic Usage looks like (journal will be collocated from the same volume group):

            ceph-volume lvm prepare --data {volume group name}


        Example calls for supported scenarios:

        Dedicated volume group for Journal(s)
        -------------------------------------

          Existing logical volume (lv) or device:

              ceph-volume lvm prepare --data {logical volume} --journal /path/to/{lv}|{device}

          Or:

              ceph-volume lvm prepare --data {data volume group} --journal {journal volume group}

        Collocated (same group) for data and journal
        --------------------------------------------

              ceph-volume lvm prepare --data {volume group}

        """)
        parser = prepare_parser(
            prog='ceph-volume lvm prepare',
            description=sub_command_help,
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        self.prepare(args)
