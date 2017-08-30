from __future__ import print_function
import json
import os
from textwrap import dedent
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system, disk
from ceph_volume import conf, decorators, terminal
from . import api
from .common import prepare_parser


def prepare_filestore(device, journal, secrets, id_=None, fsid=None):
    """
    :param device: The name of the volume group or lvm to work with
    :param journal: similar to device but can also be a regular/plain disk
    :param secrets: A dict with the secrets needed to create the osd (e.g. cephx)
    :param id_: The OSD id
    :param fsid: The OSD fsid, also known as the OSD UUID
    """
    cephx_secret = secrets.get('cephx_secret', prepare_utils.create_key())
    json_secrets = json.dumps(secrets)

    # allow re-using an existing fsid, in case prepare failed
    fsid = fsid or system.generate_uuid()
    # allow re-using an id, in case a prepare failed
    osd_id = id_ or prepare_utils.create_id(fsid, json_secrets)
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
    # write the OSD keyring if it doesn't exist already
    prepare_utils.write_keyring(osd_id, cephx_secret)


def prepare_bluestore():
    raise NotImplemented()


class Prepare(object):

    help = 'Format an LVM device and associate it with an OSD'

    def __init__(self, argv):
        self.argv = argv

    def get_journal_ptuuid(self, argument):
        uuid = disk.get_partuuid(argument)
        if not uuid:
            terminal.error('blkid could not detect a PARTUUID for device: %s' % argument)
            raise RuntimeError('unable to use device for a journal')
        return uuid

    def get_journal_lv(self, argument):
        """
        Perform some parsing of the value of ``--journal`` so that the process
        can determine correctly if it got a device path or an lv
        :param argument: The value of ``--journal``, that will need to be split
        to retrieve the actual lv
        """
        try:
            vg_name, lv_name = argument.split('/')
        except (ValueError, AttributeError):
            return None
        return api.get_lv(lv_name=lv_name, vg_name=vg_name)

    @decorators.needs_root
    def prepare(self, args):
        # FIXME we don't allow re-using a keyring, we always generate one for the
        # OSD, this needs to be fixed. This could either be a file (!) or a string
        # (!!) or some flags that we would need to compound into a dict so that we
        # can convert to JSON (!!!)
        secrets = {'cephx_secret': prepare_utils.create_key()}

        cluster_fsid = conf.ceph.get('global', 'fsid')
        fsid = args.osd_fsid or system.generate_uuid()
        #osd_id = args.osd_id or prepare_utils.create_id(fsid)
        # allow re-using an id, in case a prepare failed
        osd_id = args.osd_id or prepare_utils.create_id(fsid, json.dumps(secrets))
        vg_name, lv_name = args.data.split('/')
        if args.filestore:
            data_lv = api.get_lv(lv_name=lv_name, vg_name=vg_name)

            # we must have either an existing data_lv or a newly created, so lets make
            # sure that the tags are correct
            if not data_lv:
                raise RuntimeError('no data logical volume found with: %s' % args.data)

            if not args.journal:
                raise RuntimeError('--journal is required when using --filestore')

            journal_lv = self.get_journal_lv(args.journal)
            if journal_lv:
                journal_device = journal_lv.lv_path
                journal_uuid = journal_lv.lv_uuid
                # we can only set tags on an lv, the pv (if any) can't as we
                # aren't making it part of an lvm group (vg)
                journal_lv.set_tags({
                    'ceph.type': 'journal',
                    'ceph.osd_fsid': fsid,
                    'ceph.osd_id': osd_id,
                    'ceph.cluster_fsid': cluster_fsid,
                    'ceph.journal_device': journal_device,
                    'ceph.journal_uuid': journal_uuid,
                    'ceph.data_device': data_lv.lv_path,
                    'ceph.data_uuid': data_lv.lv_uuid,
                })

            # allow a file
            elif os.path.isfile(args.journal):
                journal_uuid = ''
                journal_device = args.journal

            # otherwise assume this is a regular disk partition
            else:
                journal_uuid = self.get_journal_ptuuid(args.journal)
                journal_device = args.journal

            data_lv.set_tags({
                'ceph.type': 'data',
                'ceph.osd_fsid': fsid,
                'ceph.osd_id': osd_id,
                'ceph.cluster_fsid': cluster_fsid,
                'ceph.journal_device': journal_device,
                'ceph.journal_uuid': journal_uuid,
                'ceph.data_device': data_lv.lv_path,
                'ceph.data_uuid': data_lv.lv_uuid,
            })

            prepare_filestore(
                data_lv.lv_path,
                journal_device,
                secrets,
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
