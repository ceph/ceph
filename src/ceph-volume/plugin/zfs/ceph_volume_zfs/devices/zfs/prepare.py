# vim: expandtab smarttab shiftwidth=4 softtabstop=4

from __future__ import print_function
import os
import json
import logging
from textwrap import dedent

from ceph_volume import conf, decorators, terminal
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system
from ceph_volume.util.arg_validators import exclude_group_options

from ceph_volume_zfs.api import zfs as api
from ceph_volume_zfs.util import disk
from ceph_volume_zfs.util import prepare as prepare_zfs

from .common import prepare_parser, rollback_osd

logger = logging.getLogger(__name__)


def prepare_filestore(device, journal, secrets, tags, osd_id, fsid):
    """
    :param device: The name of the logical volume to work with
    :param journal: similar to device but can also be a regular/plain disk
    :param secrets: A dict with secrets needed to create the osd (e.g. cephx)
    :param id_: The OSD id
    :param fsid: The OSD fsid, also known as the OSD UUID
    """
    cephx_secret = secrets.get('cephx_secret', prepare_utils.create_key())

    # create the directory
    api.create_osd_path(device, osd_id)
    if journal is not None:
        prepare_utils.link_journal(journal, osd_id)

    # get the latest monmap
    prepare_utils.get_monmap(osd_id)
    # prepare the osd filesystem
    prepare_zfs.osd_mkfs_filestore(osd_id, fsid, cephx_secret)
    # write the OSD keyring if it doesn't exist already
    prepare_utils.write_keyring(osd_id, cephx_secret)


class Prepare(object):

    help = 'Format an ZFS volume and associate it with an OSD'

    def __init__(self, argv):
        self.argv = argv
        self.osd_id = None

    def get_zfsvol(self, argument):
        """
        Perform some parsing of the command-line value so that the process
        can determine correctly if it got a device path or an zfsvol

        :param argument: The command-line value that will need to be split to
                         retrieve the actual zfsvol
        """
        try:
            zp_name, zv_name = argument.split('/')
        except (ValueError, AttributeError):
            return None
        return api.get_zfsvol(zv_name=zv_name, zp_name=zp_name)

    def setup_device(self, device_type, device_name, tags):
        """
        Check if ``device`` is an zfsvol, if so, set the tags, making sure to
        update the tags with the zfsvol_uuid and zfsvol_path which the
        incoming tags will not have.
        """
        if device_name is None:
            return '', '', tags
        tags['ceph:type'] = device_type
        zfsvol = self.get_zfsvol(device_name)
        if zfsvol:
            uuid = zfsvol.zv_uuid
            path = zfsvol.zv_path
            tags['ceph:%s_uuid' % device_type] = uuid
            tags['ceph:%s_device' % device_type] = path
            zfsvol.set_tags(tags)
        else:
            # otherwise assume this is a regular disk partition
            path = device_name
            tags['ceph:%s_uuid' % device_type] = uuid
            tags['ceph:%s_device' % device_type] = path
        return path, uuid, tags

    def prepare_device(self, arg, device_type, cluster_fsid, osd_fsid, osd_id):
        """
        Check if ``arg`` is a device or partition to create an zfsvol out of it
        with a distinct volume group name, assigning zfs tags on it and
        ultimately, returning the logical volume object.  Failing to detect
        a device or partition will result in error.

        :param arg: The value of ``--data`` when parsing args
        :param device_type: Usually, either ``data`` or ``block``
                            (filestore vs. bluestore)
        :param cluster_fsid: The cluster fsid/uuid
        :param osd_fsid: The OSD fsid/uuid
        :param osd_id: The OSD id
        return a zfs volume object just created.
        """
        # we must create a zpool, and then a single zfsvol
        zp = api.create_zpool(arg, osd_id)
        self.zv_name = "osd.%s" % osd_id
        self.zv_path = "osd.%s" % osd_id
        return api.create_zfsvol(
            zp.name,  # the zpool to create the OSD on
            osd_id,
            tags={'ceph:type': device_type})

    def safe_prepare(self, args):
        """
        An intermediate step between `main()` and `prepare()` so that we can
        capture the `self.osd_id` in case we need to rollback
        """
        try:
            self.prepare(args)
        except Exception:
            logger.exception('zfs prepare was unable to complete')
            logger.info('will rollback OSD ID: %s creation' % self.osd_id)
            rollback_osd(args, self.osd_id)
            raise
        terminal.success("ceph-volume zfs prepare successful for: %s"
                         % args.data)

    @decorators.needs_root
    def prepare(self, args):
        if not (disk.is_partition(args.data) or disk.is_device(args.data)):
            error = [
                'Cannot use device (%s).' % args.data,
                'A zfsvol path or an existing device is needed']
            raise RuntimeError(' '.join(error))

        # FIXME we don't allow re-using a keyring, we always generate one for
        # the OSD, this needs to be fixed.
        # This could either be a file (!)
        # or a string (!!)
        # or some flags that we would need to compound into a dict so that we
        # can convert to JSON (!!!)
        secrets = {'cephx_secret': prepare_utils.create_key()}

        cluster_fsid = conf.ceph.get('global', 'fsid')
        osd_fsid = args.osd_fsid or system.generate_uuid()
        crush_device_class = args.crush_device_class
        if crush_device_class:
            secrets['crush_device_class'] = crush_device_class
        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(
                osd_fsid,
                json.dumps(secrets),
                osd_id=args.osd_id)
        tags = {
            'ceph:osd_fsid': osd_fsid,
            'ceph:osd_id': self.osd_id,
            'ceph:cluster_fsid': cluster_fsid,
            'ceph:cluster_name': conf.cluster,
            'ceph:crush_device_class': crush_device_class,
        }
        if args.filestore:
            data_zfs = self.get_zfsvol(args.data)
            if not data_zfs:
                data_zfs = self.prepare_device(args.data,
                                               'data',
                                               cluster_fsid,
                                               osd_fsid,
                                               self.osd_id)
            tags['ceph:data_device'] = 'osd.%s' % self.osd_id

            journal_device = None
            # journal_device, journal_uuid, tags =
            #     self.setup_device('journal', args.journal, tags)

            tags['ceph:type'] = 'data'
            data_zfs.set_tags(tags)

            prepare_filestore(
                data_zfs.mountpoint,
                journal_device,
                secrets,
                tags,
                self.osd_id,
                osd_fsid,
            )

    def main(self):
        sub_command_help = dedent("""
            Prepare an OSD by assigning an ID and FSID, registering them with
            the cluster with an ID and FSID, formatting and mounting the
            volume, and finally by adding all the metadata to the logical
            volumes using ZFS tags, so that it can later be discovered.

            Existing logical volume (zfs volume):

                ceph-volume zfs prepare --data {zfsvol}

            Existing device, that will be made a pool and volume:

                ceph-volume zfs prepare --data /path/to/device

        """)
        parser = prepare_parser(
            prog='ceph-volume zfs prepare',
            description=sub_command_help,
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        exclude_group_options(parser, argv=self.argv,
                              groups=['filestore', 'bluestore'])
        args = parser.parse_args(self.argv)
        # Default to filestore here since defaulting it in add_argument may
        # cause both to be True
        args.filestore = True
        self.safe_prepare(args)
