from __future__ import print_function
import argparse
import logging
import os
from textwrap import dedent
from ceph_volume.util import system, disk, merge_dict
from ceph_volume.util.device import Device
from ceph_volume.util.arg_validators import valid_osd_id
from ceph_volume.util import encryption as encryption_utils
from ceph_volume import decorators, terminal, process
from ceph_volume.api import lvm as api
from ceph_volume.systemd import systemctl


logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)

def get_cluster_name(osd_id, osd_fsid):
    """
    From an ``osd_id`` and/or an ``osd_fsid``, filter out all the LVs in the
    system that match those tag values, then return cluster_name for the first
    one.
    """
    lv_tags = {}
    lv_tags['ceph.osd_id'] = osd_id
    lv_tags['ceph.osd_fsid'] = osd_fsid

    lvs = api.get_lvs(tags=lv_tags)
    if not lvs:
        mlogger.error(
            'Unable to find any LV for source OSD: id:{} fsid:{}'.format(
                osd_id,  osd_fsid)        )
        raise SystemExit('Unexpected error, terminating')
    return next(iter(lvs)).tags["ceph.cluster_name"]

def get_osd_path(osd_id, osd_fsid):
    return '/var/lib/ceph/osd/{}-{}'.format(
        get_cluster_name(osd_id, osd_fsid), osd_id)

def find_associated_devices(osd_id, osd_fsid):
    """
    From an ``osd_id`` and/or an ``osd_fsid``, filter out all the LVs in the
    system that match those tag values, further detect if any partitions are
    part of the OSD, and then return the set of LVs and partitions (if any).
    """
    lv_tags = {}
    lv_tags['ceph.osd_id'] = osd_id
    lv_tags['ceph.osd_fsid'] = osd_fsid

    lvs = api.get_lvs(tags=lv_tags)
    if not lvs:
        mlogger.error(
            'Unable to find any LV for source OSD: id:{} fsid:{}'.format(
                osd_id,  osd_fsid)        )
        raise SystemExit('Unexpected error, terminating')

    devices = set(ensure_associated_lvs(lvs, lv_tags))
    return [(Device(path), type) for path, type in devices if path]

def ensure_associated_lvs(lvs, lv_tags):
    """
    Go through each LV and ensure if backing devices (journal, wal, block)
    are LVs or partitions, so that they can be accurately reported.
    """
    # look for many LVs for each backing type, because it is possible to
    # receive a filtering for osd.1, and have multiple failed deployments
    # leaving many journals with osd.1 - usually, only a single LV will be
    # returned

    block_lvs = api.get_lvs(tags=merge_dict(lv_tags, {'ceph.type': 'block'}))
    db_lvs = api.get_lvs(tags=merge_dict(lv_tags, {'ceph.type': 'db'}))
    wal_lvs = api.get_lvs(tags=merge_dict(lv_tags, {'ceph.type': 'wal'}))
    backing_devices = [(block_lvs, 'block'), (db_lvs, 'db'),
                       (wal_lvs, 'wal')]

    verified_devices = []

    for lv in lvs:
        # go through each lv and append it, otherwise query `blkid` to find
        # a physical device. Do this for each type (journal,db,wal) regardless
        # if they have been processed in the previous LV, so that bad devices
        # with the same ID can be caught
        for ceph_lvs, type in backing_devices:

            if ceph_lvs:
                verified_devices.extend([(l.lv_path, type) for l in ceph_lvs])
                continue

            # must be a disk partition, by querying blkid by the uuid we are
            # ensuring that the device path is always correct
            try:
                device_uuid = lv.tags['ceph.{}_uuid'.format(type)]
            except KeyError:
                # Bluestore will not have ceph.journal_uuid, and Filestore
                # will not not have ceph.db_uuid
                continue

            osd_device = disk.get_device_from_partuuid(device_uuid)
            if not osd_device:
                # if the osd_device is not found by the partuuid, then it is
                # not possible to ensure this device exists anymore, so skip it
                continue
            verified_devices.append((osd_device, type))

    return verified_devices

class VolumeTagTracker(object):
    def __init__(self, devices, target_lv):
        self.target_lv = target_lv
        self.data_device = self.db_device = self.wal_device = None
        for device, type in devices:
            if type == 'block':
                self.data_device = device
            elif type == 'db':
                self.db_device = device
            elif type == 'wal':
                self.wal_device = device
        if not self.data_device:
            mlogger.error('Data device not found')
            raise SystemExit(
                "Unexpected error, terminating")
        if not self.data_device.is_lv:
            mlogger.error('Data device isn\'t LVM')
            raise SystemExit(
                "Unexpected error, terminating")

        self.old_target_tags = self.target_lv.tags.copy()
        self.old_data_tags = (
            self.data_device.lv_api.tags.copy()
            if self.data_device.is_lv else None)
        self.old_db_tags = (
            self.db_device.lv_api.tags.copy()
            if self.db_device and self.db_device.is_lv else None)
        self.old_wal_tags = (
            self.wal_device.lv_api.tags.copy()
            if self.wal_device and self.wal_device.is_lv else None)

    def update_tags_when_lv_create(self, create_type):
        tags = {}
        if not self.data_device.is_lv:
            mlogger.warning(
                'Data device is not LVM, wouldn\'t update LVM tags')
        else:
            tags["ceph.{}_uuid".format(create_type)] = self.target_lv.lv_uuid
            tags["ceph.{}_device".format(create_type)] = self.target_lv.lv_path
            self.data_device.lv_api.set_tags(tags)

            tags = self.data_device.lv_api.tags.copy()
            tags["ceph.type"] = create_type
            self.target_lv.set_tags(tags)

        aux_dev = None
        if create_type == "db" and self.wal_device:
            aux_dev = self.wal_device
        elif create_type == "wal" and self.db_device:
            aux_dev = self.db_device
        else:
            return
        if not aux_dev.is_lv:
            mlogger.warning(
                '{} device is not LVM, wouldn\'t update LVM tags'.format(
                    create_type.upper()))
        else:
            tags = {}
            tags["ceph.{}_uuid".format(create_type)] = self.target_lv.lv_uuid
            tags["ceph.{}_device".format(create_type)] = self.target_lv.lv_path
            aux_dev.lv_api.set_tags(tags)

    def remove_lvs(self, source_devices, target_type):
        remaining_devices = [self.data_device]
        if self.db_device:
            remaining_devices.append(self.db_device)
        if self.wal_device:
            remaining_devices.append(self.wal_device)

        outdated_tags = []
        for device, type in source_devices:
            if type == "block" or type == target_type:
                continue
            remaining_devices.remove(device)
            if device.is_lv:
                outdated_tags.append("ceph.{}_uuid".format(type))
                outdated_tags.append("ceph.{}_device".format(type))
                device.lv_api.clear_tags()
        if len(outdated_tags) > 0:
            for d in remaining_devices:
                if d and d.is_lv:
                    d.lv_api.clear_tags(outdated_tags)

    def replace_lvs(self, source_devices, target_type):
        remaining_devices = [self.data_device]
        if self.db_device:
            remaining_devices.append(self.db_device)
        if self.wal_device:
            remaining_devices.append(self.wal_device)

        outdated_tags = []
        for device, type in source_devices:
            if type == "block":
                continue
            remaining_devices.remove(device)
            if device.is_lv:
                outdated_tags.append("ceph.{}_uuid".format(type))
                outdated_tags.append("ceph.{}_device".format(type))
                device.lv_api.clear_tags()

        new_tags = {}
        new_tags["ceph.{}_uuid".format(target_type)] = self.target_lv.lv_uuid
        new_tags["ceph.{}_device".format(target_type)] = self.target_lv.lv_path

        for d in remaining_devices:
            if d and d.is_lv:
                if len(outdated_tags) > 0:
                    d.lv_api.clear_tags(outdated_tags)
                d.lv_api.set_tags(new_tags)

        if not self.data_device.is_lv:
            mlogger.warning(
                'Data device is not LVM, wouldn\'t properly update target LVM tags')
        else:
            tags = self.data_device.lv_api.tags.copy()

        tags["ceph.type"] = target_type
        tags["ceph.{}_uuid".format(target_type)] = self.target_lv.lv_uuid
        tags["ceph.{}_device".format(target_type)] = self.target_lv.lv_path
        self.target_lv.set_tags(tags)

    def undo(self):
        mlogger.info(
            'Undoing lv tag set')
        if self.data_device:
            if self.old_data_tags:
                self.data_device.lv_api.set_tags(self.old_data_tags)
            else:
                self.data_device.lv_api.clear_tags()
        if self.db_device:
            if self.old_db_tags:
                self.db_device.lv_api.set_tags(self.old_db_tags)
            else:
                self.db_device.lv_api.clear_tags()
        if self.wal_device:
            if self.old_wal_tags:
                self.wal_device.lv_api.set_tags(self.old_wal_tags)
            else:
                self.wal_device.lv_api.clear_tags()
        if self.old_target_tags:
            self.target_lv.set_tags(self.old_target_tags)
        else:
            self.target_lv.clear_tags()

class Migrate(object):

    help = 'Migrate BlueFS data from to another LVM device'

    def __init__(self, argv):
        self.argv = argv
        self.osd_id = None

    def get_source_devices(self, devices, target_type=""):
        ret = []
        for device, type in devices:
            if type == target_type:
                continue
            if type == 'block':
                if 'data' not in self.args.from_:
                    continue;
            elif type == 'db':
                if 'db' not in self.args.from_:
                    continue;
            elif type == 'wal':
                if 'wal' not in self.args.from_:
                    continue;
            ret.append([device, type])
        if ret == []:
            mlogger.error('Source device list is empty')
            raise SystemExit(
                'Unable to migrate to : {}'.format(self.args.target))
        return ret

    # ceph-bluestore-tool uses the following replacement rules
    # (in the order of precedence, stop on the first match)
    # if source list has DB volume - target device replaces it.
    # if source list has WAL volume - target device replace it.
    # if source list has slow volume only - operation isn't permitted,
    #  requires explicit allocation via new-db/new-wal command.detects which
    def get_target_type_by_source(self, devices):
        ret = None
        for device, type in devices:
            if type == 'db':
                return 'db'
            elif type == 'wal':
                ret = 'wal'
        return ret

    def get_filename_by_type(self, type):
        filename = 'block'
        if type == 'db' or type == 'wal':
            filename += '.' + type
        return filename

    def get_source_args(self, osd_path, devices):
        ret = []
        for device, type in devices:
            ret = ret + ["--devs-source", os.path.join(
                osd_path, self.get_filename_by_type(type))]
        return ret

    def close_encrypted(self, source_devices):
        # close source device(-s) if they're encrypted and have been removed
        for device,type in source_devices:
            if (type == 'db' or type == 'wal'):
                logger.info("closing dmcrypt volume {}"
                   .format(device.lv_api.lv_uuid))
                encryption_utils.dmcrypt_close(
                   mapping = device.lv_api.lv_uuid, skip_path_check=True)

    @decorators.needs_root
    def migrate_to_new(self, osd_id, osd_fsid, devices, target_lv):
        source_devices = self.get_source_devices(devices)
        target_type = self.get_target_type_by_source(source_devices)
        if not target_type:
            mlogger.error(
                "Unable to determine new volume type,"
                " please use new-db or new-wal command before.")
            raise SystemExit(
                "Unable to migrate to : {}".format(self.args.target))

        target_path = target_lv.lv_path
        tag_tracker = VolumeTagTracker(devices, target_lv)
        # prepare and encrypt target if data volume is encrypted
        if tag_tracker.data_device.lv_api.encrypted:
            secret = encryption_utils.get_dmcrypt_key(osd_id, osd_fsid)
            mlogger.info(' preparing dmcrypt for {}, uuid {}'.format(target_lv.lv_path, target_lv.lv_uuid))
            target_path = encryption_utils.prepare_dmcrypt(
                key=secret, device=target_path, mapping=target_lv.lv_uuid)
        try:
            # we need to update lvm tags for all the remaining volumes
            # and clear for ones which to be removed

            # ceph-bluestore-tool removes source volume(s) other than block one
            # and attaches target one after successful migration
            tag_tracker.replace_lvs(source_devices, target_type)

            osd_path = get_osd_path(osd_id, osd_fsid)
            source_args = self.get_source_args(osd_path, source_devices)
            mlogger.info("Migrate to new, Source: {} Target: {}".format(
                source_args, target_path))
            stdout, stderr, exit_code = process.call([
                'ceph-bluestore-tool',
                '--path',
                osd_path,
                '--dev-target',
                target_path,
                '--command',
                'bluefs-bdev-migrate'] +
                source_args)
            if exit_code != 0:
                mlogger.error(
                    'Failed to migrate device, error code:{}'.format(exit_code))
                raise SystemExit(
                    'Failed to migrate to : {}'.format(self.args.target))

            system.chown(os.path.join(osd_path, "block.{}".format(
                target_type)))
            if tag_tracker.data_device.lv_api.encrypted:
                self.close_encrypted(source_devices)
            terminal.success('Migration successful.')

        except:
            tag_tracker.undo()
            raise

        return

    @decorators.needs_root
    def migrate_to_existing(self, osd_id, osd_fsid, devices, target_lv):
        target_type = target_lv.tags["ceph.type"]
        if target_type == "wal":
            mlogger.error("Migrate to WAL is not supported")
            raise SystemExit(
                "Unable to migrate to : {}".format(self.args.target))
        target_filename = self.get_filename_by_type(target_type)
        if (target_filename == ""):
            mlogger.error(
                "Target Logical Volume doesn't have proper volume type "
                "(ceph.type LVM tag): {}".format(target_type))
            raise SystemExit(
                "Unable to migrate to : {}".format(self.args.target))

        osd_path = get_osd_path(osd_id, osd_fsid)
        source_devices = self.get_source_devices(devices, target_type)
        target_path = os.path.join(osd_path, target_filename)
        tag_tracker = VolumeTagTracker(devices, target_lv)

        try:
            # ceph-bluestore-tool removes source volume(s) other than
            # block and target ones after successful migration
            tag_tracker.remove_lvs(source_devices, target_type)
            source_args = self.get_source_args(osd_path, source_devices)
            mlogger.info("Migrate to existing, Source: {} Target: {}".format(
                source_args, target_path))
            stdout, stderr, exit_code = process.call([
                'ceph-bluestore-tool',
                '--path',
                osd_path,
                '--dev-target',
                target_path,
                '--command',
                'bluefs-bdev-migrate'] +
                source_args)
            if exit_code != 0:
                mlogger.error(
                    'Failed to migrate device, error code:{}'.format(exit_code))
                raise SystemExit(
                    'Failed to migrate to : {}'.format(self.args.target))
            if tag_tracker.data_device.lv_api.encrypted:
                self.close_encrypted(source_devices)
            terminal.success('Migration successful.')
        except:
            tag_tracker.undo()
            raise

        return

    @decorators.needs_root
    def migrate_osd(self):
        if self.args.osd_id and not self.args.no_systemd:
            osd_is_running = systemctl.osd_is_active(self.args.osd_id)
            if osd_is_running:
                mlogger.error('OSD is running, stop it with: '
                    'systemctl stop ceph-osd@{}'.format(
                        self.args.osd_id))
                raise SystemExit(
                    'Unable to migrate devices associated with OSD ID: {}'
                        .format(self.args.osd_id))

        target_lv = api.get_lv_by_fullname(self.args.target)
        if not target_lv:
            mlogger.error(
                'Target path "{}" is not a Logical Volume'.format(
                    self.args.target))
            raise SystemExit(
                'Unable to migrate to : {}'.format(self.args.target))
        devices = find_associated_devices(self.args.osd_id, self.args.osd_fsid)
        if (not target_lv.used_by_ceph):
            self.migrate_to_new(self.args.osd_id, self.args.osd_fsid,
                devices,
                target_lv)
        else:
            if (target_lv.tags['ceph.osd_id'] != self.args.osd_id or
                    target_lv.tags['ceph.osd_fsid'] != self.args.osd_fsid):
                mlogger.error(
                    'Target Logical Volume isn\'t used by the specified OSD: '
                        '{} FSID: {}'.format(self.args.osd_id,
                            self.args.osd_fsid))
                raise SystemExit(
                    'Unable to migrate to : {}'.format(self.args.target))

            self.migrate_to_existing(self.args.osd_id, self.args.osd_fsid,
                devices,
                target_lv)

    def make_parser(self, prog, sub_command_help):
        parser = argparse.ArgumentParser(
            prog=prog,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            '--osd-id',
            required=True,
            help='Specify an OSD ID to detect associated devices for zapping',
            type=valid_osd_id
        )

        parser.add_argument(
            '--osd-fsid',
            required=True,
            help='Specify an OSD FSID to detect associated devices for zapping',
        )
        parser.add_argument(
            '--target',
            required=True,
            help='Specify target Logical Volume (LV) to migrate data to',
        )
        parser.add_argument(
            '--from',
            nargs='*',
            dest='from_',
            required=True,
            choices=['data', 'db', 'wal'],
            help='Copy BlueFS data from DB device',
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip checking OSD systemd unit',
        )
        return parser

    def main(self):
        sub_command_help = dedent("""
        Moves BlueFS data from source volume(s) to the target one, source
        volumes (except the main (i.e. data or block) one) are removed on
        success. LVM volumes are permitted for Target only, both already
        attached or new logical one. In the latter case it is attached to OSD
        replacing one of the source devices. Following replacement rules apply
        (in the order of precedence, stop on the first match):
        * if source list has DB volume - target device replaces it.
        * if source list has WAL volume - target device replace it.
        * if source list has slow volume only - operation is not permitted,
          requires explicit allocation via new-db/new-wal command.

        Example calls for supported scenarios:

          Moves BlueFS data from main device to LV already attached as DB:

            ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data --target vgname/db

          Moves BlueFS data from shared main device to LV which will be attached
           as a new DB:

            ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data --target vgname/new_db

          Moves BlueFS data from DB device to new LV, DB is replaced:

            ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from db --target vgname/new_db

          Moves BlueFS data from main and DB devices to new LV, DB is replaced:

            ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data db --target vgname/new_db

          Moves BlueFS data from main, DB and WAL devices to new LV, WAL is
           removed and DB is replaced:

            ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from data db wal --target vgname/new_db

          Moves BlueFS data from main, DB and WAL devices to main device, WAL
           and DB are removed:

            ceph-volume lvm migrate --osd-id 1 --osd-fsid <uuid> --from db wal --target vgname/data

        """)

        parser = self.make_parser('ceph-volume lvm migrate', sub_command_help)

        if len(self.argv) == 0:
            print(sub_command_help)
            return

        self.args = parser.parse_args(self.argv)

        self.migrate_osd()

class NewVolume(object):
    def __init__(self, create_type, argv):
        self.create_type = create_type
        self.argv = argv

    def make_parser(self, prog, sub_command_help):
        parser = argparse.ArgumentParser(
            prog=prog,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            '--osd-id',
            required=True,
            help='Specify an OSD ID to attach new volume to',
            type=valid_osd_id,
        )

        parser.add_argument(
            '--osd-fsid',
            required=True,
            help='Specify an OSD FSIDto attach new volume to',
        )
        parser.add_argument(
            '--target',
            required=True,
            help='Specify target Logical Volume (LV) to attach',
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip checking OSD systemd unit',
        )
        return parser

    @decorators.needs_root
    def make_new_volume(self, osd_id, osd_fsid, devices, target_lv):
        osd_path = get_osd_path(osd_id, osd_fsid)
        mlogger.info(
            'Making new volume at {} for OSD: {} ({})'.format(
                target_lv.lv_path, osd_id, osd_path))
        target_path = target_lv.lv_path
        tag_tracker = VolumeTagTracker(devices, target_lv)
        # prepare and encrypt target if data volume is encrypted
        if tag_tracker.data_device.lv_api.encrypted:
            secret = encryption_utils.get_dmcrypt_key(osd_id, osd_fsid)
            mlogger.info(' preparing dmcrypt for {}, uuid {}'.format(target_lv.lv_path, target_lv.lv_uuid))
            target_path = encryption_utils.prepare_dmcrypt(
                key=secret, device=target_path, mapping=target_lv.lv_uuid)

        try:
            tag_tracker.update_tags_when_lv_create(self.create_type)

            stdout, stderr, exit_code = process.call([
                'ceph-bluestore-tool',
                '--path',
                osd_path,
                '--dev-target',
                target_path,
                '--command',
                'bluefs-bdev-new-{}'.format(self.create_type)
            ])
            if exit_code != 0:
                mlogger.error(
                    'failed to attach new volume, error code:{}'.format(
                        exit_code))
                raise SystemExit(
                    "Failed to attach new volume: {}".format(
                        self.args.target))
            else:
                system.chown(os.path.join(osd_path, "block.{}".format(
                    self.create_type)))
                terminal.success('New volume attached.')
        except:
            tag_tracker.undo()
            raise
        return

    @decorators.needs_root
    def new_volume(self):
        if self.args.osd_id and not self.args.no_systemd:
            osd_is_running = systemctl.osd_is_active(self.args.osd_id)
            if osd_is_running:
                mlogger.error('OSD ID is running, stop it with:'
                    ' systemctl stop ceph-osd@{}'.format(self.args.osd_id))
                raise SystemExit(
                    'Unable to attach new volume for OSD: {}'.format(
                        self.args.osd_id))

        target_lv = api.get_lv_by_fullname(self.args.target)
        if not target_lv:
            mlogger.error(
                'Target path {} is not a Logical Volume'.format(
                    self.args.target))
            raise SystemExit(
                'Unable to attach new volume : {}'.format(self.args.target))
        if target_lv.used_by_ceph:
            mlogger.error(
                'Target Logical Volume is already used by ceph: {}'.format(
                    self.args.target))
            raise SystemExit(
                'Unable to attach new volume : {}'.format(self.args.target))
        else:
            devices = find_associated_devices(self.args.osd_id,
                self.args.osd_fsid)
            self.make_new_volume(
                self.args.osd_id,
                self.args.osd_fsid,
                devices,
                target_lv)

class NewWAL(NewVolume):

    help = 'Allocate new WAL volume for OSD at specified Logical Volume'

    def __init__(self, argv):
        super(NewWAL, self).__init__("wal", argv)

    def main(self):
        sub_command_help = dedent("""
        Attaches the given logical volume to the given OSD as a WAL volume.
        Logical volume format is vg/lv. Fails if OSD has already got attached DB.

        Example:

          Attach vgname/lvname as a WAL volume to OSD 1

              ceph-volume lvm new-wal --osd-id 1 --osd-fsid 55BD4219-16A7-4037-BC20-0F158EFCC83D --target vgname/new_wal
        """)
        parser = self.make_parser('ceph-volume lvm new-wal', sub_command_help)

        if len(self.argv) == 0:
            print(sub_command_help)
            return

        self.args = parser.parse_args(self.argv)

        self.new_volume()

class NewDB(NewVolume):

    help = 'Allocate new DB volume for OSD at specified Logical Volume'

    def __init__(self, argv):
        super(NewDB, self).__init__("db", argv)

    def main(self):
        sub_command_help = dedent("""
        Attaches the given logical volume to the given OSD as a DB volume.
        Logical volume format is vg/lv. Fails if OSD has already got attached DB.

        Example:

          Attach vgname/lvname as a DB volume to OSD 1

              ceph-volume lvm new-db --osd-id 1 --osd-fsid 55BD4219-16A7-4037-BC20-0F158EFCC83D --target vgname/new_db
        """)

        parser = self.make_parser('ceph-volume lvm new-db', sub_command_help)
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        self.args = parser.parse_args(self.argv)

        self.new_volume()
