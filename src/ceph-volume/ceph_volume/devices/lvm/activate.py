from __future__ import print_function
import argparse
import logging
import os
from textwrap import dedent
from ceph_volume import process, conf, decorators, terminal, __release__, configuration
from ceph_volume.util import system, disk
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.systemd import systemctl
from ceph_volume.api import lvm as api
from .listing import direct_report


logger = logging.getLogger(__name__)


def activate_filestore(osd_lvs, no_systemd=False):
    # find the osd
    for osd_lv in osd_lvs:
        if osd_lv.tags.get('ceph.type') == 'data':
            data_lv = osd_lv
            break
    else:
        raise RuntimeError('Unable to find a data LV for filestore activation')

    is_encrypted = data_lv.tags.get('ceph.encrypted', '0') == '1'
    is_vdo = data_lv.tags.get('ceph.vdo', '0')

    osd_id = data_lv.tags['ceph.osd_id']
    configuration.load_ceph_conf_path(data_lv.tags['ceph.cluster_name'])
    configuration.load()
    # it may have a volume with a journal
    for osd_lv in osd_lvs:
        if osd_lv.tags.get('ceph.type') == 'journal':
            osd_journal_lv = osd_lv
            break
    else:
        osd_journal_lv = None

    # TODO: add sensible error reporting if this is ever the case
    # blow up with a KeyError if this doesn't exist
    osd_fsid = data_lv.tags['ceph.osd_fsid']
    if not osd_journal_lv:
        # must be a disk partition, by querying blkid by the uuid we are ensuring that the
        # device path is always correct
        journal_uuid = data_lv.tags['ceph.journal_uuid']
        osd_journal = disk.get_device_from_partuuid(journal_uuid)
    else:
        journal_uuid = osd_journal_lv.lv_uuid
        osd_journal = data_lv.tags['ceph.journal_device']

    if not osd_journal:
        raise RuntimeError('unable to detect an lv or device journal for OSD %s' % osd_id)

    # this is done here, so that previous checks that ensure path availability
    # and correctness can still be enforced, and report if any issues are found
    if is_encrypted:
        lockbox_secret = data_lv.tags['ceph.cephx_lockbox_secret']
        # this keyring writing is idempotent
        encryption_utils.write_lockbox_keyring(osd_id, osd_fsid, lockbox_secret)
        dmcrypt_secret = encryption_utils.get_dmcrypt_key(osd_id, osd_fsid)
        encryption_utils.luks_open(dmcrypt_secret, data_lv.lv_path, data_lv.lv_uuid)
        encryption_utils.luks_open(dmcrypt_secret, osd_journal, journal_uuid)

        osd_journal = '/dev/mapper/%s' % journal_uuid
        source = '/dev/mapper/%s' % data_lv.lv_uuid
    else:
        source = data_lv.lv_path

    # mount the osd
    destination = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    if not system.device_is_mounted(source, destination=destination):
        prepare_utils.mount_osd(source, osd_id, is_vdo=is_vdo)

    # ensure that the OSD destination is always chowned properly
    system.chown(destination)

    # always re-do the symlink regardless if it exists, so that the journal
    # device path that may have changed can be mapped correctly every time
    destination = '/var/lib/ceph/osd/%s-%s/journal' % (conf.cluster, osd_id)
    process.run(['ln', '-snf', osd_journal, destination])

    # make sure that the journal has proper permissions
    system.chown(osd_journal)

    if no_systemd is False:
        # enable the ceph-volume unit for this OSD
        systemctl.enable_volume(osd_id, osd_fsid, 'lvm')

        # enable the OSD
        systemctl.enable_osd(osd_id)

        # start the OSD
        systemctl.start_osd(osd_id)
    terminal.success("ceph-volume lvm activate successful for osd ID: %s" % osd_id)


def get_osd_device_path(osd_lvs, device_type, dmcrypt_secret=None):
    """
    ``device_type`` can be one of ``db``, ``wal`` or ``block`` so that
    we can query ``lvs`` (a ``Volumes`` object) and fallback to querying the uuid
    if that is not present.

    Return a path if possible, failing to do that a ``None``, since some of these devices
    are optional
    """
    osd_block_lv = None
    for lv in osd_lvs:
        if lv.tags.get('ceph.type') == 'block':
            osd_block_lv = lv
            break
    if osd_block_lv:
        is_encrypted = osd_block_lv.tags.get('ceph.encrypted', '0') == '1'
        logger.debug('Found block device (%s) with encryption: %s', osd_block_lv.name, is_encrypted)
        uuid_tag = 'ceph.%s_uuid' % device_type
        device_uuid = osd_block_lv.tags.get(uuid_tag)
        if not device_uuid:
            return None

    device_lv = None
    for lv in osd_lvs:
        if lv.tags.get('ceph.type') == device_type:
            device_lv = lv
            break
    if device_lv:
        if is_encrypted:
            encryption_utils.luks_open(dmcrypt_secret, device_lv.lv_path, device_uuid)
            return '/dev/mapper/%s' % device_uuid
        return device_lv.lv_path

    # this could be a regular device, so query it with blkid
    physical_device = disk.get_device_from_partuuid(device_uuid)
    if physical_device:
        if is_encrypted:
            encryption_utils.luks_open(dmcrypt_secret, physical_device, device_uuid)
            return '/dev/mapper/%s' % device_uuid
        return physical_device

    raise RuntimeError('could not find %s with uuid %s' % (device_type, device_uuid))


def activate_bluestore(osd_lvs, no_systemd=False):
    for lv in osd_lvs:
        if lv.tags.get('ceph.type') == 'block':
            osd_block_lv = lv
            break
    else:
        raise RuntimeError('could not find a bluestore OSD to activate')

    is_encrypted = osd_block_lv.tags.get('ceph.encrypted', '0') == '1'
    dmcrypt_secret = None
    osd_id = osd_block_lv.tags['ceph.osd_id']
    conf.cluster = osd_block_lv.tags['ceph.cluster_name']
    osd_fsid = osd_block_lv.tags['ceph.osd_fsid']

    # mount on tmpfs the osd directory
    osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    if not system.path_is_mounted(osd_path):
        # mkdir -p and mount as tmpfs
        prepare_utils.create_osd_path(osd_id, tmpfs=True)
    # XXX This needs to be removed once ceph-bluestore-tool can deal with
    # symlinks that exist in the osd dir
    for link_name in ['block', 'block.db', 'block.wal']:
        link_path = os.path.join(osd_path, link_name)
        if os.path.exists(link_path):
            os.unlink(os.path.join(osd_path, link_name))
    # encryption is handled here, before priming the OSD dir
    if is_encrypted:
        osd_lv_path = '/dev/mapper/%s' % osd_block_lv.lv_uuid
        lockbox_secret = osd_block_lv.tags['ceph.cephx_lockbox_secret']
        encryption_utils.write_lockbox_keyring(osd_id, osd_fsid, lockbox_secret)
        dmcrypt_secret = encryption_utils.get_dmcrypt_key(osd_id, osd_fsid)
        encryption_utils.luks_open(dmcrypt_secret, osd_block_lv.lv_path, osd_block_lv.lv_uuid)
    else:
        osd_lv_path = osd_block_lv.lv_path

    db_device_path = get_osd_device_path(osd_lvs, 'db', dmcrypt_secret=dmcrypt_secret)
    wal_device_path = get_osd_device_path(osd_lvs, 'wal', dmcrypt_secret=dmcrypt_secret)

    # Once symlinks are removed, the osd dir can be 'primed again. chown first,
    # regardless of what currently exists so that ``prime-osd-dir`` can succeed
    # even if permissions are somehow messed up
    system.chown(osd_path)
    prime_command = [
        'ceph-bluestore-tool', '--cluster=%s' % conf.cluster,
        'prime-osd-dir', '--dev', osd_lv_path,
        '--path', osd_path]

    if __release__ != "luminous":
        # mon-config changes are not available in Luminous
        prime_command.append('--no-mon-config')

    process.run(prime_command)
    # always re-do the symlink regardless if it exists, so that the block,
    # block.wal, and block.db devices that may have changed can be mapped
    # correctly every time
    process.run(['ln', '-snf', osd_lv_path, os.path.join(osd_path, 'block')])
    system.chown(os.path.join(osd_path, 'block'))
    system.chown(osd_path)
    if db_device_path:
        destination = os.path.join(osd_path, 'block.db')
        process.run(['ln', '-snf', db_device_path, destination])
        system.chown(db_device_path)
        system.chown(destination)
    if wal_device_path:
        destination = os.path.join(osd_path, 'block.wal')
        process.run(['ln', '-snf', wal_device_path, destination])
        system.chown(wal_device_path)
        system.chown(destination)

    if no_systemd is False:
        # enable the ceph-volume unit for this OSD
        systemctl.enable_volume(osd_id, osd_fsid, 'lvm')

        # enable the OSD
        systemctl.enable_osd(osd_id)

        # start the OSD
        systemctl.start_osd(osd_id)
    terminal.success("ceph-volume lvm activate successful for osd ID: %s" % osd_id)


class Activate(object):

    help = 'Discover and mount the LVM device associated with an OSD ID and start the Ceph OSD'

    def __init__(self, argv):
        self.argv = argv

    @decorators.needs_root
    def activate_all(self, args):
        listed_osds = direct_report()
        osds = {}
        for osd_id, devices in listed_osds.items():
            # the metadata for all devices in each OSD will contain
            # the FSID which is required for activation
            for device in devices:
                fsid = device.get('tags', {}).get('ceph.osd_fsid')
                if fsid:
                    osds[fsid] = osd_id
                    break
        if not osds:
            terminal.warning('Was unable to find any OSDs to activate')
            terminal.warning('Verify OSDs are present with "ceph-volume lvm list"')
            return
        for osd_fsid, osd_id in osds.items():
            if systemctl.osd_is_active(osd_id):
                terminal.warning(
                    'OSD ID %s FSID %s process is active. Skipping activation' % (osd_id, osd_fsid)
                )
            else:
                terminal.info('Activating OSD ID %s FSID %s' % (osd_id, osd_fsid))
                self.activate(args, osd_id=osd_id, osd_fsid=osd_fsid)

    @decorators.needs_root
    def activate(self, args, osd_id=None, osd_fsid=None):
        """
        :param args: The parsed arguments coming from the CLI
        :param osd_id: When activating all, this gets populated with an
                       existing OSD ID
        :param osd_fsid: When activating all, this gets populated with an
                         existing OSD FSID
        """
        osd_id = osd_id if osd_id else args.osd_id
        osd_fsid = osd_fsid if osd_fsid else args.osd_fsid

        if osd_id and osd_fsid:
            tags = {'ceph.osd_id': osd_id, 'ceph.osd_fsid': osd_fsid}
        elif not osd_id and osd_fsid:
            tags = {'ceph.osd_fsid': osd_fsid}
        lvs = api.get_lvs(tags=tags)
        if not lvs:
            raise RuntimeError('could not find osd.%s with osd_fsid %s' %
                               (osd_id, osd_fsid))

        # This argument is only available when passed in directly or via
        # systemd, not when ``create`` is being used
        if getattr(args, 'auto_detect_objectstore', False):
            logger.info('auto detecting objectstore')
            # may get multiple lvs, so can't do get_the_lvs() calls here
            for lv in lvs:
                has_journal = lv.tags.get('ceph.journal_uuid')
                if has_journal:
                    logger.info('found a journal associated with the OSD, '
                                'assuming filestore')
                    return activate_filestore(lvs, args.no_systemd)

            logger.info('unable to find a journal associated with the OSD, '
                        'assuming bluestore')

            return activate_bluestore(lvs, args.no_systemd)
        if args.bluestore:
            activate_bluestore(lvs, args.no_systemd)
        elif args.filestore:
            activate_filestore(lvs, args.no_systemd)

    def main(self):
        sub_command_help = dedent("""
        Activate OSDs by discovering them with LVM and mounting them in their
        appropriate destination:

            ceph-volume lvm activate {ID} {FSID}

        The lvs associated with the OSD need to have been prepared previously,
        so that all needed tags and metadata exist.

        When migrating OSDs, or a multiple-osd activation is needed, the
        ``--all`` flag can be used instead of the individual ID and FSID:

            ceph-volume lvm activate --all

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm activate',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'osd_id',
            metavar='ID',
            nargs='?',
            help='The ID of the OSD, usually an integer, like 0'
        )
        parser.add_argument(
            'osd_fsid',
            metavar='FSID',
            nargs='?',
            help='The FSID of the OSD, similar to a SHA1'
        )
        parser.add_argument(
            '--auto-detect-objectstore',
            action='store_true',
            help='Autodetect the objectstore by inspecting the OSD',
        )
        parser.add_argument(
            '--bluestore',
            action='store_true',
            help='bluestore objectstore (default)',
        )
        parser.add_argument(
            '--filestore',
            action='store_true',
            help='filestore objectstore',
        )
        parser.add_argument(
            '--all',
            dest='activate_all',
            action='store_true',
            help='Activate all OSDs found in the system',
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services',
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if not args.bluestore and not args.filestore:
            args.bluestore = True
        if args.activate_all:
            self.activate_all(args)
        else:
            self.activate(args)
