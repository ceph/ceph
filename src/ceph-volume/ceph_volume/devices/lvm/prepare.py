from __future__ import print_function
import json
import logging
from textwrap import dedent
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util import system, disk
from ceph_volume.util.arg_validators import exclude_group_options
from ceph_volume import conf, decorators, terminal
from ceph_volume.api import lvm as api
from .common import prepare_parser, rollback_osd


logger = logging.getLogger(__name__)


def prepare_dmcrypt(key, device, device_type, tags):
    """
    Helper for devices that are encrypted. The operations needed for
    block, db, wal, or data/journal devices are all the same
    """
    if not device:
        return ''
    tag_name = 'ceph.%s_uuid' % device_type
    uuid = tags[tag_name]
    # format data device
    encryption_utils.luks_format(
        key,
        device
    )
    encryption_utils.luks_open(
        key,
        device,
        uuid
    )

    return '/dev/mapper/%s' % uuid


def prepare_filestore(device, journal, secrets, tags, osd_id, fsid):
    """
    :param device: The name of the logical volume to work with
    :param journal: similar to device but can also be a regular/plain disk
    :param secrets: A dict with the secrets needed to create the osd (e.g. cephx)
    :param id_: The OSD id
    :param fsid: The OSD fsid, also known as the OSD UUID
    """
    cephx_secret = secrets.get('cephx_secret', prepare_utils.create_key())

    # encryption-only operations
    if secrets.get('dmcrypt_key'):
        # format and open ('decrypt' devices) and re-assign the device and journal
        # variables so that the rest of the process can use the mapper paths
        key = secrets['dmcrypt_key']
        device = prepare_dmcrypt(key, device, 'data', tags)
        journal = prepare_dmcrypt(key, journal, 'journal', tags)

    # vdo detection
    is_vdo = api.is_vdo(device)
    # create the directory
    prepare_utils.create_osd_path(osd_id)
    # format the device
    prepare_utils.format_device(device)
    # mount the data device
    prepare_utils.mount_osd(device, osd_id, is_vdo=is_vdo)
    # symlink the journal
    prepare_utils.link_journal(journal, osd_id)
    # get the latest monmap
    prepare_utils.get_monmap(osd_id)
    # prepare the osd filesystem
    prepare_utils.osd_mkfs_filestore(osd_id, fsid, cephx_secret)
    # write the OSD keyring if it doesn't exist already
    prepare_utils.write_keyring(osd_id, cephx_secret)
    if secrets.get('dmcrypt_key'):
        # if the device is going to get activated right away, this can be done
        # here, otherwise it will be recreated
        encryption_utils.write_lockbox_keyring(
            osd_id,
            fsid,
            tags['ceph.cephx_lockbox_secret']
        )


def prepare_bluestore(block, wal, db, secrets, tags, osd_id, fsid):
    """
    :param block: The name of the logical volume for the bluestore data
    :param wal: a regular/plain disk or logical volume, to be used for block.wal
    :param db: a regular/plain disk or logical volume, to be used for block.db
    :param secrets: A dict with the secrets needed to create the osd (e.g. cephx)
    :param id_: The OSD id
    :param fsid: The OSD fsid, also known as the OSD UUID
    """
    cephx_secret = secrets.get('cephx_secret', prepare_utils.create_key())
    # encryption-only operations
    if secrets.get('dmcrypt_key'):
        # If encrypted, there is no need to create the lockbox keyring file because
        # bluestore re-creates the files and does not have support for other files
        # like the custom lockbox one. This will need to be done on activation.
        # format and open ('decrypt' devices) and re-assign the device and journal
        # variables so that the rest of the process can use the mapper paths
        key = secrets['dmcrypt_key']
        block = prepare_dmcrypt(key, block, 'block', tags)
        wal = prepare_dmcrypt(key, wal, 'wal', tags)
        db = prepare_dmcrypt(key, db, 'db', tags)

    # create the directory
    prepare_utils.create_osd_path(osd_id, tmpfs=True)
    # symlink the block
    prepare_utils.link_block(block, osd_id)
    # get the latest monmap
    prepare_utils.get_monmap(osd_id)
    # write the OSD keyring if it doesn't exist already
    prepare_utils.write_keyring(osd_id, cephx_secret)
    # prepare the osd filesystem
    prepare_utils.osd_mkfs_bluestore(
        osd_id, fsid,
        keyring=cephx_secret,
        wal=wal,
        db=db
    )


class Prepare(object):

    help = 'Format an LVM device and associate it with an OSD'

    def __init__(self, argv):
        self.argv = argv
        self.osd_id = None

    def get_ptuuid(self, argument):
        uuid = disk.get_partuuid(argument)
        if not uuid:
            terminal.error('blkid could not detect a PARTUUID for device: %s' % argument)
            raise RuntimeError('unable to use device')
        return uuid

    def setup_device(self, device_type, device_name, tags, size):
        """
        Check if ``device`` is an lv, if so, set the tags, making sure to
        update the tags with the lv_uuid and lv_path which the incoming tags
        will not have.

        If the device is not a logical volume, then retrieve the partition UUID
        by querying ``blkid``
        """
        if device_name is None:
            return '', '', tags
        tags['ceph.type'] = device_type
        tags['ceph.vdo'] = api.is_vdo(device_name)

        try:
            vg_name, lv_name = device_name.split('/')
            lv = api.get_first_lv(filters={'lv_name': lv_name,
                                           'vg_name': vg_name})
        except ValueError:
            lv = None

        if lv:
            uuid = lv.lv_uuid
            path = lv.lv_path
            tags['ceph.%s_uuid' % device_type] = uuid
            tags['ceph.%s_device' % device_type] = path
            lv.set_tags(tags)
        elif disk.is_device(device_name):
            # We got a disk, create an lv
            lv_type = "osd-{}".format(device_type)
            uuid = system.generate_uuid()
            tags['ceph.{}_uuid'.format(device_type)] = uuid
            kwargs = {
                'device': device_name,
                'tags': tags,
                'slots': getattr(self.args,
                                 'block_{}_slots'.format(device_type),
                                 1),
            }
            if size != 0:
                kwargs['size'] = disk.Size.parse(size)
            lv = api.create_lv(
                lv_type,
                uuid,
                **kwargs)
            path = lv.lv_path
            tags['ceph.{}_device'.format(device_type)] = path
            lv.set_tags(tags)
        else:
            # otherwise assume this is a regular disk partition
            uuid = self.get_ptuuid(device_name)
            path = device_name
            tags['ceph.%s_uuid' % device_type] = uuid
            tags['ceph.%s_device' % device_type] = path
        return path, uuid, tags

    def prepare_data_device(self, device_type, osd_uuid):
        """
        Check if ``arg`` is a device or partition to create an LV out of it
        with a distinct volume group name, assigning LV tags on it and
        ultimately, returning the logical volume object.  Failing to detect
        a device or partition will result in error.

        :param arg: The value of ``--data`` when parsing args
        :param device_type: Usually, either ``data`` or ``block`` (filestore vs. bluestore)
        :param osd_uuid: The OSD uuid
        """
        device = self.args.data
        if disk.is_partition(device) or disk.is_device(device):
            # we must create a vg, and then a single lv
            lv_name_prefix = "osd-{}".format(device_type)
            kwargs = {'device': device,
                      'tags': {'ceph.type': device_type},
                      'slots': self.args.data_slots,
                     }
            logger.debug('data device size: {}'.format(self.args.data_size))
            if self.args.data_size != 0:
                kwargs['size'] = disk.Size.parse(self.args.data_size)
            return api.create_lv(
                lv_name_prefix,
                osd_uuid,
                **kwargs)
        else:
            error = [
                'Cannot use device ({}).'.format(device),
                'A vg/lv path or an existing device is needed']
            raise RuntimeError(' '.join(error))

        raise RuntimeError('no data logical volume found with: {}'.format(device))

    def safe_prepare(self, args=None):
        """
        An intermediate step between `main()` and `prepare()` so that we can
        capture the `self.osd_id` in case we need to rollback

        :param args: Injected args, usually from `lvm create` which compounds
                     both `prepare` and `create`
        """
        if args is not None:
            self.args = args

        try:
            vgname, lvname = self.args.data.split('/')
            lv = api.get_first_lv(filters={'lv_name': lvname,
                                           'vg_name': vgname})
        except ValueError:
            lv = None

        if api.is_ceph_device(lv):
            logger.info("device {} is already used".format(self.args.data))
            raise RuntimeError("skipping {}, it is already prepared".format(self.args.data))
        try:
            self.prepare()
        except Exception:
            logger.exception('lvm prepare was unable to complete')
            logger.info('will rollback OSD ID creation')
            rollback_osd(self.args, self.osd_id)
            raise
        terminal.success("ceph-volume lvm prepare successful for: %s" % self.args.data)

    def get_cluster_fsid(self):
        """
        Allows using --cluster-fsid as an argument, but can fallback to reading
        from ceph.conf if that is unset (the default behavior).
        """
        if self.args.cluster_fsid:
            return self.args.cluster_fsid
        else:
            return conf.ceph.get('global', 'fsid')

    @decorators.needs_root
    def prepare(self):
        # FIXME we don't allow re-using a keyring, we always generate one for the
        # OSD, this needs to be fixed. This could either be a file (!) or a string
        # (!!) or some flags that we would need to compound into a dict so that we
        # can convert to JSON (!!!)
        secrets = {'cephx_secret': prepare_utils.create_key()}
        cephx_lockbox_secret = ''
        encrypted = 1 if self.args.dmcrypt else 0
        cephx_lockbox_secret = '' if not encrypted else prepare_utils.create_key()

        if encrypted:
            secrets['dmcrypt_key'] = encryption_utils.create_dmcrypt_key()
            secrets['cephx_lockbox_secret'] = cephx_lockbox_secret

        cluster_fsid = self.get_cluster_fsid()

        osd_fsid = self.args.osd_fsid or system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if crush_device_class:
            secrets['crush_device_class'] = crush_device_class
        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(osd_fsid, json.dumps(secrets), osd_id=self.args.osd_id)
        tags = {
            'ceph.osd_fsid': osd_fsid,
            'ceph.osd_id': self.osd_id,
            'ceph.cluster_fsid': cluster_fsid,
            'ceph.cluster_name': conf.cluster,
            'ceph.crush_device_class': crush_device_class,
            'ceph.osdspec_affinity': prepare_utils.get_osdspec_affinity()
        }
        if self.args.filestore:
            #TODO: allow auto creation of journal on passed device, only works
            # when physical device is passed, not LV
            if not self.args.journal:
                raise RuntimeError('--journal is required when using --filestore')

            try:
                vg_name, lv_name = self.args.data.split('/')
                data_lv = api.get_first_lv(filters={'lv_name': lv_name,
                                                    'vg_name': vg_name})
            except ValueError:
                data_lv = None

            if not data_lv:
                data_lv = self.prepare_data_device('data', osd_fsid)

            tags['ceph.data_device'] = data_lv.lv_path
            tags['ceph.data_uuid'] = data_lv.lv_uuid
            tags['ceph.cephx_lockbox_secret'] = cephx_lockbox_secret
            tags['ceph.encrypted'] = encrypted
            tags['ceph.vdo'] = api.is_vdo(data_lv.lv_path)

            journal_device, journal_uuid, tags = self.setup_device(
                'journal', self.args.journal, tags, self.args.journal_size)

            tags['ceph.type'] = 'data'
            data_lv.set_tags(tags)

            prepare_filestore(
                data_lv.lv_path,
                journal_device,
                secrets,
                tags,
                self.osd_id,
                osd_fsid,
            )
        elif self.args.bluestore:
            try:
                vg_name, lv_name = self.args.data.split('/')
                block_lv = api.get_first_lv(filters={'lv_name': lv_name,
                                                 'vg_name': vg_name})
            except ValueError:
                block_lv = None

            if not block_lv:
                block_lv = self.prepare_data_device('block', osd_fsid)

            tags['ceph.block_device'] = block_lv.lv_path
            tags['ceph.block_uuid'] = block_lv.lv_uuid
            tags['ceph.cephx_lockbox_secret'] = cephx_lockbox_secret
            tags['ceph.encrypted'] = encrypted
            tags['ceph.vdo'] = api.is_vdo(block_lv.lv_path)

            wal_device, wal_uuid, tags = self.setup_device(
                'wal', self.args.block_wal, tags, self.args.block_wal_size)
            db_device, db_uuid, tags = self.setup_device(
                'db', self.args.block_db, tags, self.args.block_db_size)

            tags['ceph.type'] = 'block'
            block_lv.set_tags(tags)

            prepare_bluestore(
                block_lv.lv_path,
                wal_device,
                db_device,
                secrets,
                tags,
                self.osd_id,
                osd_fsid,
            )

    def main(self):
        sub_command_help = dedent("""
        Prepare an OSD by assigning an ID and FSID, registering them with the
        cluster with an ID and FSID, formatting and mounting the volume, and
        finally by adding all the metadata to the logical volumes using LVM
        tags, so that it can later be discovered.

        Once the OSD is ready, an ad-hoc systemd unit will be enabled so that
        it can later get activated and the OSD daemon can get started.

        Encryption is supported via dmcrypt and the --dmcrypt flag.

        Existing logical volume (lv):

            ceph-volume lvm prepare --data {vg/lv}

        Existing block device (a logical volume will be created):

            ceph-volume lvm prepare --data /path/to/device

        Optionally, can consume db and wal devices, partitions or logical
        volumes. A device will get a logical volume, partitions and existing
        logical volumes will be used as is:

            ceph-volume lvm prepare --data {vg/lv} --block.wal {partition} --block.db {/path/to/device}
        """)
        parser = prepare_parser(
            prog='ceph-volume lvm prepare',
            description=sub_command_help,
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        exclude_group_options(parser, argv=self.argv, groups=['filestore', 'bluestore'])
        self.args = parser.parse_args(self.argv)
        # the unfortunate mix of one superset for both filestore and bluestore
        # makes this validation cumbersome
        if self.args.filestore:
            if not self.args.journal:
                raise SystemExit('--journal is required when using --filestore')
        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if not self.args.bluestore and not self.args.filestore:
            self.args.bluestore = True
        self.safe_prepare()
