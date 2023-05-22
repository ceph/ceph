import json
import logging
import os
from ceph_volume import conf, terminal, decorators, configuration, process
from ceph_volume.api import lvm as api
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util import system, disk
from ceph_volume.systemd import systemctl
from ceph_volume.devices.lvm.common import rollback_osd
from ceph_volume.devices.lvm.listing import direct_report
from .bluestore import BlueStore
from typing import Dict, Any, Optional, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import argparse
    from ceph_volume.api.lvm import Volume

logger = logging.getLogger(__name__)


class LvmBlueStore(BlueStore):
    def __init__(self, args: "argparse.Namespace") -> None:
        super().__init__(args)
        self.tags: Dict[str, Any] = {}
        self.block_lv: Optional["Volume"] = None

    def pre_prepare(self) -> None:
        if self.encrypted:
            self.secrets['dmcrypt_key'] = encryption_utils.create_dmcrypt_key()

        cluster_fsid = self.get_cluster_fsid()

        self.osd_fsid = self.args.osd_fsid or system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if crush_device_class:
            self.secrets['crush_device_class'] = crush_device_class
        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(self.osd_fsid,
                                              json.dumps(self.secrets),
                                              osd_id=self.args.osd_id)
        self.tags = {
            'ceph.osd_fsid': self.osd_fsid,
            'ceph.osd_id': self.osd_id,
            'ceph.cluster_fsid': cluster_fsid,
            'ceph.cluster_name': conf.cluster,
            'ceph.crush_device_class': crush_device_class,
            'ceph.osdspec_affinity': self.get_osdspec_affinity()
        }

        try:
            vg_name, lv_name = self.args.data.split('/')
            self.block_lv = api.get_single_lv(filters={'lv_name': lv_name,
                                                       'vg_name': vg_name})
        except ValueError:
            self.block_lv = None

        if not self.block_lv:
            self.block_lv = self.prepare_data_device('block', self.osd_fsid)
        self.block_device_path = self.block_lv.__dict__['lv_path']

        self.tags['ceph.block_device'] = self.block_lv.__dict__['lv_path']
        self.tags['ceph.block_uuid'] = self.block_lv.__dict__['lv_uuid']
        self.tags['ceph.cephx_lockbox_secret'] = self.cephx_lockbox_secret
        self.tags['ceph.encrypted'] = self.encrypted
        self.tags['ceph.vdo'] = api.is_vdo(self.block_lv.__dict__['lv_path'])

    def prepare_data_device(self,
                            device_type: str,
                            osd_uuid: str) -> Optional["Volume"]:
        """
        Check if ``arg`` is a device or partition to create an LV out of it
        with a distinct volume group name, assigning LV tags on it and
        ultimately, returning the logical volume object.  Failing to detect
        a device or partition will result in error.

        :param arg: The value of ``--data`` when parsing args
        :param device_type: Usually ``block``
        :param osd_uuid: The OSD uuid
        """

        device = self.args.data
        if disk.is_partition(device) or disk.is_device(device):
            # we must create a vg, and then a single lv
            lv_name_prefix = "osd-{}".format(device_type)
            kwargs = {
                'device': device,
                'tags': {'ceph.type': device_type},
                'slots': self.args.data_slots,
                }
            logger.debug('data device size: {}'.format(self.args.data_size))
            if self.args.data_size != 0:
                kwargs['size'] = self.args.data_size
            return api.create_lv(
                lv_name_prefix,
                osd_uuid,
                **kwargs)
        else:
            error = [
                'Cannot use device ({}).'.format(device),
                'A vg/lv path or an existing device is needed']
            raise RuntimeError(' '.join(error))

    def safe_prepare(self,
                     args: Optional["argparse.Namespace"] = None) -> None:
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
            lv = api.get_single_lv(filters={'lv_name': lvname,
                                            'vg_name': vgname})
        except ValueError:
            lv = None

        if api.is_ceph_device(lv):
            logger.info("device {} is already used".format(self.args.data))
            raise RuntimeError("skipping {}, it is already prepared".format(
                self.args.data))
        try:
            self.prepare()
        except Exception:
            logger.exception('lvm prepare was unable to complete')
            logger.info('will rollback OSD ID creation')
            rollback_osd(self.args, self.osd_id)
            raise
        terminal.success("ceph-volume lvm prepare successful for: %s" %
                         self.args.data)

    @decorators.needs_root
    def prepare(self) -> None:
        # 1/
        # Need to be reworked (move it to the parent class + call super()? )
        self.pre_prepare()

        # 2/
        self.wal_device_path, wal_uuid, tags = self.setup_device(
            'wal',
            self.args.block_wal,
            self.tags,
            self.args.block_wal_size,
            self.args.block_wal_slots)
        self.db_device_path, db_uuid, tags = self.setup_device(
            'db',
            self.args.block_db,
            self.tags,
            self.args.block_db_size,
            self.args.block_db_slots)

        self.tags['ceph.type'] = 'block'
        self.block_lv.set_tags(self.tags)  # type: ignore

        # 3/ encryption-only operations
        if self.secrets.get('dmcrypt_key'):
            self.prepare_dmcrypt()

        # 4/ osd_prepare req
        self.prepare_osd_req()

        # 5/ bluestore mkfs
        # prepare the osd filesystem
        self.osd_mkfs()

    def prepare_dmcrypt(self) -> None:
        # If encrypted, there is no need to create the lockbox keyring file
        # because bluestore re-creates the files and does not have support
        # for other files like the custom lockbox one. This will need to be
        # done on activation. Format and open ('decrypt' devices) and
        # re-assign the device and journal variables so that the rest of the
        # process can use the mapper paths
        key = self.secrets['dmcrypt_key']

        self.block_device_path = \
            self.luks_format_and_open(key,
                                      self.block_device_path,
                                      'block',
                                      self.tags)
        self.wal_device_path = self.luks_format_and_open(key,
                                                         self.wal_device_path,
                                                         'wal',
                                                         self.tags)
        self.db_device_path = self.luks_format_and_open(key,
                                                        self.db_device_path,
                                                        'db',
                                                        self.tags)

    def luks_format_and_open(self,
                             key: Optional[str],
                             device: str,
                             device_type: str,
                             tags: Dict[str, Any]) -> str:
        """
        Helper for devices that are encrypted. The operations needed for
        block, db, wal devices are all the same
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

    def setup_device(self,
                     device_type: str,
                     device_name: str,
                     tags: Dict[str, Any],
                     size: int,
                     slots: int) -> Tuple[str, str, Dict[str, Any]]:
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
            lv = api.get_single_lv(filters={'lv_name': lv_name,
                                            'vg_name': vg_name})
        except ValueError:
            lv = None

        if lv:
            lv_uuid = lv.lv_uuid
            path = lv.lv_path
            tags['ceph.%s_uuid' % device_type] = lv_uuid
            tags['ceph.%s_device' % device_type] = path
            lv.set_tags(tags)
        elif disk.is_device(device_name):
            # We got a disk, create an lv
            lv_type = "osd-{}".format(device_type)
            name_uuid = system.generate_uuid()
            kwargs = {
                'device': device_name,
                'tags': tags,
                'slots': slots
            }
            # TODO use get_block_db_size and co here to get configured size in
            # conf file
            if size != 0:
                kwargs['size'] = size
            lv = api.create_lv(
                lv_type,
                name_uuid,
                **kwargs)
            path = lv.lv_path
            tags['ceph.{}_device'.format(device_type)] = path
            tags['ceph.{}_uuid'.format(device_type)] = lv.lv_uuid
            lv_uuid = lv.lv_uuid
            lv.set_tags(tags)
        else:
            # otherwise assume this is a regular disk partition
            name_uuid = self.get_ptuuid(device_name)
            path = device_name
            tags['ceph.%s_uuid' % device_type] = name_uuid
            tags['ceph.%s_device' % device_type] = path
            lv_uuid = name_uuid
        return path, lv_uuid, tags

    def get_osd_device_path(self,
                            osd_lvs: List["Volume"],
                            device_type: str,
                            dmcrypt_secret: Optional[str] =
                            None) -> Optional[str]:
        """
        ``device_type`` can be one of ``db``, ``wal`` or ``block`` so that we
        can query LVs on system and fallback to querying the uuid if that is
        not present.

        Return a path if possible, failing to do that a ``None``, since some of
        these devices are optional.
        """
        osd_block_lv = None
        for lv in osd_lvs:
            if lv.tags.get('ceph.type') == 'block':
                osd_block_lv = lv
                break
        if osd_block_lv:
            is_encrypted = osd_block_lv.tags.get('ceph.encrypted', '0') == '1'
            logger.debug('Found block device (%s) with encryption: %s',
                         osd_block_lv.name, is_encrypted)
            uuid_tag = 'ceph.%s_uuid' % device_type
            device_uuid = osd_block_lv.tags.get(uuid_tag)
            if not device_uuid:
                return None

        device_lv: Optional["Volume"] = None
        for lv in osd_lvs:
            if lv.tags.get('ceph.type') == device_type:
                device_lv = lv
                break
        if device_lv:
            if is_encrypted:
                encryption_utils.luks_open(dmcrypt_secret,
                                           device_lv.__dict__['lv_path'],
                                           device_uuid)
                return '/dev/mapper/%s' % device_uuid
            return device_lv.__dict__['lv_path']

        # this could be a regular device, so query it with blkid
        physical_device = disk.get_device_from_partuuid(device_uuid)
        if physical_device:
            if is_encrypted:
                encryption_utils.luks_open(dmcrypt_secret,
                                           physical_device,
                                           device_uuid)
                return '/dev/mapper/%s' % device_uuid
            return physical_device

        raise RuntimeError('could not find %s with uuid %s' % (device_type,
                                                               device_uuid))

    def _activate(self,
                  osd_lvs: List["Volume"],
                  no_systemd: bool = False,
                  no_tmpfs: bool = False) -> None:
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
        configuration.load_ceph_conf_path(
            osd_block_lv.tags['ceph.cluster_name'])
        configuration.load()

        # mount on tmpfs the osd directory
        self.osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
        if not system.path_is_mounted(self.osd_path):
            # mkdir -p and mount as tmpfs
            prepare_utils.create_osd_path(osd_id, tmpfs=not no_tmpfs)

        # XXX This needs to be removed once ceph-bluestore-tool can deal with
        # symlinks that exist in the osd dir
        self.unlink_bs_symlinks()

        # encryption is handled here, before priming the OSD dir
        if is_encrypted:
            osd_lv_path = '/dev/mapper/%s' % osd_block_lv.__dict__['lv_uuid']
            lockbox_secret = osd_block_lv.tags['ceph.cephx_lockbox_secret']
            encryption_utils.write_lockbox_keyring(osd_id,
                                                   osd_fsid,
                                                   lockbox_secret)
            dmcrypt_secret = encryption_utils.get_dmcrypt_key(osd_id, osd_fsid)
            encryption_utils.luks_open(dmcrypt_secret,
                                       osd_block_lv.__dict__['lv_path'],
                                       osd_block_lv.__dict__['lv_uuid'])
        else:
            osd_lv_path = osd_block_lv.__dict__['lv_path']

        db_device_path = \
            self.get_osd_device_path(osd_lvs, 'db',
                                     dmcrypt_secret=dmcrypt_secret)
        wal_device_path = \
            self.get_osd_device_path(osd_lvs,
                                     'wal',
                                     dmcrypt_secret=dmcrypt_secret)

        # Once symlinks are removed, the osd dir can be 'primed again.
        # chown first, regardless of what currently exists so that
        # ``prime-osd-dir`` can succeed even if permissions are
        # somehow messed up.
        system.chown(self.osd_path)
        prime_command = [
            'ceph-bluestore-tool', '--cluster=%s' % conf.cluster,
            'prime-osd-dir', '--dev', osd_lv_path,
            '--path', self.osd_path, '--no-mon-config']

        process.run(prime_command)
        # always re-do the symlink regardless if it exists, so that the block,
        # block.wal, and block.db devices that may have changed can be mapped
        # correctly every time
        process.run(['ln',
                     '-snf',
                     osd_lv_path,
                     os.path.join(self.osd_path, 'block')])
        system.chown(os.path.join(self.osd_path, 'block'))
        system.chown(self.osd_path)
        if db_device_path:
            destination = os.path.join(self.osd_path, 'block.db')
            process.run(['ln', '-snf', db_device_path, destination])
            system.chown(db_device_path)
            system.chown(destination)
        if wal_device_path:
            destination = os.path.join(self.osd_path, 'block.wal')
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
        terminal.success("ceph-volume lvm activate successful for osd ID: %s" %
                         osd_id)

    @decorators.needs_root
    def activate_all(self) -> None:
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
            terminal.warning('Verify OSDs are present with '
                             '"ceph-volume lvm list"')
            return
        for osd_fsid, osd_id in osds.items():
            if not self.args.no_systemd and systemctl.osd_is_active(osd_id):
                terminal.warning(
                    'OSD ID %s FSID %s process is active. '
                    'Skipping activation' % (osd_id, osd_fsid)
                )
            else:
                terminal.info('Activating OSD ID %s FSID %s' % (osd_id,
                                                                osd_fsid))
                self.activate(self.args, osd_id=osd_id, osd_fsid=osd_fsid)

    @decorators.needs_root
    def activate(self,
                 args: Optional["argparse.Namespace"] = None,
                 osd_id: Optional[str] = None,
                 osd_fsid: Optional[str] = None) -> None:
        """
        :param args: The parsed arguments coming from the CLI
        :param osd_id: When activating all, this gets populated with an
                       existing OSD ID
        :param osd_fsid: When activating all, this gets populated with an
                         existing OSD FSID
        """
        osd_id = osd_id if osd_id else self.args.osd_id
        osd_fsid = osd_fsid if osd_fsid else self.args.osd_fsid

        if osd_id and osd_fsid:
            tags = {'ceph.osd_id': osd_id, 'ceph.osd_fsid': osd_fsid}
        elif not osd_id and osd_fsid:
            tags = {'ceph.osd_fsid': osd_fsid}
        elif osd_id and not osd_fsid:
            raise RuntimeError('could not activate osd.{}, please provide the '
                               'osd_fsid too'.format(osd_id))
        else:
            raise RuntimeError('Please provide both osd_id and osd_fsid')
        lvs = api.get_lvs(tags=tags)
        if not lvs:
            raise RuntimeError('could not find osd.%s with osd_fsid %s' %
                               (osd_id, osd_fsid))

        self._activate(lvs, self.args.no_systemd, getattr(self.args,
                                                          'no_tmpfs',
                                                          False))
