import logging
import json
import os
from .bluestore import BlueStore
from ceph_volume import terminal, decorators, conf, process
from ceph_volume.util import system, disk
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.devices.lvm.common import rollback_osd
from ceph_volume.devices.raw.list import direct_report
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)


class RawBlueStore(BlueStore):
    def __init__(self, args: "argparse.Namespace") -> None:
        super().__init__(args)
        self.method = 'raw'
        self.devices: List[str] = getattr(args, 'devices', [])
        self.osd_id = getattr(self.args, 'osd_id', '')
        self.osd_fsid = getattr(self.args, 'osd_fsid', '')
        self.block_device_path = getattr(self.args, 'data', '')
        self.db_device_path = getattr(self.args, 'block_db', '')
        self.wal_device_path = getattr(self.args, 'block_wal', '')

    def prepare_dmcrypt(self) -> None:
        """
        Helper for devices that are encrypted. The operations needed for
        block, db, wal, devices are all the same
        """

        for device, device_type in [(self.block_device_path, 'block'),
                                    (self.db_device_path, 'db'),
                                    (self.wal_device_path, 'wal')]:

            if device:
                kname = disk.lsblk(device)['KNAME']
                mapping = 'ceph-{}-{}-{}-dmcrypt'.format(self.osd_fsid,
                                                         kname,
                                                         device_type)
                # format data device
                encryption_utils.luks_format(
                    self.dmcrypt_key,
                    device
                )
                if self.with_tpm:
                    self.enroll_tpm2(device)
                encryption_utils.luks_open(
                    self.dmcrypt_key,
                    device,
                    mapping,
                    self.with_tpm
                )
                self.__dict__[f'{device_type}_device_path'] = \
                    '/dev/mapper/{}'.format(mapping)  # TODO(guits): need to preserve path or find a way to get the parent device from the mapper ?

    def safe_prepare(self,
                     args: Optional["argparse.Namespace"] = None) -> None:
        """
        An intermediate step between `main()` and `prepare()` so that we can
        capture the `self.osd_id` in case we need to rollback

        :param args: Injected args, usually from `raw create` which compounds
                     both `prepare` and `create`
        """
        if args is not None:
            self.args = args  # This should be moved (to __init__ ?)
        try:
            self.prepare()
        except Exception:
            logger.exception('raw prepare was unable to complete')
            logger.info('will rollback OSD ID creation')
            rollback_osd(self.args, self.osd_id)
            raise
        dmcrypt_log = 'dmcrypt' if hasattr(args, 'dmcrypt') else 'clear'
        terminal.success("ceph-volume raw {} prepare "
                         "successful for: {}".format(dmcrypt_log,
                                                     self.args.data))

    @decorators.needs_root
    def prepare(self) -> None:
        self.osd_fsid = system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if self.encrypted and not self.with_tpm:
            self.dmcrypt_key = os.getenv('CEPH_VOLUME_DMCRYPT_SECRET', '')
            self.secrets['dmcrypt_key'] = self.dmcrypt_key
        if crush_device_class:
            self.secrets['crush_device_class'] = crush_device_class

        tmpfs = not self.args.no_tmpfs

        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(
            self.osd_fsid, json.dumps(self.secrets))

        if self.encrypted:
            self.prepare_dmcrypt()

        self.prepare_osd_req(tmpfs=tmpfs)

        # prepare the osd filesystem
        self.osd_mkfs()

    def _activate(self, osd_id: str, osd_fsid: str) -> None:
        # mount on tmpfs the osd directory
        self.osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
        if not system.path_is_mounted(self.osd_path):
            # mkdir -p and mount as tmpfs
            prepare_utils.create_osd_path(osd_id, tmpfs=not self.args.no_tmpfs)

        # XXX This needs to be removed once ceph-bluestore-tool can deal with
        # symlinks that exist in the osd dir

        self.unlink_bs_symlinks()

        # Once symlinks are removed, the osd dir can be 'primed again. chown
        # first, regardless of what currently exists so that ``prime-osd-dir``
        # can succeed even if permissions are somehow messed up
        system.chown(self.osd_path)
        prime_command = [
            'ceph-bluestore-tool',
            'prime-osd-dir',
            '--path', self.osd_path,
            '--no-mon-config',
            '--dev', self.block_device_path,
        ]
        process.run(prime_command)

        # always re-do the symlink regardless if it exists, so that the block,
        # block.wal, and block.db devices that may have changed can be mapped
        # correctly every time
        prepare_utils.link_block(self.block_device_path, osd_id)

        if self.db_device_path:
            prepare_utils.link_db(self.db_device_path, osd_id, osd_fsid)

        if self.wal_device_path:
            prepare_utils.link_wal(self.wal_device_path, osd_id, osd_fsid)

        system.chown(self.osd_path)
        terminal.success("ceph-volume raw activate "
                         "successful for osd ID: %s" % osd_id)

    @decorators.needs_root
    def activate(self) -> None:
        """Activate Ceph OSDs on the system.

        This function activates Ceph Object Storage Daemons (OSDs) on the system.
        It iterates over all block devices, checking if they have a LUKS2 signature and
        are encrypted for Ceph. If a device's OSD fsid matches and it is enrolled with TPM2,
        the function pre-activates it. After collecting the relevant devices, it attempts to
        activate any OSDs found.

        Raises:
            RuntimeError: If no matching OSDs are found to activate.
        """
        assert self.devices or self.osd_id or self.osd_fsid

        activated_any: bool = False

        for d in disk.lsblk_all(abspath=True):
            device: str = d.get('NAME')
            luks2 = encryption_utils.CephLuks2(device)
            if luks2.is_ceph_encrypted:
                if luks2.is_tpm2_enrolled and self.osd_fsid == luks2.osd_fsid:
                    self.pre_activate_tpm2(device)
        found = direct_report(self.devices)

        for osd_uuid, meta in found.items():
            osd_id = meta['osd_id']
            if self.osd_id is not None and str(osd_id) != str(self.osd_id):
                continue
            if self.osd_fsid is not None and osd_uuid != self.osd_fsid:
                continue
            self.block_device_path = meta.get('device')
            self.db_device_path = meta.get('device_db', '')
            self.wal_device_path = meta.get('device_wal', '')
            logger.info(f'Activating osd.{osd_id} uuid {osd_uuid} cluster {meta["ceph_fsid"]}')
            self._activate(osd_id, osd_uuid)
            activated_any = True

        if not activated_any:
            raise RuntimeError('did not find any matching OSD to activate')

    def pre_activate_tpm2(self, device: str) -> None:
        """Pre-activate a TPM2-encrypted device for Ceph.

        This function pre-activates a TPM2-encrypted device for Ceph by opening the
        LUKS encryption, checking the BlueStore header, and renaming the device
        mapper according to the BlueStore mapping type.

        Args:
            device (str): The path to the device to be pre-activated.

        Raises:
            RuntimeError: If the device does not have a BlueStore signature.
        """
        bs_mapping_type: Dict[str, str] = {'bluefs db': 'db',
                                           'bluefs wal': 'wal',
                                           'main': 'block'}
        self.with_tpm = 1
        self.temp_mapper: str = f'activating-{os.path.basename(device)}'
        self.temp_mapper_path: str = f'/dev/mapper/{self.temp_mapper}'
        encryption_utils.luks_open(
            '',
            device,
            self.temp_mapper,
            self.with_tpm
        )
        bluestore_header: Dict[str, Any] = disk.get_bluestore_header(self.temp_mapper_path)
        if not bluestore_header:
            raise RuntimeError(f"{device} doesn't have BlueStore signature.")

        kname: str = disk.get_parent_device_from_mapper(self.temp_mapper_path, abspath=False)
        device_type = bs_mapping_type[bluestore_header[self.temp_mapper_path]['description']]
        new_mapper: str = f'ceph-{self.osd_fsid}-{kname}-{device_type}-dmcrypt'
        self.block_device_path = f'/dev/mapper/{new_mapper}'
        self.devices.append(self.block_device_path)
        encryption_utils.rename_mapper(self.temp_mapper, new_mapper)
