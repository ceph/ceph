import logging
import json
import os
from .baseobjectstore import BaseObjectStore
from ceph_volume import terminal, decorators, conf, process
from ceph_volume.util import system, disk
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util import nvme as nvme_utils
from ceph_volume.util.raw_osd_crypt_mappers import RawOsdCryptMappers
from ceph_volume.api import lvm as lvm_api
from ceph_volume.devices.lvm.common import rollback_osd
from ceph_volume.devices.raw.list import direct_report
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)


class Raw(BaseObjectStore):
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
            rollback_osd(self.osd_id)
            raise
        dmcrypt_log = 'dmcrypt' if hasattr(args, 'dmcrypt') else 'clear'
        terminal.success("ceph-volume raw {} prepare "
                         "successful for: {}".format(dmcrypt_log,
                                                     self.args.data))

    @decorators.needs_root
    def prepare(self) -> None:
        self.osd_fsid = self.osd_fsid or system.generate_uuid()
        crush_device_class = self.args.crush_device_class
        if self.encrypted and not self.with_tpm:
            self.dmcrypt_key = os.getenv('CEPH_VOLUME_DMCRYPT_SECRET', '')
            self.secrets['dmcrypt_key'] = self.dmcrypt_key
        if crush_device_class:
            self.secrets['crush_device_class'] = crush_device_class

        tmpfs = not self.args.no_tmpfs

        # reuse a given ID if it exists, otherwise create a new ID
        self.osd_id = prepare_utils.create_id(
            self.osd_fsid, json.dumps(self.secrets), self.osd_id)

        if self.precondition_block_device():
            self.skip_mkfs_discard = True

        if self.encrypted:
            self.prepare_dmcrypt()

        self.prepare_osd_req(tmpfs=tmpfs)

        # prepare the osd filesystem
        self.osd_mkfs()

    def precondition_block_device(self) -> bool:
        """
        Run a fast NVMe format on the main block device when applicable.
        Returns True if the block device was formatted, False otherwise.
        """
        if not self.block_device_path:
            return False
        return nvme_utils.preformat(self.block_device_path)

    def _activate(self) -> None:
        mappers: Optional[RawOsdCryptMappers] = None
        if RawOsdCryptMappers.backing_device_path(self.block_device_path):
            mappers = RawOsdCryptMappers(
                self.osd_id,
                self.osd_fsid,
                self.block_device_path,
                self.db_device_path,
                self.wal_device_path,
                cluster_name=conf.cluster,
                dmcrypt_secret=os.getenv('CEPH_VOLUME_DMCRYPT_SECRET') or None,
                with_tpm=bool(self.with_tpm),
            )
        if mappers is not None and mappers.applies():
            try:
                mappers.refresh()
            except RuntimeError as e:
                mlogger.info(
                    'Failed to refresh dmcrypt mappers for osd.%s uuid %s: %s (is the OSD already running?)',
                    self.osd_id,
                    self.osd_fsid,
                    e,
                )
            (
                self.block_device_path,
                self.db_device_path,
                self.wal_device_path,
            ) = mappers.mapper_paths()

        # mount on tmpfs the osd directory
        self.osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, self.osd_id)
        if not system.path_is_mounted(self.osd_path):
            # mkdir -p and mount as tmpfs
            prepare_utils.create_osd_path(self.osd_id, tmpfs=not self.args.no_tmpfs)

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
        prepare_utils.link_block(self.block_device_path, self.osd_id)

        if self.db_device_path:
            prepare_utils.link_db(self.db_device_path, self.osd_id, self.osd_fsid)

        if self.wal_device_path:
            prepare_utils.link_wal(self.wal_device_path, self.osd_id, self.osd_fsid)

        system.chown(self.osd_path)
        terminal.success("ceph-volume raw activate "
                         "successful for osd ID: %s" % self.osd_id)

    @decorators.needs_root
    def activate(self) -> None:
        """Activate Ceph OSDs on the system.

        This function activates Ceph Object Storage Daemons (OSDs) on the system.
        It iterates over all block devices, checking if they have a LUKS2 signature and
        are encrypted for Ceph. LVs tagged by ``ceph-volume lvm prepare`` / ``lvm batch``
        (``ceph.type`` in block/db/wal) are skipped so raw activation does not consume
        LVM-backed OSDs. If a device's OSD fsid matches and it is enrolled with TPM2,
        the function pre-activates it. After collecting the relevant devices, it attempts to
        activate any OSDs found.

        Raises:
            RuntimeError: If no matching OSDs are found to activate.
        """
        assert self.devices or self.osd_id or self.osd_fsid

        activated_any: bool = False
        lvm_prepare_lv_paths = lvm_api.ceph_volume_lvm_prepare_lv_paths()

        for d in disk.lsblk_all(abspath=True):
            device: str = d.get('NAME', '')
            if lvm_api.is_ceph_volume_lvm_prepared(device, lvm_prepare_lv_paths):
                continue
            luks2 = encryption_utils.CephLuks2(device)
            if luks2.is_ceph_encrypted:
                if luks2.is_tpm2_enrolled and self.osd_fsid == luks2.osd_fsid:
                    self.pre_activate_tpm2(device)
        found = direct_report(self.devices)

        filter_osd_id = self.osd_id
        filter_osd_fsid = self.osd_fsid

        for osd_uuid, meta in found.items():
            if meta.get('type') == 'seastore':
                continue
            realpath_device = os.path.realpath(meta['device'])
            if lvm_api.is_ceph_volume_lvm_prepared(realpath_device,
                                                   lvm_prepare_lv_paths):
                continue
            osd_id = meta['osd_id']
            if filter_osd_id is not None and str(osd_id) != str(filter_osd_id):
                continue
            if filter_osd_fsid is not None and osd_uuid != filter_osd_fsid:
                continue
            self.osd_id = str(osd_id)
            self.osd_fsid = str(osd_uuid)
            self.block_device_path = meta.get('device')
            self.db_device_path = meta.get('device_db', '')
            self.wal_device_path = meta.get('device_wal', '')
            logger.info(f'Activating osd.{osd_id} uuid {osd_uuid} cluster {meta["ceph_fsid"]}')
            self._activate()
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
        if not disk.BlockSysFs(device).has_active_dmcrypt_mapper:
            encryption_utils.luks_open(
                '',
                device,
                self.temp_mapper,
                self.with_tpm
            )
            try:
                bluestore_header: Dict[str, Any] = disk.get_bluestore_header(self.temp_mapper_path)
                if not bluestore_header:
                    raise RuntimeError(f"{device} doesn't have BlueStore signature.")

                kname: str = disk.get_parent_device_from_mapper(self.temp_mapper_path, abspath=False)
                device_type = bs_mapping_type[bluestore_header[self.temp_mapper_path]['description']]
                new_mapper: str = f'ceph-{self.osd_fsid}-{kname}-{device_type}-dmcrypt'
                self.block_device_path = f'/dev/mapper/{new_mapper}'
                self.devices.append(self.block_device_path)
            finally:
                # Always close the temporary mapper. On success we reopen under
                # the canonical name below. On failure we must not leave it
                # behind for the SeaStore fallback path.
                encryption_utils.luks_close(self.temp_mapper)
            # An option could be to simply rename the mapper but the uuid remains unchanged in sysfs
            encryption_utils.luks_open('', device, new_mapper, self.with_tpm)


class RawSeastore(Raw):

    def prepare(self) -> None:
        block_db = getattr(self.args, 'block_db', None) or ''
        block_wal = getattr(self.args, 'block_wal', None) or ''
        if block_db or block_wal:
            raise RuntimeError(
                'SeaStore raw OSDs do not support --block.db or --block.wal in ceph-volume.'
            )
        super().prepare()

    def pre_activate_tpm2(self, device: str) -> None:
        """Pre-activate a TPM2-encrypted SeaStore device.

        SeaStore raw OSDs have a single data device whose role is always
        'block', so we can build the canonical mapper name directly without
        reading a BlueStore header.
        """
        self.with_tpm = 1
        temp_mapper: str = f'activating-{os.path.basename(device)}'
        temp_mapper_path: str = f'/dev/mapper/{temp_mapper}'
        if not disk.BlockSysFs(device).has_active_dmcrypt_mapper:
            encryption_utils.luks_open('', device, temp_mapper, self.with_tpm)
            kname: str = disk.get_parent_device_from_mapper(temp_mapper_path,
                                                             abspath=False)
            new_mapper: str = f'ceph-{self.osd_fsid}-{kname}-block-dmcrypt'
            self.block_device_path = f'/dev/mapper/{new_mapper}'
            self.devices.append(self.block_device_path)
            encryption_utils.luks_close(temp_mapper)
            encryption_utils.luks_open('', device, new_mapper, self.with_tpm)

    def _activate(self) -> None:
        mappers: Optional[RawOsdCryptMappers] = None
        if RawOsdCryptMappers.backing_device_path(self.block_device_path):
            mappers = RawOsdCryptMappers(
                self.osd_id,
                self.osd_fsid,
                self.block_device_path,
                cluster_name=conf.cluster,
                dmcrypt_secret=os.getenv('CEPH_VOLUME_DMCRYPT_SECRET') or None,
                with_tpm=bool(self.with_tpm),
            )
        if mappers is not None and mappers.applies():
            try:
                mappers.refresh()
            except RuntimeError as e:
                mlogger.info(
                    'Failed to refresh dmcrypt mappers for osd.%s uuid %s: %s'
                    ' (is the OSD already running?)',
                    self.osd_id,
                    self.osd_fsid,
                    e,
                )
            (self.block_device_path, _, _) = mappers.mapper_paths()

        self.osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, self.osd_id)
        if not system.path_is_mounted(self.osd_path):
            prepare_utils.create_osd_path(self.osd_id, tmpfs=not self.args.no_tmpfs)

        self.unlink_bs_symlinks()
        system.chown(self.osd_path)
        prepare_utils.link_block(self.block_device_path, self.osd_id)
        system.chown(self.osd_path)
        terminal.success("ceph-volume raw activate "
                         "successful for osd ID: %s" % self.osd_id)

    def _find_seastore_candidates(self, devices: List[str]) -> List[str]:
        """Return block devices from *devices* that carry a SeaStore signature."""
        candidates = []
        for dev in devices:
            if not dev:
                continue
            try:
                if disk.has_seastore_label(dev):
                    candidates.append(dev)
            except OSError as exc:
                logger.warning('could not read SeaStore label from %s: %s', dev, exc)
        return candidates

    @decorators.needs_root
    def activate(self) -> None:
        if not (self.devices or self.osd_id or self.osd_fsid):
            raise RuntimeError(
                'SeaStore activation requires at least --osd-id, --osd-uuid, '
                'or an explicit device.'
            )
        if not self.osd_id or not self.osd_fsid:
            raise RuntimeError(
                'SeaStore activation requires both --osd-id and --osd-uuid.'
            )

        osd_id = self.osd_id
        osd_fsid = self.osd_fsid

        # Pre activation for encrypted devices: open any matching Ceph LUKS
        # device before scanning for the SeaStore signature, because a closed
        # LUKS mapper hides the on-disk magic entirely.
        # lsblk_all() is called once here and reused for the candidate scan
        # when no explicit devices are given, avoiding a second lsblk call.
        # LVM prepared devices are excluded from both scans so that LVM backed
        # OSDs are not claimed by the raw path and can fall through to LVMActivate.
        lvm_prepare_lv_paths = lvm_api.ceph_volume_lvm_prepare_lv_paths()
        all_devices = disk.lsblk_all(abspath=True)
        for d in all_devices:
            device: str = d.get('NAME', '')
            if lvm_api.is_ceph_volume_lvm_prepared(device, lvm_prepare_lv_paths):
                continue
            luks2 = encryption_utils.CephLuks2(device)
            if not luks2.is_ceph_encrypted or luks2.osd_fsid != osd_fsid:
                continue
            if luks2.is_tpm2_enrolled:
                self.pre_activate_tpm2(device)
            else:
                # Key-based dmcrypt: open the mapper so the seastore magic
                # is readable by _find_seastore_candidates().
                kname = os.path.basename(os.path.realpath(device))
                mapper = f'ceph-{osd_fsid}-{kname}-block-dmcrypt'
                if not disk.BlockSysFs(device).has_active_dmcrypt_mapper:
                    encryption_utils.luks_open(
                        os.getenv('CEPH_VOLUME_DMCRYPT_SECRET', ''),
                        device,
                        mapper,
                        0,
                    )
                self.block_device_path = f'/dev/mapper/{mapper}'
                self.devices = [self.block_device_path]

        # Build the candidate device list: either the explicitly provided
        # devices, or all block devices discovered above (cephadm path).
        # LVM prepared devices are filtered out so an LVM OSD carrying the
        # Crimson signature is not claimed here.
        # NOTE: SeaStore does not expose the OSD fsid in a format readable
        # without a DENC parser, so we cannot filter by osd_fsid here.
        # If multiple SeaStore OSDs are present on the host, an explicit
        # --device must be passed.
        if self.devices:
            scan_devices = list(self.devices)
        else:
            scan_devices = [
                d.get('NAME', '') for d in all_devices
                if not lvm_api.is_ceph_volume_lvm_prepared(
                    d.get('NAME', ''), lvm_prepare_lv_paths)
            ]

        candidates = self._find_seastore_candidates(scan_devices)

        if not candidates:
            raise RuntimeError('did not find any SeaStore device to activate')
        if len(candidates) > 1:
            raise RuntimeError(
                'multiple SeaStore devices found; cannot determine which '
                'belongs to osd.%s — pass an explicit --device.' % osd_id
            )

        self.block_device_path = candidates[0]
        self.db_device_path = ''
        self.wal_device_path = ''
        self.osd_id = str(osd_id)
        self.osd_fsid = str(osd_fsid)
        self._activate()
