from typing import List, Optional, Union

from ceph_volume import process
from ceph_volume.api.lvm import Volume, get_lvs
from ceph_volume.util import disk
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util.osd_luks_credentials import OsdLuksCredentials


class OsdLvmMappers:
    def __init__(
        self,
        osd_id: Union[int, str],
        osd_fsid: str,
        lvs: Optional[List[Volume]] = None,
        dmcrypt_secret: Optional[str] = None,
        dmcrypt_open_opts: Optional[str] = None,
    ) -> None:
        self.osd_fsid = osd_fsid
        self.credentials = OsdLuksCredentials(
            osd_id,
            osd_fsid,
            luks_secret=dmcrypt_secret,
            with_tpm=False,
        )
        self.dmcrypt_open_opts = dmcrypt_open_opts
        self.encrypted = False
        if lvs is None:
            lvs = get_lvs(
                tags={'ceph.osd_id': self.credentials.osd_id, 'ceph.osd_fsid': osd_fsid}
            )
        self._assign_role_volumes_from_lv_list(lvs)

    def close(self) -> None:
        self._close_wal_mapper()
        self._close_db_mapper()
        self._close_block_mapper()

    def open(self) -> None:
        self._rescan_physical_volumes()
        self._refresh_osd_volumes_from_lvm()
        paths = [
            vol.lv_path
            for vol in (self.block_volume, self.db_volume, self.wal_volume)
            if vol is not None
        ]
        if paths:
            process.call(['lvchange', '-ay'] + paths, run_on_host=True)
        if self.encrypted:
            for role in ('block', 'db', 'wal'):
                self._luks_open_role(role)

    def refresh(self) -> None:
        self.close()
        self.open()

    def _assign_role_volumes_from_lv_list(self, lvs: List[Volume]) -> None:
        self.lvs = lvs
        self.block_volume: Optional[Volume] = None
        self.db_volume: Optional[Volume] = None
        self.wal_volume: Optional[Volume] = None
        for lv in lvs:
            kind = lv.tags.get('ceph.type')
            if kind == 'block' and self.block_volume is None:
                self.block_volume = lv
            elif kind == 'db' and self.db_volume is None:
                self.db_volume = lv
            elif kind == 'wal' and self.wal_volume is None:
                self.wal_volume = lv
        self._sync_encryption_flags()
        self.credentials.with_tpm = self.with_tpm
        cluster_name = self._cluster_name_for_context()
        if cluster_name is not None:
            self.credentials.apply_cluster_context(cluster_name)

    def _cluster_name_for_context(self) -> Optional[str]:
        if self.block_volume is None:
            return None
        return self.block_volume.tags.get('ceph.cluster_name') or 'ceph'

    def _sync_encryption_flags(self) -> None:
        self.encrypted = False
        self.with_tpm = False
        if self.block_volume is None:
            return
        self.encrypted = self.block_volume.tags.get('ceph.encrypted', '0') == '1'
        self.with_tpm = self.block_volume.tags.get('ceph.with_tpm') == '1'

    def _device_uuid_for_role(self, role: str) -> Optional[str]:
        if self.block_volume is None:
            return None
        if role == 'block':
            from_tag = self.block_volume.tags.get('ceph.block_uuid', '')
            if from_tag:
                return from_tag
            return self.block_volume.lv_uuid or None
        tag = 'ceph.%s_uuid' % role
        value = self.block_volume.tags.get(tag, '')
        return value if value else None

    def _crypt_mapper_device_path(self, uuid_value: Optional[str]) -> Optional[str]:
        if not uuid_value:
            return None
        return '/dev/mapper/%s' % uuid_value

    def _block_crypt_path(self) -> Optional[str]:
        if not self.encrypted:
            return None
        return self._crypt_mapper_device_path(self._device_uuid_for_role('block'))

    def _db_crypt_path(self) -> Optional[str]:
        if not self.encrypted:
            return None
        return self._crypt_mapper_device_path(self._device_uuid_for_role('db'))

    def _wal_crypt_path(self) -> Optional[str]:
        if not self.encrypted:
            return None
        return self._crypt_mapper_device_path(self._device_uuid_for_role('wal'))

    def _underlying_device_for_encrypted_role(self, role: str) -> Optional[str]:
        if role == 'block':
            if self.block_volume is None:
                return None
            return self.block_volume.lv_path
        uuid_value = self._device_uuid_for_role(role)
        if not uuid_value:
            return None
        if role == 'db' and self.db_volume is not None:
            return self.db_volume.lv_path
        if role == 'wal' and self.wal_volume is not None:
            return self.wal_volume.lv_path
        return disk.get_device_from_partuuid(uuid_value)

    def _lockbox_secret_from_block_lv(self) -> Optional[str]:
        if self.block_volume is None:
            return None
        return self.block_volume.tags.get('ceph.cephx_lockbox_secret')

    def _luks_open_role(self, role: str) -> None:
        if not self.encrypted or self.block_volume is None:
            return
        uuid_value = self._device_uuid_for_role(role)
        if not uuid_value:
            return
        device = self._underlying_device_for_encrypted_role(role)
        if not device:
            return
        encryption_utils.luks_open(
            self.credentials.resolve_secret(self._lockbox_secret_from_block_lv()),
            device,
            uuid_value,
            with_tpm=self.with_tpm,
            options=self.dmcrypt_open_opts,
        )

    def _refresh_osd_volumes_from_lvm(self) -> None:
        refreshed = get_lvs(
            tags={'ceph.osd_id': self.credentials.osd_id, 'ceph.osd_fsid': self.osd_fsid}
        )
        self._assign_role_volumes_from_lv_list(refreshed)

    @staticmethod
    def _deactivate_logical_volume(volume: Volume) -> None:
        process.call(
            [volume.binary_change, '-an', volume.lv_path],
            run_on_host=True,
            show_command=True,
        )

    def _close_block_crypt_mapper(self) -> None:
        path = self._block_crypt_path()
        if path:
            encryption_utils.dmcrypt_close(path)

    def _close_db_crypt_mapper(self) -> None:
        path = self._db_crypt_path()
        if path:
            encryption_utils.dmcrypt_close(path)

    def _close_wal_crypt_mapper(self) -> None:
        path = self._wal_crypt_path()
        if path:
            encryption_utils.dmcrypt_close(path)

    def _close_block_mapper(self) -> None:
        if self.block_volume is None:
            return
        self._close_block_crypt_mapper()
        self._deactivate_logical_volume(self.block_volume)

    def _close_db_mapper(self) -> None:
        self._close_db_crypt_mapper()
        if self.db_volume is not None:
            self._deactivate_logical_volume(self.db_volume)

    def _close_wal_mapper(self) -> None:
        self._close_wal_crypt_mapper()
        if self.wal_volume is not None:
            self._deactivate_logical_volume(self.wal_volume)

    def _rescan_physical_volumes(self) -> None:
        process.call(['pvscan', '--cache'], run_on_host=True)
