import os
from typing import Dict, List, Optional, Tuple, Union

from ceph_volume import conf
from ceph_volume.util import disk
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util.osd_luks_credentials import OsdLuksCredentials


class RawOsdCryptMappers:
    def __init__(
        self,
        osd_id: Union[int, str],
        osd_fsid: str,
        block_path: str,
        db_path: str = '',
        wal_path: str = '',
        cluster_name: Optional[str] = None,
        dmcrypt_secret: Optional[str] = None,
        dmcrypt_open_opts: Optional[str] = None,
        lockbox_secret: Optional[str] = None,
        with_tpm: bool = False,
    ) -> None:
        backing_block = self.backing_device_path(block_path)
        if not backing_block:
            raise ValueError('block backing device is required')
        self.activate_block_path = block_path
        self.activate_db_path = db_path or ''
        self.activate_wal_path = wal_path or ''
        self.block_device = backing_block
        self.db_device = self.backing_device_path(db_path) if db_path else ''
        self.wal_device = self.backing_device_path(wal_path) if wal_path else ''
        self.credentials = OsdLuksCredentials(
            osd_id,
            osd_fsid,
            luks_secret=dmcrypt_secret,
            with_tpm=with_tpm,
        )
        self.dmcrypt_open_opts = dmcrypt_open_opts
        self.lockbox_secret = lockbox_secret
        name = cluster_name or conf.cluster or 'ceph'
        self.credentials.apply_cluster_context(name)
        self._mapper_names = self._build_mapper_names()

    @staticmethod
    def backing_device_path(device_path: str) -> str:
        if not device_path:
            return ''
        if device_path.startswith('/dev/mapper/'):
            return disk.get_parent_device_from_mapper(device_path) or ''
        return device_path

    @staticmethod
    def _kname_from_activate_path(
        activate_path: str, osd_fsid: str, role: str,
    ) -> Optional[str]:
        if not activate_path.startswith('/dev/mapper/'):
            return None
        base = os.path.basename(activate_path)
        prefix = 'ceph-{}-'.format(osd_fsid)
        suffix = '-{}-dmcrypt'.format(role)
        if base.startswith(prefix) and base.endswith(suffix):
            return base[len(prefix):-len(suffix)]
        return None

    @staticmethod
    def _kname_from_backing_device(backing_device_path: str) -> str:
        return os.path.basename(os.path.realpath(backing_device_path))

    def _kname_for_role(self, backing: str, activate_path: str, role: str) -> str:
        kname = self._kname_from_activate_path(
            activate_path, self.credentials.osd_fsid, role,
        )
        if kname is not None:
            return kname
        return self._kname_from_backing_device(backing)

    def _build_mapper_names(self) -> Dict[str, str]:
        names: Dict[str, str] = {}
        for role, backing, activate_path in (
            ('block', self.block_device, self.activate_block_path),
            ('db', self.db_device, self.activate_db_path),
            ('wal', self.wal_device, self.activate_wal_path),
        ):
            if not backing:
                continue
            kname = self._kname_for_role(backing, activate_path, role)
            names[role] = 'ceph-{}-{}-{}-dmcrypt'.format(
                self.credentials.osd_fsid, kname, role,
            )
        return names

    def applies(self) -> bool:
        if self.credentials.with_tpm:
            return True
        for path in (
            self.activate_block_path,
            self.activate_db_path,
            self.activate_wal_path,
        ):
            if not path:
                continue
            if path.startswith('/dev/mapper/') and path.endswith('-dmcrypt'):
                return True
            backing = self.backing_device_path(path)
            if not backing:
                continue
            if encryption_utils.CephLuks2(backing).is_ceph_encrypted:
                return True
        return False

    def _mapper_name_for_role(self, role: str) -> str:
        return self._mapper_names[role]

    def _mapper_path_for_backing(self, backing_device_path: str, role: str) -> str:
        return '/dev/mapper/%s' % self._mapper_name_for_role(role)

    def mapper_paths(self) -> Tuple[str, str, str]:
        return (
            self._mapper_path_for_backing(self.block_device, 'block'),
            self._mapper_path_for_backing(self.db_device, 'db') if self.db_device else '',
            self._mapper_path_for_backing(self.wal_device, 'wal') if self.wal_device else '',
        )

    def close(self) -> None:
        for role, device in self._role_devices():
            self._close_crypt_for_role(role)

    def open(self) -> None:
        for role, device in self._role_devices():
            self._luks_open_for_role(role, device)

    def refresh(self) -> None:
        self.close()
        self.open()

    def _role_devices(self) -> Tuple[Tuple[str, str], ...]:
        out: List[Tuple[str, str]] = []
        if self.block_device:
            out.append(('block', self.block_device))
        if self.db_device:
            out.append(('db', self.db_device))
        if self.wal_device:
            out.append(('wal', self.wal_device))
        return tuple(out)

    def _close_crypt_for_role(self, role: str) -> None:
        encryption_utils.dmsetup_remove(
            self._mapper_name_for_role(role),
            terminal_logging=False,
        )

    def _luks_open_for_role(self, role: str, device_path: str) -> None:
        encryption_utils.luks_open(
            self.credentials.resolve_secret(self.lockbox_secret),
            device_path,
            self._mapper_name_for_role(role),
            with_tpm=1 if self.credentials.with_tpm else 0,
            options=self.dmcrypt_open_opts,
        )
