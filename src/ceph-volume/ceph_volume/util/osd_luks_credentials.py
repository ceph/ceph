from typing import Optional, Union

from ceph_volume import conf, configuration
from ceph_volume.util import encryption as encryption_utils
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system


class OsdLuksCredentials:
    def __init__(
        self,
        osd_id: Union[int, str],
        osd_fsid: str,
        luks_secret: Optional[str] = None,
        with_tpm: bool = False,
    ) -> None:
        self.osd_id = str(osd_id)
        self.osd_fsid = osd_fsid
        self.luks_secret = luks_secret
        self.with_tpm = bool(with_tpm)

    def apply_cluster_context(self, cluster_name: str) -> None:
        conf.cluster = cluster_name
        configuration.load_ceph_conf_path(cluster_name)
        configuration.load()

    def _write_lockbox_keyring_if_needed(self, lockbox_secret: Optional[str]) -> None:
        if self.with_tpm or lockbox_secret is None:
            return
        osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, self.osd_id)
        if not system.path_is_mounted(osd_path):
            prepare_utils.create_osd_path(self.osd_id, tmpfs=True)
        encryption_utils.write_lockbox_keyring(
            self.osd_id,
            self.osd_fsid,
            lockbox_secret,
        )

    def resolve_secret(self, lockbox_secret: Optional[str]) -> str:
        if self.with_tpm:
            return ''
        if self.luks_secret is not None:
            return self.luks_secret
        self._write_lockbox_keyring_if_needed(lockbox_secret)
        self.luks_secret = encryption_utils.get_dmcrypt_key(
            self.osd_id,
            self.osd_fsid,
        )
        return self.luks_secret
