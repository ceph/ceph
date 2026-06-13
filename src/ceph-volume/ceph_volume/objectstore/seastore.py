"""
Shared helpers for seastore objectstore backends (LVM and raw).
"""
import os
from ceph_volume.util import prepare as prepare_utils


class SeastoreMixin:
    """Methods shared by SeastoreLvm and SeastoreRaw.

    Mixed into each concrete class alongside its storage-specific base
    (Lvm or Raw).  Every method here overrides a BlueStore-oriented
    default from BaseObjectStore.
    """

    def add_objectstore_opts(self) -> None:
        affinity = self.get_osdspec_affinity()
        if affinity:
            self.osd_mkfs_cmd.extend(['--osdspec-affinity', affinity])

    def prepare_osd_req(self, tmpfs: bool = True) -> None:
        super().prepare_osd_req(tmpfs=tmpfs)
        for i, (device, dtype) in enumerate(
                getattr(self.args, 'seastore_secondary', [])):
            prepare_utils.link_seastore_secondary(
                device, dtype, i + 1, self.osd_id)

    def unlink_bs_symlinks(self) -> None:
        block_path = os.path.join(self.osd_path, 'block')
        if os.path.exists(block_path):
            os.unlink(block_path)
        prepare_utils.unlink_seastore_secondaries(self.osd_path)
