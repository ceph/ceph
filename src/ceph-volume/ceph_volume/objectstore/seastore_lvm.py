import os
from typing import List, Optional

from ceph_volume import conf, configuration, process, terminal
from ceph_volume.api import lvm as api
from ceph_volume.systemd import systemctl
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system
from .lvm import Lvm
from .seastore import SeastoreMixin


class SeastoreLvm(SeastoreMixin, Lvm):
    """
    LVM-backed seastore OSD.  Inherits the full prepare/activate machinery from
    Lvm but overrides the parts that are BlueStore-specific via SeastoreMixin.
    """

    def setup_metadata_devices(self) -> None:
        """
        Record secondary device metadata in LVM tags on the block LV.
        Secondaries are raw devices -- no new LVs are created for them.
        Tag keys: ceph.seastore_secondary_{i}_{count,device,type,id}
        """
        secondaries = getattr(self.args, 'seastore_secondary', [])
        self.tags['ceph.seastore_secondary_count'] = len(secondaries)
        for i, (device, dtype) in enumerate(secondaries):
            self.tags['ceph.seastore_secondary_%d_device' % i] = device
            self.tags['ceph.seastore_secondary_%d_type' % i] = dtype
            self.tags['ceph.seastore_secondary_%d_id' % i] = i + 1

    @staticmethod
    def _find_block_lv(osd_lvs: List[api.Volume]) -> Optional[api.Volume]:
        """Return the block LV from a list of OSD LVs, or None."""
        for lv in osd_lvs:
            if lv.tags.get('ceph.type') == 'block':
                return lv
        return None

    def _get_secondaries_from_lvs(self, osd_lvs: List[api.Volume]) -> list:
        """
        Reconstruct the list of (device, dtype, dev_id) tuples from LVM tags
        stored on the block LV.
        """
        osd_block_lv = self._find_block_lv(osd_lvs)
        if osd_block_lv is None:
            return []

        try:
            count = int(osd_block_lv.tags.get('ceph.seastore_secondary_count', 0))
        except (ValueError, TypeError):
            return []

        secondaries = []
        for i in range(count):
            device = osd_block_lv.tags.get('ceph.seastore_secondary_%d_device' % i)
            dtype = osd_block_lv.tags.get('ceph.seastore_secondary_%d_type' % i)
            try:
                dev_id = int(osd_block_lv.tags.get('ceph.seastore_secondary_%d_id' % i, i + 1))
            except (ValueError, TypeError):
                dev_id = i + 1
            if device and dtype:
                secondaries.append((device, dtype, dev_id))
        return secondaries

    def _activate(self,
                  osd_lvs: List[api.Volume],
                  no_systemd: bool = False,
                  no_tmpfs: bool = False) -> None:
        osd_block_lv = self._find_block_lv(osd_lvs)
        if osd_block_lv is None:
            raise RuntimeError('could not find a seastore OSD to activate')

        osd_id = osd_block_lv.tags['ceph.osd_id']
        conf.cluster = osd_block_lv.tags['ceph.cluster_name']
        osd_fsid = osd_block_lv.tags['ceph.osd_fsid']
        configuration.load_ceph_conf_path(osd_block_lv.tags['ceph.cluster_name'])
        configuration.load()

        self.osd_path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
        if not system.path_is_mounted(self.osd_path):
            prepare_utils.create_osd_path(osd_id, tmpfs=not no_tmpfs)

        self.unlink_bs_symlinks()

        block_path = os.path.join(self.osd_path, 'block')
        process.run(['ln', '-snf', osd_block_lv.lv_path, block_path])
        system.chown(block_path)
        system.chown(self.osd_path)

        for device, dtype, dev_id in self._get_secondaries_from_lvs(osd_lvs):
            sec_dir = os.path.join(self.osd_path, 'block.%s.%s' % (dtype, dev_id))
            os.makedirs(sec_dir, exist_ok=True)
            system.chown(device)
            process.run(['ln', '-snf', device, os.path.join(sec_dir, 'block')])
            system.chown(sec_dir)

        if not no_systemd:
            systemctl.enable_volume(osd_id, osd_fsid, 'lvm')
            systemctl.enable_osd(osd_id)
            systemctl.start_osd(osd_id)
        terminal.success("ceph-volume lvm activate successful for osd ID: %s" % osd_id)
