import logging
import os
from ceph_volume import conf, terminal, process
from ceph_volume.api import lvm as api
from ceph_volume.util import prepare as prepare_utils
from ceph_volume.util import system
from ceph_volume.systemd import systemctl
from ceph_volume import configuration
from .lvm import Lvm
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)


class SeastoreLvm(Lvm):
    """
    LVM-backed seastore OSD.  Inherits the full prepare/activate machinery from
    Lvm but overrides the parts that are BlueStore-specific.
    """

    def add_objectstore_opts(self) -> None:
        # Seastore does not use --bluestore-block-wal/db-path.
        affinity = self.get_osdspec_affinity()
        if affinity:
            self.osd_mkfs_cmd.extend(['--osdspec-affinity', affinity])

    def setup_metadata_devices(self) -> None:
        """
        Record secondary device metadata in LVM tags on the block LV.
        Secondaries are raw devices — no new LVs are created for them.
        Tag keys: ceph.seastore_secondary_{i}_{count,device,type,id}
        """
        secondaries = getattr(self.args, 'seastore_secondary', []) or []
        self.tags['ceph.seastore_secondary_count'] = len(secondaries)
        for i, (device, dtype) in enumerate(secondaries):
            self.tags['ceph.seastore_secondary_%d_device' % i] = device
            self.tags['ceph.seastore_secondary_%d_type' % i] = dtype
            self.tags['ceph.seastore_secondary_%d_id' % i] = i + 1

    def prepare_osd_req(self, tmpfs: bool = True) -> None:
        super().prepare_osd_req(tmpfs=tmpfs)
        secondaries = getattr(self.args, 'seastore_secondary', []) or []
        for i, (device, dtype) in enumerate(secondaries):
            prepare_utils.link_seastore_secondary(device, dtype, i + 1, self.osd_id)

    def unlink_bs_symlinks(self) -> None:
        block_path = os.path.join(self.osd_path, 'block')
        if os.path.exists(block_path):
            os.unlink(block_path)
        prepare_utils.unlink_seastore_secondaries(self.osd_path)

    def _get_secondaries_from_lvs(self, osd_lvs: List[api.Volume]):
        """
        Reconstruct the list of (device, dtype, dev_id) tuples from LVM tags
        stored on the block LV.
        """
        osd_block_lv = None
        for lv in osd_lvs:
            if lv.tags.get('ceph.type') == 'block':
                osd_block_lv = lv
                break
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
        for lv in osd_lvs:
            if lv.tags.get('ceph.type') == 'block':
                osd_block_lv = lv
                break
        else:
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

        osd_lv_path = osd_block_lv.lv_path
        system.chown(self.osd_path)
        process.run(['ln', '-snf', osd_lv_path, os.path.join(self.osd_path, 'block')])
        system.chown(os.path.join(self.osd_path, 'block'))
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
