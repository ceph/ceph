import os
from ceph_volume import sys_info
from ceph_volume.api import lvm
from ceph_volume.util import disk


class Device(object):

    def __init__(self, path):
        self.path = path
        # LVs can have a vg/lv path, while disks will have /dev/sda
        self.abspath = path
        self.lv_api = None
        self.pvs_api = []
        self.disk_api = {}
        self.blkid_api = {}
        self.sys_api = {}
        self._exists = None
        self._is_lvm_member = None
        self._parse()

    def _parse(self):
        # start with lvm since it can use an absolute or relative path
        lv = lvm.get_lv_from_argument(self.path)
        if lv:
            self.lv_api = lv
            self.abspath = lv.lv_path
        else:
            dev = disk.lsblk(self.path)
            self.blkid_api = disk.blkid(self.path)
            self.disk_api = dev
            device_type = dev.get('TYPE', '')
            # always check is this is an lvm member
            if device_type in ['part', 'disk']:
                self._set_lvm_membership()

        if not sys_info.devices:
            sys_info.devices = disk.get_devices()
        self.sys_api = sys_info.devices.get(self.abspath, {})

        self.ceph_disk = CephDiskDevice(self)

    def __repr__(self):
        prefix = 'Unknown'
        if self.is_lv:
            prefix = 'LV'
        elif self.is_partition:
            prefix = 'Partition'
        elif self.is_device:
            prefix = 'Raw Device'
        return '<%s: %s>' % (prefix, self.abspath)

    def _set_lvm_membership(self):
        if self._is_lvm_member is None:
            # check if there was a pv created with the
            # name of device
            pvs = lvm.PVolumes()
            pvs.filter(pv_name=self.abspath)
            if not pvs:
                self._is_lvm_member = False
                return self._is_lvm_member
            has_vgs = [pv.vg_name for pv in pvs if pv.vg_name]
            if has_vgs:
                self._is_lvm_member = True
                self.pvs_api = pvs
            else:
                # this is contentious, if a PV is recognized by LVM but has no
                # VGs, should we consider it as part of LVM? We choose not to
                # here, because most likely, we need to use VGs from this PV.
                self._is_lvm_member = False

        return self._is_lvm_member

    @property
    def exists(self):
        return os.path.exists(self.abspath)

    @property
    def is_lvm_member(self):
        if self._is_lvm_member is None:
            self._set_lvm_membership()
        return self._is_lvm_member

    @property
    def is_ceph_disk_member(self):
        return self.ceph_disk.is_member

    @property
    def is_mapper(self):
        return self.path.startswith('/dev/mapper')

    @property
    def is_lv(self):
        return self.lv_api is not None

    @property
    def is_partition(self):
        if self.disk_api:
            return self.disk_api['TYPE'] == 'part'
        return False

    @property
    def is_device(self):
        if self.disk_api:
            return self.disk_api['TYPE'] == 'device'
        return False


class CephDiskDevice(object):
    """
    Detect devices that have been created by ceph-disk, report their type
    (journal, data, etc..). Requires a ``Device`` object as input.
    """

    def __init__(self, device):
        self.device = device
        self._is_ceph_disk_member = None

    @property
    def partlabel(self):
        """
        In containers, the 'PARTLABEL' attribute might not be detected
        correctly via ``lsblk``, so we poke at the value with ``lsblk`` first,
        falling back to ``blkid`` (which works correclty in containers).
        """
        lsblk_partlabel = self.device.disk_api.get('PARTLABEL')
        if lsblk_partlabel:
            return lsblk_partlabel
        return self.device.blkid_api.get('PARTLABEL', '')

    @property
    def is_member(self):
        if self._is_ceph_disk_member is None:
            if 'ceph' in self.partlabel:
                self._is_ceph_disk_member = True
                return True
            return False
        return self._is_ceph_disk_member

    @property
    def type(self):
        types = [
            'data', 'wal', 'db', 'lockbox', 'journal',
            # ceph-disk uses 'ceph block' when placing data in bluestore, but
            # keeps the regular OSD files in 'ceph data' :( :( :( :(
            'block',
        ]
        for t in types:
            if t in self.partlabel:
                return t
        return 'unknown'
