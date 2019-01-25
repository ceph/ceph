# -*- coding: utf-8 -*-

import os
from functools import total_ordering
from ceph_volume import sys_info
from ceph_volume.api import lvm
from ceph_volume.util import disk

report_template = """
{dev:<25} {size:<12} {rot!s:<7} {available!s:<9} {model}"""


def encryption_status(abspath):
    """
    Helper function to run ``encryption.status()``. It is done here to avoid
    a circular import issue (encryption module imports from this module) and to
    ease testing by allowing monkeypatching of this function.
    """
    from ceph_volume.util import encryption
    return encryption.status(abspath)


class Devices(object):
    """
    A container for Device instances with reporting
    """

    def __init__(self, devices=None):
        if not sys_info.devices:
            sys_info.devices = disk.get_devices()
        self.devices = [Device(k) for k in
                            sys_info.devices.keys()]

    def pretty_report(self, all=True):
        output = [
            report_template.format(
                dev='Device Path',
                size='Size',
                rot='rotates',
                model='Model name',
                available='available',
            )]
        for device in sorted(self.devices):
            output.append(device.report())
        return ''.join(output)

    def json_report(self):
        output = []
        for device in sorted(self.devices):
            output.append(device.json_report())
        return output

@total_ordering
class Device(object):

    pretty_template = """
     {attr:<25} {value}"""

    report_fields = [
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
    ]
    pretty_report_sys_fields = [
        'human_readable_size',
        'model',
        'removable',
        'ro',
        'rotational',
        'sas_address',
        'scheduler_mode',
        'vendor',
    ]

    def __init__(self, path):
        self.path = path
        # LVs can have a vg/lv path, while disks will have /dev/sda
        self.abspath = path
        self.lv_api = None
        self.lvs = []
        self.vg_name = None
        self.lv_name = None
        self.pvs_api = []
        self.disk_api = {}
        self.blkid_api = {}
        self.sys_api = {}
        self._exists = None
        self._is_lvm_member = None
        self._parse()
        self.available, self.rejected_reasons = self._check_reject_reasons()
        self.device_id = self._get_device_id()

    def __lt__(self, other):
        '''
        Implementing this method and __eq__ allows the @total_ordering
        decorator to turn the Device class into a totally ordered type.
        This can slower then implementing all comparison operations.
        This sorting should put available devices before unavailable devices
        and sort on the path otherwise (str sorting).
        '''
        if self.available == other.available:
            return self.path < other.path
        return self.available and not other.available

    def __eq__(self, other):
        return self.path == other.path

    def _parse(self):
        if not sys_info.devices:
            sys_info.devices = disk.get_devices()
        self.sys_api = sys_info.devices.get(self.abspath, {})
        if not self.sys_api:
            # if no device was found check if we are a partition
            partname = self.abspath.split('/')[-1]
            for device, info in sys_info.devices.items():
                part = info['partitions'].get(partname, {})
                if part:
                    self.sys_api = part
                    break

        # start with lvm since it can use an absolute or relative path
        lv = lvm.get_lv_from_argument(self.path)
        if lv:
            self.lv_api = lv
            self.lvs = [lv]
            self.abspath = lv.lv_path
            self.vg_name = lv.vg_name
            self.lv_name = lv.name
        else:
            dev = disk.lsblk(self.path)
            self.blkid_api = disk.blkid(self.path)
            self.disk_api = dev
            device_type = dev.get('TYPE', '')
            # always check is this is an lvm member
            if device_type in ['part', 'disk']:
                self._set_lvm_membership()

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

    def pretty_report(self):
        def format_value(v):
            if isinstance(v, list):
                return ', '.join(v)
            else:
                return v
        def format_key(k):
            return k.strip('_').replace('_', ' ')
        output = ['\n====== Device report {} ======\n'.format(self.path)]
        output.extend(
            [self.pretty_template.format(
                attr=format_key(k),
                value=format_value(v)) for k, v in vars(self).items() if k in
                self.report_fields and k != 'disk_api' and k != 'sys_api'] )
        output.extend(
            [self.pretty_template.format(
                attr=format_key(k),
                value=format_value(v)) for k, v in self.sys_api.items() if k in
                self.pretty_report_sys_fields])
        for lv in self.lvs:
            output.append("""
    --- Logical Volume ---""")
            output.extend(
                [self.pretty_template.format(
                    attr=format_key(k),
                    value=format_value(v)) for k, v in lv.report().items()])
        return ''.join(output)

    def report(self):
        return report_template.format(
            dev=self.abspath,
            size=self.size_human,
            rot=self.rotational,
            available=self.available,
            model=self.model,
        )

    def json_report(self):
        output = {k.strip('_'): v for k, v in vars(self).items() if k in
                  self.report_fields}
        output['lvs'] = [lv.report() for lv in self.lvs]
        return output

    def _get_device_id(self):
        """
        Please keep this implementation in sync with get_device_id() in
        src/common/blkdev.cc
        """
        props = ['ID_VENDOR','ID_MODEL','ID_SERIAL_SHORT', 'ID_SERIAL',
                 'ID_SCSI_SERIAL']
        p = disk.udevadm_property(self.abspath, props)
        if 'ID_VENDOR' in p and 'ID_MODEL' in p and 'ID_SCSI_SERIAL' in p:
            dev_id = '_'.join([p['ID_VENDOR'], p['ID_MODEL'],
                              p['ID_SCSI_SERIAL']])
        elif 'ID_MODEL' in p and 'ID_SERIAL_SHORT' in p:
            dev_id = '_'.join([p['ID_MODEL'], p['ID_SERIAL_SHORT']])
        elif 'ID_SERIAL' in p:
            dev_id = p['ID_SERIAL']
            if dev_id.startswith('MTFD'):
                # Micron NVMes hide the vendor
                dev_id = 'Micron_' + dev_id
        else:
            # the else branch should fallback to using sysfs and ioctl to
            # retrieve device_id on FreeBSD. Still figuring out if/how the
            # python ioctl implementation does that on FreeBSD
            dev_id = ''
        dev_id.replace(' ', '_')
        return dev_id

    def _set_lvm_membership(self):
        if self._is_lvm_member is None:
            # this is contentious, if a PV is recognized by LVM but has no
            # VGs, should we consider it as part of LVM? We choose not to
            # here, because most likely, we need to use VGs from this PV.
            self._is_lvm_member = False
            for path in self._get_pv_paths():
                # check if there was a pv created with the
                # name of device
                pvs = lvm.PVolumes()
                pvs.filter(pv_name=path)
                has_vgs = [pv.vg_name for pv in pvs if pv.vg_name]
                if has_vgs:
                    self.vgs = list(set(has_vgs))
                    # a pv can only be in one vg, so this should be safe
                    self.vg_name = has_vgs[0]
                    self._is_lvm_member = True
                    self.pvs_api = pvs
                    for pv in pvs:
                        if pv.vg_name and pv.lv_uuid:
                            lv = lvm.get_lv(vg_name=pv.vg_name, lv_uuid=pv.lv_uuid)
                            if lv:
                                self.lvs.append(lv)
                else:
                    self.vgs = []
        return self._is_lvm_member

    def _get_pv_paths(self):
        """
        For block devices LVM can reside on the raw block device or on a
        partition. Return a list of paths to be checked for a pv.
        """
        paths = [self.abspath]
        path_dir = os.path.dirname(self.abspath)
        for part in self.sys_api.get('partitions', {}).keys():
            paths.append(os.path.join(path_dir, part))
        return paths

    @property
    def exists(self):
        return os.path.exists(self.abspath)

    @property
    def has_gpt_headers(self):
        return self.blkid_api.get("PTTYPE") == "gpt"

    @property
    def rotational(self):
        return self.sys_api['rotational'] == '1'

    @property
    def model(self):
        return self.sys_api['model']

    @property
    def size_human(self):
        return self.sys_api['human_readable_size']

    @property
    def size(self):
        return self.sys_api['size']

    @property
    def lvm_size(self):
        """
        If this device was made into a PV it would lose 1GB in total size
        due to the 1GB physical extent size we set when creating volume groups
        """
        size = disk.Size(b=self.size)
        lvm_size = disk.Size(gb=size.gb.as_int()) - disk.Size(gb=1)
        return lvm_size

    @property
    def is_lvm_member(self):
        if self._is_lvm_member is None:
            self._set_lvm_membership()
        return self._is_lvm_member

    @property
    def is_ceph_disk_member(self):
        is_member = self.ceph_disk.is_member
        if self.sys_api.get("partitions"):
            for part in self.sys_api.get("partitions").keys():
                part = Device("/dev/%s" % part)
                if part.is_ceph_disk_member:
                    is_member = True
                    break
        return is_member

    @property
    def is_mapper(self):
        return self.path.startswith(('/dev/mapper', '/dev/dm-'))

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
            is_device = self.disk_api['TYPE'] == 'device'
            is_disk = self.disk_api['TYPE'] == 'disk'
            if is_device or is_disk:
                return True
        return False

    @property
    def is_encrypted(self):
        """
        Only correct for LVs, device mappers, and partitions. Will report a ``None``
        for raw devices.
        """
        crypt_reports = [self.blkid_api.get('TYPE', ''), self.disk_api.get('FSTYPE', '')]
        if self.is_lv:
            # if disk APIs are reporting this is encrypted use that:
            if 'crypto_LUKS' in crypt_reports:
                return True
            # if ceph-volume created this, then a tag would let us know
            elif self.lv_api.encrypted:
                return True
            return False
        elif self.is_partition:
            return 'crypto_LUKS' in crypt_reports
        elif self.is_mapper:
            active_mapper = encryption_status(self.abspath)
            if active_mapper:
                # normalize a bit to ensure same values regardless of source
                encryption_type = active_mapper['type'].lower().strip('12')  # turn LUKS1 or LUKS2 into luks
                return True if encryption_type in ['plain', 'luks'] else False
            else:
                return False
        else:
            return None

    @property
    def used_by_ceph(self):
        # only filter out data devices as journals could potentially be reused
        osd_ids = [lv.tags.get("ceph.osd_id") is not None for lv in self.lvs
                   if lv.tags.get("ceph.type") in ["data", "block"]]
        return any(osd_ids)

    def _check_reject_reasons(self):
        """
        This checks a number of potential reject reasons for a drive and
        returns a tuple (boolean, list). The first element denotes whether a
        drive is available or not, the second element lists reasons in case a
        drive is not available.
        """
        reasons = [
            ('removable', 1, 'removable'),
            ('ro', 1, 'read-only'),
            ('locked', 1, 'locked'),
        ]
        rejected = [reason for (k, v, reason) in reasons if
                    self.sys_api.get(k, '') == v]
        if self.is_ceph_disk_member:
            rejected.append("Used by ceph-disk")

        return len(rejected) == 0, rejected


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
