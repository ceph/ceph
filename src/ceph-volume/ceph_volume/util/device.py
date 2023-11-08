# -*- coding: utf-8 -*-

import logging
import os
from functools import total_ordering
from ceph_volume import sys_info
from ceph_volume.api import lvm
from ceph_volume.util import disk, system
from ceph_volume.util.lsmdisk import LSMDisk
from ceph_volume.util.constants import ceph_disk_guids
from ceph_volume.util.disk import allow_loop_devices


logger = logging.getLogger(__name__)


report_template = """
{dev:<25} {size:<12} {device_nodes:<15} {rot!s:<7} {available!s:<9} {model}"""


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

    def __init__(self,
                 filter_for_batch=False,
                 with_lsm=False,
                 list_all=False):
        lvs = lvm.get_lvs()
        lsblk_all = disk.lsblk_all()
        all_devices_vgs = lvm.get_all_devices_vgs()
        if not sys_info.devices:
            sys_info.devices = disk.get_devices()
        self._devices = [Device(k,
                                with_lsm,
                                lvs=lvs,
                                lsblk_all=lsblk_all,
                                all_devices_vgs=all_devices_vgs) for k in
                         sys_info.devices.keys()]
        self.devices = []
        for device in self._devices:
            if filter_for_batch and not device.available_lvm_batch:
                continue
            if device.is_lv and not list_all:
                continue
            if device.is_partition and not list_all:
                continue
            self.devices.append(device)

    def pretty_report(self):
        output = [
            report_template.format(
                dev='Device Path',
                size='Size',
                rot='rotates',
                model='Model name',
                available='available',
                device_nodes='Device nodes',

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
        'ceph_device',
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
        'device_id',
        'lsm_data',
    ]
    pretty_report_sys_fields = [
        'actuators',
        'human_readable_size',
        'model',
        'removable',
        'ro',
        'rotational',
        'sas_address',
        'scheduler_mode',
        'vendor',
    ]

    # define some class variables; mostly to enable the use of autospec in
    # unittests
    lvs = []

    def __init__(self, path, with_lsm=False, lvs=None, lsblk_all=None, all_devices_vgs=None):
        self.path = path
        # LVs can have a vg/lv path, while disks will have /dev/sda
        self.symlink = None
        # check if we are a symlink
        if os.path.islink(self.path):
            self.symlink = self.path
            real_path = os.path.realpath(self.path)
            # check if we are not a device mapper
            if "dm-" not in real_path:
                self.path = real_path
        if not sys_info.devices.get(self.path):
            sys_info.devices = disk.get_devices()
        self.sys_api = sys_info.devices.get(self.path, {})
        self.partitions = self._get_partitions()
        self.lv_api = None
        self.lvs = [] if not lvs else lvs
        self.lsblk_all = lsblk_all
        self.all_devices_vgs = all_devices_vgs
        self.vgs = []
        self.vg_name = None
        self.lv_name = None
        self.disk_api = {}
        self.blkid_api = None
        self._exists = None
        self._is_lvm_member = None
        self.ceph_device = False
        self._parse()
        self.device_nodes = sys_info.devices[self.path]['device_nodes']
        self.lsm_data = self.fetch_lsm(with_lsm)

        self.available_lvm, self.rejected_reasons_lvm = self._check_lvm_reject_reasons()
        self.available_raw, self.rejected_reasons_raw = self._check_raw_reject_reasons()
        self.available = self.available_lvm and self.available_raw
        self.rejected_reasons = list(set(self.rejected_reasons_lvm +
                                         self.rejected_reasons_raw))

        self.device_id = self._get_device_id()

    def fetch_lsm(self, with_lsm):
        '''
        Attempt to fetch libstoragemgmt (LSM) metadata, and return to the caller
        as a dict. An empty dict is passed back to the caller if the target path
        is not a block device, or lsm is unavailable on the host. Otherwise the
        json returned will provide LSM attributes, and any associated errors that
        lsm encountered when probing the device.
        '''
        if not with_lsm or not self.exists or not self.is_device:
            return {}

        lsm_disk = LSMDisk(self.path)

        return  lsm_disk.json_report()

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

    def __hash__(self):
        return hash(self.path)

    def load_blkid_api(self):
        if self.blkid_api is None:
            self.blkid_api = disk.blkid(self.path)

    def _parse(self):
        lv = None
        if not self.sys_api:
            # if no device was found check if we are a partition
            partname = self.path.split('/')[-1]
            for device, info in sys_info.devices.items():
                part = info['partitions'].get(partname, {})
                if part:
                    self.sys_api = part
                    break

        if self.lvs:
            for _lv in self.lvs:
                # if the path is not absolute, we have 'vg/lv', let's use LV name
                # to get the LV.
                if self.path[0] == '/':
                    if _lv.lv_path == self.path:
                        lv = _lv
                        break
                else:
                    vgname, lvname = self.path.split('/')
                    if _lv.lv_name == lvname and _lv.vg_name == vgname:
                        lv = _lv
                        break
        else:
            if self.path[0] == '/':
                lv = lvm.get_single_lv(filters={'lv_path': self.path})
            else:
                vgname, lvname = self.path.split('/')
                lv = lvm.get_single_lv(filters={'lv_name': lvname,
                                                'vg_name': vgname})

        if lv:
            self.lv_api = lv
            self.lvs = [lv]
            self.path = lv.lv_path
            self.vg_name = lv.vg_name
            self.lv_name = lv.name
            self.ceph_device = lvm.is_ceph_device(lv)
        else:
            self.lvs = []
            if self.lsblk_all:
                for dev in self.lsblk_all:
                    if dev['NAME'] == os.path.basename(self.path):
                        break
            else:
                dev = disk.lsblk(self.path)
            self.disk_api = dev
            device_type = dev.get('TYPE', '')
            # always check is this is an lvm member
            valid_types = ['part', 'disk', 'mpath']
            if allow_loop_devices():
                valid_types.append('loop')
            if device_type in valid_types:
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
        return '<%s: %s>' % (prefix, self.path)

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
            dev=self.path,
            size=self.size_human,
            rot=self.rotational,
            available=self.available,
            model=self.model,
            device_nodes=self.device_nodes
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
        props = ['ID_VENDOR', 'ID_MODEL', 'ID_MODEL_ENC', 'ID_SERIAL_SHORT', 'ID_SERIAL',
                 'ID_SCSI_SERIAL']
        p = disk.udevadm_property(self.path, props)
        if p.get('ID_MODEL','').startswith('LVM PV '):
            p['ID_MODEL'] = p.get('ID_MODEL_ENC', '').replace('\\x20', ' ').strip()
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
        dev_id = dev_id.replace(' ', '_')
        while '__' in dev_id:
            dev_id = dev_id.replace('__', '_')
        return dev_id

    def _set_lvm_membership(self):
        if self._is_lvm_member is None:
            # this is contentious, if a PV is recognized by LVM but has no
            # VGs, should we consider it as part of LVM? We choose not to
            # here, because most likely, we need to use VGs from this PV.
            self._is_lvm_member = False
            device_to_check = [self.path]
            device_to_check.extend(self.partitions)

            # a pv can only be in one vg, so this should be safe
            # FIXME: While the above assumption holds, sda1 and sda2
            # can each host a PV and VG. I think the vg_name property is
            # actually unused (not 100% sure) and can simply be removed
            vgs = None
            if not self.all_devices_vgs:
                self.all_devices_vgs = lvm.get_all_devices_vgs()
            for path in device_to_check:
                for dev_vg in self.all_devices_vgs:
                    if dev_vg.pv_name == path:
                        vgs = [dev_vg]
                if vgs:
                    self.vgs.extend(vgs)
                    self.vg_name = vgs[0]
                    self._is_lvm_member = True
                    self.lvs.extend(lvm.get_device_lvs(path))
                if self.lvs:
                    self.ceph_device = any([True if lv.tags.get('ceph.osd_id') else False for lv in self.lvs])

    def _get_partitions(self):
        """
        For block devices LVM can reside on the raw block device or on a
        partition. Return a list of paths to be checked for a pv.
        """
        partitions = []
        path_dir = os.path.dirname(self.path)
        for partition in self.sys_api.get('partitions', {}).keys():
            partitions.append(os.path.join(path_dir, partition))
        return partitions

    @property
    def exists(self):
        return os.path.exists(self.path)

    @property
    def has_fs(self):
        self.load_blkid_api()
        return 'TYPE' in self.blkid_api

    @property
    def has_gpt_headers(self):
        self.load_blkid_api()
        return self.blkid_api.get("PTTYPE") == "gpt"

    @property
    def rotational(self):
        rotational = self.sys_api.get('rotational')
        if rotational is None:
            # fall back to lsblk if not found in sys_api
            # default to '1' if no value is found with lsblk either
            rotational = self.disk_api.get('ROTA', '1')
        return rotational == '1'

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
    def parent_device(self):
        if 'PKNAME' in self.disk_api:
            return '/dev/%s' % self.disk_api['PKNAME']
        return None

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
        def is_member(device):
            return 'ceph' in device.get('PARTLABEL', '') or \
                device.get('PARTTYPE', '') in ceph_disk_guids.keys()
        # If we come from Devices(), self.lsblk_all is set already.
        # Otherwise, we have to grab the data.
        details = self.lsblk_all or disk.lsblk_all()
        _is_member = False
        if self.sys_api.get("partitions"):
            for part in self.sys_api.get("partitions").keys():
                for dev in details:
                    if part.startswith(dev['NAME']):
                        if is_member(dev):
                            _is_member = True
                return _is_member
        else:
            return is_member(self.disk_api)
        raise RuntimeError(f"Couln't check if device {self.path} is a ceph-disk member.")

    @property
    def has_bluestore_label(self):
        return disk.has_bluestore_label(self.path)

    @property
    def is_mapper(self):
        return self.path.startswith(('/dev/mapper', '/dev/dm-'))

    @property
    def device_type(self):
        self.load_blkid_api()
        if 'type' in self.sys_api:
            return self.sys_api['type']
        elif self.disk_api:
            return self.disk_api['TYPE']
        elif self.blkid_api:
            return self.blkid_api['TYPE']

    @property
    def is_mpath(self):
        return self.device_type == 'mpath'

    @property
    def is_lv(self):
        return self.lv_api is not None

    @property
    def is_partition(self):
        self.load_blkid_api()
        if self.disk_api:
            return self.disk_api['TYPE'] == 'part'
        elif self.blkid_api:
            return self.blkid_api['TYPE'] == 'part'
        return False

    @property
    def is_device(self):
        self.load_blkid_api()
        api = None
        if self.disk_api:
            api = self.disk_api
        elif self.blkid_api:
            api = self.blkid_api
        if api:
            valid_types = ['disk', 'device', 'mpath']
            if allow_loop_devices():
                valid_types.append('loop')
            return self.device_type in valid_types
        return False

    @property
    def is_acceptable_device(self):
        return self.is_device or self.is_partition or self.is_lv

    @property
    def is_encrypted(self):
        """
        Only correct for LVs, device mappers, and partitions. Will report a ``None``
        for raw devices.
        """
        self.load_blkid_api()
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
            active_mapper = encryption_status(self.path)
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

    @property
    def journal_used_by_ceph(self):
        # similar to used_by_ceph() above. This is for 'journal' devices (db/wal/..)
        # needed by get_lvm_fast_allocs() in devices/lvm/batch.py
        # see https://tracker.ceph.com/issues/59640
        osd_ids = [lv.tags.get("ceph.osd_id") is not None for lv in self.lvs
                   if lv.tags.get("ceph.type") in ["db", "wal"]]
        return any(osd_ids)

    @property
    def vg_free_percent(self):
        if self.vgs:
            return [vg.free_percent for vg in self.vgs]
        else:
            return [1]

    @property
    def vg_size(self):
        if self.vgs:
            return [vg.size for vg in self.vgs]
        else:
            # TODO fix this...we can probably get rid of vg_free
            return self.vg_free

    @property
    def vg_free(self):
        '''
        Returns the free space in all VGs on this device. If no VGs are
        present, returns the disk size.
        '''
        if self.vgs:
            return [vg.free for vg in self.vgs]
        else:
            # We could also query 'lvmconfig
            # --typeconfig full' and use allocations -> physical_extent_size
            # value to project the space for a vg
            # assuming 4M extents here
            extent_size = 4194304
            vg_free = int(self.size / extent_size) * extent_size
            if self.size % extent_size == 0:
                # If the extent size divides size exactly, deduct on extent for
                # LVM metadata
                vg_free -= extent_size
            return [vg_free]

    @property
    def has_partitions(self):
        '''
        Boolean to determine if a given device has partitions.
        '''
        if self.sys_api.get('partitions'):
            return True
        return False

    def _check_generic_reject_reasons(self):
        reasons = [
            ('id_bus', 'usb', 'id_bus'),
            ('ro', '1', 'read-only'),
        ]
        rejected = [reason for (k, v, reason) in reasons if
                    self.sys_api.get(k, '') == v]
        if self.is_acceptable_device:
            # reject disks smaller than 5GB
            if int(self.sys_api.get('size', 0)) < 5368709120:
                rejected.append('Insufficient space (<5GB)')
        else:
            rejected.append("Device type is not acceptable. It should be raw device or partition")
        if self.is_ceph_disk_member:
            rejected.append("Used by ceph-disk")

        try:
            if self.has_bluestore_label:
                rejected.append('Has BlueStore device label')
        except OSError as e:
            # likely failed to open the device. assuming it is BlueStore is the safest option
            # so that a possibly-already-existing OSD doesn't get overwritten
            logger.error('failed to determine if device {} is BlueStore. device should not be used to avoid false negatives. err: {}'.format(self.path, e))
            rejected.append('Failed to determine if device is BlueStore')

        if self.is_partition:
            try:
                if disk.has_bluestore_label(self.parent_device):
                    rejected.append('Parent has BlueStore device label')
            except OSError as e:
                # likely failed to open the device. assuming the parent is BlueStore is the safest
                # option so that a possibly-already-existing OSD doesn't get overwritten
                logger.error('failed to determine if partition {} (parent: {}) has a BlueStore parent. partition should not be used to avoid false negatives. err: {}'.format(self.path, self.parent_device, e))
                rejected.append('Failed to determine if parent device is BlueStore')

        if self.has_gpt_headers:
            rejected.append('Has GPT headers')
        if self.has_partitions:
            rejected.append('Has partitions')
        if self.has_fs:
            rejected.append('Has a FileSystem')
        return rejected

    def _check_lvm_reject_reasons(self):
        rejected = []
        if self.vgs:
            available_vgs = [vg for vg in self.vgs if int(vg.vg_free_count) > 10]
            if not available_vgs:
                rejected.append('Insufficient space (<10 extents) on vgs')
        else:
            # only check generic if no vgs are present. Vgs might hold lvs and
            # that might cause 'locked' to trigger
            rejected.extend(self._check_generic_reject_reasons())

        return len(rejected) == 0, rejected

    def _check_raw_reject_reasons(self):
        rejected = self._check_generic_reject_reasons()
        if len(self.vgs) > 0:
            rejected.append('LVM detected')

        return len(rejected) == 0, rejected

    @property
    def available_lvm_batch(self):
        if self.sys_api.get("partitions"):
            return False
        if system.device_is_mounted(self.path):
            return False
        return self.is_device or self.is_lv


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
    def parttype(self):
        """
        Seems like older version do not detect PARTTYPE correctly (assuming the
        info in util/disk.py#lsblk is still valid).
        SImply resolve to using blkid since lsblk will throw an error if asked
        for an unknown columns
        """
        return self.device.blkid_api.get('PARTTYPE', '')

    @property
    def is_member(self):
        if self._is_ceph_disk_member is None:
            if 'ceph' in self.partlabel:
                self._is_ceph_disk_member = True
                return True
            elif self.parttype in ceph_disk_guids.keys():
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
        label = ceph_disk_guids.get(self.parttype, {})
        return label.get('type', 'unknown').split('.')[-1]
