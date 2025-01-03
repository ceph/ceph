# type: ignore
import pytest
from .data_list import ceph_bluestore_tool_show_label_output
from mock.mock import patch, Mock
from ceph_volume.devices import raw

# Sample lsblk output is below that overviews the test scenario. (--json output for reader clarity)
#  - sda and all its children are used for the OS
#  - sdb is a bluestore OSD with phantom Atari partitions
#  - sdc is an empty disk
#  - sdd has 2 LVM device children
# > lsblk --paths --json
#   {
#      "blockdevices": [
#         {"name": "/dev/sda", "maj:min": "8:0", "rm": "0", "size": "128G", "ro": "0", "type": "disk", "mountpoint": null,
#            "children": [
#               {"name": "/dev/sda1", "maj:min": "8:1", "rm": "0", "size": "487M", "ro": "0", "type": "part", "mountpoint": null},
#               {"name": "/dev/sda2", "maj:min": "8:2", "rm": "0", "size": "1.9G", "ro": "0", "type": "part", "mountpoint": null},
#               {"name": "/dev/sda3", "maj:min": "8:3", "rm": "0", "size": "125.6G", "ro": "0", "type": "part", "mountpoint": "/etc/hosts"}
#            ]
#         },
#         {"name": "/dev/sdb", "maj:min": "8:16", "rm": "0", "size": "1T", "ro": "0", "type": "disk", "mountpoint": null,
#            "children": [
#               {"name": "/dev/sdb2", "maj:min": "8:18", "rm": "0", "size": "48G", "ro": "0", "type": "part", "mountpoint": null},
#               {"name": "/dev/sdb3", "maj:min": "8:19", "rm": "0", "size": "6M", "ro": "0", "type": "part", "mountpoint": null}
#            ]
#         },
#         {"name": "/dev/sdc", "maj:min": "8:32", "rm": "0", "size": "1T", "ro": "0", "type": "disk", "mountpoint": null},
#         {"name": "/dev/sdd", "maj:min": "8:48", "rm": "0", "size": "1T", "ro": "0", "type": "disk", "mountpoint": null,
#            "children": [
#               {"name": "/dev/mapper/ceph--osd--block--1", "maj:min": "253:0", "rm": "0", "size": "512G", "ro": "0", "type": "lvm", "mountpoint": null},
#               {"name": "/dev/mapper/ceph--osd--block--2", "maj:min": "253:1", "rm": "0", "size": "512G", "ro": "0", "type": "lvm", "mountpoint": null}
#            ]
#         }
#      ]
#   }

def _devices_side_effect():
    return {
        "/dev/sda": {},
        "/dev/sda1": {},
        "/dev/sda2": {},
        "/dev/sda3": {},
        "/dev/sdb": {},
        "/dev/sdb2": {},
        "/dev/sdb3": {},
        "/dev/sdc": {},
        "/dev/sdd": {},
        "/dev/sde": {},
        "/dev/sde1": {},
        "/dev/mapper/ceph--osd--block--1": {},
        "/dev/mapper/ceph--osd--block--2": {},
    }

def _lsblk_all_devices(abspath=True):
    return [
        {"NAME": "/dev/sda", "KNAME": "/dev/sda", "PKNAME": "", "TYPE": "disk"},
        {"NAME": "/dev/sda1", "KNAME": "/dev/sda1", "PKNAME": "/dev/sda", "TYPE": "part"},
        {"NAME": "/dev/sda2", "KNAME": "/dev/sda2", "PKNAME": "/dev/sda", "TYPE": "part"},
        {"NAME": "/dev/sda3", "KNAME": "/dev/sda3", "PKNAME": "/dev/sda", "TYPE": "part"},
        {"NAME": "/dev/sdb", "KNAME": "/dev/sdb", "PKNAME": "", "TYPE": "disk"},
        {"NAME": "/dev/sdb2", "KNAME": "/dev/sdb2", "PKNAME": "/dev/sdb", "TYPE": "part"},
        {"NAME": "/dev/sdb3", "KNAME": "/dev/sdb3", "PKNAME": "/dev/sdb", "TYPE": "part"},
        {"NAME": "/dev/sdc", "KNAME": "/dev/sdc", "PKNAME": "", "TYPE": "disk"},
        {"NAME": "/dev/sdd", "KNAME": "/dev/sdd", "PKNAME": "", "TYPE": "disk"},
        {"NAME": "/dev/sde", "KNAME": "/dev/sde", "PKNAME": "", "TYPE": "disk"},
        {"NAME": "/dev/sde1", "KNAME": "/dev/sde1", "PKNAME": "/dev/sde", "TYPE": "part"},
        {"NAME": "/dev/mapper/ceph--osd--block--1", "KNAME": "/dev/mapper/ceph--osd--block--1", "PKNAME": "/dev/sdd", "TYPE": "lvm"},
        {"NAME": "/dev/mapper/ceph--osd--block--2", "KNAME": "/dev/mapper/ceph--osd--block--2", "PKNAME": "/dev/sdd", "TYPE": "lvm"},
    ]

# dummy lsblk output for device with optional parent output
def _lsblk_output(dev, parent=None):
    if parent is None:
        parent = ""
    ret = 'NAME="{}" KNAME="{}" PKNAME="{}"'.format(dev, dev, parent)
    return [ret] # needs to be in a list form

def _process_call_side_effect(command, **kw):
    if "lsblk" in command:
        if "/dev/" in command[-1]:
            dev = command[-1]
            if dev == "/dev/sda1" or dev == "/dev/sda2" or dev == "/dev/sda3":
                return _lsblk_output(dev, parent="/dev/sda"), '', 0
            if dev == "/dev/sdb2" or dev == "/dev/sdb3":
                return _lsblk_output(dev, parent="/dev/sdb"), '', 0
            if dev == "/dev/sda" or dev == "/dev/sdb" or dev == "/dev/sdc" or dev == "/dev/sdd":
                return _lsblk_output(dev), '', 0
            if dev == "/dev/sde1":
                return _lsblk_output(dev, parent="/dev/sde"), '', 0
            if "mapper" in dev:
                return _lsblk_output(dev, parent="/dev/sdd"), '', 0
            pytest.fail('dev {} needs behavior specified for it'.format(dev))
        if "/dev/" not in command:
            return _lsblk_all_devices(), '', 0
        pytest.fail('command {} needs behavior specified for it'.format(command))

    if "ceph-bluestore-tool" in command:
        return ceph_bluestore_tool_show_label_output, '', 0
    pytest.fail('command {} needs behavior specified for it'.format(command))

def _has_bluestore_label_side_effect(disk_path):
    if "/dev/sda" in disk_path:
        return False # disk and all children are for the OS
    if disk_path == "/dev/sdb":
        return True # sdb is a valid bluestore OSD
    if disk_path == "/dev/sdb2":
        return True # sdb2 appears to be a valid bluestore OSD even though it should not be
    if disk_path == "/dev/sdc":
        return False # empty disk
    if disk_path == "/dev/sdd":
        return False # has LVM subdevices
    if disk_path == "/dev/sde":
        return False # has partitions, it means it shouldn't be an OSD
    if disk_path == "/dev/sde1":
        return True # is a valid OSD
    if disk_path == "/dev/mapper/ceph--osd--block--1":
        return True # good OSD
    if disk_path == "/dev/mapper/ceph--osd--block--2":
        return False # corrupted
    pytest.fail('device {} needs behavior specified for it'.format(disk_path))

class TestList(object):

    @patch('ceph_volume.devices.raw.list.List.exclude_lvm_osd_devices', Mock())
    @patch('ceph_volume.util.device.disk.get_devices')
    @patch('ceph_volume.util.disk.has_bluestore_label')
    @patch('ceph_volume.process.call')
    @patch('ceph_volume.util.disk.lsblk_all')
    def test_raw_list(self, patched_disk_lsblk, patched_call, patched_bluestore_label, patched_get_devices):
        raw.list.logger.setLevel("DEBUG")
        patched_call.side_effect = _process_call_side_effect
        patched_disk_lsblk.side_effect = _lsblk_all_devices
        patched_bluestore_label.side_effect = _has_bluestore_label_side_effect
        patched_get_devices.side_effect = _devices_side_effect

        result = raw.list.List([]).generate()
        assert len(result) == 3

        sdb = result['sdb-uuid']
        assert sdb['osd_uuid'] == 'sdb-uuid'
        assert sdb['osd_id'] == 0
        assert sdb['device'] == '/dev/sdb'
        assert sdb['ceph_fsid'] == 'sdb-fsid'
        assert sdb['type'] == 'bluestore'
        lvm1 = result['lvm-1-uuid']
        assert lvm1['osd_uuid'] == 'lvm-1-uuid'
        assert lvm1['osd_id'] == 2
        assert lvm1['device'] == '/dev/mapper/ceph--osd--block--1'
        assert lvm1['ceph_fsid'] == 'lvm-1-fsid'
        assert lvm1['type'] == 'bluestore'
        sde1 = result['sde1-uuid']
        assert sde1['osd_uuid'] == 'sde1-uuid'
        assert sde1['osd_id'] == 1
        assert sde1['device'] == '/dev/sde1'
        assert sde1['ceph_fsid'] == 'sde1-fsid'
        assert sde1['type'] == 'bluestore'

    @patch('ceph_volume.devices.raw.list.List.exclude_lvm_osd_devices', Mock())
    @patch('ceph_volume.util.device.disk.get_devices')
    @patch('ceph_volume.util.disk.has_bluestore_label')
    @patch('ceph_volume.process.call')
    @patch('ceph_volume.util.disk.lsblk_all')
    def test_raw_list_with_OSError(self, patched_disk_lsblk, patched_call, patched_bluestore_label, patched_get_devices):
        def _has_bluestore_label_side_effect_with_OSError(device_path):
            if device_path == "/dev/sdd":
                raise OSError('fake OSError')
            return _has_bluestore_label_side_effect(device_path)

        raw.list.logger.setLevel("DEBUG")
        patched_disk_lsblk.side_effect = _lsblk_all_devices
        patched_call.side_effect = _process_call_side_effect
        patched_bluestore_label.side_effect = _has_bluestore_label_side_effect_with_OSError
        patched_get_devices.side_effect = _devices_side_effect

        result = raw.list.List([]).generate()
        assert len(result) == 2
        assert {'sdb-uuid', 'sde1-uuid'} == set(result.keys())
