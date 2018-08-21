from ceph_volume.util import device
from ceph_volume.api import lvm as api


class TestDevice(object):

    def test_sys_api(self, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        device_info(devices=data)
        disk = device.Device("/dev/sda")
        assert disk.sys_api
        assert "foo" in disk.sys_api

    def test_is_lv(self, device_info):
        data = {"lv_path": "vg/lv"}
        device_info(lv=data)
        disk = device.Device("vg/lv")
        assert disk.is_lv

    def test_is_device(self, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "device"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_device

    def test_is_partition(self, device_info, pvolumes):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_partition

    def test_is_not_lvm_memeber(self, device_info, pvolumes):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.is_lvm_member

    def test_is_lvm_memeber(self, device_info, pvolumes):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.is_lvm_member

    def test_is_mapper_device(self, device_info):
        device_info()
        disk = device.Device("/dev/mapper/foo")
        assert disk.is_mapper

    def test_is_not_mapper_device(self, device_info):
        device_info()
        disk = device.Device("/dev/sda")
        assert not disk.is_mapper

    def test_pv_api(self, device_info, pvolumes, monkeypatch):
        FooPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000", pv_tags={}, vg_name="vg")
        pvolumes.append(FooPVolume)
        monkeypatch.setattr(api, 'PVolumes', lambda: pvolumes)
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.pvs_api
