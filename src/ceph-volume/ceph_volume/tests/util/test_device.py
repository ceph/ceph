import pytest
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

    def test_is_ceph_disk_member_lsblk(self, device_info):
        lsblk = {"PARTLABEL": "ceph data"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member

    def test_is_not_ceph_disk_member_lsblk(self, device_info):
        lsblk = {"PARTLABEL": "gluster partition"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member is False

    def test_is_ceph_disk_member_blkid(self, device_info):
        # falls back to blkid
        lsblk = {"PARTLABEL": ""}
        blkid = {"PARTLABEL": "ceph data"}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member

    def test_is_not_ceph_disk_member_blkid(self, device_info):
        # falls back to blkid
        lsblk = {"PARTLABEL": ""}
        blkid = {"PARTLABEL": "gluster partition"}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member is False

    def test_pv_api(self, device_info, pvolumes, monkeypatch):
        FooPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000", pv_tags={}, vg_name="vg")
        pvolumes.append(FooPVolume)
        monkeypatch.setattr(api, 'PVolumes', lambda: pvolumes)
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.pvs_api


ceph_partlabels = [
    'ceph data', 'ceph journal', 'ceph block',
    'ceph block.wal', 'ceph block.db', 'ceph lockbox'
]


class TestCephDiskDevice(object):

    def test_partlabel_lsblk(self, device_info):
        lsblk = {"PARTLABEL": ""}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.partlabel == ''

    def test_partlabel_blkid(self, device_info):
        lsblk = {"PARTLABEL": ""}
        blkid = {"PARTLABEL": "ceph data"}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.partlabel == 'ceph data'

    @pytest.mark.parametrize("label", ceph_partlabels)
    def test_is_member_blkid(self, device_info, label):
        lsblk = {"PARTLABEL": ""}
        blkid = {"PARTLABEL": label}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.is_member is True

    @pytest.mark.parametrize("label", ceph_partlabels)
    def test_is_member_lsblk(self, device_info, label):
        lsblk = {"PARTLABEL": label}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.is_member is True

    def test_unknown_type(self, device_info):
        lsblk = {"PARTLABEL": "gluster"}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type == 'unknown'

    @pytest.mark.parametrize("label", ceph_partlabels)
    def test_type_blkid(self, device_info, label):
        expected = label.split()[-1].split('.')[-1]
        lsblk = {"PARTLABEL": ""}
        blkid = {"PARTLABEL": label}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type == expected

    @pytest.mark.parametrize("label", ceph_partlabels)
    def test_type_lsblk(self, device_info, label):
        expected = label.split()[-1].split('.')[-1]
        lsblk = {"PARTLABEL": label}
        blkid = {"PARTLABEL": ''}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type == expected
