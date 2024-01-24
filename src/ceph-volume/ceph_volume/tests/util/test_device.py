import os
import pytest
from copy import deepcopy
from ceph_volume.util import device
from ceph_volume.api import lvm as api
from mock.mock import patch, mock_open


class TestDevice(object):

    def test_sys_api(self, monkeypatch, device_info):
        volume = api.Volume(lv_name='lv', lv_uuid='y', vg_name='vg',
                            lv_tags={}, lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(volume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs:
                            deepcopy(volumes))

        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.sys_api
        assert "foo" in disk.sys_api

    def test_lvm_size(self, monkeypatch, device_info):
        volume = api.Volume(lv_name='lv', lv_uuid='y', vg_name='vg',
                            lv_tags={}, lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(volume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs:
                            deepcopy(volumes))

        # 5GB in size
        data = {"/dev/sda": {"size": "5368709120"}}
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.lvm_size.gb == 4

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_lvm_size_rounds_down(self, fake_call, device_info):
        # 5.5GB in size
        data = {"/dev/sda": {"size": "5905580032"}}
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.lvm_size.gb == 4

    def test_is_lv(self, fake_call, device_info):
        data = {"lv_path": "vg/lv", "vg_name": "vg", "name": "lv"}
        lsblk = {"TYPE": "lvm", "NAME": "vg-lv"}
        device_info(lv=data,lsblk=lsblk)
        disk = device.Device("vg/lv")
        assert disk.is_lv

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_vgs_is_empty(self, fake_call, device_info, monkeypatch):
        BarPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000",
                                 pv_tags={})
        pvolumes = []
        pvolumes.append(BarPVolume)
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        monkeypatch.setattr(api, 'get_pvs', lambda **kwargs: {})

        disk = device.Device("/dev/nvme0n1")
        assert disk.vgs == []

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_vgs_is_not_empty(self, fake_call, device_info, monkeypatch):
        vg = api.VolumeGroup(pv_name='/dev/nvme0n1', vg_name='foo/bar', vg_free_count=6,
                             vg_extent_size=1073741824)
        monkeypatch.setattr(api, 'get_all_devices_vgs', lambda : [vg])
        lsblk = {"TYPE": "disk", "NAME": "nvme0n1"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/nvme0n1")
        assert len(disk.vgs) == 1

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_device_is_device(self, fake_call, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "device", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_device is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_loop_device_is_not_device(self, fake_call, device_info):
        data = {"/dev/loop0": {"foo": "bar"}}
        lsblk = {"TYPE": "loop"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/loop0")
        assert disk.is_device is False

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_loop_device_is_device(self, fake_call, device_info):
        data = {"/dev/loop0": {"foo": "bar"}}
        lsblk = {"TYPE": "loop"}
        os.environ["CEPH_VOLUME_ALLOW_LOOP_DEVICES"] = "1"
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/loop0")
        assert disk.is_device is True
        del os.environ["CEPH_VOLUME_ALLOW_LOOP_DEVICES"]

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_device_is_rotational(self, fake_call, device_info):
        data = {"/dev/sda": {"rotational": "1"}}
        lsblk = {"TYPE": "device", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.rotational

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_device_is_not_rotational(self, fake_call, device_info):
        data = {"/dev/sda": {"rotational": "0"}}
        lsblk = {"TYPE": "device", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.rotational

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_device_is_rotational_lsblk(self, fake_call, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "device", "ROTA": "1", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.rotational

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_device_is_not_rotational_lsblk(self, fake_call, device_info):
        data = {"/dev/sda": {"rotational": "0"}}
        lsblk = {"TYPE": "device", "ROTA": "0", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.rotational

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_device_is_rotational_defaults_true(self, fake_call, device_info):
        # rotational will default true if no info from sys_api or lsblk is found
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "device", "foo": "bar", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.rotational

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_disk_is_device(self, fake_call, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_device is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_mpath_device_is_device(self, fake_call, device_info):
        data = {"/dev/foo": {"foo": "bar"}}
        lsblk = {"TYPE": "mpath", "NAME": "foo"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/foo")
        assert disk.is_device is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_not_lvm_member(self, fake_call, device_info):
        data = {"/dev/sda1": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "NAME": "sda1", "PKNAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda1")
        assert not disk.is_lvm_member

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_lvm_member(self, fake_call, device_info):
        data = {"/dev/sda1": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "NAME": "sda1", "PKNAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda1")
        assert not disk.is_lvm_member

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_mapper_device(self, fake_call, device_info):
        lsblk = {"TYPE": "lvm", "NAME": "foo"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/mapper/foo")
        assert disk.is_mapper

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_dm_is_mapper_device(self, fake_call, device_info):
        lsblk = {"TYPE": "lvm", "NAME": "dm-4"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/dm-4")
        assert disk.is_mapper

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_not_mapper_device(self, fake_call, device_info):
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.is_mapper

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_ceph_disk_lsblk(self, fake_call, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_ceph_disk_blkid(self, fake_call, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_ceph_disk_member_not_available_lsblk(self, fake_call, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member
        assert not disk.available
        assert "Used by ceph-disk" in disk.rejected_reasons

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_ceph_disk_member_not_available_blkid(self, fake_call, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member
        assert not disk.available
        assert "Used by ceph-disk" in disk.rejected_reasons

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_reject_removable_device(self, fake_call, device_info):
        data = {"/dev/sdb": {"removable": "1"}}
        lsblk = {"TYPE": "disk", "NAME": "sdb"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sdb")
        assert not disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_reject_device_with_gpt_headers(self, fake_call, device_info):
        data = {"/dev/sdb": {"removable": "0", "size": 5368709120}}
        lsblk = {"TYPE": "disk", "NAME": "sdb"}
        blkid= {"PTTYPE": "gpt"}
        device_info(
            devices=data,
            blkid=blkid,
            lsblk=lsblk,
        )
        disk = device.Device("/dev/sdb")
        assert not disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_accept_non_removable_device(self, fake_call, device_info):
        data = {"/dev/sdb": {"removable": "0", "size": 5368709120}}
        lsblk = {"TYPE": "disk", "NAME": "sdb"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sdb")
        assert disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_reject_not_acceptable_device(self, fake_call, device_info):
        data = {"/dev/dm-0": {"foo": "bar"}}
        lsblk = {"TYPE": "mpath", "NAME": "dm-0"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/dm-0")
        assert not disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    @patch('ceph_volume.util.device.os.path.realpath')
    @patch('ceph_volume.util.device.os.path.islink')
    def test_accept_symlink_to_device(self,
                                      m_os_path_islink,
                                      m_os_path_realpath,
                                      device_info,
                                      fake_call):
        m_os_path_islink.return_value = True
        m_os_path_realpath.return_value = '/dev/sdb'
        data = {"/dev/sdb": {"ro": "0", "size": 5368709120}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/test_symlink")
        print(disk)
        print(disk.sys_api)
        assert disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    @patch('ceph_volume.util.device.os.readlink')
    @patch('ceph_volume.util.device.os.path.islink')
    def test_reject_symlink_to_device_mapper(self,
                                             m_os_path_islink,
                                             m_os_readlink,
                                             device_info,
                                             fake_call):
        m_os_path_islink.return_value = True
        m_os_readlink.return_value = '/dev/dm-0'
        data = {"/dev/mapper/mpatha": {"ro": "0", "size": 5368709120}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/mapper/mpatha")
        assert disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_reject_readonly_device(self, fake_call, device_info):
        data = {"/dev/cdrom": {"ro": "1"}}
        lsblk = {"TYPE": "disk", "NAME": "cdrom"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/cdrom")
        assert not disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_reject_smaller_than_5gb(self, fake_call, device_info):
        data = {"/dev/sda": {"size": 5368709119}}
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.available, 'too small device is available'

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_accept_non_readonly_device(self, fake_call, device_info):
        data = {"/dev/sda": {"ro": "0", "size": 5368709120}}
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.available

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_reject_bluestore_device(self, fake_call, monkeypatch, patch_bluestore_label, device_info):
        patch_bluestore_label.return_value = True
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.available
        assert "Has BlueStore device label" in disk.rejected_reasons

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_reject_device_with_oserror(self, fake_call, monkeypatch, patch_bluestore_label, device_info):
        patch_bluestore_label.side_effect = OSError('test failure')
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.available
        assert "Failed to determine if device is BlueStore" in disk.rejected_reasons

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "device_info_not_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_not_ceph_disk_member_lsblk(self, fake_call, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member is False

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_existing_vg_available(self, fake_call, monkeypatch, device_info):
        vg = api.VolumeGroup(pv_name='/dev/nvme0n1', vg_name='foo/bar', vg_free_count=1536,
                             vg_extent_size=4194304)
        monkeypatch.setattr(api, 'get_all_devices_vgs', lambda : [vg])
        lsblk = {"TYPE": "disk", "NAME": "nvme0n1"}
        data = {"/dev/nvme0n1": {"size": "6442450944"}}
        lv = {"tags": {"ceph.osd_id": "1"}}
        device_info(devices=data, lsblk=lsblk, lv=lv)
        disk = device.Device("/dev/nvme0n1")
        assert disk.available_lvm
        assert not disk.available
        assert not disk.available_raw

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_existing_vg_too_small(self, fake_call, monkeypatch, device_info):
        vg = api.VolumeGroup(pv_name='/dev/nvme0n1', vg_name='foo/bar', vg_free_count=4,
                             vg_extent_size=1073741824)
        monkeypatch.setattr(api, 'get_all_devices_vgs', lambda : [vg])
        lsblk = {"TYPE": "disk", "NAME": "nvme0n1"}
        data = {"/dev/nvme0n1": {"size": "6442450944"}}
        lv = {"tags": {"ceph.osd_id": "1"}}
        device_info(devices=data, lsblk=lsblk, lv=lv)
        disk = device.Device("/dev/nvme0n1")
        assert not disk.available_lvm
        assert not disk.available
        assert not disk.available_raw

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_multiple_existing_vgs(self, fake_call, monkeypatch, device_info):
        vg1 = api.VolumeGroup(pv_name='/dev/nvme0n1', vg_name='foo/bar', vg_free_count=1000,
                             vg_extent_size=4194304)
        vg2 = api.VolumeGroup(pv_name='/dev/nvme0n1', vg_name='foo/bar', vg_free_count=536,
                             vg_extent_size=4194304)
        monkeypatch.setattr(api, 'get_all_devices_vgs', lambda : [vg1, vg2])
        lsblk = {"TYPE": "disk", "NAME": "nvme0n1"}
        data = {"/dev/nvme0n1": {"size": "6442450944"}}
        lv = {"tags": {"ceph.osd_id": "1"}}
        device_info(devices=data, lsblk=lsblk, lv=lv)
        disk = device.Device("/dev/nvme0n1")
        assert disk.available_lvm
        assert not disk.available
        assert not disk.available_raw

    @pytest.mark.parametrize("ceph_type", ["data", "block"])
    def test_used_by_ceph(self, fake_call, device_info,
                          monkeypatch, ceph_type):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "NAME": "sda", "PKNAME": "sda"}
        FooPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000",
                                 lv_uuid="0000", pv_tags={}, vg_name="vg")
        pvolumes = []
        pvolumes.append(FooPVolume)
        lv_data = {"lv_name": "lv", "lv_path": "vg/lv", "vg_name": "vg",
                   "lv_uuid": "0000", "lv_tags":
                   "ceph.osd_id=0,ceph.type="+ceph_type}
        volumes = []
        lv = api.Volume(**lv_data)
        volumes.append(lv)
        monkeypatch.setattr(api, 'get_pvs', lambda **kwargs: pvolumes)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs:
                            deepcopy(volumes))

        device_info(devices=data, lsblk=lsblk, lv=lv_data)
        vg = api.VolumeGroup(vg_name='foo/bar', vg_free_count=6,
                             vg_extent_size=1073741824)
        monkeypatch.setattr(api, 'get_device_vgs', lambda x: [vg])
        disk = device.Device("/dev/sda")
        assert disk.used_by_ceph

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_not_used_by_ceph(self, fake_call, device_info, monkeypatch):
        FooPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000", lv_uuid="0000", pv_tags={}, vg_name="vg")
        pvolumes = []
        pvolumes.append(FooPVolume)
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "NAME": "sda", "PKNAME": "sda"}
        lv_data = {"lv_path": "vg/lv", "vg_name": "vg", "lv_uuid": "0000", "tags": {"ceph.osd_id": 0, "ceph.type": "journal"}}
        monkeypatch.setattr(api, 'get_pvs', lambda **kwargs: pvolumes)

        device_info(devices=data, lsblk=lsblk, lv=lv_data)
        disk = device.Device("/dev/sda")
        assert not disk.used_by_ceph

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_get_device_id(self, fake_call, device_info):
        udev = {k:k for k in ['ID_VENDOR', 'ID_MODEL', 'ID_SCSI_SERIAL']}
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(udevadm=udev,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk._get_device_id() == 'ID_VENDOR_ID_MODEL_ID_SCSI_SERIAL'

    def test_has_bluestore_label(self):
        # patch device.Device __init__ function to do nothing since we want to only test the
        # low-level behavior of has_bluestore_label
        with patch.object(device.Device, "__init__", lambda self, path, with_lsm=False: None):
            disk = device.Device("/dev/sda")
            disk.path = "/dev/sda"
            with patch('builtins.open', mock_open(read_data=b'bluestore block device\n')):
                assert disk.has_bluestore_label
            with patch('builtins.open', mock_open(read_data=b'not a bluestore block device\n')):
                assert not disk.has_bluestore_label


class TestDeviceEncryption(object):

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_partition_is_not_encrypted_lsblk(self, fake_call, device_info):
        lsblk = {'TYPE': 'part', 'FSTYPE': 'xfs', 'NAME': 'sda', 'PKNAME': 'sda'}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is False

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_partition_is_encrypted_lsblk(self, fake_call, device_info):
        lsblk = {'TYPE': 'part', 'FSTYPE': 'crypto_LUKS', 'NAME': 'sda', 'PKNAME': 'sda'}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_partition_is_not_encrypted_blkid(self, fake_call, device_info):
        lsblk = {'TYPE': 'part', 'NAME': 'sda', 'PKNAME': 'sda'}
        blkid = {'TYPE': 'ceph data'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is False

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_partition_is_encrypted_blkid(self, fake_call, device_info):
        lsblk = {'TYPE': 'part', 'NAME': 'sda' ,'PKNAME': 'sda'}
        blkid = {'TYPE': 'crypto_LUKS'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_mapper_is_encrypted_luks1(self, fake_call, device_info, monkeypatch):
        status = {'type': 'LUKS1'}
        monkeypatch.setattr(device, 'encryption_status', lambda x: status)
        lsblk = {'FSTYPE': 'xfs', 'NAME': 'uuid','TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_mapper_is_encrypted_luks2(self, fake_call, device_info, monkeypatch):
        status = {'type': 'LUKS2'}
        monkeypatch.setattr(device, 'encryption_status', lambda x: status)
        lsblk = {'FSTYPE': 'xfs', 'NAME': 'uuid', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_mapper_is_encrypted_plain(self, fake_call, device_info, monkeypatch):
        status = {'type': 'PLAIN'}
        monkeypatch.setattr(device, 'encryption_status', lambda x: status)
        lsblk = {'FSTYPE': 'xfs', 'NAME': 'uuid', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_mapper_is_not_encrypted_plain(self, fake_call, device_info, monkeypatch):
        monkeypatch.setattr(device, 'encryption_status', lambda x: {})
        lsblk = {'FSTYPE': 'xfs', 'NAME': 'uuid', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is False

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_lv_is_encrypted_blkid(self, fake_call, device_info):
        lsblk = {'TYPE': 'lvm', 'NAME': 'sda'}
        blkid = {'TYPE': 'crypto_LUKS'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = {}
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_lv_is_not_encrypted_blkid(self, fake_call, factory, device_info):
        lsblk = {'TYPE': 'lvm', 'NAME': 'sda'}
        blkid = {'TYPE': 'xfs'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=None)
        assert disk.is_encrypted is False

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_lv_is_encrypted_lsblk(self, fake_call, device_info):
        lsblk = {'FSTYPE': 'crypto_LUKS', 'NAME': 'sda', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = {}
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_lv_is_not_encrypted_lsblk(self, fake_call, factory, device_info):
        lsblk = {'FSTYPE': 'xfs', 'NAME': 'sda', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=None)
        assert disk.is_encrypted is False

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_lv_is_encrypted_lvm_api(self, fake_call, factory, device_info):
        lsblk = {'FSTYPE': 'xfs', 'NAME': 'sda', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=True)
        assert disk.is_encrypted is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_lv_is_not_encrypted_lvm_api(self, fake_call, factory, device_info):
        lsblk = {'FSTYPE': 'xfs', 'NAME': 'sda', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=False)
        assert disk.is_encrypted is False


class TestDeviceOrdering(object):

    def setup_method(self):
        self.data = {
                "/dev/sda": {"removable": "0"},
                "/dev/sdb": {"removable": "1"}, # invalid
                "/dev/sdc": {"removable": "0"},
                "/dev/sdd": {"removable": "1"}, # invalid
        }

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_valid_before_invalid(self, fake_call, device_info):
        lsblk_sda = {"NAME": "sda", "TYPE": "disk"}
        lsblk_sdb = {"NAME": "sdb", "TYPE": "disk"}
        device_info(devices=self.data,lsblk=lsblk_sda)
        sda = device.Device("/dev/sda")
        device_info(devices=self.data,lsblk=lsblk_sdb)
        sdb = device.Device("/dev/sdb")

        assert sda < sdb
        assert sdb > sda

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_valid_alphabetical_ordering(self, fake_call, device_info):
        lsblk_sda = {"NAME": "sda", "TYPE": "disk"}
        lsblk_sdc = {"NAME": "sdc", "TYPE": "disk"}
        device_info(devices=self.data,lsblk=lsblk_sda)
        sda = device.Device("/dev/sda")
        device_info(devices=self.data,lsblk=lsblk_sdc)
        sdc = device.Device("/dev/sdc")

        assert sda < sdc
        assert sdc > sda

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_invalid_alphabetical_ordering(self, fake_call, device_info):
        lsblk_sdb = {"NAME": "sdb", "TYPE": "disk"}
        lsblk_sdd = {"NAME": "sdd", "TYPE": "disk"}
        device_info(devices=self.data,lsblk=lsblk_sdb)
        sdb = device.Device("/dev/sdb")
        device_info(devices=self.data,lsblk=lsblk_sdd)
        sdd = device.Device("/dev/sdd")

        assert sdb < sdd
        assert sdd > sdb


class TestCephDiskDevice(object):

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_partlabel_lsblk(self, fake_call, device_info):
        lsblk = {"TYPE": "disk", "NAME": "sda", "PARTLABEL": ""}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.partlabel == ''

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_partlabel_blkid(self, fake_call, device_info):
        lsblk = {"TYPE": "disk", "NAME": "sda", "PARTLABEL": "ceph data"}
        blkid = {"TYPE": "disk", "PARTLABEL": "ceph data"}
        device_info(blkid=blkid, lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.partlabel == 'ceph data'

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "blkid_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_member_blkid(self, fake_call, monkeypatch):
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.is_member is True

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_is_member_lsblk(self, fake_call, patch_bluestore_label, device_info):
        lsblk = {"TYPE": "disk", "NAME": "sda", "PARTLABEL": "ceph"}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.is_member is True

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_unknown_type(self, fake_call, device_info):
        lsblk = {"TYPE": "disk", "NAME": "sda", "PARTLABEL": "gluster"}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type == 'unknown'

    ceph_types = ['data', 'wal', 'db', 'lockbox', 'journal', 'block']

    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "blkid_ceph_disk_member",
                             "disable_kernel_queries")
    def test_type_blkid(self, monkeypatch, fake_call, device_info, ceph_partlabel):
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type in self.ceph_types

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    @patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
    def test_type_lsblk(self, fake_call, device_info, ceph_partlabel):
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type in self.ceph_types
