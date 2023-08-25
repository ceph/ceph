import pytest
from mock.mock import patch
from ceph_volume.api import lvm
from ceph_volume.devices.lvm import deactivate

class TestDeactivate(object):

    @patch("ceph_volume.devices.lvm.deactivate.get_lvs_by_tag")
    def test_no_osd(self, p_get_lvs):
        p_get_lvs.return_value = []
        with pytest.raises(StopIteration):
            deactivate.deactivate_osd(0)

    @patch("ceph_volume.devices.lvm.deactivate.get_lvs_by_tag")
    @patch("ceph_volume.util.system.unmount_tmpfs")
    def test_unmount_tmpfs_called_osd_id(self, p_u_tmpfs, p_get_lvs):
        FooVolume = lvm.Volume(
            lv_name='foo', lv_path='/dev/vg/foo',
            lv_tags="ceph.osd_id=0,ceph.cluster_name=foo,ceph.type=data")
        p_get_lvs.return_value = [FooVolume]

        deactivate.deactivate_osd(0)
        p_u_tmpfs.assert_called_with(
            '/var/lib/ceph/osd/{}-{}'.format('foo', 0))

    @patch("ceph_volume.devices.lvm.deactivate.get_lvs_by_tag")
    @patch("ceph_volume.util.system.unmount_tmpfs")
    def test_unmount_tmpfs_called_osd_uuid(self, p_u_tmpfs, p_get_lvs):
        FooVolume = lvm.Volume(
            lv_name='foo', lv_path='/dev/vg/foo',
            lv_tags="ceph.osd_fsid=0,ceph.osd_id=1,ceph.cluster_name=foo,ceph.type=data")
        p_get_lvs.return_value = [FooVolume]

        deactivate.deactivate_osd(None, 0)
        p_u_tmpfs.assert_called_with(
            '/var/lib/ceph/osd/{}-{}'.format('foo', 1))

    @patch("ceph_volume.devices.lvm.deactivate.get_lvs_by_tag")
    @patch("ceph_volume.util.system.unmount_tmpfs")
    @patch("ceph_volume.util.encryption.dmcrypt_close")
    def test_no_crypt_no_dmclose(self, p_dm_close, p_u_tmpfs, p_get_lvs):
        FooVolume = lvm.Volume(
            lv_name='foo', lv_path='/dev/vg/foo',
            lv_tags="ceph.osd_id=0,ceph.cluster_name=foo,ceph.type=data")
        p_get_lvs.return_value = [FooVolume]

        deactivate.deactivate_osd(0)

    @patch("ceph_volume.devices.lvm.deactivate.get_lvs_by_tag")
    @patch("ceph_volume.util.system.unmount_tmpfs")
    @patch("ceph_volume.util.encryption.dmcrypt_close")
    def test_crypt_dmclose(self, p_dm_close, p_u_tmpfs, p_get_lvs):
        FooVolume = lvm.Volume(
            lv_name='foo', lv_path='/dev/vg/foo', lv_uuid='123',
            lv_tags="ceph.osd_id=0,ceph.encrypted=1,ceph.cluster_name=foo,ceph.type=data")
        p_get_lvs.return_value = [FooVolume]

        deactivate.deactivate_osd(0)
        p_dm_close.assert_called_with(mapping='123', skip_path_check=True)
