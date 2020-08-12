import pytest
from mock.mock import patch
from ceph_volume.devices.lvm.strategies import filestore
from ceph_volume.api import lvm


class TestSingleType(object):

    def test_hdd_device_is_large_enough(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=12073740000))
        ]
        computed_osd = filestore.SingleType.with_auto_devices(args, devices).computed['osds'][0]
        assert computed_osd['data']['percentage'] == 55
        assert computed_osd['data']['parts'] == 1
        assert computed_osd['data']['human_readable_size'] == '6.24 GB'
        assert computed_osd['data']['path'] == '/dev/sda'

    def test_hdd_device_with_large_journal(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 5.66 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error.value)

    def test_ssd_device_is_large_enough(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=False, sys_api=dict(size=12073740000))
        ]
        computed_osd = filestore.SingleType.with_auto_devices(args, devices).computed['osds'][0]
        assert computed_osd['data']['percentage'] == 55
        assert computed_osd['data']['parts'] == 1
        assert computed_osd['data']['human_readable_size'] == '6.24 GB'
        assert computed_osd['data']['path'] == '/dev/sda'

    def test_ssd_device_with_large_journal(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=False, sys_api=dict(size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 5.66 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error.value)

    def test_ssd_device_multi_osd(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=4,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=False, sys_api=dict(size=16073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 14.97 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error.value)

    def test_hdd_device_multi_osd(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=4,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=16073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 14.97 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error.value)

    def test_device_is_lvm_member_fails(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=True, rotational=True, sys_api=dict(size=12073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        assert 'Unable to use device, already a member of LVM' in str(error.value)

    def test_hdd_device_with_small_configured_journal(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "journal sizes must be larger than 2GB, detected: 120.00 MB"
        assert msg in str(error.value)

    def test_ssd_device_with_small_configured_journal(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=False, sys_api=dict(size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "journal sizes must be larger than 2GB, detected: 120.00 MB"
        assert msg in str(error.value)


class TestMixedType(object):

    def test_minimum_size_is_not_met(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=False, sys_api=dict(size=6073740000)),
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        msg = "journal sizes must be larger than 2GB, detected: 120.00 MB"
        assert msg in str(error.value)

    def test_ssd_device_is_not_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '7120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=False, sys_api=dict(size=6073740000)),
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        msg = "Not enough space in fast devices (5.66 GB) to create 1 x 6.95 GB journal LV"
        assert msg in str(error.value)

    def test_hdd_device_is_lvm_member_fails(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=False, sys_api=dict(size=6073740000)),
            fakedevice(used_by_ceph=False, is_lvm_member=True, rotational=True, sys_api=dict(size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        assert 'Unable to use device, already a member of LVM' in str(error.value)

    @patch('ceph_volume.devices.lvm.strategies.strategies.MixedStrategy.get_common_vg')
    def test_ssd_is_lvm_member_doesnt_fail(self,
                                           patched_get_common_vg,
                                           volumes,
                                           fakedevice,
                                           factory,
                                           conf_ceph):
        ssd = fakedevice(
            used_by_ceph=False, is_lvm_member=True, rotational=False, sys_api=dict(size=6073740000)
        )
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=6073740000))
        vg = lvm.VolumeGroup(
                vg_name='fast', lv_name='foo',
                lv_path='/dev/vg/foo', lv_tags="ceph.type=data",
                vg_extent_size=1024*1024*1024, vg_free_count=7)
        patched_get_common_vg.return_value = vg


        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [ssd, hdd]

        result = filestore.MixedType.with_auto_devices(args, devices).\
            computed['osds'][0]
        assert result['journal']['path'] == 'vg: fast'
        assert result['journal']['percentage'] == 71
        assert result['journal']['human_readable_size'] == '5.00 GB'

    @patch('ceph_volume.api.lvm.get_device_vgs')
    def test_no_common_vg(self, patched_get_device_vgs, volumes, fakedevice, factory, conf_ceph):
        patched_get_device_vgs.side_effect = lambda x: [lvm.VolumeGroup(vg_name='{}'.format(x[-1]), vg_tags='')]
        ssd1 = fakedevice(
            used_by_ceph=False, is_lvm_member=True, rotational=False, sys_api=dict(size=6073740000)
        )
        ssd2 = fakedevice(
            used_by_ceph=False, is_lvm_member=True, rotational=False, sys_api=dict(size=6073740000)
        )
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, rotational=True, sys_api=dict(size=6073740000))

        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1, osd_ids=[],
                       journal_size=None)
        devices = [ssd1, ssd2, hdd]

        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
            assert 'Could not find a common VG between devices' in str(error.value)

    def test_ssd_device_fails_multiple_osds(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '15120')
        args = factory(filtered_devices=[], osds_per_device=2,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(is_lvm_member=False, rotational=False, sys_api=dict(size=16073740000)),
            fakedevice(is_lvm_member=False, rotational=True, sys_api=dict(size=16073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        msg = "Not enough space in fast devices (14.97 GB) to create 2 x 14.77 GB journal LV"
        assert msg in str(error.value)
