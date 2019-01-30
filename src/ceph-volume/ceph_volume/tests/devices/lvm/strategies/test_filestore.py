import pytest
from ceph_volume.devices.lvm.strategies import filestore
from ceph_volume.api import lvm


class TestSingleType(object):

    def test_hdd_device_is_large_enough(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=12073740000))
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
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 5.66 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error)

    def test_ssd_device_is_large_enough(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=12073740000))
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
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 5.66 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error)

    def test_ssd_device_multi_osd(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=4,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=16073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 14.97 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error)

    def test_hdd_device_multi_osd(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=4,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=16073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "Unable to use device 14.97 GB /dev/sda, LVs would be smaller than 5GB"
        assert msg in str(error)

    def test_device_is_lvm_member_fails(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=True, sys_api=dict(rotational='1', size=12073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        assert 'Unable to use device, already a member of LVM' in str(error)

    def test_hdd_device_with_small_configured_journal(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "journal sizes must be larger than 2GB, detected: 120.00 MB"
        assert msg in str(error)

    def test_ssd_device_with_small_configured_journal(self, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.SingleType.with_auto_devices(args, devices)
        msg = "journal sizes must be larger than 2GB, detected: 120.00 MB"
        assert msg in str(error)


class TestMixedType(object):

    def test_minimum_size_is_not_met(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000)),
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        msg = "journal sizes must be larger than 2GB, detected: 120.00 MB"
        assert msg in str(error)

    def test_ssd_device_is_not_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '7120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000)),
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        msg = "Not enough space in fast devices (5.66 GB) to create 1 x 6.95 GB journal LV"
        assert msg in str(error)

    def test_hdd_device_is_lvm_member_fails(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000)),
            fakedevice(used_by_ceph=False, is_lvm_member=True, sys_api=dict(rotational='1', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        assert 'Unable to use device, already a member of LVM' in str(error)

    def test_ssd_is_lvm_member_doesnt_fail(self, volumes, stub_vgs, fakedevice, factory, conf_ceph):
        # fast PV, because ssd is an LVM member
        CephPV = lvm.PVolume(vg_name='fast', pv_name='/dev/sda', pv_tags='')
        ssd = fakedevice(
            used_by_ceph=False, is_lvm_member=True, sys_api=dict(rotational='0', size=6073740000), pvs_api=[CephPV]
        )
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        # when get_api_vgs() gets called, it will return this one VG
        stub_vgs([
            dict(
                vg_free='7g', vg_name='fast', lv_name='foo',
                lv_path='/dev/vg/foo', lv_tags="ceph.type=data"
            )
        ])

        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [ssd, hdd]
        result = filestore.MixedType.with_auto_devices(args, devices).computed['osds'][0]
        assert result['journal']['path'] == 'vg: fast'
        assert result['journal']['percentage'] == 71
        assert result['journal']['human_readable_size'] == '5.00 GB'

    def test_no_common_vg(self, volumes, stub_vgs, fakedevice, factory, conf_ceph):
        # fast PV, because ssd is an LVM member
        CephPV1 = lvm.PVolume(vg_name='fast1', pv_name='/dev/sda', pv_tags='')
        CephPV2 = lvm.PVolume(vg_name='fast2', pv_name='/dev/sdb', pv_tags='')
        ssd1 = fakedevice(
            used_by_ceph=False, is_lvm_member=True, sys_api=dict(rotational='0', size=6073740000), pvs_api=[CephPV1]
        )
        ssd2 = fakedevice(
            used_by_ceph=False, is_lvm_member=True, sys_api=dict(rotational='0', size=6073740000), pvs_api=[CephPV2]
        )
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        # when get_api_vgs() gets called, it will return this one VG
        stub_vgs([
            dict(
                vg_free='7g', vg_name='fast1', lv_name='foo',
                lv_path='/dev/vg/fast1', lv_tags="ceph.type=data"
            ),
            dict(
                vg_free='7g', vg_name='fast2', lv_name='foo',
                lv_path='/dev/vg/fast2', lv_tags="ceph.type=data"
            )
        ])

        conf_ceph(get_safe=lambda *a: '5120')
        args = factory(filtered_devices=[], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        devices = [ssd1, ssd2, hdd]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)

        assert 'Could not find a common VG between devices' in str(error)

    def test_ssd_device_fails_multiple_osds(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: '15120')
        args = factory(filtered_devices=[], osds_per_device=2,
                       journal_size=None, osd_ids=[])
        devices = [
            fakedevice(is_lvm_member=False, sys_api=dict(rotational='0', size=16073740000)),
            fakedevice(is_lvm_member=False, sys_api=dict(rotational='1', size=16073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            filestore.MixedType.with_auto_devices(args, devices)
        msg = "Not enough space in fast devices (14.97 GB) to create 2 x 14.77 GB journal LV"
        assert msg in str(error)

    def test_filter_all_data_devs(self, fakedevice, factory):
        # in this scenario the user passed a already used device to be used for
        # data and an unused device to be used as db device.
        db_dev = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000))
        data_dev = fakedevice(used_by_ceph=True, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        args = factory(filtered_devices=[data_dev], osds_per_device=1,
                       journal_size=None, osd_ids=[])
        filestore.MixedType(args, [], [db_dev])
