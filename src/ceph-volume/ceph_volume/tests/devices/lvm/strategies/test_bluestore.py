import pytest
from ceph_volume.devices.lvm.strategies import bluestore


class TestSingleType(object):

    def test_hdd_device_is_large_enough(self, fakedevice, factory):
        args = factory(filtered_devices=[], osds_per_device=1, block_db_size=None)
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        ]
        computed_osd = bluestore.SingleType(devices, args).computed['osds'][0]
        assert computed_osd['data']['percentage'] == 100
        assert computed_osd['data']['parts'] == 1
        assert computed_osd['data']['human_readable_size'] == '5.66 GB'
        assert computed_osd['data']['path'] == '/dev/sda'

    def test_sdd_device_is_large_enough(self, fakedevice, factory):
        args = factory(filtered_devices=[], osds_per_device=1, block_db_size=None)
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000))
        ]
        computed_osd = bluestore.SingleType(devices, args).computed['osds'][0]
        assert computed_osd['data']['percentage'] == 100
        assert computed_osd['data']['parts'] == 1
        assert computed_osd['data']['human_readable_size'] == '5.66 GB'
        assert computed_osd['data']['path'] == '/dev/sda'

    def test_device_cannot_have_many_osds_per_device(self, fakedevice, factory):
        args = factory(filtered_devices=[], osds_per_device=3, block_db_size=None)
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            bluestore.SingleType(devices, args)
        assert 'Unable to use device 5.66 GB /dev/sda' in str(error)

    def test_device_is_lvm_member_fails(self, fakedevice, factory):
        args = factory(filtered_devices=[], osds_per_device=1, block_db_size=None)
        devices = [
            fakedevice(used_by_ceph=False, is_lvm_member=True, sys_api=dict(rotational='1', size=6073740000))
        ]
        with pytest.raises(RuntimeError) as error:
            bluestore.SingleType(devices, args)
        assert 'Unable to use device, already a member of LVM' in str(error)


class TestMixedTypeConfiguredSize(object):
    # uses a block.db size that has been configured via ceph.conf, instead of
    # defaulting to 'as large as possible'

    def test_hdd_device_is_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        # 3GB block.db in ceph.conf
        conf_ceph(get_safe=lambda *a: 3147483640)
        args = factory(filtered_devices=[], osds_per_device=1, block_db_size=None)
        ssd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000))
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        devices = [ssd, hdd]

        osd = bluestore.MixedType(devices, args).computed['osds'][0]
        assert osd['data']['percentage'] == 100
        assert osd['data']['human_readable_size'] == '5.66 GB'
        assert osd['data']['path'] == '/dev/sda'
        # a new vg will be created
        assert osd['block.db']['path'] == 'vg: vg/lv'
        assert osd['block.db']['percentage'] == 100

    def test_ssd_device_is_not_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        # 7GB block.db in ceph.conf
        conf_ceph(get_safe=lambda *a: 7747483640)
        args = factory(filtered_devices=[], osds_per_device=1, block_db_size=None)
        ssd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000))
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        devices = [ssd, hdd]

        with pytest.raises(RuntimeError) as error:
            bluestore.MixedType(devices, args).computed['osds'][0]
        expected = 'Not enough space in fast devices (5.66 GB) to create 1 x 7.22 GB block.db LV'
        assert expected in str(error)

    def test_multi_hdd_device_is_not_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        # 3GB block.db in ceph.conf
        conf_ceph(get_safe=lambda *a: 3147483640)
        args = factory(filtered_devices=[], osds_per_device=2, block_db_size=None)
        ssd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=60737400000))
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        devices = [ssd, hdd]

        with pytest.raises(RuntimeError) as error:
            bluestore.MixedType(devices, args)
        expected = 'Unable to use device 5.66 GB /dev/sda, LVs would be smaller than 5GB'
        assert expected in str(error)


class TestMixedTypeLargeAsPossible(object):

    def test_hdd_device_is_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: None)
        args = factory(filtered_devices=[], osds_per_device=1, block_db_size=None)
        ssd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=6073740000))
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        devices = [ssd, hdd]

        osd = bluestore.MixedType(devices, args).computed['osds'][0]
        assert osd['data']['percentage'] == 100
        assert osd['data']['human_readable_size'] == '5.66 GB'
        assert osd['data']['path'] == '/dev/sda'
        # a new vg will be created
        assert osd['block.db']['path'] == 'vg: vg/lv'
        # as large as possible
        assert osd['block.db']['percentage'] == 100

    def test_multi_hdd_device_is_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: None)
        args = factory(filtered_devices=[], osds_per_device=2, block_db_size=None)
        ssd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=60073740000))
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=60073740000))
        devices = [ssd, hdd]

        osd = bluestore.MixedType(devices, args).computed['osds'][0]
        assert osd['data']['percentage'] == 50
        assert osd['data']['human_readable_size'] == '27.97 GB'
        assert osd['data']['path'] == '/dev/sda'
        # a new vg will be created
        assert osd['block.db']['path'] == 'vg: vg/lv'
        # as large as possible
        assert osd['block.db']['percentage'] == 50

    def test_multi_hdd_device_is_not_large_enough(self, stub_vgs, fakedevice, factory, conf_ceph):
        conf_ceph(get_safe=lambda *a: None)
        args = factory(filtered_devices=[], osds_per_device=2, block_db_size=None)
        ssd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='0', size=60737400000))
        hdd = fakedevice(used_by_ceph=False, is_lvm_member=False, sys_api=dict(rotational='1', size=6073740000))
        devices = [ssd, hdd]

        with pytest.raises(RuntimeError) as error:
            bluestore.MixedType(devices, args)
        expected = 'Unable to use device 5.66 GB /dev/sda, LVs would be smaller than 5GB'
        assert expected in str(error)
