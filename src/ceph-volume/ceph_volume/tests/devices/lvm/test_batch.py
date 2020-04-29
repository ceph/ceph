import pytest
import json
from ceph_volume.devices.lvm import batch


class TestBatch(object):

    def test_batch_instance(self, is_root):
        b = batch.Batch([])
        b.main()

    def test_disjoint_device_lists(self, factory):
        device1 = factory(used_by_ceph=False, available=True, abspath="/dev/sda")
        device2 = factory(used_by_ceph=False, available=True, abspath="/dev/sdb")
        devices = [device1, device2]
        db_devices = [device2]
        with pytest.raises(Exception) as disjoint_ex:
            batch.ensure_disjoint_device_lists(devices, db_devices)
        assert 'Device lists are not disjoint' in str(disjoint_ex.value)

    def test_get_physical_osds_return_len(self, factory,
                                          mock_devices_available):
        osds_per_device = 1
        args = factory(data_slots=1, osds_per_device=osds_per_device, osd_ids=[])
        osds = batch.get_physical_osds(mock_devices_available, args)
        assert len(osds) == len(mock_devices_available) * osds_per_device

    def test_get_physical_osds_rel_size(self, factory,
                                          mock_devices_available):
        osds_per_device = 1
        args = factory(data_slots=1, osds_per_device=osds_per_device, osd_ids=[])
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd in osds:
            assert osd.data[1] == 100 / osds_per_device

    def test_get_physical_osds_abs_size(self, factory,
                                          mock_devices_available):
        osds_per_device = 1
        args = factory(data_slots=1, osds_per_device=osds_per_device, osd_ids=[])
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd, dev in zip(osds, mock_devices_available):
            assert osd.data[2] == dev.vg_size[0] / osds_per_device
