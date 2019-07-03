import pytest
from ceph_volume.util import disk
from ceph_volume.devices.lvm.strategies import validators


class TestMinimumDeviceSize(object):

    def test_size_is_larger_than_5gb(self, fakedevice):
        devices = [fakedevice(sys_api=dict(size=6073740000))]
        assert validators.minimum_device_size(devices) is None

    def test_size_is_smaller_than_5gb(self, fakedevice):
        devices = [fakedevice(sys_api=dict(size=1073740000))]
        with pytest.raises(RuntimeError) as error:
            validators.minimum_device_size(devices)
        msg = "LVs would be smaller than 5GB"
        assert msg in str(error.value)

    def test_large_device_multiple_osds_fails(self, fakedevice):
        devices = [fakedevice(sys_api=dict(size=6073740000))]
        with pytest.raises(RuntimeError) as error:
            validators.minimum_device_size(
                devices, osds_per_device=4
            )
        msg = "LVs would be smaller than 5GB"
        assert msg in str(error.value)


class TestMinimumCollocatedDeviceSize(object):

    def setup(self):
        self.journal_size = disk.Size(gb=5)

    def test_size_is_larger_than_5gb_large_journal(self, fakedevice):
        devices = [fakedevice(sys_api=dict(size=6073740000))]
        assert validators.minimum_device_collocated_size(devices, disk.Size(mb=1)) is None

    def test_size_is_larger_than_5gb_large_journal_fails(self, fakedevice):
        devices = [fakedevice(sys_api=dict(size=1073740000))]
        with pytest.raises(RuntimeError) as error:
            validators.minimum_device_collocated_size(devices, self.journal_size)
        msg = "LVs would be smaller than 5GB"
        assert msg in str(error.value)

    def test_large_device_multiple_osds_fails(self, fakedevice):
        devices = [fakedevice(sys_api=dict(size=16073740000))]
        with pytest.raises(RuntimeError) as error:
            validators.minimum_device_collocated_size(
                devices, self.journal_size, osds_per_device=3
            )
        msg = "LVs would be smaller than 5GB"
        assert msg in str(error.value)
