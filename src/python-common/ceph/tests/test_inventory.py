import datetime
import json
import os
import pytest

from ceph.deployment.inventory import Devices, Device
from ceph.utils import datetime_now


@pytest.mark.parametrize("filename",
                         [
                             os.path.dirname(__file__) + '/c-v-inventory.json',
                             os.path.dirname(__file__) + '/../../../pybind/mgr/test_orchestrator/du'
                                                         'mmy_data.json',
                         ])
def test_from_json(filename):
    with open(filename) as f:
        data = json.load(f)
    if 'inventory' in data:
        data = data['inventory']
    ds = Devices.from_json(data)
    assert len(ds.devices) == len(data)
    assert Devices.from_json(ds.to_json()) == ds


class TestDevicesEquality():
    created_time1 = datetime_now()
    created_time2 = created_time1 + datetime.timedelta(seconds=30)

    @pytest.mark.parametrize(
        "old_devices, new_devices, expected_equal",
        [
            (  # identical list should be equal
                Devices([Device('/dev/sdb', available=True, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1)]),
                Devices([Device('/dev/sdb', available=True, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1)]),
                True,
            ),
            (  # differing only in created time should still be equal
                Devices([Device('/dev/sdb', available=True, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1)]),
                Devices([Device('/dev/sdb', available=True, created=created_time2),
                         Device('/dev/sdc', available=True, created=created_time2)]),
                True,
            ),
            (  # differing in some other field should make them not equal
                Devices([Device('/dev/sdb', available=True, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1)]),
                Devices([Device('/dev/sdb', available=False, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1)]),
                False,
            ),
            (  # different amount of devices should not pass equality
                Devices([Device('/dev/sdb', available=True, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1)]),
                Devices([Device('/dev/sdb', available=True, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1),
                         Device('/dev/sdd', available=True, created=created_time1)]),
                False,
            ),
            (  # differing order should not affect equality
                Devices([Device('/dev/sdb', available=True, created=created_time1),
                         Device('/dev/sdc', available=True, created=created_time1)]),
                Devices([Device('/dev/sdc', available=True, created=created_time1),
                         Device('/dev/sdb', available=True, created=created_time1)]),
                True,
            ),
        ])
    def test_equality(self, old_devices, new_devices, expected_equal):
        assert (old_devices == new_devices) == expected_equal
