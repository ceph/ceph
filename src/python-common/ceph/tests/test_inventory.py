import json
import os
import pytest

from ceph.deployment.inventory import Devices


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
