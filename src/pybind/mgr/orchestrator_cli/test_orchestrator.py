from __future__ import absolute_import
import pytest


from orchestrator import DriveGroupSpec, DeviceSelection


def test_DriveGroup():
    dg_json = {
        'host_pattern': 'hostname',
        'data_devices': {'paths': ['/dev/sda']}
    }

    dg = DriveGroupSpec.from_json(dg_json)
    assert dg.hosts(['hostname']) == ['hostname']
    assert dg.data_devices.paths == ['/dev/sda']


def test_DriveGroup_fail():
    with pytest.raises(TypeError):
        DriveGroupSpec.from_json({})


def test_drivegroup_pattern():
    dg = DriveGroupSpec('node[1-3]', DeviceSelection())
    assert dg.hosts(['node{}'.format(i) for i in range(10)]) == ['node1', 'node2', 'node3']


def test_drive_selection():
    devs = DeviceSelection(paths=['/dev/sda'])
    spec = DriveGroupSpec('node_name', data_devices=devs)
    assert spec.data_devices.paths == ['/dev/sda']

    with pytest.raises(TypeError, match='exclusive'):
        DeviceSelection(paths=['/dev/sda'], rotates=False)

