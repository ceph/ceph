import pytest

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection, DriveGroupValidationError


def test_DriveGroup():
    dg_json = {
        'host_pattern': 'hostname',
        'data_devices': {'paths': ['/dev/sda']}
    }

    dg = DriveGroupSpec.from_json(dg_json)
    assert dg.hosts(['hostname']) == ['hostname']
    assert dg.data_devices.paths == ['/dev/sda']


def test_DriveGroup_fail():
    with pytest.raises(DriveGroupValidationError):
        DriveGroupSpec.from_json({})


def test_drivegroup_pattern():
    dg = DriveGroupSpec('node[1-3]', DeviceSelection(all=True))
    assert dg.hosts(['node{}'.format(i) for i in range(10)]) == ['node1', 'node2', 'node3']


def test_drive_selection():
    devs = DeviceSelection(paths=['/dev/sda'])
    spec = DriveGroupSpec('node_name', data_devices=devs)
    assert spec.data_devices.paths == ['/dev/sda']

    with pytest.raises(DriveGroupValidationError, match='exclusive'):
        DeviceSelection(paths=['/dev/sda'], rotational=False)
