from __future__ import absolute_import
import pytest


from orchestrator import DriveGroupSpec, DeviceSelection, DriveGroupValidationError, \
    InventoryDevice, ReadCompletion, raise_if_exception


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

    with pytest.raises(DriveGroupValidationError, match='exclusive'):
        DeviceSelection(paths=['/dev/sda'], rotates=False)

def test_inventory_device():
    i_d = InventoryDevice()
    s = i_d.pretty_print()
    assert len(s)


def test_raise():
    c = ReadCompletion()
    c.exception = ZeroDivisionError()
    with pytest.raises(ZeroDivisionError):
        raise_if_exception(c)
