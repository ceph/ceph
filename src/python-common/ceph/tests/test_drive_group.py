# flake8: noqa
import re

import pytest
import yaml

from ceph.deployment import drive_selection, translate
from ceph.deployment.hostspec import HostSpec, SpecValidationError
from ceph.deployment.inventory import Device
from ceph.deployment.service_spec import PlacementSpec
from ceph.tests.utils import _mk_inventory, _mk_device
from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection, \
    DriveGroupValidationError

@pytest.mark.parametrize("test_input",
[
    (
        [  # new style json
            {
                'service_type': 'osd',
                'service_id': 'testing_drivegroup',
                'placement': {'host_pattern': 'hostname'},
                'data_devices': {'paths': ['/dev/sda']}
            }
        ]
    ),
])
def test_DriveGroup(test_input):
    dg = [DriveGroupSpec.from_json(inp) for inp in test_input][0]
    assert dg.placement.filter_matching_hostspecs([HostSpec('hostname')]) == ['hostname']
    assert dg.service_id == 'testing_drivegroup'
    assert all([isinstance(x, Device) for x in dg.data_devices.paths])
    assert dg.data_devices.paths[0].path == '/dev/sda'

@pytest.mark.parametrize("match,test_input",
[
    (
        re.escape('Service Spec is not an (JSON or YAML) object. got "None"'),
        ''
    ),
    (
        'Failed to validate Drive Group: DeviceSelection cannot be empty', """
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
data_devices:
  limit: 1
"""
    ),
    (
        'Failed to validate Drive Group: filter_logic must be either <AND> or <OR>', """
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
data_devices:
  all: True
filter_logic: XOR
"""
    ),
    (
        'Failed to validate Drive Group: `data_devices` element is required.', """
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
spec:
  db_devices:
    model: model
"""
    ),
])
def test_DriveGroup_fail(match, test_input):
    with pytest.raises(SpecValidationError, match=match):
        osd_spec = DriveGroupSpec.from_json(yaml.safe_load(test_input))
        osd_spec.validate()


def test_drivegroup_pattern():
    dg = DriveGroupSpec(
        PlacementSpec(host_pattern='node[1-3]'),
        service_id='foobar',
        data_devices=DeviceSelection(all=True))
    assert dg.placement.filter_matching_hostspecs([HostSpec('node{}'.format(i)) for i in range(10)]) == ['node1', 'node2', 'node3']


def test_drive_selection():
    devs = DeviceSelection(paths=['/dev/sda'])
    spec = DriveGroupSpec(
        PlacementSpec('node_name'),
        service_id='foobar',
        data_devices=devs)
    assert all([isinstance(x, Device) for x in spec.data_devices.paths])
    assert spec.data_devices.paths[0].path == '/dev/sda'

    with pytest.raises(DriveGroupValidationError, match='exclusive'):
        DeviceSelection(paths=['/dev/sda'], rotational=False)


def test_ceph_volume_command_0():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(all=True)
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device()*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd'


def test_ceph_volume_command_1():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(rotational=True),
                          db_devices=DeviceSelection(rotational=False)
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --yes --no-systemd')


def test_ceph_volume_command_2():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G')
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True, size="300.00 GB")*2 +
                              _mk_device(rotational=False, size="300.00 GB")*2 +
                              _mk_device(size="10.0 GB", rotational=False)*2
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --wal-devices /dev/sde /dev/sdf '
                   '--yes --no-systemd')


def test_ceph_volume_command_3():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G'),
                          encrypted=True
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True, size="300.00 GB")*2 +
                              _mk_device(rotational=False, size="300.00 GB")*2 +
                              _mk_device(size="10.0 GB", rotational=False)*2
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd '
                   '--wal-devices /dev/sde /dev/sdf --dmcrypt '
                   '--yes --no-systemd')


def test_ceph_volume_command_4():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G'),
                          block_db_size='500M',
                          block_wal_size='500M',
                          osds_per_device=3,
                          encrypted=True
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True, size="300.00 GB")*2 +
                              _mk_device(rotational=False, size="300.00 GB")*2 +
                              _mk_device(size="10.0 GB", rotational=False)*2
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --wal-devices /dev/sde /dev/sdf '
                   '--block-wal-size 500M --block-db-size 500M --dmcrypt '
                   '--osds-per-device 3 --yes --no-systemd')


def test_ceph_volume_command_5():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(rotational=True),
                          objectstore='filestore'
                          )
    with pytest.raises(DriveGroupValidationError):
        spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --filestore --yes --no-systemd'


def test_ceph_volume_command_6():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(rotational=False),
                          journal_devices=DeviceSelection(rotational=True),
                          journal_size='500M',
                          objectstore='filestore'
                          )
    with pytest.raises(DriveGroupValidationError):
        spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == ('lvm batch --no-auto /dev/sdc /dev/sdd '
                   '--journal-size 500M --journal-devices /dev/sda /dev/sdb '
                   '--filestore --yes --no-systemd')


def test_ceph_volume_command_7():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(all=True),
                          osd_id_claims={'host1': ['0', '1']}
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, ['0', '1']).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --osd-ids 0 1 --yes --no-systemd'


def test_ceph_volume_command_8():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(rotational=True, model='INTEL SSDS'),
                          db_devices=DeviceSelection(model='INTEL SSDP'),
                          filter_logic='OR',
                          osd_id_claims={}
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True,  size='1.82 TB',  model='ST2000DM001-1ER1') +  # data
                              _mk_device(rotational=False, size="223.0 GB", model='INTEL SSDSC2KG24') +  # data
                              _mk_device(rotational=False, size="349.0 GB", model='INTEL SSDPED1K375GA')  # wal/db
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --db-devices /dev/sdc --yes --no-systemd'
