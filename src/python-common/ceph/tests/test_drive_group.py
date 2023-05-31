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
    (  # new style json
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
crush_device_class: ssd
data_devices:
  paths:
  - /dev/sda
"""
    ),
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: ssd"""
    ),
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
spec:
  osds_per_device: 2
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: hdd"""
    ),
])
def test_DriveGroup(test_input):

    dg = DriveGroupSpec.from_json(yaml.safe_load(test_input))
    assert dg.service_id == 'testing_drivegroup'
    assert all([isinstance(x, Device) for x in dg.data_devices.paths])
    if isinstance(dg.data_devices.paths[0].path, str):
        assert dg.data_devices.paths[0].path == '/dev/sda'



@pytest.mark.parametrize("match,test_input",
[
    (
        re.escape('Service Spec is not an (JSON or YAML) object. got "None"'),
        ''
    ),
    (
        'Failed to validate OSD spec "<unnamed>": `placement` required',
        """data_devices:
  all: True
"""
    ),
    (
        'Failed to validate OSD spec "mydg.data_devices": device selection cannot be empty', """
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
data_devices:
  limit: 1
"""
    ),
    (
        'Failed to validate OSD spec "mydg": filter_logic must be either <AND> or <OR>', """
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
        'Failed to validate OSD spec "mydg": `data_devices` element is required.', """
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
spec:
  db_devices:
    model: model
"""
    ),
    (
        'Failed to validate OSD spec "mydg.db_devices": Filtering for `unknown_key` is not supported', """
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
spec:
  db_devices:
    unknown_key: 1
"""
    ),
    (
        'Failed to validate OSD spec "mydg": Feature `unknown_key` is not supported', """
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
spec:
  db_devices:
    all: true
  unknown_key: 1
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
        ds = DeviceSelection(paths=['/dev/sda'], rotational=False)
        ds.validate('')


def test_ceph_volume_command_0():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(all=True)
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device()*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd' for cmd in cmds), f'Expected {cmd} in {cmds}'


def test_ceph_volume_command_1():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(rotational=True),
                          db_devices=DeviceSelection(rotational=False)
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --yes --no-systemd') for cmd in cmds), f'Expected {cmd} in {cmds}'


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
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --wal-devices /dev/sde /dev/sdf '
                   '--yes --no-systemd') for cmd in cmds), f'Expected {cmd} in {cmds}'


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
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd '
                   '--wal-devices /dev/sde /dev/sdf --dmcrypt '
                   '--yes --no-systemd') for cmd in cmds), f'Expected {cmd} in {cmds}'


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
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --wal-devices /dev/sde /dev/sdf '
                   '--block-wal-size 500M --block-db-size 500M --dmcrypt '
                   '--osds-per-device 3 --yes --no-systemd') for cmd in cmds), f'Expected {cmd} in {cmds}'


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
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --filestore --yes --no-systemd' for cmd in cmds), f'Expected {cmd} in {cmds}'


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
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == ('lvm batch --no-auto /dev/sdc /dev/sdd '
                   '--journal-size 500M --journal-devices /dev/sda /dev/sdb '
                   '--filestore --yes --no-systemd') for cmd in cmds), f'Expected {cmd} in {cmds}'


def test_ceph_volume_command_7():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(all=True),
                          osd_id_claims={'host1': ['0', '1']}
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmds = translate.to_ceph_volume(sel, ['0', '1']).run()
    assert all(cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --osd-ids 0 1 --yes --no-systemd' for cmd in cmds), f'Expected {cmd} in {cmds}'


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
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --db-devices /dev/sdc --yes --no-systemd' for cmd in cmds), f'Expected {cmd} in {cmds}'


def test_ceph_volume_command_9():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(all=True),
                          data_allocate_fraction=0.8
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device()*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --data-allocate-fraction 0.8 --yes --no-systemd' for cmd in cmds), f'Expected {cmd} in {cmds}'


@pytest.mark.parametrize("test_input_base",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
crush_device_class: ssd
data_devices:
  paths:
  - /dev/sda
"""
    ),
    ])
def test_ceph_volume_command_10(test_input_base):
    spec = DriveGroupSpec.from_json(yaml.safe_load(test_input_base))
    spec.validate()
    drive = drive_selection.DriveSelection(spec, spec.data_devices.paths)
    cmds = translate.to_ceph_volume(drive, []).run()

    assert all(cmd == 'lvm batch --no-auto /dev/sda --crush-device-class ssd --yes --no-systemd' for cmd in cmds), f'Expected {cmd} in {cmds}'


@pytest.mark.parametrize("test_input1",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
crush_device_class: ssd
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: hdd
  - path: /dev/sdb
    crush_device_class: hdd
"""
    ),
    ])
def test_ceph_volume_command_11(test_input1):
    spec = DriveGroupSpec.from_json(yaml.safe_load(test_input1))
    spec.validate()
    drive = drive_selection.DriveSelection(spec, spec.data_devices.paths)
    cmds = translate.to_ceph_volume(drive, []).run()

    assert all(cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --crush-device-class hdd --yes --no-systemd' for cmd in cmds), f'Expected {cmd} in {cmds}'


@pytest.mark.parametrize("test_input2",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
crush_device_class: ssd
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: hdd
  - path: /dev/sdb
"""
    ),
    ])
def test_ceph_volume_command_12(test_input2):

    spec = DriveGroupSpec.from_json(yaml.safe_load(test_input2))
    spec.validate()
    drive = drive_selection.DriveSelection(spec, spec.data_devices.paths)
    cmds = translate.to_ceph_volume(drive, []).run()

    assert (cmds[0] == 'lvm batch --no-auto /dev/sdb --crush-device-class ssd --yes --no-systemd')  # noqa E501
    assert (cmds[1] == 'lvm batch --no-auto /dev/sda --crush-device-class hdd --yes --no-systemd')  # noqa E501


@pytest.mark.parametrize("test_input3",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: hdd
  - path: /dev/sdb
"""
    ),
    ])
def test_ceph_volume_command_13(test_input3):

    spec = DriveGroupSpec.from_json(yaml.safe_load(test_input3))
    spec.validate()
    drive = drive_selection.DriveSelection(spec, spec.data_devices.paths)
    cmds = translate.to_ceph_volume(drive, []).run()

    assert (cmds[0] == 'lvm batch --no-auto /dev/sdb --yes --no-systemd')  # noqa E501
    assert (cmds[1] == 'lvm batch --no-auto /dev/sda --crush-device-class hdd --yes --no-systemd')  # noqa E501


@pytest.mark.parametrize("test_input4",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
data_devices:
  paths:
  - crush_device_class: hdd
"""
    ),
    ])
def test_ceph_volume_command_14(test_input4):

    with pytest.raises(DriveGroupValidationError, match='Device path'):
        spec = DriveGroupSpec.from_json(yaml.safe_load(test_input4))
        spec.validate()


def test_raw_ceph_volume_command_0():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(rotational=True),
                          db_devices=DeviceSelection(rotational=False),
                          method='raw',
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True) +  # data
                              _mk_device(rotational=True) +  # data
                              _mk_device(rotational=False) +  # db
                              _mk_device(rotational=False)  # db
                              )
    exp_cmds = ['raw prepare --bluestore --data /dev/sda --block.db /dev/sdc', 'raw prepare --bluestore --data /dev/sdb --block.db /dev/sdd']
    sel = drive_selection.DriveSelection(spec, inventory)
    cmds = translate.to_ceph_volume(sel, []).run()
    assert all(cmd in exp_cmds for cmd in cmds), f'Expected {exp_cmds} to match {cmds}'

def test_raw_ceph_volume_command_1():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          service_id='foobar',
                          data_devices=DeviceSelection(rotational=True),
                          db_devices=DeviceSelection(rotational=False),
                          method='raw',
                          )
    spec.validate()
    inventory = _mk_inventory(_mk_device(rotational=True) +  # data
                              _mk_device(rotational=True) +  # data
                              _mk_device(rotational=False) # db
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    with pytest.raises(ValueError):
        cmds = translate.to_ceph_volume(sel, []).run()

@pytest.mark.parametrize("test_input5",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
method: raw
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: hdd
  - path: /dev/sdb
    crush_device_class: hdd
  - path: /dev/sdc
    crush_device_class: hdd
db_devices:
  paths:
  - /dev/sdd
  - /dev/sde
  - /dev/sdf

"""
    ),
    ])
def test_raw_ceph_volume_command_2(test_input5):

    spec = DriveGroupSpec.from_json(yaml.safe_load(test_input5))
    spec.validate()
    drive = drive_selection.DriveSelection(spec, spec.data_devices.paths)
    cmds = translate.to_ceph_volume(drive, []).run()

    assert cmds[0] == 'raw prepare --bluestore --data /dev/sda --block.db /dev/sdd --crush-device-class hdd'
    assert cmds[1] == 'raw prepare --bluestore --data /dev/sdb --block.db /dev/sde --crush-device-class hdd'
    assert cmds[2] == 'raw prepare --bluestore --data /dev/sdc --block.db /dev/sdf --crush-device-class hdd'


@pytest.mark.parametrize("test_input6",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
method: raw
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: hdd
  - path: /dev/sdb
    crush_device_class: hdd
  - path: /dev/sdc
    crush_device_class: ssd
db_devices:
  paths:
  - /dev/sdd
  - /dev/sde
  - /dev/sdf

"""
    ),
    ])
def test_raw_ceph_volume_command_3(test_input6):

    spec = DriveGroupSpec.from_json(yaml.safe_load(test_input6))
    spec.validate()
    drive = drive_selection.DriveSelection(spec, spec.data_devices.paths)
    cmds = translate.to_ceph_volume(drive, []).run()

    assert cmds[0] == 'raw prepare --bluestore --data /dev/sda --block.db /dev/sdd --crush-device-class hdd'
    assert cmds[1] == 'raw prepare --bluestore --data /dev/sdb --block.db /dev/sde --crush-device-class hdd'
    assert cmds[2] == 'raw prepare --bluestore --data /dev/sdc --block.db /dev/sdf --crush-device-class ssd'


@pytest.mark.parametrize("test_input7",
[
    (
        """service_type: osd
service_id: testing_drivegroup
placement:
  host_pattern: hostname
method: raw
data_devices:
  paths:
  - path: /dev/sda
    crush_device_class: hdd
  - path: /dev/sdb
    crush_device_class: nvme
  - path: /dev/sdc
    crush_device_class: ssd
db_devices:
  paths:
  - /dev/sdd
  - /dev/sde
  - /dev/sdf
wal_devices:
  paths:
  - /dev/sdg
  - /dev/sdh
  - /dev/sdi

"""
    ),
    ])
def test_raw_ceph_volume_command_4(test_input7):

    spec = DriveGroupSpec.from_json(yaml.safe_load(test_input7))
    spec.validate()
    drive = drive_selection.DriveSelection(spec, spec.data_devices.paths)
    cmds = translate.to_ceph_volume(drive, []).run()

    assert cmds[0] == 'raw prepare --bluestore --data /dev/sda --block.db /dev/sdd --block.wal /dev/sdg --crush-device-class hdd'
    assert cmds[1] == 'raw prepare --bluestore --data /dev/sdb --block.db /dev/sdf --block.wal /dev/sdi --crush-device-class nvme'
    assert cmds[2] == 'raw prepare --bluestore --data /dev/sdc --block.db /dev/sde --block.wal /dev/sdh --crush-device-class ssd'
