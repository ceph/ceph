# flake8: noqa
import pytest
import yaml

from ceph.deployment import drive_selection, translate
from ceph.deployment.hostspec import HostSpec
from ceph.deployment.inventory import Device
from ceph.deployment.service_spec import PlacementSpec, ServiceSpecValidationError
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

@pytest.mark.parametrize("test_input",
[
    (
        {}
    ),
    (
        yaml.safe_load("""
service_type: osd
service_id: mydg
placement:
  host_pattern: '*'
data_devices:
  limit: 1
""")
    )
])
def test_DriveGroup_fail(test_input):
    with pytest.raises(ServiceSpecValidationError):
        DriveGroupSpec.from_json(test_input)



def test_drivegroup_pattern():
    dg = DriveGroupSpec(PlacementSpec(host_pattern='node[1-3]'), data_devices=DeviceSelection(all=True))
    assert dg.placement.filter_matching_hostspecs([HostSpec('node{}'.format(i)) for i in range(10)]) == ['node1', 'node2', 'node3']


def test_drive_selection():
    devs = DeviceSelection(paths=['/dev/sda'])
    spec = DriveGroupSpec(PlacementSpec('node_name'), data_devices=devs)
    assert all([isinstance(x, Device) for x in spec.data_devices.paths])
    assert spec.data_devices.paths[0].path == '/dev/sda'

    with pytest.raises(DriveGroupValidationError, match='exclusive'):
        DeviceSelection(paths=['/dev/sda'], rotational=False)


def test_ceph_volume_command_0():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          data_devices=DeviceSelection(all=True)
                          )
    inventory = _mk_inventory(_mk_device()*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd'


def test_ceph_volume_command_1():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          data_devices=DeviceSelection(rotational=True),
                          db_devices=DeviceSelection(rotational=False)
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --yes --no-systemd')


def test_ceph_volume_command_2():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G')
                          )
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
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G'),
                          encrypted=True
                          )
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
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G'),
                          block_db_size='500M',
                          block_wal_size='500M',
                          osds_per_device=3,
                          encrypted=True
                          )
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
                          data_devices=DeviceSelection(rotational=True),
                          objectstore='filestore'
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --filestore --yes --no-systemd'


def test_ceph_volume_command_6():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          data_devices=DeviceSelection(rotational=False),
                          journal_devices=DeviceSelection(rotational=True),
                          journal_size='500M',
                          objectstore='filestore'
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, []).run()
    assert cmd == ('lvm batch --no-auto /dev/sdc /dev/sdd '
                   '--journal-size 500M --journal-devices /dev/sda /dev/sdb '
                   '--filestore --yes --no-systemd')


def test_ceph_volume_command_7():
    spec = DriveGroupSpec(placement=PlacementSpec(host_pattern='*'),
                          data_devices=DeviceSelection(all=True),
                          osd_id_claims={'host1': ['0', '1']}
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(sel, ['0', '1']).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --osd-ids 0 1 --yes --no-systemd'
