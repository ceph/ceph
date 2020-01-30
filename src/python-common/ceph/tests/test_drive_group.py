import pytest

from ceph.deployment import drive_selection, translate
from ceph.deployment.inventory import Device
from ceph.tests.utils import _mk_inventory, _mk_device
from ceph.deployment.drive_group import DriveGroupSpec, DriveGroupSpecs, \
                                        DeviceSelection, DriveGroupValidationError


def test_DriveGroup():
    dg_json = {'testing_drivegroup':
               {'host_pattern': 'hostname',
                'data_devices': {'paths': ['/dev/sda']}
                }
               }

    dgs = DriveGroupSpecs(dg_json)
    for dg in dgs.drive_groups:
        assert dg.hosts(['hostname']) == ['hostname']
        assert dg.name == 'testing_drivegroup'
        assert all([isinstance(x, Device) for x in dg.data_devices.paths])
        assert dg.data_devices.paths[0].path == '/dev/sda'


def test_DriveGroup_fail():
    with pytest.raises(DriveGroupValidationError):
        DriveGroupSpec.from_json({})


def test_drivegroup_pattern():
    dg = DriveGroupSpec('node[1-3]', DeviceSelection(all=True))
    assert dg.hosts(['node{}'.format(i) for i in range(10)]) == ['node1', 'node2', 'node3']


def test_drive_selection():
    devs = DeviceSelection(paths=['/dev/sda'])
    spec = DriveGroupSpec('node_name', data_devices=devs)
    assert all([isinstance(x, Device) for x in spec.data_devices.paths])
    assert spec.data_devices.paths[0].path == '/dev/sda'

    with pytest.raises(DriveGroupValidationError, match='exclusive'):
        DeviceSelection(paths=['/dev/sda'], rotational=False)


def test_ceph_volume_command_0():
    spec = DriveGroupSpec(host_pattern='*',
                          data_devices=DeviceSelection(all=True)
                          )
    inventory = _mk_inventory(_mk_device()*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(spec, sel).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --yes --no-systemd'


def test_ceph_volume_command_1():
    spec = DriveGroupSpec(host_pattern='*',
                          data_devices=DeviceSelection(rotational=True),
                          db_devices=DeviceSelection(rotational=False)
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(spec, sel).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --yes --no-systemd')


def test_ceph_volume_command_2():
    spec = DriveGroupSpec(host_pattern='*',
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G')
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2 +
                              _mk_device(rotational=False)*2 +
                              _mk_device(size="10.0 GB", rotational=False)*2
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(spec, sel).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --wal-devices /dev/sde /dev/sdf '
                   '--yes --no-systemd')


def test_ceph_volume_command_3():
    spec = DriveGroupSpec(host_pattern='*',
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G'),
                          encrypted=True
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2 +
                              _mk_device(rotational=False)*2 +
                              _mk_device(size="10.0 GB", rotational=False)*2
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(spec, sel).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd '
                   '--wal-devices /dev/sde /dev/sdf --dmcrypt '
                   '--yes --no-systemd')


def test_ceph_volume_command_4():
    spec = DriveGroupSpec(host_pattern='*',
                          data_devices=DeviceSelection(size='200GB:350GB', rotational=True),
                          db_devices=DeviceSelection(size='200GB:350GB', rotational=False),
                          wal_devices=DeviceSelection(size='10G'),
                          block_db_size='500M',
                          block_wal_size='500M',
                          osds_per_device=3,
                          encrypted=True
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2 +
                              _mk_device(rotational=False)*2 +
                              _mk_device(size="10.0 GB", rotational=False)*2
                              )
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(spec, sel).run()
    assert cmd == ('lvm batch --no-auto /dev/sda /dev/sdb '
                   '--db-devices /dev/sdc /dev/sdd --wal-devices /dev/sde /dev/sdf '
                   '--block-wal-size 500M --block-db-size 500M --dmcrypt '
                   '--osds-per-device 3 --yes --no-systemd')


def test_ceph_volume_command_5():
    spec = DriveGroupSpec(host_pattern='*',
                          data_devices=DeviceSelection(rotational=True),
                          objectstore='filestore'
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(spec, sel).run()
    assert cmd == 'lvm batch --no-auto /dev/sda /dev/sdb --filestore --yes --no-systemd'


def test_ceph_volume_command_6():
    spec = DriveGroupSpec(host_pattern='*',
                          data_devices=DeviceSelection(rotational=False),
                          journal_devices=DeviceSelection(rotational=True),
                          journal_size='500M',
                          objectstore='filestore'
                          )
    inventory = _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)*2)
    sel = drive_selection.DriveSelection(spec, inventory)
    cmd = translate.to_ceph_volume(spec, sel).run()
    assert cmd == ('lvm batch --no-auto /dev/sdc /dev/sdd '
                   '--journal-size 500M --journal-devices /dev/sda /dev/sdb '
                   '--filestore --yes --no-systemd')
