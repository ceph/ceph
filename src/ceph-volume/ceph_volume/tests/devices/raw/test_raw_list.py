import os
import uuid

from ceph_volume.devices.raw import list as raw_list
from ceph_volume.util import disk


def test_bluestore_device_realpaths_collects_known_keys(monkeypatch):
    monkeypatch.setattr(os.path, 'realpath', lambda p: p)
    report = {
        'u1': {
            'device': '/dev/sda',
            'device_db': '/dev/sdb',
            'type': 'bluestore',
        }
    }
    paths = raw_list.bluestore_device_realpaths(report)
    assert paths == {'/dev/sda', '/dev/sdb'}


def test_get_seastore_info_skips_bluestore_paths(monkeypatch):
    monkeypatch.setattr(
        raw_list.disk,
        'seastore_raw_device_report',
        lambda dev: {
            'type': 'seastore',
            'device': dev,
            'osd_uuid': '11111111-1111-1111-1111-111111111111',
            'synthetic_osd_uuid': True,
        },
    )
    monkeypatch.setattr(os.path, 'realpath', lambda p: '/same')
    bluestore_paths = {'/same'}
    out = raw_list.get_seastore_info(['/dev/vdc'], bluestore_paths)
    assert out == {}


def test_seastore_raw_device_report_synthetic_uuid(monkeypatch):
    dev = '/dev/vdz'
    monkeypatch.setattr(disk, 'has_seastore_label', lambda _d: True)
    monkeypatch.setattr(os.path, 'realpath', lambda p: p)
    expected = str(
        uuid.uuid5(uuid.NAMESPACE_URL, 'ceph-volume:seastore-raw:' + dev)
    )
    row = disk.seastore_raw_device_report(dev)
    assert row is not None
    assert row['type'] == 'seastore'
    assert row['device'] == dev
    assert row['osd_uuid'] == expected
    assert row['synthetic_osd_uuid'] is True
    assert disk.seastore_raw_device_report(dev)['osd_uuid'] == expected


def test_get_seastore_info_merges_when_not_bluestore(monkeypatch):
    monkeypatch.setattr(
        raw_list.disk,
        'seastore_raw_device_report',
        lambda dev: {
            'type': 'seastore',
            'device': dev,
            'osd_uuid': '22222222-2222-2222-2222-222222222222',
            'synthetic_osd_uuid': True,
        },
    )
    monkeypatch.setattr(os.path, 'realpath', lambda p: p)
    out = raw_list.get_seastore_info(['/dev/vdd'], set())
    u = '22222222-2222-2222-2222-222222222222'
    assert u in out
    assert out[u]['type'] == 'seastore'
    assert out[u]['synthetic_osd_uuid'] is True
