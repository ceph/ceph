from typing import List

import pytest

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from ceph.deployment.drive_selection import DriveSelection
from ceph.deployment.drive_selection.explain import (
    explain_drive_selection,
    FILTER_MISMATCH,
    LIMIT_REACHED,
    NOT_IN_PATHS,
    OTHER_SPEC,
    SELECTED,
    SELECTION_ERROR,
    TAKEN_BY_ROLE,
    UNAVAILABLE,
)
from ceph.deployment.drive_selection.matchers import (
    AllMatcher,
    EqualityMatcher,
    SizeMatcher,
    SubstringMatcher,
    ERROR,
    MATCH,
    MISMATCH,
)
from ceph.deployment.inventory import Device
from ceph.deployment.service_spec import PlacementSpec

TB4 = 4000787030016      # a disk sold as 4 TB
TB1_92 = 1920383410176   # a disk sold as 1.92 TB


def _dev(path: str, rotational: bool, model: str, size: int, **kwargs) -> Device:
    return Device(path, available=kwargs.pop('available', True), sys_api={
        'rotational': '1' if rotational else '0',
        'model': model,
        'vendor': 'ATA',
        'size': size,
        # same computation as ceph-volume: base 1024, labelled TB
        'human_readable_size': '%.2f TB' % (size / 1024 ** 4),
    }, **kwargs)


def _inventory() -> List[Device]:
    return [
        _dev('/dev/sdb', True, 'ST4000NM0035', TB4),
        _dev('/dev/sdc', True, 'ST4000NM0035', TB4),
        _dev('/dev/sdd', False, 'SAMSUNG MZ7L31T9HBLT', TB1_92),
    ]


def _spec(**kwargs) -> DriveGroupSpec:
    return DriveGroupSpec(service_id='hdd_ssd_db',
                          placement=PlacementSpec(host_pattern='*'),
                          **kwargs)


def _roles(expl, path):
    return {r.role: r for d in expl.devices if d.device.path == path for r in d.roles}


class TestMatcherEvaluate:

    def test_size_base_1024_hint(self):
        ev = SizeMatcher('size', '4TB:').evaluate(_dev('/dev/sdb', True, 'X', TB4))
        assert ev.result == MISMATCH
        assert ev.expected == '>= 4TB (4e+12 B)'
        assert ev.actual == "'3.64 TB' (3.64e+12 B)"
        assert 'base 1024' in ev.hint
        assert str(TB4) in ev.hint

    def test_size_no_hint_when_really_too_small(self):
        ev = SizeMatcher('size', '4TB:').evaluate(_dev('/dev/sdd', False, 'X', TB1_92))
        assert ev.result == MISMATCH
        assert ev.hint is None

    def test_size_match(self):
        ev = SizeMatcher('size', '3TB:').evaluate(_dev('/dev/sdb', True, 'X', TB4))
        assert ev.result == MATCH
        assert ev.hint is None

    @pytest.mark.parametrize('value,expected', [
        ('1TB:2TB', 'between 1TB (1e+12 B) and 2TB (2e+12 B)'),
        (':2TB', '<= 2TB (2e+12 B)'),
        ('2TB', '== 2TB (2e+12 B)'),
    ])
    def test_size_describe_expected(self, value, expected):
        assert SizeMatcher('size', value).describe_expected() == expected

    def test_substring_case_hint(self):
        ev = SubstringMatcher('model', 'mz7l3').evaluate(
            _dev('/dev/sdd', False, 'SAMSUNG MZ7L31T9HBLT', TB1_92))
        assert ev.result == MISMATCH
        assert ev.expected == "contains 'mz7l3'"
        assert ev.actual == "'SAMSUNG MZ7L31T9HBLT'"
        assert 'case-sensitive' in ev.hint

    def test_substring_no_hint(self):
        ev = SubstringMatcher('model', 'MZ7L4').evaluate(
            _dev('/dev/sdd', False, 'SAMSUNG MZ7L31T9HBLT', TB1_92))
        assert ev.result == MISMATCH
        assert ev.hint is None

    def test_rotational(self):
        ev = EqualityMatcher('rotational', '0').evaluate(_dev('/dev/sdb', True, 'X', TB4))
        assert ev.result == MISMATCH
        assert ev.expected == '0 (non-rotational)'
        assert ev.actual == '1 (rotational)'
        assert 'queue/rotational' in ev.hint

    def test_missing_attribute(self):
        ev = SubstringMatcher('vendor', 'ATA').evaluate(Device('/dev/sdx', sys_api={}))
        assert ev.result == ERROR
        assert ev.actual is None
        assert 'does not report' in ev.hint

    def test_all(self):
        ev = AllMatcher('all', 'True').evaluate(_dev('/dev/sdb', True, 'X', TB4))
        assert ev.result == MATCH

    @pytest.mark.parametrize('value', ['3TB:', '4TB:', ':4TB', '1TB:5TB', '3.64TB'])
    def test_evaluate_consistent_with_compare(self, value):
        m = SizeMatcher('size', value)
        for dev in _inventory():
            assert (m.evaluate(dev).result == MATCH) == m.compare(dev)


class TestExplainDriveSelection:

    def test_selected(self):
        spec = _spec(data_devices=DeviceSelection(rotational=True),
                     db_devices=DeviceSelection(rotational=False))
        expl = explain_drive_selection(spec, _inventory())
        assert expl.error is None
        assert expl.notes == []
        assert {r.role: r.selected for r in expl.roles} == {
            'data_devices': ['/dev/sdb', '/dev/sdc'],
            'db_devices': ['/dev/sdd'],
        }
        assert _roles(expl, '/dev/sdb')['data_devices'].reason == SELECTED
        assert _roles(expl, '/dev/sdd')['db_devices'].reason == SELECTED
        # an HDD is not a db device: the filter mismatch is the reason that
        # matters, being already taken as data device is only a detail
        role = _roles(expl, '/dev/sdb')['db_devices']
        assert role.reason == FILTER_MISMATCH
        assert role.detail == 'also taken by role: selected as data_devices'

    def test_all_criteria_evaluated(self):
        # AND logic: model would fail too, it must be reported along size
        spec = _spec(data_devices=DeviceSelection(size='4TB:', model='HGST', rotational=True))
        expl = explain_drive_selection(spec, _inventory())
        role = _roles(expl, '/dev/sdb')['data_devices']
        assert role.reason == FILTER_MISMATCH
        assert role.filters_match is False
        assert {c.criterion: c.result for c in role.criteria} == {
            'size': MISMATCH, 'model': MISMATCH, 'rotational': MATCH}
        assert expl.notes == ['data_devices selects no device: no OSD will be created']

    def test_or_logic(self):
        spec = _spec(data_devices=DeviceSelection(size='4TB:', rotational=True),
                     filter_logic='OR')
        expl = explain_drive_selection(spec, _inventory())
        role = _roles(expl, '/dev/sdb')['data_devices']
        assert role.selected
        assert role.filters_match is True
        assert _roles(expl, '/dev/sdd')['data_devices'].reason == FILTER_MISMATCH

    def test_unavailable(self):
        inventory = _inventory()
        inventory[2].available = False
        inventory[2].rejected_reasons = ['Has GPT headers']
        spec = _spec(data_devices=DeviceSelection(rotational=True),
                     db_devices=DeviceSelection(rotational=False))
        expl = explain_drive_selection(spec, inventory)
        role = _roles(expl, '/dev/sdd')['db_devices']
        assert role.reason == UNAVAILABLE
        assert role.detail == 'Has GPT headers'
        # the filters themselves would match
        assert role.filters_match is True
        db = [r for r in expl.roles if r.role == 'db_devices'][0]
        assert db.unsatisfied
        assert any('db_devices is defined but selects no device' in n for n in expl.notes)

    def test_other_spec(self):
        inventory = _inventory()
        inventory[2].available = False
        inventory[2].ceph_device_lvm = True
        inventory[2].lvs = [{'osd_id': '3', 'osdspec_affinity': 'other'}]
        spec = _spec(data_devices=DeviceSelection(rotational=True),
                     db_devices=DeviceSelection(rotational=False))
        expl = explain_drive_selection(spec, inventory)
        role = _roles(expl, '/dev/sdd')['db_devices']
        assert role.reason == OTHER_SPEC
        assert role.detail == 'used by osd.other'

    def test_existing_osd_of_this_spec(self):
        inventory = _inventory()
        for dev in inventory:
            dev.available = False
            dev.ceph_device_lvm = True
            dev.lvs = [{'osd_id': '0', 'osdspec_affinity': 'hdd_ssd_db'}]
        spec = _spec(data_devices=DeviceSelection(rotational=True),
                     db_devices=DeviceSelection(rotational=False))
        expl = explain_drive_selection(spec, inventory, existing_daemons=2)
        role = _roles(expl, '/dev/sdb')['data_devices']
        assert role.reason == SELECTED
        assert role.detail == 'already used by an OSD of this spec'
        assert expl.new_data_devices == []
        assert expl.notes == ['all selected data devices already back OSDs of this spec: '
                              'no new OSD will be created']

    def test_taken_by_role(self):
        # wal_devices is assigned before db_devices
        spec = _spec(data_devices=DeviceSelection(rotational=True),
                     db_devices=DeviceSelection(rotational=False),
                     wal_devices=DeviceSelection(rotational=False))
        expl = explain_drive_selection(spec, _inventory())
        assert _roles(expl, '/dev/sdd')['wal_devices'].reason == SELECTED
        role = _roles(expl, '/dev/sdd')['db_devices']
        assert role.reason == TAKEN_BY_ROLE
        assert role.detail == 'selected as wal_devices'
        assert role.filters_match is True

    def test_limit_reached(self):
        spec = _spec(data_devices=DeviceSelection(rotational=True, limit=1))
        expl = explain_drive_selection(spec, _inventory())
        assert _roles(expl, '/dev/sdb')['data_devices'].reason == SELECTED
        role = _roles(expl, '/dev/sdc')['data_devices']
        assert role.reason == LIMIT_REACHED
        assert role.detail == 'limit: 1'

    def test_explicit_paths(self):
        spec = _spec(data_devices=DeviceSelection(paths=['/dev/sdb']))
        expl = explain_drive_selection(spec, _inventory())
        assert _roles(expl, '/dev/sdb')['data_devices'].reason == SELECTED
        role = _roles(expl, '/dev/sdc')['data_devices']
        assert role.reason == NOT_IN_PATHS
        assert role.criteria == []

    def test_selection_error(self):
        # the inventory of /dev/sdx doesn't report a vendor: DriveSelection raises
        inventory = _inventory() + [Device('/dev/sdx', available=True, sys_api={})]
        spec = _spec(data_devices=DeviceSelection(vendor='ATA'))
        expl = explain_drive_selection(spec, inventory)
        assert expl.error is not None
        role = _roles(expl, '/dev/sdx')['data_devices']
        assert role.reason == SELECTION_ERROR
        assert role.criteria[0].result == ERROR
        assert _roles(expl, '/dev/sdb')['data_devices'].filters_match is True

    @pytest.mark.parametrize('spec', [
        _spec(data_devices=DeviceSelection(rotational=True),
              db_devices=DeviceSelection(rotational=False)),
        _spec(data_devices=DeviceSelection(size='3TB:'),
              db_devices=DeviceSelection(model='MZ7L3')),
        _spec(data_devices=DeviceSelection(all=True, limit=2)),
        _spec(data_devices=DeviceSelection(rotational=True, limit=1),
              wal_devices=DeviceSelection(rotational=False),
              db_devices=DeviceSelection(rotational=False)),
    ])
    def test_consistent_with_drive_selection(self, spec):
        inventory = _inventory()
        sel = DriveSelection(spec, inventory)
        expl = explain_drive_selection(spec, inventory)
        for role in expl.roles:
            assert role.selected == [d.path for d in getattr(sel, role.role)()]
            for dev in expl.devices:
                ev = _roles(expl, dev.device.path)[role.role]
                assert ev.selected == (dev.device.path in role.selected)

    def test_to_json(self):
        spec = _spec(data_devices=DeviceSelection(size='4TB:'),
                     db_devices=DeviceSelection(model='MZ7L3'))
        j = explain_drive_selection(spec, _inventory()).to_json()
        assert j['filter_logic'] == 'AND'
        dev = [d for d in j['devices'] if d['path'] == '/dev/sdb'][0]
        assert dev['size'] == '3.64 TB'
        assert dev['size_bytes'] == TB4
        data = [r for r in dev['roles'] if r['role'] == 'data_devices'][0]
        assert data['reason'] == FILTER_MISMATCH
        assert data['criteria'][0]['criterion'] == 'size'
        assert data['criteria'][0]['result'] == MISMATCH
