import json

from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from rbd_support.schedule import (Interval, LevelSpec, Schedule, Schedules,
                                  StartTime)


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.mark.parametrize('spec,minutes,canonical', [
    ('30', 30, '30m'),
    ('30m', 30, '30m'),
    ('90m', 90, '90m'),
    ('120m', 120, '2h'),
    ('2h', 120, '2h'),
    ('1d', 60 * 24, '1d'),
    ('14d', 60 * 24 * 14, '2w'),
    ('1w', 60 * 24 * 7, '1w'),
])
def test_interval_from_string(spec, minutes, canonical):
    interval = Interval.from_string(spec)
    assert interval.minutes == minutes
    assert interval.to_string() == canonical
    assert Interval.from_string(canonical) == interval


@pytest.mark.parametrize('spec', ['', 'h', '1y', '1.5h', '-1h', '1 h'])
def test_interval_from_string_invalid(spec):
    with pytest.raises(ValueError):
        Interval.from_string(spec)


def test_start_time_is_normalized_to_utc_minutes():
    start_time = StartTime.from_string('2026-03-04T05:06:07+02:00')
    assert start_time is not None
    assert start_time.dt == utc(2026, 3, 4, 3, 6)
    assert start_time.to_string() == '2026-03-04 03:06:00'

    # a time without an offset is taken as UTC
    naive = StartTime.from_string('2026-03-04T03:06')
    assert naive == start_time


def test_start_time_empty():
    assert StartTime.from_string(None) is None
    assert StartTime.from_string('') is None


def test_start_time_legacy_format():
    # schedules saved by older releases only stored a time of day
    with pytest.raises(ValueError):
        StartTime.from_string('2:30 PM')
    start_time = StartTime.from_string('2:30 PM', allow_legacy=True)
    assert start_time is not None
    assert start_time.dt == utc(1970, 1, 1, 14, 30)

    with pytest.raises(ValueError):
        StartTime.from_string('not a time', allow_legacy=True)


@pytest.mark.parametrize('now,expected', [
    # before the start time: the first run is the start time itself
    (utc(2026, 1, 1, 8, 0), utc(2026, 1, 1, 9, 0)),
    # exactly on a period boundary: run now
    (utc(2026, 1, 1, 9, 0), utc(2026, 1, 1, 9, 0)),
    (utc(2026, 1, 1, 12, 0), utc(2026, 1, 1, 12, 0)),
    # in between: the next period boundary
    (utc(2026, 1, 1, 10, 7), utc(2026, 1, 1, 11, 0)),
    (utc(2026, 1, 1, 10, 59), utc(2026, 1, 1, 11, 0)),
])
def test_next_run_with_start_time(now, expected):
    schedule = Schedule('rbd/')
    schedule.add(Interval.from_string('1h'),
                 StartTime.from_string('2026-01-01T09:00:00Z'))
    assert schedule.next_run(now, '1/') == expected


def test_next_run_without_start_time_is_phase_shifted_per_entity():
    period = 60 * 6
    schedule = Schedule('rbd/')
    schedule.add(Interval(period))
    now = utc(2026, 5, 17, 13, 41)

    for entity_id in ['1/', '1/ns', '2/', '1//abcdef']:
        offset = Schedule._compute_phase_offset_minutes(entity_id, period)
        assert 0 <= offset < period

        next_run = schedule.next_run(now, entity_id)
        assert now <= next_run < now + timedelta(minutes=period)
        # the run times of an entity are spaced a whole period apart from
        # its own offset, so the same entity always lands on the same slots
        since_anchor = next_run - (EPOCH + timedelta(minutes=offset))
        assert since_anchor % timedelta(minutes=period) == timedelta(0)
        assert schedule.next_run(now, entity_id) == next_run


def test_next_run_picks_the_earliest_item():
    schedule = Schedule('rbd/')
    schedule.add(Interval.from_string('1d'),
                 StartTime.from_string('2026-01-01T00:00:00Z'))
    schedule.add(Interval.from_string('1h'),
                 StartTime.from_string('2026-01-01T00:30:00Z'))
    now = utc(2026, 1, 2, 10, 10)
    assert schedule.next_run(now, '1/') == utc(2026, 1, 2, 10, 30)


def test_next_run_without_items():
    with pytest.raises(ValueError):
        Schedule('rbd/').next_run(utc(2026, 1, 1), '1/')


def test_schedule_add_remove():
    schedule = Schedule('rbd/')
    start_time = StartTime.from_string('2026-01-01T00:00:00Z')
    schedule.add(Interval.from_string('1h'))
    schedule.add(Interval.from_string('60m'))
    schedule.add(Interval.from_string('1d'), start_time)
    assert len(schedule) == 2

    # an item is only removed when both the interval and start time match
    schedule.remove(Interval.from_string('1d'))
    assert len(schedule) == 2
    schedule.remove(Interval.from_string('1d'), start_time)
    assert len(schedule) == 1


def test_schedule_json_round_trip():
    schedule = Schedule('rbd/')
    schedule.add(Interval.from_string('1h'))
    schedule.add(Interval.from_string('1d'),
                 StartTime.from_string('2026-01-01T12:00:00Z'))

    loaded = Schedule.from_json('rbd/', schedule.to_json())
    assert loaded.items == schedule.items
    assert sorted(json.loads(loaded.to_json()),
                  key=lambda i: i['interval']) == [
        {'interval': '1d', 'start_time': '2026-01-01 12:00:00'},
        {'interval': '1h', 'start_time': None},
    ]


def test_schedule_from_json_legacy_start_time():
    loaded = Schedule.from_json(
        'rbd/', '[{"interval": "1h", "start_time": "2:30 PM"}]')
    assert loaded.items == {(Interval(60),
                             StartTime(utc(1970, 1, 1, 14, 30)))}


@pytest.mark.parametrize('val', [
    'not json',
    '[{"start_time": null}]',
    '[1]',
    '[{"interval": "1y"}]',
])
def test_schedule_from_json_invalid(val):
    with pytest.raises(ValueError):
        Schedule.from_json('rbd/', val)


GLOBAL = LevelSpec.make_global()
POOL = LevelSpec.from_pool_spec(1, 'rbd')
POOL_NS = LevelSpec.from_pool_spec(1, 'rbd', 'ns')
POOL_NS_IMAGE = LevelSpec('rbd/ns/img', '1/ns/abc', '1', 'ns', 'abc')
OTHER_POOL = LevelSpec.from_pool_spec(2, 'other')


def test_level_spec_from_pool_spec():
    assert (POOL.name, POOL.id, POOL.pool_id, POOL.namespace) == \
        ('rbd/', '1', '1', None)
    assert (POOL_NS.name, POOL_NS.id, POOL_NS.namespace) == \
        ('rbd/ns/', '1/ns', 'ns')
    assert GLOBAL.is_global()
    assert not POOL.is_global()


@pytest.mark.parametrize('child,parent,expected', [
    (POOL, GLOBAL, True),
    (POOL_NS_IMAGE, GLOBAL, True),
    (GLOBAL, GLOBAL, False),
    (POOL_NS, POOL, True),
    (POOL_NS_IMAGE, POOL, True),
    (POOL_NS_IMAGE, POOL_NS, True),
    (POOL, POOL, False),
    (POOL, POOL_NS, False),
    (POOL_NS, OTHER_POOL, False),
    (POOL_NS, LevelSpec.from_pool_spec(1, 'rbd', 'ns2'), False),
])
def test_level_spec_is_child_of(child, parent, expected):
    assert child.is_child_of(parent) == expected


@pytest.mark.parametrize('a,b,expected', [
    (GLOBAL, POOL_NS_IMAGE, True),
    (POOL, POOL_NS_IMAGE, True),
    (POOL, OTHER_POOL, False),
    (POOL_NS, LevelSpec.from_pool_spec(1, 'rbd', 'ns2'), False),
    (POOL_NS_IMAGE, LevelSpec('rbd/ns/img2', '1/ns/def', '1', 'ns', 'def'),
     False),
])
def test_level_spec_intersects(a, b, expected):
    assert a.intersects(b) == expected
    assert b.intersects(a) == expected


def test_level_spec_matches():
    assert GLOBAL.matches('2', 'ns', 'abc')
    assert POOL.matches('1', '')
    assert POOL.matches('1', 'ns', 'abc')
    assert not POOL.matches('2', '')
    assert POOL_NS.matches('1', 'ns')
    assert not POOL_NS.matches('1', '')
    assert POOL_NS_IMAGE.matches('1', 'ns', 'abc')
    assert not POOL_NS_IMAGE.matches('1', 'ns', 'def')


def test_level_spec_from_name(module):
    module.rados.pool_lookup.side_effect = \
        lambda name: {'rbd': 1, 'cephfs_data': 3}.get(name)
    image = mock.MagicMock()
    image.id.return_value = 'abc'
    with mock.patch('rbd_support.schedule.rbd') as rbd:
        rbd.Image.return_value.__enter__.return_value = image

        assert LevelSpec.from_name(module, '').is_global()

        spec = LevelSpec.from_name(module, 'rbd/')
        assert (spec.id, spec.pool_id, spec.namespace) == ('1', '1', None)

        # 'rbd//img' and 'rbd/img' name the same image in the default
        # namespace
        for name in ['rbd//img', 'rbd/img']:
            spec = LevelSpec.from_name(module, name)
            assert (spec.name, spec.id, spec.namespace, spec.image_id) == \
                ('rbd/img', '1//abc', '', 'abc')

        with pytest.raises(ValueError, match='image level is not allowed'):
            LevelSpec.from_name(module, 'rbd/img', allow_image_level=False)
        with pytest.raises(ValueError, match='does not exist'):
            LevelSpec.from_name(module, 'nopool/')
        with pytest.raises(ValueError, match='not an RBD pool'):
            LevelSpec.from_name(module, 'cephfs_data/')
        with pytest.raises(ValueError, match='failed to parse'):
            LevelSpec.from_name(module, 'a/b/c/d')


@pytest.fixture
def handler(module):
    h = mock.MagicMock()
    h.MODULE_OPTION_NAME = 'test_schedule'
    h.SCHEDULE_OID = 'rbd_test_schedule'
    h.module = module
    return h


def test_schedules_moves_misplaced_global_config(handler, module):
    # older releases stored the global schedule as a localized option
    module.get_localized_module_option.return_value = '[{"interval": "1h"}]'
    Schedules(handler)
    module.set_module_option.assert_called_once_with(
        'test_schedule', '[{"interval": "1h"}]')
    module.set_localized_module_option.assert_called_once_with(
        'test_schedule', None)


def test_schedules_add_remove_global(handler, module):
    schedules = Schedules(handler)
    schedules.add(GLOBAL, '1h', None)
    assert len(schedules) == 1
    saved = module.set_module_option.call_args[0]
    assert saved[0] == 'test_schedule'
    assert json.loads(saved[1]) == [{'interval': '1h', 'start_time': None}]

    # an invalid interval must fail without touching the existing schedule
    with pytest.raises(ValueError):
        schedules.remove(GLOBAL, '1y', None)
    assert len(schedules) == 1

    # no interval removes the whole schedule for that level
    schedules.remove(GLOBAL, None, None)
    assert len(schedules) == 0
    assert GLOBAL.id not in schedules.level_specs
    module.set_module_option.assert_called_with('test_schedule', None)


def test_schedules_add_pool_level_is_saved_to_pool_omap(handler, module):
    schedules = Schedules(handler)
    schedules.add(POOL_NS, '2h', '2026-01-01T00:00:00Z')

    module.rados.open_ioctx2.assert_called_once_with(1)
    ioctx = module.rados.open_ioctx2.return_value.__enter__.return_value
    keys, vals = ioctx.set_omap.call_args[0][1:]
    assert keys == ('1/ns', )
    assert json.loads(vals[0]) == [
        {'interval': '2h', 'start_time': '2026-01-01 00:00:00'}]
    ioctx.operate_write_op.assert_called_once_with(
        mock.ANY, 'rbd_test_schedule')


def test_schedules_remove_last_item_drops_level(handler, module):
    schedules = Schedules(handler)
    schedules.add(POOL, '1h', None)
    schedules.add(POOL, '1d', None)
    schedules.remove(POOL, '1h', None)
    assert POOL.id in schedules.level_specs
    schedules.remove(POOL, '1d', None)
    assert POOL.id not in schedules.level_specs
    assert schedules.find('1', '') is None
    ioctx = module.rados.open_ioctx2.return_value.__enter__.return_value
    ioctx.remove_omap_keys.assert_called_once_with(mock.ANY, ('1', ))


def _schedules_with(handler, *level_specs):
    schedules = Schedules(handler)
    for level_spec in level_specs:
        schedule = Schedule(level_spec.name)
        schedule.add(Interval.from_string('1h'))
        schedules.level_specs[level_spec.id] = level_spec
        schedules.schedules[level_spec.id] = schedule
    return schedules


def test_schedules_find_falls_back_to_parent_levels(handler):
    schedules = _schedules_with(handler, GLOBAL, POOL, POOL_NS_IMAGE)
    assert schedules.find('1', 'ns', 'abc').name == POOL_NS_IMAGE.name
    assert schedules.find('1', 'ns', 'def').name == POOL.name
    assert schedules.find('1', 'ns').name == POOL.name
    assert schedules.find('1', '').name == POOL.name
    assert schedules.find('2', '').name == GLOBAL.name

    schedules = _schedules_with(handler, POOL)
    assert schedules.find('2', '', 'abc') is None


def test_schedules_load_from_pool_drops_stale_keys(handler, module):
    schedules = Schedules(handler)
    module.rados.pool_reverse_lookup.return_value = 'rbd'
    ioctx = mock.MagicMock()
    ioctx.get_pool_name.return_value = 'rbd'
    ioctx.get_omap_vals.side_effect = [
        (iter([('1', b'[{"interval": "1h"}]'),
               ('1/ns', b'[{"interval": "1d"}]'),
               ('bogus key', b'[{"interval": "1h"}]')]), 0),
        (iter([]), 0),
    ]
    with mock.patch('rbd_support.schedule.rbd') as rbd:
        rbd.RBD.return_value.namespace_list.return_value = []
        schedules.load_from_pool(ioctx, None, None)

    # '1/ns' names a namespace that no longer exists
    assert list(schedules.schedules) == ['1']
    assert schedules.find('1', '').to_list() == [
        {'interval': '1h', 'start_time': None}]
    ioctx.remove_omap_keys.assert_called_once_with(
        mock.ANY, ['1/ns', 'bogus key'])


def test_schedules_to_list(handler):
    other_image = LevelSpec('rbd/ns/img2', '1/ns/def', '1', 'ns', 'def')
    schedules = _schedules_with(handler, POOL, POOL_NS_IMAGE)

    assert set(schedules.to_list(GLOBAL)) == {POOL.id, POOL_NS_IMAGE.id}
    assert set(schedules.to_list(POOL)) == {POOL.id, POOL_NS_IMAGE.id}
    # a level without its own schedule lists the schedule it inherits
    assert set(schedules.to_list(other_image)) == {POOL.id}
    assert set(schedules.to_list(POOL_NS)) == {POOL.id, POOL_NS_IMAGE.id}
    assert schedules.to_list(OTHER_POOL) == {}
