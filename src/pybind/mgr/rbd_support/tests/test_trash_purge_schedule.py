import json

from datetime import datetime, timedelta, timezone

import pytest

from rbd_support.schedule import Interval, LevelSpec, Schedule, Schedules
from rbd_support.trash_purge_schedule import TrashPurgeScheduleHandler


@pytest.fixture
def handler(module):
    h = TrashPurgeScheduleHandler(module)
    h.queue = {}
    h.pools = {'1': {'': 'rbd', 'ns': 'rbd'}}
    h.schedules = Schedules(h)
    return h


def add_schedule(handler, level_spec, interval='1h'):
    schedule = Schedule(level_spec.name)
    schedule.add(Interval.from_string(interval))
    handler.schedules.level_specs[level_spec.id] = level_spec
    handler.schedules.schedules[level_spec.id] = schedule


def queued(handler):
    return sorted(ns for nss in handler.queue.values() for ns in nss)


def test_enqueue_uses_the_inherited_schedule(handler):
    add_schedule(handler, LevelSpec.from_pool_spec(1, 'rbd'))
    now = datetime.now(timezone.utc)
    handler.enqueue(now, '1', '')
    handler.enqueue(now, '1', 'ns')
    # namespaces of pools without a schedule are not queued
    handler.enqueue(now, '2', '')
    assert queued(handler) == [('1', ''), ('1', 'ns')]

    # queueing the same namespace again is a no-op
    handler.enqueue(now, '1', '')
    assert queued(handler) == [('1', ''), ('1', 'ns')]

    schedule = handler.schedules.find('1', '')
    for schedule_time, nss in handler.queue.items():
        for pool_id, namespace in nss:
            assert schedule_time == schedule.next_run(
                now, '{}/{}'.format(pool_id, namespace))
            assert now <= schedule_time < now + timedelta(hours=1)


def test_dequeue(handler):
    assert handler.dequeue() == (None, 1000.0)

    now = datetime.now(timezone.utc)
    handler.queue[now + timedelta(minutes=10)] = [('1', 'ns')]
    ns_spec, wait = handler.dequeue()
    assert ns_spec is None
    assert 590 < wait <= 600

    due = now - timedelta(minutes=1)
    handler.queue[due] = [('1', ''), ('2', '')]
    first, wait = handler.dequeue()
    assert wait == 0.0
    second, _ = handler.dequeue()
    assert sorted([first, second]) == [('1', ''), ('2', '')]
    # the due slot is dropped once it is empty, the future one is kept
    assert list(handler.queue) == [now + timedelta(minutes=10)]


def test_remove_from_queue(handler):
    now = datetime.now(timezone.utc)
    handler.queue = {now: [('1', ''), ('1', 'ns')],
                     now + timedelta(hours=1): [('1', 'ns')]}
    handler.remove_from_queue('1', 'ns')
    assert handler.queue == {now: [('1', '')]}
    handler.remove_from_queue('1', '')
    assert handler.queue == {}


def test_refresh_queue_follows_pool_changes(handler):
    add_schedule(handler, LevelSpec.make_global())
    now = datetime.now(timezone.utc)
    handler.enqueue(now, '1', '')
    handler.enqueue(now, '1', 'ns')

    # namespace 'ns' was removed and pool 2 was added
    current_pools = {'1': {'': 'rbd'}, '2': {'': 'other'}}
    with handler.lock:
        handler.refresh_queue(current_pools)
    assert queued(handler) == [('1', ''), ('2', '')]


def test_rebuild_queue_keeps_due_entries(handler):
    pool = LevelSpec.from_pool_spec(1, 'rbd')
    add_schedule(handler, pool)
    now = datetime.now(timezone.utc)
    due = now - timedelta(minutes=5)
    handler.queue = {due: [('1', '')],
                     now + timedelta(days=1): [('1', 'ns')]}

    # the schedule changed, so everything not yet due is recomputed
    handler.schedules.schedules[pool.id] = Schedule(pool.name)
    handler.schedules.schedules[pool.id].add(Interval.from_string('5m'))
    with handler.lock:
        handler.rebuild_queue()

    assert handler.queue[due] == [('1', '')]
    assert all(t <= now + timedelta(minutes=6) for t in handler.queue)
    assert queued(handler) == [('1', ''), ('1', ''), ('1', 'ns')]


def test_status_filters_by_level_spec(handler):
    handler.pools['2'] = {'': 'other'}
    t1 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc)
    handler.queue = {t2: [('2', '')], t1: [('1', 'ns'), ('1', '')]}

    r, out, _ = handler.status(LevelSpec.make_global())
    assert r == 0
    assert json.loads(out)['scheduled'] == [
        {'schedule_time': '2026-01-01 10:00:00', 'pool_id': '1',
         'pool_name': 'rbd', 'namespace': 'ns'},
        {'schedule_time': '2026-01-01 10:00:00', 'pool_id': '1',
         'pool_name': 'rbd', 'namespace': ''},
        {'schedule_time': '2026-01-01 11:00:00', 'pool_id': '2',
         'pool_name': 'other', 'namespace': ''},
    ]

    _, out, _ = handler.status(LevelSpec.from_pool_spec(1, 'rbd', 'ns'))
    assert [(s['pool_id'], s['namespace'])
            for s in json.loads(out)['scheduled']] == [('1', 'ns')]
