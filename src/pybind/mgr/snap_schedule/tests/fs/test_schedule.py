import datetime
import json
import pytest
import random
import sqlite3
from ...fs.schedule import Schedule, parse_retention

SELECT_ALL = ('select * from schedules s'
              ' INNER JOIN schedules_meta sm'
              ' ON sm.schedule_id = s.id')


def assert_updated(new, old, update_expected={}):
    '''
    This helper asserts that an object new has been updated in the
    attributes in the dict updated AND has not changed in other attributes
    compared to old.
    if update expected is the empty dict, equality is checked
    '''

    for var in vars(new):
        if var in update_expected:
            expected_val = update_expected.get(var)
            new_val = getattr(new, var)
            if isinstance(expected_val, datetime.datetime):
                assert new_val.year == expected_val.year
                assert new_val.month == expected_val.month
                assert new_val.day == expected_val.day
                assert new_val.hour == expected_val.hour
                assert new_val.minute == expected_val.minute
                assert new_val.second == expected_val.second
            else:
                assert new_val == expected_val, f'new did not update value for {var}'
        else:
            expected_val = getattr(old, var)
            new_val = getattr(new, var)
            if isinstance(expected_val, datetime.datetime):
                assert new_val.year == expected_val.year
                assert new_val.month == expected_val.month
                assert new_val.day == expected_val.day
                assert new_val.hour == expected_val.hour
                assert new_val.minute == expected_val.minute
                assert new_val.second == expected_val.second
            else:
                assert new_val == expected_val, f'new changed unexpectedly in value for {var}'


class TestSchedule(object):
    '''
    Test the schedule class basics and that its methods update self as expected
    '''

    def test_start_default_midnight(self, simple_schedule):
        now = datetime.datetime.now(datetime.timezone.utc)
        assert simple_schedule.start.second == 0
        assert simple_schedule.start.minute == 0
        assert simple_schedule.start.hour == 0
        assert simple_schedule.start.day == now.day
        assert simple_schedule.start.month == now.month
        assert simple_schedule.start.year == now.year
        assert simple_schedule.start.tzinfo == now.tzinfo

    def test_created_now(self, simple_schedule):
        now = datetime.datetime.now(datetime.timezone.utc)
        assert simple_schedule.created.minute == now.minute
        assert simple_schedule.created.hour == now.hour
        assert simple_schedule.created.day == now.day
        assert simple_schedule.created.month == now.month
        assert simple_schedule.created.year == now.year
        assert simple_schedule.created.tzinfo == now.tzinfo

    def test_repeat_valid(self, simple_schedule):
        repeat = simple_schedule.repeat
        assert isinstance(repeat, int)

    def test_store_single(self, db, simple_schedule):
        simple_schedule.store_schedule(db)
        row = ()
        with db:
            row = db.execute(SELECT_ALL).fetchone()

        db_schedule = Schedule._from_db_row(row, simple_schedule.fs)

        assert_updated(db_schedule, simple_schedule)

    def test_store_multiple(self, db, simple_schedules):
        [s.store_schedule(db) for s in simple_schedules]

        rows = []
        with db:
            rows = db.execute(SELECT_ALL).fetchall()

        assert len(rows) == len(simple_schedules)

    def test_update_last(self, db, simple_schedule):
        simple_schedule.store_schedule(db)

        with db:
            _ = db.execute(SELECT_ALL).fetchone()

        first_time = datetime.datetime.now(datetime.timezone.utc)
        simple_schedule.update_last(first_time, db)

        with db:
            after = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after, simple_schedule.fs),
                       simple_schedule)

        second_time = datetime.datetime.now(datetime.timezone.utc)
        simple_schedule.update_last(second_time, db)

        with db:
            after2 = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after2, simple_schedule.fs),
                       simple_schedule)

    def test_set_inactive_active(self, db, simple_schedule):
        simple_schedule.store_schedule(db)

        with db:
            _ = db.execute(SELECT_ALL).fetchone()

        simple_schedule.set_inactive(db)

        with db:
            after = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after, simple_schedule.fs),
                       simple_schedule)

        simple_schedule.set_active(db)

        with db:
            after2 = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after2, simple_schedule.fs),
                       simple_schedule)

    def test_update_pruned(self, db, simple_schedule):
        simple_schedule.store_schedule(db)

        with db:
            _ = db.execute(SELECT_ALL).fetchone()

        now = datetime.datetime.now(datetime.timezone.utc)
        pruned_count = random.randint(1, 1000)

        simple_schedule.update_pruned(now, db, pruned_count)

        with db:
            after = db.execute(SELECT_ALL).fetchone()

        assert_updated(Schedule._from_db_row(after, simple_schedule.fs),
                       simple_schedule)

    # TODO test get_schedules and list_schedules


class TestScheduleDB(object):
    '''
    This class tests that Schedules methods update the DB correctly
    '''

    def test_update_last(self, db, simple_schedule):
        simple_schedule.store_schedule(db)

        with db:
            before = db.execute(SELECT_ALL).fetchone()

        first_time = datetime.datetime.now(datetime.timezone.utc)
        simple_schedule.update_last(first_time, db)

        with db:
            after = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after, simple_schedule.fs),
                       Schedule._from_db_row(before, simple_schedule.fs),
                       {'created_count': 1,
                        'last': first_time,
                        'first': first_time})

        second_time = datetime.datetime.now(datetime.timezone.utc)
        simple_schedule.update_last(second_time, db)

        with db:
            after2 = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after2, simple_schedule.fs),
                       Schedule._from_db_row(after, simple_schedule.fs),
                       {'created_count': 2, 'last': second_time})

    def test_set_inactive_active(self, db, simple_schedule):
        simple_schedule.store_schedule(db)

        with db:
            before = db.execute(SELECT_ALL).fetchone()

        simple_schedule.set_inactive(db)

        with db:
            after = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after, simple_schedule.fs),
                       Schedule._from_db_row(before, simple_schedule.fs),
                       {'active': 0})

        simple_schedule.set_active(db)

        with db:
            after2 = db.execute(SELECT_ALL).fetchone()
        assert_updated(Schedule._from_db_row(after2, simple_schedule.fs),
                       Schedule._from_db_row(after, simple_schedule.fs),
                       {'active': 1})

    def test_update_pruned(self, db, simple_schedule):
        simple_schedule.store_schedule(db)

        with db:
            before = db.execute(SELECT_ALL).fetchone()

        now = datetime.datetime.now(datetime.timezone.utc)
        pruned_count = random.randint(1, 1000)

        simple_schedule.update_pruned(now, db, pruned_count)

        with db:
            after = db.execute(SELECT_ALL).fetchone()

        assert_updated(Schedule._from_db_row(after, simple_schedule.fs),
                       Schedule._from_db_row(before, simple_schedule.fs),
                       {'last_pruned': now, 'pruned_count': pruned_count})

    def test_add_retention(self, db, simple_schedule):
        simple_schedule.store_schedule(db)

        with db:
            before = db.execute(SELECT_ALL).fetchone()

        retention = "7d12m"
        simple_schedule.add_retention(db, simple_schedule.path, retention)

        with db:
            after = db.execute(SELECT_ALL).fetchone()

        assert after['retention'] == json.dumps(parse_retention(retention))

        retention2 = "4w"
        simple_schedule.add_retention(db, simple_schedule.path, retention2)

        with db:
            after = db.execute(SELECT_ALL).fetchone()

        assert after['retention'] == json.dumps(parse_retention(retention + retention2))

    def test_per_path_and_repeat_uniqueness(self, db):
        s1 = Schedule(*('/foo', '24h', 'fs_name', '/foo'))
        s2 = Schedule(*('/foo', '1d', 'fs_name', '/foo'))

        s1.store_schedule(db)
        with pytest.raises(sqlite3.IntegrityError):
            s2.store_schedule(db)
