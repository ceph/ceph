import datetime
from fs.schedule import Schedule

SELECT_ALL = ('select * from schedules s'
              ' INNER JOIN schedules_meta sm'
              ' ON sm.schedule_id = s.id')


class TestSchedule(object):

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

        db_schedule = Schedule._from_get_query(row, simple_schedule.fs)

        for var in vars(db_schedule):
            assert getattr(simple_schedule, var) == getattr(db_schedule, var)

    def test_store_multiple(self, db, simple_schedules):
        [s.store_schedule(db) for s in simple_schedules]

        rows = []
        with db:
            rows = db.execute(SELECT_ALL).fetchall()

        assert len(rows) == len(simple_schedules)
