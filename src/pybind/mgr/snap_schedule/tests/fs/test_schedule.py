import datetime
from fs.schedule import Schedule


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
