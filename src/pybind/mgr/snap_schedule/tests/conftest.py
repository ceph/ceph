import pytest
import sqlite3
from fs.schedule import Schedule


# simple_schedule fixture returns schedules without any timing arguments
# the tuple values correspong to ctor args for Schedule
simple_schedules = [
    ('/foo', '6h', '', 'fs_name', '/foo'),
    ('/foo', '24h', '', 'fs_name', '/foo'),
    ('/bar', '1d', '', 'fs_name', '/bar'),
    ('/fnord', '1w', '', 'fs_name', '/fnord'),
]
@pytest.fixture(params=simple_schedules)
def simple_schedule(request):
    return Schedule(*request.param)

@pytest.fixture
def db_and_simple_schedules():
    db = sqlite3.connect(':memory:',
                         check_same_thread=False)
    with db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA FOREIGN_KEYS = 1")
        db.executescript(Schedule.CREATE_TABLES)
    return db, [Schedule(*s) for s in simple_schedules]

