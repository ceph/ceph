import pytest
import sqlite3
from ..fs.schedule import Schedule


# simple_schedule fixture returns schedules without any timing arguments
# the tuple values correspong to ctor args for Schedule
_simple_schedules = [
    ('/foo', '6h', 'fs_name', '/foo'),
    ('/foo', '24h', 'fs_name', '/foo'),
    ('/bar', '1d', 'fs_name', '/bar'),
    ('/fnord', '1w', 'fs_name', '/fnord'),
]


@pytest.fixture(params=_simple_schedules)
def simple_schedule(request):
    return Schedule(*request.param)


@pytest.fixture
def simple_schedules():
    return [Schedule(*s) for s in _simple_schedules]


@pytest.fixture
def db():
    db = sqlite3.connect(':memory:',
                         check_same_thread=False)
    with db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA FOREIGN_KEYS = 1")
        db.executescript(Schedule.CREATE_TABLES)
        _create_snap_schedule_kv_db(db)
        _upgrade_snap_schedule_db_schema(db)
    return db

def _create_snap_schedule_kv_db(db):
    SQL = """
    CREATE TABLE IF NOT EXISTS SnapScheduleModuleKV (
      key TEXT PRIMARY KEY,
      value NOT NULL
    ) WITHOUT ROWID;
    INSERT OR IGNORE INTO SnapScheduleModuleKV (key, value) VALUES ('__snap_schedule_db_version', 1);
    """
    db.executescript(SQL)

def _get_snap_schedule_db_version(db):
    SQL = """
    SELECT value
    FROM SnapScheduleModuleKV
    WHERE key = '__snap_schedule_db_version';
    """
    cur = db.execute(SQL)
    row = cur.fetchone()
    assert row is not None
    return int(row[0])

# add all upgrades here
def _upgrade_snap_schedule_db_schema(db):
    # add a column to hold the subvolume group name
    if _get_snap_schedule_db_version(db) < 2:
        SQL = """
        ALTER TABLE schedules
        ADD COLUMN group_name TEXT;
        """
        db.executescript(SQL)

        # bump up the snap-schedule db version to 2
        SQL = "UPDATE OR ROLLBACK SnapScheduleModuleKV SET value = ? WHERE key = '__snap_schedule_db_version';"
        db.execute(SQL, (2,))
