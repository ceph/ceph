"""
Copyright (C) 2020 SUSE

LGPL2.1.  See file COPYING.
"""
from datetime import datetime, timezone
import json
import logging
import re
import sqlite3
from typing import cast, Any, Dict, List, Tuple, Optional, Union

log = logging.getLogger(__name__)

# Work around missing datetime.fromisoformat for < python3.7
SNAP_DB_TS_FORMAT = '%Y-%m-%dT%H:%M:%S'
try:
    from backports.datetime_fromisoformat import MonkeyPatch
    MonkeyPatch.patch_fromisoformat()
except ImportError:
    log.debug('backports.datetime_fromisoformat not found')

try:
    # have mypy ignore this line. We use the attribute error to detect if we
    # have fromisoformat or not
    ts_parser = datetime.fromisoformat  # type: ignore
    log.debug('found datetime.fromisoformat')
except AttributeError:
    log.info(('Couldn\'t find datetime.fromisoformat, falling back to '
              f'static timestamp parsing ({SNAP_DB_TS_FORMAT}'))

    def ts_parser(data_string: str) -> datetime:  # type: ignore
        try:
            date = datetime.strptime(data_string, SNAP_DB_TS_FORMAT)
            return date
        except ValueError:
            msg = f'''The date string {data_string} does not match the required format
            {SNAP_DB_TS_FORMAT}. For more flexibel date parsing upgrade to
            python3.7 or install
            https://github.com/movermeyer/backports.datetime_fromisoformat'''
            log.error(msg)
            raise ValueError(msg)


def parse_timestamp(ts: str) -> datetime:
    date = ts_parser(ts)
    # normalize any non utc timezone to utc. If no tzinfo is supplied, assume
    # its already utc
    # import pdb; pdb.set_trace()
    if date.tzinfo is not timezone.utc and date.tzinfo is not None:
        date = date.astimezone(timezone.utc)
    return date


def parse_retention(retention: str) -> Dict[str, int]:
    ret = {}
    log.debug(f'parse_retention({retention})')
    matches = re.findall(r'\d+[a-z]', retention)
    for m in matches:
        ret[m[-1]] = int(m[0:-1])
    matches = re.findall(r'\d+[A-Z]', retention)
    for m in matches:
        ret[m[-1]] = int(m[0:-1])
    log.debug(f'parse_retention({retention}) -> {ret}')
    return ret


RETENTION_MULTIPLIERS = ['n', 'm', 'h', 'd', 'w', 'M', 'Y']

TableRowT = Dict[str, Union[int, str]]


def dump_retention(retention: Dict[str, str]) -> str:
    ret = ''
    for mult in RETENTION_MULTIPLIERS:
        if mult in retention:
            ret += str(retention[mult]) + mult
    return ret


class Schedule(object):
    '''
    Wrapper to work with schedules stored in sqlite
    '''
    def __init__(self,
                 path: str,
                 schedule: str,
                 fs_name: str,
                 rel_path: str,
                 start: Optional[str] = None,
                 subvol: Optional[str] = None,
                 group: Optional[str] = None,
                 retention_policy: str = '{}',
                 created: Optional[str] = None,
                 first: Optional[str] = None,
                 last: Optional[str] = None,
                 last_pruned: Optional[str] = None,
                 created_count: int = 0,
                 pruned_count: int = 0,
                 active: bool = True,
                 ) -> None:
        self.fs = fs_name
        self.subvol = subvol
        self.group = group
        self.path = path
        self.rel_path = rel_path
        self.schedule = schedule
        # test to see if period and spec are valid
        # this test will throw a ValueError exception if
        # period is negative or zero
        # spec is empty or other than n,m,h,d,w,M,Y
        rep = self.repeat
        self.retention = json.loads(retention_policy)
        if start is None:
            now = datetime.now(timezone.utc)
            self.start = datetime(now.year,
                                  now.month,
                                  now.day,
                                  tzinfo=now.tzinfo)
        else:
            self.start = parse_timestamp(start)
        if created is None:
            self.created: Optional[datetime] = datetime.now(timezone.utc)
        else:
            self.created = parse_timestamp(created)
        if first:
            self.first: Optional[datetime] = parse_timestamp(first)
        else:
            self.first = None
        if last:
            self.last: Optional[datetime] = parse_timestamp(last)
        else:
            self.last = None
        if last_pruned:
            self.last_pruned: Optional[datetime] = parse_timestamp(last_pruned)
        else:
            self.last_pruned = None
        self.created_count = created_count
        self.pruned_count = pruned_count
        self.active = bool(active)

    @classmethod
    def _from_db_row(cls, table_row: TableRowT, fs: str) -> 'Schedule':
        return cls(cast(str, table_row['path']),
                   cast(str, table_row['schedule']),
                   fs,
                   cast(str, table_row['rel_path']),
                   cast(str, table_row['start']),
                   cast(str, table_row['subvol']),
                   cast(str, table_row['group_name']),
                   cast(str, table_row['retention']),
                   cast(str, table_row['created']),
                   cast(str, table_row['first']),
                   cast(str, table_row['last']),
                   cast(str, table_row['last_pruned']),
                   cast(int, table_row['created_count']),
                   cast(int, table_row['pruned_count']),
                   cast(bool, table_row['active']),
                   )

    def __str__(self) -> str:
        return f'{self.path} {self.schedule} {dump_retention(self.retention)}'

    def json_list(self) -> str:
        return json.dumps({'path': self.path, 'schedule': self.schedule,
                           'retention': dump_retention(self.retention)})

    CREATE_TABLES = '''CREATE TABLE IF NOT EXISTS schedules(
        id INTEGER PRIMARY KEY ASC,
        path TEXT NOT NULL UNIQUE,
        subvol TEXT,
        retention TEXT DEFAULT '{}',
        rel_path TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS schedules_meta(
        id INTEGER PRIMARY KEY ASC,
        schedule_id INT,
        start TEXT NOT NULL,
        first TEXT,
        last TEXT,
        last_pruned TEXT,
        created TEXT NOT NULL,
        repeat INT NOT NULL,
        schedule TEXT NOT NULL,
        created_count INT DEFAULT 0,
        pruned_count INT DEFAULT 0,
        active INT NOT NULL,
        FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE,
        UNIQUE (schedule_id, start, repeat)
    );'''

    EXEC_QUERY = '''SELECT
        s.retention,
        sm.repeat - (strftime("%s", "now") - strftime("%s", sm.start)) %
        sm.repeat "until",
        sm.start, sm.repeat, sm.schedule
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        WHERE
            s.path = ? AND
            strftime("%s", "now") - strftime("%s", sm.start) > 0 AND
            sm.active = 1
        ORDER BY until;'''

    PROTO_GET_SCHEDULES = '''SELECT
          s.path, s.subvol, s.group_name, s.rel_path, sm.active,
          sm.schedule, s.retention, sm.start, sm.first, sm.last,
          sm.last_pruned, sm.created, sm.created_count, sm.pruned_count
          FROM schedules s
              INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
          WHERE'''

    GET_SCHEDULES = PROTO_GET_SCHEDULES + ' s.path = ?'

    @classmethod
    def get_db_schedules(cls,
                         path: str,
                         db: sqlite3.Connection,
                         fs: str,
                         schedule: Optional[str] = None,
                         start: Optional[str] = None,
                         repeat: Optional[str] = None) -> List['Schedule']:
        query = cls.GET_SCHEDULES
        data: Tuple[Any, ...] = (path,)
        if repeat:
            query += ' AND sm.repeat = ?'
            data += (repeat,)
        if schedule:
            query += ' AND sm.schedule = ?'
            data += (schedule,)
        if start:
            query += ' AND sm.start = ?'
            data += (start,)
        with db:
            c = db.execute(query, data)
            return [cls._from_db_row(row, fs) for row in c.fetchall()]

    @classmethod
    def list_schedules(cls,
                       path: str,
                       db: sqlite3.Connection,
                       fs: str, recursive: bool) -> List['Schedule']:
        with db:
            if recursive:
                c = db.execute(cls.PROTO_GET_SCHEDULES + ' path LIKE ?',
                               (f'{path}%',))
            else:
                c = db.execute(cls.PROTO_GET_SCHEDULES + ' path = ?',
                               (f'{path}',))
            return [cls._from_db_row(row, fs) for row in c.fetchall()]

    @classmethod
    def list_all_schedules(cls,
                           db: sqlite3.Connection,
                           fs: str) -> List['Schedule']:
        with db:
            c = db.execute(cls.PROTO_GET_SCHEDULES + " path LIKE '%'")
            return [cls._from_db_row(row, fs) for row in c.fetchall()]

    INSERT_SCHEDULE = '''INSERT INTO
        schedules(path, subvol, group_name, retention, rel_path)
        Values(?, ?, ?, ?, ?);'''
    INSERT_SCHEDULE_META = '''INSERT INTO
        schedules_meta(schedule_id, start, created, repeat, schedule,
        active)
        SELECT ?, ?, ?, ?, ?, ?'''

    def store_schedule(self, db: sqlite3.Connection) -> None:
        sched_id = None
        with db:
            try:
                log.debug(f'schedule with retention {self.retention}')
                c = db.execute(self.INSERT_SCHEDULE,
                               (self.path,
                                self.subvol,
                                self.group,
                                json.dumps(self.retention),
                                self.rel_path,))
                sched_id = c.lastrowid
            except sqlite3.IntegrityError:
                # might be adding another schedule, retrieve sched id
                log.debug((f'found schedule entry for {self.path}, '
                           'trying to add meta'))
                c = db.execute('SELECT id FROM schedules where path = ?',
                               (self.path,))
                sched_id = c.fetchone()[0]
                pass
            assert self.created, "self.created should be set"
            db.execute(self.INSERT_SCHEDULE_META,
                       (sched_id,
                        self.start.strftime(SNAP_DB_TS_FORMAT),
                        self.created.strftime(SNAP_DB_TS_FORMAT),
                        self.repeat,
                        self.schedule,
                        1))

    @classmethod
    def rm_schedule(cls,
                    db: sqlite3.Connection,
                    path: str,
                    repeat: Optional[str],
                    start: Optional[str]) -> None:
        with db:
            cur = db.execute('SELECT id FROM schedules WHERE path = ?',
                             (path,))
            row = cur.fetchone()

            if row is None:
                log.info(f'no schedule for {path} found')
                raise ValueError('SnapSchedule for {} not found'.format(path))

            id_ = tuple(row)

            if repeat or start:
                meta_delete = ('DELETE FROM schedules_meta '
                               'WHERE schedule_id = ?')
                delete_param = id_
                if repeat:
                    meta_delete += ' AND schedule = ?'
                    delete_param += (repeat,)
                if start:
                    meta_delete += ' AND start = ?'
                    delete_param += (start,)
                # maybe only delete meta entry
                log.debug(f'executing {meta_delete}, {delete_param}')
                res = db.execute(meta_delete + ';', delete_param).rowcount
                if res < 1:
                    raise ValueError(f'No schedule found for {repeat} {start}')
                db.execute('COMMIT;')
                # now check if we have schedules in meta left, if not delete
                # the schedule as well
                meta_count = db.execute(
                    'SELECT COUNT() FROM schedules_meta WHERE schedule_id = ?',
                    id_)
                if meta_count.fetchone() == (0,):
                    log.debug(
                        'no more schedules left, cleaning up schedules table')
                    db.execute('DELETE FROM schedules WHERE id = ?;', id_)
            else:
                # just delete the schedule CASCADE DELETE takes care of the
                # rest
                db.execute('DELETE FROM schedules WHERE id = ?;', id_)

    GET_RETENTION = '''SELECT retention FROM schedules
    WHERE path = ?'''
    UPDATE_RETENTION = '''UPDATE schedules
    SET retention = ?
    WHERE path = ?'''

    @classmethod
    def add_retention(cls,
                      db: sqlite3.Connection,
                      path: str,
                      retention_spec: str) -> None:
        with db:
            row = db.execute(cls.GET_RETENTION, (path,)).fetchone()
            if row is None:
                raise ValueError(f'No schedule found for {path}')
            retention = parse_retention(retention_spec)
            if not retention:
                raise ValueError(f'Retention spec {retention_spec} is invalid')
            log.debug(f'db result is {tuple(row)}')
            current = row['retention']
            current_retention = json.loads(current)
            for r, v in retention.items():
                if r in current_retention:
                    msg = (f'Retention for {r} is already present with value'
                           f'{current_retention[r]}. Please remove first')
                    raise ValueError(msg)
            current_retention.update(retention)
            db.execute(cls.UPDATE_RETENTION,
                       (json.dumps(current_retention), path))

    @classmethod
    def rm_retention(cls,
                     db: sqlite3.Connection,
                     path: str,
                     retention_spec: str) -> None:
        with db:
            row = db.execute(cls.GET_RETENTION, (path,)).fetchone()
            if row is None:
                raise ValueError(f'No schedule found for {path}')
            retention = parse_retention(retention_spec)
            current = row['retention']
            current_retention = json.loads(current)
            for r, v in retention.items():
                if r not in current_retention or current_retention[r] != v:
                    msg = (f'Retention for {r}: {v} was not set for {path} '
                           'can\'t remove')
                    raise ValueError(msg)
                current_retention.pop(r)
            db.execute(cls.UPDATE_RETENTION,
                       (json.dumps(current_retention), path))

    def report(self) -> str:
        return self.report_json()

    def report_json(self) -> str:
        return json.dumps(dict(self.__dict__),
                          default=lambda o: o.strftime(SNAP_DB_TS_FORMAT))

    @classmethod
    def parse_schedule(cls, schedule: str) -> Tuple[int, str]:
        return int(schedule[0:-1]), schedule[-1]

    @property
    def repeat(self) -> int:
        period = -1
        mult = ""
        try:
            period, mult = self.parse_schedule(self.schedule)
        except ValueError:
            raise ValueError('invalid schedule specified - period should be '
                             'non-zero positive value and multiplier should '
                             'be one of h,d,w,M,Y e.g. 1h or 4d etc.')
        if period <= 0:
            raise ValueError('invalid schedule specified - period must be a '
                             'non-zero positive value e.g. 1h or 4d etc.')
        # 'm' is only for developer testing of minute level snapshots
        if mult == 'm':
            return period * 60
        elif mult == 'h':
            return period * 60 * 60
        elif mult == 'd':
            return period * 60 * 60 * 24
        elif mult == 'w':
            return period * 60 * 60 * 24 * 7
        elif mult == 'M':
            return period * 60 * 60 * 24 * 30
        elif mult == 'Y':
            return period * 60 * 60 * 24 * 365
        else:
            raise ValueError('invalid schedule specified - multiplier should '
                             'be one of h,d,w,M,Y')

    UPDATE_LAST = '''UPDATE schedules_meta
    SET
      last = ?,
      created_count = created_count + 1,
      first = CASE WHEN first IS NULL THEN ? ELSE first END
    WHERE EXISTS(
      SELECT id
      FROM schedules s
      WHERE s.id = schedules_meta.schedule_id
      AND s.path = ?
      AND schedules_meta.start = ?
      AND schedules_meta.repeat = ?);'''

    def update_last(self, time: datetime, db: sqlite3.Connection) -> None:
        with db:
            db.execute(self.UPDATE_LAST,
                       (time.strftime(SNAP_DB_TS_FORMAT),
                        time.strftime(SNAP_DB_TS_FORMAT),
                        self.path,
                        self.start.strftime(SNAP_DB_TS_FORMAT),
                        self.repeat))
        self.created_count += 1
        self.last = time
        if not self.first:
            self.first = time

    UPDATE_INACTIVE = '''UPDATE schedules_meta
    SET
      active = 0
    WHERE EXISTS(
      SELECT id
      FROM schedules s
      WHERE s.id = schedules_meta.schedule_id
      AND s.path = ?
      AND schedules_meta.start = ?
      AND schedules_meta.repeat = ?);'''

    def set_inactive(self, db: sqlite3.Connection) -> None:
        with db:
            log.debug((f'Deactivating schedule ({self.repeat}, '
                       f'{self.start}) on path {self.path}'))
            db.execute(self.UPDATE_INACTIVE,
                       (self.path,
                        self.start.strftime(SNAP_DB_TS_FORMAT),
                        self.repeat))
        self.active = False

    UPDATE_ACTIVE = '''UPDATE schedules_meta
    SET
      active = 1
    WHERE EXISTS(
      SELECT id
      FROM schedules s
      WHERE s.id = schedules_meta.schedule_id
      AND s.path = ?
      AND schedules_meta.start = ?
      AND schedules_meta.repeat = ?);'''

    def set_active(self, db: sqlite3.Connection) -> None:
        with db:
            log.debug(f'Activating schedule ({self.repeat}, {self.start}) '
                      f'on path {self.path}')
            db.execute(self.UPDATE_ACTIVE,
                       (self.path,
                        self.start.strftime(SNAP_DB_TS_FORMAT),
                        self.repeat))
        self.active = True

    UPDATE_PRUNED = '''UPDATE schedules_meta
    SET
      last_pruned = ?,
      pruned_count = pruned_count + ?
    WHERE EXISTS(
      SELECT id
      FROM schedules s
      WHERE s.id = schedules_meta.schedule_id
      AND s.path = ?
      AND schedules_meta.start = ?
      AND schedules_meta.repeat = ?);'''

    def update_pruned(self,
                      time: datetime,
                      db: sqlite3.Connection,
                      pruned: int) -> None:
        with db:
            db.execute(self.UPDATE_PRUNED,
                       (time.strftime(SNAP_DB_TS_FORMAT), pruned,
                        self.path,
                        self.start.strftime(SNAP_DB_TS_FORMAT),
                        self.repeat))
        self.pruned_count += pruned
        self.last_pruned = time
