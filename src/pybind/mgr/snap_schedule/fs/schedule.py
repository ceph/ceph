"""
Copyright (C) 2020 SUSE

LGPL2.1.  See file COPYING.
"""
from datetime import datetime, timezone
import logging
import sqlite3

log = logging.getLogger(__name__)


class Schedule(object):
    '''
    Wrapper to work with schedules stored in sqlite
    '''
    def __init__(self,
                 path,
                 schedule,
                 retention_policy,
                 fs_name,
                 rel_path,
                 start=None,
                 subvol=None,
                 created=None,
                 first=None,
                 last=None,
                 last_pruned=None,
                 created_count=0,
                 pruned_count=0,
                 active=True,
                 ):
        self.fs = fs_name
        self.subvol = subvol
        self.path = path
        self.rel_path = rel_path
        self.schedule = schedule
        self.retention = retention_policy
        if start is None:
            now = datetime.now(timezone.utc)
            self.start = datetime(now.year,
                                  now.month,
                                  now.day,
                                  tzinfo=now.tzinfo)
        else:
            self.start = datetime.fromisoformat(start).astimezone(timezone.utc)
        if created is None:
            self.created = datetime.now(timezone.utc)
        else:
            self.created = datetime.fromisoformat(created)
        if first:
            self.first = datetime.fromisoformat(first)
        else:
            self.first = first
        if last:
            self.last = datetime.fromisoformat(last)
        else:
            self.last = last
        if last_pruned:
            self.last_pruned = datetime.fromisoformat(last_pruned)
        else:
            self.last_pruned = last_pruned
        self.created_count = created_count
        self.pruned_count = pruned_count
        self.active = active

    @classmethod
    def _from_get_query(cls, table_row, fs):
        return cls(table_row['path'],
                   table_row['schedule'],
                   table_row['retention'],
                   fs,
                   table_row['rel_path'],
                   table_row['start'],
                   table_row['subvol'],
                   table_row['created'],
                   table_row['first'],
                   table_row['last'],
                   table_row['last_pruned'],
                   table_row['created_count'],
                   table_row['pruned_count'],
                   table_row['active'])

    def __str__(self):
        return f'''{self.path} {self.schedule} {self.retention}'''

    CREATE_TABLES = '''CREATE TABLE schedules(
        id integer PRIMARY KEY ASC,
        path text NOT NULL UNIQUE,
        subvol text,
        rel_path text NOT NULL,
        active int NOT NULL
    );
    CREATE TABLE schedules_meta(
        id INTEGER PRIMARY KEY ASC,
        schedule_id INT,
        start TEXT NOT NULL,
        first TEXT,
        last TEXT,
        last_pruned TEXT,
        created TEXT,
        repeat BIGINT NOT NULL,
        schedule TEXT NOT NULL,
        created_count INT DEFAULT 0,
        pruned_count INT DEFAULT 0,
        retention TEXT,
        FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE,
        UNIQUE (start, repeat)
    );'''

    EXEC_QUERY = '''SELECT
        sm.retention,
        sm.repeat - (strftime("%s", "now") - strftime("%s", sm.start)) %
        sm.repeat "until",
        sm.start, sm.repeat
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        WHERE
            s.path = ? AND
            strftime("%s", "now") - strftime("%s", sm.start) > 0 AND
            s.active = 1
        ORDER BY until;'''

    PROTO_GET_SCHEDULES = '''SELECT
          s.path, s.subvol, s.rel_path, s.active,
          sm.schedule, sm.retention, sm.start, sm.first, sm.last,
          sm.last_pruned, sm.created, sm.created_count, sm.pruned_count
          FROM schedules s
              INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
          WHERE'''

    GET_SCHEDULES = PROTO_GET_SCHEDULES + ' s.path = ?'

    GET_SCHEDULE = PROTO_GET_SCHEDULES + ' s.path = ? and sm.start = ? AND sm.repeat = ?'

    # TODO merge these two methods
    @classmethod
    def get_db_schedules(cls, path, db, fs):
        with db:
            c = db.execute(cls.GET_SCHEDULES, (path,))
        return [cls._from_get_query(row, fs) for row in c.fetchall()]

    @classmethod
    def get_db_schedule(cls, db, fs, path, start, repeat):
        with db:
            c = db.execute(cls.GET_SCHEDULE, (path, start, repeat))
        r = c.fetchone()
        if r:
            return cls._from_get_query(r, fs)
        else:
            return None

    @classmethod
    def list_schedules(cls, path, db, fs, recursive):
        with db:
            if recursive:
                c = db.execute(cls.PROTO_GET_SCHEDULES + ' path LIKE ?',
                               (f'{path}%',))
            else:
                c = db.execute(cls.PROTO_GET_SCHEDULES + ' path = ?',
                               (f'{path}',))
        return [cls._from_get_query(row, fs) for row in c.fetchall()]

    INSERT_SCHEDULE = '''INSERT INTO
        schedules(path, subvol, rel_path, active)
        Values(?, ?, ?, ?);'''
    INSERT_SCHEDULE_META = '''INSERT INTO
        schedules_meta(schedule_id, start, created, repeat, schedule, retention)
        SELECT ?, ?, ?, ?, ?, ?'''

    def store_schedule(self, db):
        sched_id = None
        with db:
            try:
                c = db.execute(self.INSERT_SCHEDULE,
                               (self.path,
                                self.subvol,
                                self.rel_path,
                                1))
                sched_id = c.lastrowid
            except sqlite3.IntegrityError:
                # might be adding another schedule, retrieve sched id
                log.debug(f'found schedule entry for {self.path}, trying to add meta')
                c = db.execute('SELECT id FROM schedules where path = ?',
                               (self.path,))
                sched_id = c.fetchone()[0]
                pass
            db.execute(self.INSERT_SCHEDULE_META,
                       (sched_id,
                        self.start.isoformat(),
                        self.created.isoformat(),
                        self.repeat,
                        self.schedule,
                        self.retention))

    @classmethod
    def rm_schedule(cls, db, path, repeat, start):
        with db:
            cur = db.execute('SELECT id FROM schedules WHERE path = ?',
                             (path,))
            row = cur.fetchone()

            if len(row) == 0:
                log.info(f'no schedule for {path} found')
                raise ValueError('SnapSchedule for {} not found'.format(path))

            id_ = tuple(row)

            if repeat or start:
                meta_delete = 'DELETE FROM schedules_meta WHERE schedule_id = ?'
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
                        f'no more schedules left, cleaning up schedules table')
                    db.execute('DELETE FROM schedules WHERE id = ?;', id_)
            else:
                # just delete the schedule CASCADE DELETE takes care of the
                # rest
                db.execute('DELETE FROM schedules WHERE id = ?;', id_)

    def report(self):
        import pprint
        return pprint.pformat(self.__dict__)

    @property
    def repeat(self):
        mult = self.schedule[-1]
        period = int(self.schedule[0:-1])
        if mult == 'm':
            return period * 60
        elif mult == 'h':
            return period * 60 * 60
        elif mult == 'd':
            return period * 60 * 60 * 24
        elif mult == 'w':
            return period * 60 * 60 * 24 * 7
        else:
            raise Exception('schedule multiplier not recognized')

    UPDATE_LAST = '''UPDATE schedules_meta
    SET
      last = ?,
      created_count = created_count + 1,
      first = CASE WHEN first IS NULL THEN ? ELSE first END
    WHERE
      start = ? AND
      repeat = ?;'''

    def update_last(self, time, db):
        with db:
            db.execute(self.UPDATE_LAST, (time.isoformat(), time.isoformat(),
                                          self.start.isoformat(), self.repeat))
        self.created_count += 1
        self.last = time

    # TODO add option to only change one snapshot in a path, i.e. pass repeat
    # and start time as well
    UPDATE_INACTIVE = '''UPDATE schedules
    SET
      active = 0
    WHERE
      path = ?;'''

    def set_inactive(self, db):
        with db:
            log.debug(f'Deactivating schedule on path {self.path}')
            db.execute(self.UPDATE_INACTIVE, (self.path,))

    UPDATE_ACTIVE = '''UPDATE schedules
    SET
      active = 1
    WHERE
      path = ?;'''

    def set_active(self, db):
        with db:
            log.debug(f'Activating schedule on path {self.path}')
            db.execute(self.UPDATE_ACTIVE, (self.path,))

    UPDATE_PRUNED = '''UPDATE schedules_meta
    SET
      last_pruned = ?,
      pruned_count = pruned_count + ?
    WHERE
      start = ? AND
      repeat = ?;'''

    def update_pruned(self, time, db, pruned):
        with db:
            db.execute(self.UPDATE_PRUNED, (time.isoformat(), pruned,
                                            self.start.isoformat(),
                                            self.repeat))
        self.pruned_count += pruned
        self.last_pruned = time
