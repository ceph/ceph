"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import cephfs
import errno
import rados
from contextlib import contextmanager
from mgr_util import CephfsClient, CephfsConnectionException, connection_pool_wrap
from datetime import datetime, timedelta
from threading import Timer
import sqlite3


SNAP_SCHEDULE_NAMESPACE = 'cephfs-snap-schedule'
SNAP_DB_OBJECT_NAME = 'snap_db'


@contextmanager
def open_ioctx(self, pool):
    try:
        if type(pool) is int:
            with self.mgr.rados.open_ioctx2(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
        else:
            with self.mgr.rados.open_ioctx(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
    except rados.ObjectNotFound:
        self.log.error("Failed to locate pool {}".format(pool))
        raise


def updates_schedule_db(func):
    def f(self, fs, *args):
        func(self, fs, *args)
        self.refresh_snap_timers(fs)
    return f


class Schedule(object):
    '''
    Wrapper to work with schedules stored in Rados objects
    '''
    def __init__(self,
                 path,
                 schedule,
                 retention_policy,
                 start,
                 fs_name,
                 subvol,
                 rel_path,
                 ):
        self.fs = fs_name
        self.subvol = subvol
        self.path = path
        self.rel_path = rel_path
        self.schedule = schedule
        self.retention = retention_policy
        self.first_run = start
        self.last_run = None

    def __str__(self):
        return f'''{self.rel_path}: {self.schedule}; {self.retention}'''

    @classmethod
    def from_db_row(cls, table_row, fs):
        return cls(table_row[0],
                   table_row[1],
                   table_row[2],
                   fs,
                   table_row[3],
                   table_row[4],
                   None)

    def repeat_in_s(self):
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


class SnapSchedClient(CephfsClient):

    CREATE_TABLES = '''CREATE TABLE schedules(
        id integer PRIMARY KEY ASC,
        path text NOT NULL UNIQUE,
        schedule text NOT NULL,
        subvol text,
        rel_path text NOT NULL,
        active int NOT NULL
    );
    CREATE TABLE schedules_meta(
        id integer PRIMARY KEY ASC,
        schedule_id int,
        start bigint NOT NULL,
        repeat bigint NOT NULL,
        retention text,
        FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE
    );'''

    def __init__(self, mgr):
        super(SnapSchedClient, self).__init__(mgr)
        # TODO maybe iterate over all fs instance in fsmap and load snap dbs?
        self.sqlite_connections = {}
        self.active_timers = {}

    EXEC_QUERY = '''SELECT
        s.path, sm.retention,
        sm.repeat - (strftime("%s", "now") - sm.start) % sm.repeat "until"
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        ORDER BY until;'''

    def refresh_snap_timers(self, fs):
        self.log.debug(f'SnapDB on {fs} changed, updating next Timer')
        db = self.get_schedule_db(fs)
        rows = []
        with db:
            cur = db.execute(self.EXEC_QUERY)
            rows = cur.fetchmany(1)
        timers = self.active_timers.get(fs, [])
        for timer in timers:
            timer.cancel()
        timers = []
        for row in rows:
            self.log.debug(f'Creating new snapshot timer')
            t = Timer(row[2],
                      self.create_scheduled_snapshot,
                      args=[fs, row[0], row[1]])
            t.start()
            timers.append(t)
            self.log.debug(f'Will snapshot {row[0]} in fs {fs} in {row[2]}s')
        self.active_timers[fs] = timers

    def get_schedule_db(self, fs):
        if fs not in self.sqlite_connections:
            self.sqlite_connections[fs] = sqlite3.connect(':memory:')
            with self.sqlite_connections[fs] as con:
                con.execute("PRAGMA FOREIGN_KEYS = 1")
                pool = self.get_metadata_pool(fs)
                with open_ioctx(self, pool) as ioctx:
                    try:
                        size, _mtime = ioctx.stat('SNAP_DB_OBJECT_NAME')
                        db = ioctx.read('SNAP_DB_OBJECT_NAME', size).decode('utf-8')
                        con.executescript(db)
                    except rados.ObjectNotFound:
                        self.log.info(f'No schedule DB found in {fs}')
                        con.executescript(self.CREATE_TABLES)
        return self.sqlite_connections[fs]

    def store_schedule_db(self, fs):
        # only store db is it exists, otherwise nothing to do
        metadata_pool = self.get_metadata_pool(fs)
        if not metadata_pool:
            raise CephfsConnectionException(
                -errno.ENOENT, "Filesystem {} does not exist".format(fs))
        if fs in self.sqlite_connections:
            db_content = []
            # TODO maybe do this in a transaction?
            db = self.sqlite_connections[fs]
            with db:
                for row in db.iterdump():
                    db_content.append(row)
        with open_ioctx(self, metadata_pool) as ioctx:
            ioctx.write_full("SNAP_DB_OBJECT_NAME",
                             '\n'.join(db_content).encode('utf-8'))

    @connection_pool_wrap
    def create_scheduled_snapshot(self, fs_info, path, retention):
        self.log.debug(f'Scheduled snapshot of {path} triggered')
        try:
            time = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H_%M_%S')
            fs_info[1].mkdir(f'{path}/.snap/scheduled-{time}', 0o755)
            self.log.info(f'created scheduled snapshot of {path}')
        except cephfs.Error as e:
            self.log.info(f'scheduled snapshot creating of {path} failed')
            raise CephfsConnectionException(-e.args[0], e.args[1])
        finally:
            self.refresh_snap_timers(fs_info[0])
        # TODO: handle snap pruning accoring to retention

    @connection_pool_wrap
    def validate_schedule(self, fs_handle, sched):
        try:
            fs_handle.stat(sched.path)
        except cephfs.ObjectNotFound:
            self.log.error('Path {} not found'.format(sched.path))
            return False
        return True

    def list_snap_schedule(self, fs, path):
        db = self.get_schedule_db(fs)
        # with db:
        scheds = []
        for row in db.execute('SELECT * FROM SCHEDULES WHERE path LIKE ?',
                              (f'{path}%',)):
            scheds.append(row)
        return scheds

    UPDATE_SCHEDULE = '''UPDATE schedules
        SET
            schedule = ?,
            active = 1
        WHERE path = ?;'''
    UPDATE_SCHEDULE_META = '''UPDATE schedules_meta
        SET
            start = ?,
            repeat = ?,
            retention = ?
        WHERE schedule_id = (
            SELECT id FROM SCHEDULES WHERE path = ?);'''

    @updates_schedule_db
    def update_snap_schedule(self, fs, sched):
        db = self.get_schedule_db(fs)
        with db:
            db.execute(self.UPDATE_SCHEDULE,
                       (sched.schedule,
                        sched.path))
            db.execute(self.UPDATE_SCHEDULE_META,
                       (f'strftime("%s", "{sched.first_run}")',
                        sched.repeat_in_s(),
                        sched.retention,
                        sched.path))

    INSERT_SCHEDULE = '''INSERT INTO
        schedules(path, schedule, subvol, rel_path, active)
        Values(?, ?, ?, ?, ?);'''
    INSERT_SCHEDULE_META = '''INSERT INTO
        schedules_meta(schedule_id, start, repeat, retention)
        Values(last_insert_rowid(), ?, ?, ?)'''

    @updates_schedule_db
    def store_snap_schedule(self, fs, sched):
        db = self.get_schedule_db(fs)
        with db:
            db.execute(self.INSERT_SCHEDULE,
                       (sched.path,
                        sched.schedule,
                        sched.subvol,
                        sched.rel_path,
                        1))
            db.execute(self.INSERT_SCHEDULE_META,
                       (f'strftime("%s", "{sched.first_run}")',
                        sched.repeat_in_s(),
                        sched.retention))
            self.store_schedule_db(sched.fs)

    GET_SCHEULE = '''SELECT
        s.path, s.schedule, sm.retention, sm.start, s.subvol, s.rel_path
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        WHERE s.path = ?'''

    def get_snap_schedule(self, fs, path):
        db = self.get_schedule_db(fs)
        c = db.execute(self.GET_SCHEDULE, (path,))
        return Schedule.from_db_row(c.fetchone(), fs)

    @updates_schedule_db
    def rm_snap_schedule(self, fs, path):
        db = self.get_schedule_db(fs)
        with db:
            cur = db.execute('SELECT id FROM SCHEDULES WHERE path = ?',
                             (path,))
            id_ = cur.fetchone()
            db.execute('DELETE FROM schedules WHERE id = ?;', (id_,))
