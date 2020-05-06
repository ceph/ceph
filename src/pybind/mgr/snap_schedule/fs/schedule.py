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


class Schedule(object):
    '''
    Wrapper to work with schedules stored in Rados objects
    '''
    def __init__(self,
                 path,
                 schedule,
                 retention_policy,
                 fs_name,
                 subvol,
                 first_run=None,
                 ):
        self.fs = fs_name
        self.subvol = subvol
        self.path = path
        self.schedule = schedule
        self.retention = retention_policy
        self.first_run = None
        self.last_run = None

    def __str__(self):
        return f'''{self.path}: {self.schedule}; {self.retention}'''

    @classmethod
    def from_db_row(cls, table_row, fs):
        return cls(table_row[0], table_row[1], table_row[2], fs, None)

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

    CREATE_TABLES = '''CREATE TABLE SCHEDULES(
        id INTEGER PRIMARY KEY ASC,
        path text NOT NULL UNIQUE,
        schedule text NOT NULL,
        retention text
    );
    CREATE TABLE SCHEDULES_META(
        id INTEGER PRIMARY KEY ASC,
        schedule_id int,
        start bigint NOT NULL,
        repeat bigint NOT NULL,
        FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE
    );'''
    INSERT_SCHEDULE = '''INSERT INTO schedules(path, schedule, retention) Values(?, ?, ?);'''
    INSERT_SCHEDULE_META = ''' INSERT INTO schedules_meta(schedule_id, start, repeat) Values(last_insert_rowid(), ?, ?)'''
    UPDATE_SCHEDULE = '''UPDATE schedules SET schedule = ?, retention = ? where path = ?;'''
    UPDATE_SCHEDULE_META = '''UPDATE schedules_meta SET start = ?, repeat = ? where schedule_id = (SELECT id FROM SCHEDULES WHERE path = ?);'''
    DELETE_SCHEDULE = '''DELETE FROM schedules WHERE id = ?;'''
    exec_query = '''select s.id, s.path, sm.repeat - ((strftime("%s", "now") - sm.start) % sm.repeat) "until" from schedules s inner join schedules_meta sm on sm.event_id = s.id order by until;'''

    def __init__(self, mgr):
        super(SnapSchedClient, self).__init__(mgr)
        self.sqlite_connections = {}

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
                        self.log.info('No schedule DB found in {}'.format(fs))
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
            for row in self.sqlite_connections[fs].iterdump():
                db_content.append(row)
        with open_ioctx(self, metadata_pool) as ioctx:
            ioctx.write_full("SNAP_DB_OBJECT_NAME",
                             '\n'.join(db_content).encode('utf-8'))

    @connection_pool_wrap
    def validate_schedule(self, fs_handle, **kwargs):
        try:
            fs_handle.stat(kwargs['sched'].path)
        except cephfs.ObjectNotFound:
            self.log.error('Path {} not found'.format(kwargs['sched'].path))
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

    def update_snap_schedule(self, fs, sched):
        db = self.get_schedule_db(fs)
        with db:
            db.execute(self.UPDATE_SCHEDULE,
                       (sched.schedule,
                        sched.retention,
                        sched.path))
            db.execute(self.UPDATE_SCHEDULE_META,
                       ('strftime("%s", "now")',
                        sched.repeat_in_s(),
                        sched.path))

    def store_snap_schedule(self, fs, sched):
        db = self.get_schedule_db(fs)
        with db:
            db.execute(self.INSERT_SCHEDULE,
                       (sched.path,
                        sched.schedule,
                        sched.retention))
            db.execute(self.INSERT_SCHEDULE_META,
                       ('strftime("%s", "now")',
                        sched.repeat_in_s()))
            self.store_schedule_db(sched.fs)

    def get_snap_schedule(self, fs, path):
        db = self.get_schedule_db(fs)
        c = db.execute('SELECT path, schedule, retention FROM SCHEDULES WHERE path = ?', (path,))
        return Schedule.from_db_row(c.fetchone(), fs)

    def rm_snap_schedule(self, fs, path):
        db = self.get_schedule_db(fs)
        with db:
            cur = db.execute('SELECT id FROM SCHEDULES WHERE path = ?',
                             (path,))
            id_ = cur.fetchone()
            db.execute(self.DELETE_SCHEDULE, id_)
