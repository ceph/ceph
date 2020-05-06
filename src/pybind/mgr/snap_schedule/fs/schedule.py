"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import cephfs
import errno
import rados
from contextlib import contextmanager
import re
from mgr_util import CephfsClient, CephfsConnectionException, \
        open_filesystem
from collections import OrderedDict
from datetime import datetime
import logging
from operator import attrgetter, itemgetter
from threading import Timer
import sqlite3


SNAP_SCHEDULE_NAMESPACE = 'cephfs-snap-schedule'
SNAP_DB_OBJECT_NAME = 'snap_db'
TS_FORMAT = '%Y-%m-%d-%H_%M_%S'

log = logging.getLogger(__name__)


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
        log.error("Failed to locate pool {}".format(pool))
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
        self._ret = {}
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


def parse_retention(retention):
    ret = {}
    matches = re.findall(r'\d+[a-z]', retention)
    for m in matches:
        ret[m[-1]] = int(m[0:-1])
    matches = re.findall(r'\d+[A-Z]', retention)
    for m in matches:
        ret[m[-1]] = int(m[0:-1])
    return ret


class SnapSchedClient(CephfsClient):

    CREATE_TABLES = '''CREATE TABLE schedules(
        id integer PRIMARY KEY ASC,
        path text NOT NULL UNIQUE,
        subvol text,
        rel_path text NOT NULL,
        active int NOT NULL
    );
    CREATE TABLE schedules_meta(
        id integer PRIMARY KEY ASC,
        schedule_id int,
        start bigint NOT NULL,
        repeat bigint NOT NULL,
        schedule text NOT NULL,
        retention text,
        FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE,
        UNIQUE (start, repeat)
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
        try:
            log.debug(f'SnapDB on {fs} changed, updating next Timer')
            db = self.get_schedule_db(fs)
            rows = []
            with db:
                cur = db.execute(self.EXEC_QUERY)
                rows = cur.fetchmany(1)
                log.debug(f'retrieved {cur.rowcount} rows')
            timers = self.active_timers.get(fs, [])
            for timer in timers:
                timer.cancel()
            timers = []
            for row in rows:
                log.debug(f'adding timer for {row}')
                log.debug(f'Creating new snapshot timer')
                t = Timer(row[2],
                          self.create_scheduled_snapshot,
                          args=[fs, row[0], row[1]])
                t.start()
                timers.append(t)
                log.debug(f'Will snapshot {row[0]} in fs {fs} in {row[2]}s')
            self.active_timers[fs] = timers
        except Exception as e:
            log.error(f'refresh raised {e}')

    def get_schedule_db(self, fs):
        if fs not in self.sqlite_connections:
            self.sqlite_connections[fs] = sqlite3.connect(':memory:',
                                                          check_same_thread=False)
            with self.sqlite_connections[fs] as con:
                con.execute("PRAGMA FOREIGN_KEYS = 1")
                pool = self.get_metadata_pool(fs)
                with open_ioctx(self, pool) as ioctx:
                    try:
                        size, _mtime = ioctx.stat(SNAP_DB_OBJECT_NAME)
                        db = ioctx.read(SNAP_DB_OBJECT_NAME,
                                        size).decode('utf-8')
                        con.executescript(db)
                    except rados.ObjectNotFound:
                        log.info(f'No schedule DB found in {fs}')
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
            db = self.sqlite_connections[fs]
            with db:
                for row in db.iterdump():
                    db_content.append(row)
        with open_ioctx(self, metadata_pool) as ioctx:
            ioctx.write_full(SNAP_DB_OBJECT_NAME,
                             '\n'.join(db_content).encode('utf-8'))

    def create_scheduled_snapshot(self, fs_name, path, retention):
        log.debug(f'Scheduled snapshot of {path} triggered')
        try:
            time = datetime.utcnow().strftime(TS_FORMAT)
            with open_filesystem(self, fs_name) as fs_handle:
                fs_handle.mkdir(f'{path}/.snap/scheduled-{time}', 0o755)
            log.info(f'created scheduled snapshot of {path}')
        except cephfs.Error as e:
            log.info(f'scheduled snapshot creating of {path} failed: {e}')
        except Exception as e:
            # catch all exceptions cause otherwise we'll never know since this
            # is running in a thread
            log.error(f'ERROR create_scheduled_snapshot raised{e}')
        finally:
            log.info(f'finally branch')
            self.refresh_snap_timers(fs_name)
            log.info(f'calling prune')
            self.prune_snapshots(fs_name, path, retention)

    def prune_snapshots(self, fs_name, path, retention):
        log.debug('Pruning snapshots')
        ret = parse_retention(retention)
        try:
            prune_candidates = set()
            with open_filesystem(self, fs_name) as fs_handle:
                with fs_handle.opendir(f'{path}/.snap') as d_handle:
                    dir_ = fs_handle.readdir(d_handle)
                    while dir_:
                        if dir_.d_name.startswith(b'scheduled-'):
                            log.debug(f'add {dir_.d_name} to pruning')
                            ts = datetime.strptime(
                                dir_.d_name.lstrip(b'scheduled-').decode('utf-8'),
                                TS_FORMAT)
                            prune_candidates.add((dir_, ts))
                        else:
                            log.debug(f'skipping dir entry {dir_.d_name}')
                        dir_ = fs_handle.readdir(d_handle)
                to_keep = self.get_prune_set(prune_candidates, ret)
                for k in prune_candidates - to_keep:
                    dirname = k[0].d_name.decode('utf-8')
                    log.debug(f'rmdir on {dirname}')
                    fs_handle.rmdir(f'{path}/.snap/{dirname}')
                log.debug(f'keeping {to_keep}')
        except Exception as e:
            log.debug(f'prune_snapshots threw {e}')
        # TODO: handle snap pruning accoring to retention

    def get_prune_set(self, candidates, retention):
        PRUNING_PATTERNS = OrderedDict([
            #TODO remove M for release
            ("M", '%Y-%m-%d-%H_%M'),
            ("h", '%Y-%m-%d-%H'),
            ("d", '%Y-%m-%d'),
            ("w", '%G-%V'),
            ("m", '%Y-%m'),
            ("y", '%Y'),
        ])
        keep = set()
        log.debug(retention)
        for period, date_pattern in PRUNING_PATTERNS.items():
            period_count = retention.get(period, 0)
            if not period_count:
                log.debug(f'skipping period {period}')
                continue
            last = None
            for snap in sorted(candidates, key=lambda x: x[0].d_name,
                               reverse=True):
                snap_ts = snap[1].strftime(date_pattern)
                log.debug(f'{snap_ts} : {last}')
                if snap_ts != last:
                    last = snap_ts
                    if snap not in keep:
                        log.debug(f'keeping {snap[0].d_name} due to {period_count}{period}')
                        keep.add(snap)
                        if len(keep) == period_count:
                            log.debug(f'found enough snapshots for {period_count}{period}')
                            break
            # TODO maybe do candidates - keep here? we want snaps counting it
            # hours not be considered for days and it cuts down on iterations
        return keep

    def validate_schedule(self, fs_name, sched):
        try:
            with open_filesystem(self, fs_name) as fs_handle:
                fs_handle.stat(sched.path)
        except cephfs.ObjectNotFound:
            log.error('Path {} not found'.format(sched.path))
            return False
        return True

    LIST_SCHEDULES = '''SELECT
        s.path, sm.schedule, sm.retention, sm.start, s.subvol, s.rel_path
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        WHERE s.path = ?'''

    def list_snap_schedules(self, fs, path):
        db = self.get_schedule_db(fs)
        c = db.execute(self.LIST_SCHEDULES, (path,))
        return [Schedule.from_db_row(row, fs) for row in c.fetchall()]

    def dump_snap_schedule(self, fs, path):
        db = self.get_schedule_db(fs)
        # TODO retrieve multiple schedules per path from schedule_meta
        # with db:
        c = db.execute('SELECT * FROM SCHEDULES WHERE path LIKE ?',
                       (f'{path}%',))
        return [row for row in c.fetchall()]

# TODO currently not used, probably broken
    UPDATE_SCHEDULE = '''UPDATE schedules
        SET
            schedule = ?,
            active = 1
        WHERE path = ?;'''
    UPDATE_SCHEDULE_META = '''UPDATE schedules_meta
        SET
            start = ?,
            repeat = ?,
            schedule = ?
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
        schedules(path, subvol, rel_path, active)
        Values(?, ?, ?, ?);'''
    INSERT_SCHEDULE_META = '''INSERT INTO
        schedules_meta(schedule_id, start, repeat, schedule, retention)
        Values(last_insert_rowid(), ?, ?, ?, ?)'''

    @updates_schedule_db
    def store_snap_schedule(self, fs, sched):
        db = self.get_schedule_db(fs)
        with db:
            try:
                db.execute(self.INSERT_SCHEDULE,
                           (sched.path,
                            sched.subvol,
                            sched.rel_path,
                            1))
            except sqlite3.IntegrityError:
                # might be adding another schedule
                pass
            db.execute(self.INSERT_SCHEDULE_META,
                       (f'strftime("%s", "{sched.first_run}")',
                        sched.repeat_in_s(),
                        sched.schedule,
                        sched.retention))
            self.store_schedule_db(sched.fs)

    @updates_schedule_db
    def rm_snap_schedule(self, fs, path):
        db = self.get_schedule_db(fs)
        with db:
            cur = db.execute('SELECT id FROM SCHEDULES WHERE path = ?',
                             (path,))
            id_ = cur.fetchone()
            # TODO check for repeat-start pair and delete that if present, all
            # otherwise. If repeat-start was speced, delete only those
            db.execute('DELETE FROM schedules WHERE id = ?;', id_)
