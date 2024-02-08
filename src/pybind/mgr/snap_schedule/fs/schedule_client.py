"""
Copyright (C) 2020 SUSE

LGPL2.1.  See file COPYING.
"""
import cephfs
import rados
from contextlib import contextmanager
from mgr_util import CephfsClient, open_filesystem
from collections import OrderedDict
from datetime import datetime, timezone
import logging
from threading import Timer, Lock
from typing import cast, Any, Callable, Dict, Iterator, List, Set, Optional, \
    Tuple, TypeVar, Union, Type
from types import TracebackType
import sqlite3
from .schedule import Schedule
import traceback


SNAP_SCHEDULE_NAMESPACE = 'cephfs-snap-schedule'
SNAP_DB_PREFIX = 'snap_db'
# increment this every time the db schema changes and provide upgrade code
SNAP_DB_VERSION = '0'
SNAP_DB_OBJECT_NAME = f'{SNAP_DB_PREFIX}_v{SNAP_DB_VERSION}'
# scheduled snapshots are tz suffixed
SNAPSHOT_TS_FORMAT_TZ = '%Y-%m-%d-%H_%M_%S_%Z'
# for backward compat snapshot name parsing
SNAPSHOT_TS_FORMAT = '%Y-%m-%d-%H_%M_%S'
# length of timestamp format (without tz suffix)
# e.g.: scheduled-2022-04-19-05_39_00_UTC (len = "2022-04-19-05_39_00")
SNAPSHOT_TS_FORMAT_LEN = 19
SNAPSHOT_PREFIX = 'scheduled'

log = logging.getLogger(__name__)


CephfsClientT = TypeVar('CephfsClientT', bound=CephfsClient)


@contextmanager
def open_ioctx(self: CephfsClientT,
               pool: Union[int, str]) -> Iterator[rados.Ioctx]:
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


FuncT = TypeVar('FuncT', bound=Callable[..., None])


def updates_schedule_db(func: FuncT) -> FuncT:
    def f(self: 'SnapSchedClient', fs: str, schedule_or_path: str, *args: Any) -> None:
        ret = func(self, fs, schedule_or_path, *args)
        path = schedule_or_path
        if isinstance(schedule_or_path, Schedule):
            path = schedule_or_path.path
        self.refresh_snap_timers(fs, path)
        return ret
    return cast(FuncT, f)


def get_prune_set(candidates: Set[Tuple[cephfs.DirEntry, datetime]],
                  retention: Dict[str, int],
                  max_snaps_to_retain: int) -> Set:
    PRUNING_PATTERNS = OrderedDict([
        # n is for keep last n snapshots, uses the snapshot name timestamp
        # format for lowest granularity
        # NOTE: prune set has tz suffix stripped out.
        ("n", SNAPSHOT_TS_FORMAT),
        # TODO remove M for release
        ("m", '%Y-%m-%d-%H_%M'),
        ("h", '%Y-%m-%d-%H'),
        ("d", '%Y-%m-%d'),
        ("w", '%G-%V'),
        ("M", '%Y-%m'),
        ("Y", '%Y'),
    ])
    keep = []
    if not retention:
        log.info(f'no retention set, assuming n: {max_snaps_to_retain}')
        retention = {'n': max_snaps_to_retain}
    for period, date_pattern in PRUNING_PATTERNS.items():
        log.debug(f'compiling keep set for period {period}')
        period_count = retention.get(period, 0)
        if not period_count:
            continue
        last = None
        kept_for_this_period = 0
        for snap in sorted(candidates, key=lambda x: x[0].d_name,
                           reverse=True):
            snap_ts = snap[1].strftime(date_pattern)
            if snap_ts != last:
                last = snap_ts
                if snap not in keep:
                    log.debug((f'keeping {snap[0].d_name} due to '
                               f'{period_count}{period}'))
                    keep.append(snap)
                    kept_for_this_period += 1
                    if kept_for_this_period == period_count:
                        log.debug(('found enough snapshots for '
                                   f'{period_count}{period}'))
                        break
    if len(keep) > max_snaps_to_retain:
        log.info(f'Pruning keep set; would retain first {max_snaps_to_retain}'
                 f' out of {len(keep)} snaps')
        keep = keep[:max_snaps_to_retain]
    return candidates - set(keep)

def snap_name_to_timestamp(scheduled_snap_name: str) -> str:
    """ extract timestamp from a schedule snapshot with tz suffix stripped out """
    ts = scheduled_snap_name.lstrip(f'{SNAPSHOT_PREFIX}-')
    return ts[0:SNAPSHOT_TS_FORMAT_LEN]

class DBInfo():
    def __init__(self, fs: str, db: sqlite3.Connection):
        self.fs: str = fs
        self.lock: Lock = Lock()
        self.db: sqlite3.Connection = db


# context manager for serializing db connection usage
class DBConnectionManager():
    def __init__(self, info: DBInfo):
        self.dbinfo: DBInfo = info

    # using string as return type hint since __future__.annotations is not
    # available with Python 3.6; its avaialbe starting from Pytohn 3.7
    def __enter__(self) -> 'DBConnectionManager':
        log.debug(f'locking db connection for {self.dbinfo.fs}')
        self.dbinfo.lock.acquire()
        log.debug(f'locked db connection for {self.dbinfo.fs}')
        return self

    def __exit__(self,
                 exception_type: Optional[Type[BaseException]],
                 exception_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        log.debug(f'unlocking db connection for {self.dbinfo.fs}')
        self.dbinfo.lock.release()
        log.debug(f'unlocked db connection for {self.dbinfo.fs}')


class SnapSchedClient(CephfsClient):

    def __init__(self, mgr: Any) -> None:
        super(SnapSchedClient, self).__init__(mgr)
        # Each db connection is now guarded by a Lock; this is required to
        # avoid concurrent DB transactions when more than one paths in a
        # file-system are scheduled at the same interval eg. 1h; without the
        # lock, there are races to use the same connection, causing  nested
        # transactions to be aborted
        self.sqlite_connections: Dict[str, DBInfo] = {}
        self.active_timers: Dict[Tuple[str, str], List[Timer]] = {}
        self.conn_lock: Lock = Lock()  # lock to protect add/lookup db connections

        # restart old schedules
        for fs_name in self.get_all_filesystems():
            with self.get_schedule_db(fs_name) as conn_mgr:
                db = conn_mgr.dbinfo.db
                sched_list = Schedule.list_all_schedules(db, fs_name)
                for sched in sched_list:
                    self.refresh_snap_timers(fs_name, sched.path, db)

    @property
    def allow_minute_snaps(self) -> None:
        return self.mgr.get_module_option('allow_m_granularity')

    @property
    def dump_on_update(self) -> None:
        return self.mgr.get_module_option('dump_on_update')

    def _create_snap_schedule_kv_db(self, db: sqlite3.Connection) -> None:
        SQL = """
        CREATE TABLE IF NOT EXISTS SnapScheduleModuleKV (
          key TEXT PRIMARY KEY,
          value NOT NULL
        ) WITHOUT ROWID;
        INSERT OR IGNORE INTO SnapScheduleModuleKV (key, value) VALUES ('__snap_schedule_db_version', 1);
        """
        db.executescript(SQL)

    def _get_snap_schedule_db_version(self, db: sqlite3.Connection) -> int:
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
    def _upgrade_snap_schedule_db_schema(self, db: sqlite3.Connection) -> None:
        # add a column to hold the subvolume group name
        if self._get_snap_schedule_db_version(db) < 2:
            SQL = """
            ALTER TABLE schedules
            ADD COLUMN group_name TEXT;
            """
            db.executescript(SQL)

            # bump up the snap-schedule db version to 2
            SQL = "UPDATE OR ROLLBACK SnapScheduleModuleKV SET value = ? WHERE key = '__snap_schedule_db_version';"
            db.execute(SQL, (2,))

    def get_schedule_db(self, fs: str) -> DBConnectionManager:
        dbinfo = None
        self.conn_lock.acquire()
        if fs not in self.sqlite_connections:
            poolid = self.get_metadata_pool(fs)
            assert poolid, f'fs "{fs}" not found'
            uri = f"file:///*{poolid}:/{SNAP_DB_OBJECT_NAME}.db?vfs=ceph"
            log.debug(f"using uri {uri}")
            db = sqlite3.connect(uri, check_same_thread=False, uri=True)
            db.execute('PRAGMA FOREIGN_KEYS = 1')
            db.execute('PRAGMA JOURNAL_MODE = PERSIST')
            db.execute('PRAGMA PAGE_SIZE = 65536')
            db.execute('PRAGMA CACHE_SIZE = 256')
            db.execute('PRAGMA TEMP_STORE = memory')
            db.row_factory = sqlite3.Row
            # check for legacy dump store
            pool_param = cast(Union[int, str], poolid)
            with open_ioctx(self, pool_param) as ioctx:
                try:
                    size, _mtime = ioctx.stat(SNAP_DB_OBJECT_NAME)
                    dump = ioctx.read(SNAP_DB_OBJECT_NAME, size).decode('utf-8')
                    db.executescript(dump)
                    ioctx.remove_object(SNAP_DB_OBJECT_NAME)
                except rados.ObjectNotFound:
                    log.debug(f'No legacy schedule DB found in {fs}')
            db.executescript(Schedule.CREATE_TABLES)
            self._create_snap_schedule_kv_db(db)
            self._upgrade_snap_schedule_db_schema(db)
            self.sqlite_connections[fs] = DBInfo(fs, db)
        dbinfo = self.sqlite_connections[fs]
        self.conn_lock.release()
        return DBConnectionManager(dbinfo)

    def _is_allowed_repeat(self, exec_row: Dict[str, str], path: str) -> bool:
        if Schedule.parse_schedule(exec_row['schedule'])[1] == 'm':
            if self.allow_minute_snaps:
                log.debug(('Minute repeats allowed, '
                           f'scheduling snapshot on path {path}'))
                return True
            else:
                log.info(('Minute repeats disabled, '
                          f'skipping snapshot on path {path}'))
                return False
        else:
            return True

    def fetch_schedules(self, db: sqlite3.Connection, path: str) -> List[sqlite3.Row]:
        with db:
            if self.dump_on_update:
                dump = [line for line in db.iterdump()]
                dump = "\n".join(dump)
                log.debug(f"db dump:\n{dump}")
            cur = db.execute(Schedule.EXEC_QUERY, (path,))
            all_rows = cur.fetchall()
            rows = [r for r in all_rows
                    if self._is_allowed_repeat(r, path)][0:1]
            return rows

    def refresh_snap_timers(self, fs: str, path: str, olddb: Optional[sqlite3.Connection] = None) -> None:
        try:
            log.debug((f'SnapDB on {fs} changed for {path}, '
                       'updating next Timer'))
            rows = []
            # olddb is passed in the case where we land here without a timer
            # the lock on the db connection has already been taken
            if olddb:
                rows = self.fetch_schedules(olddb, path)
            else:
                with self.get_schedule_db(fs) as conn_mgr:
                    db = conn_mgr.dbinfo.db
                    rows = self.fetch_schedules(db, path)
            timers = self.active_timers.get((fs, path), [])
            for timer in timers:
                timer.cancel()
            timers = []
            for row in rows:
                log.debug(f'Creating new snapshot timer for {path}')
                t = Timer(row[1],
                          self.create_scheduled_snapshot,
                          args=[fs, path, row[0], row[2], row[3]])
                t.start()
                timers.append(t)
                log.debug(f'Will snapshot {path} in fs {fs} in {row[1]}s')
            self.active_timers[(fs, path)] = timers
        except Exception:
            self._log_exception('refresh_snap_timers')

    def _log_exception(self, fct: str) -> None:
        log.error(f'{fct} raised an exception:')
        log.error(traceback.format_exc())

    def create_scheduled_snapshot(self,
                                  fs_name: str,
                                  path: str,
                                  retention: str,
                                  start: str,
                                  repeat: str) -> None:
        log.debug(f'Scheduled snapshot of {path} triggered')
        set_schedule_to_inactive = False
        try:
            with self.get_schedule_db(fs_name) as conn_mgr:
                db = conn_mgr.dbinfo.db
                try:
                    sched = Schedule.get_db_schedules(path,
                                                      db,
                                                      fs_name,
                                                      repeat=repeat,
                                                      start=start)[0]
                    time = datetime.now(timezone.utc)
                    with open_filesystem(self, fs_name) as fs_handle:
                        snap_ts = time.strftime(SNAPSHOT_TS_FORMAT_TZ)
                        snap_dir = self.mgr.rados.conf_get('client_snapdir')
                        snap_name = f'{path}/{snap_dir}/{SNAPSHOT_PREFIX}-{snap_ts}'
                        fs_handle.mkdir(snap_name, 0o755)
                    log.info(f'created scheduled snapshot of {path}')
                    log.debug(f'created scheduled snapshot {snap_name}')
                    sched.update_last(time, db)
                except cephfs.ObjectNotFound:
                    # maybe path is missing or wrong
                    self._log_exception('create_scheduled_snapshot')
                    log.debug(f'path {path} is probably missing or wrong; '
                              'remember to strip off the mount point path '
                              'prefix to provide the correct path')
                    set_schedule_to_inactive = True
                except cephfs.Error:
                    self._log_exception('create_scheduled_snapshot')
                except Exception:
                    # catch all exceptions cause otherwise we'll never know since this
                    # is running in a thread
                    self._log_exception('create_scheduled_snapshot')
                finally:
                    if set_schedule_to_inactive:
                        sched.set_inactive(db)
        finally:
            with self.get_schedule_db(fs_name) as conn_mgr:
                db = conn_mgr.dbinfo.db
                self.refresh_snap_timers(fs_name, path, db)
            self.prune_snapshots(sched)

    def prune_snapshots(self, sched: Schedule) -> None:
        try:
            log.debug('Pruning snapshots')
            ret = sched.retention
            path = sched.path
            prune_candidates = set()
            time = datetime.now(timezone.utc)
            mds_max_snaps_per_dir = self.mgr.get_ceph_option('mds_max_snaps_per_dir')
            with open_filesystem(self, sched.fs) as fs_handle:
                snap_dir = self.mgr.rados.conf_get('client_snapdir')
                with fs_handle.opendir(f'{path}/{snap_dir}') as d_handle:
                    dir_ = fs_handle.readdir(d_handle)
                    while dir_:
                        if dir_.d_name.decode('utf-8').startswith(f'{SNAPSHOT_PREFIX}-'):
                            log.debug(f'add {dir_.d_name} to pruning')
                            ts = datetime.strptime(
                                snap_name_to_timestamp(dir_.d_name.decode('utf-8')), SNAPSHOT_TS_FORMAT)
                            prune_candidates.add((dir_, ts))
                        else:
                            log.debug(f'skipping dir entry {dir_.d_name}')
                        dir_ = fs_handle.readdir(d_handle)
                # Limit ourselves to one snapshot less than allowed by config to allow for
                # snapshot creation before pruning
                to_prune = get_prune_set(prune_candidates, ret, mds_max_snaps_per_dir - 1)
                for k in to_prune:
                    dirname = k[0].d_name.decode('utf-8')
                    log.debug(f'rmdir on {dirname}')
                    fs_handle.rmdir(f'{path}/{snap_dir}/{dirname}')
                if to_prune:
                    with self.get_schedule_db(sched.fs) as conn_mgr:
                        db = conn_mgr.dbinfo.db
                        sched.update_pruned(time, db, len(to_prune))
        except Exception:
            self._log_exception('prune_snapshots')

    def get_snap_schedules(self, fs: str, path: str) -> List[Schedule]:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            return Schedule.get_db_schedules(path, db, fs)

    def list_snap_schedules(self,
                            fs: str,
                            path: str,
                            recursive: bool) -> List[Schedule]:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            return Schedule.list_schedules(path, db, fs, recursive)

    @updates_schedule_db
    # TODO improve interface
    def store_snap_schedule(self,
                            fs: str, path_: str,
                            args: Tuple[str, str, str, str,
                                        Optional[str], Optional[str],
                                        Optional[str]]) -> None:
        sched = Schedule(*args)
        log.debug(f'repeat is {sched.repeat}')
        if sched.parse_schedule(sched.schedule)[1] == 'm' and not self.allow_minute_snaps:
            log.error('not allowed')
            raise ValueError('invalid schedule specified - multiplier should '
                             'be one of h,d,w,M,Y')
        log.debug(f'attempting to add schedule {sched}')
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            sched.store_schedule(db)

    @updates_schedule_db
    def rm_snap_schedule(self,
                         fs: str, path: str,
                         schedule: Optional[str],
                         start: Optional[str]) -> None:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            Schedule.rm_schedule(db, path, schedule, start)

    @updates_schedule_db
    def add_retention_spec(self,
                           fs: str,
                           path: str,
                           retention_spec_or_period: str,
                           retention_count: Optional[str]) -> None:
        retention_spec = retention_spec_or_period
        if retention_count:
            retention_spec = retention_count + retention_spec
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            Schedule.add_retention(db, path, retention_spec)

    @updates_schedule_db
    def rm_retention_spec(self,
                          fs: str,
                          path: str,
                          retention_spec_or_period: str,
                          retention_count: Optional[str]) -> None:
        retention_spec = retention_spec_or_period
        if retention_count:
            retention_spec = retention_count + retention_spec
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            Schedule.rm_retention(db, path, retention_spec)

    @updates_schedule_db
    def activate_snap_schedule(self,
                               fs: str,
                               path: str,
                               schedule: Optional[str],
                               start: Optional[str]) -> None:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            schedules = Schedule.get_db_schedules(path, db, fs,
                                                  schedule=schedule,
                                                  start=start)
            for s in schedules:
                s.set_active(db)

    @updates_schedule_db
    def deactivate_snap_schedule(self,
                                 fs: str, path: str,
                                 schedule: Optional[str],
                                 start: Optional[str]) -> None:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            schedules = Schedule.get_db_schedules(path, db, fs,
                                                  schedule=schedule,
                                                  start=start)
            for s in schedules:
                s.set_inactive(db)
