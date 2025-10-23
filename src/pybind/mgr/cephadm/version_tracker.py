import errno
import json
import datetime
import sqlite3
from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    from .module import CephadmOrchestrator


SCHEMA = [
    '''
    CREATE TABLE IF NOT EXISTS ClusterVersionInfo(
        cluster_version_id INTEGER PRIMARY KEY,
        cluster_version TEXT NOT NULL,
        creation_time TEXT NOT NULL
    );
    ''',
    '''
    CREATE TABLE IF NOT EXISTS VersionAssociation(
        id INTEGER PRIMARY KEY,
        cluster_version_id INTEGER NOT NULL,
        FOREIGN KEY (cluster_version_id) REFERENCES ClusterVersionInfo(cluster_version_id)
    );
    '''
]

SCHEMA_VERSIONED = [
    [
        '''
        CREATE TABLE IF NOT EXISTS ClusterVersionInfo(
            cluster_version_id INTEGER PRIMARY KEY,
            cluster_version TEXT NOT NULL,
            creation_time TEXT NOT NULL
        );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS VersionAssociation(
            id INTEGER PRIMARY KEY,
            cluster_version_id INTEGER NOT NULL,
            FOREIGN KEY (cluster_version_id) REFERENCES ClusterVersionInfo(cluster_version_id)
        );
        '''
    ]
]


class VersionTracker:

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr

    def _cluster_version_history_is_empty(self) -> bool:
        """
        Returns true if there is no cluster version history, returns false if there is cluster version history or on error
        """

        SQL_QUERY = '''
        SELECT 1
            FROM ClusterVersionInfo
            LIMIT 1;
        '''

        if not self.mgr.db_ready():
            self.mgr.log.debug('Version Tracker, cluster version history empty status could not be checked: mgr db not ready')
            return False

        with self.mgr._db_lock, self.mgr.db:
            try:
                cursor = self.mgr.db.execute(SQL_QUERY)
                row = cursor.fetchone()
            except sqlite3.Error as error:
                self.mgr.log.debug('Version Tracker, cluster version history empty status could not be checked: ' + str(error))
                return False

            if row is None:
                return True

        if not self.mgr.bootstrap_version_stored:
            self.mgr.bootstrap_version_stored = True

        return False

    def add_cluster_version(self, version: str, time: str) -> bool:
        """
        Adds cluster version to mgr db, returns true on success, returns false on error
        """

        SQL_QUERY = '''
        INSERT OR IGNORE INTO ClusterVersionInfo (cluster_version, creation_time)
            VALUES (?, ?);
        '''

        if not self.mgr.db_ready():
            self.mgr.log.debug('Version Tracker, cluster version "' + str(version) + '" could not be added: mgr db not ready')
            return False

        with self.mgr._db_lock, self.mgr.db:
            try:
                self.mgr.db.execute(SQL_QUERY, (version, time))
            except sqlite3.Error as error:
                self.mgr.log.debug('Version Tracker, cluster version "' + str(version) + '" could not be added: ' + str(error))
                return False

        self.mgr.log.debug('Version Tracker, cluster version "' + str(version) + '" added successfully')

        return True

    def add_bootstrap_cluster_version(self) -> None:
        if self._cluster_version_history_is_empty():
            status = False

            if self.mgr.get_store_prefix('bootstrap_version') and self.mgr.get_store_prefix('bootstrap_time'):
                status = self.add_cluster_version(self.mgr.get_store('bootstrap_version'), self.mgr.get_store('bootstrap_time'))
            else:
                status = self.add_cluster_version(self.mgr._version, str(datetime.datetime.now(datetime.timezone.utc)))

            if status:
                self.mgr.bootstrap_version_stored = True
                self.mgr.log.debug('Version Tracker, cluster bootstrap version added successfully')
            else:
                self.mgr.log.debug('Version Tracker, cluster bootstrap version could not be added')

    def get_cluster_version_history(self) -> Tuple[int, str, str]:
        SQL_QUERY = '''
        SELECT cluster_version, creation_time
            FROM ClusterVersionInfo
            ORDER BY creation_time ASC;
        '''

        if not self.mgr.db_ready():
            return -errno.EAGAIN, '', 'mgr db not yet available'

        res = dict()

        with self.mgr._db_lock, self.mgr.db:
            try:
                cursor = self.mgr.db.execute(SQL_QUERY)
                rows = cursor.fetchall()
            except sqlite3.Error as error:
                return -errno.EIO, '', str(error)

            for row in rows:
                res[row['creation_time']] = row['cluster_version']

        if not res:
            return 0, 'No Cluster Version History', ''

        return 0, json.dumps(res, indent=4), ''

    def remove_cluster_version_history(self, all: Optional[bool] = False, before: Optional[str] = None, after: Optional[str] = None) -> Tuple[int, str, str]:
        SQL_QUERY_BEFORE = '''
        DELETE FROM ClusterVersionInfo
            WHERE creation_time <= ?;
        '''

        SQL_QUERY_AFTER = '''
        DELETE FROM ClusterVersionInfo
            WHERE creation_time >= ?;
        '''

        SQL_QUERY_RANGE = '''
        DELETE FROM ClusterVersionInfo
            WHERE creation_time BETWEEN ? AND ?
        '''

        SQL_QUERY_ALL = '''
        DELETE FROM ClusterVersionInfo;
        '''

        if not self.mgr.db_ready():
            return -errno.EAGAIN, '', 'mgr db not yet available'

        if self._cluster_version_history_is_empty():
            return 0, 'No Cluster Version History', ''

        if not all and not before and not after:
            return -errno.EINVAL, '', 'requires at least one of the options "all", "before", "after" to be set'

        if all and (before or after):
            return -errno.EINVAL, '', 'cannot have option "all" set while option "before" or option "after" are set'

        if before:
            try:
                datetime.datetime.strptime(before, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return -errno.EINVAL, '', 'invalid datetime format for option "before", use "YYYY-MM-DD HH:MM:SS"'

        if after:
            try:
                datetime.datetime.strptime(after, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return -errno.EINVAL, '', 'invalid datetime format for option "after", use "YYYY-MM-DD HH:MM:SS"'

        with self.mgr._db_lock, self.mgr.db:
            if all:
                try:
                    self.mgr.db.execute(SQL_QUERY_ALL)
                except sqlite3.Error as error:
                    return -errno.EIO, '', str(error)
            elif before and after:
                if before < after:
                    return -errno.EINVAL, '', 'option "before" cannot be a datetime less than option "after", command will remove entries in range [AFTER, BEFORE]'
                try:
                    self.mgr.db.execute(SQL_QUERY_RANGE, (after, before))
                except sqlite3.Error as error:
                    return -errno.EIO, '', str(error)
            elif before:
                try:
                    self.mgr.db.execute(SQL_QUERY_BEFORE, (before,))
                except sqlite3.Error as error:
                    return -errno.EIO, '', str(error)
            else:
                try:
                    self.mgr.db.execute(SQL_QUERY_AFTER, (after,))
                except sqlite3.Error as error:
                    return -errno.EIO, '', str(error)

        return 0, 'Cluster Version History Deletion Successful', ''
