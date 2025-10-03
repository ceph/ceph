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
            self.mgr.log.debug('Version Tracker, Cluster version history empty status could not be checked: mgr db not ready')
            return False
        
        with self.mgr._db_lock, self.mgr.db:
            try:
                cursor = self.mgr.db.execute(SQL_QUERY)
                row = cursor.fetchone()
            except sqlite3.Error as error:
                self.mgr.log.debug('Version Tracker, Cluster version history empty status could not be checked: ' + str(error))
                return False
        
            if row is None:
                return True
            
            return False
        
    def _set_bootstrap_version(self, version: str) -> Tuple[int, str, str]:
        self.mgr.set_store('bootstrap-version', version)

        return 0, '', ''
    
    def _get_bootstrap_version(self) -> Tuple[int, str, str]:
        if self.mgr.get_store_prefix('bootstrap-version'):
            return 0, self.mgr.get_store('bootstrap-version'), ''
        
        return -errno.EPERM, '', 'bootstrap version not stored'
    
    def _set_bootstrap_time(self, time: str) -> Tuple[int, str, str]:
        self.mgr.set_store('bootstrap-time', time)

        return 0, '', ''
    
    def _get_bootstrap_time(self) -> Tuple[int, str, str]:
        if self.mgr.get_store_prefix('bootstrap-time'):
            return 0, self.mgr.get_store('bootstrap-time'), ''
        
        return -errno.EPERM, '', 'bootstrap time not stored'
    
    def add_cluster_version(self, version: str, time: str) -> bool:
        """
        Adds cluster version to mgr db, returns true on success, returns false on error
        """

        SQL_QUERY = '''
        INSERT OR IGNORE INTO ClusterVersionInfo (cluster_version, creation_time)
            VALUES (?, ?);
        '''

        if not self.mgr.db_ready():
            self.mgr.log.debug('Version Tracker, Cluster version "' + version + '" could not be added: mgr db not ready')
            return False
        
        with self.mgr._db_lock, self.mgr.db:
            try:
                self.mgr.db.execute(SQL_QUERY, (version, time))
            except sqlite3.Error as error:
                self.mgr.log.debug('Version Tracker, Cluster version "' + version + '" could not be added: ' + str(error))
                return False
        
        self.mgr.log.debug('Version Tracker, Cluster version "' + version + '" added successfully')
        
        return True

    def add_bootstrap_cluster_version(self) -> None:
        if self._cluster_version_history_is_empty():
            status = False

            if self.mgr.get_store_prefix('bootstrap-version') and self.mgr.get_store_prefix('bootstrap-time'):
                status = self.add_cluster_version(self.mgr.get_store('bootstrap-version'), self.mgr.get_store('bootstrap-time'))
            else:  
                status = self.add_cluster_version(self.mgr._version, str(datetime.datetime.now(datetime.timezone.utc)))

            if status:
                self.mgr.log.debug('Version Tracker, Cluster bootstrap version added successfully')
            else:
                self.mgr.log.debug('Version Tracker, Cluster bootstrap version could not be added')

    def get_cluster_version_history(self) -> Tuple[int, str, str]:
        SQL_QUERY = '''
        SELECT cluster_version, creation_time
            FROM ClusterVersionInfo
            ORDER BY creation_time ASC;
        '''

        if not self.mgr.db_ready():
            return -errno.EAGAIN, '', 'mgr db not yet available'
        
        self.add_bootstrap_cluster_version()

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

    def remove_cluster_version_history(self, time_stamp: Optional[str] = None) -> Tuple[int, str, str]:
        SQL_QUERY_OPTION = '''
        DELETE FROM ClusterVersionInfo
            WHERE creation_time < ?;
        '''

        SQL_QUERY_ALL = '''
        DELETE FROM ClusterVersionInfo;
        '''

        if not self.mgr.db_ready():
            return -errno.EAGAIN, '', 'mgr db not yet available'
        
        if self._cluster_version_history_is_empty():
            return 0, 'No Cluster Version History', ''
        
        with self.mgr._db_lock, self.mgr.db:
            if time_stamp is None:
                try:
                    self.mgr.db.execute(SQL_QUERY_ALL)
                except sqlite3.Error as error:
                    return -errno.EIO, '', str(error)
            else:
                try:
                    datetime.datetime.strptime(time_stamp, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return -errno.EINVAL, '', 'invalid datetime format, use "YYYY-MM-DD HH:MM:SS"'
                else:
                    try:
                        self.mgr.db.execute(SQL_QUERY_OPTION, (time_stamp,))
                    except sqlite3.Error as error:
                        return -errno.EIO, '', str(error)
                    
        return 0, 'Cluster Version History Deletion Successful', ''
    