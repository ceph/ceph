import errno
import json
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Optional, Tuple

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator



class VersionTracker:

    def __init__(self, mgr: CephadmOrchestrator) -> None:
        self.mgr = mgr
        self.db = self.mgr.db()
        self._db_lock = self.mgr._db_lock


    def add_cluster_version(self, version: str) -> None:
        SQL_QUERY = '''
        INSERT OR IGNORE INTO ClusterVersionInfo (version)
            VALUES (?);
        '''

        with self._db_lock, self.db:
            self.db.execute(SQL_QUERY, (version,))


    def get_cluster_version_history(self) -> Tuple[int, str, str]:
        SQL_QUERY = '''
        SELECT cluster_version, creation_time
            FROM ClusterVersionInfo
            ORDER BY creation_time ASC;
        '''

        res = Dict()

        with self._db_lock, self.db:
            cursor = self.db.execute(SQL_QUERY)

            for row in cursor:
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

        with self._db_lock, self.db:
            if time_stamp is None:
                self.db.execute(SQL_QUERY_ALL)
            else:
                try:
                    datetime.strptime(time_stamp, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return errno.EINVAL, '', 'not a valid time stamp'
                else:
                    self.db.execute(SQL_QUERY_OPTION, (time_stamp,))
        
        return 0, 'Cluster Version History Deletion Successful', ''



# look through devicehealth to see how it integrated with MgrModule's db functions, check if it's possible to even do that with cephadm
