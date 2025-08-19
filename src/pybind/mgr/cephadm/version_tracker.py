import json
from typing import TYPE_CHECKING, Dict, Tuple

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
            self.db.execute(SQL_QUERY, (version))


    def _do_get_cluster_version_history(self) -> Tuple[int, str, str]:
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

        return 0, json.dumps(res, indent=4), ''
    
    


# look through devicehealth to see how it integrated with MgrModule's db functions, check if it's possible to even do that with cephadm
