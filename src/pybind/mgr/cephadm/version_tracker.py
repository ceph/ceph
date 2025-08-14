from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator



class VersionTracker:

    def __init__(self, mgr: CephadmOrchestrator) -> None:
        self.mgr = mgr

        self.db = self.mgr.db()
        

    def add_cluster_version(self, version: str) -> None:
        SQL_QUERY = '''
        INSERT OR IGNORE INTO ClusterVersionInfo (version)
            VALUES (?);
        '''

        self.db.execute(SQL_QUERY, (version))

    def get_cluster_version_history():
        SQL_QUERY = '''
        SELECT cluster_version, creation_time
            FROM ClusterVersionInfo
            ORDER BY creation_time ASC;
        '''
        
    def get_initial_cluster_version():
        SQL_QUERY = '''
        SELECT cluster_version, creation_time
            From ClusterVersionInfo
            WHERE is_initial_version = 1
        '''

    def get_current_cluster_version():
        SQL_QUERY = '''
        SELECT cluster_version, creation_time
            From ClusterVersionInfo
            WHERE is_current_version = 1
        '''


# look through devicehealth to see how it integrated with MgrModule's db functions, check if it's possible to even do that with cephadm
