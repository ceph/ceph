import sqlite3

CREATE_TABLE = '''CREATE TABLE IF NOT EXISTS ClusterVersionInfo(
    cluster_version_id INTEGER PRIMARY KEY, 
    cluster_version TEXT NOT NULL,
    creation_time TEXT DEFAULT CURRENT_TIMESTAMP,
    is_initial_version INTEGER DEFAULT 0,
    is_current_version INTEGER DEFAULT 0,
);
CREATE TABLE IF NOT EXISTS VersionAssociation(
    id INTEGER PRIMARY KEY,
    FOREIGN KEY (cluster_version_id) REFERENCES ClusterVersionInfo(cluster_version_id) 
);'''


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
