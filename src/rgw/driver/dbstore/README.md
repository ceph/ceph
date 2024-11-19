# DBStore
Standalone Rados Gateway (RGW) on DBStore (Experimental)


## CMake Option
Add below cmake option (enabled by default)

    -DWITH_RADOSGW_DBSTORE=ON 


## Build

    cd build
    ninja [vstart]


## Running Test cluster
Edit ceph.conf to add below options

    [client]
        rgw backend store = dbstore
        rgw config store = dbstore

To start the `vstart` cluster, run the following cmd:

    MON=0 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d --rgw_store dbstore

The above `vstart` command brings up the RGW server on DBStore without the need for MONs or OSDs. It creates a default zonegroup, zone, and few default users (e.g., `testid`) to be used for S3 operations, and generates database files in the `dev` subdirectory, by default, to store them.

`radosgw-admin` command can be used to create and remove other users, zonegroups and zones.

The location and prefix for the database files can be configured using the following options:
    [client]
        dbstore db dir = <path for the directory for storing the db backend store data>
        dbstore db name prefix = <prefix to the file names created by db backend store>
        dbstore config uri = <Config database URI. URIs beginning with file: refer to local files opened with SQLite.>


## DBStore Unit Tests
To execute DBStore unit test cases (using Gtest framework), from build directory

    ninja unittest_dbstore_tests
    ./bin/unittest_dbstore_tests [logfile] [loglevel]
    (default logfile: rgw_dbstore_tests.log, loglevel: 20)
    ninja unittest_dbstore_mgr_tests
    ./bin/unittest_dbstore_mgr_tests

To execute Sample test file

    ninja src/rgw/driver/dbstore/install
    ./bin/dbstore-bin [logfile] [loglevel]
    (default logfile: rgw_dbstore_bin.log, loglevel: 20)

