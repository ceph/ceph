# DBStore
Standalone Rados Gateway (RGW) on DBStore (Experimental)


## CMake Option
Add below cmake option (enabled by default)

    -DWITH_RADOSGW_DBSTORE=ON 


## Build

    cd build
    ninja [vstart]


## Running Test cluster
Edit ceph.conf to add below option

    [client]
        rgw backend store = dbstore
        rgw config store = dbstore

Start vstart cluster

    MON=1 RGW=1 ../src/vstart.sh -o rgw_backend_store=dbstore -o rgw_config_store=dbstore -n -d

The above vstart command brings up RGW server on dbstore. It creates default zonegroup, zone and few default users (eg., testid) to be used for s3 operations.

`radosgw-admin` can be used to create and remove other users, zonegroups and zones.


By default, dbstore creates .db file *'/var/lib/ceph/radosgw/dbstore-default_ns.db'* to store the data and *'/var/lib/ceph/radosgw/dbstore-config.db'* file to store the configuration. This can be configured using below options in ceph.conf

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

