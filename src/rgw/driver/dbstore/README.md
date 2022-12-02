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

Start vstart cluster

    [..] RGW=1 ../src/vstart.sh -o rgw_backend_store=dbstore -n -d

The above vstart command brings up RGW server on dbstore and creates few default users (eg., testid) to be used for s3 operations.

`radosgw-admin` can be used to create and remove other users.


By default, dbstore creates .db file *'/var/lib/ceph/radosgw/dbstore-default_ns.db'* to store the data. This can be configured using below options in ceph.conf

    [client]
        dbstore db dir = <path for the directory for storing the db backend store data>
        dbstore db name prefix = <prefix to the file names created by db backend store>


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

