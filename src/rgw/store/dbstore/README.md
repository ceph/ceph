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

Restart vstart cluster or just RGW server

    [..] RGW=1 ../src/vstart.sh -d

The above configuration brings up RGW server on dbstore and creates testid user to be used for s3 operations.

By default, dbstore creates .db file named 'default_ns.db' where in the data is stored.


## DBStore Unit Tests
To execute DBStore unit test cases (using Gtest framework), from build directory

    ninja src/rgw/store/dbstore/install
    ./bin/dbstore-tests [logfile] [loglevel]
    (default logfile: rgw_dbstore_tests.log, loglevel: 20)

To execute Sample test file

    ./bin/dbstore-bin [logfile] [loglevel]
    (default logfile: rgw_dbstore_bin.log, loglevel: 20)

