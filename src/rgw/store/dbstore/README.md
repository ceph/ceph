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


By default, dbstore creates .db file *'/var/run/ceph/dbstore-default_ns.db'* to store the data. This can be configured using below options in ceph.conf

    [client]
        dbstore db dir = <path for the directory for storing the db backend store data>
        dbstore db name prefix = <prefix to the file names created by db backend store>


## DBStore Unit Tests
To execute DBStore unit test cases (using Gtest framework), from build directory

    ninja src/rgw/store/dbstore/install
    ./bin/dbstore-tests [logfile] [loglevel]
    (default logfile: rgw_dbstore_tests.log, loglevel: 20)

To execute Sample test file

    ./bin/dbstore-bin [logfile] [loglevel]
    (default logfile: rgw_dbstore_bin.log, loglevel: 20)

