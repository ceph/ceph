# dbstore
DBStore for Rados Gateway (RGW)

## Pre-install
fmt(-devel) and gtest(-devel) packages need to be installed

## Build
Add below options to cmake - 
-DWITH_RADOSGW_DBSTORE=ON -DWITH_RADOSGW_LUA_PACKAGES=OFF

cd build

ninja src/rgw/store/dbstore/install

## Gtests
To execute Gtest cases, from build directory

./bin/dbstore-tests
(default logfile: rgw_dbstore_tests.log, loglevel: 20)

## Execute Sample test file

./bin/dbstore-bin
(default logfile: rgw_dbstore_bin.log, loglevel: 20)

## Logging
[To provide custom log file and change log level]

./dbstore-tests log_file log_level
./dbstore-bin log_file log_level

When run as RADOSGW process, logfile and loglevel are set to the
ones provided to the radosgw cmd args.
