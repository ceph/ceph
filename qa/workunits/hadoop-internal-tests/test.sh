#!/bin/sh -e

BASE=/tmp/cephtest
TLIB=binary/usr/local/lib

echo "starting hadoop-internal-tests tests"

export LD_LIBRARY_PATH=$BASE/$TLIB 
command1="cd $BASE/hadoop"
command2="ant -Dextra.library.path=$BASE/$TLIB -Dceph.conf.file=$BASE/ceph.conf test -Dtestcase=TestCephFileSystem"

#print out the command
echo "----------------------"
echo $command1
echo "----------------------"
echo $command2
echo "----------------------"

#now execute the command
$command1
$command2

echo "completed hadoop-internal-tests tests"
exit 0
