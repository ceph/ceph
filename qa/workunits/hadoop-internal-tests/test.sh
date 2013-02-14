#!/bin/sh -e

echo "starting hadoop-internal-tests tests"

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

command1="cd $TESTDIR/hadoop"
command2="ant -Dextra.library.path=$LD_LIBRARY_PATH -Dceph.conf.file=$CEPH_CONF -Dtestcase=TestCephFileSystem"

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
