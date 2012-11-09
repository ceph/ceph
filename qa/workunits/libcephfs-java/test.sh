#!/bin/sh -e

BASE=/tmp/cephtest
TLIB=binary/usr/local/lib

echo "starting libcephfs-java tests"

export LD_LIBRARY_PATH=$BASE/$TLIB 
command="java -DCEPH_CONF_FILE=$BASE/ceph.conf -Djava.library.path=$LD_LIBRARY_PATH -cp /usr/share/java/junit4.jar:$BASE/$TLIB/libcephfs.jar:$BASE/$TLIB/libcephfs-test.jar org.junit.runner.JUnitCore com.ceph.fs.CephAllTests"

echo "----------------------"
echo $command
echo "----------------------"

$command

echo "completed libcephfs-java tests"

exit 0
