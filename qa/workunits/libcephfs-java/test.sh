#!/bin/sh -e

echo "starting libcephfs-java tests"

command="java -DCEPH_CONF_FILE=$CEPH_CONF -Djava.library.path=$LD_LIBRARY_PATH -cp /usr/share/java/junit4.jar:$CEPH_JAVA_PATH/libcephfs.jar:$CEPH_JAVA_PATH/libcephfs-test.jar org.junit.runner.JUnitCore com.ceph.fs.CephAllTests"

echo "----------------------"
echo $command
echo "----------------------"

$command

echo "completed libcephfs-java tests"

exit 0
