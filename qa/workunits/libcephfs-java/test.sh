#!/bin/sh -e

BASE=/tmp/cephtest
TLIB=binary/usr/local/lib

echo "starting libcephfs-java tests"

echo "----------------------"
echo java -DCEPH_CONF_FILE=$BASE/ceph.conf -Djava.library.path=$BASE/$TLIB/libcephfs.so:$BASE/$TLIB/libcephfs_jni.so:$BASE/$TLIB -cp /home/buck/git/ceph/src/java/lib/junit-4.8.2.jar:$BASE/$TLIB/libcephfs.jar:$BASE/$TLIB/libcephfs-test.jar org.junit.runner.JUnitCore com.ceph.fs.CephAllTests
echo "----------------------"

java -DCEPH_CONF_FILE=$BASE/ceph.conf -Djava.library.path=$BASE/$TLIB/libcephfs.so:$BASE/$TLIB/libcephfs_jni.so:$BASE/$TLIB -cp /home/buck/git/ceph/src/java/lib/junit-4.8.2.jar:$BASE/$TLIB/libcephfs.jar:$BASE/$TLIB/libcephfs-test.jar org.junit.runner.JUnitCore com.ceph.fs.CephAllTests

echo "completing libcephfs-java tests"

exit 0
