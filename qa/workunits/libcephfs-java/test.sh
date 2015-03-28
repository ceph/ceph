#!/bin/sh -e

echo "starting libcephfs-java tests"
# configure CEPH_CONF and LD_LIBRARY_PATH if they're not already set
conf="$CEPH_CONF"
if [ -z "$conf" ] ; then
	echo "Setting conf to /etc/ceph/ceph.conf" 
	conf="/etc/ceph/ceph.conf"
else
	echo "conf is set to $conf"
fi

ld_lib_path="$LD_LIBRARY_PATH"
if [ -z "$ld_lib_path" ] ; then
	echo "Setting ld_lib_path to /usr/lib/jni:/usr/lib64"
	ld_lib_path="/usr/lib/jni:/usr/lib64"
else
	echo "ld_lib_path was set to $ld_lib_path"
fi

ceph_java="$CEPH_JAVA_PATH"
if [ -z "$ceph_java" ] ; then
	echo "Setting ceph_java to /usr/share/java"
	ceph_java="/usr/share/java"
else
	echo "ceph_java was set to $ceph_java"
fi

command="java -DCEPH_CONF_FILE=$conf -Djava.library.path=$ld_lib_path -cp /usr/share/java/junit4.jar:$ceph_java/libcephfs.jar:$ceph_java/libcephfs-test.jar org.junit.runner.JUnitCore com.ceph.fs.CephAllTests"

echo "----------------------"
echo $command
echo "----------------------"

$command

echo "completed libcephfs-java tests"

exit 0
