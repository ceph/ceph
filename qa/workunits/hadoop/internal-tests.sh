#!/bin/bash -e

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR ] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

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
	echo "Setting ld_lib_path to /usr/lib/jni"
	ld_lib_path="/usr/lib/jni"
else
	echo "ld_lib_path was set to $ld_lib_path"
fi

POOL_SIZES=`seq 1 8`
POOL_BASE=hadoop
POOL_NAMES=`echo -n $POOL_SIZES | sed "s/\([0-9]*\)/$POOL_BASE\1/g" | sed "s/ /,/g"`

function gen_hadoop_conf() {
local outfile=$1
local poolnames=$2
local conf=$3
cat << EOF > $outfile
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>ceph.conf.file</name>
  <value>$conf</value>
</property>
<property>
  <name>ceph.data.pools</name>
  <value>$poolnames</value>
</property>
</configuration>
EOF
}

echo creating hadoop test pools
for size in $POOL_SIZES; do
  name=${POOL_BASE}$size
  echo creating pool $name
  ceph osd pool create $name 100 100
  ceph osd pool set $name size $size

  echo making pool $name a data pool
  poolid=`ceph osd dump | sed -n "s/^pool \([0-9]*\) '$name'.*/\1/p"`
  ceph mds add_data_pool $poolid
done

def_repl_conf=`mktemp`
echo generating default replication hadoop config $def_repl_conf
gen_hadoop_conf $def_repl_conf "" $conf

cust_repl_conf=`mktemp`
echo generating custom replication hadoop config $cust_repl_conf
gen_hadoop_conf $cust_repl_conf $POOL_NAMES $conf

echo running default replication hadoop tests
java -Dhadoop.conf.file=$def_repl_conf -Djava.library.path=$ld_lib_path -cp /usr/share/java/junit4.jar:$TESTDIR/apache_hadoop/build/hadoop-core-1.0.4-SNAPSHOT.jar:$TESTDIR/inktank_hadoop/build/hadoop-cephfs.jar:$TESTDIR/inktank_hadoop/build/hadoop-cephfs-test.jar:$TESTDIR/apache_hadoop/build/hadoop-test-1.0.4-SNAPSHOT.jar:$TESTDIR/apache_hadoop/build/ivy/lib/Hadoop/common/commons-logging-1.1.1.jar:/usr/share/java/libcephfs.jar org.junit.runner.JUnitCore org.apache.hadoop.fs.ceph.TestCephDefaultReplication 

echo running custom replication hadoop tests
java -Dhadoop.conf.file=$cust_repl_conf -Djava.library.path=$ld_lib_path -cp /usr/share/java/junit4.jar:$TESTDIR/apache_hadoop/build/hadoop-core-1.0.4-SNAPSHOT.jar:$TESTDIR/inktank_hadoop/build/hadoop-cephfs.jar:$TESTDIR/inktank_hadoop/build/hadoop-cephfs-test.jar:$TESTDIR/apache_hadoop/build/hadoop-test-1.0.4-SNAPSHOT.jar:$TESTDIR/apache_hadoop/build/ivy/lib/Hadoop/common/commons-logging-1.1.1.jar:/usr/share/java/libcephfs.jar org.junit.runner.JUnitCore org.apache.hadoop.fs.ceph.TestCephCustomReplication 

echo "completed hadoop-internal-tests tests"
exit 0
