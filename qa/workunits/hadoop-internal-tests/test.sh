#!/bin/bash -e

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR ] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

POOL_SIZES=`seq 1 8`
POOL_BASE=hadoop
POOL_NAMES=`echo -n $POOL_SIZES | sed "s/\([0-9]*\)/$POOL_BASE\1/g" | sed "s/ /,/g"`

function gen_hadoop_conf() {
local outfile=$1
local poolnames=$2
cat << EOF > $outfile
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>ceph.conf.file</name>
  <value>$CEPH_CONF</value>
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
  #./ceph osd pool create $name 100 100
  #./ceph osd pool set $name size $size
  ceph osd pool create $name 100 100
  ceph osd pool set $name size $size

  echo making pool $name a data pool
  poolid=`ceph osd dump | sed -n "s/^pool \([0-9]*\) '$name'.*/\1/p"`
  ceph mds add_data_pool $poolid
  #./ceph mds add_data_pool $poolid
done

def_repl_conf=`mktemp`
echo generating default replication hadoop config $def_repl_conf
gen_hadoop_conf $def_repl_conf ""

cust_repl_conf=`mktemp`
echo generating custom replication hadoop config $cust_repl_conf
gen_hadoop_conf $cust_repl_conf $POOL_NAMES

pushd $TESTDIR/hadoop

echo running default replication hadoop tests
ant -Dextra.library.path=$LD_LIBRARY_PATH -Dhadoop.conf.file=$def_repl_conf -Dtestcase=TestCephDefaultReplication test

echo running custom replication hadoop tests
ant -Dextra.library.path=$LD_LIBRARY_PATH -Dhadoop.conf.file=$cust_repl_conf -Dtestcase=TestCephCustomReplication test

popd

echo "completed hadoop-internal-tests tests"
exit 0
