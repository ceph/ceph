#!/bin/bash -e

BASE=/tmp/cephtest
TLIB=binary/usr/local/lib
export LD_LIBRARY_PATH=$BASE/$TLIB 
CEPH_CONF_FILE=$BASE/ceph.conf

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
  <value>$CEPH_CONF_FILE</value>
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
  ./ceph osd pool create $name 100 100
  ./ceph osd pool set $name size $size

  echo making pool $name a data pool
  poolid=`./ceph osd dump | sed -n "s/^pool \([0-9]*\) '$name'.*/\1/p"`
  ./ceph mds add_data_pool $poolid
done

def_repl_conf=`mktemp`
echo generating default replication hadoop config $def_repl_conf
gen_hadoop_conf $def_repl_conf ""

cust_repl_conf=`mktemp`
echo generating custom replication hadoop config $cust_repl_conf
gen_hadoop_conf $cust_repl_conf $POOL_NAMES

pushd $BASE/hadoop

echo running default replication hadoop tests
ant -Dextra.library.path=$BASE/$TLIB -Dhadoop.conf.file=$def_repl_conf -Dtestcase=TestCephDefaultReplication test

echo running custom replication hadoop tests
ant -Dextra.library.path=$BASE/$TLIB -Dhadoop.conf.file=$def_repl_conf -Dtestcase=TestCephCustomReplication test

popd

echo "completed hadoop-internal-tests tests"
exit 0
