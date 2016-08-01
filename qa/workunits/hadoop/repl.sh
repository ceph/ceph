#!/bin/bash

set -e
set -x

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR ] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

# if HADOOP_PREFIX is not set, use default
[ -z $HADOOP_PREFIX ] && { HADOOP_PREFIX=$TESTDIR/hadoop; }

# create pools with different replication factors
for repl in 2 3 7 8 9; do
  name=hadoop.$repl
  ceph osd pool create $name 8 8
  ceph osd pool set $name size $repl

  id=`ceph osd dump | sed -n "s/^pool \([0-9]*\) '$name'.*/\1/p"`
  ceph mds add_data_pool $id
done

# create a file in each of the pools
for repl in 2 3 7 8 9; do
  name=hadoop.$repl
  $HADOOP_PREFIX/bin/hadoop fs -rm -f /$name.dat
  dd if=/dev/zero bs=1048576 count=1 | \
    $HADOOP_PREFIX/bin/hadoop fs -Dceph.data.pools="$name" \
    -put - /$name.dat
done

# check that hadoop reports replication matching
# that of the pool the file was written into
for repl in 2 3 7 8 9; do
  name=hadoop.$repl
  repl2=$($HADOOP_PREFIX/bin/hadoop fs -ls /$name.dat | awk '{print $2}')
  if [ $repl -ne $repl2 ]; then
    echo "replication factors didn't match!"
    exit 1
  fi
done

exit 0
