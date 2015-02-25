#!/bin/bash

set -e
set -x

INPUT=/terasort-input
OUTPUT=/terasort-output
REPORT=/tersort-report

num_records=100000
[ ! -z $NUM_RECORDS ] && num_records=$NUM_RECORDS

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR ] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

# if HADOOP_PREFIX is not set, use default
[ -z $HADOOP_PREFIX ] && { HADOOP_PREFIX=$TESTDIR/hadoop; }

# Nuke hadoop directories
$HADOOP_PREFIX/bin/hadoop fs -rm -r $INPUT $OUTPUT $REPORT || true

# Generate terasort data
#
#-Ddfs.blocksize=512M \
#-Dio.file.buffer.size=131072 \
#-Dmapreduce.map.java.opts=-Xmx1536m \
#-Dmapreduce.map.memory.mb=2048 \
#-Dmapreduce.task.io.sort.mb=256 \
#-Dyarn.app.mapreduce.am.resource.mb=1024 \
#-Dmapred.map.tasks=64 \
$HADOOP_PREFIX/bin/hadoop jar \
  $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  teragen \
  -Dmapred.map.tasks=9 \
  $num_records \
  $INPUT

# Run the sort job
#
#-Ddfs.blocksize=512M \
#-Dio.file.buffer.size=131072 \
#-Dmapreduce.map.java.opts=-Xmx1536m \
#-Dmapreduce.map.memory.mb=2048 \
#-Dmapreduce.map.output.compress=true \
#-Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.Lz4Codec \
#-Dmapreduce.reduce.java.opts=-Xmx1536m \
#-Dmapreduce.reduce.memory.mb=2048 \
#-Dmapreduce.task.io.sort.factor=100 \
#-Dmapreduce.task.io.sort.mb=768 \
#-Dyarn.app.mapreduce.am.resource.mb=1024 \
#-Dmapred.reduce.tasks=100 \
#-Dmapreduce.terasort.output.replication=1 \
$HADOOP_PREFIX/bin/hadoop jar \
  $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  terasort \
  -Dmapred.reduce.tasks=10 \
  $INPUT $OUTPUT

# Validate the sorted data
#
#-Ddfs.blocksize=512M \
#-Dio.file.buffer.size=131072 \
#-Dmapreduce.map.java.opts=-Xmx1536m \
#-Dmapreduce.map.memory.mb=2048 \
#-Dmapreduce.reduce.java.opts=-Xmx1536m \
#-Dmapreduce.reduce.memory.mb=2048 \
#-Dmapreduce.task.io.sort.mb=256 \
#-Dyarn.app.mapreduce.am.resource.mb=1024 \
#-Dmapred.reduce.tasks=1 \
$HADOOP_PREFIX/bin/hadoop jar \
  $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  teravalidate \
  -Dmapred.reduce.tasks=1 \
  $OUTPUT $REPORT

exit 0
