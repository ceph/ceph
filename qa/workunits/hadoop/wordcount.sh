#!/bin/bash

set -e
set -x

WC_INPUT=/wc_input
WC_OUTPUT=/wc_output
DATA_INPUT=$(mktemp -d)

echo "starting hadoop-wordcount test"

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR ] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

# if HADOOP_PREFIX is not set, use default
[ -z $HADOOP_PREFIX ] && { HADOOP_PREFIX=$TESTDIR/hadoop; }

# Nuke hadoop directories
$HADOOP_PREFIX/bin/hadoop fs -rm -r $WC_INPUT $WC_OUTPUT || true

# Fetch and import testing data set
curl http://download.ceph.com/qa/hadoop_input_files.tar | tar xf - -C $DATA_INPUT
$HADOOP_PREFIX/bin/hadoop fs -copyFromLocal $DATA_INPUT $WC_INPUT
rm -rf $DATA_INPUT

# Run the job
$HADOOP_PREFIX/bin/hadoop jar \
  $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  wordcount $WC_INPUT $WC_OUTPUT

# Cleanup
$HADOOP_PREFIX/bin/hadoop fs -rm -r $WC_INPUT $WC_OUTPUT || true

echo "completed hadoop-wordcount test"
exit 0
