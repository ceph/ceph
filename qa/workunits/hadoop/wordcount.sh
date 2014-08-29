#!/bin/sh -ex

echo "starting hadoop-wordcount test"

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR ] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

# if HADOOP_PREFIX is not set, use default
[ -z $HADOOP_PREFIX ] && { HADOOP_PREFIX=$TESTDIR/apache_hadoop; }

# if HADOOP_MR_HOME is not set, use default
[ -z $HADOOP_MR_HOME ] && { HADOOP_MR_HOME=$TESTDIR/apache_hadoop/build; }

export JAVA_HOME=/usr/lib/jvm/default-java

set -e
set -x

# Clear out in case there was a previous run (idempotency)
if $HADOOP_PREFIX/bin/hadoop fs -ls /wordcount_output 2>/dev/null ; then
    $HADOOP_PREFIX/bin/hadoop fs -rmr /wordcount_output
fi
if $HADOOP_PREFIX/bin/hadoop fs -ls /wordcount_input 2>/dev/null ; then
    $HADOOP_PREFIX/bin/hadoop fs -rmr /wordcount_input
fi
rm -rf $TESTDIR/hadoop_input

# Load input files into local filesystem
mkdir -p $TESTDIR/hadoop_input
wget http://ceph.com/qa/hadoop_input_files.tar -O $TESTDIR/hadoop_input/files.tar
cd $TESTDIR/hadoop_input
tar -xf $TESTDIR/hadoop_input/files.tar

# Load input files into hadoop filesystem
$HADOOP_PREFIX/bin/hadoop fs -mkdir /wordcount_input
$HADOOP_PREFIX/bin/hadoop fs -put $TESTDIR/hadoop_input/*txt /wordcount_input/

# Execute job
$HADOOP_PREFIX/bin/hadoop jar $HADOOP_MR_HOME/hadoop*examples*jar wordcount /wordcount_input /wordcount_output

# Clean up
$HADOOP_PREFIX/bin/hadoop fs -rmr /wordcount_output
$HADOOP_PREFIX/bin/hadoop fs -rmr /wordcount_input
cd $TESTDIR
rm -rf $TESTDIR/hadoop_input

echo "completed hadoop-wordcount test"
exit 0
