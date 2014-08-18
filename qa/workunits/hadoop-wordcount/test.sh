#!/bin/sh -ex

echo "starting hadoop-wordcount test"

# bail if $TESTDIR is not set as this test will fail in that scenario
[ -z $TESTDIR ] && { echo "\$TESTDIR needs to be set, but is not. Exiting."; exit 1; }

# if HADOOP_HOME is not set, use default
[ -z $HADOOP_HOME ] && { HADOOP_HOME=$TESTDIR/apache_hadoop; }

# if HADOOP_MR_HOME is not set, use default
[ -z $HADOOP_MR_HOME ] && { HADOOP_MR_HOME=$TESTDIR/apache_hadoop/build; }

export JAVA_HOME=/usr/lib/jvm/default-java
mkdir -p $TESTDIR/hadoop_input
wget http://ceph.com/qa/hadoop_input_files.tar -O $TESTDIR/hadoop_input/files.tar
cd $TESTDIR/hadoop_input
tar -xf $TESTDIR/hadoop_input/files.tar
$HADOOP_HOME/bin/hadoop fs -mkdir /wordcount_input
$HADOOP_HOME/bin/hadoop fs -rm -r -f /wordcount_output
$HADOOP_HOME/bin/hadoop fs -put $TESTDIR/hadoop_input/*txt /wordcount_input/
$HADOOP_HOME/bin/hadoop jar $HADOOP_MR_HOME/hadoop-*examples.jar wordcount /wordcount_input /wordcount_output
rm -rf $TESTDIR/hadoop_input

echo "completed hadoop-wordcount test"
exit 0
