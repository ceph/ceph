#!/bin/bash -x

#set -e

basedir=`pwd`
testdir="${basedir}/testspace"

mkdir -p $testdir

for test in `cd $basedir && find workunits/* | grep .sh`
do
  cd $testdir
  echo "------ running test $test ------"
  mkdir -p $test
  pushd .
  cd $test
  ${basedir}/${test}
  popd
done