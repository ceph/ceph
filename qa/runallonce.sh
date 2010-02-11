#!/bin/bash -x

set -e

basedir=`echo $0 | sed 's/[^/]*$//g'`.
testdir="$1"

[ ${basedir:0:1} == "." ] && basedir=`pwd`/${basedir:1}

[ -z "$testdir" ] || [ ! -d "$testdir" ] && echo "specify test dir" && exit 1
cd $testdir

for test in `cd $basedir/workunits && ls | grep .sh`
do
  echo "------ running test $test ------"
  mkdir -p $test
  test -d ${basedir}/logs || mkdir -p ${basedir}/logs
  test -e ${basedir}/logs/${test}.log && rm ${basedir}/logs/${test}.log
  pushd .
  cd $test
  ${basedir}/workunits/${test} 2>&1 | tee ${basedir}/logs/${test}.log
  popd
done
