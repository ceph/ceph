#!/bin/bash

set -e

DEPTH=5
COUNT=10000

kill_jobs() {
  jobs -p | xargs kill
}
trap kill_jobs INT

create_files() {
  for i in `seq 1 $COUNT`
  do
    touch file$i
  done
}

delete_files() {
  for i in `ls -f`
  do
    if [[ ${i}a = file*a ]]
    then
      rm -f $i
    fi
  done
}

rm -rf testdir
mkdir testdir
cd testdir

echo "creating folder hierarchy"
for i in `seq 1 $DEPTH`; do
  mkdir dir$i
  cd dir$i
  create_files &
done
wait

echo "created hierarchy, now cleaning up"

for i in `seq 1 $DEPTH`; do
  delete_files &
  cd ..
done
wait

echo "cleaned up hierarchy"
cd ..
rm -rf testdir
