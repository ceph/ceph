#!/bin/sh -e

DEPTH=5
COUNT=10000

function kill_jobs {
  jobs -p | xargs kill
}
trap kill_jobs SIGINT

function create_files {
  for i in `seq 1 $COUNT`
  do
    touch file$i
  done
}

function delete_files {
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

for i in `seq 1 $DEPTH`; do
  mkdir dir$i
  cd dir$i
  create_files &
done
wait

for i in `seq 1 $DEPTH`; do
  delete_files &
  cd ..
done
wait

cd ..
rm -rf testdir
