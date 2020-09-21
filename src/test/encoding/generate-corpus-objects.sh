#!/usr/bin/env bash
set -ex

BDIR=`pwd`

p=$1
echo path $p
test ! -d $p
mkdir $p
strings bin/ceph-osd | grep "^$p/%s__%d.%x"

v=`git describe | cut -c 2-`
echo version $v

echo 'binaries look ok, vstarting'
echo

MON=3 MDS=3 OSD=5 MDS=3 MGR=2 RGW=1 ../src/vstart.sh -x -n -l --bluestore -e

export PATH=bin:$PATH

# do some work to generate a hopefully braod set of object instances

echo 'starting some background work'
../qa/workunits/rados/test.sh &
../qa/workunits/rbd/test_librbd.sh &
../qa/workunits/libcephfs/test.sh &
../qa/workunits/rgw/run-s3tests.sh &
ceph-syn --syn makedirs 3 3 3 &

echo 'waiting a bit'

sleep 10
echo 'triggering some recovery'

kill -9 `cat out/osd.0.pid`
sleep 10
ceph osd out 0
sleep 10
init-ceph start osd.0
ceph osd in 0

sleep 5
echo 'triggering mds work'
bin/ceph mds fail 0

echo 'waiting for worker to join (and ignoring errors)'
wait || true

echo 'importing'
../src/test/encoding/import.sh $p $v ../ceph-object-corpus/archive

for d in ../ceph-object-corpus/archive/$v/objects/*
do
    echo prune $d
    ../ceph-object-corpus/bin/prune.sh $d 25
done

echo 'done'
