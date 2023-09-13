#!/bin/sh -x

set -e

num_osd=$2
maxosd=$((num_osd-1))

eval "rm out/*.heap" || echo "no heap dumps to rm"

mkdir -p out/pg_stable
for osd_num in `seq 0 $maxosd`; do
    ./ceph osd tell $osd_num heapdump
    sleep 1
    eval "mv out/*.heap out/pg_stable"
done


for i in `seq 0 $1`; do
    for j in `seq 0 9`; do
	poolnum=$((i*10+j))
	poolname="pool$poolnum"
	./rados -p $poolname bench 1 write -t 1 &
    done
    wait
done

eval "rm out/*.heap" || echo "no heap dumps to rm"
mkdir out/one_write

for osd_num in `seq 0 $maxosd`; do
    ./ceph osd tell $osd_num heapdump
    sleep 1
    eval "mv out/*.heap out/one_write"
done


for i in `seq 0 $1`; do
    for j in `seq 0 9`; do
	poolnum=$((i*10+j))
	poolname="pool$poolnum"
	./rados -p $poolname bench 1 write -t 4 &
    done
    wait
done

eval "rm out/*.heap"
mkdir out/five_writes

for osd_num in `seq 0 $maxosd`; do
    ./ceph osd tell $osd_num heapdump
    sleep 1
    eval "mv out/*.heap out/five_writes"
done

