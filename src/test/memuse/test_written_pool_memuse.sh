#! /bin/sh -x

set -e

for i in `seq 0 $1`; do
    for j in `seq 0 9`; do
	poolnum=$((i*10+j))
	poolname="pool$poolnum"
	./rados -p $poolname bench 1 write -t 1 &
    done
    wait
done