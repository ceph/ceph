#!/bin/sh

NUM="$1"
GAP="$2"
DUR="$3"

[ -z "$NUM" ] && NUM=30
[ -z "$GAP" ] && GAP=5
[ -z "$DUR" ] && DUR=30

for n in `seq 1 $NUM`; do
    echo "Starting $n of $NUM ..."
    ceph_smalliobenchrbd --pool rbd --duration $DUR --disable-detailed-ops 1 &
    sleep $GAP
done
echo "Waiting..."
wait
echo "OK"
