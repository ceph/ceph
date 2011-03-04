#!/bin/sh

CEPH_NUM_OSD=3 ./vstart.sh -d -n -x -o 'osd recovery max active = 1'

./ceph osd pool set data size 3

sleep 20

./init-ceph stop osd.1
./ceph osd down 1   # faster

for f in `seq 1 100`
do
    ./rados -p data put test_$f /etc/passwd
done

# zap some objects on both replicas
#rm dev/osd[02]/current/*/test_40*

# some on only one
rm dev/osd0/current/*/test_*
#rm dev/osd2/current/*/test_6*

# ...and see how we fare!
./init-ceph start osd.1


