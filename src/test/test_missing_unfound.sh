#!/bin/sh

CEPH_NUM_OSD=3 ./vstart.sh -d -n -x -o 'osd recovery max active = 1'

TEST_POOL=rbd

./ceph -c ./ceph.conf osd pool set $TEST_POOL size 3

sleep 20

./init-ceph stop osd.1
./ceph osd down 1   # faster

for f in `seq 1 100`
do
    ./rados -c ./ceph.conf -p $TEST_POOL put test_$f /etc/passwd
done

# zap some objects on both replicas
#rm dev/osd[02]/current/*/test_40*

# some on only one
rm dev/osd0/current/*/test_*
#rm dev/osd2/current/*/test_6*

# ...and see how we fare!
./init-ceph start osd.1


