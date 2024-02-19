#!/bin/sh

TEST_POOL=rbd

./stop.sh
CEPH_NUM_OSD=3 ./vstart.sh -d -n -x -o 'osd min pg log entries = 5'
./rados -p $TEST_POOL bench 15 write -b 4096
./ceph osd out 0
 ./init-ceph stop osd.0
 ./ceph osd down 0
./rados -p $TEST_POOL bench 600 write -b 4096
