#!/bin/sh

./stop.sh
CEPH_NUM_OSD=3 ./vstart.sh -d -n -x
./rados -p data bench 15 write -b 4096
./ceph osd out 0
./rados -p data bench 600 write -b 4096
