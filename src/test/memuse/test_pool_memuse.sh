#! /bin/sh -x

#
# Create a bunch of pools in parallel
# This test isn't very smart -- run it from your src dir.
#

set -e

CEPH_NUM_MON=1 CEPH_NUM_MDS=1 CEPH_NUM_OSD=$2 ./vstart.sh -n -d --valgrind_osd 'massif'

for i in `seq 0 $1`; do
    for j in `seq 0 9`; do
	poolnum=$((i*10+j))
	poolname="pool$poolnum"
	./ceph osd pool create $poolname 8 &
    done
    wait
done
