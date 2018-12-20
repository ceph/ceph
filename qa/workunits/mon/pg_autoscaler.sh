#!/bin/bash -ex

NUM_OSDS=$(ceph osd ls | wc -l)
if [ $NUM_OSDS -lt 6 ]; then
    echo "test requires at least 6 OSDs"
    exit 1
fi

NUM_POOLS=$(ceph osd pool ls | wc -l)
if [ $NUM_POOLS -gt 0 ]; then
    echo "test requires no preexisting pools"
    exit 1
fi

function wait_for() {
    local sec=$1
    local cmd=$2

    while true ; do
	if bash -c "$cmd" ; then
	    break
	fi
	sec=$(( $sec - 1 ))
	if [ $sec -eq 0 ]; then
	    echo failed
	    return 1
	fi
	sleep 1
    done
    return 0
}

# enable
ceph config set mgr mgr/pg_autoscaler/sleep_interval 5
ceph mgr module enable pg_autoscaler

# pg_num_min
ceph osd pool create a 16 --pg-num-min 4
ceph osd pool create b 16 --pg-num-min 2
ceph osd pool set a pg_autoscale_mode on
ceph osd pool set b pg_autoscale_mode on

wait_for 120 "ceph osd pool get a pg_num | grep 4"
wait_for 120 "ceph osd pool get b pg_num | grep 2"

# target ratio
ceph osd pool set a target_size_ratio .5
ceph osd pool set b target_size_ratio .1
sleep 30
APGS=$(ceph osd dump -f json-pretty | jq '.pools[0].pg_num')
BPGS=$(ceph osd dump -f json-pretty | jq '.pools[1].pg_num')
test $APGS -gt 100
test $BPGS -gt 10

# small ratio change does not change pg_num
ceph osd pool set a target_size_ratio .7
ceph osd pool set b target_size_ratio .2
sleep 10
ceph osd pool get a pg_num | grep $APGS
ceph osd pool get b pg_num | grep $BPGS

# too much ratio
ceph osd pool set a target_size_ratio .9
ceph osd pool set b target_size_ratio .9
wait_for 60 "ceph health detail | grep POOL_TARGET_SIZE_RATIO_OVERCOMMITTED"
wait_for 60 "ceph health detail | grep 1.8"
ceph osd pool set a target_size_ratio 0
ceph osd pool set b target_size_ratio 0

# target_size
ceph osd pool set a target_size_bytes 1000000000000000
ceph osd pool set b target_size_bytes 1000000000000000
wait_for 60 "ceph health detail | grep POOL_TARGET_SIZE_BYTES_OVERCOMMITTED"
ceph osd pool set a target_size_bytes 0
ceph osd pool set b target_size_ratio 0

ceph osd pool rm a a --yes-i-really-really-mean-it
ceph osd pool rm b b --yes-i-really-really-mean-it

echo OK
