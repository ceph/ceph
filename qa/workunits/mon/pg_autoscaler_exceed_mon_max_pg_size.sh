#!/bin/bash -ex

NUM_OSDS=$(ceph osd ls | wc -l)
if [ $NUM_OSDS -ne 6 ]; then
    echo "test requires 6 OSDs"
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

function power2_floor() { echo "x=l($1)/l(2); scale=0; 2^(x/1)" | bc -l;}

function power2_ceil() { echo "x=l($1)/l(2); scale=0; 2^((x+0.999)/1)" | bc -l; }

function eval_actual_expected_val() {
    local actual_value=$1
    local expected_value=$2
    if [[ $actual_value = $expected_value ]]
    then
     echo "Success: " $actual_value "=" $expected_value
    else
      echo "Error: " $actual_value "!=" $expected_value
      exit 1
    fi
}

# enable
ceph config set mgr mgr/pg_autoscaler/sleep_interval 60
ceph mgr module enable pg_autoscaler
ceph osd pool set threshold 1.0

# Don't increase pool size if exceeding mon_max_pg_per_osd ceiling
MON_TARGET_PG_PER_OSD=80
ceph config set global mon_target_pg_per_osd $MON_TARGET_PG_PER_OSD
ceph osd pool create bulk0 --bulk --size=3 --autoscale_mode=on
ceph osd pool create bulk1 --bulk --size=3 --autoscale_mode=on
ceph osd pool set threshold 1.0

sleep 60
ceph osd pool create data1 --size=3 --autoscale_mode=on
if ! ceph osd pool set data1 size 4 2>&1 | grep -q "Error ERANGE"; then
    echo "failed"
    exit 1
fi

ceph osd pool rm bulk0 bulk0 --yes-i-really-really-mean-it
ceph osd pool rm bulk1 bulk1 --yes-i-really-really-mean-it
ceph osd pool rm data1 data1 --yes-i-really-really-mean-it


echo OK

