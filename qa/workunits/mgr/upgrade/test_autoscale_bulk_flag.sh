#!/bin/bash -ex

# This test evaluates if the pool created before upgrade has False bulk value.
# Moreover, it evaluates the bulk value of pools created after the upgrade.

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


ceph osd pool create test_pool1 --bulk

ceph osd pool create test_pool2

DEFAULT_POOL=$(ceph osd pool autoscale-status | grep -o -m 1 'device_health_metrics\|.mgr' || true)

if [[ -z $DEFAULT_POOL ]]
then
  echo "Error:" $DEFAULT_POOL "pool doesn't exist"
  exit 1
fi

BULK_FLAG1=$(ceph osd pool autoscale-status | grep $DEFAULT_POOL | grep -o -m 1 'True\|False' || true)

if [[ -z $BULK_FLAG1 ]]
then
  echo "Error:" $DEFAULT_POOL "BULK value is empty!"
  exit 1
fi

sleep 5

BULK_FLAG2=$(ceph osd pool autoscale-status | grep 'test_pool1' | grep -o -m 1 'True\|False' || true)

if [[ -z $BULK_FLAG2 ]]
then
  echo "Error: test_pool1 BULK value is empty!"
  exit 1
fi

BULK_FLAG3=$(ceph osd pool autoscale-status | grep 'test_pool2' | grep -o -m 1 'True\|False' || true)

if [[ -z $BULK_FLAG3 ]]
then
  echo "Error: test_pool2 BULK value is empty!"
  exit 1
fi

eval_actual_expected_val $BULK_FLAG_1 'False'
eval_actual_expected_val $BULK_FLAG_2 'True'
eval_actual_expected_val $BULK_FLAG_3 'False'

echo OK
