#!/bin/bash -ex

DHM_POOL=$(ceph osd pool autoscale-status | grep -o -m 1 'device_health_metrics' || true)

if [[ -z $DHM_POOL ]]
then
  echo "Error: device_health_metrics pool doesn't exist "
  exit 1
fi

PROFILE1=$(ceph osd pool autoscale-status | grep 'device_health_metrics' | grep -o -m 1 'scale-up\|scale-down' || true)

if [[ -z $PROFILE1 ]]
then
  echo "Error: device_health_metrics PROFILE is empty!"
  exit 1
fi

if [[ $PROFILE1 = "scale-up" ]]
then
  echo "Success: device_health_metrics PROFILE is scale-up"
else
  echo "Error: device_health_metrics PROFILE is scale-down"
  exit 1
fi


ceph osd pool create test_pool

sleep 5

PROFILE2=$(ceph osd pool autoscale-status | grep 'test_pool' | grep -o -m 1 'scale-up\|scale-down' || true)

if [[ -z $PROFILE2 ]]
then
  echo "Error: test_pool PROFILE is empty!"
  exit 1
fi

if [[ $PROFILE2 = "scale-up" ]]
then
  echo "Success, test_pool PROFILE is scale-up"
else
  echo "Error: test_pool PROFILE is scale-down"
  exit 1
fi

echo OK
