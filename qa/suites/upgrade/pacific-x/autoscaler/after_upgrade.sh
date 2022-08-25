#!/bin/bash -ex

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

ceph osd pool autoscale-status

TEST1 = $(ceph osd pool autoscale-status | grep 'test1' | grep -o -m 1 'on\|off\|warn' || true)

if [[ -z $TEST1]]
then
  echo "Error: autoscale value is empty!"
  exit 1
fi

TEST2 = $(ceph osd pool autoscale-status | grep 'test2' | grep -o -m 1 'on\|off\|warn' || true)

if [[ -z $TEST2]]
then
  echo "Error: autoscale value is empty!"
  exit 1
fi

TEST3 = $(ceph osd pool autoscale-status | grep 'test3' | grep -o -m 1 'on\|off\|warn' || true)

if [[ -z $TEST3]]
then
  echo "Error: autoscale value is empty!"
  exit 1
fi

eval_actual_expected_val $TEST1 'on'
eval_actual_expected_val $TEST2 'warn'
eval_actual_expected_val $TEST3 'off'

echo OK