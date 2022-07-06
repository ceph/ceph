#!/bin/bash -ex

unset CEPH_CLI_TEST_DUP_COMMAND

NUM_POOLS=$(ceph osd pool ls | wc -l)

if [ "$NUM_POOLS" -gt 0 ]; then
    echo "test requires no preexisting pools"
    exit 1
fi

ceph osd pool set noautoscale

ceph osd pool create pool_a

echo 'pool_a autoscale_mode:' $(ceph osd pool autoscale-status | grep pool_a | grep -o -m 1 'on\|off')

NUM_POOLS=$[NUM_POOLS+1]

sleep 2

# Count the number of Pools with AUTOSCALE `off`

RESULT1=$(ceph osd pool autoscale-status | grep -oe 'off' | wc -l)

# number of Pools with AUTOSCALE `off` should equal to 2

test "$RESULT1" -eq "$NUM_POOLS"

ceph osd pool unset noautoscale

echo $(ceph osd pool get noautoscale)


ceph osd pool create pool_b

echo 'pool_a autoscale_mode:' $(ceph osd pool autoscale-status | grep pool_a | grep -o -m 1 'on\|off')

echo 'pool_b autoscale_mode:' $(ceph osd pool autoscale-status | grep pool_b | grep -o -m 1 'on\|off')


NUM_POOLS=$[NUM_POOLS+1]

sleep 2

# Count the number of Pools with AUTOSCALE `on`

RESULT2=$(ceph osd pool autoscale-status | grep -oe 'on' | wc -l)

# number of Pools with AUTOSCALE `on` should equal to 3

test "$RESULT2" -eq "$NUM_POOLS"

ceph osd pool set noautoscale

ceph osd pool create pool_c

echo 'pool_a autoscale_mode:' $(ceph osd pool autoscale-status | grep pool_a | grep -o -m 1 'on\|off')

echo 'pool_b autoscale_mode:' $(ceph osd pool autoscale-status | grep pool_b | grep -o -m 1 'on\|off')

echo 'pool_c autoscale_mode:' $(ceph osd pool autoscale-status | grep pool_c | grep -o -m 1 'on\|off')


NUM_POOLS=$[NUM_POOLS+1]

sleep 2

# Count the number of Pools with AUTOSCALE `off`

RESULT3=$(ceph osd pool autoscale-status | grep -oe 'off' | wc -l)

# number of Pools with AUTOSCALE `off` should equal to 4

test "$RESULT3" -eq "$NUM_POOLS"

ceph osd pool rm pool_a pool_a  --yes-i-really-really-mean-it

ceph osd pool rm pool_b pool_b  --yes-i-really-really-mean-it

ceph osd pool rm pool_c pool_c  --yes-i-really-really-mean-it

echo OK
