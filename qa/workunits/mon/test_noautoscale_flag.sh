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

# number of Pools with AUTOSCALE `off` should equal to $NUM_POOLS

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

test "$RESULT2" -eq "$[NUM_POOLS-1]"

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

# Now we test if we retain individual pool state of autoscale mode
# when we set and unset the noautoscale flag.

ceph osd pool unset noautoscale

ceph osd pool set pool_a pg_autoscale_mode on

ceph osd pool set pool_b pg_autoscale_mode warn

ceph osd pool set noautoscale

ceph osd pool unset noautoscale

RESULT4=$(ceph osd pool autoscale-status | grep pool_a | grep -o -m 1 'on\|off\|warn')
RESULT5=$(ceph osd pool autoscale-status | grep pool_b | grep -o -m 1 'on\|off\|warn')
RESULT6=$(ceph osd pool autoscale-status | grep pool_c | grep -o -m 1 'on\|off\|warn')

test "$RESULT4" == 'on'
test "$RESULT5" == 'warn'
test "$RESULT6" == 'off'

ceph osd pool rm pool_a pool_a  --yes-i-really-really-mean-it

ceph osd pool rm pool_b pool_b  --yes-i-really-really-mean-it

ceph osd pool rm pool_c pool_c  --yes-i-really-really-mean-it

echo OK
