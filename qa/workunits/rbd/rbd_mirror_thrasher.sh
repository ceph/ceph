#!/bin/sh
#
# rbd_mirror_thrasher.sh - test rbd-mirror daemons in HA mode
#

. $(dirname $0)/rbd_mirror_helpers.sh

is_test_started()
{
    rados --cluster ${CLUSTER1} lspools | grep "^${POOL}\$" && return 0
    rados --cluster ${CLUSTER2} lspools | grep "^${POOL}\$" && return 0
    return 1
}

wait_for_test_started()
{
    while ! is_test_started; do
	sleep 5
    done
}

release_leader()
{
    local cluster pool peer

    for cluster in ${CLUSTER1} ${CLUSTER2}; do
	test ${cluster} = ${CLUSTER1} && peer=${CLUSTER2} || peer=${CLUSTER1}
	for pool in ${POOL} ${PARENT_POOL}; do
	    flush ${cluster} ${pool} || :
	    is_pool_leader ${cluster} ${pool} ${peer} || continue
	    release_pool_leader ${cluster} ${pool} ${peer}
	done
    done
}

setup_tempdir

wait_for_test_started

while is_test_started; do
    release_leader
    duration=$(awk 'BEGIN {srand(); print int(60 * rand()) + 30}')
    sleep ${duration}
done
