#!/usr/bin/env bash
#
# rbd_mirror_ha.sh - test rbd-mirror daemons in HA mode
#

set -ex

RBD_MIRROR_INSTANCES=${RBD_MIRROR_INSTANCES:-7}

. $(dirname $0)/rbd_mirror_helpers.sh

setup

is_leader()
{
    local instance=$1
    local pool=$2

    test -n "${pool}" || pool=${POOL}

    admin_daemon "${CLUSTER1}:${instance}" \
		 rbd mirror status ${pool} ${CLUSTER2}${PEER_CLUSTER_SUFFIX} |
	grep '"leader": true'
}

wait_for_leader()
{
    local s instance

    for s in 1 1 2 4 4 4 4 4 8 8 8 8 16 16 32 64; do
	sleep $s
	for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
	    is_leader ${instance} || continue
	    LEADER=${instance}
	    return 0
	done
    done

    LEADER=
    return 1
}

release_leader()
{
    local pool=$1
    local cmd="rbd mirror leader release"

    test -n "${pool}" && cmd="${cmd} ${pool} ${CLUSTER2}"

    admin_daemon "${CLUSTER1}:${LEADER}" ${cmd}
}

wait_for_leader_released()
{
    local i

    test -n "${LEADER}"
    for i in `seq 10`; do
	is_leader ${LEADER} || return 0
	sleep 1
    done

    return 1
}

test_replay()
{
    local image

    for image; do
	wait_for_image_replay_started ${CLUSTER1}:${LEADER} ${POOL} ${image}
	write_image ${CLUSTER2} ${POOL} ${image} 100
	wait_for_replay_complete ${CLUSTER1}:${LEADER} ${CLUSTER2} ${POOL} \
				 ${image}
	wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' \
                                    'primary_position' \
                                    "${MIRROR_USER_ID_PREFIX}${LEADER} on $(hostname -s)"
	if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
	    wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} \
					'down+unknown'
	fi
	compare_images ${POOL} ${image}
    done
}

testlog "TEST: start first daemon instance and test replay"
start_mirror ${CLUSTER1}:0
image1=test1
create_image ${CLUSTER2} ${POOL} ${image1}
LEADER=0
test_replay ${image1}

testlog "TEST: release leader and wait it is reacquired"
is_leader 0 ${POOL}
is_leader 0 ${PARENT_POOL}
release_leader ${POOL}
wait_for_leader_released
is_leader 0 ${PARENT_POOL}
wait_for_leader
release_leader
wait_for_leader_released
expect_failure "" is_leader 0 ${PARENT_POOL}
wait_for_leader

testlog "TEST: start second daemon instance and test replay"
start_mirror ${CLUSTER1}:1
image2=test2
create_image ${CLUSTER2} ${POOL} ${image2}
test_replay ${image1} ${image2}

testlog "TEST: release leader and test it is acquired by secondary"
is_leader 0 ${POOL}
is_leader 0 ${PARENT_POOL}
release_leader ${POOL}
wait_for_leader_released
wait_for_leader
test_replay ${image1} ${image2}
release_leader
wait_for_leader_released
wait_for_leader
test "${LEADER}" = 0

testlog "TEST: stop first daemon instance and test replay"
stop_mirror ${CLUSTER1}:0
image3=test3
create_image ${CLUSTER2} ${POOL} ${image3}
LEADER=1
test_replay ${image1} ${image2} ${image3}

testlog "TEST: start first daemon instance and test replay"
start_mirror ${CLUSTER1}:0
image4=test4
create_image ${CLUSTER2} ${POOL} ${image4}
test_replay ${image3} ${image4}

testlog "TEST: crash leader and test replay"
stop_mirror ${CLUSTER1}:1 -KILL
image5=test5
create_image ${CLUSTER2} ${POOL} ${image5}
LEADER=0
test_replay ${image1} ${image4} ${image5}

testlog "TEST: start crashed leader and test replay"
start_mirror ${CLUSTER1}:1
image6=test6
create_image ${CLUSTER2} ${POOL} ${image6}
test_replay ${image1} ${image6}

testlog "TEST: start yet another daemon instance and test replay"
start_mirror ${CLUSTER1}:2
image7=test7
create_image ${CLUSTER2} ${POOL} ${image7}
test_replay ${image1} ${image7}

testlog "TEST: release leader and test it is acquired by secondary"
is_leader 0
release_leader
wait_for_leader_released
wait_for_leader
test_replay ${image1} ${image2}

testlog "TEST: stop leader and test replay"
stop_mirror ${CLUSTER1}:${LEADER}
image8=test8
create_image ${CLUSTER2} ${POOL} ${image8}
prev_leader=${LEADER}
wait_for_leader
test_replay ${image1} ${image8}

testlog "TEST: start previous leader and test replay"
start_mirror ${CLUSTER1}:${prev_leader}
image9=test9
create_image ${CLUSTER2} ${POOL} ${image9}
test_replay ${image1} ${image9}

testlog "TEST: crash leader and test replay"
stop_mirror ${CLUSTER1}:${LEADER} -KILL
image10=test10
create_image ${CLUSTER2} ${POOL} ${image10}
prev_leader=${LEADER}
wait_for_leader
test_replay ${image1} ${image10}

testlog "TEST: start previous leader and test replay"
start_mirror ${CLUSTER1}:${prev_leader}
image11=test11
create_image ${CLUSTER2} ${POOL} ${image11}
test_replay ${image1} ${image11}

testlog "TEST: start some more daemon instances and test replay"
start_mirror ${CLUSTER1}:3
start_mirror ${CLUSTER1}:4
start_mirror ${CLUSTER1}:5
start_mirror ${CLUSTER1}:6
image13=test13
create_image ${CLUSTER2} ${POOL} ${image13}
test_replay ${leader} ${image1} ${image13}

testlog "TEST: release leader and test it is acquired by secondary"
release_leader
wait_for_leader_released
wait_for_leader
test_replay ${image1} ${image2}

testlog "TEST: in loop: stop leader and test replay"
for i in 0 1 2 3 4 5; do
    stop_mirror ${CLUSTER1}:${LEADER}
    wait_for_leader
    test_replay ${image1}
done

stop_mirror ${CLUSTER1}:${LEADER}
