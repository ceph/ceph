#!/bin/sh -xe

LOC_POOL=rbd_mirror_local$$
RMT_POOL=rbd_mirror_remote$$
IMAGE=rbdimagereplay$$
CLIENT_ID=rbd_mirror_image_replay
RBD_IMAGE_REPLAY_PID_FILE=
TEMPDIR=

#
# Functions
#

setup()
{
    trap cleanup INT TERM EXIT

    TEMPDIR=`mktemp -d`

    ceph osd pool create ${LOC_POOL} 128 128 || :
    ceph osd pool create ${RMT_POOL} 128 128 || :

    rbd -p ${RMT_POOL} create \
	--image-feature exclusive-lock --image-feature journaling \
	--size 128 ${IMAGE}

    rbd -p ${RMT_POOL} info ${IMAGE}
}

cleanup()
{
    set +e

    stop_replay

    if [ -n "${RBD_IMAGE_REPLAY_NOCLEANUP}" ]
    then
	return
    fi

    rm -Rf ${TEMPDIR}
    remove_image ${LOC_POOL} ${IMAGE}
    remove_image ${RMT_POOL} ${IMAGE}
    ceph osd pool delete ${LOC_POOL} ${LOC_POOL} --yes-i-really-really-mean-it
    ceph osd pool delete ${RMT_POOL} ${RMT_POOL} --yes-i-really-really-mean-it
}

remove_image()
{
    local pool=$1
    local image=$2

    if rbd -p ${pool} status ${image} 2>/dev/null; then
	for s in 0.1 0.2 0.4 0.8 1.6 3.2 6.4 12.8; do
	    sleep $s
	    rbd -p ${pool} status ${image} | grep 'Watchers: none' && break
	done
	rbd -p ${pool} remove ${image}
    fi
}

start_replay()
{
    RBD_IMAGE_REPLAY_PID_FILE=${TEMPDIR}/rbd-mirror-image-replay.pid

    ceph_test_rbd_mirror_image_replay \
	--pid-file=${RBD_IMAGE_REPLAY_PID_FILE} \
	--log-file=${TEMPDIR}/rbd-mirror-image-replay.log \
	--admin-socket=${TEMPDIR}/rbd-mirror-image-replay.asok \
	--debug-rbd=30 --debug-journaler=30 \
	--debug-rbd_mirror=30 \
	--daemonize=true \
	${CLIENT_ID} ${LOC_POOL} ${RMT_POOL} ${IMAGE}

    wait_for_replay_started
}

stop_replay()
{
    if [ -z "${RBD_IMAGE_REPLAY_PID_FILE}" ]
    then
	return 0
    fi

    local pid
    pid=$(cat ${RBD_IMAGE_REPLAY_PID_FILE} 2>/dev/null) || :
    if [ -n "${pid}" ]
    then
	kill ${pid}
    fi
    for s in 0.2 0.4 0.8 1.6 2 4 8; do
	sleep $s
	ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}' && break
    done
    ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}'
    rm -f ${TEMPDIR}/rbd-mirror-image-replay.asok
    rm -f ${RBD_IMAGE_REPLAY_PID_FILE}
    RBD_IMAGE_REPLAY_PID_FILE=
}

wait_for_replay_started()
{
    local s

    for s in 0.1 0.2 0.4 0.8 1.6 3.2 6.4; do
	sleep ${s}
	ceph --admin-daemon ${TEMPDIR}/rbd-mirror-image-replay.asok help || :
	test -S ${TEMPDIR}/rbd-mirror-image-replay.asok &&
	    ceph --admin-daemon ${TEMPDIR}/rbd-mirror-image-replay.asok help |
		fgrep "rbd mirror status ${LOC_POOL}/${IMAGE}" && return 0
    done
    return 1
}

flush()
{
    ceph --admin-daemon ${TEMPDIR}/rbd-mirror-image-replay.asok \
	 rbd mirror flush ${LOC_POOL}/${IMAGE}
}

get_position()
{
    local id=$1

    # Parse line like below, looking for the first position
    # [id=, commit_position=[positions=[[object_number=1, tag_tid=3, entry_tid=9], [object_number=0, tag_tid=3, entry_tid=8], [object_number=3, tag_tid=3, entry_tid=7], [object_number=2, tag_tid=3, entry_tid=6]]]]

    local status_log=${TEMPDIR}/${RMT_POOL}-${IMAGE}.status
    rbd -p ${RMT_POOL} journal status --image ${IMAGE} | tee ${status_log} >&2
    sed -nEe 's/^.*\[id='"${id}"',.*positions=\[\[([^]]*)\],.*$/\1/p' \
	${status_log}
}

get_master_position()
{
    get_position ''
}

get_mirror_position()
{
    get_position "${CLIENT_ID}"
}

wait_for_replay_complete()
{
    for s in 0.2 0.4 0.8 1.6 2 2 4 4 8; do
	sleep ${s}
	flush
	master_pos=$(get_master_position)
	mirror_pos=$(get_mirror_position)
	test -n "${master_pos}" -a "${master_pos}" = "${mirror_pos}" && return 0
    done
    return 1
}

compare_images()
{
    local rmt_export=${TEMPDIR}/${RMT_POOL}-${IMAGE}.export
    local loc_export=${TEMPDIR}/${LOC_POOL}-${IMAGE}.export

    rm -f ${rmt_export} ${loc_export}
    rbd -p ${RMT_POOL} export ${IMAGE} ${rmt_export}
    rbd -p ${LOC_POOL} export ${IMAGE} ${loc_export}
    cmp ${rmt_export} ${loc_export}
}

#
# Main
#

setup

start_replay
wait_for_replay_complete
stop_replay
compare_images

count=10
rbd -p ${RMT_POOL} bench-write ${IMAGE} --io-size 4096 --io-threads 1 \
    --io-total $((4096 * count)) --io-pattern seq
start_replay
wait_for_replay_complete
compare_images

rbd -p ${RMT_POOL} bench-write ${IMAGE} --io-size 4096 --io-threads 1 \
    --io-total $((4096 * count)) --io-pattern rand
wait_for_replay_complete
compare_images

stop_replay

rbd -p ${RMT_POOL} bench-write ${IMAGE} --io-size 4096 --io-threads 1 \
    --io-total $((4096 * count)) --io-pattern rand
start_replay
wait_for_replay_complete
compare_images

echo OK
