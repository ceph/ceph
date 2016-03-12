#!/bin/sh

LOC_CLUSTER=local
RMT_CLUSTER=remote
POOL=mirror
RBD_MIRROR_PID_FILE=
RBD_MIRROR_ASOK=
SRC_DIR=$(readlink -f $(dirname $0)/../../../src)
TEMPDIR=

#
# Functions
#

setup()
{
    local c
    trap cleanup INT TERM EXIT

    TEMPDIR=`mktemp -d`

    cd ${SRC_DIR}
    ./mstart.sh ${LOC_CLUSTER} -n
    ./mstart.sh ${RMT_CLUSTER} -n

    ln -s $(readlink -f run/${LOC_CLUSTER}/ceph.conf) \
       ${TEMPDIR}/${LOC_CLUSTER}.conf
    ln -s $(readlink -f run/${RMT_CLUSTER}/ceph.conf) \
       ${TEMPDIR}/${RMT_CLUSTER}.conf

    cd ${TEMPDIR}

    start_mirror

    ceph --cluster ${LOC_CLUSTER} osd pool create ${POOL} 64 64
    ceph --cluster ${RMT_CLUSTER} osd pool create ${POOL} 64 64

    rbd --cluster ${LOC_CLUSTER} mirror pool enable ${POOL} pool
    rbd --cluster ${RMT_CLUSTER} mirror pool enable ${POOL} pool

    rbd --cluster ${LOC_CLUSTER} mirror pool peer add ${POOL} ${RMT_CLUSTER}
    rbd --cluster ${RMT_CLUSTER} mirror pool peer add ${POOL} ${LOC_CLUSTER}
}

cleanup()
{
    test  -n "${RBD_MIRROR_NOCLEANUP}" && return

    set +e

    stop_mirror

    cd ${SRC_DIR}

    ./mstop.sh ${LOC_CLUSTER}
    ./mstop.sh ${RMT_CLUSTER}

    rm -Rf ${TEMPDIR}
}

start_mirror()
{
    RBD_MIRROR_PID_FILE=${TEMPDIR}/rbd-mirror.pid
    RBD_MIRROR_ASOK=${TEMPDIR}/rbd-mirror.asok

    rbd-mirror \
	--cluster ${LOC_CLUSTER} \
	--pid-file=${RBD_MIRROR_PID_FILE} \
	--log-file=${TEMPDIR}/rbd-mirror.log \
	--admin-socket=${RBD_MIRROR_ASOK} \
	--debug-rbd=30 --debug-journaler=30 \
	--debug-rbd_mirror=30 \
	--daemonize=true
}

stop_mirror()
{
    if [ -z "${RBD_MIRROR_PID_FILE}" ]
    then
	return 0
    fi

    local pid
    pid=$(cat ${RBD_MIRROR_PID_FILE} 2>/dev/null) || :
    if [ -n "${pid}" ]
    then
	kill ${pid}
	for s in 1 2 4 8 16 32; do
	    sleep $s
	    ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}' && break
	done
	ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}'
    fi
    rm -f ${RBD_MIRROR_ASOK}
    rm -f ${RBD_MIRROR_PID_FILE}
    RBD_MIRROR_PID_FILE=
    RBD_MIRROR_ASOK=
}

flush()
{
    local image=$1
    local image_id cmd

    test -n "${RBD_MIRROR_ASOK}"

    image_id=$(remote_image_id ${image})
    test -n "${image_id}"

    cmd=$(ceph --admin-daemon ${RBD_MIRROR_ASOK} help |
		 sed -nEe 's/^.*"(rbd mirror flush.*'${image_id}'])":.*$/\1/p')
    test -n "${cmd}"
    ceph --admin-daemon ${TEMPDIR}/rbd-mirror.asok ${cmd}
}

wait_for_image_replay_started()
{
    local image=$1
    local image_id s

    test -n "${RBD_MIRROR_ASOK}"

    image_id=$(remote_image_id ${image})
    test -n "${image_id}"

    # TODO: add a way to force rbd-mirror to update replayers

    for s in 1 2 4 8 8 8 8 8 8 8 8; do
	sleep ${s}
	ceph --admin-daemon ${RBD_MIRROR_ASOK} help | grep "${image_id}" &&
	    return 0
    done
    return 1
}

get_position()
{
    local image=$1
    local id_regexp=$2

    # Parse line like below, looking for the first entry_tid
    # [id=, commit_position=[positions=[[object_number=1, tag_tid=3, entry_tid=9], [object_number=0, tag_tid=3, entry_tid=8], [object_number=3, tag_tid=3, entry_tid=7], [object_number=2, tag_tid=3, entry_tid=6]]]]

    local status_log=${TEMPDIR}/${RMT_CLUSTER}-${POOL}-${image}.status
    rbd --cluster ${RMT_CLUSTER} -p ${POOL} journal status --image ${image} |
	tee ${status_log} >&2
    sed -Ee 's/[][,]/ /g' ${status_log} |
	awk '$1 ~ /id='"${id_regexp}"'$/ {
               for (i = 1; i < NF; i++) {
                 if ($i ~ /entry_tid=/) {
                   print $i;
                   exit
                 }
               }
             }'
}

get_master_position()
{
    local image=$1

    get_position ${image} ''
}

get_mirror_position()
{
    local image=$1

    get_position ${image} '..*'
}

wait_for_replay_complete()
{
    local image=$1
    local s master_pos mirror_pos

    for s in 0.2 0.4 0.8 1.6 2 2 4 4 8; do
	sleep ${s}
	flush ${image}
	master_pos=$(get_master_position ${image})
	mirror_pos=$(get_mirror_position ${image})
	test -n "${master_pos}" -a "${master_pos}" = "${mirror_pos}" && return 0
    done
    return 1
}

create_image()
{
    local cluster=$1
    local image=$2

    rbd --cluster ${cluster} -p ${POOL} create --size 128 \
	--image-feature exclusive-lock --image-feature journaling ${image}
}

create_remote_image()
{
    local image=$1

    create_image ${RMT_CLUSTER} ${image}
}

create_local_image()
{
    local image=$1

    create_image ${LOC_CLUSTER} ${image}
}

image_id()
{
    local cluster=$1
    local image=$2

    rbd --cluster ${cluster} -p ${POOL} info --image test |
	sed -ne 's/^.*block_name_prefix: rbd_data\.//p'
}

remote_image_id()
{
    local image=$1

    image_id ${RMT_CLUSTER} ${image}
}

local_image_id()
{
    local image=$1

    image_id ${LOC_CLUSTER} ${image}
}

write_image()
{
    local image=$1
    local count=$2

    rbd --cluster ${RMT_CLUSTER} -p ${POOL} bench-write ${image} \
	--io-size 4096 --io-threads 1 --io-total $((4096 * count)) \
	--io-pattern rand
}

compare_images()
{
    local image=$1

    local rmt_export=${TEMPDIR}/${RMT_CLUSTER}-${POOL}-${image}.export
    local loc_export=${TEMPDIR}/${LOC_CLUSTER}-${POOL}-${image}.export

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${RMT_CLUSTER} -p ${POOL} export ${image} ${rmt_export}
    rbd --cluster ${LOC_CLUSTER} -p ${POOL} export ${image} ${loc_export}
    cmp ${rmt_export} ${loc_export}
}

#
# Main
#

if [ "$1" = clean ]; then
    TEMPDIR=$2

    test -n "${TEMPDIR}"

    RBD_MIRROR_PID_FILE=${TEMPDIR}/rbd-mirror.pid
    RBD_MIRROR_ASOK=${TEMPDIR}/rbd-mirror.asok
    RBD_MIRROR_NOCLEANUP=

    cleanup
    exit
fi

set -xe

setup

# add image and test replay
image=test
create_remote_image ${image}
wait_for_image_replay_started ${image}
write_image ${image} 100
wait_for_replay_complete ${image}
compare_images ${image}

# stop mirror, add image, start mirror and test replay
stop_mirror
image1=test1
create_remote_image ${image1}
write_image ${image1} 100
start_mirror
wait_for_image_replay_started ${image1}
wait_for_replay_complete ${image}
compare_images ${image1}

# test the first image is replaying after restart
write_image ${image} 100
wait_for_image_replay_started ${image}
compare_images ${image}

echo OK
