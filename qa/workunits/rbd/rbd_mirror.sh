#!/bin/sh
#
# rbd_mirror.sh - test rbd-mirror daemon
#
# The scripts starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#
# There are several env variables useful when troubleshooting a test failure:
#
#  RBD_MIRROR_NOCLEANUP - if not empty, don't run the cleanup (stop processes,
#                         destroy the clusters and remove the temp directory)
#                         on exit, so it is possible to check the test state
#                         after failure.
#  RBD_MIRROR_TEMDIR    - use this path when creating the temporary directory
#                         (should not exist) instead of running mktemp(1).
#
# The cleanup can be done as a separate step, running the script with
# `cleanup ${RBD_MIRROR_TEMDIR}' arguments.
#
# Note, as other workunits tests, rbd_mirror.sh expects to find ceph binaries
# in PATH.
#
# Thus a typical troubleshooting session:
#
# From Ceph src dir (CEPH_SRC_PATH), start the test in NOCLEANUP mode and with
# TEMPDIR pointing to a known location:
#
#   cd $CEPH_SRC_PATH
#   PATH=$CEPH_SRC_PATH:$PATH
#   RBD_MIRROR_NOCLEANUP=1 RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror \
#     ../qa/workunits/rbd/rbd_mirror.sh
#
# After the test failure cd to TEMPDIR and check the current state:
#
#   cd /tmp/tmp.rbd_mirror
#   ls
#   less rbd-mirror.cluster1_daemon.$pid.log
#   ceph --cluster cluster1 -s
#   ceph --cluster cluster1 -s
#   rbd --cluster cluster2 -p mirror ls
#   rbd --cluster cluster2 -p mirror journal status --image test
#   ceph --admin-daemon rbd-mirror.cluster1_daemon.cluster1.$pid.asok help
#   ...
#
# Also you can execute commands (functions) from the script:
#
#   cd $CEPH_SRC_PATH
#   export RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror
#   ../qa/workunits/rbd/rbd_mirror.sh status
#   ../qa/workunits/rbd/rbd_mirror.sh stop_mirror cluster1
#   ../qa/workunits/rbd/rbd_mirror.sh start_mirror cluster2
#   ../qa/workunits/rbd/rbd_mirror.sh flush cluster2
#   ...
#
# Eventually, run the cleanup:
#
#   cd $CEPH_SRC_PATH
#   RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror \
#     ../qa/workunits/rbd/rbd_mirror.sh cleanup
#

CLUSTER1=cluster1
CLUSTER2=cluster2
POOL=mirror
SRC_DIR=$(readlink -f $(dirname $0)/../../../src)
TEMPDIR=

#
# Functions
#

daemon_asok_file()
{
    local local_cluster=$1
    local cluster=$2

    echo "${TEMPDIR}/rbd-mirror.${local_cluster}_daemon.${cluster}.asok"
}

daemon_pid_file()
{
    local cluster=$1

    echo "${TEMPDIR}/rbd-mirror.${cluster}_daemon.pid"
}

setup()
{
    local c
    trap cleanup INT TERM EXIT

    if [ -n "${RBD_MIRROR_TEMDIR}" ]; then
	mkdir "${RBD_MIRROR_TEMDIR}"
	TEMPDIR="${RBD_MIRROR_TEMDIR}"
    else
	TEMPDIR=`mktemp -d`
    fi

    cd ${SRC_DIR}
    ./mstart.sh ${CLUSTER1} -n
    ./mstart.sh ${CLUSTER2} -n

    ln -s $(readlink -f run/${CLUSTER1}/ceph.conf) \
       ${TEMPDIR}/${CLUSTER1}.conf
    ln -s $(readlink -f run/${CLUSTER2}/ceph.conf) \
       ${TEMPDIR}/${CLUSTER2}.conf

    cd ${TEMPDIR}

    ceph --cluster ${CLUSTER1} osd pool create ${POOL} 64 64
    ceph --cluster ${CLUSTER2} osd pool create ${POOL} 64 64

    rbd --cluster ${CLUSTER1} mirror pool enable ${POOL} pool
    rbd --cluster ${CLUSTER2} mirror pool enable ${POOL} pool

    rbd --cluster ${CLUSTER1} mirror pool peer add ${POOL} ${CLUSTER2}
    rbd --cluster ${CLUSTER2} mirror pool peer add ${POOL} ${CLUSTER1}
}

cleanup()
{
    test  -n "${RBD_MIRROR_NOCLEANUP}" && return

    set +e

    stop_mirror "${CLUSTER1}"
    stop_mirror "${CLUSTER2}"

    cd ${SRC_DIR}

    ./mstop.sh ${CLUSTER1}
    ./mstop.sh ${CLUSTER2}

    rm -Rf ${TEMPDIR}
}

start_mirror()
{
    local cluster=$1

    rbd-mirror \
	--cluster ${cluster} \
	--pid-file=$(daemon_pid_file "${cluster}") \
	--log-file=${TEMPDIR}/rbd-mirror.\$cluster_daemon.\$pid.log \
	--admin-socket=${TEMPDIR}/rbd-mirror.${cluster}_daemon.\$cluster.asok \
	--debug-rbd=30 --debug-journaler=30 \
	--debug-rbd_mirror=30 \
	--daemonize=true
}

stop_mirror()
{
    local cluster=$1

    local pid
    pid=$(cat $(daemon_pid_file "${cluster}") 2>/dev/null) || :
    if [ -n "${pid}" ]
    then
	kill ${pid}
	for s in 1 2 4 8 16 32; do
	    sleep $s
	    ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}' && break
	done
	ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}'
    fi
    rm -f $(daemon_asok_file "${cluster}" "${CLUSTER1}")
    rm -f $(daemon_asok_file "${cluster}" "${CLUSTER2}")
    rm -f $(daemon_pid_file "${cluster}")
}

status()
{
    local cluster daemon image

    for cluster in ${CLUSTER1} ${CLUSTER2}
    do
	echo "${cluster} status"
	ceph --cluster ${cluster} -s
	echo

	echo "${cluster} ${POOL} images"
	rbd --cluster ${cluster} -p ${POOL} ls
	echo

	for image in `rbd --cluster ${cluster} -p ${POOL} ls 2>/dev/null`
	do
	    echo "image ${image} journal status"
	    rbd --cluster ${cluster} -p ${POOL} journal status --image ${image}
	    echo
	done
    done

    local ret

    for cluster in "${CLUSTER1}" "${CLUSTER2}"
    do
	local pid_file=$(daemon_pid_file ${cluster} )
	if [ ! -e ${pid_file} ]
	then
	    echo "${cluster} rbd-mirror not running or unknown" \
		 "(${pid_file} not exist)"
	    continue
	fi

	local pid
	pid=$(cat ${pid_file} 2>/dev/null) || :
	if [ -z "${pid}" ]
	then
	    echo "${cluster} rbd-mirror not running or unknown" \
		 "(can't find pid using ${pid_file})"
	    ret=1
	    continue
	fi

	echo "${daemon} rbd-mirror process in ps output:"
	if ps auxww |
		awk -v pid=${pid} 'NR == 1 {print} $2 == pid {print; exit 1}'
	then
	    echo
	    echo "${cluster} rbd-mirror not running" \
		 "(can't find pid $pid in ps output)"
	    ret=1
	    continue
	fi
	echo

	local asok_file=$(daemon_asok_file ${cluster} ${cluster})
	if [ ! -S "${asok_file}" ]
	then
	    echo "${cluster} rbd-mirror asok is unknown (${asok_file} not exits)"
	    ret=1
	    continue
	fi

	echo "${cluster} rbd-mirror status"
	ceph --admin-daemon ${asok_file} rbd mirror status
	echo
    done

    return ${ret}
}

flush()
{
    local cluster=$1
    local image=$2
    local cmd="rbd mirror flush"

    if [ -n "${image}" ]
    then
       cmd="${cmd} ${POOL}/${image}"
    fi

    local asok_file=$(daemon_asok_file "${cluster}" "${cluster}")
    test -S "${asok_file}"

    ceph --admin-daemon ${asok_file} ${cmd}
}

test_image_replay_state()
{
    local cluster=$1
    local image=$2
    local test_state=$3
    local current_state=stopped

    local asok_file=$(daemon_asok_file "${cluster}" "${cluster}")
    test -S "${asok_file}"

    ceph --admin-daemon ${asok_file} help |
	fgrep "\"rbd mirror status ${POOL}/${image}\"" && current_state=started
    test "${test_state}" = "${current_state}"
}

wait_for_image_replay_state()
{
    local cluster=$1
    local image=$2
    local state=$3
    local s

    # TODO: add a way to force rbd-mirror to update replayers
    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
	sleep ${s}
	test_image_replay_state "${cluster}" "${image}" "${state}" && return 0
    done
    return 1
}

wait_for_image_replay_started()
{
    local cluster=$1
    local image=$2

    wait_for_image_replay_state "${cluster}" "${image}" started
}

wait_for_image_replay_stopped()
{
    local cluster=$1
    local image=$2

    wait_for_image_replay_state "${cluster}" "${image}" stopped
}

get_position()
{
    local cluster=$1
    local image=$2
    local id_regexp=$3

    # Parse line like below, looking for the first position
    # [id=, commit_position=[positions=[[object_number=1, tag_tid=3, entry_tid=9], [object_number=0, tag_tid=3, entry_tid=8], [object_number=3, tag_tid=3, entry_tid=7], [object_number=2, tag_tid=3, entry_tid=6]]]]

    local status_log=${TEMPDIR}/${CLUSTER2}-${POOL}-${image}.status
    rbd --cluster ${cluster} -p ${POOL} journal status --image ${image} |
	tee ${status_log} >&2
    sed -nEe 's/^.*\[id='"${id_regexp}"',.*positions=\[\[([^]]*)\],.*$/\1/p' \
	${status_log}
}

get_master_position()
{
    local cluster=$1
    local image=$2

    get_position "${cluster}" "${image}" ''
}

get_mirror_position()
{
    local cluster=$1
    local image=$2

    get_position "${cluster}" "${image}" '..*'
}

wait_for_replay_complete()
{
    local local_cluster=$1
    local cluster=$2
    local image=$3
    local s master_pos mirror_pos

    for s in 0.2 0.4 0.8 1.6 2 2 4 4 8 8 16 16; do
	sleep ${s}
	flush "${local_cluster}" "${image}"
	master_pos=$(get_master_position "${cluster}" "${image}")
	mirror_pos=$(get_mirror_position "${cluster}" "${image}")
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

write_image()
{
    local cluster=$1
    local image=$2
    local count=$3

    rbd --cluster ${cluster} -p ${POOL} bench-write ${image} \
	--io-size 4096 --io-threads 1 --io-total $((4096 * count)) \
	--io-pattern rand
}

compare_images()
{
    local image=$1

    local rmt_export=${TEMPDIR}/${CLUSTER2}-${POOL}-${image}.export
    local loc_export=${TEMPDIR}/${CLUSTER1}-${POOL}-${image}.export

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${CLUSTER2} -p ${POOL} export ${image} ${rmt_export}
    rbd --cluster ${CLUSTER1} -p ${POOL} export ${image} ${loc_export}
    cmp ${rmt_export} ${loc_export}
}

demote_image()
{
    local cluster=$1
    local image=$2

    rbd --cluster=${cluster} mirror image demote ${POOL}/${image}
}

promote_image()
{
    local cluster=$1
    local image=$2

    rbd --cluster=${cluster} mirror image promote ${POOL}/${image}
}

#
# Main
#

if [ "$#" -gt 0 ]
then
    if [ -z "${RBD_MIRROR_TEMDIR}" ]
    then
       echo "RBD_MIRROR_TEMDIR is not set" >&2
       exit 1
    fi

    TEMPDIR="${RBD_MIRROR_TEMDIR}"
    cd ${TEMPDIR}
    $@
    exit $?
fi

set -xe

setup

echo "TEST: add image and test replay"
start_mirror ${CLUSTER1}
image=test
create_image ${CLUSTER2} ${image}
wait_for_image_replay_started ${CLUSTER1} ${image}
write_image ${CLUSTER2} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${image}
compare_images ${image}

echo "TEST: stop mirror, add image, start mirror and test replay"
stop_mirror ${CLUSTER1}
image1=test1
create_image ${CLUSTER2} ${image1}
write_image ${CLUSTER2} ${image1} 100
start_mirror ${CLUSTER1}
wait_for_image_replay_started ${CLUSTER1} ${image1}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${image1}
compare_images ${image1}

echo "TEST: test the first image is replaying after restart"
write_image ${CLUSTER2} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${image}
compare_images ${image}

echo "TEST: failover and failback"
start_mirror ${CLUSTER2}

# failover
demote_image ${CLUSTER2} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${image}
promote_image ${CLUSTER1} ${image}
wait_for_image_replay_started ${CLUSTER2} ${image}
write_image ${CLUSTER1} ${image} 100
wait_for_replay_complete ${CLUSTER2} ${CLUSTER1} ${image}
compare_images ${image}

# failback
demote_image ${CLUSTER1} ${image}
wait_for_image_replay_stopped ${CLUSTER2} ${image}
promote_image ${CLUSTER2} ${image}
wait_for_image_replay_started ${CLUSTER1} ${image}
write_image ${CLUSTER2} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${image}
compare_images ${image}

echo OK
