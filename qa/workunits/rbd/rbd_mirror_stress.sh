#!/bin/bash
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

# These vars facilitate running this script in an environment with
# ceph installed from packages, like teuthology. These are not defined
# by default.
#
# RBD_MIRROR_USE_EXISTING_CLUSTER - if set, do not start and stop ceph clusters
# RBD_MIRROR_USE_EXISTING_DAEMON - if set, use an existing instance of rbd-mirror
#                                  running as ceph client $CEPH_ID. If empty,
#                                  this script will start and stop rbd-mirror

#
# Functions
#

daemon_asok_file()
{
    local local_cluster=$1
    local cluster=$2

    if [ -n "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
        echo $(ceph-conf --cluster $local_cluster --name "client.${CEPH_ID}" 'admin socket')
    else
        echo "${TEMPDIR}/rbd-mirror.${local_cluster}_daemon.${cluster}.asok"
    fi
}

daemon_pid_file()
{
    local cluster=$1

    if [ -n "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
        echo $(ceph-conf --cluster $cluster --name "client.${CEPH_ID}" 'pid file')
    else
        echo "${TEMPDIR}/rbd-mirror.${cluster}_daemon.pid"
    fi
}

testlog()
{
    echo $(date '+%F %T') $@ | tee -a "${TEMPDIR}/rbd-mirror.test.log"
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

    if [ -z "${RBD_MIRROR_USE_EXISTING_CLUSTER}" ]; then
        cd ${SRC_DIR}
        ./mstart.sh ${CLUSTER1} -n
        ./mstart.sh ${CLUSTER2} -n

        ln -s $(readlink -f run/${CLUSTER1}/ceph.conf) \
           ${TEMPDIR}/${CLUSTER1}.conf
        ln -s $(readlink -f run/${CLUSTER2}/ceph.conf) \
           ${TEMPDIR}/${CLUSTER2}.conf

        cd ${TEMPDIR}
    fi

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

    if [ -z "${RBD_MIRROR_USE_EXISTING_CLUSTER}" ]; then
        cd ${SRC_DIR}
        ./mstop.sh ${CLUSTER1}
        ./mstop.sh ${CLUSTER2}
    else
        ceph --cluster ${CLUSTER1} osd pool rm ${POOL} ${POOL} --yes-i-really-really-mean-it
        ceph --cluster ${CLUSTER2} osd pool rm ${POOL} ${POOL} --yes-i-really-really-mean-it
    fi
    rm -Rf ${TEMPDIR}
}

start_mirror()
{
    local cluster=$1

    test -n "${RBD_MIRROR_USE_RBD_MIRROR}" && return

    rbd-mirror \
	--cluster ${cluster} \
	--pid-file=$(daemon_pid_file "${cluster}") \
	--log-file=${TEMPDIR}/rbd-mirror.${cluster}_daemon.\$cluster.\$pid.log \
	--admin-socket=${TEMPDIR}/rbd-mirror.${cluster}_daemon.\$cluster.asok \
	--debug-rbd=30 --debug-journaler=30 \
	--debug-rbd_mirror=30 \
	--daemonize=true
}

stop_mirror()
{
    local cluster=$1

    test -n "${RBD_MIRROR_USE_RBD_MIRROR}" && return

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

admin_daemon()
{
    local cluster=$1 ; shift

    local asok_file=$(daemon_asok_file "${cluster}" "${cluster}")
    test -S "${asok_file}"

    ceph --admin-daemon ${asok_file} $@
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

    admin_daemon "${cluster}" ${cmd}
}

test_image_replay_state()
{
    local cluster=$1
    local image=$2
    local test_state=$3
    local current_state=stopped

    admin_daemon "${cluster}" help |
	fgrep "\"rbd mirror status ${POOL}/${image}\"" &&
    admin_daemon "${cluster}" rbd mirror status ${POOL}/${image} |
	grep -i 'state.*Replaying' &&
    current_state=started

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

test_status_in_pool_dir()
{
    local cluster=$1
    local image=$2
    local state_pattern=$3
    local description_pattern=$4

    local status_log=${TEMPDIR}/${cluster}-${image}.mirror_status
    rbd --cluster ${cluster} -p ${POOL} mirror image status ${image} |
	tee ${status_log}
    grep "state: .*${state_pattern}" ${status_log}
    grep "description: .*${description_pattern}" ${status_log}
}

create_image()
{
    local cluster=$1
    local image=$2
    local size=$3

    rbd --cluster ${cluster} -p ${POOL} create --size ${size} \
	--image-feature exclusive-lock --image-feature journaling ${image}
}

write_image()
{
    local cluster=$1
    local image=$2
    local duration=$(($RANDOM % 35 + 15))

    timeout ${duration}s rbd --cluster ${cluster} -p ${POOL} bench-write \
	${image} --io-size 4096 --io-threads 8 --io-total 10G --io-pattern rand || true
}

create_snap()
{
    local cluster=$1
    local image=$2
    local snap_name=$3

    rbd --cluster ${cluster} -p ${POOL} snap create ${image}@${snap_name}
}

wait_for_snap()
{
    local cluster=$1
    local image=$2
    local snap_name=$3
    local s

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16 16 16 32 32; do
	sleep ${s}
        rbd --cluster ${cluster} -p ${POOL} info ${image}@${snap_name} || continue
        return 0
    done
    return 1
}

compare_images()
{
    local image=$1
    local snap_name=$2

    local rmt_export=${TEMPDIR}/${CLUSTER2}-${POOL}-${image}.export
    local loc_export=${TEMPDIR}/${CLUSTER1}-${POOL}-${image}.export

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${CLUSTER2} -p ${POOL} export ${image}@${snap_name} ${rmt_export}
    rbd --cluster ${CLUSTER1} -p ${POOL} export ${image}@${snap_name} ${loc_export}
    cmp ${rmt_export} ${loc_export}
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

testlog "TEST: add image and test replay"
start_mirror ${CLUSTER1}
image=test
create_image ${CLUSTER2} ${image} '512M'
wait_for_image_replay_started ${CLUSTER1} ${image}

for i in `seq 1 10`
do
  write_image ${CLUSTER2} ${image}

  test_status_in_pool_dir ${CLUSTER1} ${image} 'up+replaying' 'master_position'

  snap_name="snap${i}"
  create_snap ${CLUSTER2} ${image} ${snap_name}
  wait_for_snap ${CLUSTER1} ${image} ${snap_name}
  compare_images ${image} ${snap_name}
done

echo OK
