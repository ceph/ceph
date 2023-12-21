#!/bin/sh
#
# rbd_mirror_helpers.sh - shared rbd-mirror daemon helper functions
#
# The scripts starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#
# There are several env variables useful when troubleshooting a test failure:
#
#  RBD_MIRROR_NOCLEANUP  - if not empty, don't run the cleanup (stop processes,
#                          destroy the clusters and remove the temp directory)
#                          on exit, so it is possible to check the test state
#                          after failure.
#  RBD_MIRROR_TEMDIR     - use this path when creating the temporary directory
#                          (should not exist) instead of running mktemp(1).
#  RBD_MIRROR_ARGS       - use this to pass additional arguments to started
#                          rbd-mirror daemons.
#  RBD_MIRROR_VARGS      - use this to pass additional arguments to vstart.sh
#                          when starting clusters.
#  RBD_MIRROR_INSTANCES  - number of daemons to start per cluster
#  RBD_MIRROR_CONFIG_KEY - if not empty, use config-key for remote cluster
#                          secrets
# The cleanup can be done as a separate step, running the script with
# `cleanup ${RBD_MIRROR_TEMDIR}' arguments.
#
# Note, as other workunits tests, rbd_mirror_helpers.sh expects to find ceph binaries
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
#     RBD_MIRROR_MODE=journal ../qa/workunits/rbd/rbd_mirror.sh
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
# To setup the environment without actually running the tests:
#
#   cd $CEPH_SRC_PATH
#   RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror \
#     ../qa/workunits/rbd_mirror_helpers.sh setup
#
# Also you can execute commands (functions) from the script:
#
#   cd $CEPH_SRC_PATH
#   export RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror
#   ../qa/workunits/rbd/rbd_mirror_helpers.sh status
#   ../qa/workunits/rbd/rbd_mirror_helpers.sh stop_mirror cluster1
#   ../qa/workunits/rbd/rbd_mirror_helpers.sh start_mirror cluster2
#   ../qa/workunits/rbd/rbd_mirror_helpers.sh flush cluster2
#   ...
#
# Eventually, run the cleanup:
#
#   cd $CEPH_SRC_PATH
#   RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror \
#     ../qa/workunits/rbd_mirror_helpers.sh cleanup
#

if type xmlstarlet > /dev/null 2>&1; then
    XMLSTARLET=xmlstarlet
elif type xml > /dev/null 2>&1; then
    XMLSTARLET=xml
else
    echo "Missing xmlstarlet binary!"
    exit 1
fi

RBD_MIRROR_INSTANCES=${RBD_MIRROR_INSTANCES:-2}

CLUSTER1=cluster1
CLUSTER2=cluster2
PEER_CLUSTER_SUFFIX=
POOL=mirror
PARENT_POOL=mirror_parent
NS1=ns1
NS2=ns2
TEMPDIR=
CEPH_ID=${CEPH_ID:-mirror}
RBD_IMAGE_FEATURES=${RBD_IMAGE_FEATURES:-layering,exclusive-lock,journaling}
MIRROR_USER_ID_PREFIX=${MIRROR_USER_ID_PREFIX:-${CEPH_ID}.}
RBD_MIRROR_MODE=${RBD_MIRROR_MODE:-journal}
MIRROR_POOL_MODE=${MIRROR_POOL_MODE:-pool}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  MIRROR_POOL_MODE=image
fi

export CEPH_ARGS="--id ${CEPH_ID}"

LAST_MIRROR_INSTANCE=$((${RBD_MIRROR_INSTANCES} - 1))

CEPH_ROOT=$(readlink -f $(dirname $0)/../../../src)
CEPH_BIN=.
CEPH_SRC=.
if [ -e CMakeCache.txt ]; then
    CEPH_SRC=${CEPH_ROOT}
    CEPH_ROOT=${PWD}
    CEPH_BIN=./bin

    # needed for ceph CLI under cmake
    export LD_LIBRARY_PATH=${CEPH_ROOT}/lib:${LD_LIBRARY_PATH}
    export PYTHONPATH=${PYTHONPATH}:${CEPH_SRC}/pybind:${CEPH_ROOT}/lib/cython_modules/lib.3
fi

# These vars facilitate running this script in an environment with
# ceph installed from packages, like teuthology. These are not defined
# by default.
#
# RBD_MIRROR_USE_EXISTING_CLUSTER - if set, do not start and stop ceph clusters
# RBD_MIRROR_USE_RBD_MIRROR - if set, use an existing instance of rbd-mirror
#                             running as ceph client $CEPH_ID. If empty,
#                             this script will start and stop rbd-mirror

#
# Functions
#

# Parse a value in format cluster[:instance] and set cluster and instance vars.
set_cluster_instance()
{
    local val=$1
    local cluster_var_name=$2
    local instance_var_name=$3

    cluster=${val%:*}
    instance=${val##*:}

    if [ "${instance}" =  "${val}" ]; then
        # instance was not specified, use default
        instance=0
    fi

    eval ${cluster_var_name}=${cluster}
    eval ${instance_var_name}=${instance}
}

daemon_asok_file()
{
    local local_cluster=$1
    local cluster=$2
    local instance

    set_cluster_instance "${local_cluster}" local_cluster instance

    echo $(ceph-conf --cluster $local_cluster --name "client.${MIRROR_USER_ID_PREFIX}${instance}" 'admin socket')
}

daemon_pid_file()
{
    local cluster=$1
    local instance

    set_cluster_instance "${cluster}" cluster instance

    echo $(ceph-conf --cluster $cluster --name "client.${MIRROR_USER_ID_PREFIX}${instance}" 'pid file')
}

testlog()
{
    echo $(date '+%F %T') $@ | tee -a "${TEMPDIR}/rbd-mirror.test.log" >&2
}

expect_failure()
{
    local expected="$1" ; shift
    local out=${TEMPDIR}/expect_failure.out

    if "$@" > ${out} 2>&1 ; then
        cat ${out} >&2
        return 1
    fi

    if [ -z "${expected}" ]; then
        return 0
    fi

    if ! grep -q "${expected}" ${out} ; then
        cat ${out} >&2
        return 1
    fi

    return 0
}

mkfname()
{
    echo "$@" | sed -e 's|[/ ]|_|g'
}

create_users()
{
    local cluster=$1

    CEPH_ARGS='' ceph --cluster "${cluster}" \
        auth get-or-create client.${CEPH_ID} \
        mon 'profile rbd' osd 'profile rbd' mgr 'profile rbd' >> \
        ${CEPH_ROOT}/run/${cluster}/keyring
    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        CEPH_ARGS='' ceph --cluster "${cluster}" \
            auth get-or-create client.${MIRROR_USER_ID_PREFIX}${instance} \
            mon 'profile rbd-mirror' osd 'profile rbd' mgr 'profile rbd' >> \
            ${CEPH_ROOT}/run/${cluster}/keyring
    done
}

setup_cluster()
{
    local cluster=$1

    CEPH_ARGS='' ${CEPH_SRC}/mstart.sh ${cluster} -n ${RBD_MIRROR_VARGS}

    cd ${CEPH_ROOT}
    rm -f ${TEMPDIR}/${cluster}.conf
    ln -s $(readlink -f run/${cluster}/ceph.conf) \
       ${TEMPDIR}/${cluster}.conf

    cd ${TEMPDIR}
    create_users "${cluster}"

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        cat<<EOF >> ${TEMPDIR}/${cluster}.conf
[client.${MIRROR_USER_ID_PREFIX}${instance}]
    admin socket = ${TEMPDIR}/rbd-mirror.\$cluster-\$name.asok
    pid file = ${TEMPDIR}/rbd-mirror.\$cluster-\$name.pid
    log file = ${TEMPDIR}/rbd-mirror.${cluster}_daemon.${instance}.log
EOF
    done
}

peer_add()
{
    local cluster=$1 ; shift
    local pool=$1 ; shift
    local client_cluster=$1 ; shift
    local remote_cluster="${client_cluster##*@}"

    local uuid_var_name
    if [ -n "$1" ]; then
        uuid_var_name=$1 ; shift
    fi

    local error_code
    local peer_uuid

    for s in 1 2 4 8 16 32; do
        set +e
        peer_uuid=$(rbd --cluster ${cluster} mirror pool peer add \
            ${pool} ${client_cluster} $@)
        error_code=$?
        set -e

        if [ $error_code -eq 17 ]; then
            # raced with a remote heartbeat ping -- remove and retry
            sleep $s
            peer_uuid=$(rbd mirror pool info --cluster ${cluster} --pool ${pool} --format xml | \
                xmlstarlet sel -t -v "//peers/peer[site_name='${remote_cluster}']/uuid")

            CEPH_ARGS='' rbd --cluster ${cluster} --pool ${pool} mirror pool peer remove ${peer_uuid}
        else
            test $error_code -eq 0
            if [ -n "$uuid_var_name" ]; then
                eval ${uuid_var_name}=${peer_uuid}
            fi
            return 0
        fi
    done

    return 1
}

setup_pools()
{
    local cluster=$1
    local remote_cluster=$2
    local mon_map_file
    local mon_addr
    local admin_key_file
    local uuid

    CEPH_ARGS='' ceph --cluster ${cluster} osd pool create ${POOL} 64 64
    CEPH_ARGS='' ceph --cluster ${cluster} osd pool create ${PARENT_POOL} 64 64

    CEPH_ARGS='' rbd --cluster ${cluster} pool init ${POOL}
    CEPH_ARGS='' rbd --cluster ${cluster} pool init ${PARENT_POOL}

    if [ -n "${RBD_MIRROR_CONFIG_KEY}" ]; then
      PEER_CLUSTER_SUFFIX=-DNE
    fi

    CEPH_ARGS='' rbd --cluster ${cluster} mirror pool enable \
        --site-name ${cluster}${PEER_CLUSTER_SUFFIX} ${POOL} ${MIRROR_POOL_MODE}
    rbd --cluster ${cluster} mirror pool enable ${PARENT_POOL} image

    rbd --cluster ${cluster} namespace create ${POOL}/${NS1}
    rbd --cluster ${cluster} namespace create ${POOL}/${NS2}

    rbd --cluster ${cluster} mirror pool enable ${POOL}/${NS1} ${MIRROR_POOL_MODE}
    rbd --cluster ${cluster} mirror pool enable ${POOL}/${NS2} image

    if [ -z ${RBD_MIRROR_MANUAL_PEERS} ]; then
      if [ -z ${RBD_MIRROR_CONFIG_KEY} ]; then
        peer_add ${cluster} ${POOL} ${remote_cluster}
        peer_add ${cluster} ${PARENT_POOL} ${remote_cluster}
      else
        mon_map_file=${TEMPDIR}/${remote_cluster}.monmap
        CEPH_ARGS='' ceph --cluster ${remote_cluster} mon getmap > ${mon_map_file}
        mon_addr=$(monmaptool --print ${mon_map_file} | grep -E 'mon\.' |
          head -n 1 | sed -E 's/^[0-9]+: ([^ ]+).+$/\1/' | sed -E 's/\/[0-9]+//g')

        admin_key_file=${TEMPDIR}/${remote_cluster}.client.${CEPH_ID}.key
        CEPH_ARGS='' ceph --cluster ${remote_cluster} auth get-key client.${CEPH_ID} > ${admin_key_file}

        CEPH_ARGS='' peer_add ${cluster} ${POOL} \
            client.${CEPH_ID}@${remote_cluster}${PEER_CLUSTER_SUFFIX} '' \
            --remote-mon-host "${mon_addr}" --remote-key-file ${admin_key_file}

        peer_add ${cluster} ${PARENT_POOL} client.${CEPH_ID}@${remote_cluster}${PEER_CLUSTER_SUFFIX} uuid
        CEPH_ARGS='' rbd --cluster ${cluster} mirror pool peer set ${PARENT_POOL} ${uuid} mon-host ${mon_addr}
        CEPH_ARGS='' rbd --cluster ${cluster} mirror pool peer set ${PARENT_POOL} ${uuid} key-file ${admin_key_file}
      fi
    fi
}

setup_tempdir()
{
    if [ -n "${RBD_MIRROR_TEMDIR}" ]; then
        test -d "${RBD_MIRROR_TEMDIR}" ||
        mkdir "${RBD_MIRROR_TEMDIR}"
        TEMPDIR="${RBD_MIRROR_TEMDIR}"
        cd ${TEMPDIR}
    else
        TEMPDIR=`mktemp -d`
    fi
}

setup()
{
    local c
    trap 'cleanup $?' INT TERM EXIT

    setup_tempdir
    if [ -z "${RBD_MIRROR_USE_EXISTING_CLUSTER}" ]; then
        setup_cluster "${CLUSTER1}"
        setup_cluster "${CLUSTER2}"
    fi

    setup_pools "${CLUSTER1}" "${CLUSTER2}"
    setup_pools "${CLUSTER2}" "${CLUSTER1}"

    if [ -n "${RBD_MIRROR_MIN_COMPAT_CLIENT}" ]; then
        CEPH_ARGS='' ceph --cluster ${CLUSTER1} osd \
                 set-require-min-compat-client ${RBD_MIRROR_MIN_COMPAT_CLIENT}
        CEPH_ARGS='' ceph --cluster ${CLUSTER2} osd \
                 set-require-min-compat-client ${RBD_MIRROR_MIN_COMPAT_CLIENT}
    fi
}

cleanup()
{
    local error_code=$1

    set +e

    if [ "${error_code}" -ne 0 ]; then
        status
    fi

    if [ -z "${RBD_MIRROR_NOCLEANUP}" ]; then
        local cluster instance

        CEPH_ARGS='' ceph --cluster ${CLUSTER1} osd pool rm ${POOL} ${POOL} --yes-i-really-really-mean-it
        CEPH_ARGS='' ceph --cluster ${CLUSTER2} osd pool rm ${POOL} ${POOL} --yes-i-really-really-mean-it
        CEPH_ARGS='' ceph --cluster ${CLUSTER1} osd pool rm ${PARENT_POOL} ${PARENT_POOL} --yes-i-really-really-mean-it
        CEPH_ARGS='' ceph --cluster ${CLUSTER2} osd pool rm ${PARENT_POOL} ${PARENT_POOL} --yes-i-really-really-mean-it

        for cluster in "${CLUSTER1}" "${CLUSTER2}"; do
            stop_mirrors "${cluster}"
        done

        if [ -z "${RBD_MIRROR_USE_EXISTING_CLUSTER}" ]; then
            cd ${CEPH_ROOT}
            CEPH_ARGS='' ${CEPH_SRC}/mstop.sh ${CLUSTER1}
            CEPH_ARGS='' ${CEPH_SRC}/mstop.sh ${CLUSTER2}
        fi
        test "${RBD_MIRROR_TEMDIR}" = "${TEMPDIR}" || rm -Rf ${TEMPDIR}
    fi

    if [ "${error_code}" -eq 0 ]; then
        echo "OK"
    else
        echo "FAIL"
    fi

    exit ${error_code}
}

start_mirror()
{
    local cluster=$1
    local instance

    set_cluster_instance "${cluster}" cluster instance

    test -n "${RBD_MIRROR_USE_RBD_MIRROR}" && return

    rbd-mirror \
        --cluster ${cluster} \
        --id ${MIRROR_USER_ID_PREFIX}${instance} \
        --rbd-mirror-delete-retry-interval=5 \
        --rbd-mirror-image-state-check-interval=5 \
        --rbd-mirror-journal-poll-age=1 \
        --rbd-mirror-pool-replayers-refresh-interval=5 \
        --debug-rbd=30 --debug-journaler=30 \
        --debug-rbd_mirror=30 \
        --daemonize=true \
        ${RBD_MIRROR_ARGS}
}

start_mirrors()
{
    local cluster=$1

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        start_mirror "${cluster}:${instance}"
    done
}

stop_mirror()
{
    local cluster=$1
    local sig=$2

    test -n "${RBD_MIRROR_USE_RBD_MIRROR}" && return

    local pid
    pid=$(cat $(daemon_pid_file "${cluster}") 2>/dev/null) || :
    if [ -n "${pid}" ]
    then
        kill ${sig} ${pid}
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

stop_mirrors()
{
    local cluster=$1
    local sig=$2

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        stop_mirror "${cluster}:${instance}" "${sig}"
    done
}

admin_daemon()
{
    local cluster=$1 ; shift
    local instance

    set_cluster_instance "${cluster}" cluster instance

    local asok_file=$(daemon_asok_file "${cluster}:${instance}" "${cluster}")
    test -S "${asok_file}"

    ceph --admin-daemon ${asok_file} $@
}

admin_daemons()
{
    local cluster_instance=$1 ; shift
    local cluster="${cluster_instance%:*}"
    local instance="${cluster_instance##*:}"
    local loop_instance

    for s in 0 1 2 4 8 8 8 8 8 8 8 8 16 16; do
        sleep ${s}
        if [ "${instance}" != "${cluster_instance}" ]; then
            admin_daemon "${cluster}:${instance}" $@ && return 0
        else
            for loop_instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
                admin_daemon "${cluster}:${loop_instance}" $@ && return 0
            done
        fi
    done
    return 1
}

all_admin_daemons()
{
    local cluster=$1 ; shift

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        admin_daemon "${cluster}:${instance}" $@
    done
}

status()
{
    local cluster daemon image_pool image_ns image

    for cluster in ${CLUSTER1} ${CLUSTER2}
    do
        echo "${cluster} status"
        CEPH_ARGS='' ceph --cluster ${cluster} -s
        CEPH_ARGS='' ceph --cluster ${cluster} service dump
        CEPH_ARGS='' ceph --cluster ${cluster} service status
        echo

        for image_pool in ${POOL} ${PARENT_POOL}
        do
            for image_ns in "" "${NS1}" "${NS2}"
            do
                echo "${cluster} ${image_pool} ${image_ns} images"
                rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" ls -l
                echo

                echo "${cluster} ${image_pool}${image_ns} mirror pool info"
                rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" mirror pool info
                echo

                echo "${cluster} ${image_pool}${image_ns} mirror pool status"
                CEPH_ARGS='' rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" mirror pool status --verbose
                echo

                for image in `rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" ls 2>/dev/null`
                do
                    echo "image ${image} info"
                    rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" info ${image}
                    echo
                    echo "image ${image} journal status"
                    rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" journal status --image ${image}
                    echo
                    echo "image ${image} snapshots"
                    rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" snap ls --all ${image}
                    echo
                done

                echo "${cluster} ${image_pool} ${image_ns} rbd_mirroring omap vals"
                rados --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" listomapvals rbd_mirroring
                echo "${cluster} ${image_pool} ${image_ns} rbd_mirror_leader omap vals"
                rados --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" listomapvals rbd_mirror_leader
                echo
            done
        done
    done

    local ret

    for cluster in "${CLUSTER1}" "${CLUSTER2}"
    do
        for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
            local pid_file=$(daemon_pid_file ${cluster}:${instance})
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

            local asok_file=$(daemon_asok_file ${cluster}:${instance} ${cluster})
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
    done

    return ${ret}
}

flush()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local cmd="rbd mirror flush"

    if [ -n "${image}" ]
    then
       cmd="${cmd} ${pool}/${image}"
    fi

    admin_daemons "${cluster}" ${cmd}
}

test_image_replay_state()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local test_state=$4
    local status_result
    local current_state=stopped

    status_result=$(admin_daemons "${cluster}" rbd mirror status ${pool}/${image} | grep -i 'state') || return 1
    echo "${status_result}" | grep -i 'Replaying' && current_state=started
    test "${test_state}" = "${current_state}"
}

wait_for_image_replay_state()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state=$4
    local s

    # TODO: add a way to force rbd-mirror to update replayers
    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
        sleep ${s}
        test_image_replay_state "${cluster}" "${pool}" "${image}" "${state}" && return 0
    done
    return 1
}

wait_for_image_replay_started()
{
    local cluster=$1
    local pool=$2
    local image=$3

    wait_for_image_replay_state "${cluster}" "${pool}" "${image}" started
}

wait_for_image_replay_stopped()
{
    local cluster=$1
    local pool=$2
    local image=$3

    wait_for_image_replay_state "${cluster}" "${pool}" "${image}" stopped
}

get_journal_position()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local id_regexp=$4

    # Parse line like below, looking for the first position
    # [id=, commit_position=[positions=[[object_number=1, tag_tid=3, entry_tid=9], [object_number=0, tag_tid=3, entry_tid=8], [object_number=3, tag_tid=3, entry_tid=7], [object_number=2, tag_tid=3, entry_tid=6]]]]

    local status_log=${TEMPDIR}/$(mkfname ${CLUSTER2}-${pool}-${image}.status)
    rbd --cluster ${cluster} journal status --image ${pool}/${image} |
        tee ${status_log} >&2
    sed -nEe 's/^.*\[id='"${id_regexp}"',.*positions=\[\[([^]]*)\],.*state=connected.*$/\1/p' \
        ${status_log}
}

get_master_journal_position()
{
    local cluster=$1
    local pool=$2
    local image=$3

    get_journal_position "${cluster}" "${pool}" "${image}" ''
}

get_mirror_journal_position()
{
    local cluster=$1
    local pool=$2
    local image=$3

    get_journal_position "${cluster}" "${pool}" "${image}" '..*'
}

wait_for_journal_replay_complete()
{
    local local_cluster=$1
    local cluster=$2
    local pool=$3
    local image=$4
    local s master_pos mirror_pos last_mirror_pos
    local master_tag master_entry mirror_tag mirror_entry

    while true; do
        for s in 0.2 0.4 0.8 1.6 2 2 4 4 8 8 16 16 32 32; do
            sleep ${s}
            flush "${local_cluster}" "${pool}" "${image}"
            master_pos=$(get_master_journal_position "${cluster}" "${pool}" "${image}")
            mirror_pos=$(get_mirror_journal_position "${cluster}" "${pool}" "${image}")
            test -n "${master_pos}" -a "${master_pos}" = "${mirror_pos}" && return 0
            test "${mirror_pos}" != "${last_mirror_pos}" && break
        done

        test "${mirror_pos}" = "${last_mirror_pos}" && return 1
        last_mirror_pos="${mirror_pos}"

        # handle the case where the mirror is ahead of the master
        master_tag=$(echo "${master_pos}" | grep -Eo "tag_tid=[0-9]*" | cut -d'=' -f 2)
        mirror_tag=$(echo "${mirror_pos}" | grep -Eo "tag_tid=[0-9]*" | cut -d'=' -f 2)
        master_entry=$(echo "${master_pos}" | grep -Eo "entry_tid=[0-9]*" | cut -d'=' -f 2)
        mirror_entry=$(echo "${mirror_pos}" | grep -Eo "entry_tid=[0-9]*" | cut -d'=' -f 2)
        test "${master_tag}" = "${mirror_tag}" -a ${master_entry} -le ${mirror_entry} && return 0
    done
    return 1
}

mirror_image_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster "${cluster}" mirror image snapshot "${pool}/${image}"
}

get_newest_mirror_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local log=$4

    rbd --cluster "${cluster}" snap list --all "${pool}/${image}" --format xml | \
        xmlstarlet sel -t -c "//snapshots/snapshot[namespace/complete='true' and position()=last()]" > \
        ${log} || true
}

wait_for_snapshot_sync_complete()
{
    local local_cluster=$1
    local cluster=$2
    local pool=$3
    local image=$4

    local status_log=${TEMPDIR}/$(mkfname ${cluster}-${pool}-${image}.status)
    local local_status_log=${TEMPDIR}/$(mkfname ${local_cluster}-${pool}-${image}.status)

    mirror_image_snapshot "${cluster}" "${pool}" "${image}"
    get_newest_mirror_snapshot "${cluster}" "${pool}" "${image}" "${status_log}"
    local snapshot_id=$(xmlstarlet sel -t -v "//snapshot/id" < ${status_log})

    while true; do
        for s in 0.2 0.4 0.8 1.6 2 2 4 4 8 8 16 16 32 32; do
            sleep ${s}

            get_newest_mirror_snapshot "${local_cluster}" "${pool}" "${image}" "${local_status_log}"
            local primary_snapshot_id=$(xmlstarlet sel -t -v "//snapshot/namespace/primary_snap_id" < ${local_status_log})

            test "${snapshot_id}" = "${primary_snapshot_id}" && return 0
        done

        return 1
    done
    return 1
}

wait_for_replay_complete()
{
    local local_cluster=$1
    local cluster=$2
    local pool=$3
    local image=$4

    if [ "${RBD_MIRROR_MODE}" = "journal" ]; then
        wait_for_journal_replay_complete ${local_cluster} ${cluster} ${pool} ${image}
    elif [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
        wait_for_snapshot_sync_complete ${local_cluster} ${cluster} ${pool} ${image}
    else
        return 1
    fi
}


test_status_in_pool_dir()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state_pattern="$4"
    local description_pattern="$5"
    local service_pattern="$6"

    local status_log=${TEMPDIR}/$(mkfname ${cluster}-${pool}-${image}.mirror_status)
    CEPH_ARGS='' rbd --cluster ${cluster} mirror image status ${pool}/${image} |
        tee ${status_log} >&2
    grep "^  state: .*${state_pattern}" ${status_log} || return 1
    grep "^  description: .*${description_pattern}" ${status_log} || return 1

    if [ -n "${service_pattern}" ]; then
        grep "service: *${service_pattern}" ${status_log} || return 1
    elif echo ${state_pattern} | grep '^up+'; then
        grep "service: *${MIRROR_USER_ID_PREFIX}.* on " ${status_log} || return 1
    else
        grep "service: " ${status_log} && return 1
    fi

    # recheck using `mirror pool status` command to stress test it.

    local last_update="$(sed -nEe 's/^  last_update: *(.*) *$/\1/p' ${status_log})"
    test_mirror_pool_status_verbose \
        ${cluster} ${pool} ${image} "${state_pattern}" "${last_update}" &&
    return 0

    echo "'mirror pool status' test failed" >&2
    exit 1
}

test_mirror_pool_status_verbose()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state_pattern="$4"
    local prev_last_update="$5"

    local status_log=${TEMPDIR}/$(mkfname ${cluster}-${pool}.mirror_status)

    rbd --cluster ${cluster} mirror pool status ${pool} --verbose --format xml \
        > ${status_log}

    local last_update state
    last_update=$($XMLSTARLET sel -t -v \
        "//images/image[name='${image}']/last_update" < ${status_log})
    state=$($XMLSTARLET sel -t -v \
        "//images/image[name='${image}']/state" < ${status_log})

    echo "${state}" | grep "${state_pattern}" ||
    test "${last_update}" '>' "${prev_last_update}"
}

wait_for_status_in_pool_dir()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state_pattern="$4"
    local description_pattern="$5"
    local service_pattern="$6"

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
        sleep ${s}
        test_status_in_pool_dir ${cluster} ${pool} ${image} "${state_pattern}" \
                                "${description_pattern}" "${service_pattern}" &&
            return 0
    done
    return 1
}

wait_for_replaying_status_in_pool_dir()
{
    local cluster=$1
    local pool=$2
    local image=$3

    if [ "${RBD_MIRROR_MODE}" = "journal" ]; then
        wait_for_status_in_pool_dir ${cluster} ${pool} ${image} 'up+replaying' \
                                    'primary_position'
    else
        wait_for_status_in_pool_dir ${cluster} ${pool} ${image} 'up+replaying'
    fi
}

create_image()
{
    local cluster=$1 ; shift
    local pool=$1 ; shift
    local image=$1 ; shift
    local size=128

    if [ -n "$1" ]; then
        size=$1
        shift
    fi

    rbd --cluster ${cluster} create --size ${size} \
        --image-feature "${RBD_IMAGE_FEATURES}" $@ ${pool}/${image}
}

is_pool_mirror_mode_image()
{
    local pool=$1

    if [ "${MIRROR_POOL_MODE}" = "image" ]; then
        return 0
    fi

    case "${pool}" in
        */${NS2} | ${PARENT_POOL})
            return 0
            ;;
    esac

    return 1
}

create_image_and_enable_mirror()
{
    local cluster=$1 ; shift
    local pool=$1 ; shift
    local image=$1 ; shift
    local mode=${1:-${RBD_MIRROR_MODE}}
    if [ -n "$1" ]; then
        shift
    fi

    create_image ${cluster} ${pool} ${image} $@
    if is_pool_mirror_mode_image ${pool}; then
        enable_mirror ${cluster} ${pool} ${image} ${mode}
    fi
}

enable_journaling()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} feature enable ${pool}/${image} journaling
}

set_image_meta()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local key=$4
    local val=$5

    rbd --cluster ${cluster} image-meta set ${pool}/${image} $key $val
}

compare_image_meta()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local key=$4
    local value=$5

    test `rbd --cluster ${cluster} image-meta get ${pool}/${image} ${key}` = "${value}"
}

rename_image()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local new_name=$4

    rbd --cluster=${cluster} rename ${pool}/${image} ${pool}/${new_name}
}

remove_image()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} snap purge ${pool}/${image}
    rbd --cluster=${cluster} rm ${pool}/${image}
}

remove_image_retry()
{
    local cluster=$1
    local pool=$2
    local image=$3

    for s in 0 1 2 4 8 16 32; do
        sleep ${s}
        remove_image ${cluster} ${pool} ${image} && return 0
    done
    return 1
}

trash_move() {
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} trash move ${pool}/${image}
}

trash_restore() {
    local cluster=$1
    local pool=$2
    local image=$3
    local image_id=$4
    local mode=${5:-${RBD_MIRROR_MODE}}

    rbd --cluster=${cluster} trash restore ${pool}/${image_id}
    if is_pool_mirror_mode_image ${pool}; then
        enable_mirror ${cluster} ${pool} ${image} ${mode}
    fi
}

clone_image()
{
    local cluster=$1
    local parent_pool=$2
    local parent_image=$3
    local parent_snap=$4
    local clone_pool=$5
    local clone_image=$6

    shift 6

    rbd --cluster ${cluster} clone \
        ${parent_pool}/${parent_image}@${parent_snap} \
        ${clone_pool}/${clone_image} --image-feature "${RBD_IMAGE_FEATURES}" $@
}

clone_image_and_enable_mirror()
{
    local cluster=$1
    local parent_pool=$2
    local parent_image=$3
    local parent_snap=$4
    local clone_pool=$5
    local clone_image=$6
    shift 6

    local mode=${1:-${RBD_MIRROR_MODE}}
    if [ -n "$1" ]; then
        shift
    fi

    clone_image ${cluster} ${parent_pool} ${parent_image} ${parent_snap} ${clone_pool} ${clone_image} $@
    if is_pool_mirror_mode_image ${clone_pool}; then
      enable_mirror ${cluster} ${clone_pool} ${clone_image} ${mode}
    fi
}

disconnect_image()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} journal client disconnect \
        --image ${pool}/${image}
}

create_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} snap create ${pool}/${image}@${snap}
}

remove_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} snap rm ${pool}/${image}@${snap}
}

rename_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4
    local new_snap=$5

    rbd --cluster ${cluster} snap rename ${pool}/${image}@${snap} \
        ${pool}/${image}@${new_snap}
}

purge_snapshots()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} snap purge ${pool}/${image}
}

protect_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} snap protect ${pool}/${image}@${snap}
}

unprotect_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} snap unprotect ${pool}/${image}@${snap}
}

unprotect_snapshot_retry()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    for s in 0 1 2 4 8 16 32; do
        sleep ${s}
        unprotect_snapshot ${cluster} ${pool} ${image} ${snap} && return 0
    done
    return 1
}

wait_for_snap_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap_name=$4
    local s

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16 16 16 32 32 32 32; do
        sleep ${s}
        rbd --cluster ${cluster} info ${pool}/${image}@${snap_name} || continue
        return 0
    done
    return 1
}

test_snap_moved_to_trash()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap_name=$4

    rbd --cluster ${cluster} snap ls ${pool}/${image} --all |
        grep -F " trash (${snap_name})"
}

wait_for_snap_moved_to_trash()
{
    local s

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16 16 16 32 32 32 32; do
        sleep ${s}
        test_snap_moved_to_trash $@ || continue
        return 0
    done
    return 1
}

test_snap_removed_from_trash()
{
    test_snap_moved_to_trash $@ && return 1
    return 0
}

wait_for_snap_removed_from_trash()
{
    local s

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16 16 16 32 32 32 32; do
        sleep ${s}
        test_snap_removed_from_trash $@ || continue
        return 0
    done
    return 1
}

count_mirror_snaps()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} snap ls ${pool}/${image} --all |
        grep -c -F " mirror ("
}

write_image()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local count=$4
    local size=$5

    test -n "${size}" || size=4096

    rbd --cluster ${cluster} bench ${pool}/${image} --io-type write \
        --io-size ${size} --io-threads 1 --io-total $((size * count)) \
        --io-pattern rand
}

stress_write_image()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local duration=$(awk 'BEGIN {srand(); print int(10 * rand()) + 5}')

    set +e
    timeout ${duration}s ceph_test_rbd_mirror_random_write \
        --cluster ${cluster} ${pool} ${image} \
        --debug-rbd=20 --debug-journaler=20 \
        2> ${TEMPDIR}/rbd-mirror-random-write.log
    error_code=$?
    set -e

    if [ $error_code -eq 124 ]; then
        return 0
    fi
    return 1
}

show_diff()
{
    local file1=$1
    local file2=$2

    xxd ${file1} > ${file1}.xxd
    xxd ${file2} > ${file2}.xxd
    sdiff -s ${file1}.xxd ${file2}.xxd | head -n 64
    rm -f ${file1}.xxd ${file2}.xxd
}

compare_images()
{
    local pool=$1
    local image=$2
    local ret=0

    local rmt_export=${TEMPDIR}/$(mkfname ${CLUSTER2}-${pool}-${image}.export)
    local loc_export=${TEMPDIR}/$(mkfname ${CLUSTER1}-${pool}-${image}.export)

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${CLUSTER2} export ${pool}/${image} ${rmt_export}
    rbd --cluster ${CLUSTER1} export ${pool}/${image} ${loc_export}
    if ! cmp ${rmt_export} ${loc_export}
    then
        show_diff ${rmt_export} ${loc_export}
        ret=1
    fi
    rm -f ${rmt_export} ${loc_export}
    return ${ret}
}

compare_image_snapshots()
{
    local pool=$1
    local image=$2
    local ret=0

    local rmt_export=${TEMPDIR}/${CLUSTER2}-${pool}-${image}.export
    local loc_export=${TEMPDIR}/${CLUSTER1}-${pool}-${image}.export

    for snap_name in $(rbd --cluster ${CLUSTER1} --format xml \
                           snap list ${pool}/${image} | \
                           $XMLSTARLET sel -t -v "//snapshot/name" | \
                           grep -E -v "^\.rbd-mirror\."); do
        rm -f ${rmt_export} ${loc_export}
        rbd --cluster ${CLUSTER2} export ${pool}/${image}@${snap_name} ${rmt_export}
        rbd --cluster ${CLUSTER1} export ${pool}/${image}@${snap_name} ${loc_export}
        if ! cmp ${rmt_export} ${loc_export}
        then
            show_diff ${rmt_export} ${loc_export}
            ret=1
        fi
    done
    rm -f ${rmt_export} ${loc_export}
    return ${ret}
}

demote_image()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} mirror image demote ${pool}/${image}
}

promote_image()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local force=$4

    rbd --cluster=${cluster} mirror image promote ${pool}/${image} ${force}
}

set_pool_mirror_mode()
{
    local cluster=$1
    local pool=$2
    local mode=${3:-${MIRROR_POOL_MODE}}

    rbd --cluster=${cluster} mirror pool enable ${pool} ${mode}
}

disable_mirror()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} mirror image disable ${pool}/${image}
}

enable_mirror()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local mode=${4:-${RBD_MIRROR_MODE}}

    rbd --cluster=${cluster} mirror image enable ${pool}/${image} ${mode}
    # Display image info including the global image id for debugging purpose
    rbd --cluster=${cluster} info ${pool}/${image}
}

test_image_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local test_state=$4
    local image_id=$5
    local current_state=deleted
    local current_image_id

    current_image_id=$(get_image_id ${cluster} ${pool} ${image})
    test -n "${current_image_id}" &&
    test -z "${image_id}" -o "${image_id}" = "${current_image_id}" &&
    current_state=present

    test "${test_state}" = "${current_state}"
}

wait_for_image_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state=$4
    local image_id=$5
    local s

    test -n "${image_id}" ||
    image_id=$(get_image_id ${cluster} ${pool} ${image})

    # TODO: add a way to force rbd-mirror to update replayers
    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32; do
        sleep ${s}
        test_image_present \
            "${cluster}" "${pool}" "${image}" "${state}" "${image_id}" &&
        return 0
    done
    return 1
}

get_image_id()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} info ${pool}/${image} |
        sed -ne 's/^.*block_name_prefix: rbd_data\.//p'
}

request_resync_image()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local image_id_var_name=$4

    eval "${image_id_var_name}='$(get_image_id ${cluster} ${pool} ${image})'"
    eval 'test -n "$'${image_id_var_name}'"'

    rbd --cluster=${cluster} mirror image resync ${pool}/${image}
}

get_image_data_pool()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} info ${pool}/${image} |
        awk '$1 == "data_pool:" {print $2}'
}

get_clone_format()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} info ${pool}/${image} |
        awk 'BEGIN {
               format = 1
             }
             $1 == "parent:" {
               parent = $2
             }
             /op_features: .*clone-child/ {
               format = 2
             }
             END {
               if (!parent) exit 1
               print format
             }'
}

list_omap_keys()
{
    local cluster=$1
    local pool=$2
    local obj_name=$3

    rados --cluster ${cluster} -p ${pool} listomapkeys ${obj_name}
}

count_omap_keys_with_filter()
{
    local cluster=$1
    local pool=$2
    local obj_name=$3
    local filter=$4

    list_omap_keys ${cluster} ${pool} ${obj_name} | grep -c ${filter}
}

wait_for_omap_keys()
{
    local cluster=$1
    local pool=$2
    local obj_name=$3
    local filter=$4

    for s in 0 1 2 2 4 4 8 8 8 16 16 32; do
        sleep $s

        set +e
        test "$(count_omap_keys_with_filter ${cluster} ${pool} ${obj_name} ${filter})" = 0
        error_code=$?
        set -e

        if [ $error_code -eq 0 ]; then
            return 0
        fi
    done

    return 1
}

wait_for_image_in_omap()
{
    local cluster=$1
    local pool=$2

    wait_for_omap_keys ${cluster} ${pool} rbd_mirroring status_global
    wait_for_omap_keys ${cluster} ${pool} rbd_mirroring image_
    wait_for_omap_keys ${cluster} ${pool} rbd_mirror_leader image_map
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
