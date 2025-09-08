#!/usr/bin/env bash
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
#  RBD_MIRROR_SHOW_CLI_CMD if not empty, external commands sent to the cluster and
#                          more information on test failures will be printed.
#                          The script will exit on fatal test failures
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
#     ../qa/workunits/rbd/rbd_mirror_helpers.sh setup
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
#     ../qa/workunits/rbd/rbd_mirror_helpers.sh cleanup
#

RBD_MIRROR_INSTANCES=${RBD_MIRROR_INSTANCES:-2}

CLUSTER1=cluster1
CLUSTER2=cluster2
PEER_CLUSTER_SUFFIX=
POOL=mirror
PARENT_POOL=mirror_parent
NS1=ns1
NS2=ns2
TEMPDIR=
CMD_STDOUT=last_cmd.stdout
CMD_STDERR=last_cmd.stderr
CEPH_ID=${CEPH_ID:-mirror}
RBD_IMAGE_FEATURES=${RBD_IMAGE_FEATURES:-layering,exclusive-lock,journaling}
MIRROR_USER_ID_PREFIX=${MIRROR_USER_ID_PREFIX:-${CEPH_ID}.}
RBD_MIRROR_MODE=${RBD_MIRROR_MODE:-journal}
MIRROR_POOL_MODE=${MIRROR_POOL_MODE:-pool}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  MIRROR_POOL_MODE=image
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
NO_COLOUR='\033[0m'

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

    eval "${cluster_var_name}"="${cluster}"
    eval "${instance_var_name}"="${instance}"
}

print_stacktrace() {
    local frame=1 LINE SUB FILE
    while read -r LINE SUB FILE < <(caller "$frame"); do
      printf '[%s]  %s @ %s:%s\n' "$((frame++))" "${SUB}" "${FILE}" "${LINE}" 1>&2
    done
}

run_cmd_internal() {
    local exit_on_failure=$1 ; shift
    local as_admin=$1 ; shift
    local rc
    local frame=0 LINE SUB FILE

    if [ -n "${RBD_MIRROR_SHOW_CLI_CMD}" ]; then
        if [ 'true' = "${as_admin}" ]; then
            echo "CEPH_ARGS=''" "$@"
        else
            echo "CEPH_ARGS='--id ${CEPH_ID}'" "$@"
        fi
    fi

    if [ 'true' = "${as_admin}" ]; then
        export CEPH_ARGS=''
    else
        export CEPH_ARGS="--id ${CEPH_ID}"
    fi

    echo "$@" >> "${TEMPDIR}/rbd-mirror.cmd.log"

    # Don't exit immediately if the command exits with a non-zero status.
    set +e
    eval $@ >"${CMD_STDOUT}" 2>"${CMD_STDERR}"
    rc=$?
    set -e

    if [ -n "${RBD_MIRROR_SHOW_CLI_CMD}" ]; then
        cat "$CMD_STDOUT"
        cat "$CMD_STDERR" 1>&2
    fi

    if [ -n "${RBD_MIRROR_SAVE_CLI_OUTPUT}" ]; then 
        if [ 'true' = "${as_admin}" ]; then
            echo "CEPH_ARGS=''" "$@" >> "${TEMPDIR}/${RBD_MIRROR_SAVE_CLI_OUTPUT}"
        else
            echo "CEPH_ARGS='--id ${CEPH_ID}'" "$@" >> "${TEMPDIR}/${RBD_MIRROR_SAVE_CLI_OUTPUT}"
        fi
        cat "$CMD_STDOUT" >> "${TEMPDIR}/${RBD_MIRROR_SAVE_CLI_OUTPUT}"
        cat "$CMD_STDERR" >> "${TEMPDIR}/${RBD_MIRROR_SAVE_CLI_OUTPUT}"
    fi

    if [ 0 = $rc ] ; then
        return 0
    fi

    if [ 'true' = "${exit_on_failure}" ]; then
        echo -e "${RED}ERR: rc=" $rc 1>&2
        print_stacktrace
        echo -e "${NO_COLOUR}"
        exit $rc
    else
        local frame=1 LINE SUB FILE

        if [ -n "${RBD_MIRROR_SHOW_CLI_CMD}" ]; then
            echo "ERR: rc=" $rc 1>&2
            read -r LINE SUB FILE < <(caller "$frame")
            printf "ERR: Non-fatal failure at: %s:%s %s()\n" "${FILE}" "${LINE}" "${SUB}" 1>&2
        fi

        return $rc
    fi
}

run_cmd() {
    run_cmd_internal 'true' 'false' $@
}

# run the command but ignore any failure and return the exit status
try_cmd() {
    run_cmd_internal 'false' 'false' $@
}

run_admin_cmd() {
    run_cmd_internal 'true' 'true' $@
}

# run the command but ignore any failure and return the exit status
try_admin_cmd() {
    run_cmd_internal 'false' 'true' $@
}

fail() {
    local fatal=$1
    local frame=0 LINE SUB FILE

    if [ -z "${RBD_MIRROR_SHOW_CLI_CMD}" ]; then
        return 0
    fi

    if [ -n "${fatal}" ]; then
        echo -e "${RED}${fatal}" 1>&2
        print_stacktrace
        echo -e "${NO_COLOUR}"
        exit 1
    fi

    read -r LINE SUB FILE < <(caller "$frame")
    printf 'ERR: Non-fatal failure at: %s:%s %s()\n' "${FILE}" "${LINE}" "${SUB}" 1>&2

    return 0
}

daemon_asok_file()
{
    local local_cluster=$1
    local cluster=$2
    local instance

    set_cluster_instance "${local_cluster}" local_cluster instance

    echo $(ceph-conf --cluster $local_cluster --name "client.${MIRROR_USER_ID_PREFIX}${instance}" 'admin socket') || fail
}

daemon_pid_file()
{
    local cluster=$1
    local instance

    set_cluster_instance "${cluster}" cluster instance

    echo $(ceph-conf --cluster $cluster --name "client.${MIRROR_USER_ID_PREFIX}${instance}" 'pid file')
}

echo_red()
{
    echo -e "${RED}$@${NO_COLOUR}"
}

testlog()
{
    echo -e "${RED}"$(date '+%F %T') $@ "${NO_COLOUR}"| tee -a "${TEMPDIR}/rbd-mirror.test.log" >&2
}

expect_failure()
{
    local expected="$1" ; shift
    local out=${TEMPDIR}/expect_failure.out

    if "$@" > ${out} 2>&1 ; then
        cat ${out} >&2
        echo "Command did not fail"
        return 1
    fi

    if [ -z "${expected}" ]; then
        return 0
    fi

    if ! grep -q "${expected}" ${out} ; then
        cat ${out} >&2
        echo "Command did not fail with expected message"
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

    CEPH_ARGS='' MDS=0 ${CEPH_SRC}/mstart.sh ${cluster} -n ${RBD_MIRROR_VARGS} --without-dashboard

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

namespace_create()
{
    local cluster=$1
    local namespace_spec=$2

    run_cmd "rbd --cluster ${cluster} namespace create ${namespace_spec}"
}

namespace_remove()
{
    local cluster=$1
    local namespace_spec=$2
    local runner=${3:-"run_cmd"}

    "$runner" "rbd --cluster ${cluster} namespace remove ${namespace_spec}"
}

namespace_remove_retry()
{
    local cluster=$1
    local namespace_spec=$2

    for s in 0 1 2 4 8 16 32; do
        sleep ${s}
        namespace_remove "${cluster}" "${namespace_spec}" "try_cmd" && return 0
    done
    return 1
}

setup_dummy_objects()
{
    local cluster=$1
    # Create and delete a pool, image, group and snapshots so that ids on the two clusters mismatch
    run_admin_cmd "ceph --cluster ${cluster} osd pool create dummy_pool 64 64"
    image_create "${cluster}" "dummy_pool/dummy_image"
    create_snapshot "${cluster}" "dummy_pool" "dummy_image" "dummy_snap"
    group_create "${cluster}" "dummy_pool/dummy_group"
    group_snap_create "${cluster}" "dummy_pool/dummy_group" "dummy_snap"
    run_admin_cmd "ceph --cluster ${cluster} osd pool delete dummy_pool dummy_pool --yes-i-really-really-mean-it"
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
    rbd --cluster ${cluster} namespace create ${PARENT_POOL}/${NS1}

    rbd --cluster ${cluster} mirror pool enable ${POOL}/${NS1} ${MIRROR_POOL_MODE}
    rbd --cluster ${cluster} mirror pool enable ${POOL}/${NS2} image
    rbd --cluster ${cluster} mirror pool enable ${PARENT_POOL}/${NS1} ${MIRROR_POOL_MODE}

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
    trap 'cleanup $?' INT TERM EXIT

    setup_tempdir
    if [ -z "${RBD_MIRROR_USE_EXISTING_CLUSTER}" ]; then
        setup_cluster "${CLUSTER1}"
        setup_cluster "${CLUSTER2}"
        setup_dummy_objects "${CLUSTER1}"
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
    if [ "${error_code}" -ne 0 ] && [ -z "${RBD_MIRROR_NO_STATUS}" ]; then
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
    local log=${TEMPDIR}/rbd-mirror-${cluster}-${instance}.out
    ulimit -c unlimited

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
        --log-file=${log} \
        ${RBD_MIRROR_ARGS}
}

start_mirrors()
{
    local cluster=$1

    for instance in `seq 0 ${LAST_MIRROR_INSTANCE}`; do
        start_mirror "${cluster}:${instance}"
    done
}

check_daemon_running()
{
    local cluster=$1
    local restart=$2
    local pid
    local cmd

    pid=$(cat "$(daemon_pid_file "${cluster}")" 2>/dev/null) || :
    if [ -z "${pid}" ] && [ -z "${restart}" ]
    then
        fail 'cannot determine daemon pid'
    fi

    cmd=$(ps -p ${pid} -o comm -h) || :
    if [ "${cmd}" != 'rbd-mirror' ] && [ -z "${restart}" ]; then
        echo 'pid='"${pid}"
        echo 'cmd='"${cmd}"
        fail 'rdb-mirror not running'
    fi

    if [ -n "${restart}" ]; then
        start_mirror "${cluster}"
    fi
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

    for instance in $(seq 0 ${LAST_MIRROR_INSTANCE}); do
        stop_mirror "${cluster}:${instance}" "${sig}"
    done
}

admin_daemon()
{
    local cluster=$1 ; shift
    local instance

    set_cluster_instance "${cluster}" cluster instance

    local asok_file=$(daemon_asok_file "${cluster}:${instance}" "${cluster}") || { fail; return 1; }
    test -S "${asok_file}"

    try_cmd "ceph --admin-daemon ${asok_file} $*"
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
            admin_daemon "${cluster}:${instance}" "$@" && return 0
        else
            for loop_instance in $(seq 0 ${LAST_MIRROR_INSTANCE}); do
                admin_daemon "${cluster}:${loop_instance}" "$@" && return 0
            done
        fi
    done
    fail "daemon command failed after multiple retries"
    return 1
}

all_admin_daemons()
{
    local cluster=$1 ; shift

    for instance in $(seq 0 ${LAST_MIRROR_INSTANCE}); do
        admin_daemon "${cluster}:${instance}" "$@"
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

                echo "${cluster} ${image_pool} ${image_ns} mirror pool info"
                rbd --cluster ${cluster} -p ${image_pool} --namespace "${image_ns}" mirror pool info
                echo

                echo "${cluster} ${image_pool} ${image_ns} mirror pool status"
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

    echo "Dumping contents of $CMD_STDOUT"
    cat "$CMD_STDOUT" || :
    echo 

    echo "Dumping contents of $CMD_STDERR"
    cat "$CMD_STDERR" || :
    echo 

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

get_pool_status_json()
{
    local cluster="$1"
    local pool="$2"

    CEPH_ARGS='' rbd --cluster "${cluster}" mirror pool status "${pool}" --verbose --format json
}

test_health_state()
{
    local cluster="$1"
    local pool="$2"
    local state="$3"

    local status
    status="$(get_pool_status_json "${cluster}" "${pool}")"
    jq -e '.summary.health == "'"${state}"'"' <<< "${status}"
}

wait_for_health_state()
{
    local cluster="$1"
    local pool="$2"
    local state="$3"
    local s

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
        sleep "${s}"
        test_health_state "${cluster}" "${pool}" "${state}" && return 0
    done
    return 1
}

test_image_replay_state()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local test_state=$4
    local current_state=stopped

    admin_daemons "${cluster}" rbd mirror status ${pool}/${image} --format xml-pretty || { fail; return 1; }
    test "Replaying" = "$(xmlstarlet sel -t -v "//image_replayer/state" < "$CMD_STDOUT" )" && current_state=started
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
    local local_pool=$3
    local remote_pool=$4
    local image=$5
    local s master_pos mirror_pos last_mirror_pos
    local master_tag master_entry mirror_tag mirror_entry

    while true; do
        for s in 0.2 0.4 0.8 1.6 2 2 4 4 8 8 16 16 32 32; do
            sleep ${s}
            flush "${local_cluster}" "${local_pool}" "${image}"
            master_pos=$(get_master_journal_position "${cluster}" "${remote_pool}" "${image}")
            mirror_pos=$(get_mirror_journal_position "${cluster}" "${remote_pool}" "${image}")
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

# get the primary_snap_id for the most recent complete snap on the secondary cluster
get_primary_snap_id_for_newest_mirror_snapshot_on_secondary()
{
    local secondary_cluster=$1
    local image_spec=$2
    local -n _snap_id=$3

    run_cmd "rbd --cluster ${secondary_cluster} snap list --all ${image_spec} --format xml --pretty-format" 
    _snap_id=$(xmlstarlet sel -t -v "(//snapshots/snapshot/namespace[complete='true']/primary_snap_id)[last()]" "$CMD_STDOUT" )
}

# get the snap_id for the most recent complete snap on the primary cluster
get_newest_mirror_snapshot_id_on_primary()
{
    local primary_cluster=$1
    local image_spec=$2
    local -n _snap_id=$3

    run_cmd "rbd --cluster ${primary_cluster} snap list --all ${image_spec} --format xml --pretty-format" 
    _snap_id=$(xmlstarlet sel -t -v "(//snapshots/snapshot[namespace/complete='true']/id)[last()]" "$CMD_STDOUT" )
}

test_snap_present()
{
    local secondary_cluster=$1
    local image_spec=$2
    local snap_id=$3
    local expected_snap_count=$4

    run_cmd "rbd --cluster ${secondary_cluster} snap list -a ${image_spec} --format xml --pretty-format" 
    test "${expected_snap_count}" = "$(xmlstarlet sel -t -v "count(//snapshots/snapshot/namespace[primary_snap_id='${snap_id}'])" < "$CMD_STDOUT")" || { fail; return 1; }
}

test_snap_complete()
{
    local secondary_cluster=$1
    local image_spec=$2
    local snap_id=$3
    local expected_complete=$4

    run_cmd "rbd --cluster ${secondary_cluster} snap list -a ${image_spec} --format xml --pretty-format" 
    test "${expected_complete}" = "$(xmlstarlet sel -t -v "//snapshots/snapshot/namespace[primary_snap_id='${snap_id}']/complete" < "$CMD_STDOUT")" || { fail; return 1; }
}

wait_for_test_snap_present()
{
    local secondary_cluster=$1
    local image_spec=$2
    local snap_id=$3
    local test_snap_count=$4
    local s

    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32; do
        sleep ${s}
        test_snap_present "${secondary_cluster}" "${image_spec}" "${snap_id}" "${test_snap_count}" && return 0
    done

    fail "wait for count of snaps with id ${snap_id} to be ${test_snap_count} failed on ${secondary_cluster}"
    return 1
}

wait_for_snap_id_present()
{
    local secondary_cluster=$1
    local image_spec=$2
    local snap_id=$3

    wait_for_test_snap_present "${secondary_cluster}" "${image_spec}" "${snap_id}" 1
}

wait_for_snap_id_not_present()
{
    local secondary_cluster=$1
    local image_spec=$2
    local snap_id=$3

    wait_for_test_snap_present "${secondary_cluster}" "${image_spec}" "${snap_id}" 0
}

wait_for_snapshot_sync_complete()
{
    local local_cluster=$1 
    local cluster=$2
    local local_pool=$3
    local remote_pool=$4
    local image=$5

    local primary_snapshot_id snapshot_id
    get_newest_mirror_snapshot_id_on_primary "${cluster}" "${remote_pool}/${image}" primary_snapshot_id

    while true; do
        for s in 0.2 0.4 0.8 1.6 2 2 4 4 8 8 16 16 32 32; do
            sleep ${s}
            get_primary_snap_id_for_newest_mirror_snapshot_on_secondary "${local_cluster}" "${local_pool}/${image}" snapshot_id || continue
            test "${snapshot_id}" = "${primary_snapshot_id}" && return 0
        done
        return 1
    done
    return 1
}

wait_for_replay_complete()
{
    local local_cluster=$1 #sec
    local cluster=$2 #pri
    local local_pool=$3
    local remote_pool=$4
    local image=$5

    if [ "${RBD_MIRROR_MODE}" = "journal" ]; then
        wait_for_journal_replay_complete ${local_cluster} ${cluster} ${local_pool} ${remote_pool} ${image}
    elif [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
        mirror_image_snapshot "${cluster}" "${remote_pool}" "${image}"
        wait_for_snapshot_sync_complete ${local_cluster} ${cluster} ${local_pool} ${remote_pool} ${image}
    else
        return 1
    fi
}

count_fields_in_mirror_pool_status()
{
    local cluster=$1 ; shift
    local pool=$1 ; shift
    local -n _pool_result_count_arr=$1 ; shift
    local fields=("$@")

    run_cmd "rbd --cluster ${cluster} mirror pool status --verbose ${pool} --format xml --pretty-format" || { fail; return 1; }

    local field result
    for field in "${fields[@]}"; do
      result=$(xmlstarlet sel -t -v "count($field)"  < "$CMD_STDOUT")
      _pool_result_count_arr+=( "${result}" )
    done
}

get_fields_from_mirror_pool_status()
{
    local cluster=$1 ; shift
    local pool=$1 ; shift
    local -n _pool_result_arr=$1 ; shift
    local fields=("$@")

    run_cmd "rbd --cluster ${cluster} mirror pool status --verbose ${pool} --format xml --pretty-format" || { fail; return 1; }

    local field result
    for field in "${fields[@]}"; do
      result=$(xmlstarlet sel -t -v "$field"  < "$CMD_STDOUT") || { fail "field not found: ${field}"; return; }
      _pool_result_arr+=( "${result}" )
    done
}

get_fields_from_mirror_group_status()
{
    local cluster=$1 ; shift
    local group_spec=$1 ; shift
    local -n _group_result_arr=$1 ; shift
    local fields=("$@")

    run_admin_cmd "rbd --cluster ${cluster} mirror group status ${group_spec} --format xml --pretty-format" || { fail; return 1; }

    local field result
    for field in "${fields[@]}"; do
      result=$(xmlstarlet sel -t -v "$field"  < "$CMD_STDOUT") || { fail "field not found: ${field}"; return; }
      _group_result_arr+=( "${result}" )
    done
}

get_fields_from_group_info()
{
    local cluster=$1 ; shift
    local group_spec=$1 ; shift
    local runner=$1 ; shift
    local -n _group_info_result_arr=$1 ; shift
    local fields=("$@")

    "$runner" "rbd --cluster ${cluster} group info ${group_spec} --format xml --pretty-format" || { fail; return 1; }

    local field result
    for field in "${fields[@]}"; do
      result=$(xmlstarlet sel -t -v "$field"  < "$CMD_STDOUT") || { fail "field not found: ${field}"; return; }
      _group_info_result_arr+=( "${result}" )
    done
}

test_fields_in_group_info()
{
    local cluster=$1 ; shift
    local group_spec=$1 ; shift
    local expected_mode=$1 ; shift
    local expected_state=$1 ; shift
    local expected_is_primary=$1 ; shift

    local fields=(//group/group_name //group/group_id //group/mirroring/mode //group/mirroring/state //group/mirroring/global_id //group/mirroring/primary)
    local fields_arr
    get_fields_from_group_info "${cluster}" "${group_spec}" "run_cmd" fields_arr "${fields[@]}"
    test "${fields_arr[2]}" = "${expected_mode}" || { fail "mode = ${fields_arr[2]}"; return 1; }
    test "${fields_arr[3]}" = "${expected_state}" || { fail "state = ${fields_arr[3]}"; return 1; }
    test "${fields_arr[5]}" = "${expected_is_primary}" || { fail "primary = ${fields_arr[5]}"; return 1; }
}

get_id_from_group_info()
{
    local cluster=$1 ; shift
    local group_spec=$1 ; shift
    local -n _result=$1 ; shift
    local runner=${1:-"run_cmd"}

    local fields=(//group/group_id)
    local fields_arr
    get_fields_from_group_info "${cluster}" "${group_spec}" "${runner}" fields_arr "${fields[@]}" || { fail; return 1; }
    _result="${fields_arr[0]}"
}

get_fields_from_mirror_image_status()
{
    local cluster=$1 ; shift
    local image_spec=$1 ; shift
    local -n _image_result_arr=$1 ; shift
    local fields=("$@")

    run_admin_cmd "rbd --cluster ${cluster} mirror image status ${image_spec} --format xml --pretty-format" || { fail; return 1; }

    local field result
    for field in "${fields[@]}"; do
      result=$(xmlstarlet sel -t -v "$field"  < "$CMD_STDOUT") || { fail "field not found: ${field}"; return; }
      _image_result_arr+=( "${result}" )
    done
}

check_fields_in_group_and_image_status()
{
    local cluster=$1
    local group_spec=$2

    local fields=(//group/state //group/description)
    local group_fields_arr
    get_fields_from_mirror_group_status "${cluster}" "${group_spec}" group_fields_arr "${fields[@]}"

    local image_spec
    for image_spec in $(rbd --cluster "${cluster}" group image list "${group_spec}" | xargs); do
        local fields=(//image/state //image/description)
        local image_fields_arr
        get_fields_from_mirror_image_status "${cluster}" "${image_spec}" image_fields_arr "${fields[@]}"

        # check that the image "state" matches the group "state"
        test "${image_fields_arr[0]}" = "${group_fields_arr[0]}" || { fail "image:${image_spec} ${image_fields_arr[0]} != ${group_fields_arr[0]}"; return 1; } 

        # check that the image "description" matches the group "description".  Need to remove the extra information from the image description first
        local image_description
        image_description=$(cut -d ',' -f 1 <<< "${image_fields_arr[1]}")
        test "${image_description}" = "${group_fields_arr[1]}" || { fail "image:${image_spec} ${image_description} != ${group_fields_arr[1]}"; return 1; } 
    done
}

test_status_in_pool_dir()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state_pattern="$4"
    local description_pattern="$5"
    local service_pattern="$6"

    local status
    status=$(CEPH_ARGS='' rbd --cluster ${cluster} mirror image status \
                 ${pool}/${image})
    grep "^  state: .*${state_pattern}" <<< "$status" || return 1
    grep "^  description: .*${description_pattern}" <<< "$status" || return 1

    if [ -n "${service_pattern}" ]; then
        grep "service: *${service_pattern}" <<< "$status" || return 1
    elif echo ${state_pattern} | grep '^up+'; then
        grep "service: *${MIRROR_USER_ID_PREFIX}.* on " <<< "$status" || return 1
    else
        grep "service: " <<< "$status" && return 1
    fi

    # recheck using `mirror pool status` command to stress test it.
    local last_update
    last_update="$(sed -nEe 's/^  last_update: *(.*) *$/\1/p' <<< "$status")"
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

    local status
    status=$(CEPH_ARGS='' rbd --cluster ${cluster} mirror pool status ${pool} \
                 --verbose --format xml)

    local last_update state
    last_update=$(xmlstarlet sel -t -v \
        "//images/image[name='${image}']/last_update" <<< "$status")
    state=$(xmlstarlet sel -t -v \
        "//images/image[name='${image}']/state" <<< "$status")

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

    run_cmd "rbd --cluster ${cluster} create --size ${size} --image-feature ${RBD_IMAGE_FEATURES} $* ${pool}/${image}"
}

image_create()
{
    local cluster=$1 ; shift
    local image_spec=$1 ; shift
    local size=128

    if [ -n "$1" ]; then
        size=$1
        shift
    fi

    run_cmd "rbd --cluster ${cluster} create --size ${size} --image-feature ${RBD_IMAGE_FEATURES} $* ${image_spec}"
}

images_create()
{
  local cluster=$1 ; shift
  local image_spec=$1 ; shift
  local count=$1 ; shift
  local size=128

  if [ -n "$1" ]; then
    size=$1
    shift
  fi

  local loop_instance
  for loop_instance in $(seq 0 $((count-1))); do
    image_create "${cluster}" "${image_spec}${loop_instance}" "$size" || return 1
  done
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

image_rename()
{
    local cluster=$1
    local src_image_spec=$2
    local dst_image_spec=$3

    run_cmd "rbd --cluster=${cluster} rename ${src_image_spec} ${dst_image_spec}"
}

remove_image()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local runner=${4:-"run_cmd"}

    "$runner" "rbd --cluster=${cluster} snap purge ${pool}/${image}"
    "$runner" "rbd --cluster=${cluster} rm ${pool}/${image}"
}

image_remove()
{
    local cluster=$1
    local image_spec=$2

    run_cmd "rbd --cluster=${cluster} snap purge ${image_spec}"
    run_cmd "rbd --cluster=${cluster} rm ${image_spec}"
}

remove_image_retry()
{
    local cluster=$1
    local pool=$2
    local image=$3

    for s in 0 1 2 4 8 16 32; do
        sleep ${s}
        remove_image ${cluster} ${pool} ${image} "try_cmd" && return 0
    done
    return 1
}

images_remove()
{
  local cluster=$1 ; shift
  local image_prefix=$1 ; shift
  local count=$1 ; shift

  local loop_instance
  for loop_instance in $(seq 0 $((count-1))); do
      image_remove "${cluster}" "${image_prefix}${loop_instance}" || return 1
  done
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

    run_cmd "rbd --cluster ${cluster} snap create ${pool}/${image}@${snap}"
}

remove_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    run_cmd "rbd --cluster ${cluster} snap rm ${pool}/${image}@${snap}"
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

get_snap_count()
{
    local cluster=$1
    local image_spec=$2
    local snap=$3
    local -n _snap_count=$4
    
    run_cmd "rbd --cluster=${cluster} snap ls --all --format xml --pretty-format ${image_spec}"
    if [ "${snap}" = '*' ]; then
        _snap_count="$(xmlstarlet sel -t -v "count(//snapshots/snapshot)" < "$CMD_STDOUT")"
    else
        _snap_count="$(xmlstarlet sel -t -v "count(//snapshots/snapshot[name='${snap}'])" < "$CMD_STDOUT")"
    fi
}

check_snap_doesnt_exist()
{
    local cluster=$1
    local image_spec=$2
    local snap=$3
    local count

    get_snap_count "${cluster}" "${image_spec}" "${snap}" count
    test 0 = "${count}" || { fail "snap count = ${count}"; return 1; }
}

check_snap_exists()
{
    local cluster=$1
    local image_spec=$2
    local snap=$3
    local count

    get_snap_count "${cluster}" "${image_spec}" "${snap}" count
    test 1 = "${count}" || { fail "snap count = ${count}"; return 1; }
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
        grep -F " trash (user ${snap_name})"
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

get_snaps_json()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} snap ls ${pool}/${image} --all --format json
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
    local ret=0
    local local_cluster=$1
    local cluster=$2
    local local_pool=$3
    local remote_pool=$4
    local image=$5

    local rmt_export=${TEMPDIR}/$(mkfname ${cluster}-${remote_pool}-${image}.export)
    local loc_export=${TEMPDIR}/$(mkfname ${local_cluster}-${local_pool}-${image}.export)

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${cluster} export ${remote_pool}/${image} ${rmt_export}
    rbd --cluster ${local_cluster} export ${local_pool}/${image} ${loc_export}
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
                           xmlstarlet sel -t -v "//snapshot/name" | \
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

compare_image_with_snapshot()
{
    local img_cluster=$1 ; shift
    local image_spec=$1 ; shift
    local snap_cluster=$1 ; shift
    local snap_spec=$1 ; shift

    if [ -n "$1" ]; then
        expect_difference=$1 ; shift
    fi

    local ret=0

    local img_export snap_export
    img_export=${TEMPDIR}/$(mkfname ${img_cluster}-${image_spec}.export)
    snap_export=${TEMPDIR}/$(mkfname ${snap_cluster}-${snap_spec}.export)
    rm -f "${img_export}" "${snap_export}"

    rbd --cluster "${img_cluster}" export "${image_spec}" "${img_export}"
    rbd --cluster "${snap_cluster}" export "${snap_spec}" "${snap_export}"

    if ! cmp "${img_export}" "${snap_export}"
    then
        if [ 'true' != "${expect_difference}" ]; then
            show_diff "${img_export}" "${snap_export}"
            ret=1
        fi
    fi
    rm -f "${img_export}" "${snap_export}"
    return "${ret}"
}

compare_image_with_snapshot_expect_difference()
{
    compare_image_with_snapshot "$@" 'true'
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

mirror_image_disable()
{
    local cluster=$1 ; shift
    local image_spec=$1 ; shift

    run_cmd "rbd --cluster=${cluster} mirror image disable $* ${image_spec}"
}

enable_mirror()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local mode=${4:-${RBD_MIRROR_MODE}}

    run_cmd "rbd --cluster=${cluster} mirror image enable ${pool}/${image} ${mode}"
    # Display image info including the global image id for debugging purpose
    run_cmd "rbd --cluster=${cluster} info ${pool}/${image}"
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

test_image_with_global_id_count()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local global_id=$4
    local test_image_count=$5

    run_cmd "rbd --cluster ${cluster} info ${pool}/${image} --format xml --pretty-format"
    test "${test_image_count}" = "$(xmlstarlet sel -t -v "count(//image/mirroring[global_id='${global_id}'])" < "$CMD_STDOUT")" || { fail; return 1; }
}

get_pool_image_count()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local -n _pool_image_count=$4
    
    run_cmd "rbd --cluster ${cluster} ls ${pool} --format xml --pretty-format"
    if [ "${image}" = '*' ]; then
        _pool_image_count="$(xmlstarlet sel -t -v "count(//images/name)" < "$CMD_STDOUT")"
    else
        _pool_image_count="$(xmlstarlet sel -t -v "count(//images[name='${image}'])" < "$CMD_STDOUT")"
    fi
}

test_image_count()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local test_image_count=$4

    local actual_image_count
    get_pool_image_count "${cluster}" "${pool}" "${image}" actual_image_count
    test "${test_image_count}" = "${actual_image_count}" || { fail; return 1; }
}

wait_for_pool_image_count()
{
    local cluster=$1
    local pool=$2
    local count=$3
    local s

    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32; do
        sleep ${s}
        test_image_count "${cluster}" "${pool}" '*' "${count}" && return 0
    done
    return 1
}

test_image_with_global_id_not_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local global_id=$4

    # if the image is not listed in the pool then no need to check the global id
    test_image_count "${cluster}" "${pool}" "${image}" 0 && return 0;

    test_image_with_global_id_count "${cluster}" "${pool}" "${image}" "${global_id}" 0 || { fail "image present"; return 1; }
}

test_image_with_global_id_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local global_id=$4

    # if the image is not listed in the pool then no need to check the global id
    test_image_count "${cluster}" "${pool}" "${image}" 1 || return 1;

    test_image_with_global_id_count "${cluster}" "${pool}" "${image}" "${global_id}" 1
}

test_image_not_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local image_id=$4

    test_image_present "${cluster}" "${pool}" "${image}" 'deleted' "${image_id}" 
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

get_image_id2()
{
    local cluster=$1
    local image_spec=$2
    local -n _id=$3

    run_cmd "rbd --cluster ${cluster} info ${image_spec} --format xml --pretty-format"
    _id=$(xmlstarlet sel -t -v "//image/id" "$CMD_STDOUT") || { fail "no id!"; return; }
}

get_image_mirroring_global_id()
{
    local cluster=$1
    local image_spec=$2
    local -n _global_id=$3

    run_cmd "rbd --cluster ${cluster} info ${image_spec} --format xml --pretty-format"
    _global_id=$(xmlstarlet sel -t -v "//image/mirroring/global_id" "$CMD_STDOUT") || { fail "not mirrored"; return; }
}

image_resize()
{
    local cluster=$1 ; shift
    local image_spec=$1 ; shift
    local size=$1 ; shift

    run_cmd "rbd --cluster ${cluster} resize --image ${image_spec} --size ${size} $*"
}

get_image_size()
{
    local cluster=$1
    local image_spec=$2
    local -n _size=$3

    run_cmd "rbd --cluster ${cluster} info ${image_spec} --format xml --pretty-format"
    _size=$(xmlstarlet sel -t -v "//image/size" "$CMD_STDOUT") || { fail "unable to determine size"; return; }
}

test_image_size_matches()
{
    local cluster=$1
    local image_spec=$2
    local test_size=$3

    local current_size
    get_image_size "${cluster}" "${image_spec}" current_size
    test "${current_size}" = "${test_size}" || { fail; return 1; }
}

wait_for_image_size_matches()
{
    local cluster=$1
    local image_spec=$2
    local test_size=$3
    local s

    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32; do
        sleep ${s}
        test_image_size_matches "${cluster}" "${image_spec}" "${test_size}" && return 0
    done
    fail "size never matched"; return 1
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

count_omap_keys_with_filter()
{
    local cluster=$1
    local pool=$2
    local obj_name=$3
    local filter=$4
    local -n _count=$5

    # listomapkeys fails if the object doesn't exist, assume 0 keys
    try_cmd "rados --cluster ${cluster} -p ${pool} listomapkeys ${obj_name}" || :
    _count=$(grep -c "${filter}" "$CMD_STDOUT") || return 0
}

wait_for_omap_keys()
{
    local cluster=$1
    local pool=$2
    local obj_name=$3
    local filter=$4

    local key_count
    for s in 0 1 2 2 4 4 8 8 8 16 16 32; do
        sleep $s
        count_omap_keys_with_filter ${cluster} ${pool} ${obj_name} ${filter} key_count
        test "${key_count}" = 0 && return 0
    done
    fail "wait for count of keys 0 failed on ${cluster}.  Actual count=${key_count}"
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

group_create()
{
    local cluster=$1
    local group_spec=$2

    run_cmd "rbd --cluster ${cluster} group create ${group_spec}"
}

group_rename()
{
    local cluster=$1
    local current_name=$2
    local new_name=$3

    run_cmd "rbd --cluster ${cluster} group rename ${current_name} ${new_name}"
}

group_remove()
{
    local cluster=$1
    local group_spec=$2

    run_cmd "rbd --cluster ${cluster} group remove ${group_spec}"
}

wait_for_group_synced()
{
    local cluster=$1
    local group_spec=$2
    local secondary_cluster=$3
    local secondary_group_spec=$4
    local group_snap_id
    
    get_newest_group_snapshot_id "${cluster}" "${group_spec}" group_snap_id
    wait_for_group_snap_present "${secondary_cluster}" "${secondary_group_spec}" "${group_snap_id}"
    wait_for_group_snap_sync_complete "${secondary_cluster}" "${secondary_group_spec}" "${group_snap_id}"
}

group_image_add()
{
    local cluster=$1
    local group_spec=$2
    local image_spec=$3

    run_cmd "rbd --cluster ${cluster} group image add ${group_spec} ${image_spec}"
}

group_images_add()
{
    local cluster=$1
    local group_spec=$2
    local image_prefix=$3
    local count=$4

    local loop_instance
    for loop_instance in $(seq 0 $((count-1))); do
      group_image_add ${cluster} ${group_spec} ${image_prefix}${loop_instance}
    done
}

group_image_remove()
{
    local cluster=$1
    local group_spec=$2
    local image_spec=$3

    run_cmd "rbd --cluster ${cluster} group image remove ${group_spec} ${image_spec}"
}

group_images_remove()
{
    local cluster=$1
    local group_spec=$2
    local image_prefix=$3
    local count=$4

    local loop_instance
    for loop_instance in $(seq 0 $((count-1))); do
      group_image_remove ${cluster} ${group_spec} ${image_prefix}${loop_instance}
    done
}

mirror_group_enable()
{
    local cluster=$1
    local group_spec=$2
    local mode=${3:-${RBD_MIRROR_MODE}}
    local runner=${4:-"run_cmd"}

    "$runner" "rbd --cluster=${cluster} mirror group enable ${group_spec} ${mode}"
}

mirror_group_enable_try()
{
    local mode=${3:-${RBD_MIRROR_MODE}}
    mirror_group_enable "$@" "${mode}" "try_cmd"
}

mirror_group_disable()
{
    local cluster=$1 ; shift
    local group_spec=$1 ; shift

    local force
    if [ -n "$1" ]; then
        force=$1; shift
    fi

    run_cmd "rbd --cluster=${cluster} mirror group disable $* ${group_spec} ${force}"
}

create_group_and_enable_mirror()
{
    local cluster=$1
    local group_spec=$2
    local mode=${3:-${RBD_MIRROR_MODE}}

    group_create ${cluster} ${group_spec}
    mirror_group_enable ${cluster} ${group_spec} ${mode}
}

mirror_group_demote()
{
    local cluster=$1
    local group_spec=$2

    run_cmd "rbd --cluster=${cluster} mirror group demote ${group_spec}"
}

mirror_group_promote()
{
    local cluster=$1
    local group_spec=$2
    local force=$3
    local runner=${4:-"run_cmd"}

    "$runner" "rbd --cluster=${cluster} mirror group promote ${group_spec} ${force}"
}

mirror_group_promote_try()
{
    local force=${3:-''}

    mirror_group_promote "$@" "${force}" "try_cmd"
}

mirror_group_snapshot()
{
    local cluster=$1
    local group_spec=$2

    run_cmd "rbd --cluster=${cluster} mirror group snapshot ${group_spec}" || return 1

    if [ "$#" -gt 2 ]
    then
      local -n _group_snap_id=$3
      _group_snap_id=$( awk -F": " '/Snapshot ID:/ {print $2}' "$CMD_STDOUT" )
    fi
}

group_snap_create()
{
    local cluster=$1
    local group_spec=$2
    local snap=$3

    run_cmd "rbd --cluster=${cluster} group snap create ${group_spec}@${snap}"
}

group_snap_remove()
{
    local cluster=$1
    local group_spec=$2
    local snap=$3

    run_cmd "rbd --cluster=${cluster} group snap rm ${group_spec}@${snap}"
}

get_group_snap_count()
{
    local cluster=$1
    local group_spec=$2
    local snap=$3
    local -n _group_snap_count=$4
    
    run_cmd "rbd --cluster=${cluster} group snap ls --format xml --pretty-format ${group_spec}"
    if [ "${snap}" = '*' ]; then
        _group_snap_count="$(xmlstarlet sel -t -v "count(//group_snaps/group_snap)" < "$CMD_STDOUT")"
    else
        _group_snap_count="$(xmlstarlet sel -t -v "count(//group_snaps/group_snap[snapshot='${snap}'])" < "$CMD_STDOUT")"
    fi
}

get_group_snap_name()
{
    local cluster=$1
    local group_spec=$2
    local snap_id=$3
    local -n _group_snap_name=$4

    run_cmd "rbd --cluster=${cluster} group snap ls --format xml --pretty-format ${group_spec}"
    _group_snap_name="$(xmlstarlet sel -t -v "//group_snaps/group_snap[id='${snap_id}']/snapshot" < "$CMD_STDOUT")"
}

get_pool_count()
{
    local cluster=$1
    local pool_name=$2
    local -n _count=$3

    run_cmd "ceph --cluster ${cluster} osd pool ls --format xml-pretty"
    if [ "${pool_name}" = '*' ]; then
        _count="$(xmlstarlet sel -t -v "count(//pools/pool_name)" < "$CMD_STDOUT")"
    else
        _count="$(xmlstarlet sel -t -v "count(//pools[pool_name='${pool_name}'])" < "$CMD_STDOUT")"
    fi
}

get_pool_obj_count()
{
    local cluster=$1
    local pool=$2
    local obj_name=$3
    local -n _count=$4

    run_cmd "rados --cluster ${cluster} -p ${pool} ls --format xml-pretty"
    _count="$(xmlstarlet sel -t -v "count(//objects/object[name='${obj_name}'])" < "$CMD_STDOUT")"
}

get_image_snap_id_from_group_snap_info()
{
    local cluster=$1
    local snap_spec=$2
    local image_spec=$3
    local -n _image_snap_id=$4

    run_cmd "rbd --cluster=${cluster} group snap info --format xml --pretty-format ${snap_spec}"
    local image_name
    image_name=$(echo "${image_spec}" | awk -F'/' '{print $NF}')
    _image_snap_id="$(xmlstarlet sel -t -v "//group_snapshot/images/image[image_name='${image_name}']/snap_id" < "$CMD_STDOUT")"
}

get_images_from_group_snap_info()
{
    local cluster=$1
    local snap_spec=$2
    local -n _images=$3

    run_cmd "rbd --cluster=${cluster} group snap info --format xml --pretty-format ${snap_spec}"
    # sed script removes extra path delimiter if namespace field is blank
    _images="$(xmlstarlet sel -t -m "//group_snapshot/images/image" -v "pool_name" -o "/" -v "namespace" -o "/" -v "image_name" -o " " < "$CMD_STDOUT" |  sed s/"\/\/"/"\/"/g )"
}

get_image_snap_complete()
{
    local cluster=$1
    local image_spec=$2
    local snap_id=$3
    local -n _is_complete=$4
    run_cmd "rbd --cluster=${cluster} snap list --all --format xml --pretty-format ${image_spec}"
    _is_complete="$(xmlstarlet sel -t -v "//snapshots/snapshot[id='${snap_id}']/namespace/complete" < "$CMD_STDOUT")"
}

check_group_snap_doesnt_exist()
{
    local cluster=$1
    local group_spec=$2
    local snap=$3
    local count

    get_group_snap_count "${cluster}" "${group_spec}" "${snap}" count
    test 0 = "${count}" || { fail "snap count = ${count}"; return 1; }
}

check_group_snap_exists()
{
    local cluster=$1
    local group_spec=$2
    local snap=$3
    local count

    get_group_snap_count "${cluster}" "${group_spec}" "${snap}" count
    test 1 = "${count}" || { fail "snap count = ${count}"; return 1; }
}

mirror_group_resync()
{
    local cluster=$1
    local group_spec=$2

    run_cmd "rbd --cluster=${cluster} mirror group resync ${group_spec}"
}

test_group_present()
{
    local cluster=$1
    local pool=$2
    local group=$3
    local test_group_count=$4
    local test_image_count=$5
    local current_image_count

    run_cmd "rbd --cluster ${cluster} group list ${pool} --format xml --pretty-format" || { fail; return 1; }
    test "${test_group_count}" = "$(xmlstarlet sel -t -v "count(//groups[name='${group}'])" < "$CMD_STDOUT")" || { fail; return 1; }

    # if the group is not expected to be present in the list then don't bother checking for images
    test "${test_group_count}" = 0 && return 0

    run_cmd "rbd --cluster ${cluster} group image list ${pool}/${group}"
    current_image_count=$(wc -l < "$CMD_STDOUT")
    test "${test_image_count}" = "${current_image_count}" || { fail; return 1; }
}

wait_for_test_group_present()
{
    local cluster=$1
    local pool=$2
    local group=$3
    local test_group_count=$4
    local image_count=$5
    local s

    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32; do
        sleep ${s}
        test_group_present \
            "${cluster}" "${pool}" "${group}" "${test_group_count}" "${image_count}" &&
        return 0
    done
    fail "wait for count of groups with name ${group} to be ${test_group_count} failed on ${cluster}"
    return 1
}

wait_for_group_present()
{
    local cluster=$1
    local pool=$2
    local group=$3
    local image_count=$4

    wait_for_test_group_present "${cluster}" "${pool}" "${group}" 1 "${image_count}"
}

wait_for_group_not_present()
{
    local cluster=$1
    local pool=$2
    local group=$3

    wait_for_test_group_present "${cluster}" "${pool}" "${group}" 0 0
}

test_group_id_changed()
{
    local cluster=$1
    local group_spec=$2
    local orig_group_id=$3    
    local current_group_id

    get_id_from_group_info "${cluster}" "${group_spec}" current_group_id "try_cmd" || { fail; return 1; }
    test "${orig_group_id}" != "${current_group_id}" || { fail; return 1; }
  }

wait_for_group_id_changed()
{
    local cluster=$1
    local group_spec=$2
    local orig_group_id=$3
    local s

    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32; do
        sleep ${s}
        test_group_id_changed "${cluster}" "${group_spec}" "${orig_group_id}" && return 0
    done
    fail "wait for group with name ${group} to change id from ${orig_group_id} failed on ${cluster}"
    return 1
}

test_group_snap_present()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3
    local expected_snap_count=$4

    run_cmd "rbd --cluster ${cluster} group snap list ${group_spec} --format xml --pretty-format" 

    test "${expected_snap_count}" = "$(xmlstarlet sel -t -v "count(//group_snaps/group_snap[id='${group_snap_id}'])" < "$CMD_STDOUT")" || { fail; return 1; }
}

wait_for_test_group_snap_present()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3
    local test_group_snap_count=$4
    local s

    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32; do
        sleep ${s}
        test_group_snap_present "${cluster}" "${group_spec}" "${group_snap_id}" "${test_group_snap_count}" && return 0
    done

    fail "wait for count of group snaps with id ${group_snap_id} to be ${test_group_snap_count} failed on ${cluster}"
    return 1
}

wait_for_group_snap_present()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3

    wait_for_test_group_snap_present "${cluster}" "${group_spec}" "${group_snap_id}" 1
}

wait_for_group_snap_not_present()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3

    wait_for_test_group_snap_present "${cluster}" "${group_spec}" "${group_snap_id}" 0
}

test_group_snap_sync_state()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3
    local expected_state=$4

    run_cmd "rbd --cluster ${cluster} group snap list ${group_spec} --format xml --pretty-format" 

    test "${expected_state}" = "$(xmlstarlet sel -t -v "//group_snaps/group_snap[id='${group_snap_id}']/state" < "$CMD_STDOUT")" || { fail; return 1; }
}

test_group_snap_sync_complete()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3

    test_group_snap_sync_state "${cluster}" "${group_spec}" "${group_snap_id}" 'complete'
}

test_group_snap_sync_incomplete()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3

    test_group_snap_sync_state "${cluster}" "${group_spec}" "${group_snap_id}" 'incomplete'
}

list_image_snaps_for_group()
{
    local cluster=$1
    local group_spec=$2

    try_cmd "rbd --cluster ${cluster} group image list ${group_spec}"
    for image_spec in $(cat "$CMD_STDOUT" | xargs); do
        try_cmd "rbd --cluster ${cluster} snap list -a ${image_spec}" || :
    done
}

wait_for_test_group_snap_sync_complete()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3
    local s

    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16 32 32 32 32 64 64 64 64 64; do
        sleep ${s}
        test_group_snap_sync_complete "${cluster}" "${group_spec}" "${group_snap_id}" && return 0

        if  (( $(bc <<<"$s > 32") )); then
            # query the snap progress for each image in the group - debug info to check that sync is progressing
            list_image_snaps_for_group "${cluster}" "${group_spec}"
        fi

    done

    fail "wait for group snap with id ${group_snap_id} to be synced failed on ${cluster}"
    return 1
}

wait_for_group_snap_sync_complete()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3

    wait_for_test_group_snap_sync_complete "${cluster}" "${group_spec}" "${group_snap_id}"
}

test_group_replay_state()
{
    local cluster=$1
    local group_spec=$2
    local test_state=$3
    local image_count=$4
    local current_state=stopped
    local actual_image_count
    local started_image_count

    # Query the state from the rbd-mirror daemon directly
    admin_daemons "${cluster}" rbd mirror group status "${group_spec}" --format xml-pretty || { fail; return 1; }
    test "Replaying" = "$(xmlstarlet sel -t -v "//group_replayer/state" < "$CMD_STDOUT" )" && current_state=started
    # from GroupReplayer.h, valid states are Starting, Replaying, Stopping, Stopped
    test "${test_state}" = "${current_state}" || { fail; return 1; }
    if [ -n "${image_count}" ]; then
      actual_image_count=$(xmlstarlet sel -t -v "count(//image_replayer/state)"  < "$CMD_STDOUT")
      started_image_count=$(xmlstarlet sel -t -v "count(//image_replayer[state='Replaying'])" < "$CMD_STDOUT")
      test "${image_count}" = "${actual_image_count}" ||  { fail; return 1; }

      # If the group is started then check that all images are started too
      test "${test_state}" = "started" || return 0
      test "${image_count}" = "${started_image_count}" ||  { fail; return 1; }
    fi
}

test_group_replay_state_cli()
{
    local cluster=$1
    local group_spec=$2
    local test_state=$3
    local image_count=$4
    local current_state=stopped
    local actual_image_count
    local started_image_count

    # need to use "try" here because the command can fail if the group is not yet present on the remote cluster
    try_admin_cmd "rbd --cluster ${cluster} mirror group status ${group_spec} --format xml --pretty-format" || { fail; return 1; }
    xmlstarlet sel -Q -t -v "//group/state[contains(text(), ${test_state})]" "${CMD_STDOUT}" || { fail; return 1; }

    if [ -n "${image_count}" ]; then
      actual_image_count=$(xmlstarlet sel -t -v "count(//group/images/image)"  < "$CMD_STDOUT")
      started_image_count=$(xmlstarlet sel -t -v "count(//group/images/image/status[contains(text(), ${test_state})])" < "$CMD_STDOUT")
      test "${image_count}" = "${actual_image_count}" ||  { fail; return 1; }

      # If the group is started then check that all images are started too
      test "${test_state}" = "started" || return 0
      test "${image_count}" = "${started_image_count}" ||  { fail; return 1; }
    fi
}

query_replayer_assignment()
{
    local cluster=$1
    local instance=$2
    local -n _result=$3

    local group_replayers
    local image_replayers
    local group_replayers_count
    local image_replayers_count

    admin_daemon "${cluster}:${instance}" rbd mirror status --format xml-pretty || { fail; return 1; }
    group_replayers=$(xmlstarlet sel -t -v "//mirror_status/pool_replayers/pool_replayer_status/group_replayers/group_replayer/name" < "$CMD_STDOUT") || { group_replayers=''; }
    image_replayers=$(xmlstarlet sel -t -v "//mirror_status/pool_replayers/pool_replayer_status/group_replayers/group_replayer/image_replayers/image_replayer/name" < "$CMD_STDOUT") || { image_replayers=''; }
    group_replayers_count=$(xmlstarlet sel -t -v "count(//mirror_status/pool_replayers/pool_replayer_status/group_replayers/group_replayer/name)" < "$CMD_STDOUT") || { group_replayers_count='0'; }
    image_replayers_count=$(xmlstarlet sel -t -v "count(//mirror_status/pool_replayers/pool_replayer_status/group_replayers/group_replayer/image_replayers/image_replayer/name)" < "$CMD_STDOUT") || { image_replayers_count='0'; }
    _result=("${group_replayers}" "${image_replayers}" "${group_replayers_count}" "${image_replayers_count}")
}

wait_for_group_replay_state()
{
    local cluster=$1
    local group_spec=$2
    local state=$3
    local image_count=$4
    local asok_query=$5
    local s

    # TODO: add a way to force rbd-mirror to update replayers
    for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16; do
        sleep ${s}
        if [ 'true' = "${asok_query}" ]; then
            test_group_replay_state "${cluster}" "${group_spec}" "${state}" && return 0
        else
            test_group_replay_state_cli "${cluster}" "${group_spec}" "${state}" "${image_count}" && return 0
        fi
    done
    fail "Failed to reach expected state"
    return 1
}

wait_for_group_replay_started()
{
    local cluster=$1
    local group_spec=$2
    local image_count=$3

    # Query the state via daemon socket and also via the cli to confirm that they agree
    wait_for_group_replay_state "${cluster}" "${group_spec}" 'started' "${image_count}" 'true'
    wait_for_group_replay_state "${cluster}" "${group_spec}" 'replaying' "${image_count}" 'false'
}

wait_for_group_replay_stopped()
{
    local cluster=$1
    local group_spec=$2
    local image_count=$3

    # Image_count will be 0 if the group is stopped except when the group is primary
    # Query the state via daemon socket and also via the cli.
    # The admin socket status does not include image information
    wait_for_group_replay_state "${cluster}" "${group_spec}" 'stopped' 0 'true'
    wait_for_group_replay_state "${cluster}" "${group_spec}" 'stopped' "${image_count}" 'false'
}

get_newest_group_snapshot_id()
{
    local cluster=$1
    local group_spec=$2
    local -n _group_snap_id=$3

    run_cmd "rbd --cluster ${cluster} group snap list ${group_spec} --format xml --pretty-format" 
    _group_snap_id=$(xmlstarlet sel -t -v "(//group_snaps/group_snap[state='complete']/id)[last()]" "$CMD_STDOUT" ) && return 0

    fail "Failed to get snapshot id"
    return 1
}

mirror_group_snapshot_and_wait_for_sync_complete()
{
    local secondary_cluster=$1
    local primary_cluster=$2
    local group_spec=$3
    local group_snap_id

    mirror_group_snapshot "${primary_cluster}" "${group_spec}" group_snap_id
    wait_for_group_snap_present "${secondary_cluster}" "${group_spec}" "${group_snap_id}"
    wait_for_group_snap_sync_complete "${secondary_cluster}" "${group_spec}" "${group_snap_id}"
}

test_group_synced_image_status()
{
    local cluster=$1
    local group_spec=$2
    local group_snap_id=$3
    local expected_synced_image_count=$4

    local group_snap_name
    get_group_snap_name "${cluster}" "${group_spec}" "${group_snap_id}" group_snap_name

    local images
    get_images_from_group_snap_info "${cluster}" "${group_spec}@${group_snap_name}" images

    local image_count=0
    local image_spec
    for image_spec in ${images}; do

        # get the snap_id for this image from the group snap info
        local image_snap_id
        get_image_snap_id_from_group_snap_info "${cluster}" "${group_spec}@${group_snap_name}" "${image_spec}" image_snap_id
            
        # get the value in the "complete" field for the image and snap_id 
        local is_complete
        get_image_snap_complete "${cluster}" "${image_spec}" "${image_snap_id}" is_complete

        test "${is_complete}" != "true" && { fail "image ${image_spec} is not synced"; return 1; }

        image_count=$((image_count+1))
    done

    test "${image_count}" != "${expected_synced_image_count}" && fail "unexpected count ${image_count} != ${expected_synced_image_count}"

    return 0
}

test_images_in_latest_synced_group()
{
    local cluster=$1
    local group_spec=$2
    local expected_synced_image_count=$3

    local group_snap_id
    get_newest_group_snapshot_id "${cluster}" "${group_spec}" group_snap_id
    test_group_synced_image_status "${cluster}" "${group_spec}" "${group_snap_id}" "${expected_synced_image_count}"
}

test_group_status_in_pool_dir()
{
    local cluster=$1
    local group_spec=$2
    local state_pattern=$3
    local image_count=$4
    local description_pattern=$5
    local current_state=stopped

    run_admin_cmd "rbd --cluster ${cluster} mirror group status ${group_spec} --format xml --pretty-format" || { fail; return 1; }

    test -n "${state_pattern}" && { test "${state_pattern}" = $(xmlstarlet sel -t -v "//group/state" < "${CMD_STDOUT}" ) || { fail; return 1; } }
    test -n "${description_pattern}" && { test "${description_pattern}" = "$(xmlstarlet sel -t -v "//group/description" "${CMD_STDOUT}" )" || { fail; return 1; } }

    if echo ${state_pattern} | grep '^up+' >/dev/null; then
        xmlstarlet sel -Q -t -v "//group/daemon_service/daemon_id[contains(text(), ${MIRROR_USER_ID_PREFIX})]" "${CMD_STDOUT}" || { fail; return 1; }
    else
        xmlstarlet sel -Q -t -v "//group/daemon_service/daemon_id" "${CMD_STDOUT}" && { fail; return 1; }
    fi

    test "Replaying" = "$(xmlstarlet sel -t -v "//group_replayer/state" < "$CMD_STDOUT" )" && current_state=started
    if [ -n "${image_count}" ]; then
        actual_image_count=$(xmlstarlet sel -t -v "count(/group/images/image/status)" "$CMD_STDOUT")
        started_image_count=$(xmlstarlet sel -t -v "count(/group/images/image/status[contains(text(), 'replaying')])" "$CMD_STDOUT")

        test "${image_count}" = "${actual_image_count}" || { fail; return 1; }

        # If the group is started then check that all images are started too
        if [ "${current_state}" = "started" ]; then
            test "${image_count}" = "${started_image_count}" ||  { fail; return 1; }
        fi
    fi

    # TODO enable this once there is more coordination between the group and image replayer to ensure that the state is in sync
    #check_fields_in_group_and_image_status "${cluster}" "${group_spec}" ||  { fail; return 1; }
    
    return 0
}

wait_for_group_status_in_pool_dir()
{
    local cluster=$1
    local group_spec=$2
    local state_pattern=$3
    local image_count=$4
    local description_pattern=$5

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
        sleep ${s}
        test_group_status_in_pool_dir ${cluster} ${group_spec} \
            "${state_pattern}" "${image_count}" "${description_pattern}" &&
            return 0
    done
    fail "failed to reach expected status"
    return 1
}

test_peer_group_status_in_pool_dir()
{
    local cluster=$1
    local group_spec=$2
    local state_pattern=$3
    local description_pattern=$4

    local fields=(//group/peer_sites/peer_site/state //group/peer_sites/peer_site/description)
    local group_fields_arr
    get_fields_from_mirror_group_status "${cluster}" "${group_spec}" group_fields_arr "${fields[@]}"

    test "${state_pattern}" = "${group_fields_arr[0]}" || { fail; return 1; } 
    if [ -n "${description_pattern}" ]; then
        test "${description_pattern}" = "${group_fields_arr[1]}" || { fail; return 1; } 
    fi
}

wait_for_peer_group_status_in_pool_dir()
{
    local cluster=$1
    local group_spec=$2
    local state_pattern=$3
    local description_pattern=$4

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
        sleep ${s}
        test_peer_group_status_in_pool_dir "${cluster}" "${group_spec}" "${state_pattern}" "${description_pattern}" && return 0
    done
    fail "failed to reach expected peer status"
    return 1
}

stop_daemons_on_clusters()
{
    local cluster_list=$1
    local cluster

    for cluster in ${cluster_list}; do
        echo 'cluster:'${cluster}
        stop_mirrors ${cluster} '-9'
    done
}

delete_pools_on_clusters()
{
    local cluster_list=$1
    local cluster

    for cluster in ${cluster_list}; do
        echo 'cluster:'${cluster}
        for pool in $(CEPH_ARGS='' ceph --cluster ${cluster} osd pool ls  | grep -v "^\." | xargs); do
            echo 'pool:'${pool}
             run_admin_cmd "ceph --cluster ${cluster} osd pool delete ${pool} ${pool} --yes-i-really-really-mean-it"
        done
    done        
}

# stops all daemons and deletes all pools (groups and images included)
tidy()
{
    local primary_cluster=cluster2
    local secondary_cluster=cluster1

    stop_daemons_on_clusters "${primary_cluster} ${secondary_cluster}"
    delete_pools_on_clusters "${primary_cluster} ${secondary_cluster}"
}

# list all groups, images and snaps
list()
{
    local primary_cluster=cluster2
    local secondary_cluster=cluster1
    local cluster pool group group_spec image image_spec

    for cluster in ${primary_cluster} ${secondary_cluster}; do
        echo 'cluster:'${cluster}
        for pool in "${POOL}" "${PARENT_POOL}" "${POOL}/${NS1}" "${POOL}/${NS2}"; do
            echo 'groups in pool:'"${pool}"
            try_cmd "rbd --cluster ${cluster} group list ${pool}" || :
            for group in $(rbd --cluster ${cluster} group list ${pool} | xargs); do
                group_spec=${pool}/${group}
                echo 'snaps for group:'"${group_spec}"
                try_cmd "rbd --cluster ${cluster} group snap list ${group_spec}" || :
                echo 'images in group:'"${group_spec}"
                try_cmd "rbd --cluster ${cluster} group image list ${group_spec}" || :
            done
            echo 'images in pool:'"${pool}"
            try_cmd "rbd --cluster ${cluster} list ${pool}" || :
            for image in $(rbd --cluster ${cluster} list ${pool} | xargs); do
                image_spec=${pool}/${image}
                echo 'snaps for image:'"${image_spec}"
                try_cmd "rbd --cluster ${cluster} snap list -a ${image_spec}" || :
            done
        done
    done
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
    "$@"
    exit $?
fi
