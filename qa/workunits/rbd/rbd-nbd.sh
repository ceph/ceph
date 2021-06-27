#!/usr/bin/env bash
set -ex

. $(dirname $0)/../../standalone/ceph-helpers.sh

POOL=rbd
ANOTHER_POOL=new_default_pool$$
NS=ns
IMAGE=testrbdnbd$$
SIZE=64
DATA=
DEV=

_sudo()
{
    local cmd

    if [ `id -u` -eq 0 ]
    then
	"$@"
	return $?
    fi

    # Look for the command in the user path. If it fails run it as is,
    # supposing it is in sudo path.
    cmd=`which $1 2>/dev/null` || cmd=$1
    shift
    sudo -nE "${cmd}" "$@"
}

setup()
{
    local ns x

    if [ -e CMakeCache.txt ]; then
	# running under cmake build dir

	CEPH_SRC=$(readlink -f $(dirname $0)/../../../src)
	CEPH_ROOT=${PWD}
	CEPH_BIN=${CEPH_ROOT}/bin

	export LD_LIBRARY_PATH=${CEPH_ROOT}/lib:${LD_LIBRARY_PATH}
	export PYTHONPATH=${PYTHONPATH}:${CEPH_SRC}/pybind:${CEPH_ROOT}/lib/cython_modules/lib.3
	PATH=${CEPH_BIN}:${PATH}
    fi

    _sudo echo test sudo

    trap cleanup INT TERM EXIT
    TEMPDIR=`mktemp -d`
    DATA=${TEMPDIR}/data
    dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}

    rbd namespace create ${POOL}/${NS}

    for ns in '' ${NS}; do
        rbd --dest-pool ${POOL} --dest-namespace "${ns}" --no-progress import \
            ${DATA} ${IMAGE}
    done

    # create another pool
    ceph osd pool create ${ANOTHER_POOL} 8
    rbd pool init ${ANOTHER_POOL}
}

function cleanup()
{
    local ns s

    set +e

    mount | fgrep ${TEMPDIR}/mnt && _sudo umount -f ${TEMPDIR}/mnt

    rm -Rf ${TEMPDIR}
    if [ -n "${DEV}" ]
    then
	_sudo rbd device --device-type nbd unmap ${DEV}
    fi

    for ns in '' ${NS}; do
        if rbd -p ${POOL} --namespace "${ns}" status ${IMAGE} 2>/dev/null; then
	    for s in 0.5 1 2 4 8 16 32; do
	        sleep $s
	        rbd -p ${POOL} --namespace "${ns}" status ${IMAGE} |
                    grep 'Watchers: none' && break
	    done
	    rbd -p ${POOL} --namespace "${ns}" snap purge ${IMAGE}
	    rbd -p ${POOL} --namespace "${ns}" remove ${IMAGE}
        fi
    done
    rbd namespace remove ${POOL}/${NS}

    # cleanup/reset default pool
    rbd config global rm global rbd_default_pool
    ceph osd pool delete ${ANOTHER_POOL} ${ANOTHER_POOL} --yes-i-really-really-mean-it
}

function expect_false()
{
  if "$@"; then return 1; else return 0; fi
}

function get_pid()
{
    local pool=$1
    local ns=$2

    PID=$(rbd device --device-type nbd --format xml list | $XMLSTARLET sel -t -v \
      "//devices/device[pool='${pool}'][namespace='${ns}'][image='${IMAGE}'][device='${DEV}']/id")
    test -n "${PID}" || return 1
    ps -p ${PID} -C rbd-nbd
}

unmap_device()
{
    local dev=$1
    local pid=$2

    _sudo rbd device --device-type nbd unmap ${dev}
    rbd device --device-type nbd list | expect_false grep "^${pid}\\b" || return 1
    ps -C rbd-nbd | expect_false grep "^ *${pid}\\b" || return 1

    # workaround possible race between unmap and following map
    sleep 0.5
}

#
# main
#

setup

# exit status test
expect_false rbd-nbd
expect_false rbd-nbd INVALIDCMD
if [ `id -u` -ne 0 ]
then
    expect_false rbd device --device-type nbd map ${IMAGE}
fi
expect_false _sudo rbd device --device-type nbd map INVALIDIMAGE
expect_false _sudo rbd-nbd --device INVALIDDEV map ${IMAGE}

# list format test
expect_false rbd device --device-type nbd --format INVALID list
rbd device --device-type nbd --format json --pretty-format list
rbd device --device-type nbd --format xml list

# map test using the first unused device
DEV=`_sudo rbd device --device-type nbd map ${POOL}/${IMAGE}`
get_pid ${POOL}
# map test specifying the device
expect_false _sudo rbd-nbd --device ${DEV} map ${POOL}/${IMAGE}
dev1=${DEV}
unmap_device ${DEV} ${PID}
DEV=
# XXX: race possible when the device is reused by other process
DEV=`_sudo rbd-nbd --device ${dev1} map ${POOL}/${IMAGE}`
[ "${DEV}" = "${dev1}" ]
rbd device --device-type nbd list | grep "${IMAGE}"
get_pid ${POOL}

# read test
[ "`dd if=${DATA} bs=1M | md5sum`" = "`_sudo dd if=${DEV} bs=1M | md5sum`" ]

# write test
dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}
_sudo dd if=${DATA} of=${DEV} bs=1M oflag=direct
[ "`dd if=${DATA} bs=1M | md5sum`" = "`rbd -p ${POOL} --no-progress export ${IMAGE} - | md5sum`" ]

# trim test
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -eq "${provisioned}" ]
_sudo mkfs.ext4 -E discard ${DEV} # better idea?
sync
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -lt "${provisioned}" ]

# resize test
devname=$(basename ${DEV})
blocks=$(awk -v dev=${devname} '$4 == dev {print $3}' /proc/partitions)
test -n "${blocks}"
rbd resize ${POOL}/${IMAGE} --size $((SIZE * 2))M
rbd info ${POOL}/${IMAGE}
blocks2=$(awk -v dev=${devname} '$4 == dev {print $3}' /proc/partitions)
test -n "${blocks2}"
test ${blocks2} -eq $((blocks * 2))
rbd resize ${POOL}/${IMAGE} --allow-shrink --size ${SIZE}M
blocks2=$(awk -v dev=${devname} '$4 == dev {print $3}' /proc/partitions)
test -n "${blocks2}"
test ${blocks2} -eq ${blocks}

# read-only option test
unmap_device ${DEV} ${PID}
DEV=`_sudo rbd --device-type nbd map --read-only ${POOL}/${IMAGE}`
PID=$(rbd device --device-type nbd list | awk -v pool=${POOL} -v img=${IMAGE} -v dev=${DEV} \
    '$2 == pool && $3 == img && $5 == dev {print $1}')
test -n "${PID}"
ps -p ${PID} -C rbd-nbd

_sudo dd if=${DEV} of=/dev/null bs=1M
expect_false _sudo dd if=${DATA} of=${DEV} bs=1M oflag=direct
unmap_device ${DEV} ${PID}

# exclusive option test
DEV=`_sudo rbd --device-type nbd map --exclusive ${POOL}/${IMAGE}`
get_pid ${POOL}

_sudo dd if=${DATA} of=${DEV} bs=1M oflag=direct
expect_false timeout 10 \
	rbd bench ${IMAGE} --io-type write --io-size=1024 --io-total=1024
unmap_device ${DEV} ${PID}
DEV=
rbd bench ${IMAGE} --io-type write --io-size=1024 --io-total=1024

# unmap by image name test
DEV=`_sudo rbd device --device-type nbd map ${POOL}/${IMAGE}`
get_pid ${POOL}
unmap_device ${IMAGE} ${PID}
DEV=

# map/unmap snap test
rbd snap create ${POOL}/${IMAGE}@snap
DEV=`_sudo rbd device --device-type nbd map ${POOL}/${IMAGE}@snap`
get_pid ${POOL}
unmap_device "${IMAGE}@snap" ${PID}
DEV=

# map/unmap namespace test
rbd snap create ${POOL}/${NS}/${IMAGE}@snap
DEV=`_sudo rbd device --device-type nbd map ${POOL}/${NS}/${IMAGE}@snap`
get_pid ${POOL} ${NS}
unmap_device "${POOL}/${NS}/${IMAGE}@snap" ${PID}
DEV=

# unmap by image name test 2
DEV=`_sudo rbd device --device-type nbd map ${POOL}/${IMAGE}`
get_pid ${POOL}
pid=$PID
DEV=`_sudo rbd device --device-type nbd map ${POOL}/${NS}/${IMAGE}`
get_pid ${POOL} ${NS}
unmap_device ${POOL}/${NS}/${IMAGE} ${PID}
DEV=
unmap_device ${POOL}/${IMAGE} ${pid}

# map/unmap test with just image name and expect image to come from default pool
if [ "${POOL}" = "rbd" ];then
    DEV=`_sudo rbd device --device-type nbd map ${IMAGE}`
    get_pid ${POOL}
    unmap_device ${IMAGE} ${PID}
    DEV=
fi

# map/unmap test with just image name after changing default pool
rbd config global set global rbd_default_pool ${ANOTHER_POOL}
rbd create --size 10M ${IMAGE}
DEV=`_sudo rbd device --device-type nbd map ${IMAGE}`
get_pid ${ANOTHER_POOL}
unmap_device ${IMAGE} ${PID}
DEV=

# reset
rbd config global rm global rbd_default_pool

# auto unmap test
DEV=`_sudo rbd device --device-type nbd map ${POOL}/${IMAGE}`
get_pid ${POOL}
_sudo kill ${PID}
for i in `seq 10`; do
  rbd device --device-type nbd list | expect_false grep "^${PID} *${POOL} *${IMAGE}" && break
  sleep 1
done
rbd device --device-type nbd list | expect_false grep "^${PID} *${POOL} *${IMAGE}"

# quiesce test
QUIESCE_HOOK=${TEMPDIR}/quiesce.sh
DEV=`_sudo rbd device --device-type nbd map --quiesce --quiesce-hook ${QUIESCE_HOOK} ${POOL}/${IMAGE}`
get_pid ${POOL}

# test it fails if the hook does not exists
test ! -e ${QUIESCE_HOOK}
expect_false rbd snap create ${POOL}/${IMAGE}@quiesce1
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct

# test the hook is executed
touch ${QUIESCE_HOOK}
chmod +x ${QUIESCE_HOOK}
cat > ${QUIESCE_HOOK} <<EOF
#/bin/sh
echo "test the hook is executed" >&2
echo \$1 > ${TEMPDIR}/\$2
EOF
rbd snap create ${POOL}/${IMAGE}@quiesce1
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct
test "$(cat ${TEMPDIR}/quiesce)" = ${DEV}
test "$(cat ${TEMPDIR}/unquiesce)" = ${DEV}

# test snap create fails if the hook fails
touch ${QUIESCE_HOOK}
chmod +x ${QUIESCE_HOOK}
cat > ${QUIESCE_HOOK} <<EOF
#/bin/sh
echo "test snap create fails if the hook fails" >&2
exit 22
EOF
expect_false rbd snap create ${POOL}/${IMAGE}@quiesce2
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct

# test the hook is slow
cat > ${QUIESCE_HOOK} <<EOF
#/bin/sh
echo "test the hook is slow" >&2
sleep 7
EOF
rbd snap create ${POOL}/${IMAGE}@quiesce2
_sudo dd if=${DATA} of=${DEV} bs=1M count=1 oflag=direct

# test rbd-nbd_quiesce hook that comes with distribution
unmap_device ${DEV} ${PID}
LOG_FILE=${TEMPDIR}/rbd-nbd.log
if [ -n "${CEPH_SRC}" ]; then
    QUIESCE_HOOK=${CEPH_SRC}/tools/rbd_nbd/rbd-nbd_quiesce
    DEV=`_sudo rbd device --device-type nbd map --quiesce --quiesce-hook ${QUIESCE_HOOK} \
               ${POOL}/${IMAGE} --log-file=${LOG_FILE}`
else
    DEV=`_sudo rbd device --device-type nbd map --quiesce ${POOL}/${IMAGE} --log-file=${LOG_FILE}`
fi
get_pid ${POOL}
_sudo mkfs ${DEV}
mkdir ${TEMPDIR}/mnt
_sudo mount ${DEV} ${TEMPDIR}/mnt
rbd snap create ${POOL}/${IMAGE}@quiesce3
_sudo dd if=${DATA} of=${TEMPDIR}/mnt/test bs=1M count=1 oflag=direct
_sudo umount ${TEMPDIR}/mnt
unmap_device ${DEV} ${PID}
DEV=
cat ${LOG_FILE}
expect_false grep 'quiesce failed' ${LOG_FILE}

# test detach/attach
DEV=`_sudo rbd device --device-type nbd --options try-netlink map ${POOL}/${IMAGE}`
get_pid ${POOL}
_sudo mount ${DEV} ${TEMPDIR}/mnt
_sudo rbd device detach ${POOL}/${IMAGE} --device-type nbd
expect_false get_pid ${POOL}
expect_false _sudo rbd device attach --device ${DEV} ${POOL}/${IMAGE} --device-type nbd
_sudo rbd device attach --device ${DEV} ${POOL}/${IMAGE} --device-type nbd --force
get_pid ${POOL}
_sudo rbd device detach ${DEV} --device-type nbd
expect_false get_pid ${POOL}
_sudo rbd device attach --device ${DEV} ${POOL}/${IMAGE} --device-type nbd --force
get_pid ${POOL}
ls ${TEMPDIR}/mnt/
dd if=${TEMPDIR}/mnt/test of=/dev/null bs=1M count=1
_sudo dd if=${DATA} of=${TEMPDIR}/mnt/test1 bs=1M count=1 oflag=direct
_sudo umount ${TEMPDIR}/mnt
unmap_device ${DEV} ${PID}

echo OK
