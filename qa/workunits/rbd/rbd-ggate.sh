#!/bin/sh -ex

POOL=testrbdggate$$
IMAGE=test
SIZE=64
DATA=
DEV=

if which xmlstarlet > /dev/null 2>&1; then
  XMLSTARLET=xmlstarlet
elif which xml > /dev/null 2>&1; then
  XMLSTARLET=xml
else
  echo "Missing xmlstarlet binary!"
  exit 1
fi

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
    if [ -e CMakeCache.txt ]; then
	# running under cmake build dir

	CEPH_SRC=$(readlink -f $(dirname $0)/../../../src)
	CEPH_ROOT=${PWD}
	CEPH_BIN=${CEPH_ROOT}/bin

	export LD_LIBRARY_PATH=${CEPH_ROOT}/lib:${LD_LIBRARY_PATH}
	export PYTHONPATH=${PYTHONPATH}:${CEPH_SRC}/pybind
	for x in ${CEPH_ROOT}/lib/cython_modules/lib* ; do
            PYTHONPATH="${PYTHONPATH}:${x}"
	done
	PATH=${CEPH_BIN}:${PATH}
    fi

    _sudo echo test sudo

    trap cleanup INT TERM EXIT
    TEMPDIR=`mktemp -d`
    DATA=${TEMPDIR}/data
    dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}
    ceph osd pool create ${POOL} 32
    rbd --dest-pool ${POOL} --no-progress import ${DATA} ${IMAGE}
}

cleanup()
{
    set +e
    rm -Rf ${TEMPDIR}
    if [ -n "${DEV}" ]
    then
	_sudo rbd-ggate unmap ${DEV}
    fi
    ceph osd pool delete ${POOL} ${POOL} --yes-i-really-really-mean-it
}

expect_false()
{
  if "$@"; then return 1; else return 0; fi
}

#
# main
#

setup

# exit status test
expect_false rbd-ggate
expect_false rbd-ggate INVALIDCMD
if [ `id -u` -ne 0 ]
then
    expect_false rbd-ggate map ${IMAGE}
fi
expect_false _sudo rbd-ggate map INVALIDIMAGE

# map test using the first unused device
DEV=`_sudo rbd-ggate map ${POOL}/${IMAGE}`
_sudo rbd-ggate list | grep " ${DEV} *$"

# map test specifying the device
expect_false _sudo rbd-ggate --device ${DEV} map ${POOL}/${IMAGE}
dev1=${DEV}
_sudo rbd-ggate unmap ${DEV}
_sudo rbd-ggate list | expect_false grep " ${DEV} *$"
DEV=
# XXX: race possible when the device is reused by other process
DEV=`_sudo rbd-ggate --device ${dev1} map ${POOL}/${IMAGE}`
[ "${DEV}" = "${dev1}" ]
_sudo rbd-ggate list | grep " ${DEV} *$"

# list format test
expect_false _sudo rbd-ggate --format INVALID list
_sudo rbd-ggate --format json --pretty-format list
_sudo rbd-ggate --format xml list

# read test
[ "`dd if=${DATA} bs=1M | md5`" = "`_sudo dd if=${DEV} bs=1M | md5`" ]

# write test
dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}
_sudo dd if=${DATA} of=${DEV} bs=1M
_sudo sync
[ "`dd if=${DATA} bs=1M | md5`" = "`rbd -p ${POOL} --no-progress export ${IMAGE} - | md5`" ]

# trim test
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -eq "${provisioned}" ]
_sudo newfs -E ${DEV}
_sudo sync
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -lt "${provisioned}" ]

# resize test
devname=$(basename ${DEV})
size=$(geom gate list ${devname} | awk '$1 ~ /Mediasize:/ {print $2}')
test -n "${size}"
rbd resize ${POOL}/${IMAGE} --size $((SIZE * 2))M
rbd info ${POOL}/${IMAGE}
if [ -z "$RBD_GGATE_RESIZE_SUPPORTED" ]; then
    # XXX: ggate device resize is not supported by vanila kernel.
    # rbd-ggate should terminate when detecting resize.
    _sudo rbd-ggate list | expect_false grep " ${DEV} *$"
else
    _sudo rbd-ggate list | grep " ${DEV} *$"
    size2=$(geom gate list ${devname} | awk '$1 ~ /Mediasize:/ {print $2}')
    test -n "${size2}"
    test ${size2} -eq $((size * 2))
    dd if=/dev/urandom of=${DATA} bs=1M count=$((SIZE * 2))
    _sudo dd if=${DATA} of=${DEV} bs=1M
    _sudo sync
    [ "`dd if=${DATA} bs=1M | md5`" = "`rbd -p ${POOL} --no-progress export ${IMAGE} - | md5`" ]
    rbd resize ${POOL}/${IMAGE} --allow-shrink --size ${SIZE}M
    rbd info ${POOL}/${IMAGE}
    size2=$(geom gate list ${devname} | awk '$1 ~ /Mediasize:/ {print $2}')
    test -n "${size2}"
    test ${size2} -eq ${size}
    truncate -s ${SIZE}M ${DATA}
    [ "`dd if=${DATA} bs=1M | md5`" = "`rbd -p ${POOL} --no-progress export ${IMAGE} - | md5`" ]
    _sudo rbd-ggate unmap ${DEV}
fi
DEV=

# read-only option test
DEV=`_sudo rbd-ggate map --read-only ${POOL}/${IMAGE}`
devname=$(basename ${DEV})
_sudo rbd-ggate list | grep " ${DEV} *$"
access=$(geom gate list ${devname} | awk '$1 == "access:" {print $2}')
test "${access}" = "read-only"
_sudo dd if=${DEV} of=/dev/null bs=1M
expect_false _sudo dd if=${DATA} of=${DEV} bs=1M
_sudo rbd-ggate unmap ${DEV}

# exclusive option test
DEV=`_sudo rbd-ggate map --exclusive ${POOL}/${IMAGE}`
_sudo rbd-ggate list | grep " ${DEV} *$"
_sudo dd if=${DATA} of=${DEV} bs=1M
_sudo sync
expect_false timeout 10 \
    rbd -p ${POOL} bench ${IMAGE} --io-type=write --io-size=1024 --io-total=1024
_sudo rbd-ggate unmap ${DEV}
DEV=
rbd bench -p ${POOL} ${IMAGE} --io-type=write --io-size=1024 --io-total=1024

# unmap by image name test
DEV=`_sudo rbd-ggate map ${POOL}/${IMAGE}`
_sudo rbd-ggate list | grep " ${DEV} *$"
_sudo rbd-ggate unmap "${POOL}/${IMAGE}"
rbd-ggate list-mapped | expect_false grep " ${DEV} *$"
DEV=

# map/unmap snap test
rbd snap create ${POOL}/${IMAGE}@snap
DEV=`_sudo rbd-ggate map ${POOL}/${IMAGE}@snap`
_sudo rbd-ggate list | grep " ${DEV} *$"
_sudo rbd-ggate unmap "${POOL}/${IMAGE}@snap"
rbd-ggate list-mapped | expect_false grep " ${DEV} *$"
DEV=

echo OK
