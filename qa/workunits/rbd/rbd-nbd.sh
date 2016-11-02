#!/bin/bash -ex

. $(dirname $0)/../ceph-helpers.sh

POOL=rbd
IMAGE=testrbdnbd$$
TOO_LARGE_IMAGE=${IMAGE}_large
SUDO=sudo
SIZE=64
DATA=
DEV=

setup()
{
    trap cleanup INT TERM EXIT
    TEMPDIR=`mktemp -d`
    DATA=${TEMPDIR}/data
    dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}
    rbd --dest-pool ${POOL} --no-progress import ${DATA} ${IMAGE}
    rbd -p ${POOL} create ${TOO_LARGE_IMAGE} --size 3T

    if [ `id -u` = 0 ]
    then
	SUDO=
    fi
}

function cleanup()
{
    set +e
    rm -Rf ${TMPDIR}
    if [ -n "${DEV}" ]
    then
	${SUDO} rbd-nbd unmap ${DEV}
    fi
    if rbd -p ${POOL} status ${IMAGE} 2>/dev/null; then
	for s in 0.5 1 2 4 8 16 32; do
	    sleep $s
	    rbd -p ${POOL} status ${IMAGE} | grep 'Watchers: none' && break
	done
	rbd -p ${POOL} remove ${IMAGE}
    fi
    rbd -p ${POOL} remove ${TOO_LARGE_IMAGE}
}

function expect_false()
{
  if "$@"; then return 1; else return 0; fi
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
    expect_false rbd-nbd map ${IMAGE}
fi
expect_false ${SUDO} rbd-nbd map INVALIDIMAGE
expect_false ${SUDO} rbd-nbd --device INVALIDDEV map ${IMAGE}
expect_false ${SUDO} rbd-nbd map ${TOO_LARGE_IMAGE}

# map test using the first unused device
DEV=`${SUDO} rbd-nbd map ${POOL}/${IMAGE}`
${SUDO} rbd-nbd list-mapped | grep "^${DEV}$"

# map test specifying the device
expect_false ${SUDO} rbd-nbd --device ${DEV} map ${POOL}/${IMAGE}
dev1=${DEV}
${SUDO} rbd-nbd unmap ${DEV}
${SUDO} rbd-nbd list-mapped | expect_false grep "^${DEV}$"
DEV=
# XXX: race possible when the device is reused by other process
DEV=`${SUDO} rbd-nbd --device ${dev1} map ${POOL}/${IMAGE}`
[ "${DEV}" = "${dev1}" ]
${SUDO} rbd-nbd list-mapped | grep "^${DEV}$"

# read test
[ "`dd if=${DATA} bs=1M | md5sum`" = "`${SUDO} dd if=${DEV} bs=1M | md5sum`" ]

# write test
dd if=/dev/urandom of=${DATA} bs=1M count=${SIZE}
${SUDO} dd if=${DATA} of=${DEV} bs=1M oflag=direct
[ "`dd if=${DATA} bs=1M | md5sum`" = "`rbd -p ${POOL} --no-progress export ${IMAGE} - | md5sum`" ]

# trim test
provisioned=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/provisioned_size" -v .`
used=`rbd -p ${POOL} --format xml du ${IMAGE} |
  $XMLSTARLET sel -t -m "//stats/images/image/used_size" -v .`
[ "${used}" -eq "${provisioned}" ]
${SUDO} mkfs.ext4 -E discard ${DEV} # better idea?
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
${SUDO} rbd-nbd unmap ${DEV}
DEV=`${SUDO} rbd-nbd map --read-only ${POOL}/${IMAGE}`
${SUDO} rbd-nbd list-mapped | grep "^${DEV}$"
${SUDO} dd if=${DEV} of=/dev/null bs=1M
expect_false ${SUDO} dd if=${DATA} of=${DEV} bs=1M oflag=direct
${SUDO} rbd-nbd unmap ${DEV}

# exclusive option test
DEV=`${SUDO} rbd-nbd map --exclusive ${POOL}/${IMAGE}`
${SUDO} rbd-nbd list-mapped | grep "^${DEV}$"
${SUDO} dd if=${DATA} of=${DEV} bs=1M oflag=direct
expect_false timeout 10 \
	rbd bench ${IMAGE} --io-type write --io-size=1024 --io-total=1024
${SUDO} rbd-nbd unmap ${DEV}
DEV=
rbd bench ${IMAGE} --io-type write --io-size=1024 --io-total=1024

echo OK
