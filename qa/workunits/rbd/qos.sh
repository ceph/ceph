#!/bin/sh -ex

POOL=rbd
IMAGE=test$$
IMAGE_SIZE=1G
TOLERANCE_PRCNT=10

rbd_bench() {
    local image=$1
    local type=$2
    local total=$3
    local qos_type=$4
    local qos_limit=$5
    local iops_var_name=$6
    local bps_var_name=$7
    local timeout=$8
    local timeout_cmd=""

    if [ -n "${timeout}" ]; then
        timeout_cmd="timeout --preserve-status ${timeout}"
    fi

    # parse `rbd bench` output for string like this:
    # elapsed:    25  ops:     2560  ops/sec:   100.08  bytes/sec: 409928.13
    iops_bps=$(${timeout_cmd} rbd bench "${image}" \
                              --io-type ${type} --io-size 4K \
                              --io-total ${total} --rbd-cache=false \
                              --rbd_qos_${qos_type}_limit ${qos_limit} |
                   awk '/elapsed:/ {print int($6) ":" int($8)}')
    eval ${iops_var_name}=${iops_bps%:*}
    eval ${bps_var_name}=${iops_bps#*:}
}

rbd create "${POOL}/${IMAGE}" -s ${IMAGE_SIZE}
rbd bench "${POOL}/${IMAGE}" --io-type write --io-size 4M --io-total ${IMAGE_SIZE}

rbd_bench "${POOL}/${IMAGE}" write ${IMAGE_SIZE} iops 0 iops bps 60
iops_unlimited=$iops
bps_unlimited=$bps

test "${iops_unlimited}" -ge 20 || exit 0

io_total=$((bps_unlimited * 30))

rbd_bench "${POOL}/${IMAGE}" write ${io_total} iops $((iops_unlimited / 2)) iops bps
test "${iops}" -lt $((iops_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd_bench "${POOL}/${IMAGE}" write ${io_total} write_iops $((iops_unlimited / 2)) iops bps
test "${iops}" -lt $((iops_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd_bench "${POOL}/${IMAGE}" write ${io_total} bps $((bps_unlimited / 2)) iops bps
test "${bps}" -lt $((bps_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd_bench "${POOL}/${IMAGE}" write ${io_total} write_bps $((bps_unlimited / 2)) iops bps
test "${bps}" -lt $((bps_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd_bench "${POOL}/${IMAGE}" read ${io_total} iops 0 iops bps
iops_unlimited=$iops
bps_unlimited=$bps

test "${iops_unlimited}" -ge 20 || exit 0

io_total=$((bps_unlimited * 30))

rbd_bench "${POOL}/${IMAGE}" read ${io_total} iops $((iops_unlimited / 2)) iops bps
test "${iops}" -lt $((iops_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd_bench "${POOL}/${IMAGE}" read ${io_total} read_iops $((iops_unlimited / 2)) iops bps
test "${iops}" -lt $((iops_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd_bench "${POOL}/${IMAGE}" read ${io_total} bps $((bps_unlimited / 2)) iops bps
test "${bps}" -lt $((bps_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd_bench "${POOL}/${IMAGE}" read ${io_total} read_bps $((bps_unlimited / 2)) iops bps
test "${bps}" -lt $((bps_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

# test a config override is applied
rbd config image set "${POOL}/${IMAGE}" rbd_qos_iops_limit $((iops_unlimited / 4))
rbd_bench "${POOL}/${IMAGE}" read ${io_total} iops $((iops_unlimited / 2)) iops bps
test "${iops}" -lt $((iops_unlimited / 4 * (100 + TOLERANCE_PRCNT) / 100))
rbd config image remove "${POOL}/${IMAGE}" rbd_qos_iops_limit
rbd_bench "${POOL}/${IMAGE}" read ${io_total} iops $((iops_unlimited / 2)) iops bps
test "${iops}" -lt $((iops_unlimited / 2 * (100 + TOLERANCE_PRCNT) / 100))

rbd rm "${POOL}/${IMAGE}"

echo OK
