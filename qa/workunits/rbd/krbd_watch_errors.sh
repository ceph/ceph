#!/usr/bin/env bash

set -ex
set -o pipefail

function refresh_loop() {
    local dev_id="$1"

    set +x

    local i
    for ((i = 1; ; i++)); do
        echo 1 | sudo tee "${SYSFS_DIR}/${dev_id}/refresh" > /dev/null
        if ((i % 100 == 0)); then
            echo "Refreshed ${i} times"
        fi
    done
}

readonly SYSFS_DIR="/sys/bus/rbd/devices"
readonly IMAGE_NAME="watch-errors-test"

rbd create -s 1G --image-feature exclusive-lock "${IMAGE_NAME}"

# induce a watch error every 30 seconds
dev="$(sudo rbd device map -o osdkeepalive=60 "${IMAGE_NAME}")"
dev_id="${dev#/dev/rbd}"

# constantly refresh, not just on watch errors
refresh_loop "${dev_id}" &
refresh_pid=$!

sudo dmesg -C

# test that none of the above triggers a deadlock with a workload
fio --name test --filename="${dev}" --ioengine=libaio --direct=1 \
    --rw=randwrite --norandommap --randrepeat=0 --bs=512 --iodepth=128 \
    --time_based --runtime=1h --eta=never

num_errors="$(dmesg | grep -c "rbd${dev_id}: encountered watch error")"
echo "Recorded ${num_errors} watch errors"

kill "${refresh_pid}"
wait

sudo rbd device unmap "${dev}"

if ((num_errors < 60)); then
    echo "Too few watch errors"
    exit 1
fi

echo OK
