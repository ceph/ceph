#!/usr/bin/env bash

set -ex
set -o pipefail

readonly IMAGE_NAME="watch-errors-exclusive-test"

rbd create -s 1G --image-feature exclusive-lock,object-map "${IMAGE_NAME}"

# induce a watch error every 30 seconds
dev="$(sudo rbd device map -o exclusive,osdkeepalive=60 "${IMAGE_NAME}")"
dev_id="${dev#/dev/rbd}"

sudo dmesg -C

# test that a workload doesn't encounter EIO errors
fio --name test --filename="${dev}" --ioengine=libaio --direct=1 \
    --rw=randwrite --norandommap --randrepeat=0 --bs=512 --iodepth=128 \
    --time_based --runtime=1h --eta=never

num_errors="$(dmesg | grep -c "rbd${dev_id}: encountered watch error")"
echo "Recorded ${num_errors} watch errors"

sudo rbd device unmap "${dev}"

if ((num_errors < 60)); then
    echo "Too few watch errors"
    exit 1
fi

echo OK
