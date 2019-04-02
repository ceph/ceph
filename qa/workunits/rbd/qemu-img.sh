#!/usr/bin/env bash
set -ex

POOL_NAME="qemu-test-pool"
PG_NUM="32"
IMAGE_NAME="image-$$"
IMAGE_SIZE="200"
UNIT="M"

#### Start

sudo zypper --non-interactive install --no-recommends qemu-block-rbd qemu-tools
ceph osd pool create ${POOL_NAME} ${PG_NUM}
rbd pool init ${POOL_NAME}
qemu-img create -f rbd rbd:${POOL_NAME}/${IMAGE_NAME} ${IMAGE_SIZE}${UNIT}
qemu-img info rbd:${POOL_NAME}/${IMAGE_NAME} | sed -n '3p' | grep "virtual size: ${IMAGE_SIZE}${UNIT}"
qemu-img resize rbd:${POOL_NAME}/${IMAGE_NAME} "$(($IMAGE_SIZE * 2))"${UNIT} | grep "Image resized"
rbd rm ${POOL_NAME}/${IMAGE_NAME}
