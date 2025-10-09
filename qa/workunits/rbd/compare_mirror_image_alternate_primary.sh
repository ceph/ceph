#!/usr/bin/env bash

set -ex

IMAGE=image-alternate-primary
MIRROR_IMAGE_MODE=snapshot
MIRROR_POOL_MODE=image
MOUNT=test-alternate-primary
RBD_IMAGE_FEATURES='layering,exclusive-lock,object-map,fast-diff'
RBD_MIRROR_INSTANCES=1
RBD_MIRROR_MODE=snapshot
RBD_MIRROR_USE_EXISTING_CLUSTER=1

. $(dirname $0)/rbd_mirror_helpers.sh

take_mirror_snapshots() {
  local cluster=$1
  local pool=$2
  local image=$3

  for i in {1..30}; do
    mirror_image_snapshot $cluster $pool $image
    sleep 3
  done
}

slow_untar_workload() {
  local mountpt=$1

  cp linux-5.4.tar.gz $mountpt
  # run workload that updates the data and metadata of multiple files on disk.
  # rate limit the workload such that the mirror snapshots can be taken as the
  # contents of the image are progressively changed by the workload.
  local ret=0
  timeout 5m bash -c "zcat $mountpt/linux-5.4.tar.gz \
    | pv -L 256K | tar xf - -C $mountpt" || ret=$?
  if ((ret != 124)); then
    echo "Workload completed prematurely"
    return 1
  fi
}

setup

start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}

# initial setup
create_image_and_enable_mirror ${CLUSTER1} ${POOL} ${IMAGE} \
  ${RBD_MIRROR_MODE} 10G

if [[ $RBD_DEVICE_TYPE == "nbd" ]]; then
  DEV=$(sudo rbd --cluster ${CLUSTER1} device map -t nbd \
           -o try-netlink ${POOL}/${IMAGE})
elif [[ $RBD_DEVICE_TYPE == "krbd" ]]; then
  DEV=$(sudo rbd --cluster ${CLUSTER1} device map -t krbd \
           ${POOL}/${IMAGE})
else
  echo "Unknown RBD_DEVICE_TYPE: ${RBD_DEVICE_TYPE}"
  exit 1
fi
sudo mkfs.ext4 ${DEV}
mkdir ${MOUNT}

wget https://download.ceph.com/qa/linux-5.4.tar.gz

for i in {1..25}; do
  # create mirror snapshots every few seconds under I/O
  sudo mount ${DEV} ${MOUNT}
  sudo chown $(whoami) ${MOUNT}
  rm -rf ${MOUNT}/*
  take_mirror_snapshots ${CLUSTER1} ${POOL} ${IMAGE} &
  SNAP_PID=$!
  slow_untar_workload ${MOUNT}
  wait $SNAP_PID
  sudo umount ${MOUNT}

  # calculate hash before demotion of primary image
  DEMOTE_MD5=$(sudo md5sum ${DEV} | awk '{print $1}')
  sudo rbd --cluster ${CLUSTER1} device unmap -t ${RBD_DEVICE_TYPE} ${DEV}

  demote_image ${CLUSTER1} ${POOL} ${IMAGE}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${IMAGE} 'up+unknown'
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${IMAGE} 'up+unknown'
  promote_image ${CLUSTER2} ${POOL} ${IMAGE}

  # calculate hash after promotion of secondary image
  if [[ $RBD_DEVICE_TYPE == "nbd" ]]; then
    DEV=$(sudo rbd --cluster ${CLUSTER2} device map -t nbd \
             -o try-netlink ${POOL}/${IMAGE})
  elif [[ $RBD_DEVICE_TYPE == "krbd" ]]; then
    DEV=$(sudo rbd --cluster ${CLUSTER2} device map -t krbd ${POOL}/${IMAGE})
  fi
  PROMOTE_MD5=$(sudo md5sum ${DEV} | awk '{print $1}')

  if [[ "${DEMOTE_MD5}" != "${PROMOTE_MD5}" ]]; then
    echo "Mismatch at iteration ${i}: ${DEMOTE_MD5} != ${PROMOTE_MD5}"
    exit 1
  fi

  TEMP=${CLUSTER1}
  CLUSTER1=${CLUSTER2}
  CLUSTER2=${TEMP}
done

echo OK
