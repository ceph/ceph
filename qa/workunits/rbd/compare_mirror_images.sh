#!/usr/bin/env bash

set -ex

IMG_PREFIX=image-primary
MIRROR_IMAGE_MODE=snapshot
MIRROR_POOL_MODE=image
MNTPT_PREFIX=test-primary
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

wait_for_image_removal() {
  local cluster=$1
  local pool=$2
  local image=$3

  for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
    if ! rbd --cluster $cluster ls $pool | grep -wq $image; then
      return 0
    fi
    sleep $s
  done

  echo "image ${pool}/${image} not removed from cluster ${cluster}"
  return 1
}

compare_demoted_promoted_image() {
  local dev=${DEVS[$1-1]}
  local img=${IMG_PREFIX}$1
  local mntpt=${MNTPT_PREFIX}$1
  local demote_md5 promote_md5

  sudo umount ${mntpt}

  # calculate hash before demotion of primary image
  demote_md5=$(sudo md5sum ${dev} | awk '{print $1}')
  sudo rbd --cluster ${CLUSTER1} device unmap -t ${RBD_DEVICE_TYPE} \
      ${POOL}/${img}

  demote_image ${CLUSTER1} ${POOL} ${img}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${img} 'up+unknown'
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${img} 'up+unknown'
  promote_image ${CLUSTER2} ${POOL} ${img}

  # calculate hash after promotion of secondary image
  if [[ $RBD_DEVICE_TYPE == "nbd" ]]; then
    dev=$(sudo rbd --cluster ${CLUSTER2} device map -t nbd \
             -o try-netlink ${POOL}/${img})
  elif [[ $RBD_DEVICE_TYPE == "krbd" ]]; then
    dev=$(sudo rbd --cluster ${CLUSTER2} device map -t krbd ${POOL}/${img})
  fi
  promote_md5=$(sudo md5sum ${dev} | awk '{print $1}')
  sudo rbd --cluster ${CLUSTER2} device unmap -t ${RBD_DEVICE_TYPE} ${dev}

  if [[ "${demote_md5}" != "${promote_md5}" ]]; then
    echo "Mismatch for image ${POOL}/${img}: ${demote_md5} != ${promote_md5}"
    return 1
  fi
}

setup

start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}

wget https://download.ceph.com/qa/linux-5.4.tar.gz

for i in {1..10}; do
  DEVS=()
  SNAP_PIDS=()
  COMPARE_PIDS=()
  WORKLOAD_PIDS=()
  RET=0
  for j in {1..10}; do
    IMG=${IMG_PREFIX}${j}
    MNTPT=${MNTPT_PREFIX}${j}
    create_image_and_enable_mirror ${CLUSTER1} ${POOL} ${IMG} \
      ${RBD_MIRROR_MODE} 10G
    if [[ $RBD_DEVICE_TYPE == "nbd" ]]; then
      DEV=$(sudo rbd --cluster ${CLUSTER1} device map -t nbd \
	      -o try-netlink ${POOL}/${IMG})
    elif [[ $RBD_DEVICE_TYPE == "krbd" ]]; then
      DEV=$(sudo rbd --cluster ${CLUSTER1} device map -t krbd \
	      ${POOL}/${IMG})
    else
      echo "Unknown RBD_DEVICE_TYPE: ${RBD_DEVICE_TYPE}"
      exit 1
    fi
    DEVS+=($DEV)
    sudo mkfs.ext4 ${DEV}
    mkdir ${MNTPT}
    sudo mount ${DEV} ${MNTPT}
    sudo chown $(whoami) ${MNTPT}
    # create mirror snapshots under I/O every few seconds
    take_mirror_snapshots ${CLUSTER1} ${POOL} ${IMG} &
    SNAP_PIDS+=($!)
    slow_untar_workload ${MNTPT} &
    WORKLOAD_PIDS+=($!)
  done
  for pid in ${SNAP_PIDS[@]}; do
    wait $pid || RET=$?
  done
  if ((RET != 0)); then
    echo "take_mirror_snapshots failed"
    exit 1
  fi
  for pid in ${WORKLOAD_PIDS[@]}; do
    wait $pid || RET=$?
  done
  if ((RET != 0)); then
    echo "slow_untar_workload failed"
    exit 1
  fi

  for j in {1..10}; do
    compare_demoted_promoted_image $j &
    COMPARE_PIDS+=($!)
  done
  for pid in ${COMPARE_PIDS[@]}; do
    wait $pid || RET=$?
  done
  if ((RET != 0)); then
    echo "compare_demoted_promoted_image failed"
    exit 1
  fi

  for j in {1..10}; do
    IMG=${IMG_PREFIX}${j}
    # Allow for removal of non-primary image by checking that mirroring
    # image status is "up+replaying"
    wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${IMG} 'up+replaying'
    remove_image ${CLUSTER2} ${POOL} ${IMG}
    wait_for_image_removal ${CLUSTER1} ${POOL} ${IMG}
    rm -rf ${MNTPT_PREFIX}${j}
  done
done

echo OK
