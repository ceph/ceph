#!/bin/bash

set -ex

IMAGE=image
MOUNT=/mnt/test
WORKLOAD_TIMEOUT=5m

. $(dirname $0)/rbd_mirror_helpers.sh

launch_manual_msnaps() {
  local cluster=$1
  local pool=$2
  local image=$3

  for i in {1..30}; do
    mirror_image_snapshot $cluster $pool $image
    sleep 3s;
  done
}

run_bench() {
  local mountpt=$1
  local timeout=$2

  KERNEL_TAR_URL="https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.14.280.tar.gz"
  sudo wget $KERNEL_TAR_URL -O $mountpt/kernel.tar.gz
  sudo timeout $timeout bash -c "tar xvfz $mountpt/kernel.tar.gz -C $mountpt | pv -L 1k --timer &> /dev/null" || true
}

wait_for_demote_snap () {
  local cluster=$1
  local pool=$2
  local image=$3

  for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
    RET=`rbd --cluster $cluster snap ls --all $pool/$image | grep non_primary | tail -n 1 | grep demote | grep -v "%" ||true`
    if [ "$RET" != "" ]; then
      echo demoted snapshot received, continuing
      sleep 10s #wait a bit for it to propagate
      break
    fi

    echo waiting for demoted snapshot...
    sleep $s
  done
}

setup

start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}

#initial setup
create_image ${CLUSTER1} ${POOL} ${IMAGE} 10G
enable_mirror ${CLUSTER1} ${POOL} ${IMAGE}

BDEV=$(map ${CLUSTER1} ${POOL} ${IMAGE})
sudo mkfs.ext4 ${BDEV}
sudo mkdir -p ${MOUNT}

for i in {1..25};
do
    sudo mount ${BDEV} ${MOUNT}
    launch_manual_msnaps ${CLUSTER1} ${POOL} ${IMAGE} &
    run_bench ${MOUNT} ${WORKLOAD_TIMEOUT}
  wait

  sudo umount ${MOUNT}
  unmap ${CLUSTER1} ${POOL} ${IMAGE}

  # demote and calc hash
  demote_image ${CLUSTER1} ${POOL} ${IMAGE}
  DEMOTE=$(rbd --cluster ${CLUSTER1} snap ls --all ${POOL}/${IMAGE} | tail -n 1 | grep mirror\.primary | grep demoted | awk '{print $2}')
  BDEV=$(map ${CLUSTER1} ${POOL} ${IMAGE} ${DEMOTE})
  DEMOTE_MD5=$(sudo dd if=${BDEV} bs=4M | md5sum | awk '{print $1}')
  unmap ${CLUSTER1} ${POOL} ${IMAGE} ${DEMOTE}

  wait_for_demote_snap ${CLUSTER2} ${POOL} ${IMAGE}

  #swap clusters
  TEMP=${CLUSTER1}
  CLUSTER1=${CLUSTER2}
  CLUSTER2=${TEMP}

  #promote and calc hash
  promote_image ${CLUSTER1} ${POOL} ${IMAGE}
  PROMOTE=$(rbd --cluster ${CLUSTER1} snap ls --all ${POOL}/${IMAGE} | tail -n 1 | grep mirror\.primary | awk '{print $2}')
  BDEV=$(map ${CLUSTER1} ${POOL} ${IMAGE} ${PROMOTE})
  PROMOTE_MD5=$(sudo dd if=${BDEV} bs=4M | md5sum | awk '{print $1}')
  unmap ${CLUSTER1} ${POOL} ${IMAGE} ${PROMOTE}

  [ "${DEMOTE_MD5}" == "${PROMOTE_MD5}" ];

  BDEV=$(map ${CLUSTER1} ${POOL} ${IMAGE})
  enable_mirror ${CLUSTER1} ${POOL} ${IMAGE}
done

echo OK
