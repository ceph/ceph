#!/bin/sh
#
# rbd_mirror_stress.sh - stress test rbd-mirror daemon
#

IMAGE_COUNT=50
export LOCKDEP=0

if [ -n "${CEPH_REF}" ]; then
  wget -O rbd_mirror_helpers.sh "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=$CEPH_REF;f=qa/workunits/rbd/rbd_mirror_helpers.sh"
  . ./rbd_mirror_helpers.sh
else
  . $(dirname $0)/rbd_mirror_helpers.sh
fi

create_snap()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap_name=$4

    rbd --cluster ${cluster} -p ${pool} snap create ${image}@${snap_name} \
	--debug-rbd=20 --debug-journaler=20 2> ${TEMPDIR}/rbd-snap-create.log
}

compare_image_snaps()
{
    local pool=$1
    local image=$2
    local snap_name=$3

    local rmt_export=${TEMPDIR}/${CLUSTER2}-${pool}-${image}.export
    local loc_export=${TEMPDIR}/${CLUSTER1}-${pool}-${image}.export

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${CLUSTER2} -p ${pool} export ${image}@${snap_name} ${rmt_export}
    rbd --cluster ${CLUSTER1} -p ${pool} export ${image}@${snap_name} ${loc_export}
    cmp ${rmt_export} ${loc_export}
}

wait_for_pool_healthy()
{
    local cluster=$1
    local pool=$2
    local image_count=$3
    local s
    local count
    local state

    for s in `seq 1 40`; do
	sleep 30
        count=$(rbd --cluster ${cluster} -p ${pool} mirror pool status | grep 'images: ')
        test "${count}" = "images: ${image_count} total" || continue

        state=$(rbd --cluster ${cluster} -p ${pool} mirror pool status | grep 'health:')
        test "${state}" = "health: ERROR" && return 1
        test "${state}" = "health: OK" && return 0
    done
    return 1
}

start_mirror ${CLUSTER1}
start_mirror ${CLUSTER2}

testlog "TEST: add image and test replay after client crashes"
image=test
create_image ${CLUSTER2} ${POOL} ${image} '512M'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

for i in `seq 1 10`
do
  stress_write_image ${CLUSTER2} ${POOL} ${image}

  test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'

  snap_name="snap${i}"
  create_snap ${CLUSTER2} ${POOL} ${image} ${snap_name}
  wait_for_snap_present ${CLUSTER1} ${POOL} ${image} ${snap_name}
  compare_image_snaps ${POOL} ${image} ${snap_name}
done

for i in `seq 1 10`
do
  snap_name="snap${i}"
  remove_snapshot ${CLUSTER2} ${POOL} ${image} ${snap_name}
done

remove_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'

testlog "TEST: create many images"
for i in `seq 1 ${IMAGE_COUNT}`
do
  image="image_${i}"
  create_image ${CLUSTER2} ${POOL} ${image} '128M'
  write_image ${CLUSTER2} ${POOL} ${image} 100
done

wait_for_pool_healthy ${CLUSTER2} ${POOL} ${IMAGE_COUNT}
wait_for_pool_healthy ${CLUSTER1} ${POOL} ${IMAGE_COUNT}

testlog "TEST: compare many images"
for i in `seq 1 ${IMAGE_COUNT}`
do
  image="image_${i}"
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
  compare_images ${POOL} ${image}
done

testlog "TEST: delete many images"
for i in `seq 1 ${IMAGE_COUNT}`
do
  image="image_${i}"
  remove_image ${CLUSTER2} ${POOL} ${image}
done

testlog "TEST: image deletions should propagate"
wait_for_pool_healthy ${CLUSTER1} ${POOL} 0
for i in `seq 1 ${IMAGE_COUNT}`
do
  image="image_${i}"
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
done

testlog "TEST: delete images during bootstrap"
set_pool_mirror_mode ${CLUSTER1} ${POOL} 'image'
set_pool_mirror_mode ${CLUSTER2} ${POOL} 'image'

start_mirror ${CLUSTER1}
image=test

for i in `seq 1 10`
do
  image="image_${i}"
  create_image ${CLUSTER2} ${POOL} ${image} '512M'
  enable_mirror ${CLUSTER2} ${POOL} ${image}

  stress_write_image ${CLUSTER2} ${POOL} ${image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'

  disable_mirror ${CLUSTER2} ${POOL} ${image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
  purge_snapshots ${CLUSTER2} ${POOL} ${image}
  remove_image_retry ${CLUSTER2} ${POOL} ${image}
done

echo OK
