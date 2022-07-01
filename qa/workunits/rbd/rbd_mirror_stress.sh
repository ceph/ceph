#!/bin/sh -ex
#
# rbd_mirror_stress.sh - stress test rbd-mirror daemon
#
# The following additional environment variables affect the test:
#
#  RBD_MIRROR_REDUCE_WRITES - if not empty, don't run the stress bench write
#                             tool during the many image test
#

IMAGE_COUNT=50
export LOCKDEP=0

. $(dirname $0)/rbd_mirror_helpers.sh

setup

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
    local ret=0

    local rmt_export=${TEMPDIR}/${CLUSTER2}-${pool}-${image}.export
    local loc_export=${TEMPDIR}/${CLUSTER1}-${pool}-${image}.export

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${CLUSTER2} -p ${pool} export ${image}@${snap_name} ${rmt_export}
    rbd --cluster ${CLUSTER1} -p ${pool} export ${image}@${snap_name} ${loc_export}
    if ! cmp ${rmt_export} ${loc_export}
    then
        show_diff ${rmt_export} ${loc_export}
        ret=1
    fi
    rm -f ${rmt_export} ${loc_export}
    return ${ret}
}

wait_for_pool_images()
{
    local cluster=$1
    local pool=$2
    local image_count=$3
    local s
    local count
    local last_count=0

    while true; do
        for s in `seq 1 40`; do
            test $s -ne 1 && sleep 30
            count=$(rbd --cluster ${cluster} -p ${pool} mirror pool status | grep 'images: ' | cut -d' ' -f 2)
            test "${count}" = "${image_count}" && return 0

            # reset timeout if making forward progress
            test $count -ne $last_count && break
        done

        test $count -eq $last_count && break
        last_count=$count
    done
    rbd --cluster ${cluster} -p ${pool} mirror pool status --verbose >&2
    return 1
}

wait_for_pool_healthy()
{
    local cluster=$1
    local pool=$2
    local s
    local state

    for s in `seq 1 40`; do
        test $s -ne 1 && sleep 30
        state=$(rbd --cluster ${cluster} -p ${pool} mirror pool status | grep 'image health:' | cut -d' ' -f 3)
        test "${state}" = "ERROR" && break
        test "${state}" = "OK" && return 0
    done
    rbd --cluster ${cluster} -p ${pool} mirror pool status --verbose >&2
    return 1
}

start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}

testlog "TEST: add image and test replay after client crashes"
image=test
create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${image} ${MIRROR_IMAGE_MODE} '512M'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

clean_snap_name=
for i in `seq 1 10`
do
  stress_write_image ${CLUSTER2} ${POOL} ${image}

  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'

  snap_name="snap${i}"
  create_snap ${CLUSTER2} ${POOL} ${image} ${snap_name}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
  wait_for_snap_present ${CLUSTER1} ${POOL} ${image} ${snap_name}

  if [ -n "${clean_snap_name}" ]; then
      compare_image_snaps ${POOL} ${image} ${clean_snap_name}
  fi
  compare_image_snaps ${POOL} ${image} ${snap_name}

  clean_snap_name="snap${i}-clean"
  create_snap ${CLUSTER2} ${POOL} ${image} ${clean_snap_name}
done

wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
wait_for_snap_present ${CLUSTER1} ${POOL} ${image} ${clean_snap_name}

for i in `seq 1 10`
do
  snap_name="snap${i}"
  compare_image_snaps ${POOL} ${image} ${snap_name}

  snap_name="snap${i}-clean"
  compare_image_snaps ${POOL} ${image} ${snap_name}
done

for i in `seq 1 10`
do
  snap_name="snap${i}"
  remove_snapshot ${CLUSTER2} ${POOL} ${image} ${snap_name}

  snap_name="snap${i}-clean"
  remove_snapshot ${CLUSTER2} ${POOL} ${image} ${snap_name}
done

remove_image_retry ${CLUSTER2} ${POOL} ${image}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'

testlog "TEST: create many images"
snap_name="snap"
for i in `seq 1 ${IMAGE_COUNT}`
do
  image="image_${i}"
  create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${image} ${MIRROR_IMAGE_MODE} '128M'
  if [ -n "${RBD_MIRROR_REDUCE_WRITES}" ]; then
    write_image ${CLUSTER2} ${POOL} ${image} 100
  else
    stress_write_image ${CLUSTER2} ${POOL} ${image}
  fi
done

wait_for_pool_images ${CLUSTER2} ${POOL} ${IMAGE_COUNT}
wait_for_pool_healthy ${CLUSTER2} ${POOL}

wait_for_pool_images ${CLUSTER1} ${POOL} ${IMAGE_COUNT}
wait_for_pool_healthy ${CLUSTER1} ${POOL}

testlog "TEST: compare many images"
for i in `seq 1 ${IMAGE_COUNT}`
do
  image="image_${i}"
  create_snap ${CLUSTER2} ${POOL} ${image} ${snap_name}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
  wait_for_snap_present ${CLUSTER1} ${POOL} ${image} ${snap_name}
  compare_image_snaps ${POOL} ${image} ${snap_name}
done

testlog "TEST: delete many images"
for i in `seq 1 ${IMAGE_COUNT}`
do
  image="image_${i}"
  remove_snapshot ${CLUSTER2} ${POOL} ${image} ${snap_name}
  remove_image_retry ${CLUSTER2} ${POOL} ${image}
done

testlog "TEST: image deletions should propagate"
wait_for_pool_images ${CLUSTER1} ${POOL} 0
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

testlog "TEST: check if removed images' OMAP are removed"

wait_for_image_in_omap ${CLUSTER1} ${POOL}
wait_for_image_in_omap ${CLUSTER2} ${POOL}
