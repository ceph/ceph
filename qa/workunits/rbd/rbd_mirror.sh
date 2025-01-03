#!/usr/bin/env bash
#
# rbd_mirror.sh - test rbd-mirror daemon in snapshot or journal mirroring mode
#
# Usage:
#   RBD_MIRROR_MODE=journal rbd_mirror.sh
#
# Use environment variable RBD_MIRROR_MODE to set the mode
# Available modes: snapshot | journal
#
# The scripts starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#

set -ex

if [ "${#}" -gt 0 ]; then
  echo "unnecessary arguments: ${@}"
  exit 100
fi

if [ "${RBD_MIRROR_MODE}" != "snapshot" ] && [ "${RBD_MIRROR_MODE}" != "journal" ]; then
  echo "unknown mode: ${RBD_MIRROR_MODE}"
  echo "set RBD_MIRROR_MODE env variable, available modes: snapshot | journal"
  exit 100
fi

. $(dirname $0)/rbd_mirror_helpers.sh
setup

testlog "TEST: add image and test replay"
start_mirrors ${CLUSTER1}
image=test
create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${image} ${RBD_MIRROR_MODE}
set_image_meta ${CLUSTER2} ${POOL} ${image} "key1" "value1"
set_image_meta ${CLUSTER2} ${POOL} ${image} "key2" "value2"
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'down+unknown'
fi
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
compare_image_meta ${CLUSTER1} ${POOL} ${image} "key1" "value1"
compare_image_meta ${CLUSTER1} ${POOL} ${image} "key2" "value2"

testlog "TEST: stop mirror, add image, start mirror and test replay"
stop_mirrors ${CLUSTER1}
image1=test1
create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${image1} ${RBD_MIRROR_MODE}
write_image ${CLUSTER2} ${POOL} ${image1} 100
start_mirrors ${CLUSTER1}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1}
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image1} 'down+unknown'
fi
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog "TEST: test the first image is replaying after restart"
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  testlog "TEST: stop/start/restart mirror via admin socket"
  all_admin_daemons ${CLUSTER1} rbd mirror stop
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+stopped'

  all_admin_daemons ${CLUSTER1} rbd mirror start
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+replaying'

  all_admin_daemons ${CLUSTER1} rbd mirror restart
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+replaying'

  all_admin_daemons ${CLUSTER1} rbd mirror stop
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+stopped'

  all_admin_daemons ${CLUSTER1} rbd mirror restart
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+replaying'

  all_admin_daemons ${CLUSTER1} rbd mirror stop ${POOL} ${CLUSTER2}${PEER_CLUSTER_SUFFIX}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+stopped'

  admin_daemons ${CLUSTER1} rbd mirror start ${POOL}/${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'

  all_admin_daemons ${CLUSTER1} rbd mirror start ${POOL} ${CLUSTER2}${PEER_CLUSTER_SUFFIX}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+replaying'

  admin_daemons ${CLUSTER1} rbd mirror restart ${POOL}/${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'

  all_admin_daemons ${CLUSTER1} rbd mirror restart ${POOL} ${CLUSTER2}${PEER_CLUSTER_SUFFIX}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

  all_admin_daemons ${CLUSTER1} rbd mirror stop ${POOL} ${CLUSTER2}${PEER_CLUSTER_SUFFIX}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+stopped'

  all_admin_daemons ${CLUSTER1} rbd mirror restart ${POOL} ${CLUSTER2}${PEER_CLUSTER_SUFFIX}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+replaying'

  flush ${CLUSTER1}
  all_admin_daemons ${CLUSTER1} rbd mirror status
fi

remove_image_retry ${CLUSTER2} ${POOL} ${image1}

testlog "TEST: test image rename"
new_name="${image}_RENAMED"
rename_image ${CLUSTER2} ${POOL} ${image} ${new_name}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  mirror_image_snapshot ${CLUSTER2} ${POOL} ${new_name}
fi
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${new_name}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${new_name} 'up+replaying'
admin_daemons ${CLUSTER1} rbd mirror status ${POOL}/${new_name}
admin_daemons ${CLUSTER1} rbd mirror restart ${POOL}/${new_name}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${new_name}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${new_name} 'up+replaying'
rename_image ${CLUSTER2} ${POOL} ${new_name} ${image}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  mirror_image_snapshot ${CLUSTER2} ${POOL} ${image}
fi
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

testlog "TEST: test trash move restore"
image_id=$(get_image_id ${CLUSTER2} ${POOL} ${image})
trash_move ${CLUSTER2} ${POOL} ${image}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
trash_restore ${CLUSTER2} ${POOL} ${image} ${image_id} ${RBD_MIRROR_MODE}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

testlog "TEST: check if removed images' OMAP are removed (with rbd-mirror on one cluster)"
remove_image_retry ${CLUSTER2} ${POOL} ${image}

wait_for_image_in_omap ${CLUSTER1} ${POOL}
wait_for_image_in_omap ${CLUSTER2} ${POOL}

create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${image} ${RBD_MIRROR_MODE}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'

testlog "TEST: failover and failback"
start_mirrors ${CLUSTER2}

# demote and promote same cluster
demote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+unknown'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+unknown'
promote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

# failover (unmodified)
demote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+unknown'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+unknown'
promote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER2} ${POOL} ${image}

# failback (unmodified)
demote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER2} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+unknown'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+unknown'
promote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

# failover
demote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+unknown'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+unknown'
promote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER2} ${POOL} ${image}
write_image ${CLUSTER1} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
wait_for_replaying_status_in_pool_dir ${CLUSTER2} ${POOL} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

# failback
demote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER2} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+unknown'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+unknown'
promote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog "TEST: failover / failback loop"
for i in `seq 1 20`; do
  demote_image ${CLUSTER2} ${POOL} ${image}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+unknown'
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+unknown'
  promote_image ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_started ${CLUSTER2} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+replaying'
  demote_image ${CLUSTER1} ${POOL} ${image}
  wait_for_image_replay_stopped ${CLUSTER2} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+unknown'
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+unknown'
  promote_image ${CLUSTER2} ${POOL} ${image}
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying'
done
# check that demote (or other mirror snapshots) don't pile up
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  test "$(count_mirror_snaps ${CLUSTER1} ${POOL} ${image})" -le 3
  test "$(count_mirror_snaps ${CLUSTER2} ${POOL} ${image})" -le 3
fi

testlog "TEST: force promote"
force_promote_image=test_force_promote
create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${force_promote_image} ${RBD_MIRROR_MODE}
write_image ${CLUSTER2} ${POOL} ${force_promote_image} 100
wait_for_image_replay_stopped ${CLUSTER2} ${POOL} ${force_promote_image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${force_promote_image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${force_promote_image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${force_promote_image}
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${force_promote_image} 'up+stopped'
promote_image ${CLUSTER1} ${POOL} ${force_promote_image} '--force'
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${force_promote_image}
wait_for_image_replay_stopped ${CLUSTER2} ${POOL} ${force_promote_image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${force_promote_image} 'up+stopped'
wait_for_status_in_pool_dir ${CLUSTER2} ${POOL} ${force_promote_image} 'up+stopped'
write_image ${CLUSTER1} ${POOL} ${force_promote_image} 100
write_image ${CLUSTER2} ${POOL} ${force_promote_image} 100
remove_image_retry ${CLUSTER1} ${POOL} ${force_promote_image}
remove_image_retry ${CLUSTER2} ${POOL} ${force_promote_image}

testlog "TEST: cloned images"
testlog " - default"
parent_image=test_parent
parent_snap=snap
create_image_and_enable_mirror ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${RBD_MIRROR_MODE}
write_image ${CLUSTER2} ${PARENT_POOL} ${parent_image} 100
create_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
protect_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}

clone_image=test_clone
clone_image ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap} ${POOL} ${clone_image}
write_image ${CLUSTER2} ${POOL} ${clone_image} 100
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  enable_mirror ${CLUSTER2} ${POOL} ${clone_image} ${RBD_MIRROR_MODE}
else
  enable_mirror ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${RBD_MIRROR_MODE}
fi
wait_for_image_replay_started ${CLUSTER1} ${PARENT_POOL} ${parent_image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${PARENT_POOL} ${PARENT_POOL} ${parent_image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${PARENT_POOL} ${parent_image}
compare_images ${CLUSTER1} ${CLUSTER2} ${PARENT_POOL} ${PARENT_POOL} ${parent_image}

wait_for_image_replay_started ${CLUSTER1} ${POOL} ${clone_image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${clone_image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${clone_image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${clone_image}
remove_image_retry ${CLUSTER2} ${POOL} ${clone_image}

testlog " - clone v1"
clone_image_and_enable_mirror ${CLUSTER1} ${PARENT_POOL} \
    ${parent_image} ${parent_snap} ${POOL} ${clone_image}1 \
    ${RBD_MIRROR_MODE}
clone_image_and_enable_mirror ${CLUSTER2} ${PARENT_POOL} \
    ${parent_image} ${parent_snap} ${POOL} ${clone_image}_v1 \
    ${RBD_MIRROR_MODE} --rbd-default-clone-format 1
test $(get_clone_format ${CLUSTER2} ${POOL} ${clone_image}_v1) = 1
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${clone_image}_v1
test $(get_clone_format ${CLUSTER1} ${POOL} ${clone_image}_v1) = 1
remove_image_retry ${CLUSTER2} ${POOL} ${clone_image}_v1
remove_image_retry ${CLUSTER1} ${POOL} ${clone_image}1
unprotect_snapshot_retry ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
remove_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}

testlog " - clone v2"
parent_snap=snap_v2
create_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  mirror_image_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image}
fi
clone_image_and_enable_mirror ${CLUSTER2} ${PARENT_POOL} \
    ${parent_image} ${parent_snap} ${POOL} ${clone_image}_v2 \
    ${RBD_MIRROR_MODE} --rbd-default-clone-format 2
test $(get_clone_format ${CLUSTER2} ${POOL} ${clone_image}_v2) = 2
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${clone_image}_v2
test $(get_clone_format ${CLUSTER1} ${POOL} ${clone_image}_v2) = 2

remove_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  mirror_image_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image}
fi
test_snap_moved_to_trash ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
wait_for_snap_moved_to_trash ${CLUSTER1} ${PARENT_POOL} ${parent_image} ${parent_snap}
remove_image_retry ${CLUSTER2} ${POOL} ${clone_image}_v2
wait_for_image_present ${CLUSTER1} ${POOL} ${clone_image}_v2 'deleted'
test_snap_removed_from_trash ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
wait_for_snap_removed_from_trash ${CLUSTER1} ${PARENT_POOL} ${parent_image} ${parent_snap}

testlog " - clone v2 non-primary"
create_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  mirror_image_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image}
fi
wait_for_snap_present ${CLUSTER1} ${PARENT_POOL} ${parent_image} ${parent_snap}
clone_image_and_enable_mirror ${CLUSTER1} ${PARENT_POOL} \
    ${parent_image} ${parent_snap} ${POOL} ${clone_image}_v2 \
    ${RBD_MIRROR_MODE} --rbd-default-clone-format 2
remove_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
test_snap_removed_from_trash ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  mirror_image_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image}
fi
wait_for_snap_moved_to_trash ${CLUSTER1} ${PARENT_POOL} ${parent_image} ${parent_snap}
remove_image_retry ${CLUSTER1} ${POOL} ${clone_image}_v2
wait_for_snap_removed_from_trash ${CLUSTER1} ${PARENT_POOL} ${parent_image} ${parent_snap}
remove_image_retry ${CLUSTER2} ${PARENT_POOL} ${parent_image}

testlog "TEST: data pool"
dp_image=test_data_pool
create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${dp_image} \
    ${RBD_MIRROR_MODE} 128 --data-pool ${PARENT_POOL}
data_pool=$(get_image_data_pool ${CLUSTER2} ${POOL} ${dp_image})
test "${data_pool}" = "${PARENT_POOL}"
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${dp_image}
data_pool=$(get_image_data_pool ${CLUSTER1} ${POOL} ${dp_image})
test "${data_pool}" = "${PARENT_POOL}"
create_snapshot ${CLUSTER2} ${POOL} ${dp_image} 'snap1'
write_image ${CLUSTER2} ${POOL} ${dp_image} 100
create_snapshot ${CLUSTER2} ${POOL} ${dp_image} 'snap2'
write_image ${CLUSTER2} ${POOL} ${dp_image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${dp_image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${dp_image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${dp_image}@snap1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${dp_image}@snap2
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${dp_image}
remove_image_retry ${CLUSTER2} ${POOL} ${dp_image}

testlog "TEST: disable mirroring / delete non-primary image"
image2=test2
image3=test3
image4=test4
image5=test5
for i in ${image2} ${image3} ${image4} ${image5}; do
  create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${i} ${RBD_MIRROR_MODE}
  write_image ${CLUSTER2} ${POOL} ${i} 100
  create_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
  create_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  if [ "${i}" = "${image4}" ] || [ "${i}" = "${image5}" ]; then
    protect_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
    protect_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  fi
  write_image ${CLUSTER2} ${POOL} ${i} 100
  if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
    mirror_image_snapshot ${CLUSTER2} ${POOL} ${i}
  fi
  wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'present'
  wait_for_snap_present ${CLUSTER1} ${POOL} ${i} 'snap2'
done

set_pool_mirror_mode ${CLUSTER2} ${POOL} 'image'
for i in ${image2} ${image4}; do
  disable_mirror ${CLUSTER2} ${POOL} ${i}
done

unprotect_snapshot ${CLUSTER2} ${POOL} ${image5} 'snap1'
unprotect_snapshot ${CLUSTER2} ${POOL} ${image5} 'snap2'
for i in ${image3} ${image5}; do
  remove_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
  remove_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  remove_image_retry ${CLUSTER2} ${POOL} ${i}
done

for i in ${image2} ${image3} ${image4} ${image5}; do
  wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'deleted'
done

if [ "${RBD_MIRROR_MODE}" = "journal" ]; then
  set_pool_mirror_mode ${CLUSTER2} ${POOL} 'pool'
  for i in ${image2} ${image4}; do
    enable_journaling ${CLUSTER2} ${POOL} ${i}
    wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'present'
    wait_for_snap_present ${CLUSTER1} ${POOL} ${i} 'snap2'
    wait_for_image_replay_started ${CLUSTER1} ${POOL} ${i}
    wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${i}
    compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${i}
  done

  testlog "TEST: remove mirroring pool"
  pool=pool_to_remove
  for cluster in ${CLUSTER1} ${CLUSTER2}; do
    CEPH_ARGS='' ceph --cluster ${cluster} osd pool create ${pool} 16 16
    CEPH_ARGS='' rbd --cluster ${cluster} pool init ${pool}
    rbd --cluster ${cluster} mirror pool enable ${pool} pool
  done
  peer_add ${CLUSTER1} ${pool} ${CLUSTER2}
  peer_add ${CLUSTER2} ${pool} ${CLUSTER1}
  rdp_image=test_remove_data_pool
  create_image ${CLUSTER2} ${pool} ${image} 128
  create_image ${CLUSTER2} ${POOL} ${rdp_image} 128 --data-pool ${pool}
  write_image ${CLUSTER2} ${pool} ${image} 100
  write_image ${CLUSTER2} ${POOL} ${rdp_image} 100
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${pool} ${pool} ${image}
  wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${pool} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${rdp_image}
  wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${rdp_image}
  for cluster in ${CLUSTER1} ${CLUSTER2}; do
    CEPH_ARGS='' ceph --cluster ${cluster} osd pool rm ${pool} ${pool} --yes-i-really-really-mean-it
  done
  remove_image_retry ${CLUSTER2} ${POOL} ${rdp_image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${rdp_image} 'deleted'
  for i in 0 1 2 4 8 8 8 8 16 16; do
    sleep $i
    admin_daemons "${CLUSTER2}" rbd mirror status ${pool}/${image} || break
  done
  admin_daemons "${CLUSTER2}" rbd mirror status ${pool}/${image} && false
fi

testlog "TEST: snapshot rename"
snap_name='snap_rename'
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  enable_mirror ${CLUSTER2} ${POOL} ${image2}
fi
create_snapshot ${CLUSTER2} ${POOL} ${image2} "${snap_name}_0"
for i in `seq 1 20`; do
  rename_snapshot ${CLUSTER2} ${POOL} ${image2} "${snap_name}_$(expr ${i} - 1)" "${snap_name}_${i}"
done
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  mirror_image_snapshot ${CLUSTER2} ${POOL} ${image2}
fi
wait_for_snap_present ${CLUSTER1} ${POOL} ${image2} "${snap_name}_${i}"

unprotect_snapshot ${CLUSTER2} ${POOL} ${image4} 'snap1'
unprotect_snapshot ${CLUSTER2} ${POOL} ${image4} 'snap2'
for i in ${image2} ${image4}; do
  remove_image_retry ${CLUSTER2} ${POOL} ${i}
done

testlog "TEST: disable mirror while daemon is stopped"
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
if [ "${RBD_MIRROR_MODE}" = "journal" ]; then
  set_pool_mirror_mode ${CLUSTER2} ${POOL} 'image'
fi
disable_mirror ${CLUSTER2} ${POOL} ${image}
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  test_image_present ${CLUSTER1} ${POOL} ${image} 'present'
fi
start_mirrors ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
if [ "${RBD_MIRROR_MODE}" = "snapshot" ]; then
  enable_mirror ${CLUSTER2} ${POOL} ${image}
else
  set_pool_mirror_mode ${CLUSTER2} ${POOL} 'pool'
  enable_journaling ${CLUSTER2} ${POOL} ${image}
fi
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

testlog "TEST: non-default namespace image mirroring"
testlog " - replay"
create_image_and_enable_mirror ${CLUSTER2} ${POOL}/${NS1} ${image} ${RBD_MIRROR_MODE}
create_image_and_enable_mirror ${CLUSTER2} ${POOL}/${NS2} ${image} ${RBD_MIRROR_MODE}
wait_for_image_replay_started ${CLUSTER1} ${POOL}/${NS1} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL}/${NS2} ${image}
write_image ${CLUSTER2} ${POOL}/${NS1} ${image} 100
write_image ${CLUSTER2} ${POOL}/${NS2} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1} ${POOL}/${NS1} ${image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS2} ${POOL}/${NS2} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS1} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS2} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1} ${POOL}/${NS1} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS2} ${POOL}/${NS2} ${image}

testlog " - disable mirroring / delete image"
remove_image_retry ${CLUSTER2} ${POOL}/${NS1} ${image}
disable_mirror ${CLUSTER2} ${POOL}/${NS2} ${image}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS1} ${image} 'deleted'
wait_for_image_present ${CLUSTER1} ${POOL}/${NS2} ${image} 'deleted'
remove_image_retry ${CLUSTER2} ${POOL}/${NS2} ${image}

testlog "TEST: mirror to a different remote namespace"
testlog " - replay"
NS3=ns3
NS4=ns4
rbd --cluster ${CLUSTER1} namespace create ${POOL}/${NS3}
rbd --cluster ${CLUSTER2} namespace create ${POOL}/${NS4}
rbd --cluster ${CLUSTER1} mirror pool enable ${POOL}/${NS3} ${MIRROR_POOL_MODE} --remote-namespace ${NS4}
rbd --cluster ${CLUSTER2} mirror pool enable ${POOL}/${NS4} ${MIRROR_POOL_MODE} --remote-namespace ${NS3}
create_image_and_enable_mirror ${CLUSTER2} ${POOL}/${NS4} ${image} ${RBD_MIRROR_MODE}
wait_for_image_replay_started ${CLUSTER1} ${POOL}/${NS3} ${image}
write_image ${CLUSTER2} ${POOL}/${NS4} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS3} ${POOL}/${NS4} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS3} ${image}
compare_images  ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS3} ${POOL}/${NS4} ${image}

testlog " - disable mirroring and re-enable without remote-namespace"
remove_image_retry ${CLUSTER2} ${POOL}/${NS4} ${image}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS3} ${image} 'deleted'
rbd --cluster ${CLUSTER1} mirror pool disable ${POOL}/${NS3}
rbd --cluster ${CLUSTER2} mirror pool disable ${POOL}/${NS4}
rbd --cluster ${CLUSTER2} namespace create ${POOL}/${NS3}
rbd --cluster ${CLUSTER2} mirror pool enable ${POOL}/${NS3} ${MIRROR_POOL_MODE}
rbd --cluster ${CLUSTER1} mirror pool enable ${POOL}/${NS3} ${MIRROR_POOL_MODE}
create_image_and_enable_mirror ${CLUSTER2} ${POOL}/${NS3} ${image} ${RBD_MIRROR_MODE}
wait_for_image_replay_started ${CLUSTER1} ${POOL}/${NS3} ${image}
write_image ${CLUSTER2} ${POOL}/${NS3} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS3} ${POOL}/${NS3} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS3} ${image}
compare_images  ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS3} ${POOL}/${NS3} ${image}
remove_image_retry ${CLUSTER2} ${POOL}/${NS3} ${image}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS3} ${image} 'deleted'
rbd --cluster ${CLUSTER1} mirror pool disable ${POOL}/${NS3}
rbd --cluster ${CLUSTER2} mirror pool disable ${POOL}/${NS3}

testlog " - data pool"
dp_image=test_data_pool
create_image_and_enable_mirror ${CLUSTER2} ${POOL}/${NS1} ${dp_image} ${RBD_MIRROR_MODE} 128 --data-pool ${PARENT_POOL}
data_pool=$(get_image_data_pool ${CLUSTER2} ${POOL}/${NS1} ${dp_image})
test "${data_pool}" = "${PARENT_POOL}"
wait_for_image_replay_started ${CLUSTER1} ${POOL}/${NS1} ${dp_image}
data_pool=$(get_image_data_pool ${CLUSTER1} ${POOL}/${NS1} ${dp_image})
test "${data_pool}" = "${PARENT_POOL}"
write_image ${CLUSTER2} ${POOL}/${NS1} ${dp_image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1} ${POOL}/${NS1} ${dp_image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS1} ${dp_image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1} ${POOL}/${NS1} ${dp_image}
remove_image_retry ${CLUSTER2} ${POOL}/${NS1} ${dp_image}

testlog "TEST: simple image resync"
request_resync_image ${CLUSTER1} ${POOL} ${image} image_id
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  testlog "TEST: image resync while replayer is stopped"
  admin_daemons ${CLUSTER1} rbd mirror stop ${POOL}/${image}
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  request_resync_image ${CLUSTER1} ${POOL} ${image} image_id
  admin_daemons ${CLUSTER1} rbd mirror start ${POOL}/${image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
  admin_daemons ${CLUSTER1} rbd mirror start ${POOL}/${image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
  compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
fi

testlog "TEST: request image resync while daemon is offline"
stop_mirrors ${CLUSTER1}
request_resync_image ${CLUSTER1} ${POOL} ${image} image_id
start_mirrors ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
remove_image_retry ${CLUSTER2} ${POOL} ${image}

if [ "${RBD_MIRROR_MODE}" = "journal" ]; then
  testlog "TEST: client disconnect"
  image=laggy
  create_image ${CLUSTER2} ${POOL} ${image} 128 --journal-object-size 64K
  write_image ${CLUSTER2} ${POOL} ${image} 10

  testlog " - replay stopped after disconnect"
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  test -n "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
  disconnect_image ${CLUSTER2} ${POOL} ${image}
  test -z "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+error' 'disconnected'

  testlog " - replay started after resync requested"
  request_resync_image ${CLUSTER1} ${POOL} ${image} image_id
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  test -n "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
  compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

  testlog " - disconnected after max_concurrent_object_sets reached"
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    admin_daemons ${CLUSTER1} rbd mirror stop ${POOL}/${image}
    wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
    test -n "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
    set_image_meta ${CLUSTER2} ${POOL} ${image} \
	    conf_rbd_journal_max_concurrent_object_sets 1
    write_image ${CLUSTER2} ${POOL} ${image} 20 16384
    write_image ${CLUSTER2} ${POOL} ${image} 20 16384
    test -z "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
    set_image_meta ${CLUSTER2} ${POOL} ${image} \
	    conf_rbd_journal_max_concurrent_object_sets 0

    testlog " - replay is still stopped (disconnected) after restart"
    admin_daemons ${CLUSTER1} rbd mirror start ${POOL}/${image}
    wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
    wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+error' 'disconnected'
  fi

  testlog " - replay started after resync requested"
  request_resync_image ${CLUSTER1} ${POOL} ${image} image_id
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  test -n "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
  compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

  testlog " - rbd_mirroring_resync_after_disconnect config option"
  set_image_meta ${CLUSTER2} ${POOL} ${image} \
	  conf_rbd_mirroring_resync_after_disconnect true
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  image_id=$(get_image_id ${CLUSTER1} ${POOL} ${image})
  disconnect_image ${CLUSTER2} ${POOL} ${image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  test -n "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
  compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  set_image_meta ${CLUSTER2} ${POOL} ${image} \
	  conf_rbd_mirroring_resync_after_disconnect false
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}
  disconnect_image ${CLUSTER2} ${POOL} ${image}
  test -z "$(get_mirror_journal_position ${CLUSTER2} ${POOL} ${image})"
  wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
  wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+error' 'disconnected'
  remove_image_retry ${CLUSTER2} ${POOL} ${image}
fi

testlog "TEST: split-brain"
image=split-brain
create_image_and_enable_mirror ${CLUSTER2} ${POOL} ${image} ${RBD_MIRROR_MODE}
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
promote_image ${CLUSTER1} ${POOL} ${image} --force
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
write_image ${CLUSTER1} ${POOL} ${image} 10
demote_image ${CLUSTER1} ${POOL} ${image}
wait_for_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+error' 'split-brain'
request_resync_image ${CLUSTER1} ${POOL} ${image} image_id
wait_for_replaying_status_in_pool_dir ${CLUSTER1} ${POOL} ${image}
remove_image_retry ${CLUSTER2} ${POOL} ${image}

testlog "TEST: check if removed images' OMAP are removed"
start_mirrors ${CLUSTER2}
wait_for_image_in_omap ${CLUSTER1} ${POOL}
wait_for_image_in_omap ${CLUSTER2} ${POOL}

if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  # teuthology will trash the daemon
  testlog "TEST: no blocklists"
  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER1} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER2} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
fi
