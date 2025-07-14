#!/usr/bin/env bash
#
# rbd_mirror_group.sh - test rbd-mirror daemon for snapshot-based group mirroring
#
# The script starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#

if [ -n "${RBD_MIRROR_HIDE_BASH_DEBUGGING}" ]; then
  set -e
else  
  set -ex
fi  

RBD_MIRROR_INSTANCES=${RBD_MIRROR_INSTANCES:-1}
RBD_MIRROR_MODE=snapshot

. $(dirname $0)/rbd_mirror_helpers.sh

setup

testlog "TEST: create group and test replay"
start_mirrors ${CLUSTER1}
check_daemon_running ${CLUSTER1}
group=test-group
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${group}
wait_for_group_present "${CLUSTER1}" "${POOL}" "${group}" 0
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 0
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 0
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group} 'down+unknown'
fi

testlog "TEST: create another group and test replay of both"
group0=test-group0
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${group0}
wait_for_group_present "${CLUSTER1}" "${POOL}" "${group0}" 0
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group0} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group0} 'up+replaying' 0
wait_for_group_present ${CLUSTER1} ${POOL} ${group0} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 0
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group0} 'down+unknown'
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group} 'down+unknown'
fi

testlog "TEST: add image to group and test replay"
image=test-image
create_image ${CLUSTER2} ${POOL} ${image}

if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_add ${CLUSTER2} ${POOL}/${group} ${POOL}/${image}

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}"/"${group}"
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${group}"
  wait_for_group_not_present "${CLUSTER1}" "${POOL}" "${group}"
  group_image_add ${CLUSTER2} ${POOL}/${group} ${POOL}/${image}
  mirror_group_enable "${CLUSTER2}" "${POOL}/${group}"
  test_fields_in_group_info "${CLUSTER2}" "${POOL}/${group}" 'snapshot' 'enabled' 'true'
  wait_for_group_present "${CLUSTER2}" "${POOL}" "${group}" 1
fi  

wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
write_image ${CLUSTER2} ${POOL} ${image} 100
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog "TEST: test replay with remove image and later add the same"
if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_remove ${CLUSTER2} ${POOL}/${group} ${POOL}/${image}

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}"/"${group}"
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 0
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${group}"
  wait_for_group_not_present "${CLUSTER1}" "${POOL}" "${group}"
  group_image_remove ${CLUSTER2} ${POOL}/${group} ${POOL}/${image}
  
  mirror_group_enable "${CLUSTER2}" "${POOL}/${group}"
  wait_for_group_present "${CLUSTER1}" "${POOL}" "${group}" 0
fi  

wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 0

if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_add ${CLUSTER2} ${POOL}/${group} ${POOL}/${image}
  
  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}"/"${group}"
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${group}"
  wait_for_group_not_present "${CLUSTER1}" "${POOL}" "${group}"
  group_image_add ${CLUSTER2} ${POOL}/${group} ${POOL}/${image}
  mirror_group_enable "${CLUSTER2}" "${POOL}/${group}"
fi

wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1

: ' # different pools - not MVP
testlog "TEST: add image from a different pool to group and test replay"
image0=test-image-diff-pool
create_image ${CLUSTER2} ${PARENT_POOL} ${image0}
group_image_add ${CLUSTER2} ${POOL}/${group} ${PARENT_POOL}/${image0}
if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 1
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}"/"${group}"
fi
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 2
wait_for_image_present ${CLUSTER1} ${PARENT_POOL} ${image0} 'present'
write_image ${CLUSTER2} ${PARENT_POOL} ${image0} 100
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying'
compare_images ${PARENT_POOL} ${image0}
group_image_remove ${CLUSTER1} ${POOL}/${group} ${PARENT_POOL}/${image0}
if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 2
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}"/"${group}"
fi
'

testlog "TEST: create regular group snapshots and test replay"
snap=snap1
group_snap_create ${CLUSTER2} ${POOL}/${group} ${snap}
check_group_snap_exists "${CLUSTER2}" "${POOL}/${group}" "${snap}"
wait_for_group_replay_started  ${CLUSTER1} ${POOL}/${group} 1
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
check_group_snap_exists ${CLUSTER1} ${POOL}/${group} ${snap}
group_snap_remove ${CLUSTER2} ${POOL}/${group} ${snap}
check_group_snap_doesnt_exist ${CLUSTER2} ${POOL}/${group} ${snap}
# this next extra mirror_group_snapshot should not be needed to remove the snap on the secondary - waiting for fix TODO
mirror_group_snapshot ${CLUSTER2} ${POOL}/${group}
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
check_group_snap_doesnt_exist ${CLUSTER1} ${POOL}/${group} ${snap}

testlog "TEST: stop mirror, create group, start mirror and test replay"
stop_mirrors ${CLUSTER1}
group1=test-group1
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${group1}
image1=test-image1
create_image ${CLUSTER2} ${POOL} ${image1}
if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_add ${CLUSTER2} ${POOL}/${group1} ${POOL}/${image1}

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    mirror_group_snapshot "${CLUSTER2}" "${POOL}"/"${group1}" "${group_snap_id}"
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${group1}"
  group_image_add ${CLUSTER2} ${POOL}/${group1} ${POOL}/${image1}
  mirror_group_enable "${CLUSTER2}" "${POOL}/${group1}"
fi
start_mirrors ${CLUSTER1}
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group1} 1
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group1}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+replaying' 1
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group1} 'down+unknown' 0
fi
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog "TEST: test the first group is replaying after restart"
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  testlog "TEST: stop/start/restart group via admin socket"

  admin_daemons ${CLUSTER1} rbd mirror group stop ${POOL}/${group1}
  wait_for_group_replay_stopped ${CLUSTER1} ${POOL}/${group1} 1
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+stopped' 1

  admin_daemons ${CLUSTER1} rbd mirror group start ${POOL}/${group1}
  wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group1} 1
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+replaying' 1

  admin_daemons ${CLUSTER1} rbd mirror group restart ${POOL}/${group1}
  wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group1} 1
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+replaying' 1

  flush ${CLUSTER1}
  admin_daemons ${CLUSTER1} rbd mirror group status ${POOL}/${group1}
fi

testlog "TEST: add a large image to group and test replay"
big_image=test-image-big
create_image ${CLUSTER2} ${POOL} ${big_image} 1G
write_image ${CLUSTER2} ${POOL} ${big_image} 1024 4194304

if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_add ${CLUSTER2} ${POOL}/${group} ${POOL}/${big_image}
  
  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 1
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}"/"${group}"
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${group}"
  group_image_add ${CLUSTER2} ${POOL}/${group} ${POOL}/${big_image}
  mirror_group_enable "${CLUSTER2}" "${POOL}/${group}"
fi

wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 2
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group} 
test_images_in_latest_synced_group ${CLUSTER1} ${POOL}/${group} 2
if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_remove ${CLUSTER2} ${POOL}/${group} ${POOL}/${big_image}

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 1
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}"/"${group}"
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}"/"${group}" 'up+replaying' 1
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${group}"
  group_image_remove ${CLUSTER2} ${POOL}/${group} ${POOL}/${big_image}
  mirror_group_enable "${CLUSTER2}" "${POOL}/${group}"
fi  

remove_image_retry ${CLUSTER2} ${POOL} ${big_image}
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
test_images_in_latest_synced_group ${CLUSTER1} ${POOL}/${group} 1

testlog "TEST: test group rename"
new_name="${group}_RENAMED"
group_rename ${CLUSTER2} ${POOL}/${group} ${POOL}/${new_name}
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${new_name} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${new_name} 'up+replaying' 1
group_rename ${CLUSTER2} ${POOL}/${new_name} ${POOL}/${group}
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 1

# TODO add new image to syncing snapshot
#  rollback needs to remove new image before rolling back
# also remove image and rollback
testlog "TEST: failover and failback"
start_mirrors ${CLUSTER2}

testlog " - demote and promote same cluster"
mirror_group_demote ${CLUSTER2} ${POOL}/${group1}
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group1} 'snapshot' 'enabled' 'false'
wait_for_group_replay_stopped ${CLUSTER1} ${POOL}/${group1} 0

wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+unknown' 0
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group1} 'up+unknown' 0
mirror_group_promote ${CLUSTER2} ${POOL}/${group1}
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group1} 'snapshot' 'enabled' 'true'

wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group1} 1

write_image ${CLUSTER2} ${POOL} ${image1} 100
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group1}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog " - failover (unmodified)"
mirror_group_demote ${CLUSTER2} ${POOL}/${group}
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group} 'snapshot' 'enabled' 'false'
wait_for_group_replay_stopped ${CLUSTER1} ${POOL}/${group} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+unknown' 0
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group} 'up+unknown' 0
mirror_group_promote ${CLUSTER1} ${POOL}/${group}
test_fields_in_group_info ${CLUSTER1} ${POOL}/${group} 'snapshot' 'enabled' 'true'
wait_for_group_replay_started ${CLUSTER2} ${POOL}/${group} 1

testlog " - failback (unmodified)"
mirror_group_demote ${CLUSTER1} ${POOL}/${group}
test_fields_in_group_info ${CLUSTER1} ${POOL}/${group} 'snapshot' 'enabled' 'false'
wait_for_group_replay_stopped ${CLUSTER2} ${POOL}/${group} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+unknown' 0
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group} 'up+unknown' 0
mirror_group_promote ${CLUSTER2} ${POOL}/${group}
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group} 'snapshot' 'enabled' 'true'
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 1
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group} 'up+stopped' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog " - failover"
mirror_group_demote ${CLUSTER2} ${POOL}/${group1}
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group1} 'snapshot' 'enabled' 'false'
wait_for_group_replay_stopped ${CLUSTER1} ${POOL}/${group1} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+unknown' 0
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group1} 'up+unknown' 0
mirror_group_promote ${CLUSTER1} ${POOL}/${group1}
test_fields_in_group_info ${CLUSTER1} ${POOL}/${group1} 'snapshot' 'enabled' 'true'
wait_for_group_replay_started ${CLUSTER2} ${POOL}/${group1} 1
write_image ${CLUSTER1} ${POOL} ${image1} 100
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER2} ${CLUSTER1} ${POOL}/${group1}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+stopped' 1
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group1} 'up+replaying' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog " - failback to cluster2"
mirror_group_demote ${CLUSTER1} ${POOL}/${group1}
test_fields_in_group_info ${CLUSTER1} ${POOL}/${group1} 'snapshot' 'enabled' 'false'
wait_for_group_replay_stopped ${CLUSTER2} ${POOL}/${group1} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+unknown' 0
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group1} 'up+unknown' 0
mirror_group_promote ${CLUSTER2} ${POOL}/${group1}
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group1} 'snapshot' 'enabled' 'true'
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group1} 1
write_image ${CLUSTER2} ${POOL} ${image1} 100
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group1}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group1} 'up+replaying' 1
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group1} 'up+stopped' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog " - force promote cluster1"
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
write_image ${CLUSTER2} ${POOL} ${image} 100
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${group}
stop_mirrors ${CLUSTER1}
mirror_group_promote ${CLUSTER1} ${POOL}/${group} '--force'
start_mirrors ${CLUSTER1}
test_fields_in_group_info ${CLUSTER1} ${POOL}/${group} 'snapshot' 'enabled' 'true'
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group} 'snapshot' 'enabled' 'true'

wait_for_group_replay_stopped ${CLUSTER1} ${POOL}/${group} 1
wait_for_group_replay_stopped ${CLUSTER2} ${POOL}/${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+stopped' 1
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL}/${group} 'up+stopped' 1
write_image ${CLUSTER1} ${POOL} ${image} 100
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_present ${CLUSTER2} ${POOL} ${group} 1
if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_remove ${CLUSTER1} ${POOL}/${group} ${POOL}/${image}
  wait_for_group_present ${CLUSTER1} ${POOL} ${group} 0
  remove_image_retry ${CLUSTER1} ${POOL} ${image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
  # need to stop the group replayer to ensure that it doesn't recreate the group
  # as a secondary immediately after the local group is removed
  test_fields_in_group_info "${CLUSTER1}" "${POOL}/${group}" 'snapshot' 'enabled' 'true'
  admin_daemons ${CLUSTER1} rbd mirror group stop ${POOL}/${group}
  group_remove ${CLUSTER1} ${POOL}/${group}
  wait_for_group_not_present ${CLUSTER1} ${POOL} ${group}
else
  # need to stop the group replayer to ensure that it doesn't recreate the group
  # as a secondary immediately after the local group is removed
  test_fields_in_group_info "${CLUSTER1}" "${POOL}/${group}" 'snapshot' 'enabled' 'true'
  admin_daemons ${CLUSTER1} rbd mirror group stop ${POOL}/${group}
  group_remove ${CLUSTER1} ${POOL}/${group}
  wait_for_group_not_present ${CLUSTER1} ${POOL} ${group}
  remove_image_retry ${CLUSTER1} ${POOL} ${image}
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
fi
admin_daemons ${CLUSTER1} rbd mirror group start ${POOL}/${group}
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 1
test_fields_in_group_info "${CLUSTER1}" "${POOL}/${group}" 'snapshot' 'enabled' 'false'
wait_for_group_present ${CLUSTER2} ${POOL} ${group} 1

testlog "TEST: disable mirroring / delete non-primary group"
mirror_group_disable ${CLUSTER2} ${POOL}/${group}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
wait_for_group_not_present ${CLUSTER1} ${POOL} ${group}
mirror_group_enable ${CLUSTER2} ${POOL}/${group}
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'

testlog "TEST: disable mirror while daemon is stopped"
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
mirror_group_disable ${CLUSTER2} ${POOL}/${group}
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_present ${CLUSTER1} ${POOL} ${group} 1
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
fi
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
wait_for_group_not_present ${CLUSTER1} ${POOL} ${group}
mirror_group_enable ${CLUSTER2} ${POOL}/${group}
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1

testlog "TEST: non-default namespace group mirroring"
testlog " - replay"
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${NS1}/${group}
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${NS2}/${group}
create_image ${CLUSTER2} ${POOL}/${NS1} ${image}
create_image ${CLUSTER2} ${POOL}/${NS2} ${image}

if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_add ${CLUSTER2} ${POOL}/${NS1}/${group} ${POOL}/${NS1}/${image}
  group_image_add ${CLUSTER2} ${POOL}/${NS2}/${group} ${POOL}/${NS2}/${image}
  
  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}/${NS1}/${group}" 'up+replaying' 0
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}/${NS2}/${group}" 'up+replaying' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}/${NS1}/${group}"
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}/${NS2}/${group}"
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${NS1}/${group}"
  mirror_group_disable "${CLUSTER2}" "${POOL}/${NS2}/${group}"
  group_image_add ${CLUSTER2} ${POOL}/${NS1}/${group} ${POOL}/${NS1}/${image}
  group_image_add ${CLUSTER2} ${POOL}/${NS2}/${group} ${POOL}/${NS2}/${image}
  mirror_group_enable "${CLUSTER2}" "${POOL}/${NS1}/${group}"
  mirror_group_enable "${CLUSTER2}" "${POOL}/${NS2}/${group}"
fi

wait_for_group_replay_started ${CLUSTER1} ${POOL}/${NS1}/${group} 1
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${NS2}/${group} 1
write_image ${CLUSTER2} ${POOL}/${NS1} ${image} 100
write_image ${CLUSTER2} ${POOL}/${NS2} ${image} 100
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1}/${group}
mirror_group_snapshot_and_wait_for_sync_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS2}/${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS1}/${group} 'up+replaying' 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS2}/${group} 'up+replaying' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1} ${POOL}/${NS1} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS2} ${POOL}/${NS2} ${image}

testlog " - disable mirroring / remove group"
if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
  group_image_remove ${CLUSTER2} ${POOL}/${NS1}/${group} ${POOL}/${NS1}/${image}

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${CLUSTER1}" "${POOL}/${NS1}/${group}" 'up+replaying' 1
    mirror_group_snapshot_and_wait_for_sync_complete "${CLUSTER1}" "${CLUSTER2}" "${POOL}/${NS1}/${group}"
  fi
else
  mirror_group_disable "${CLUSTER2}" "${POOL}/${NS1}/${group}"
  group_image_remove ${CLUSTER2} ${POOL}/${NS1}/${group} ${POOL}/${NS1}/${image}
  mirror_group_enable "${CLUSTER2}" "${POOL}/${NS1}/${group}"
fi  
wait_for_group_present ${CLUSTER1} ${POOL}/${NS1} ${group} 0
remove_image_retry ${CLUSTER2} ${POOL}/${NS1} ${image}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS1} ${image} 'deleted'
group_remove ${CLUSTER2} ${POOL}/${NS1}/${group}
wait_for_group_not_present ${CLUSTER1} ${POOL} ${NS1}/${group}
mirror_group_disable ${CLUSTER2} ${POOL}/${NS2}/${group}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS2} ${image} 'deleted'
wait_for_group_not_present ${CLUSTER1} ${POOL} ${NS2}/${group}

testlog "TEST: simple group resync"
image_id=$(get_image_id ${CLUSTER1} ${POOL} ${image})
wait_for_image_present ${CLUSTER2} ${POOL} ${image} 'present'
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
mirror_group_resync ${CLUSTER1} ${POOL}/${group}
# original image is deleted and recreated
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog "TEST: request group resync while daemon is offline"
image_id=$(get_image_id ${CLUSTER1} ${POOL} ${image})
stop_mirrors ${CLUSTER1}
mirror_group_resync ${CLUSTER1} ${POOL}/${group}
start_mirrors ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 1
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog "TEST: split-brain"
stop_mirrors ${CLUSTER1}
mirror_group_promote ${CLUSTER1} ${POOL}/${group} --force
start_mirrors ${CLUSTER1}
test_fields_in_group_info ${CLUSTER1} ${POOL}/${group} 'snapshot' 'enabled' 'true'
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group} 'snapshot' 'enabled' 'true'
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+stopped' 1
write_image ${CLUSTER1} ${POOL} ${image} 10
mirror_group_demote ${CLUSTER1} ${POOL}/${group}
test_fields_in_group_info ${CLUSTER1} ${POOL}/${group} 'snapshot' 'enabled' 'false'
test_fields_in_group_info ${CLUSTER2} ${POOL}/${group} 'snapshot' 'enabled' 'true'
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+error' 1 'split-brain'

get_id_from_group_info ${CLUSTER1} ${POOL}/${group} group_id_before
mirror_group_resync ${CLUSTER1} ${POOL}/${group}
wait_for_group_id_changed ${CLUSTER1} ${POOL}/${group} ${group_id_before}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${group} 'up+replaying' 1

#TODO: Fix blocklisting IP's which are consequence of "TEST: stop mirror, create group, start mirror and test replay"
#if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
# teuthology will trash the daemon
#  testlog "TEST: no blocklists"
#  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER1} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
#  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER2} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
#fi

testlog "TEST: end"
