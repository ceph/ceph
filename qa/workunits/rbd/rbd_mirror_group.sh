#!/usr/bin/env bash
#
# rbd_mirror_group.sh - test rbd-mirror daemon for snapshot-based group mirroring
#
# The script starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#

set -ex

MIRROR_POOL_MODE=image
MIRROR_IMAGE_MODE=snapshot

. $(dirname $0)/rbd_mirror_helpers.sh

setup
testlog "TEST: create group and test replay"
start_mirrors ${CLUSTER1}
group=test-group
create_group_and_enable_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 0
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'present'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'down+unknown'
fi

testlog "TEST: add image to group and test replay"
image=test-image
create_image ${CLUSTER2} ${POOL} ${image}
group_add_image ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog "TEST: test replay with remove image and later add the same"
group_remove_image ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 0
group_add_image ${CLUSTER2} ${POOL} ${group} ${POOL} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1

testlog "TEST: stop mirror, create group, start mirror and test replay"
stop_mirrors ${CLUSTER1}
group1=test-group1
create_group_and_enable_mirror ${CLUSTER2} ${POOL} ${group1}
image1=test-image1
create_image ${CLUSTER2} ${POOL} ${image1}
group_add_image ${CLUSTER2} ${POOL} ${group1} ${POOL} ${image1}
start_mirrors ${CLUSTER1}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 1
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group1}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'down+unknown'
fi
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog "TEST: test the first group is replaying after restart"
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  testlog "TEST: stop/start/restart group via admin socket"

  admin_daemons ${CLUSTER1} rbd mirror group stop ${POOL}/${group1}
  wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group1} 1
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+stopped'

  admin_daemons ${CLUSTER1} rbd mirror group start ${POOL}/${group1}
  wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 1
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'

  admin_daemons ${CLUSTER1} rbd mirror group restart ${POOL}/${group1}
  wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 1
  wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'

  flush ${CLUSTER1}
  admin_daemons ${CLUSTER1} rbd mirror group status ${POOL}/${group1}
fi

testlog "TEST: test group rename"
new_name="${group}_RENAMED"
rename_group ${CLUSTER2} ${POOL} ${group} ${new_name}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${new_name} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${new_name} 'up+replaying'
rename_group ${CLUSTER2} ${POOL} ${new_name} ${group}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'

testlog "TEST: failover and failback"
start_mirrors ${CLUSTER2}

testlog " - demote and promote same cluster"
demote_group ${CLUSTER2} ${POOL} ${group1}
wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group1} 1

wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+stopped'
promote_group ${CLUSTER2} ${POOL} ${group1}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 1

write_image ${CLUSTER2} ${POOL} ${image1} 100
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group1}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog " - failover (unmodified)"
demote_group ${CLUSTER2} ${POOL} ${group}
wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+stopped'
promote_group ${CLUSTER1} ${POOL} ${group}
wait_for_group_replay_started ${CLUSTER2} ${POOL} ${group} 1

testlog " - failback (unmodified)"
demote_group ${CLUSTER1} ${POOL} ${group}
wait_for_group_replay_stopped ${CLUSTER2} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+stopped'
promote_group ${CLUSTER2} ${POOL} ${group}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+stopped'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog " - failover"
demote_group ${CLUSTER2} ${POOL} ${group1}
wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group1} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+stopped'
promote_group ${CLUSTER1} ${POOL} ${group1}
wait_for_group_replay_started ${CLUSTER2} ${POOL} ${group1} 1
write_image ${CLUSTER1} ${POOL} ${image1} 100
wait_for_group_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${group1}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+replaying'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog " - failback"
demote_group ${CLUSTER1} ${POOL} ${group1}
wait_for_group_replay_stopped ${CLUSTER2} ${POOL} ${group1} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+stopped'
promote_group ${CLUSTER2} ${POOL} ${group1}
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group1} 1
write_image ${CLUSTER2} ${POOL} ${image1} 100
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group1}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group1} 'up+replaying'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group1} 'up+stopped'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image1}

testlog " - force promote"
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${group}
promote_group ${CLUSTER1} ${POOL} ${group} '--force'

wait_for_group_replay_stopped ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_replay_stopped ${CLUSTER2} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+stopped'
wait_for_group_status_in_pool_dir ${CLUSTER2} ${POOL} ${group} 'up+stopped'
write_image ${CLUSTER1} ${POOL} ${image} 100
write_image ${CLUSTER2} ${POOL} ${image} 100
group_remove_image ${CLUSTER1} ${POOL} ${group} ${POOL} ${image}
remove_image_retry ${CLUSTER1} ${POOL} ${image}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
remove_group ${CLUSTER1} ${POOL} ${group}

testlog "TEST: disable mirroring / delete non-primary group"
disable_group_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'deleted'
enable_group_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'present' 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'

testlog "TEST: disable mirror while daemon is stopped"
stop_mirrors ${CLUSTER1}
stop_mirrors ${CLUSTER2}
disable_group_mirror ${CLUSTER2} ${POOL} ${group}
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'present' 1
  wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
fi
start_mirrors ${CLUSTER1}
start_mirrors ${CLUSTER2}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'deleted'
enable_group_mirror ${CLUSTER2} ${POOL} ${group}
wait_for_group_present ${CLUSTER1} ${POOL} ${group} 'present' 1
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1

testlog "TEST: non-default namespace group mirroring"
testlog " - replay"
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${NS1} ${group}
create_group_and_enable_mirror ${CLUSTER2} ${POOL}/${NS2} ${group}
create_image ${CLUSTER2} ${POOL}/${NS1} ${image}
create_image ${CLUSTER2} ${POOL}/${NS2} ${image}
group_add_image ${CLUSTER2} ${POOL}/${NS1} ${group} ${POOL}/${NS1} ${image}
group_add_image ${CLUSTER2} ${POOL}/${NS2} ${group} ${POOL}/${NS2} ${image}
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${NS1} ${group} 1
wait_for_group_replay_started ${CLUSTER1} ${POOL}/${NS2} ${group} 1
write_image ${CLUSTER2} ${POOL}/${NS1} ${image} 100
write_image ${CLUSTER2} ${POOL}/${NS2} ${image} 100
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1} ${group}
wait_for_group_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS2} ${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS1} ${group} 'up+replaying'
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL}/${NS2} ${group} 'up+replaying'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS1} ${POOL}/${NS1} ${image}
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL}/${NS2} ${POOL}/${NS2} ${image}

testlog " - disable mirroring / remove group"
group_remove_image ${CLUSTER2} ${POOL}/${NS1} ${group} ${POOL}/${NS1} ${image}
remove_image_retry ${CLUSTER2} ${POOL}/${NS1} ${image}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS1} ${image} 'deleted'
remove_group ${CLUSTER1} ${POOL}/${NS1} ${group}
wait_for_group_present ${CLUSTER1} ${POOL}/${NS1} ${group} 'deleted'
disable_group_mirror ${CLUSTER2} ${POOL}/${NS2} ${group}
wait_for_image_present ${CLUSTER1} ${POOL}/${NS2} ${image} 'deleted'
wait_for_group_present ${CLUSTER1} ${POOL}/${NS2} ${group} 'deleted'

testlog "TEST: simple group resync"
image_id=$(get_image_id ${CLUSTER1} ${POOL} ${image})
wait_for_image_present ${CLUSTER2} ${POOL} ${image} 'present'
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
request_resync_group ${CLUSTER1} ${POOL} ${group}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog "TEST: request group resync while daemon is offline"
image_id=$(get_image_id ${CLUSTER1} ${POOL} ${image})
stop_mirrors ${CLUSTER1}
request_resync_group ${CLUSTER1} ${POOL} ${group}
start_mirrors ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted' ${image_id}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_group_replay_started ${CLUSTER1} ${POOL} ${group} 1
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'
compare_images ${CLUSTER1} ${CLUSTER2} ${POOL} ${POOL} ${image}

testlog "TEST: split-brain"
promote_group ${CLUSTER1} ${POOL} ${group} --force
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+stopped'
write_image ${CLUSTER1} ${POOL} ${image} 10
demote_group ${CLUSTER1} ${POOL} ${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+error' 'split-brain'
request_resync_group ${CLUSTER1} ${POOL} ${group}
wait_for_group_status_in_pool_dir ${CLUSTER1} ${POOL} ${group} 'up+replaying'

#TODO: Fix blocklisting IP's which are consequence of "TEST: stop mirror, create group, start mirror and test replay"
#if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
# teuthology will trash the daemon
#  testlog "TEST: no blocklists"
#  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER1} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
#  CEPH_ARGS='--id admin' ceph --cluster ${CLUSTER2} osd blocklist ls 2>&1 | grep -q "listed 0 entries"
#fi
