#!/bin/sh
#
# rbd_mirror.sh - test rbd-mirror daemon
#
# The scripts starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#

if [ -n "${CEPH_REF}" ]; then
  wget -O rbd_mirror_helpers.sh "https://git.ceph.com/?p=ceph.git;a=blob_plain;hb=$CEPH_REF;f=qa/workunits/rbd/rbd_mirror_helpers.sh"
  . ./rbd_mirror_helpers.sh
else
  . $(dirname $0)/rbd_mirror_helpers.sh
fi

testlog "TEST: add image and test replay"
start_mirror ${CLUSTER1}
image=test
create_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'down+unknown'
fi
compare_images ${POOL} ${image}

testlog "TEST: stop mirror, add image, start mirror and test replay"
stop_mirror ${CLUSTER1}
image1=test1
create_image ${CLUSTER2} ${POOL} ${image1}
write_image ${CLUSTER2} ${POOL} ${image1} 100
start_mirror ${CLUSTER1}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image1}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+replaying' 'master_position'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image1} 'down+unknown'
fi
compare_images ${POOL} ${image1}

testlog "TEST: test the first image is replaying after restart"
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

testlog "TEST: stop/start/restart mirror via admin socket"
admin_daemon ${CLUSTER1} rbd mirror stop
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror start
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror restart
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror stop
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror restart
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror stop ${POOL} ${CLUSTER2}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror start ${POOL}/${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

admin_daemon ${CLUSTER1} rbd mirror start
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror start ${POOL} ${CLUSTER2}

admin_daemon ${CLUSTER1} rbd mirror restart ${POOL}/${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

admin_daemon ${CLUSTER1} rbd mirror restart ${POOL} ${CLUSTER2}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror flush
admin_daemon ${CLUSTER1} rbd mirror status

testlog "TEST: failover and failback"
start_mirror ${CLUSTER2}

# demote and promote same cluster
demote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
promote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

# failover
demote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
promote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER2} ${POOL} ${image}
write_image ${CLUSTER1} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

# failback
demote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
promote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
compare_images ${POOL} ${image}

testlog "TEST: cloned images"
parent_image=test_parent
parent_snap=snap
create_image ${CLUSTER2} ${PARENT_POOL} ${parent_image}
write_image ${CLUSTER2} ${PARENT_POOL} ${parent_image} 100
create_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
protect_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}

clone_image=test_clone
clone_image ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap} ${POOL} ${clone_image}
write_image ${CLUSTER2} ${POOL} ${clone_image} 100

enable_mirror ${CLUSTER2} ${PARENT_POOL} ${parent_image}
wait_for_image_replay_started ${CLUSTER1} ${PARENT_POOL} ${parent_image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${PARENT_POOL} ${parent_image}
test_status_in_pool_dir ${CLUSTER1} ${PARENT_POOL} ${parent_image} 'up+replaying' 'master_position'
compare_images ${PARENT_POOL} ${parent_image}

wait_for_image_replay_started ${CLUSTER1} ${POOL} ${clone_image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${clone_image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${clone_image} 'up+replaying' 'master_position'
compare_images ${POOL} ${clone_image}

testlog "TEST: disable mirroring / delete non-primary image"
image2=test2
image3=test3
image4=test4
image5=test5
for i in ${image2} ${image3} ${image4} ${image5}; do
  create_image ${CLUSTER2} ${POOL} ${i}
  write_image ${CLUSTER2} ${POOL} ${i} 100
  create_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
  create_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  if [ "${i}" = "${image4}" ] || [ "${i}" = "${image5}" ]; then
    protect_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
    protect_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  fi
  write_image ${CLUSTER2} ${POOL} ${i} 100
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
  remove_image ${CLUSTER2} ${POOL} ${i}
done

for i in ${image2} ${image3} ${image4} ${image5}; do
  wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'deleted'
done

set_pool_mirror_mode ${CLUSTER2} ${POOL} 'pool'
for i in ${image2} ${image4}; do
  wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'present'
  wait_for_snap_present ${CLUSTER1} ${POOL} ${i} 'snap2'
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${i}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${i}
  compare_images ${POOL} ${i}
done

testlog "TEST: snapshot rename"
snap_name='snap_rename'
create_snapshot ${CLUSTER2} ${POOL} ${image2} "${snap_name}_0"
for i in `seq 1 20`; do
  rename_snapshot ${CLUSTER2} ${POOL} ${image2} "${snap_name}_$(expr ${i} - 1)" "${snap_name}_${i}"
done
wait_for_snap_present ${CLUSTER1} ${POOL} ${image2} "${snap_name}_${i}"

testlog "TEST: disable mirror while daemon is stopped"
stop_mirror ${CLUSTER1}
stop_mirror ${CLUSTER2}
set_pool_mirror_mode ${CLUSTER2} ${POOL} 'image'
disable_mirror ${CLUSTER2} ${POOL} ${image}
test_image_present ${CLUSTER1} ${POOL} ${image} 'present'
start_mirror ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
set_pool_mirror_mode ${CLUSTER2} ${POOL} 'pool'
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

testlog "TEST: simple image resync"
request_resync_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

testlog "TEST: image resync while replayer is stopped"
admin_daemon ${CLUSTER1} rbd mirror stop ${POOL}/${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
request_resync_image ${CLUSTER1} ${POOL} ${image}
admin_daemon ${CLUSTER1} rbd mirror start ${POOL}/${image}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
admin_daemon ${CLUSTER1} rbd mirror start ${POOL}/${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

testlog "TEST: request image resync while daemon is offline"
stop_mirror ${CLUSTER1}
request_resync_image ${CLUSTER1} ${POOL} ${image}
start_mirror ${CLUSTER1}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

echo OK
