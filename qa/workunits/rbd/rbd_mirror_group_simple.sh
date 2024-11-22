#!/usr/bin/env bash
#
# rbd_mirror_group_simple.sh
#
# This script has a set of tests that should pass when run.
# It may repeat some of the tests from rbd_mirror_group.sh, but only those that are known to work
# It has a number of extra tests that imclude multiple images in a group
#

export RBD_MIRROR_NOCLEANUP=1
export RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror
export RBD_MIRROR_SHOW_CMD=1
export RBD_MIRROR_MODE=snapshot

. $(dirname $0)/rbd_mirror_helpers.sh

test_create_group_with_images_then_mirror()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4
  local image_prefix=$5
  local disable_before_remove=$6

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" 5
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" 5

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  # rbd group list poolName  (check groupname appears in output list)
  # do this before checking for replay_started because queries directed at the daemon fail with an unhelpful
  # error message before the group appears on the remote cluster
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 5
  check_daemon_running "${secondary_cluster}"

  # ceph --daemon mirror group status groupName
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 5
  check_daemon_running "${secondary_cluster}"

  # rbd mirror group status groupName
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 5

  check_daemon_running "${secondary_cluster}"
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi
  check_daemon_running "${secondary_cluster}"

  if [ 'false' != "${disable_before_remove}" ]; then
      mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  fi

  group_remove "${primary_cluster}" "${pool}/${group}"
  check_daemon_running "${secondary_cluster}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" 5
}

test_create_group_mirror_then_add_images()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4
  local image_prefix=$5
  local disable_before_remove=$6

  group_create "${primary_cluster}" "${pool}/${group}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  images_create "${primary_cluster}" "${pool}/${image_prefix}" 5
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" 5

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 5
  check_daemon_running "${secondary_cluster}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 5
  check_daemon_running "${secondary_cluster}"

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 5

  check_daemon_running "${secondary_cluster}"
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi
  check_daemon_running "${secondary_cluster}"

  if [ 'false' != "${disable_before_remove}" ]; then
      mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  fi

  group_remove "${primary_cluster}" "${pool}/${group}"
  check_daemon_running "${secondary_cluster}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" 5
}

test_empty_group()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4

  group_create "${primary_cluster}" "${pool}/${group}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
  check_daemon_running "${secondary_cluster}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
  check_daemon_running "${secondary_cluster}"

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0

  check_daemon_running "${secondary_cluster}"
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi
  check_daemon_running "${secondary_cluster}"

  try_cmd "rbd --cluster ${secondary_cluster} group snap list ${pool}/${group}" || :
  try_cmd "rbd --cluster ${primary_cluster} group snap list ${pool}/${group}" || :

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"

  try_cmd "rbd --cluster ${secondary_cluster} group snap list ${pool}/${group}" || :
  try_cmd "rbd --cluster ${primary_cluster} group snap list ${pool}/${group}" || :

  group_remove "${primary_cluster}" "${pool}/${group}"
  check_daemon_running "${secondary_cluster}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"
}

test_empty_groups()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group0=$4
  local group1=$5

  group_create "${primary_cluster}" "${pool}/${group0}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' 0
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'down+unknown' 0
  fi

  group_create "${primary_cluster}" "${pool}/${group1}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group1}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group1}" 0

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying' 0
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'down+unknown' 0
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group1}" 'down+unknown' 0
  fi

  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  mirror_group_disable "${primary_cluster}" "${pool}/${group1}"

  group_remove "${primary_cluster}" "${pool}/${group1}"
  group_remove "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group1}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group1}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"
  check_daemon_running "${secondary_cluster}"
}

# This test is not MVP
test_images_different_pools()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool0=$3
  local pool1=$4
  local group=$5
  local image_prefix=$6

  group_create "${primary_cluster}" "${pool0}/${group}"
  mirror_group_enable "${primary_cluster}" "${pool0}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool0}" "${group}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool0}"/"${group}" 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool0}"/"${group}" 'up+replaying' 0
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool0}"/"${group}" 'down+unknown' 0
  fi

  image_create "${primary_cluster}" "${pool0}/${image_prefix}0"
  group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool0}/${image_prefix}0"
  image_create "${primary_cluster}" "${pool1}/${image_prefix}1" 
  group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool1}/${image_prefix}1" 

  wait_for_group_present "${secondary_cluster}" "${pool0}" "${group}" 2
  wait_for_group_replay_started "${secondary_cluster}" "${pool0}"/"${group}" 2
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool0}"/"${group}" 'up+replaying' 2

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool0}"/"${group}" 'down+unknown' 0
  fi

  group_remove "${primary_cluster}" "${pool0}/${group}"

  wait_for_group_not_present "${primary_cluster}" "${pool0}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool0}" "${group}"

  image_remove "${primary_cluster}" "${pool0}/${image_prefix}0"
  image_remove "${primary_cluster}" "${pool1}/${image_prefix}1"
}

test_create_group_with_images_then_mirror_with_regular_snapshots()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4
  local image_prefix=$5

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" 5
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" 5

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 5
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 5
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 5

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  snap='regular_snap'
  check_group_snap_doesnt_exist "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"

  group_snap_create "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_exists "${primary_cluster}" "${pool}/${group}" "${snap}"
  # snap is currently copied to secondary cluster, where it remains in the "incomplete" state, but this is maybe incorrect - see slack thread TODO
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_exists "${secondary_cluster}" "${pool}/${group}" "${snap}"

  group_snap_remove "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_doesnt_exist "${primary_cluster}" "${pool}/${group}" "${snap}"
  # this next extra mirror_group_snapshot should not be needed - waiting for fix TODO
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group}"
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"

  #TODO DEFECT
  #exit 0
  # if I exit at this point and then
  # - force disable mirroring for the group on the secondary
  # - remove the group on the secondary
  # we end up with snapshots that belong to the group being left lying around.
  # see discussion in slack, might need defect

  #TODO also try taking multiple regular group snapshots and check the behaviour there

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" 5
}

test_create_group_with_large_image()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool0=$3
  local group=$4
  local image_prefix=$5

  group_create "${primary_cluster}" "${pool0}/${group}"
  image_create "${primary_cluster}" "${pool0}/${image_prefix}"
  group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool0}/${image_prefix}"

  mirror_group_enable "${primary_cluster}" "${pool0}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool0}" "${group}" 1
  wait_for_group_replay_started "${secondary_cluster}" "${pool0}"/"${group}" 1
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool0}"/"${group}" 'up+replaying' 1

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool0}"/"${group}" 'down+unknown' 0
  fi

  big_image=test-image-big
  image_create "${primary_cluster}" "${pool0}/${big_image}" 4G
  group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool0}/${big_image}"

  write_image "${primary_cluster}" "${pool0}" "${big_image}" 1024 4194304
  wait_for_group_replay_started "${secondary_cluster}" "${pool0}/${group}" 2
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool0}/${group}"

  test_group_and_image_sync_status "${secondary_cluster}" "${primary_cluster}" "${pool0}/${group}" "${pool0}/${big_image}"

  group_image_remove "${primary_cluster}" "${pool0}/${group}" "${pool0}/${big_image}"
  remove_image_retry "${primary_cluster}" "${pool0}" "${big_image}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool0}/${group}" 1
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool0}/${group}"

  mirror_group_disable "${primary_cluster}" "${pool0}/${group}"
  group_remove "${primary_cluster}" "${pool0}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool0}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool0}" "${group}"

  image_remove "${primary_cluster}" "${pool0}/${image_prefix}"
}

test_create_group_with_multiple_images_do_io()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4
  local image_prefix=$5

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" 5
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" 5

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 5

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 5
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 5

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  local io_count=1024
  local io_size=4096
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" "${io_count}" "${io_size}"

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group}"

exit 0
  # TODO this test needs finishing.  The next function is not yet complete - see the TODO in it
  test_group_and_image_sync_status "${secondary_cluster}" "${primary_cluster}" "${pool}/${group}" "${pool1}/${big_image}"


  snap='regular_snap'
  group_snap_create "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_exists "${primary_cluster}" "${pool}/${group}" "${snap}"

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_exists "${secondary_cluster}" "${pool}/${group}" "${snap}"

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" 5
}

set -ex

# If the tmpdir or cluster conf file doesn't exist then assume that the cluster needs setting up
if [ ! -d "${RBD_MIRROR_TEMDIR}" ] || [ ! -f "${RBD_MIRROR_TEMDIR}"'/cluster1.conf' ]
then
    setup
fi
export RBD_MIRROR_USE_EXISTING_CLUSTER=1

# rbd_mirror_helpers assumes that we are running from tmpdir
setup_tempdir

# see if we need to (re)start rbd-mirror deamon
pid=$(cat "$(daemon_pid_file "${CLUSTER1}")" 2>/dev/null) || :
if [ -z "${pid}" ]
then
    start_mirrors "${CLUSTER1}"
fi
check_daemon_running "${CLUSTER1}" 'restart'

group0=test-group0
group1=test-group1
pool0=mirror
pool1=mirror_parent
image_prefix=test-image

testlog "TEST: empty group"
test_empty_group "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}"

testlog "TEST: empty group with namespace"
test_empty_group "${CLUSTER2}" "${CLUSTER1}" "${pool0}/${NS1}" "${group0}"

testlog "TEST: create group with images then enable mirroring.  Remove group without disabling mirroring"
test_create_group_with_images_then_mirror "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'false'

testlog "TEST: create group with images then enable mirroring.  Disable mirroring then remove group"
test_create_group_with_images_then_mirror "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'true'

testlog "TEST: create group then enable mirroring before adding images to the group.  Remove group without disabling mirroring"
test_create_group_mirror_then_add_images "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'false'

testlog "TEST: create group then enable mirroring before adding images to the group.  Disable mirroring then remove group"
test_create_group_mirror_then_add_images "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'true'

testlog "TEST: two empty groups"
test_empty_groups "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${group1}"

# testlog "TEST: add image from a different pool to group and test replay"
# different pools - not MVP
# test_images_different_pools "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${pool1}" "${group0}" "${image_prefix}"

testlog "TEST: create regular group snapshots and test replay"
test_create_group_with_images_then_mirror_with_regular_snapshots "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}"

testlog "TEST: add a large image to group and test replay"
test_create_group_with_large_image "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}"

#TODO - test with mirrored images not in group

: ' # TODO next test needs finishing
testlog "TEST: multiple images in group with io"
test_create_group_with_multiple_images_do_io "${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}"
'

exit 0
