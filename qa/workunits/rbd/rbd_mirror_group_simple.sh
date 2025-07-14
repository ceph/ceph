#!/usr/bin/env bash
#
# rbd_mirror_group_simple.sh
#
# This script has a set of tests that should pass when run.
# It may repeat some of the tests from rbd_mirror_group.sh, but only those that are known to work
# It has a number of extra tests that imclude multiple images in a group
#
# shellcheck disable=SC2034  # Don't warn about unused variables and functions
# shellcheck disable=SC2317  # Don't warn about unreachable commands
#
# -------------------------------------------------
# INSTRUCTIONS FOR RUNNING
# 
# This script contains many tests.  They can be all be run together (in sequence) or run individually.
# To run all tests the script can be run without any arguments
#
#    ../qa/workunits/rbd/rbd_mirror_group_simple.sh 
#
# Alternatively the script takes a number of optional arguments:
#  -d <filename>  Save debug info for a test+scenario to $TEMPDIR/filename.  After a successful test completion the contents of this file
#                 are deleted prior to running the next test to free up disk space.
#  -m <multiplier> Some tests can be run with a variable number of images.  The multiplier value can be specified
#                  to increase the default number of images. (image_count = default_count * multiplier)                          
#  -r <repeat_count> repeat_count is a number that sets the number of times each test should be run. 
#                    If not specified then the tests are run once
#  -t <test_name> test_name is a string that specifies the name of a single test to run ie the name of the function containing the test
#                 If not specifed then all tests are run.
#  -s <scenario_number> scenario_number sets the number of the scenario to use for the specified test.  Each test has one or more scenarios
#                       declared before the test function.  The declaration defines the argments that the test function is called with.
#                       (only valid if -t attribute is also specified)   
#  -f <feature_set> feature_set is a number that defines the features to use when creating any images during the tests.
#                   Default value is 0
#                     0 - "layering,exclusive-lock,object-map,fast-diff,deep-flatten"
#                     1 - "layering,exclusive-lock,object-map,fast-diff" 
#                     2 - "layering,exclusive-lock" 
#                     3 - "layering" 
#
#  For example, to run the single test called test_create_group_with_images_then_mirror() with the values in the second defined scenario 
#
#    ../qa/workunits/rbd/rbd_mirror_group_simple.sh -t test_create_group_with_images_then_mirror -s 2

export RBD_MIRROR_NOCLEANUP=1
export RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror

group0=test-group0
group1=test-group1
pool0=mirror
pool1=mirror_parent
image_prefix=test-image

image_multiplier=1
repeat_count=1
feature=0

while getopts "d:f:m:pr:s:t:" opt; do
  case $opt in
    d)
      RBD_MIRROR_SAVE_CLI_OUTPUT=$OPTARG
      ;;
    f)
      feature=$OPTARG
      ;;
    m)
      image_multiplier=$OPTARG
      ;;
    p)
      RBD_MIRROR_PRINT_TESTS='true'
      ;;
    r)
      repeat_count=$OPTARG
      ;;
    s)
      scenario_number=$OPTARG
      ;;
    t)
      test_name=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

features=(
  "layering,exclusive-lock,object-map,fast-diff,deep-flatten"
  "layering,exclusive-lock,object-map,fast-diff" 
  "layering,exclusive-lock" 
  "layering" 
)

if [ -z "${RBD_IMAGE_FEATURES}" ]; then 
  echo "RBD_IMAGE_FEATURES=${features[${feature}]}"
  RBD_IMAGE_FEATURES=${features[${feature}]}
fi

if [ -n "${scenario_number}" ] && [ -z "${test_name}" ]; then
  echo "Option -s requires -t to be specifed"; exit 1;
fi

if [ "${RBD_MIRROR_PRINT_TESTS}" != 'true' ]; then
  echo "Repeat count: $repeat_count"
  echo "Scenario number: $scenario_number"
  echo "Test name: $test_name"
  echo "Features: $RBD_IMAGE_FEATURES"
fi

RBD_MIRROR_INSTANCES=${RBD_MIRROR_INSTANCES:-1}
RBD_MIRROR_MODE=snapshot

# save and clear the cli args (can't call rbd_mirror_helpers with these defined)
args=("$@")
set --

. $(dirname $0)/rbd_mirror_helpers.sh

# create group with images then enable mirroring.  Remove group without disabling mirroring
declare -a test_create_group_with_images_then_mirror_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'false' 5)
# create group with images then enable mirroring.  Disable mirroring then remove group
declare -a test_create_group_with_images_then_mirror_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'true' 5)
declare -a test_create_group_with_images_then_mirror_3=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'true' 30)

test_create_group_with_images_then_mirror_scenarios=3

test_create_group_with_images_then_mirror()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local disable_before_remove=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift
  if [ -n "$1" ]; then
    local get_times='true'
    local -n _times_result_arr=$1
  fi

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"

  # write to every image in the group
  local io_count=10240
  local io_size=4096
  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done
  
  local start_time enabled_time synced_time
  start_time=$(date +%s)
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  enabled_time=$(date +%s)

  # rbd group list poolName  (check groupname appears in output list)
  # do this before checking for replay_started because queries directed at the daemon fail with an unhelpful
  # error message before the group appears on the remote cluster
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"
  check_daemon_running "${secondary_cluster}"

  # ceph --daemon mirror group status groupName
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
  check_daemon_running "${secondary_cluster}"

  # rbd mirror group status groupName
  #sleep 10
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${image_count}"
  wait_for_group_synced "${primary_cluster}" "${pool}/${group}" "${secondary_cluster}" "${pool}"/"${group}"
  synced_time=$(date +%s)
  if [ -n "$get_times" ]; then
    _times_result_arr+=($((enabled_time-start_time)))
    _times_result_arr+=($((synced_time-enabled_time)))
  fi  

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

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
}

# create mirrored group with images then try some invalid actions
declare -a test_invalid_actions_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 5)

test_invalid_actions_scenarios=1

test_invalid_actions()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"

  # write to every image in the group
  local io_count=10240
  local io_size=4096
  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done
  
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${image_count}"
  wait_for_group_synced "${primary_cluster}" "${pool}/${group}" "${secondary_cluster}" "${pool}"/"${group}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  #TODO next command does not fail.  Fix is not MVP
  expect_failure "group is readonly" rbd --cluster="${secondary_cluster}" group rename "${pool}/${group}" "${pool}/${group}_renamed"

  group_remove "${primary_cluster}" "${pool}/${group}"
  check_daemon_running "${secondary_cluster}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
}

# Create a group and enable mirroring.
# Rename the group on the primary and confirm that the group is renamed on the primary, but not on the secondary
# Do some more rename testing with a second group - this time checking the new name appears on the secondary
# Delete the second group
# Check that the original group still has the expected names on each cluster
# (All with mirror daemon running on both clusters)
declare -a test_group_rename_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${group1}" "${image_prefix}" "${image_prefix}_1" 2)

test_group_rename_scenarios=1

test_group_rename()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local group1=$1 ; shift
  local image_prefix=$1 ; shift
  local image_prefix1=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group}" 'up+replaying' "${image_count}"

  group_rename "${primary_cluster}" "${pool}/${group}" "${pool}/${group}_renamed"

  # check that the group has the expected names on both clusters - rename shouldn't be mirrored
  test_group_present "${primary_cluster}" "${pool}" "${group}" 0
  test_group_present "${primary_cluster}" "${pool}" "${group}_renamed" 1 "${image_count}"
  # TODO deferred task #8 means that it is possible for the group to be renamed on the secondary cluster at
  # any point - disable the next 2 lines until this task is done
  # test_group_present "${secondary_cluster}" "${pool}" "${group}" 1 "${image_count}"
  # test_group_present "${secondary_cluster}" "${pool}" "${group}_renamed" 0

  # Test that rename on secondary only happens on snapshot sync
  # Create another group with images, rename the group, check that it is only renamed locally
  # Then do a mirror igroup snapshot and check that the new name is mirrored to the secondary.
  # Do another rename on the primary and without another mirror group snapshot, delete it all 
  group_create "${primary_cluster}" "${pool}/${group1}"
  images_create "${primary_cluster}" "${pool}/${image_prefix1}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix1}" "${image_count}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group1}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group1}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group1}" 'up+replaying' "${image_count}"

  group_rename "${primary_cluster}" "${pool}/${group1}" "${pool}/${group1}_renamed"

  # check that the group has the expected names on both clusters - rename shouldn't be mirrored
  test_group_present "${primary_cluster}" "${pool}" "${group1}" 0
  test_group_present "${primary_cluster}" "${pool}" "${group1}_renamed" 1 "${image_count}"
  # TODO deferred task #8 means that it is possible for the group to be renamed on the secondary cluster at
  # any point - disable the next 2 lines until this task is done
  # test_group_present "${secondary_cluster}" "${pool}" "${group1}" 1 "${image_count}"
  # test_group_present "${secondary_cluster}" "${pool}" "${group1}_renamed" 0

  # write to every image in the group
  local io_count=10240
  local io_size=4096
  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix1}${loop_instance}" "${io_count}" "${io_size}"
  done

  #mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group1}_renamed"

  # TODO temp workaround waiting for deferred task #8.  When that is delivered the following 4 lines can be deleted and the above 
  # line uncommented instead
  local group_snap_id
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group1}_renamed" group_snap_id
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}_renamed" "${image_count}"
  wait_for_group_snap_present "${secondary_cluster}" "${pool}/${group1}_renamed" "${group_snap_id}"
  wait_for_group_snap_sync_complete "${secondary_cluster}" "${pool}/${group1}_renamed" "${group_snap_id}"

  test_group_present "${primary_cluster}" "${pool}" "${group1}" 0
  test_group_present "${primary_cluster}" "${pool}" "${group1}_renamed" 1 "${image_count}"
  test_group_present "${secondary_cluster}" "${pool}" "${group1}" 0
  test_group_present "${secondary_cluster}" "${pool}" "${group1}_renamed" 1 "${image_count}"

  # set name back to original and then remove
  group_rename "${primary_cluster}" "${pool}/${group1}_renamed" "${pool}/${group1}"

  group_remove "${primary_cluster}" "${pool}/${group1}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group1}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group1}"
  images_remove "${primary_cluster}" "${pool}/${image_prefix1}" "${image_count}"

  test_group_present "${primary_cluster}" "${pool}" "${group1}" 0
  test_group_present "${primary_cluster}" "${pool}" "${group1}_renamed" 0
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group1}" 

  # TODO temp workaround waiting for deferred task #8.  When that is delivered the following line can be deleted
  test_group_present "${secondary_cluster}" "${pool}" "${group1}_renamed" 0 || group_remove "${secondary_cluster}" "${pool}/${group1}_renamed"

  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group1}_renamed" 

  # check that the original group has the expected names on both clusters
  test_group_present "${primary_cluster}" "${pool}" "${group}" 0 || fail "Group rename was undone on primary"
  test_group_present "${primary_cluster}" "${pool}" "${group}_renamed" 1 "${image_count}" || fail "Group rename is missing on primary"
  # TODO deferred task #8 means that it is possible for the group to be renamed on the secondary cluster at
  # any point - disable the next 2 lines until this task is done
  # test_group_present "${secondary_cluster}" "${pool}" "${group}_renamed" 0 || fail "Group name was incorrectly modified on secondary"
  # test_group_present "${secondary_cluster}" "${pool}" "${group}" 1 "${image_count}" || fail "Group is missing on secondary"

  group_remove "${primary_cluster}" "${pool}/${group}_renamed"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}_renamed"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"

  stop_mirrors "${primary_cluster}"
}

declare -a test_enable_mirroring_when_duplicate_group_exists_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" 'remove')
declare -a test_enable_mirroring_when_duplicate_group_exists_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" 'rename_secondary')
declare -a test_enable_mirroring_when_duplicate_group_exists_3=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" 'rename_primary')
declare -a test_enable_mirroring_when_duplicate_group_exists_4=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" 'disable_then_rename_primary')

# scenario 3 fails see TODO below
test_enable_mirroring_when_duplicate_group_exists_scenarios='1 2 4'

# This test does the following
# 1. create a group on primary site
# 2. create a group with the same name on the secondary site
# 3. enable mirroring on the primary site 
# 4. take different actions to allow mirroring to proceed
#    scenario 1 - delete the duplicate named group on the secondary
#    scenario 2 - rename the duplicate named group on the secondary
#    scenario 3 - rename the duplicate named group on the primary without disabling mirroring
#    scenario 4 - disable mirroing then rename the duplicate named group on the primary and re-enable mirroring
# 5. check that group is successfully mirrored to secondary - apart from scenario 3
test_enable_mirroring_when_duplicate_group_exists()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local scenario=$1 ; shift

  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group}"
  group_create "${secondary_cluster}" "${pool}/${group}"
  
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  # group will be present on secondary, but won't be mirrored
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+stopped' 0 'local group is primary'
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'true'

  # Look at the "state" and "description" fields for the peer site in the group status output.
  # Can't look at the state directly on the secondary because mirroring should have failed to be enabled

  if [ "${scenario}" = 'remove' ]; then
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+error' 'split-brain detected'
    # remove the non-mirrored group on the secondary
    group_remove "${secondary_cluster}" "${pool}/${group}"
  elif  [ "${scenario}" = 'rename_secondary' ]; then
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+error' 'split-brain detected'
    group_rename "${secondary_cluster}" "${pool}/${group}" "${pool}/${group}_renamed"
  elif  [ "${scenario}" = 'rename_primary' ]; then
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+error' 'split-brain detected'
    group_rename "${primary_cluster}" "${pool}/${group}" "${pool}/${group}_renamed"
    group_orig="${group}"
    group="${group}_renamed"
  elif  [ "${scenario}" = 'disable_then_rename_primary' ]; then
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+error' 'split-brain detected'
    mirror_group_disable "${primary_cluster}" "${pool}/${group}"
    group_rename "${primary_cluster}" "${pool}/${group}" "${pool}/${group}_renamed"
    group_orig="${group}"
    group="${group}_renamed"
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  fi

  if [ "${scenario}" = 'remove' ]; then
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+replaying'
    test_fields_in_group_info "${secondary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'false'
  elif  [ "${scenario}" = 'rename_secondary' ]; then
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+replaying'
    test_fields_in_group_info "${secondary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'false'
  elif  [ "${scenario}" = 'rename_primary' ]; then
    # Group should still not be mirrored in this case - need to disable, rename and renable to fix
    # TODO sometimes fails on next line with group mirrored -
    # Groups do not currently behave like images - see this thread
    # https://ibm-systems-storage.slack.com/archives/C07J9Q2E268/p1745320514846339?thread_ts=1745293182.701399&cid=C07J9Q2E268
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+error' 'split-brain detected'
  elif  [ "${scenario}" = 'disable_then_rename_primary' ]; then
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+replaying'
    test_fields_in_group_info "${secondary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'false'
  fi

  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  if [ "${scenario}" = 'rename_secondary' ]; then
    group_remove "${secondary_cluster}" "${pool}/${group}_renamed"
  elif  [ "${scenario}" = 'rename_primary' ]; then
    group_remove "${secondary_cluster}" "${pool}/${group_orig}"
  elif  [ "${scenario}" = 'disable_then_rename_primary' ]; then
    group_remove "${secondary_cluster}" "${pool}/${group_orig}"
  fi

  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
}

declare -a test_enable_mirroring_when_duplicate_image_exists_1=("${CLUSTER2}" "${CLUSTER1}" 'remove')
declare -a test_enable_mirroring_when_duplicate_image_exists_2=("${CLUSTER2}" "${CLUSTER1}" 'rename_secondary')
declare -a test_enable_mirroring_when_duplicate_image_exists_3=("${CLUSTER2}" "${CLUSTER1}" 'rename_primary')
declare -a test_enable_mirroring_when_duplicate_image_exists_4=("${CLUSTER2}" "${CLUSTER1}" 'disable_then_rename_primary')

test_enable_mirroring_when_duplicate_image_exists_scenarios=4

# This test does the following 
# 1. create a group with an image on primary site
# 2. create an image with the same name on the secondary site
# 3. enable mirroring for the group on the primary site 
# 4. take different actions to allow mirroring to proceed
#    scenario 1 - delete the duplicate image on the secondary
#    scenario 2 - rename the duplicate image on the secondary
#    scenario 3 - rename the duplicate image on the primary without disabling mirroring
#    scenario 4 - disable mirroing then rename the duplicate named image on the primary and re-enable mirroring
# 5. check that group and all images are successfully mirrored to secondary - apart from scenario 3
test_enable_mirroring_when_duplicate_image_exists()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local scenario=$1 ; shift
  local image_count=1
  local pool="${pool0}"
  local group="${group0}"

  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"

  images_create "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  # group will be present on secondary, but image won't be mirrored
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+stopped' 1 'local group is primary'
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'true'
  wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+error' 'failed to start image replayers'

  # group should be mirrored, but image can't be
  # TODO fails on next line with"rbd: mirroring not enabled on the group" rc= 22
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+error' 0 'failed to start image replayers'
  test_fields_in_group_info "${secondary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'false'

  if [ "${scenario}" = 'remove' ]; then
    # remove the non-mirrored image on the secondary
    images_remove "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  elif  [ "${scenario}" = 'rename_secondary' ]; then
    image_rename "${secondary_cluster}" "${pool}/${image_prefix}0" "${pool}/${image_prefix}_renamed0"
  elif  [ "${scenario}" = 'rename_primary' ]; then
    image_rename "${primary_cluster}" "${pool}/${image_prefix}0" "${pool}/${image_prefix}_renamed0"
    image_prefix_orig="${image_prefix}"
    image_prefix="${image_prefix}_renamed"
  elif  [ "${scenario}" = 'disable_then_rename_primary' ]; then
    mirror_group_disable "${primary_cluster}" "${pool}/${group}"
    image_rename "${primary_cluster}" "${pool}/${image_prefix}0" "${pool}/${image_prefix}_renamed0"
    image_prefix_orig="${image_prefix}"
    image_prefix="${image_prefix}_renamed"
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"
  fi

  if [ "${scenario}" = 'rename_primary' ]; then
    # Group should still not be mirrored in this case - need to disable, rename and renable to fix
    # TODO scenario 3 fails on the next line - description is blank'
    wait_for_peer_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+stopped' 'local group is primary'
  else
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${image_count}"
    wait_for_group_synced "${primary_cluster}" "${pool}/${group}" "${secondary_cluster}" "${pool}"/"${group}"
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+stopped' "${image_count}"
  fi

  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"

  if [ "${scenario}" = 'rename_secondary' ]; then
    images_remove "${secondary_cluster}" "${pool}/${image_prefix}_renamed" "${image_count}"
  elif  [ "${scenario}" = 'rename_primary' ]; then
    images_remove "${secondary_cluster}" "${pool}/${image_prefix_orig}" "${image_count}"
  fi

  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
}

# record the time taken to enable and sync for a group with increasing number of images.
# Line item 13 in coding leftovers tab - should be near constant time after that is fixed
declare -a test_group_enable_times_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}")

test_group_enable_times_scenarios=1

test_group_enable_times()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4
  local image_prefix=$5
  local disable_before_remove='true'
  local image_count
  local results=()
  local times=()

  for image_count in {0,10,20,30}; do
    times=()
    test_create_group_with_images_then_mirror "${primary_cluster}" "${secondary_cluster}" "${pool}" "${group}" "${image_prefix}" 'true' "${image_count}" times
    results+=("image count:$image_count enable time:${times[0]} sync_time:${times[1]}")
  done

  for result in "${results[@]}"; do
    echo -e "${RED}${result}${NO_COLOUR}"
  done

#results:
#image count:0 enable time:0 sync_time:6
#image count:10 enable time:9 sync_time:9
#image count:20 enable time:20 sync_time:13
#image count:30 enable time:30 sync_time:22

}

# create group with images then enable mirroring.  Remove group without disabling mirroring
declare -a test_create_group_with_image_remove_then_repeat_1=("${CLUSTER2}" "${CLUSTER1}" )

test_create_group_with_image_remove_then_repeat_scenarios=1

test_create_group_with_image_remove_then_repeat()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool="${pool0}"
  local group="${group0}"

  images_create "${primary_cluster}" "${pool}/${image_prefix}" 1
  group_create "${primary_cluster}" "${pool}/${group}"

  local loop_instance
  for loop_instance in $(seq 0 5); do
    testlog "test_create_group_with_image_remove_then_repeat ${loop_instance}"

    group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}0" 
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"

    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 1
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 1
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 1

    if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
      wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
    fi

    mirror_group_disable "${primary_cluster}" "${pool}/${group}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

    group_image_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}0" 

    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0

    mirror_group_disable "${primary_cluster}" "${pool}/${group}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

    check_daemon_running "${secondary_cluster}"
  done
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" 1
}

# create group with images and sync.  Restart daemon then recreate group on primary
declare -a test_create_group_stop_daemon_then_recreate_1=("${CLUSTER2}" "${CLUSTER1}" 'no_stop')
declare -a test_create_group_stop_daemon_then_recreate_2=("${CLUSTER2}" "${CLUSTER1}" 'stop_restart_before_recreate')
declare -a test_create_group_stop_daemon_then_recreate_3=("${CLUSTER2}" "${CLUSTER1}" 'stop_restart_after_recreate')

test_create_group_stop_daemon_then_recreate_scenarios=3

test_create_group_stop_daemon_then_recreate()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local scenario=$1 ; shift

  local pool="${pool0}"
  local group="${group0}"

  images_create "${primary_cluster}" "${pool}/${image_prefix}" 1
  group_create "${primary_cluster}" "${pool}/${group}"

  group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}0" 
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 1
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 1
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 1

  local key_count
  count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
  test "${key_count}" = 1 || fail "unexpected key count:${key_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  if [ "${scenario}" = 'no_stop' ]; then
    # remove and recreate group without stopping daemon
    group_remove "${primary_cluster}" "${pool}/${group}"
    group_create "${primary_cluster}" "${pool}/${group}"
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0

    count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
    test "${key_count}" = 1 || fail "unexpected key count:${key_count}"
  elif  [ "${scenario}" = 'stop_restart_before_recreate' ]; then
    # restart daemon
    echo "stopping daemon on secondary"
    stop_mirrors "${secondary_cluster}"
    sleep 5
    echo "restarting daemon on secondary"
    start_mirrors "${secondary_cluster}"

    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 1
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 1

    group_remove "${primary_cluster}" "${pool}/${group}"
    group_create "${primary_cluster}" "${pool}/${group}"
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0

    count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
    test "${key_count}" = 1 || fail "unexpected key count:${key_count}"
  elif  [ "${scenario}" = 'stop_restart_after_recreate' ]; then
    echo "stopping daemon on secondary"
    stop_mirrors "${secondary_cluster}"

    group_remove "${primary_cluster}" "${pool}/${group}"
    group_create "${primary_cluster}" "${pool}/${group}"
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    
    echo "restarting daemon on secondary"
    start_mirrors "${secondary_cluster}"

    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0

    count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
    test "${key_count}" = 1 || fail "unexpected key count:${key_count}"
  fi

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  # Wait for rbd_mirror_leader to be empty
  for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16; do
    sleep "${s}"
    count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
    test "${key_count}" = 0 && break
  done
  test "${key_count}" = 0 || fail "unexpected key count:${key_count}"

  check_daemon_running "${secondary_cluster}"

  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" 1
}

# create group with images then enable mirroring.  Disable and re-enable repeatedly
declare -a test_enable_disable_repeat_1=("${CLUSTER2}" "${CLUSTER1}" )

test_enable_disable_repeat_scenarios=1

test_enable_disable_repeat()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool="${pool0}"
  local group="group"

  local group_instance
  for group_instance in $(seq 0 9); do
    group_create "${primary_cluster}" "${pool}/${group}${group_instance}"
  done

  stop_mirrors "${primary_cluster}"
  stop_mirrors "${secondary_cluster}"
  start_mirrors "${secondary_cluster}"

  local loop_instance
  for loop_instance in $(seq 0 9); do
    testlog "test_enable_disable_repeat ${loop_instance}"

    for group_instance in $(seq 0 9); do
      mirror_group_enable "${primary_cluster}" "${pool}/${group}${group_instance}"
    done

    for group_instance in $(seq 0 9); do
      mirror_group_disable "${primary_cluster}" "${pool}/${group}${group_instance}"
    done
  done

  check_daemon_running "${secondary_cluster}"

  for group_instance in $(seq 0 9); do
    group_remove "${primary_cluster}" "${pool}/${group}${group_instance}"
    wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}${group_instance}"
  done
}

# add and remove images to/from a mirrored group
declare -a test_mirrored_group_add_and_remove_images_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 5)

test_mirrored_group_add_and_remove_images_scenarios=1

test_mirrored_group_add_and_remove_images()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local group_image_count=$(($1*"${image_multiplier}")) ; shift


  group_create "${primary_cluster}" "${pool}/${group}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0

  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi
  # create another image and populate it with some data
  local image_name="test_image"
  image_create "${primary_cluster}" "${pool}/${image_name}" 
  local io_count=10240
  local io_size=4096
  write_image "${primary_cluster}" "${pool}" "${image_name}" "${io_count}" "${io_size}"

  # add, wait for stable and then remove the image from the group
  group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' $((1+"${group_image_count}"))
  group_image_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' $((1+"${group_image_count}"))
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
  fi

  # re-add and immediately remove
  group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 
  group_image_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 

  # check that expected number of images exist on secondary  
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  # remove and immediately re-add a different image
  group_image_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}2"
  group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}2" 

  # check that expected number of images exist on secondary  
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  # remove all images from the group
  group_images_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  image_remove "${primary_cluster}" "${pool}/${image_name}"
  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi

  # check that expected number of images exist on secondary - TODO this should be replaying, but deleting the last image seems to cause 
  # the group to go stopped atm  
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+stopped' 0

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  check_daemon_running "${secondary_cluster}"
}

# create group with images then enable mirroring.  Remove all images from group and check state matches initial empty group state
declare -a test_mirrored_group_remove_all_images_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 2)

test_mirrored_group_remove_all_images_scenarios=1

test_mirrored_group_remove_all_images()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local group_image_count=$(($1*"${image_multiplier}")) ; shift

  group_create "${primary_cluster}" "${pool}/${group}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0

  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  # remove all images from the group
  group_images_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi

  # check that expected number of images exist on secondary
  # TODO why is the state "stopped" - a new empty group is in the "replaying" state
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+stopped' 0

  # adding the images back into the group causes it to go back to replaying
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+stopped' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi
  
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  # remove all images from the group again
  group_images_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi

  # check that expected number of images exist on secondary  
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+stopped' 0

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  check_daemon_running "${secondary_cluster}"
}

# create group then enable mirroring before adding images to the group.  Disable mirroring before removing group
declare -a test_create_group_mirror_then_add_images_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'false' 5)
# create group then enable mirroring before adding images to the group.  Remove group with mirroring enabled
declare -a test_create_group_mirror_then_add_images_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'true' 5)

test_create_group_mirror_then_add_images_scenarios=2

test_create_group_mirror_then_add_images()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local disable_before_remove=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  group_create "${primary_cluster}" "${pool}/${group}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  fi

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"
  check_daemon_running "${secondary_cluster}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
  check_daemon_running "${secondary_cluster}"

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${image_count}"

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

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
}

#test remote namespace with different name
declare -a test_remote_namespace_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 'true' 5 'both')
declare -a test_remote_namespace_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 'true' 5 'neither')
declare -a test_remote_namespace_3=("${CLUSTER2}" "${CLUSTER1}" 'new_pool' 'true' 5 'primary')
declare -a test_remote_namespace_4=("${CLUSTER2}" "${CLUSTER1}" 'new_pool' 'true' 5 'secondary')
declare -a test_remote_namespace_5=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 'false' 5 'both')
declare -a test_remote_namespace_6=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 'false' 5 'neither')
declare -a test_remote_namespace_7=("${CLUSTER2}" "${CLUSTER1}" 'new_pool' 'false' 5 'primary')
declare -a test_remote_namespace_8=("${CLUSTER2}" "${CLUSTER1}" 'new_pool' 'false' 5 'secondary')

test_remote_namespace_scenarios=8

test_remote_namespace()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local test_group=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift
  local scenario=$1 ; shift

  local group="${group0}"

  # configure primary namespace mirrored to secondary namespace with a different name 
  local primary_namespace
  primary_namespace='ns3'
  local secondary_namespace
  secondary_namespace='ns3_remote'

  local primary_pool_spec
  local secondary_pool_spec

  start_mirrors "${primary_cluster}"

  # mirror pool default namespace is already enabled for mirroring - need a new pool when testing with default namespace
  if [ "${scenario}" = 'primary' ] || [ "${scenario}" = 'secondary' ]; then
    run_admin_cmd "ceph --cluster ${secondary_cluster} osd pool create ${pool} 64 64"
    run_admin_cmd "rbd --cluster ${secondary_cluster} pool init ${pool}"
    run_admin_cmd "ceph --cluster ${primary_cluster} osd pool create ${pool} 64 64"
    run_admin_cmd "rbd --cluster ${primary_cluster} pool init ${pool}"
  fi

  if [ "${scenario}" = 'primary' ]; then
    primary_pool_spec="${pool}/${primary_namespace}"
    secondary_pool_spec="${pool}"
  elif [ "${scenario}" = 'secondary' ]; then
    primary_pool_spec="${pool}"
    secondary_pool_spec="${pool}/${secondary_namespace}"
  elif [ "${scenario}" = 'both' ]; then
    primary_pool_spec="${pool}/${primary_namespace}"
    secondary_pool_spec="${pool}/${secondary_namespace}"
  elif [ "${scenario}" = 'neither' ]; then
    primary_pool_spec="${pool}"
    secondary_pool_spec="${pool}"
  fi

  if [ "${scenario}" = 'both' ] || [ "${scenario}" = 'primary' ]; then
    namespace_create "${primary_cluster}" "${primary_pool_spec}"
  fi
  if [ "${scenario}" = 'both' ] || [ "${scenario}" = 'secondary' ]; then
    namespace_create "${secondary_cluster}" "${secondary_pool_spec}"
  fi

  if [ "${scenario}" = 'both' ]; then
    run_cmd "rbd --cluster ${primary_cluster} mirror pool enable ${primary_pool_spec} image --remote-namespace ${secondary_namespace}"
    run_cmd "rbd --cluster ${secondary_cluster} mirror pool enable ${secondary_pool_spec} image --remote-namespace ${primary_namespace}"
  elif [ "${scenario}" = 'neither' ]; then
    run_cmd "rbd --cluster ${primary_cluster} mirror pool enable ${primary_pool_spec} image"
    run_cmd "rbd --cluster ${secondary_cluster} mirror pool enable ${secondary_pool_spec} image"
  elif [ "${scenario}" = 'primary' ]; then
    # primary_namespace on primary, default namespace on secondary 
    run_cmd "rbd --cluster ${secondary_cluster} mirror pool enable ${secondary_pool_spec} image --remote-namespace ${primary_namespace}"
    run_cmd "rbd --cluster ${primary_cluster} mirror pool enable ${pool} init-only"
    run_cmd "rbd --cluster ${primary_cluster} mirror pool enable ${primary_pool_spec} image --remote-namespace ''"
    peer_add "${secondary_cluster}" "${pool}" "${primary_cluster}"
    peer_add "${primary_cluster}" "${pool}" "${secondary_cluster}"
  elif [ "${scenario}" = 'secondary' ]; then
    run_cmd "rbd --cluster ${primary_cluster} mirror pool enable ${primary_pool_spec} image --remote-namespace ${secondary_namespace}"
    run_cmd "rbd --cluster ${secondary_cluster} mirror pool enable ${pool} init-only"
    run_cmd "rbd --cluster ${secondary_cluster} mirror pool enable ${secondary_pool_spec} image --remote-namespace ''"
    peer_add "${secondary_cluster}" "${pool}" "${primary_cluster}"
    peer_add "${primary_cluster}" "${pool}" "${secondary_cluster}"
  fi

  if [ "${test_group}" = 'true' ]; then
    group_create "${primary_cluster}" "${primary_pool_spec}/${group}"
    mirror_group_enable "${primary_cluster}" "${primary_pool_spec}/${group}"
    wait_for_group_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}" 0 
    wait_for_group_replay_started "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 0
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 'up+replaying' 0
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}"/"${group}" 'up+stopped' 0

    mirror_group_disable "${primary_cluster}" "${primary_pool_spec}/${group}"

    group_remove "${primary_cluster}" "${primary_pool_spec}/${group}"
    wait_for_group_not_present "${primary_cluster}" "${primary_pool_spec}" "${group}"
    wait_for_group_not_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}"
    check_daemon_running "${secondary_cluster}"
  fi  

  # repeat the test - this time with some images
  if [ "${test_group}" = 'true' ]; then
    group_create "${primary_cluster}" "${primary_pool_spec}/${group}"
    images_create "${primary_cluster}" "${primary_pool_spec}/${image_prefix}" "${image_count}"
    group_images_add "${primary_cluster}" "${primary_pool_spec}/${group}" "${primary_pool_spec}/${image_prefix}" "${image_count}"

    mirror_group_enable "${primary_cluster}" "${primary_pool_spec}/${group}"
    wait_for_group_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}" "${image_count}"
    wait_for_group_replay_started "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" "${image_count}"
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 'up+replaying' "${image_count}"
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}"/"${group}" 'up+stopped' "${image_count}"
  else
    standalone_image='standalone_image0'
    images_create "${primary_cluster}" "${primary_pool_spec}/standalone_image" 1
    enable_mirror "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}"
    wait_for_image_replay_started  "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}"
    wait_for_replay_complete "${secondary_cluster}" "${primary_cluster}" "${secondary_pool_spec}" "${primary_pool_spec}" "${standalone_image}"
    wait_for_replaying_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}"
  fi

  # try a manual snapshot and check that it syncs
  if [ "${test_group}" = 'true' ]; then
    write_image "${primary_cluster}" "${primary_pool_spec}" "${image_prefix}0" 10 4096
    local group_snap_id
    mirror_group_snapshot "${primary_cluster}" "${primary_pool_spec}/${group}" group_snap_id
    wait_for_group_snap_present "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}"
    wait_for_group_snap_sync_complete "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}"

    # Check all images in the group and confirms that they are synced
    test_group_synced_image_status "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}" "${image_count}"
  else
    # try a standalone image
    mirror_image_snapshot "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}"
    wait_for_snapshot_sync_complete "${secondary_cluster}" "${primary_cluster}" "${secondary_pool_spec}" "${primary_pool_spec}" "${standalone_image}"
  fi  

  # try demote
  if [ "${test_group}" = 'true' ]; then
    mirror_group_demote "${primary_cluster}" "${primary_pool_spec}/${group}"
    test_fields_in_group_info "${primary_cluster}" "${primary_pool_spec}/${group}" 'snapshot' 'enabled' 'false'
    wait_for_group_replay_stopped "${secondary_cluster}" "${secondary_pool_spec}/${group}"
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}/${group}" 'up+unknown' 0
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}/${group}" 'up+unknown' 0
  else
    demote_image "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}"
    wait_for_image_replay_stopped "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}"
    wait_for_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}" 'up+unknown'
    wait_for_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}" 'up+unknown'
  fi

  # try promote (of original secondary), modify image and check new snapshot syncs 
  if [ "${test_group}" = 'true' ]; then
    mirror_group_promote "${secondary_cluster}" "${secondary_pool_spec}/${group}"
    test_fields_in_group_info "${secondary_cluster}" "${secondary_pool_spec}/${group}" 'snapshot' 'enabled' 'true'
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}"/"${group}" 'up+replaying' "${image_count}"

    write_image "${secondary_cluster}" "${secondary_pool_spec}" "${image_prefix}0" 10 4096
    mirror_group_snapshot "${secondary_cluster}" "${secondary_pool_spec}/${group}" group_snap_id
    wait_for_group_snap_present "${primary_cluster}" "${primary_pool_spec}/${group}" "${group_snap_id}"
    wait_for_group_snap_sync_complete "${primary_cluster}" "${primary_pool_spec}/${group}" "${group_snap_id}"
  else
    promote_image "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}" 
    wait_for_image_replay_started "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}"

    write_image "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}" 10 4096
    mirror_image_snapshot "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}"
    wait_for_snapshot_sync_complete "${primary_cluster}" "${secondary_cluster}" "${primary_pool_spec}" "${secondary_pool_spec}" "${standalone_image}"
  fi  

  # try demote, promote (of original primary) and resync
  if [ "${test_group}" = 'true' ]; then
    mirror_group_demote "${secondary_cluster}" "${secondary_pool_spec}/${group}"
    test_fields_in_group_info "${secondary_cluster}" "${secondary_pool_spec}/${group}" 'snapshot' 'enabled' 'false'
    wait_for_group_replay_stopped "${secondary_cluster}" "${secondary_pool_spec}/${group}"
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}/${group}" 'up+unknown' 0
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}/${group}" 'up+unknown' 0
    mirror_group_promote "${primary_cluster}" "${primary_pool_spec}/${group}"
    test_fields_in_group_info "${primary_cluster}" "${primary_pool_spec}/${group}" 'snapshot' 'enabled' 'true'
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 'up+replaying' "${image_count}"

    local group_id_before
    get_id_from_group_info "${secondary_cluster}" "${secondary_pool_spec}/${group}" group_id_before
    mirror_group_resync "${secondary_cluster}" "${secondary_pool_spec}/${group}"
    wait_for_group_id_changed "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_id_before}"

    wait_for_group_synced "${primary_cluster}" "${primary_pool_spec}/${group}" "${secondary_cluster}" "${secondary_pool_spec}/${group}"

    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 'up+replaying' "${image_count}"
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}"/"${group}" 'up+stopped' "${image_count}"
  else
    demote_image "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}"  
    wait_for_image_replay_stopped "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}"
    wait_for_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}" 'up+unknown'
    wait_for_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}" 'up+unknown'
    promote_image "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}" 
    wait_for_image_replay_started "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}"

    image_id=$(get_image_id "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}")
    request_resync_image "${secondary_cluster}" "${secondary_pool_spec}"  "${standalone_image}" image_id

    wait_for_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}" "${standalone_image}" 'up+replaying'
    wait_for_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}" 'up+stopped'
    wait_for_snapshot_sync_complete "${secondary_cluster}" "${primary_cluster}" "${secondary_pool_spec}" "${primary_pool_spec}" "${standalone_image}"
  fi    

  # try another manual snapshot and check that it still syncs OK
  if [ "${test_group}" = 'true' ]; then
    write_image "${primary_cluster}" "${primary_pool_spec}" "${image_prefix}0" 10 4096
    local group_snap_id
    mirror_group_snapshot "${primary_cluster}" "${primary_pool_spec}/${group}" group_snap_id
    wait_for_group_snap_present "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}"
    wait_for_group_snap_sync_complete "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}"

    # Check all images in the group and confirms that they are synced
    test_group_synced_image_status "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}" "${image_count}"
  else
    # try a standalone image
    mirror_image_snapshot "${primary_cluster}" "${primary_pool_spec}" "${standalone_image}"
    wait_for_snapshot_sync_complete "${secondary_cluster}" "${primary_cluster}" "${secondary_pool_spec}" "${primary_pool_spec}" "${standalone_image}"
  fi  

  if [ "${test_group}" = 'true' ]; then
    group_remove "${primary_cluster}" "${primary_pool_spec}/${group}"
    wait_for_group_not_present "${primary_cluster}" "${primary_pool_spec}" "${group}"
    wait_for_group_not_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}"

    images_remove "${primary_cluster}" "${primary_pool_spec}/${image_prefix}" "${image_count}"
  else
    images_remove "${primary_cluster}" "${primary_pool_spec}/standalone_image" 1
  fi  

  if [ "${scenario}" = 'both' ] || [ "${scenario}" = 'primary' ]; then
    namespace_remove "${primary_cluster}" "${pool}/${primary_namespace}"
  fi
  if [ "${scenario}" = 'both' ] || [ "${scenario}" = 'secondary' ]; then
    # can't delete the namespace on the secondary until all images have gone so need to retry until cmd succeeds
    namespace_remove_retry "${secondary_cluster}" "${pool}/${secondary_namespace}"
  fi

  if [ "${scenario}" = 'primary' ] || [ "${scenario}" = 'secondary' ]; then
    run_admin_cmd "ceph --cluster ${primary_cluster} osd pool delete ${pool} ${pool} --yes-i-really-really-mean-it"
    run_admin_cmd "ceph --cluster ${secondary_cluster} osd pool delete ${pool} ${pool} --yes-i-really-really-mean-it"
  fi

  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  check_daemon_running "${secondary_cluster}"
}

#test empty group
declare -a test_empty_group_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}")
#test empty group with namespace
declare -a test_empty_group_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}/${NS1}" "${group0}")

test_empty_group_scenarios=2

test_empty_group()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4

  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+stopped' 0

  # try a snapshot and check that it syncs
  local group_snap_id
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group}" group_snap_id
  wait_for_group_snap_present "${secondary_cluster}" "${pool}/${group}" "${group_snap_id}"
  wait_for_group_snap_sync_complete "${secondary_cluster}" "${pool}/${group}" "${group_snap_id}"

  # try demote
  mirror_group_demote "${primary_cluster}" "${pool}/${group}"
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'false'
  wait_for_group_replay_stopped "${secondary_cluster}" "${pool}/${group}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group}" 'up+unknown' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group}" 'up+unknown' 0

  # try promote, modify image and check new snapshot syncs 
  mirror_group_promote "${secondary_cluster}" "${pool}/${group}"
  test_fields_in_group_info "${secondary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'true'
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+replaying'
  mirror_group_snapshot "${secondary_cluster}" "${pool}/${group}" group_snap_id
  wait_for_group_snap_present "${primary_cluster}" "${pool}/${group}" "${group_snap_id}"
  wait_for_group_snap_sync_complete "${primary_cluster}" "${pool}/${group}" "${group_snap_id}"

  # try demote, promote and resync
  mirror_group_demote "${secondary_cluster}" "${pool}/${group}"
  test_fields_in_group_info "${secondary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'false'
  wait_for_group_replay_stopped "${secondary_cluster}" "${pool}/${group}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group}" 'up+unknown' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group}" 'up+unknown' 0

  mirror_group_promote "${primary_cluster}" "${pool}/${group}"
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group}" 'snapshot' 'enabled' 'true'
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying'

  local group_id_before
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group}" group_id_before
  mirror_group_resync "${secondary_cluster}" "${pool}/${group}"
  wait_for_group_id_changed "${secondary_cluster}" "${pool}/${group}" "${group_id_before}"

  wait_for_group_synced "${primary_cluster}" "${pool}/${group}" "${secondary_cluster}" "${pool}/${group}"

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying'
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'up+stopped'

  # tidy up
  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  check_daemon_running "${secondary_cluster}"
}

#check that omap keys have been correctly deleted
declare -a test_empty_group_omap_keys_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}")

test_empty_group_omap_keys_scenarios=1

test_empty_group_omap_keys()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3

  wait_for_omap_keys "${primary_cluster}" "${pool}" "rbd_mirroring" "gremote_status"
  wait_for_omap_keys "${primary_cluster}" "${pool}" "rbd_mirroring" "gstatus_global"
  wait_for_omap_keys "${secondary_cluster}" "${pool}" "rbd_mirroring" "gremote_status"
  wait_for_omap_keys "${secondary_cluster}" "${pool}" "rbd_mirroring" "gstatus_global"

  run_test test_empty_group 1

  wait_for_omap_keys "${primary_cluster}" "${pool}" "rbd_mirroring" "gremote_status"
  wait_for_omap_keys "${primary_cluster}" "${pool}" "rbd_mirroring" "gstatus_global"
  wait_for_omap_keys "${secondary_cluster}" "${pool}" "rbd_mirroring" "gremote_status"
  wait_for_omap_keys "${secondary_cluster}" "${pool}" "rbd_mirroring" "gstatus_global"
}

#check that pool doesn't get in strange state
declare -a test_group_with_clone_image_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}")

test_group_with_clone_image_scenarios=1

test_group_with_clone_image()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4

  group_create "${primary_cluster}" "${pool}/${group}"

  # create an image and then clone it
  image_create "${primary_cluster}" "${pool}/parent_image"
  create_snapshot "${primary_cluster}" "${pool}" "parent_image" "snap1"
  protect_snapshot "${primary_cluster}" "${pool}" "parent_image" "snap1"
  clone_image "${primary_cluster}" "${pool}" "parent_image" "snap1" "${pool}" "child_image"

  # create some other images
  image_create "${primary_cluster}" "${pool}/other_image0"
  image_create "${primary_cluster}" "${pool}/other_image1"

  # add the other images and the clone to the group
  group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/other_image0"
  group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/other_image1"
  group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/child_image"

  # command fails with the following message now
  #  2025-01-30T16:34:25.359+0000 7fc1a79bfb40 -1 librbd::api::Mirror: image_enable: mirroring is not enabled for the parent
  #  2025-01-30T16:34:25.359+0000 7fc1a79bfb40 -1 librbd::api::Mirror: group_enable: failed enabling image: child_image: (22) Invalid argument
  expect_failure "failed enabling image" rbd --cluster=${primary_cluster} mirror group enable ${pool}/${group}

  # tidy up
  group_remove "${primary_cluster}" "${pool}/${group}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  image_remove "${primary_cluster}" "${pool}/other_image0"
  image_remove "${primary_cluster}" "${pool}/other_image1"
  image_remove "${primary_cluster}" "${pool}/child_image"
}

# test two empty groups
declare -a test_empty_groups_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${group1}")

test_empty_groups_scenarios=1

test_empty_groups()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group0=$4
  local group1=$5

  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group0}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' 0

  group_create "${primary_cluster}" "${pool}/${group1}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group1}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group1}" 0

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group1}" 'up+stopped' 0

  # try a snapshot and check that it syncs
  local group_snap_id
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group1}" group_snap_id
  wait_for_group_snap_present "${secondary_cluster}" "${pool}/${group1}" "${group_snap_id}"
  wait_for_group_snap_sync_complete "${secondary_cluster}" "${pool}/${group1}" "${group_snap_id}"

  # try demote
  mirror_group_demote "${primary_cluster}" "${pool}/${group1}"
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group1}" 'snapshot' 'enabled' 'false'
  wait_for_group_replay_stopped "${secondary_cluster}" "${pool}/${group1}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group1}" 'up+unknown' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group1}" 'up+unknown' 0

  # try promote, modify image and check new snapshot syncs 
  mirror_group_promote "${secondary_cluster}" "${pool}/${group1}"
  test_fields_in_group_info "${secondary_cluster}" "${pool}/${group1}" 'snapshot' 'enabled' 'true'
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group1}" 'up+replaying'
  mirror_group_snapshot "${secondary_cluster}" "${pool}/${group1}" group_snap_id
  wait_for_group_snap_present "${primary_cluster}" "${pool}/${group1}" "${group_snap_id}"
  wait_for_group_snap_sync_complete "${primary_cluster}" "${pool}/${group1}" "${group_snap_id}"

  # try demote, promote and resync
  mirror_group_demote "${secondary_cluster}" "${pool}/${group1}"
  test_fields_in_group_info "${secondary_cluster}" "${pool}/${group1}" 'snapshot' 'enabled' 'false'
  wait_for_group_replay_stopped "${secondary_cluster}" "${pool}/${group1}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group1}" 'up+unknown' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group1}" 'up+unknown' 0

  mirror_group_promote "${primary_cluster}" "${pool}/${group1}"
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group1}" 'snapshot' 'enabled' 'true'
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying'

  local group_id_before
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group1}" group_id_before
  mirror_group_resync "${secondary_cluster}" "${pool}/${group1}"
  wait_for_group_id_changed "${secondary_cluster}" "${pool}/${group1}" "${group_id_before}"

  wait_for_group_synced "${primary_cluster}" "${pool}/${group1}" "${secondary_cluster}" "${pool}/${group1}"

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying'
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group1}" 'up+stopped'

  # tidy up
  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  mirror_group_disable "${primary_cluster}" "${pool}/${group1}"

  group_remove "${primary_cluster}" "${pool}/${group1}"
  group_remove "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group1}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group1}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"

  check_daemon_running "${secondary_cluster}"
}

# add image from a different pool to group and test replay
declare -a test_images_different_pools_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${pool1}" "${group0}" "${image_prefix}")

test_images_different_pools_scenarios=1

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
  image_create "${primary_cluster}" "${pool0}/${image_prefix}0"
  group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool0}/${image_prefix}0"
  image_create "${primary_cluster}" "${pool1}/${image_prefix}1"
  group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool1}/${image_prefix}1"

  # command fails with the following message now
  #  2025-06-12T17:33:25.241+0530 7fe40ccfbd00 -1 librbd::api::Mirror: prepare_group_images: cannot enable mirroring: image is in a different pool
  expect_failure "cannot enable mirroring: image is in a different pool" rbd --cluster=${primary_cluster} mirror group enable ${pool0}/${group}

  # tidy up
  group_remove "${primary_cluster}" "${pool0}/${group}"

  wait_for_group_not_present "${primary_cluster}" "${pool0}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool0}" "${group}"

  image_remove "${primary_cluster}" "${pool0}/${image_prefix}0"
  image_remove "${primary_cluster}" "${pool1}/${image_prefix}1"
}

# create regular group snapshots and test replay
declare -a test_create_group_with_images_then_mirror_with_regular_snapshots_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 2 'remove_snap')
declare -a test_create_group_with_images_then_mirror_with_regular_snapshots_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 2 'leave_snap')
declare -a test_create_group_with_images_then_mirror_with_regular_snapshots_3=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 2 'force_disable')

# TODO scenario 1 is sometimes failing so is disabled - see issue on line 45
test_create_group_with_images_then_mirror_with_regular_snapshots_scenarios=3

test_create_group_with_images_then_mirror_with_regular_snapshots()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift
  local scenario=$1 ; shift

  local snap='regular_snap'

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  check_group_snap_doesnt_exist "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"

  group_snap_create "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_exists "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_exists "${secondary_cluster}" "${pool}/${group}" "${snap}"

  if [ "${scenario}" = 'remove_snap' ]; then
    group_snap_remove "${primary_cluster}" "${pool}/${group}" "${snap}"
    check_group_snap_doesnt_exist "${primary_cluster}" "${pool}/${group}" "${snap}"
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
    check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"
  else
    check_group_snap_exists "${primary_cluster}" "${pool}/${group}" "${snap}"
    check_group_snap_exists "${secondary_cluster}" "${pool}/${group}" "${snap}"
  fi

  if [ "${scenario}" = 'force_disable' ]; then
    # need to stop the mirror daemon to prevent it from just resyncing the group
    stop_mirrors "${secondary_cluster}"
    # Force disable mirroring on the secondary and check that everything can be cleaned up
    mirror_group_disable "${secondary_cluster}" "${pool}/${group}" '--force'
    group_remove "${secondary_cluster}" "${pool}/${group}"
    wait_for_group_present "${primary_cluster}" "${pool}" "${group}" "${image_count}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
    images_remove "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  fi  

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"

  if [ "${scenario}" = 'force_disable' ]; then
    start_mirrors "${secondary_cluster}"
  fi
}

# create regular group snapshots before enable mirroring
declare -a test_create_group_with_regular_snapshots_then_mirror_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 12)

test_create_group_with_regular_snapshots_then_mirror_scenarios=1

test_create_group_with_regular_snapshots_then_mirror()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local group_image_count=$(($1*"${image_multiplier}")) ; shift
  local snap='regular_snap'

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  group_snap_create "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_exists "${primary_cluster}" "${pool}/${group}" "${snap}"
  local group_snap_id
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}/${group}" group_snap_id

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${group_image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${group_image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  check_group_snap_exists "${secondary_cluster}" "${pool}/${group}" "${snap}"
  wait_for_group_snap_sync_complete "${secondary_cluster}" "${pool}/${group}" "${group_snap_id}"

  wait_for_group_synced "${primary_cluster}" "${pool}/${group}" "${secondary_cluster}" "${pool}/${group}"
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"

  group_snap_remove "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_doesnt_exist "${primary_cluster}" "${pool}/${group}" "${snap}"
  # this next extra mirror_group_snapshot should not be needed - waiting for fix  - coding leftover 38

  mirror_group_snapshot "${primary_cluster}" "${pool}/${group}"
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
}

# create regular image snapshots and enable group mirroring
declare -a test_image_snapshots_with_group_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 2)

test_image_snapshots_with_group_scenarios=1

test_image_snapshots_with_group()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local group_image_count=$(($1*"${image_multiplier}")) ; shift
  local snap0='image_snap0'
  local snap1='image_snap1'

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap0}"
  check_snap_exists "${primary_cluster}" "${pool}/${image_prefix}0" "${snap0}"

  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap1}"
  check_snap_exists "${primary_cluster}" "${pool}/${image_prefix}0" "${snap1}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${group_image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${group_image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  check_snap_exists "${secondary_cluster}" "${pool}/${image_prefix}0" "${snap0}"
  check_snap_exists "${secondary_cluster}" "${pool}/${image_prefix}0" "${snap1}"

  wait_for_group_synced "${primary_cluster}" "${pool}/${group}" "${secondary_cluster}" "${pool}"/"${group}" 

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"

  remove_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap0}"
  check_snap_doesnt_exist "${primary_cluster}" "${pool}/${image_prefix}0" "${snap0}"

  # check that the snap is deleted on the secondary too after a mirror group snapshot
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_snap_doesnt_exist "${secondary_cluster}" "${pool}/${image_prefix}0" "${snap0}"

  # check that the other image snapshot is still present
  check_snap_exists "${secondary_cluster}" "${pool}/${image_prefix}0" "${snap1}"

  # disable and tidy up - check that other image snapshot does not cause problems
  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
}

# add a large image to group and test replay
declare -a test_create_group_with_large_image_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}")

test_create_group_with_large_image_scenarios=1

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

  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool0}/${big_image}"

    if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
      mirror_group_snapshot_and_wait_for_sync_complete "${primary_cluster}" "${secondary_cluster}" "${pool0}/${group}"
      wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool0}/${group}" 'up+replaying' 2
    fi
  else
    mirror_group_disable "${primary_cluster}" "${pool0}/${group}"
    wait_for_group_not_present "${secondary_cluster}" "${pool0}" "${group}"
    group_image_add "${primary_cluster}" "${pool0}/${group}" "${pool0}/${big_image}"
    mirror_group_enable "${primary_cluster}" "${pool0}/${group}"
    wait_for_group_present "${secondary_cluster}" "${pool0}" "${group}" 2
  fi

  write_image "${primary_cluster}" "${pool0}" "${big_image}" 1024 4194304
  local group_snap_id
  mirror_group_snapshot "${primary_cluster}" "${pool0}/${group}" group_snap_id
  wait_for_group_snap_present "${secondary_cluster}" "${pool0}/${group}" "${group_snap_id}"

  # TODO if the sync process could be controlled then we could check that test-image is synced before test-image-big
  # and that the group is only marked as synced once both images have completed their sync
  wait_for_group_snap_sync_complete "${secondary_cluster}" "${pool0}/${group}" "${group_snap_id}"

  # Check all images in the group and confirms that they are synced
  test_group_synced_image_status "${secondary_cluster}" "${pool0}/${group}" "${group_snap_id}" 2

  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_remove "${primary_cluster}" "${pool0}/${group}" "${pool0}/${big_image}"

    if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
      wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool0}"/"${group}" 'up+replaying' 1
      mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool0}"/"${group}"
    fi
  else
    mirror_group_disable "${primary_cluster}" "${pool0}/${group}"
    wait_for_group_not_present "${secondary_cluster}" "${pool0}" "${group}"
    group_image_remove "${primary_cluster}" "${pool0}/${group}" "${pool0}/${big_image}"
    mirror_group_enable "${primary_cluster}" "${pool0}/${group}"
    wait_for_group_present "${secondary_cluster}" "${pool0}" "${group}" 1
  fi

  remove_image_retry "${primary_cluster}" "${pool0}" "${big_image}"

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool0}/${group}"
  test_images_in_latest_synced_group "${secondary_cluster}" "${pool0}/${group}" 1

  mirror_group_disable "${primary_cluster}" "${pool0}/${group}"
  group_remove "${primary_cluster}" "${pool0}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool0}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool0}" "${group}"

  image_remove "${primary_cluster}" "${pool0}/${image_prefix}"
}

# multiple images in group with io
declare -a test_create_group_with_multiple_images_do_io_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 5)

test_create_group_with_multiple_images_do_io_scenarios=1

test_create_group_with_multiple_images_do_io()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  local io_count=1024
  local io_size=4096

  local loop_instance
  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group}"
  test_images_in_latest_synced_group "${secondary_cluster}" "${pool}/${group}" "${image_count}"

  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
      compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}${loop_instance}"
  done

  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done

  snap='regular_snap'
  group_snap_create "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_exists "${primary_cluster}" "${pool}/${group}" "${snap}"

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_exists "${secondary_cluster}" "${pool}/${group}" "${snap}"
  test_images_in_latest_synced_group "${secondary_cluster}" "${pool}/${group}" "${image_count}"

  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
      compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}${loop_instance}"
  done

  group_snap_remove "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_doesnt_exist "${primary_cluster}" "${pool}/${group}" "${snap}"
  # this next extra mirror_group_snapshot should not be needed - waiting for fix TODO - coding leftover 38
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group}"
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"

  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
      compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}${loop_instance}"
  done

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
}

# multiple images in group with io
declare -a test_stopped_daemon_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 3)

test_stopped_daemon_scenarios=1

test_stopped_daemon()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local group_image_count=$(($1*"${image_multiplier}")) ; shift

  check_daemon_running "${secondary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${group_image_count}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${group_image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group}" "${secondary_cluster}" "${pool}"/"${group}" 

  local primary_group_snap_id
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}"/"${group}" primary_group_snap_id
  local secondary_group_snap_id
  get_newest_group_snapshot_id "${secondary_cluster}" "${pool}"/"${group}" secondary_group_snap_id
  test "${primary_group_snap_id}" = "${secondary_group_snap_id}" ||  { fail "mismatched ids"; return 1; }

  # Add image to synced group (whilst daemon is stopped)
  echo "stopping daemon"
  stop_mirrors "${secondary_cluster}"

  local image_name="test_image"
  image_create "${primary_cluster}" "${pool}/${image_name}" 

  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 

    if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
      mirror_group_snapshot_and_wait_for_sync_complete "${primary_cluster}" "${secondary_cluster}" "${pool}/${group}"
      wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group}" 'up+replaying' $(("${group_image_count}"+1))
    fi
  else
    mirror_group_disable "${primary_cluster}" "${pool}/${group}"
    group_image_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    # group on secondary cluster should still have the original number of images
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${group_image_count}"
  fi

  get_newest_group_snapshot_id "${primary_cluster}" "${pool}"/"${group}" primary_group_snap_id
  test "${primary_group_snap_id}" != "${secondary_group_snap_id}" ||  { fail "matched ids"; return 1; }

  echo "starting daemon"
  start_mirrors "${secondary_cluster}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" $(("${group_image_count}"+1))
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' $(("${group_image_count}"+1))
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group}" "${secondary_cluster}" "${pool}"/"${group}" 

  get_newest_group_snapshot_id "${secondary_cluster}" "${pool}"/"${group}" secondary_group_snap_id
  test "${primary_group_snap_id}" = "${secondary_group_snap_id}" ||  { fail "mismatched ids"; return 1; }

  # removed image from synced group (whilst daemon is stopped)
  echo "stopping daemon"
  stop_mirrors "${secondary_cluster}"

  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 

    if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
      mirror_group_snapshot_and_wait_for_sync_complete "${primary_cluster}" "${secondary_cluster}" "${pool}/${group}"
      wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group}" 'up+replaying' $(("${group_image_count}"))
    fi
  else
    mirror_group_disable "${primary_cluster}" "${pool}/${group}"
    group_image_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
  fi

  get_newest_group_snapshot_id "${primary_cluster}" "${pool}"/"${group}" primary_group_snap_id
  test "${primary_group_snap_id}" != "${secondary_group_snap_id}" ||  { fail "matched ids"; return 1; }

  echo "starting daemon"
  start_mirrors "${secondary_cluster}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${group_image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group}" "${secondary_cluster}" "${pool}"/"${group}" 

  get_newest_group_snapshot_id "${secondary_cluster}" "${pool}"/"${group}" secondary_group_snap_id
  test "${primary_group_snap_id}" = "${secondary_group_snap_id}" ||  { fail "mismatched ids"; return 1; }

  # TODO When dynamic groups are support this test could be extended with more actions whilst daemon is stopped.
  # eg add image, take snapshot, remove image, take snapshot, restart

  # Disable mirroring for synced group (whilst daemon is stopped)
  echo "stopping daemon"
  stop_mirrors "${secondary_cluster}"

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  test_group_present "${secondary_cluster}" "${pool}" "${group}" 1 "${group_image_count}"

  # restart daemon and confirm that group is removed from secondary
  echo "starting daemon"
  start_mirrors "${secondary_cluster}"

  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  image_remove "${primary_cluster}" "${pool}/${image_name}"
}

# multiple images in group and standalone images too with io
declare -a test_group_and_standalone_images_do_io_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}")

test_group_and_standalone_images_do_io_scenarios=1 

test_group_and_standalone_images_do_io()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4
  local image_prefix=$5

  local standalone_image_prefix=standalone-image
  local standalone_image_count=4
  local group_image_count=2

  images_create "${primary_cluster}" "${pool}/${standalone_image_prefix}" "${standalone_image_count}"

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  local fields=(//status/images/image/name //status/groups/group/name)
  local pool_fields_count_arr=()
  count_fields_in_mirror_pool_status "${primary_cluster}" "${pool}" pool_fields_count_arr "${fields[@]}"
  # Check count of images and groups in the command output
  test 0 = "${pool_fields_count_arr[0]}" || fail "unexpected count of images : ${pool_fields_count_arr[0]}"
  test 0 = "${pool_fields_count_arr[1]}" || fail "unexpected count of groups : ${pool_fields_count_arr[1]}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  pool_fields_count_arr=()
  count_fields_in_mirror_pool_status "${primary_cluster}" "${pool}" pool_fields_count_arr "${fields[@]}"
  # Check count of images and groups in the command output
  test $((${group_image_count})) = "${pool_fields_count_arr[0]}" || fail "unexpected count of images : ${pool_fields_count_arr[0]}"
  test 1 = "${pool_fields_count_arr[1]}" || fail "unexpected count of groups : ${pool_fields_count_arr[1]}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${group_image_count}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${group_image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  # enable mirroring for standalone images
  local loop_instance
  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    enable_mirror "${primary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    wait_for_image_replay_started  "${secondary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    wait_for_replay_complete "${secondary_cluster}" "${primary_cluster}" "${pool}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    wait_for_replaying_status_in_pool_dir "${secondary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    compare_images "${secondary_cluster}" "${primary_cluster}" "${pool}" "${pool}" "${standalone_image_prefix}${loop_instance}"
  done

  pool_fields_count_arr=()
  count_fields_in_mirror_pool_status "${primary_cluster}" "${pool}" pool_fields_count_arr "${fields[@]}"
  # Check count of images and groups in the command output
  test $((${standalone_image_count}+${group_image_count})) = "${pool_fields_count_arr[0]}" || fail "unexpected count of images : ${pool_fields_count_arr[0]}"
  test 1 = "${pool_fields_count_arr[1]}" || fail "unexpected count of groups : ${pool_fields_count_arr[1]}"

  local io_count=1024
  local io_size=4096

  # write to all of the images
  for loop_instance in $(seq 0 $(("${group_image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done
  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done

  # snapshot the group
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group}"
  test_images_in_latest_synced_group "${secondary_cluster}" "${pool}/${group}" "${group_image_count}"

  for loop_instance in $(seq 0 $(("${group_image_count}"-1))); do
    compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}${loop_instance}"
  done

  # snapshot the individual images too, wait for sync and compare
  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    mirror_image_snapshot "${primary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    wait_for_snapshot_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${standalone_image_prefix}${loop_instance}"
  done

  # do more IO
  for loop_instance in $(seq 0 $(("${group_image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done
  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done

  # Snapshot the group and images.  Sync both in parallel
  local group_snap_id
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group}" group_snap_id
  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    mirror_image_snapshot "${primary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}"
  done

  wait_for_group_snap_sync_complete "${secondary_cluster}" "${pool}/${group}" "${group_snap_id}"
  for loop_instance in $(seq 0 $(("${group_image_count}"-1))); do
    compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}${loop_instance}"
  done

  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    wait_for_snapshot_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${standalone_image_prefix}${loop_instance}"
  done

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  # re-check images
  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${standalone_image_prefix}${loop_instance}"
  done

  # disable mirroring for standalone images
  local loop_instance
  for loop_instance in $(seq 0 $(("${standalone_image_count}"-1))); do
    disable_mirror "${primary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}"
    wait_for_image_present "${secondary_cluster}" "${pool}" "${standalone_image_prefix}${loop_instance}" 'deleted'
  done

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  images_remove "${primary_cluster}" "${pool}/${standalone_image_prefix}" "${standalone_image_count}"
}

# multiple groups with images in each with io
# mismatched size groups (same size images)
declare -a test_create_multiple_groups_do_io_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 1 10 128 5 2 128)
declare -a test_create_multiple_groups_do_io_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 1 10 128 1 1 128)
# mismatched size groups (mismatched size images)
declare -a test_create_multiple_groups_do_io_3=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 1 10 4 5 2 128)
declare -a test_create_multiple_groups_do_io_4=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 1 10 4 1 1 128)
# equal size groups
declare -a test_create_multiple_groups_do_io_5=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 2 5 128 2 5 128)

test_create_multiple_groups_do_io_scenarios=5

test_create_multiple_groups_do_io()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group_a_count=$4
  local group_a_image_count=$5
  local group_a_image_size=$6
  local group_b_count=$7
  local group_b_image_count=$8
  local group_b_image_size=$9

  local image_count=$(("${group_a_count}"*"${group_a_image_count}" + "${group_b_count}"*"${group_b_image_count}"))
  local image_prefix='test-image'
  local group_prefix='test-group'
  local loop_instance
  local group_instance

  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    group_create "${primary_cluster}" "${pool}/${group_prefix}-a${loop_instance}"
    images_create "${primary_cluster}" "${pool}/${image_prefix}-a${loop_instance}-" "${group_a_image_count}" "${group_a_image_size}"
    group_images_add "${primary_cluster}" "${pool}/${group_prefix}-a${loop_instance}" "${pool}/${image_prefix}-a${loop_instance}-" "${group_a_image_count}"
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    group_create "${primary_cluster}" "${pool}/${group_prefix}-b${loop_instance}"
    images_create "${primary_cluster}" "${pool}/${image_prefix}-b${loop_instance}-" "${group_b_image_count}" "${group_b_image_size}"
    group_images_add "${primary_cluster}" "${pool}/${group_prefix}-b${loop_instance}" "${pool}/${image_prefix}-b${loop_instance}-" "${group_b_image_count}"
  done

  # enable mirroring for every group
  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    mirror_group_enable "${primary_cluster}" "${pool}/${group_prefix}-a${loop_instance}"
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    mirror_group_enable "${primary_cluster}" "${pool}/${group_prefix}-b${loop_instance}"
  done

  # check that every group appears on the secondary
  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group_prefix}-a${loop_instance}" "${group_a_image_count}"
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group_prefix}-b${loop_instance}" "${group_b_image_count}"
  done

  # export RBD_MIRROR_INSTANCES=X and rerun the test to check assignment
  # image size appears not to influence the distribution of group replayers
  # number of images in a group does influence the distribution
  # FUTURE - implement some checking on the assignment rather than just printing it
  #        - also maybe change image count in groups and check rebalancing
  local group_arr
  local image_arr
  for instance in $(seq 0 "${LAST_MIRROR_INSTANCE}"); do
    local result
    query_replayer_assignment "${secondary_cluster}" "${instance}" result
    group_arr+=("${result[0]}")
    image_arr+=("${result[1]}")
    group_count_arr+=("${result[2]}")
    image_count_arr+=("${result[3]}")
  done
  for instance in $(seq 0 "${LAST_MIRROR_INSTANCE}"); do
      echo -e "${RED}MIRROR DAEMON INSTANCE:${instance}${NO_COLOUR}";
      echo -e "${RED}GROUP_REPLAYERS:${group_count_arr[$instance]}${NO_COLOUR}";
      echo -e "${RED}${group_arr[$instance]}${NO_COLOUR}";
      echo -e "${RED}IMAGE_REPLAYERS:${image_count_arr[$instance]}${NO_COLOUR}";
      echo -e "${RED}${image_arr[$instance]}${NO_COLOUR}";
  done  

  # check that every group and image are in the correct state on the secondary
  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group_prefix}-a${loop_instance}" "${group_a_image_count}"
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group_prefix}-a${loop_instance}" 'up+replaying' "${group_a_image_count}"

    if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
      wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group_prefix}-a${loop_instance}" 'down+unknown' 0
    fi
  done  
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group_prefix}-b${loop_instance}" "${group_b_image_count}"
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group_prefix}-b${loop_instance}" 'up+replaying' "${group_b_image_count}"

    if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
      wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group_prefix}-b${loop_instance}" 'down+unknown' 0
    fi
  done  

  local io_count=10240
  local io_size=4096
  local group_to_mirror='-a0'

  # write to every image in one a group, mirror group and compare images
  for loop_instance in $(seq 0 $(("${group_a_image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${group_to_mirror}-${loop_instance}" "${io_count}" "${io_size}"
  done

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group_prefix}${group_to_mirror}"

  for loop_instance in $(seq 0 $(("${group_a_image_count}"-1))); do
    compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}${group_to_mirror}-${loop_instance}"
  done

  group_to_mirror='-b0'

  # write to every image in one b group, mirror group and compare images
  for loop_instance in $(seq 0 $(("${group_b_image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${group_to_mirror}-${loop_instance}" "${io_count}" "${io_size}"
  done

  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group_prefix}${group_to_mirror}"

  for loop_instance in $(seq 0 $(("${group_b_image_count}"-1))); do
    compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}${group_to_mirror}-${loop_instance}"
  done

  # write to one image in every group, mirror groups and compare images
  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}-a${loop_instance}-0" "${io_count}" "${io_size}"
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}-b${loop_instance}-0" "${io_count}" "${io_size}"
  done

  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group_prefix}-a${loop_instance}" 
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group_prefix}-b${loop_instance}" 
  done

  for group_instance in $(seq 0 $(("${group_a_count}"-1))); do
    for loop_instance in $(seq 0 $(("${group_a_image_count}"-1))); do
      compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}-a${group_instance}-${loop_instance}"
    done
  done
  for group_instance in $(seq 0 $(("${group_b_count}"-1))); do
    for loop_instance in $(seq 0 $(("${group_b_image_count}"-1))); do
      compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}-b${group_instance}-${loop_instance}"
    done
  done

  # write to every image in every group, mirror groups and compare images
  for group_instance in $(seq 0 $(("${group_a_count}"-1))); do
    for loop_instance in $(seq 0 $(("${group_a_image_count}"-1))); do
      write_image "${primary_cluster}" "${pool}" "${image_prefix}-a${group_instance}-${loop_instance}" "${io_count}" "${io_size}"
    done
  done
  for group_instance in $(seq 0 $(("${group_b_count}"-1))); do
    for loop_instance in $(seq 0 $(("${group_b_image_count}"-1))); do
      write_image "${primary_cluster}" "${pool}" "${image_prefix}-b${group_instance}-${loop_instance}" "${io_count}" "${io_size}"
    done
  done

  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group_prefix}-a${loop_instance}" 
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group_prefix}-b${loop_instance}" 
  done

  for group_instance in $(seq 0 $(("${group_a_count}"-1))); do
    for loop_instance in $(seq 0 $(("${group_a_image_count}"-1))); do
      compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}-a${group_instance}-${loop_instance}"
    done
  done
  for group_instance in $(seq 0 $(("${group_b_count}"-1))); do
    for loop_instance in $(seq 0 $(("${group_b_image_count}"-1))); do
      compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}-b${group_instance}-${loop_instance}"
    done
  done

  # disable and remove all groups
  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    mirror_group_disable "${primary_cluster}" "${pool}/${group_prefix}-a${loop_instance}"
    group_remove "${primary_cluster}" "${pool}/${group_prefix}-a${loop_instance}"
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    mirror_group_disable "${primary_cluster}" "${pool}/${group_prefix}-b${loop_instance}"
    group_remove "${primary_cluster}" "${pool}/${group_prefix}-b${loop_instance}"
  done

  # check all groups have been deleted and remove images
  for loop_instance in $(seq 0 $(("${group_a_count}"-1))); do
    wait_for_group_not_present "${primary_cluster}" "${pool}" "${group_prefix}-a${loop_instance}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group_prefix}-a${loop_instance}"
    images_remove "${primary_cluster}" "${pool}/${image_prefix}-a${loop_instance}-" "${group_a_image_count}"
  done
  for loop_instance in $(seq 0 $(("${group_b_count}"-1))); do
    wait_for_group_not_present "${primary_cluster}" "${pool}" "${group_prefix}-b${loop_instance}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group_prefix}-b${loop_instance}"
    images_remove "${primary_cluster}" "${pool}/${image_prefix}-b${loop_instance}-" "${group_b_image_count}"
  done
}

# mirror a group then remove an image from that group and add to a different mirrored group.
declare -a test_image_move_group_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 5)

test_image_move_group_scenarios=1

test_image_move_group()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  local group0=test-group0
  local group1=test-group1

  group_create "${primary_cluster}" "${pool}/${group0}"
  group_create "${primary_cluster}" "${pool}/${group1}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'down+unknown' 0
  fi

  local io_count=10240
  local io_size=4096

  # write to every image in the group, mirror group
  for loop_instance in $(seq 0 $(("${image_count}"-1))); do
    write_image "${primary_cluster}" "${pool}" "${image_prefix}${loop_instance}" "${io_count}" "${io_size}"
  done
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}/${group0}"

  # remove an image from the group and add to a different group
  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}4" 
  else
    mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}4" 
    mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  fi

  group_image_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix}4"
  mirror_group_enable "${primary_cluster}" "${pool}/${group1}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}" 1

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group1}" 1
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying' 1
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" $(("${image_count}"-1))
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' $(("${image_count}"-1))

  # remove another image from group0 - add to group 1 (add to a group that is already mirror enabled)
  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}2" 
    group_image_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix}2"
  else
    mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
    mirror_group_disable "${primary_cluster}" "${pool}/${group1}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"
    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group1}"
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}2" 
    mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
    group_image_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix}2"
    mirror_group_enable "${primary_cluster}" "${pool}/${group1}"
  fi

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}" 2
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group1}" 2
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying' 2
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" $(("${image_count}"-2))
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' $(("${image_count}"-2))

  echo "stopping daemon"
  stop_mirrors "${secondary_cluster}"

  # remove another image from group0 - add to group 1 (add to a group that is already mirror enabled) with the mirror daemon stopped
  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}0" 
    group_image_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix}0"
  else
    mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
    mirror_group_disable "${primary_cluster}" "${pool}/${group1}"
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}0" 
    group_image_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix}0"
    mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
    mirror_group_enable "${primary_cluster}" "${pool}/${group1}"
  fi

  echo "starting daemon"
  start_mirrors "${secondary_cluster}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}" 3
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group1}" 3
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying' 3
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" $(("${image_count}"-3))
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' $(("${image_count}"-3))

  echo "stopping daemon"
  stop_mirrors "${secondary_cluster}"

  # remove another image from group0 - add to group 1 (add to a group that is already mirror enabled) with the mirror daemon stopped
  # this time the moved image is still present in a snapshot for the old group that needs syncing and in a snapshot for the new group
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}"
  
  if [ -n "${RBD_MIRROR_SUPPORT_DYNAMIC_GROUPS}" ]; then
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}1" 
    group_image_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix}1"
  else
    mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
    mirror_group_disable "${primary_cluster}" "${pool}/${group1}"
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}1" 
    group_image_add "${primary_cluster}" "${pool}/${group1}" "${pool}/${image_prefix}1"
    mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
    mirror_group_enable "${primary_cluster}" "${pool}/${group1}"
  fi

  echo "starting daemon"
  start_mirrors "${secondary_cluster}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group1}" 4
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group1}" 4
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group1}" 'up+replaying' 4
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" $(("${image_count}"-4))
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' $(("${image_count}"-4))

  # TODO when dynamic groups are supported, this test could be extended to set up a chain of moves 
  # ie stop daemon, move image from group A->B, move image from B->C then restart daemon

  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  group_remove "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  mirror_group_disable "${primary_cluster}" "${pool}/${group1}"
  group_remove "${primary_cluster}" "${pool}/${group1}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group1}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group1}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
}

# test force promote scenarios
# TODO first two scenarios require support for dynamic groups
#declare -a test_force_promote_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'image_add' 5)
#declare -a test_force_promote_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'image_remove' 5)
declare -a test_force_promote_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'no_change' 5)
declare -a test_force_promote_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'image_expand' 5)
declare -a test_force_promote_3=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'image_shrink' 5)
declare -a test_force_promote_4=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'image_rename' 5)
declare -a test_force_promote_5=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'no_change_primary_up' 5)

# TODO scenario 4 is currently failing - low priority
# test_force_promote_scenarios=5
test_force_promote_scenarios='1 2 3 5'

test_force_promote()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local scenario=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  local group0=test-group0
  local snap0='snap_0'
  local snap1='snap_1'

  if [ "${scenario}" = 'no_change_primary_up' ]; then
    start_mirrors "${primary_cluster}"
  fi

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" $(("${image_count}"-1))
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" $(("${image_count}"-1))
  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap0}"
  compare_image_with_snapshot "${primary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  big_image=test-image-big
  image_create "${primary_cluster}" "${pool}/${big_image}" 4G
  write_image "${primary_cluster}" "${pool}" "${big_image}" 1024 4096
  group_image_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${big_image}"
  create_snapshot "${primary_cluster}" "${pool}" "${big_image}" "${snap0}"
  compare_image_with_snapshot "${primary_cluster}" "${pool}/${big_image}" "${primary_cluster}" "${pool}/${big_image}@${snap0}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    if [ "${scenario}" = 'no_change_primary_up' ]; then
      wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' "${image_count}"
    else
      wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'down+unknown' 0
    fi
  fi

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${big_image}" "${primary_cluster}" "${pool}/${big_image}@${snap0}"

  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap1}"

  # make some changes to the big image so that the next sync will take a long time
  write_image "${primary_cluster}" "${pool}" "${big_image}" 1024 4194304
  create_snapshot "${primary_cluster}" "${pool}" "${big_image}" "${snap1}"

  local global_id
  local image_size
  local test_image_size
  if [ "${scenario}" = 'image_add' ]; then
    new_image=test-image-new
    image_create "${primary_cluster}" "${pool}/${new_image}" 
    group_image_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${new_image}"
    get_image_mirroring_global_id "${primary_cluster}" "${pool}/${new_image}" global_id
    test_image_with_global_id_present "${primary_cluster}" "${pool}" "${new_image}" "${global_id}"
    test_image_with_global_id_not_present "${secondary_cluster}" "${pool}" "${new_image}" "${global_id}"
  elif [ "${scenario}" = 'image_remove' ]; then
    get_image_mirroring_global_id "${primary_cluster}" "${pool}/${image_prefix}0" global_id
    test_image_with_global_id_present "${secondary_cluster}" "${pool}" "${image_prefix}0" "${global_id}"
    group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}0"
    test_image_with_global_id_not_present "${primary_cluster}" "${pool}" "${image_prefix}0" "${global_id}"
  elif [ "${scenario}" = 'image_rename' ]; then
    get_image_mirroring_global_id "${primary_cluster}" "${pool}/${image_prefix}0" global_id
    image_rename "${primary_cluster}" "${pool}/${image_prefix}0" "${pool}/${image_prefix}_renamed_0"
    test_image_with_global_id_present "${primary_cluster}" "${pool}" "${image_prefix}_renamed_0" "${global_id}"
    test_image_with_global_id_present "${secondary_cluster}" "${pool}" "${image_prefix}0" "${global_id}"
    test_image_with_global_id_not_present "${secondary_cluster}" "${pool}" "${image_prefix}_renamed_0" "${global_id}"
    mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}"
  elif [ "${scenario}" = 'image_expand' ]; then
    get_image_size "${primary_cluster}" "${pool}/${image_prefix}2" image_size
    image_resize "${primary_cluster}" "${pool}/${image_prefix}2" $((("${image_size}"/1024/1024)+4))
    test_image_size_matches "${primary_cluster}" "${pool}/${image_prefix}2" $(("${image_size}"+4*1024*1024))
    test_image_size_matches "${secondary_cluster}" "${pool}/${image_prefix}2" "${image_size}"
    mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}"
  elif [ "${scenario}" = 'image_shrink' ]; then
    get_image_size "${primary_cluster}" "${pool}/${image_prefix}3" image_size
    image_resize "${primary_cluster}" "${pool}/${image_prefix}3" $((("${image_size}"/1024/1024)-4)) '--allow-shrink'
    test_image_size_matches "${primary_cluster}" "${pool}/${image_prefix}3" $(("${image_size}"-4*1024*1024))
    test_image_size_matches "${secondary_cluster}" "${pool}/${image_prefix}3" "${image_size}"
    mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}"
  elif [ "${scenario}" = 'no_change' ] || [ "${scenario}" = 'no_change_primary_up' ]; then
    mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}"
  fi

  local group_snap_id
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}/${group0}" group_snap_id
  echo "id = ${group_snap_id}"
  wait_for_test_group_snap_present "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}" 1

  if [ "${scenario}" = 'image_add' ]; then
    wait_for_image_present "${secondary_cluster}" "${pool}" "${new_image}" 'present' 
    test_image_with_global_id_present "${secondary_cluster}" "${pool}" "${new_image}" "${global_id}"
  elif [ "${scenario}" = 'image_remove' ]; then
    wait_for_image_present "${secondary_cluster}" "${pool}" "${image_prefix}0" 'deleted'
    test_image_with_global_id_not_present "${secondary_cluster}" "${pool}" "${image_prefix}0" "${global_id}"
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" $(("${image_count}"-1))
  elif [ "${scenario}" = 'image_rename' ]; then
    wait_for_image_present "${secondary_cluster}" "${pool}" "${image_prefix}_renamed_0" 'present' 
    test_image_with_global_id_present "${secondary_cluster}" "${pool}" "${image_prefix}_renamed_0" "${global_id}"
  elif [ "${scenario}" = 'image_expand' ]; then
    wait_for_image_size_matches "${secondary_cluster}" "${pool}/${image_prefix}2" $(("${image_size}"+4*1024*1024))
  elif [ "${scenario}" = 'image_shrink' ]; then
    wait_for_image_size_matches "${secondary_cluster}" "${pool}/${image_prefix}3" $(("${image_size}"-4*1024*1024))
  elif [ "${scenario}" = 'no_change' ] || [ "${scenario}" = 'no_change_primary_up' ]; then
    # ensure that a small image has completed its sync
    local snap_id
    get_newest_mirror_snapshot_id_on_primary "${primary_cluster}" "${pool}/${image_prefix}0" snap_id
    wait_for_snap_id_present "${secondary_cluster}" "${pool}/${image_prefix}0" "${snap_id}"
    wait_for_snapshot_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}" "${pool}" "${image_prefix}0" "${snap_id}"
  fi


  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}" '-9'

  # check that latest snap is incomplete
  test_group_snap_sync_incomplete "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}" 

  if [ "${scenario}" = 'no_change' ] || [ "${scenario}" = 'no_change_primary_up' ]; then
    # if we waited for a small image to complete its sync then the big image should still be mid-sync
    # if we didn't wait for the small image to sync then its possible that the big image hasn't even started syncing the latest image yet
    local big_image_snap_id
    get_newest_mirror_snapshot_id_on_primary "${primary_cluster}" "${pool}/${big_image}" big_image_snap_id
    test_snap_complete "${secondary_cluster}" "${pool}/${big_image}" "${big_image_snap_id}" 'false' || fail "big image is synced"
  fi

  # force promote the group on the secondary - should rollback to the last complete snapshot
  local old_primary_cluster
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'

  old_primary_cluster="${primary_cluster}"
  primary_cluster="${secondary_cluster}"

  mirror_group_demote "${old_primary_cluster}" "${pool}/${group0}"
  secondary_cluster="${old_primary_cluster}"

  # check that we rolled back to snap0 state
  compare_image_with_snapshot "${primary_cluster}" "${pool}/${image_prefix}0" "${secondary_cluster}" "${pool}/${image_prefix}0@${snap0}"
  compare_image_with_snapshot "${primary_cluster}" "${pool}/${big_image}" "${secondary_cluster}" "${pool}/${big_image}@${snap0}"

  # Check that the rollback reverted the state 
  if [ "${scenario}" = 'image_add' ]; then
    # check that new image is not present
    test_image_with_global_id_not_present "${primary_cluster}" "${pool}" "${new_image}" "${global_id}"
  elif [ "${scenario}" = 'image_remove' ]; then
    test_image_with_global_id_present "${primary_cluster}" "${pool}" "${image_prefix}0" "${global_id}"
  elif [ "${scenario}" = 'image_rename' ]; then
    # check that the image is back with the original name
    test_image_with_global_id_not_present "${primary_cluster}" "${pool}" "${image_prefix}_renamed_0" "${global_id}"
    test_image_with_global_id_present "${primary_cluster}" "${pool}" "${image_prefix}0" "${global_id}"
  elif [ "${scenario}" = 'image_expand' ]; then
    test_image_size_matches "${primary_cluster}" "${pool}/${image_prefix}2" "${image_size}" || fail "size mismatch"
  elif [ "${scenario}" = 'image_shrink' ]; then
    test_image_size_matches "${primary_cluster}" "${pool}/${image_prefix}3" "${image_size}" || fail "size mismatch"
  fi

  local group_id_before
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_before

  mirror_group_resync "${secondary_cluster}" "${pool}/${group0}"

  if [ "${scenario}" != 'no_change_primary_up' ]; then
    start_mirrors "${secondary_cluster}"
    sleep 5
  fi  

  wait_for_group_id_changed "${secondary_cluster}" "${pool}/${group0}" "${group_id_before}"

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}/${group0}"

  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${secondary_cluster}" "${pool}/${image_prefix}0@${snap0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${big_image}" "${secondary_cluster}" "${pool}/${big_image}@${snap0}"

  # Check that snapshots work on the new primary
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}" group_snap_id
  wait_for_group_snap_present "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}"
  wait_for_group_snap_sync_complete "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}"

  # tidy up
  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  group_remove "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" $(("${image_count}"-1))
  image_remove "${primary_cluster}" "${pool}/${big_image}"

  # Note: we altered primary and secondary cluster, so reset and restart daemon
  old_primary_cluster="${primary_cluster}"
  primary_cluster="${secondary_cluster}"
  secondary_cluster="${old_primary_cluster}"
  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  start_mirrors "${secondary_cluster}"
}

declare -a test_force_promote_delete_group_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 5)

test_force_promote_delete_group_scenarios=1

test_force_promote_delete_group()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  local group0=test-group0

  start_mirrors "${primary_cluster}"
  start_mirrors "${secondary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' "${image_count}" 
  fi

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}"

  # force promote the group on the secondary 
  # disable mirror daemon here - see slack thread https://ibm-systems-storage.slack.com/archives/C07J9Q2E268/p1739856204809159
  stop_mirrors "${secondary_cluster}" '-9'
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'
  start_mirrors "${secondary_cluster}"
  wait_for_group_replay_stopped "${secondary_cluster}" "${pool}/${group0}"
  wait_for_group_replay_stopped "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group0}" 'up+stopped' "${image_count}"
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group0}" 'up+stopped' "${image_count}"

  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  mirror_group_disable "${secondary_cluster}" "${pool}/${group0}"

  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  # group still exists on original primary
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group0}" 'up+stopped' "${image_count}"

  group_image_remove "${secondary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}0"

  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" $(("${image_count}"-1))
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group0}" 'snapshot' 'enabled' 'true'
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group0}" 'up+stopped' "${image_count}"

  mirror_group_enable "${secondary_cluster}" "${pool}/${group0}"
  test_fields_in_group_info "${primary_cluster}" "${pool}/${group0}" 'snapshot' 'enabled' 'true'
  test_fields_in_group_info "${secondary_cluster}" "${pool}/${group0}" 'snapshot' 'enabled' 'true'

  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group0}" 'up+stopped' "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group0}" 'up+stopped' $(("${image_count}"-1))

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" $(("${image_count}"-1))
  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"

  group_remove "${secondary_cluster}" "${pool}/${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"
  images_remove "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"

  # disable and re-enable on original primary
  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"

  # confirm that group is mirrored back to secondary
  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  # tidy up
  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  group_remove "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"

  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
}

declare -a test_force_promote_before_initial_sync_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 5)

test_force_promote_before_initial_sync_scenarios=1

test_force_promote_before_initial_sync()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  local group0=test-group0

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" $(("${image_count}"-1))
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" $(("${image_count}"-1))

  big_image=test-image-big
  local sync_incomplete=false

  multiplier=1
  while [ "${sync_incomplete}" = false ]; do
    image_size=$((multiplier*1024))
    io_count=$((multiplier*256))

    image_create "${primary_cluster}" "${pool}/${big_image}" "${image_size}M"
    # make some changes to the big image so that the sync will take a long time count,size 
    # io-total = count*size 1K, 4M (1024, 4M writes)
    write_image "${primary_cluster}" "${pool}" "${big_image}" "${io_count}" 4194304
    group_image_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${big_image}"

    mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
    local group_snap_id
    get_newest_group_snapshot_id "${primary_cluster}" "${pool}/${group0}" group_snap_id
    wait_for_test_group_snap_present "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}" 1

    # stop the daemon to prevent further syncing of snapshots
    stop_mirrors "${secondary_cluster}" '-9'

    # see if the latest snap is incomplete
    test_group_snap_sync_incomplete "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}" && sync_incomplete=true

    # If the sync for the last snapshot is already complete then we need to repeat with a larger image and write more data.
    # Disable mirroring, delete the image and go round the loop again
    if [ "${sync_incomplete}" = false ]; then
      start_mirrors "${secondary_cluster}"
      # wait for daemon to restart
      wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"
      mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
      group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${big_image}"
      image_remove "${primary_cluster}" "${pool}/${big_image}"
      wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

      multiplier=$((multiplier*2))
    fi

  done

  # force promote the group on the secondary - this should fail with a sensible error message
  expect_failure "cannot rollback, no complete mirror group snapshot available on group" rbd --cluster=${secondary_cluster} mirror group promote ${pool}/${group0} --force

  local group_id_before
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_before
  mirror_group_resync "${secondary_cluster}" "${pool}/${group0}"
  start_mirrors "${secondary_cluster}"
  wait_for_group_id_changed "${secondary_cluster}" "${pool}/${group0}" "${group_id_before}"

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}/${group0}"

  # try another force promote - this time it should work
  stop_mirrors "${secondary_cluster}" '-9'
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'

  # demote and try to resync again
  mirror_group_demote "${secondary_cluster}" "${pool}/${group0}"

  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_before
  mirror_group_resync "${secondary_cluster}" "${pool}/${group0}"
  start_mirrors "${secondary_cluster}"
  wait_for_group_id_changed "${secondary_cluster}" "${pool}/${group0}" "${group_id_before}"

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}/${group0}"

  # tidy up
  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  group_remove "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" $(("${image_count}"-1))
  image_remove "${primary_cluster}" "${pool}/${big_image}"

  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  start_mirrors "${secondary_cluster}"
}

# test force unlink time
declare -a test_multiple_mirror_group_snapshot_unlink_time_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}")

test_multiple_mirror_group_snapshot_unlink_time_scenarios=1

test_multiple_mirror_group_snapshot_unlink_time()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  
  local image_count
  local image_counts=(2 16)
  local results=()
  local time

  for image_count in "${image_counts[@]}"; do
    test_multiple_mirror_group_snapshot_whilst_stopped "${primary_cluster}" "${secondary_cluster}" "${pool}" "${image_count}" time
    results+=(${time})
  done

  for i in $(seq 0 "${#results[@]}"); do
    echo -e "${RED}image count:${image_counts[$i]} snapshot time:${results[$i]}${NO_COLOUR}"
  done

  if [ ${results[1]} -gt $((${results[0]}+3)) ]; then
    fail "Snapshot time isn't independent of the group image count" 
  fi
}

# test force promote scenarios
declare -a test_multiple_mirror_group_snapshot_whilst_stopped_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 5)

test_multiple_mirror_group_snapshot_whilst_stopped_scenarios=1

test_multiple_mirror_group_snapshot_whilst_stopped()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift
  if [ -n "$1" ]; then
    local get_average='true'
    local -n _average_snapshot_time=$1 ; shift
  fi  

  local group0=test-group0
  local image_prefix="test_image"

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}" 12M
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}" 

  echo "stopping daemon on secondary"
  stop_mirrors "${secondary_cluster}"

  local count
  get_group_snap_count "${secondary_cluster}" "${pool}"/"${group0}" '*' count
  test "${count}" = 1 || { fail "snap count = ${count}"; return 1; }

  local start_time end_time
  local times_result_arr=()
  for i in {0..9}; do  
    start_time=$(date +%s)
    mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}"
    end_time=$(date +%s)
    echo -e "mirror group snapshot time ="$((end_time-start_time))
    times_result_arr+=($((end_time-start_time)))
  done;

  if [ -n "$get_average" ]; then
    local cnt total
    total=0
    cnt=0
    # ignore the times of the first 5 snapshots then determine the average of the rest
    for i in {5..9}; do  
      total=$((total + times_result_arr[$i]))
      cnt=$((cnt + 1))
    done;
    echo "average=$((total/cnt))"
    _average_snapshot_time=$((total/cnt))
  fi

  get_group_snap_count "${primary_cluster}" "${pool}"/"${group0}" '*' count
  test "${count}" -gt 3 || { fail "snap count = ${count}"; return 1; }

  get_group_snap_count "${secondary_cluster}" "${pool}"/"${group0}" '*' count
  test "${count}" -eq 1 || { fail "snap count = ${count}"; return 1; }

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  echo "starting daemon on secondary"
  start_mirrors "${secondary_cluster}"

  wait_for_group_synced "${primary_cluster}" "${pool}/${group0}" "${secondary_cluster}" "${pool}"/"${group0}" 
  max_image=$((image_count-1))
  for i in $(seq 0 "${max_image}"); do  
      wait_for_status_in_pool_dir "${secondary_cluster}" "${pool}" "${image_prefix}$i" 'up+replaying'
  done;

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"

  # tidy up
  mirror_group_disable "${primary_cluster}" "${pool}/${group0}"
  group_remove "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"
  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
}

# test ODF failover/failback sequence
declare -a test_odf_failover_failback_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'wait_before_promote' 3)
declare -a test_odf_failover_failback_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'retry_promote' 3)
declare -a test_odf_failover_failback_3=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'resync_on_failback' 1)

test_odf_failover_failback_scenarios=3

# ODF takes the following steps in failover/failback.  This test does the same.
# Failover:
# rbd --cluster=site-b mirror group promote test_pool/test_group --force
#
# When site-a comes alive again request a resync
# rbd --cluster=site-a mirror group demote test_pool/test_group
# rbd --cluster=site-a mirror group resync test_pool/test_group
#
# Failback:
# rbd --cluster=site-b mirror group demote test_pool/test_group
#  (scenario 3 requests a resync on site-b here)
# rbd --cluster=site-a mirror group promote test_pool/test_group
test_odf_failover_failback()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local scenario=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  local snap0='snap_0'
  local snap1='snap_1'

  # ODF has daemon running on both clusters always
  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" "${image_count}"

  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap0}"
  compare_image_with_snapshot "${primary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' 

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}" 
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  # promote secondary (cluster1), demote original primary (cluster2) and request resync
  stop_mirrors "${secondary_cluster}" '-9'
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'
  start_mirrors "${secondary_cluster}"
  # original site comes alive again
  mirror_group_demote "${primary_cluster}" "${pool}/${group0}"

  local group_id_before
  get_id_from_group_info "${primary_cluster}" "${pool}/${group0}" group_id_before
  mirror_group_resync "${primary_cluster}" "${pool}/${group0}" 
  wait_for_group_id_changed "${primary_cluster}" "${pool}/${group0}" "${group_id_before}"
  wait_for_group_synced "${secondary_cluster}" "${pool}"/"${group0}" "${primary_cluster}" "${pool}/${group0}" 

  compare_image_with_snapshot "${primary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  write_image "${secondary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  compare_image_with_snapshot_expect_difference "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"
  mirror_group_snapshot_and_wait_for_sync_complete "${primary_cluster}" "${secondary_cluster}" "${pool}"/"${group0}"
  compare_images "${secondary_cluster}" "${primary_cluster}" "${pool}" "${pool}" "${image_prefix}0"

  write_image "${secondary_cluster}" "${pool}" "${image_prefix}0" 10 4096

  # failback to original primary (cluster2)
  local group_snap_id_a group_snap_id_b
  get_newest_group_snapshot_id "${secondary_cluster}" "${pool}"/"${group0}" group_snap_id_a
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}"/"${group0}" group_snap_id_b
  test "${group_snap_id_a}" = "${group_snap_id_b}" || fail "group not synced"

  # demote - neither site is primary
  mirror_group_demote "${secondary_cluster}" "${pool}/${group0}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+unknown'

  # confirm that a new snapshot was taken by the demote operation
  local group_snap_id_c
  get_newest_group_snapshot_id "${secondary_cluster}" "${pool}"/"${group0}" group_snap_id_c
  test "${group_snap_id_a}" != "${group_snap_id_c}" || fail "new snap not taken by demote"

  local group_id_before group_id_after
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_before
  local image_id_before image_id_after
  get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_before
  
  if [ "${scenario}" = 'resync_on_failback' ]; then
    # request resync - won't happen until other site is marked as primary
    mirror_group_resync "${secondary_cluster}" "${pool}/${group0}" 
  fi  

  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_after
  test "${group_id_before}" = "${group_id_after}" || fail "group recreated with no primary"
  get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_after
  test "${image_id_before}" = "${image_id_after}" || fail "image recreated with no primary"

  if [ "${scenario}" != 'retry_promote' ]; then
    # wait for the demote snapshot to be synced before promoting the other site
    wait_for_group_synced "${secondary_cluster}" "${pool}"/"${group0}" "${primary_cluster}" "${pool}/${group0}" 

    local group_snap_id_e group_snap_id_f
    get_newest_group_snapshot_id "${secondary_cluster}" "${pool}"/"${group0}" group_snap_id_e
    get_newest_group_snapshot_id "${primary_cluster}" "${pool}"/"${group0}" group_snap_id_f
    test "${group_snap_id_c}" = "${group_snap_id_e}" || fail "new snap on original secondary"
    test "${group_snap_id_c}" = "${group_snap_id_f}" || fail "group not synced"

    # Waiting for demote snapshot to be synced is not a sufficient check on its own.  
    # Must wait for up+unknown state to be reported.
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+unknown'
  fi

  # promote original primary again
  if [ "${scenario}" = 'retry_promote' ]; then
    local i=0
    while true; do
      { mirror_group_promote_try "${primary_cluster}" "${pool}/${group0}" && break; } || :
      if [ "$i" -lt 10000 ]; then 
        i=$((i+1))
      else
        fail "promote command failed after $i attempts"
      fi  
    done
    echo "promote command succeeded after $i attempts"
  else
    mirror_group_promote "${primary_cluster}" "${pool}/${group0}"
  fi  

  if [ "${scenario}" = 'resync_on_failback' ]; then
    # check that group and images were deleted and recreated on secondary cluster (as a result of the resync request)
    wait_for_group_id_changed "${secondary_cluster}" "${pool}/${group0}" "${group_id_before}"
    get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_after
    test "${image_id_before}" != "${image_id_after}" || fail "image not recreated by resync"
  fi  

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped'

  # Write some data, take a regular mirror snapshot, wait for it to sync on secondary cluster
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group0}"

  group_remove "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  check_daemon_running "${secondary_cluster}"
}

# test ODF failover/failback sequence
declare -a test_resync_marker_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'no_change' 3)

test_resync_marker_scenarios=1

# This test does the following:
# 1) setup a nothing-fancy group, write some data, let it sync from site-a to site-b
# 2) demote site-a
# 3) execute rbd mirror group resync command on site-b
# 4) assert that nothing happens at this point (on a bad build the group on site-b would be removed after some time, note that rbd-mirror daemons must be running for that to happen)
# 5) promote site-b
# 6) write some more data, let it sync from site-b to site-a
# 7) demote site-b
# 8) promote site-a
# 9) ensure that the group on site-b doesnt get resynced at this point 
# 10) write some data on site-a and let it sync to site-b
# 11) check that site-b group id has not changed again -  since just after step 5
test_resync_marker()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local scenario=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  local snap0='snap_0'
  local snap1='snap_1'

  # ODF has daemon running on both clusters always
  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' 

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}"

  local group_id_before group_id_after
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_before
  local image_id_before image_id_after
  get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_before

  # demote primary and request resync on secondary - check that group does not get deleted (due to resync request flag)
  mirror_group_demote "${primary_cluster}" "${pool}/${group0}" 
  mirror_group_resync "${secondary_cluster}" "${pool}/${group0}" 
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+unknown'

  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_after
  test "${group_id_before}" = "${group_id_after}" || fail "group recreated with no primary"
  get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_after
  test "${image_id_before}" = "${image_id_after}" || fail "image recreated with no primary"

  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}"

  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_after
  test "${group_id_before}" = "${group_id_after}" || fail "group recreated"
  get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_after
  test "${image_id_before}" = "${image_id_after}" || fail "image recreated"

  write_image "${secondary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  mirror_group_snapshot_and_wait_for_sync_complete "${primary_cluster}" "${secondary_cluster}" "${pool}"/"${group0}"

  # demote - neither site is primary
  mirror_group_demote "${secondary_cluster}" "${pool}/${group0}" 

  # Must wait for the demote snapshot to be synced before promoting the other site.
  # This is done by waiting for 'up+unknown' state to be reached.  
  # It is then safe to issue a promote.
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+unknown'

  # promote original primary again
  mirror_group_promote "${primary_cluster}" "${pool}/${group0}"

  # confirm that group and image are not recreated - resync flag was cleared
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_after
  test "${group_id_before}" = "${group_id_after}" || fail "group recreated"
  get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_after
  test "${image_id_before}" = "${image_id_after}" || fail "image recreated"

  # write some data, take a snapshot and wait for sync to complete
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group0}"
  
  # check that group and image ids still not changed on secondary
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_after
  test "${group_id_before}" = "${group_id_after}" || fail "group recreated"
  get_image_id2 "${secondary_cluster}" "${pool}/${image_prefix}0" image_id_after
  test "${image_id_before}" = "${image_id_after}" || fail "image recreated"

  group_remove "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  check_daemon_running "${secondary_cluster}"
}

# test resync scenarios
declare -a test_resync_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'no_change' 3)

test_resync_scenarios=1

test_resync()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local scenario=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  local group0=test-group0
  local snap0='snap_0'
  local snap1='snap_1'

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" "${image_count}"

  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap0}"
  compare_image_with_snapshot "${primary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'down+unknown' 0
  fi

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  local group_snap_id secondary_group_snap_id primary_group_snap_id
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}/${group0}" primary_group_snap_id
  echo "id = ${primary_group_snap_id}"

  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}" '-9'

  # promote secondary and change data on image
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'
  write_image "${secondary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  compare_image_with_snapshot_expect_difference "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  # demote secondary again
  mirror_group_demote "${secondary_cluster}" "${pool}/${group0}"
  
  # restart daemon and request a resync from primary
  start_mirrors "${secondary_cluster}"
  local group_id_before
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_before
  mirror_group_resync "${secondary_cluster}" "${pool}"/"${group0}"
  wait_for_group_id_changed  "${secondary_cluster}" "${pool}/${group0}" "${group_id_before}"

  # confirm that data on secondary again matches initial snapshot on primary
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  # Repeat the test this time changing the data on the primary too.

  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}" '-9'

  # promote secondary and change data on image
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'
  write_image "${secondary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  compare_image_with_snapshot_expect_difference "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}" group_snap_id

  # demote secondary again
  mirror_group_demote "${secondary_cluster}" "${pool}/${group0}"
  
  # restart daemon and request a resync from primary
  start_mirrors "${secondary_cluster}"
  get_id_from_group_info "${secondary_cluster}" "${pool}/${group0}" group_id_before
  mirror_group_resync "${secondary_cluster}" "${pool}/${group0}"
  wait_for_group_id_changed  "${secondary_cluster}" "${pool}/${group0}" "${group_id_before}"

  # confirm that data on secondary again matches latest snapshot on primary
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}"  "${secondary_cluster}" "${pool}"/"${group0}"
  wait_for_test_group_snap_present "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}" 1
  compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}0"

  # Repeat the test this time swapping the primary and secondary and resyncing back to the new secondary.
 
  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}" '-9'

  # promote secondary
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'
  write_image "${secondary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  mirror_group_snapshot "${secondary_cluster}" "${pool}/${group0}" group_snap_id

  # change data on old primary too
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096

  # demote old primary
  mirror_group_demote "${primary_cluster}" "${pool}/${group0}"

  # restart daemon and request a resync from primary
  start_mirrors "${primary_cluster}"
  
  get_id_from_group_info "${primary_cluster}" "${pool}/${group0}" group_id_before
  mirror_group_resync "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_id_changed  "${primary_cluster}" "${pool}/${group0}" "${group_id_before}"

  # confirm that data on secondary again matches latest snapshot on primary
  wait_for_group_synced "${secondary_cluster}" "${pool}"/"${group0}" "${primary_cluster}" "${pool}"/"${group0}"
  wait_for_test_group_snap_present "${primary_cluster}" "${pool}/${group0}" "${group_snap_id}" 1
  compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}0"

  mirror_group_disable "${secondary_cluster}" "${pool}/${group0}"
  group_remove "${secondary_cluster}" "${pool}/${group0}"

  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"

  images_remove "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"

  # reset: start the right daemons for the next test
  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  start_mirrors "${secondary_cluster}"
}

# test ODF failover/failback sequence
declare -a test_demote_snap_sync_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${image_prefix}" 'wait_before_promote' 3)

test_demote_snap_sync_scenarios=1

# 1. Create and mirror enable a group. Wait until it syncs to the secondary
# 2. Kill the rbd-mirror daemon on the secondary
# 3. Demote the primary group
# 4. Start the rbd-mirror daemon on the secondary
# 5. Check that the demote snap is synced to the secondary
# 6. promote the group on the secondary

test_demote_snap_sync()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local image_prefix=$1 ; shift
  local scenario=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  start_mirrors "${primary_cluster}"

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' 

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}" "${secondary_cluster}" "${pool}"/"${group0}" 

  # stop daemon on secondary (cluster1), demote original primary (cluster2) and restart daemon on secondary
  stop_mirrors "${secondary_cluster}" 
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'down+stopped'
  local group_snap_id
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}/${group0}" group_snap_id

  mirror_group_demote "${primary_cluster}" "${pool}/${group0}"

  local primary_demote_snap_id
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}/${group0}" primary_demote_snap_id

  test "${group_snap_id}" != "${primary_demote_snap_id}" ||  { fail "no new snapshot after demote"; return 1; }

  start_mirrors "${secondary_cluster}"

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}/${group0}" 'up+unknown'
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}/${group0}" 'up+unknown'

  local secondary_snap_id
  get_newest_group_snapshot_id "${secondary_cluster}" "${pool}/${group0}" secondary_snap_id

  test "${primary_demote_snap_id}" = "${secondary_snap_id}" ||  { fail "demote snapshot ${primary_demote_snap_id} not synced"; return 1; }

  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" 
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+stopped' 

  group_remove "${secondary_cluster}" "${pool}/${group0}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"

  images_remove "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  wait_for_no_keys "${primary_cluster}"
  stop_mirrors "${primary_cluster}"
  check_daemon_running "${secondary_cluster}"
}

check_for_no_keys()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local cluster pools pool key_count obj_count

  for cluster in ${primary_cluster} ${secondary_cluster}; do
    local pools
    pools=$(CEPH_ARGS='' ceph --cluster "${cluster}" osd pool ls  | grep -v "^\." | xargs)

    for pool in ${pools}; do
      # see if the rbd_mirror_leader object exists in the pool
      get_pool_obj_count "${cluster}" "${pool}" "rbd_mirror_leader" obj_count

      # if it does then check that there are no entries left in it
      if [ "$obj_count" -gt 0 ]; then
        count_omap_keys_with_filter "${cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
        test "${key_count}" = 0 || testlog "last test left keys" 
      fi
    done
  done    
}

wait_for_no_keys()
{
  local cluster=$1
  local pools pool key_count obj_count

  local pools
  pools=$(CEPH_ARGS='' ceph --cluster "${cluster}" osd pool ls  | grep -v "^\." | xargs)

  for pool in ${pools}; do
    # see if the rbd_mirror_leader object exists in the pool
    get_pool_obj_count "${cluster}" "${pool}" "rbd_mirror_leader" obj_count

    # if it does then wait until there are no entries left in it
    if [ "${obj_count}" -gt 0 ]; then
      count_omap_keys_with_filter "${cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
      if [ "${key_count}" -gt 0 ]; then
        for s in 0.1 1 2 4 8 8 8 8 8 8 8 8 16 16; do
          sleep "${s}"
          count_omap_keys_with_filter "${cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
          test "${key_count}" = 0 && break
        done
        test "${key_count}" = 0 || testlog "waiting did not clear leftover entries"
      fi
    fi
  done
}

run_test()
{
  local test_name=$1
  local test_scenario=$2

  declare -n test_parameters="$test_name"_"$test_scenario"

  local primary_cluster=cluster2
  local secondary_cluster=cluster1

  if [ "${RBD_MIRROR_PRINT_TESTS}" = 'true' ]; then
    echo "${test_name}" "${test_scenario}"
    return 0;
  fi

  # If the tmpdir and cluster conf file exist then reuse the existing cluster 
  # but stop the daemon on the primary if it was left running by the last test
  # and check that there are no unexpected objects left
  if [ -d "${RBD_MIRROR_TEMDIR}" ] && [ -f "${RBD_MIRROR_TEMDIR}"'/cluster1.conf' ]
  then
    export RBD_MIRROR_USE_EXISTING_CLUSTER=1

    # need to call this before checking the current state
    setup_tempdir

    if [ -n "${RBD_MIRROR_SAVE_CLI_OUTPUT}" ]; then 
      # Record the test name and scenario and clear any old output in the file
      echo "Test:${test_name} Scenario:${test_scenario}" > "${TEMPDIR}/${RBD_MIRROR_SAVE_CLI_OUTPUT}"
    fi

     # if the "mirror" pool doesn't exist then call setup to recreate all the required pools
    local pool_count
    get_pool_count "${primary_cluster}" 'mirror' pool_count
    if [ 0 = "${pool_count}" ]; then
      setup
    fi
  else
    setup  
  fi

  if [ -n "${RBD_MIRROR_GLOBAL_DEBUG}" ]; then
    run_admin_cmd "ceph --cluster ${primary_cluster} config set global ${RBD_MIRROR_GLOBAL_DEBUG}" 
    run_admin_cmd "ceph --cluster ${secondary_cluster} config set global ${RBD_MIRROR_GLOBAL_DEBUG}" 
  fi  

  # stop mirror daemon if it has been left running on the primary cluster
  stop_mirrors "${primary_cluster}"
  # restart mirror daemon if it has been stopped on the secondary cluster
  start_mirrors "${secondary_cluster}"

  testlog "TEST:$test_name scenario:$test_scenario parameters:" "${test_parameters[@]}"
  "$test_name" "${test_parameters[@]}"

  sleep 5

  # look at every pool on both clusters and check that there are no entries leftover in rbd_image_leader
  check_for_no_keys "${primary_cluster}" "${secondary_cluster}"
}

# exercise all scenarios that are defined for the specified test 
# optional second argument can be a list of scenarios to run
run_test_all_scenarios()
{
  local test_name=$1

  declare -n test_scenarios="$test_name"_scenarios

  # test_scenarios can either be a number or a sequence of numbers
  # in the former case it should be the number of the maximum valid scenario
  # in the later case it should be a sequence of valid scenario numbers
  # The later case is required when a non-contiguous sequnence of scenario numbers is valid
  local working_test_scenarios 
  if [[ $test_scenarios =~ ^[0-9]+$ ]]
  then
    working_test_scenarios=$(seq 1 "$test_scenarios")
  else
    working_test_scenarios=$test_scenarios
  fi

  if [ "$#" -gt 1 ]
  then
    working_test_scenarios=$2
  fi

  local loop
  for loop in $working_test_scenarios; do
    run_test "$test_name" "$loop"
  done
}

# exercise all scenarios for all tests
run_all_tests()
{
  run_test_all_scenarios test_empty_group
  run_test_all_scenarios test_empty_groups
  # This next test requires support for dynamic groups TODO
  # run_test_all_scenarios test_mirrored_group_remove_all_images
  # This next test also requires dynamic groups - TODO enable
  # run_test_all_scenarios test_mirrored_group_add_and_remove_images
  # This next also requires dynamic groups - TODO enable
  # run_test_all_scenarios test_create_group_mirror_then_add_images
  run_test_all_scenarios test_create_group_with_images_then_mirror
  # TODO: add the capabilty to have image from different pool in the mirror group
  run_test_all_scenarios test_images_different_pools
  run_test_all_scenarios test_create_group_with_images_then_mirror_with_regular_snapshots
  run_test_all_scenarios test_create_group_with_large_image
  run_test_all_scenarios test_create_group_with_multiple_images_do_io
  run_test_all_scenarios test_group_and_standalone_images_do_io
  run_test_all_scenarios test_stopped_daemon
  run_test_all_scenarios test_create_group_with_regular_snapshots_then_mirror
  run_test_all_scenarios test_image_move_group
  run_test_all_scenarios test_force_promote
  run_test_all_scenarios test_resync
  run_test_all_scenarios test_multiple_mirror_group_snapshot_whilst_stopped
  run_test_all_scenarios test_create_group_with_image_remove_then_repeat
  run_test_all_scenarios test_enable_disable_repeat
  run_test_all_scenarios test_empty_group_omap_keys
  # TODO: add the capabilty to have clone images support in the mirror group
  run_test_all_scenarios test_group_with_clone_image
  run_test_all_scenarios test_multiple_mirror_group_snapshot_unlink_time
  run_test_all_scenarios test_force_promote_delete_group
  run_test_all_scenarios test_create_group_stop_daemon_then_recreate
  run_test_all_scenarios test_enable_mirroring_when_duplicate_group_exists
  # TODO this next test is disabled as it fails with incorrect state/description in mirror group status - issue 50
  #run_test_all_scenarios test_enable_mirroring_when_duplicate_image_exists
  run_test_all_scenarios test_odf_failover_failback
  run_test_all_scenarios test_resync_marker
  run_test_all_scenarios test_force_promote_before_initial_sync
  run_test_all_scenarios test_image_snapshots_with_group
  run_test_all_scenarios test_group_rename
  run_test_all_scenarios test_demote_snap_sync
  # TODO this test is disabled - policing is missing for actions against groups on the secondary - not MVP
  #run_test_all_scenarios test_invalid_actions
  run_test_all_scenarios test_remote_namespace
  run_test_all_scenarios test_create_multiple_groups_do_io
}

if [ -n "${RBD_MIRROR_HIDE_BASH_DEBUGGING}" ]; then
  set -e
else  
  set -ex
fi  

# restore the arguments from the cli
set -- "${args[@]}"

for loop in $(seq 1 "${repeat_count}"); do
  if [ "${RBD_MIRROR_PRINT_TESTS}" != 'true' ]; then
    echo "run number ${loop} of ${repeat_count}"
  fi
  if [ -n "${test_name}" ]; then
    if [ -n "${scenario_number}" ]; then
      run_test_all_scenarios "${test_name}" "${scenario_number}"
    else
      run_test_all_scenarios "${test_name}"
    fi  
  else
    run_all_tests
  fi
done

exit 0
