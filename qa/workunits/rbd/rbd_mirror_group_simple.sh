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
export RBD_MIRROR_MODE=snapshot

group0=test-group0
group1=test-group1
pool0=mirror
pool1=mirror_parent
image_prefix=test-image

image_multiplier=1
repeat_count=1
feature=0

while getopts ":f:m:r:s:t:" opt; do
  case $opt in
    f)
      feature=$OPTARG
      ;;
    m)
      image_multiplier=$OPTARG
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

echo "Repeat count: $repeat_count"
echo "Scenario number: $scenario_number"
echo "Test name: $test_name"
echo "Features: $RBD_IMAGE_FEATURES"

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
  wait_for_group_synced "${primary_cluster}" "${pool}/${group}"
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

declare -a test_enable_mirroring_when_duplicate_group_exists_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 5)

test_enable_mirroring_when_duplicate_group_exists_scenarios=1

test_enable_mirroring_when_duplicate_group_exists()
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

  group_create "${secondary_cluster}" "${pool}/${group}"
  images_create "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"
  group_images_add "${secondary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${image_count}"
  
  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" "${image_count}"
  check_daemon_running "${secondary_cluster}"
exit 0
# TODO finish this test - group should not be mirrored on secondary at this point. 
# peer status when looking at status on primary should be "up+stopped"
# could rename group on secondary without renaming images
# or could delete group on secondary.
# or rename group on primary?
  # ceph --daemon mirror group status groupName
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${image_count}"
  check_daemon_running "${secondary_cluster}"

  # rbd mirror group status groupName
  #sleep 10
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${image_count}"
  wait_for_group_synced "${primary_cluster}" "${pool}/${group}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group}" 'down+unknown' 0
  fi

  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
  check_daemon_running "${secondary_cluster}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
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
    results+=("image count:$image_count enable time:"${times[0]}" sync_time:"${times[1]})
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

# TODO enable scenarios 2 and 3 when they pass
test_create_group_stop_daemon_then_recreate_scenarios=3

test_create_group_stop_daemon_then_recreate()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local scenario=$1 ; shift

  local pool="${pool0}"
  local group="${group0}"

  testlog "test_create_group_stop_daemon_then_recreate"

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

    # remove and recreate group - expect 2 keys due to bug TODO
    group_remove "${primary_cluster}" "${pool}/${group}"
    group_create "${primary_cluster}" "${pool}/${group}"
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0

    count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
    test "${key_count}" = 2 || fail "unexpected key count:${key_count}"
  elif  [ "${scenario}" = 'stop_restart_after_recreate' ]; then
    echo "stopping daemon on secondary"
    stop_mirrors "${secondary_cluster}"

    group_remove "${primary_cluster}" "${pool}/${group}"
    group_create "${primary_cluster}" "${pool}/${group}"
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
    
    echo "restarting daemon on secondary"
    start_mirrors "${secondary_cluster}"

    # TODO fails on next step - group is not present on secondary (restarting the daemon again will cause it to appear)
    wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" 0
    wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" 0
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' 0

    count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
    test "${key_count}" = 2 || fail "unexpected key count:${key_count}"
  fi

  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

# Not sure what the expected count is here - have asked Nithya
#  count_omap_keys_with_filter "${secondary_cluster}" "${pool}" "rbd_mirror_leader" "image_map" key_count
#  test "${key_count}" = 2 || fail "unexpected key count:${key_count}"

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
declare -a test_remote_namespace_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" 5)

test_remote_namespace_scenarios=1

test_remote_namespace()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

  # configure primary namespace mirrored to secondary namespace with a different name 
  local primary_namespace
  primary_namespace='ns3'
  local secondary_namespace
  secondary_namespace='ns3_remote'

  local primary_pool_spec
  local secondary_pool_spec
  primary_pool_spec="${pool}/${primary_namespace}"
  secondary_pool_spec="${pool}/${secondary_namespace}"
  
  run_cmd "rbd --cluster ${primary_cluster} namespace create ${pool}/${primary_namespace}"
  run_cmd "rbd --cluster ${secondary_cluster} namespace create ${pool}/${secondary_namespace}"

  run_cmd "rbd --cluster ${primary_cluster} mirror pool enable ${pool}/${primary_namespace} image --remote-namespace ${secondary_namespace}"
  run_cmd "rbd --cluster ${secondary_cluster} mirror pool enable ${pool}/${secondary_namespace} image --remote-namespace ${primary_namespace}"

  group_create "${primary_cluster}" "${primary_pool_spec}/${group}"
  mirror_group_enable "${primary_cluster}" "${primary_pool_spec}/${group}"
  wait_for_group_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}" 0
  wait_for_group_replay_started "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 0
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 'up+replaying' 0

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}"/"${group}" 'down+unknown' 0
  fi

  try_cmd "rbd --cluster ${secondary_cluster} group snap list ${secondary_pool_spec}/${group}" || :
  try_cmd "rbd --cluster ${primary_cluster} group snap list ${primary_pool_spec}/${group}" || :

  mirror_group_disable "${primary_cluster}" "${primary_pool_spec}/${group}"

  try_cmd "rbd --cluster ${secondary_cluster} group snap list ${secondary_pool_spec}/${group}" || :
  try_cmd "rbd --cluster ${primary_cluster} group snap list ${primary_pool_spec}/${group}" || :

  group_remove "${primary_cluster}" "${primary_pool_spec}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${primary_pool_spec}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}"
  check_daemon_running "${secondary_cluster}"

  # repeat the test - this time with some images
  group_create "${primary_cluster}" "${primary_pool_spec}/${group}"
  images_create "${primary_cluster}" "${primary_pool_spec}/${image_prefix}" "${image_count}"
  group_images_add "${primary_cluster}" "${primary_pool_spec}/${group}" "${primary_pool_spec}/${image_prefix}" "${image_count}"

  mirror_group_enable "${primary_cluster}" "${primary_pool_spec}/${group}"
  wait_for_group_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}" "${image_count}"
  wait_for_group_replay_started "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}"/"${group}" 'down+unknown' 0
  fi

  # try a manual snapshot and check that it syncs
  write_image "${primary_cluster}" "${primary_pool_spec}" "${image_prefix}0" 10 4096
  local group_snap_id
  mirror_group_snapshot "${primary_cluster}" "${primary_pool_spec}/${group}" group_snap_id
  wait_for_group_snap_present "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}"
  wait_for_group_snap_sync_complete "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}"

  # Check all images in the group and confirms that they are synced
  test_group_synced_image_status "${secondary_cluster}" "${secondary_pool_spec}/${group}" "${group_snap_id}" "${image_count}"

  # try demote, promote and resync
  mirror_group_demote "${primary_cluster}" "${primary_pool_spec}/${group}"
  test_fields_in_group_info "${primary_cluster}" "${primary_pool_spec}/${group}" 'snapshot' 'enabled' 'false'
  wait_for_group_replay_stopped "${secondary_cluster}" "${secondary_pool_spec}/${group}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}/${group}" 'up+stopped' 0
  wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}/${group}" 'down+unknown' 0
  mirror_group_promote "${secondary_cluster}" "${secondary_pool_spec}/${group}"
  test_fields_in_group_info "${secondary_cluster}" "${secondary_pool_spec}/${group}" 'snapshot' 'enabled' 'true'

  write_image "${secondary_cluster}" "${secondary_pool_spec}" "${image_prefix}0" 10 4096

  mirror_group_demote "${secondary_cluster}" "${secondary_pool_spec}/${group}"
  test_fields_in_group_info "${secondary_cluster}" "${secondary_pool_spec}/${group}" 'snapshot' 'enabled' 'false'
  mirror_group_promote "${primary_cluster}" "${primary_pool_spec}/${group}"
  test_fields_in_group_info "${primary_cluster}" "${primary_pool_spec}/${group}" 'snapshot' 'enabled' 'true'

  mirror_group_resync "${secondary_cluster}" "${secondary_pool_spec}/${group}"

  wait_for_group_synced "${primary_cluster}" "${primary_pool_spec}/${group}"

  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${secondary_pool_spec}"/"${group}" 'up+replaying' "${image_count}"
  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${primary_pool_spec}"/"${group}" 'down+unknown' 0
  fi

  group_remove "${primary_cluster}" "${primary_pool_spec}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${primary_pool_spec}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${secondary_pool_spec}" "${group}"

  images_remove "${primary_cluster}" "${primary_pool_spec}/${image_prefix}" "${image_count}"
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

  # next command fails with the following message
  #  2025-01-30T16:34:25.359+0000 7fc1a79bfb40 -1 librbd::api::Mirror: image_enable: mirroring is not enabled for the parent
  #  2025-01-30T16:34:25.359+0000 7fc1a79bfb40 -1 librbd::api::Mirror: group_enable: failed enabling image: child_image: (22) Invalid argument
  mirror_group_enable_try "${primary_cluster}" "${pool}/${group}" || :
  test 0 = "$(grep -c "interrupted" "$CMD_STDERR")" || fail "unexpected output"

  # next command appears to succeed
  mirror_group_disable "${primary_cluster}" "${pool}/${group}"

  # another attempt at enable fails with a strange message 
  #  2025-01-30T17:17:34.421+0000 7f9ad0d74b40 -1 librbd::api::Mirror: group_enable: enabling mirroring for group test-group0 either in progress or was interrupted
  mirror_group_enable_try "${primary_cluster}" "${pool}/${group}" || :
  test 0 = "$(grep -c "interrupted" "$CMD_STDERR")" || fail "unexpected output"

  # tidy up
  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"

  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  image_remove "${primary_cluster}" "${pool}/other_image0"
  image_remove "${primary_cluster}" "${pool}/other_image1"
  image_remove "${primary_cluster}" "${pool}/child_image"
}

test_from_nithya_that_will_stop_working_when_api_changes()
{
[root@server1 build]# rbd-a group create data/grp1
[root@server1 build]# rbd-a group image add data/grp1 data/img-1
[root@server1 build]# rbd-a group image add data/grp1 data/img-2
[root@server1 build]# rbd-a group image add data/grp1 data/img-3
[root@server1 build]# rbd-a mirror group enable data/grp1
[root@server1 build]# rbd-a mirror image demote data/img-2
[root@server1 build]# rbd-a mirror group snapshot data/grp1
[root@server1 build]# rbd-a snap ls --all data/img-3
[root@server1 build]# rbd-a group snap ls data/grp1
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

  if [ -n "${RBD_MIRROR_NEW_IMPLICIT_BEHAVIOUR}" ]; then
    # check secondary cluster sees 0 images
    wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool0}"/"${group}" 'up+replaying' 0
    mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool0}"/"${group}"
  fi

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

# create regular group snapshots and test replay
declare -a test_create_group_with_images_then_mirror_with_regular_snapshots_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 5)

test_create_group_with_images_then_mirror_with_regular_snapshots_scenarios=1

test_create_group_with_images_then_mirror_with_regular_snapshots()
{
  local primary_cluster=$1 ; shift
  local secondary_cluster=$1 ; shift
  local pool=$1 ; shift
  local group=$1 ; shift
  local image_prefix=$1 ; shift
  local image_count=$(($1*"${image_multiplier}")) ; shift

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
  # snap is currently copied to secondary cluster, where it remains in the "incomplete" state, but this is maybe incorrect - see slack thread TODO
  # - should not be copied until mirrored.
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

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${image_count}"
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

  wait_for_group_synced "${primary_cluster}" "${pool}/${group}"
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"

  group_snap_remove "${primary_cluster}" "${pool}/${group}" "${snap}"
  check_group_snap_doesnt_exist "${primary_cluster}" "${pool}/${group}" "${snap}"
  # this next extra mirror_group_snapshot should not be needed - waiting for fix TODO
  mirror_group_snapshot "${primary_cluster}" "${pool}/${group}"
  mirror_group_snapshot_and_wait_for_sync_complete "${secondary_cluster}" "${primary_cluster}" "${pool}"/"${group}"
  check_group_snap_doesnt_exist "${secondary_cluster}" "${pool}/${group}" "${snap}"

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
  # this next extra mirror_group_snapshot should not be needed - waiting for fix TODO
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
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group}"

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
  # TODO often fails on next step - group is not present on secondary.
  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" $(("${group_image_count}"+1))
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' $(("${group_image_count}"+1))
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group}"

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
#    wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"
    group_image_remove "${primary_cluster}" "${pool}/${group}" "${pool}/${image_name}" 
    mirror_group_enable "${primary_cluster}" "${pool}/${group}"
   # wait_for_group_present "${secondary_cluster}" "${pool}" "${group}" $(("${group_image_count}"))
  fi

  get_newest_group_snapshot_id "${primary_cluster}" "${pool}"/"${group}" primary_group_snap_id
  test "${primary_group_snap_id}" != "${secondary_group_snap_id}" ||  { fail "matched ids"; return 1; }

  echo "starting daemon"
  start_mirrors "${secondary_cluster}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group}" "${group_image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group}" 'up+replaying' "${group_image_count}"
  # TODO next command fails because rbd group snap list command fails with -2
  # though group does exist on secondary
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group}"

  get_newest_group_snapshot_id "${secondary_cluster}" "${pool}"/"${group}" secondary_group_snap_id
  test "${primary_group_snap_id}" = "${secondary_group_snap_id}" ||  { fail "mismatched ids"; return 1; }

  # TODO test more actions whilst daemon is stopped
  # add image, take snapshot, remove image, take snapshot, restart
  # disable mirroring 
  
  mirror_group_disable "${primary_cluster}" "${pool}/${group}"
  group_remove "${primary_cluster}" "${pool}/${group}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group}"
  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group}"

  images_remove "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  image_remove "${primary_cluster}" "${pool}/${image_name}"
}

# multiple images in group and standalone images too with io
declare -a test_group_and_standalone_images_do_io_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'false')
declare -a test_group_and_standalone_images_do_io_2=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" "${group0}" "${image_prefix}" 'true')

# TODO scenario 2 fails currently - it is waiting for the groups to be listed in the pool status
test_group_and_standalone_images_do_io_scenarios=1 

test_group_and_standalone_images_do_io()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  local group=$4
  local image_prefix=$5
  local test_pool_status=$6

  local standalone_image_prefix=standalone-image
  local standalone_image_count=4
  local group_image_count=2

  images_create "${primary_cluster}" "${pool}/${standalone_image_prefix}" "${standalone_image_count}"

  group_create "${primary_cluster}" "${pool}/${group}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" "${group_image_count}"
  group_images_add "${primary_cluster}" "${pool}/${group}" "${pool}/${image_prefix}" "${group_image_count}"

  if [ 'true' = "${test_pool_status}" ]; then
    local fields=(//status/images/image/name //status/groups/group/name)
    local pool_fields_count_arr=()
    count_fields_in_mirror_pool_status "${primary_cluster}" "${pool}" pool_fields_count_arr "${fields[@]}"
    # Check count of images and groups in the command output
    test 0 = "${pool_fields_count_arr[0]}" || fail "unexpected count of images : ${pool_fields_count_arr[0]}"
    test 0 = "${pool_fields_count_arr[1]}" || fail "unexpected count of groups : ${pool_fields_count_arr[1]}"
  fi

  mirror_group_enable "${primary_cluster}" "${pool}/${group}"

  if [ 'true' = "${test_pool_status}" ]; then
    local fields=(//status/images/image/name //status/groups/group/name)
    pool_fields_count_arr=()
    count_fields_in_mirror_pool_status "${primary_cluster}" "${pool}" pool_fields_count_arr "${fields[@]}"
    # Check count of images and groups in the command output
    test $((${group_image_count})) = "${pool_fields_count_arr[0]}" || fail "unexpected count of images : ${pool_fields_count_arr[0]}"
    test 1 = "${pool_fields_count_arr[1]}" || fail "unexpected count of groups : ${pool_fields_count_arr[1]}"
  fi

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

  if [ 'true' = "${test_pool_status}" ]; then
    local fields=(//status/images/image/name //status/groups/group/name)
    pool_fields_count_arr=()
    count_fields_in_mirror_pool_status "${primary_cluster}" "${pool}" pool_fields_count_arr "${fields[@]}"
    # Check count of images and groups in the command output
    test $((${standalone_image_count}+${group_image_count})) = "${pool_fields_count_arr[0]}" || fail "unexpected count of images : ${pool_fields_count_arr[0]}"
    test 1 = "${pool_fields_count_arr[1]}" || fail "unexpected count of groups : ${pool_fields_count_arr[1]}"
  fi

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
  for instance in $(seq 0 ${LAST_MIRROR_INSTANCE}); do
    local result
    query_replayer_assignment "${secondary_cluster}" "${instance}" result
    group_arr+=("${result[0]}")
    image_arr+=("${result[1]}")
    group_count_arr+=("${result[2]}")
    image_count_arr+=("${result[3]}")
  done
  for instance in $(seq 0 ${LAST_MIRROR_INSTANCE}); do
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

  # set up a chain of moves TODO

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

# TODO scenarios 2, 3 and 4 are currently failing
test_force_promote_scenarios=3

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

  group_create "${primary_cluster}" "${pool}/${group0}"
  images_create "${primary_cluster}" "${pool}/${image_prefix}" $(("${image_count}"-1))
  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  group_images_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}" $(("${image_count}"-1))
  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap0}"
  compare_image_with_snapshot "${primary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  big_image=test-image-big
  image_create "${primary_cluster}" "${pool}/${big_image}" 4G
  group_image_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${big_image}"

  mirror_group_enable "${primary_cluster}" "${pool}/${group0}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  wait_for_group_replay_started "${secondary_cluster}" "${pool}"/"${group0}" "${image_count}"
  wait_for_group_status_in_pool_dir "${secondary_cluster}" "${pool}"/"${group0}" 'up+replaying' "${image_count}"

  if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'down+unknown' 0
  fi

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  write_image "${primary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  create_snapshot "${primary_cluster}" "${pool}" "${image_prefix}0" "${snap1}"

  # make some changes to the big image so that the next sync will take a long time
  write_image "${primary_cluster}" "${pool}" "${big_image}" 1024 4194304

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
  elif [ "${scenario}" = 'no_change' ]; then
    mirror_group_snapshot "${primary_cluster}" "${pool}/${group0}"
  fi

  # TODO add the following test
: '
  # This test removes and recreates an image - it fails currently as the request to list the group snaps on the secondary fails
  group_image_remove "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}0"
  image_remove "${primary_cluster}" "${pool}/${image_prefix}0" 
  image_create "${primary_cluster}" "${pool}/${image_prefix}0" maybe different size?
  group_image_add "${primary_cluster}" "${pool}/${group0}" "${pool}/${image_prefix}0"
'

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
  fi

  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}"

  # check that latest snap is incomplete
    ## this fails in the delete case as follows:
    ##CEPH_ARGS='--id mirror' rbd --cluster cluster1 group snap list mirror/group_0
    ##ERR: rc= 2
  test_group_snap_sync_incomplete "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}" 

  # TODO remove - just capturing debug info
  try_cmd "rbd --cluster ${primary_cluster} group snap list ${pool}/${group0}" || :
  try_cmd "rbd --cluster ${secondary_cluster} group snap list ${pool}/${group0}" || :

  # force promote the group on the secondary - should rollback to the last complete snapshot
  local old_primary_cluster
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'

  # TODO remove - just capturing debug info
  try_cmd "rbd --cluster ${primary_cluster} group snap list ${pool}/${group0}" || :
  try_cmd "rbd --cluster ${secondary_cluster} group snap list ${pool}/${group0}" || :

  old_primary_cluster="${primary_cluster}"
  primary_cluster="${secondary_cluster}"

  mirror_group_demote "${old_primary_cluster}" "${pool}/${group0}"
  secondary_cluster="${old_primary_cluster}"

  # TODO remove - just capturing debug info
  try_cmd "rbd --cluster ${secondary_cluster} group snap list ${pool}/${group0}" || :
  try_cmd "rbd --cluster ${primary_cluster} group snap list ${pool}/${group0}" || :

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

  mirror_group_resync ${secondary_cluster} ${pool}/${group0}

  start_mirrors "${secondary_cluster}"
  sleep 5
# TODO check that data can be copied back to original primary cluster
# next line fails because latest snapshot on primary is never copied back to secondary
# finish off the resync function
# check that tidy up steps below work
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}"

  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

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

  # Note: we altered primary and secondary cluster, so reset.
  old_primary_cluster="${primary_cluster}"
  primary_cluster="${secondary_cluster}"
  secondary_cluster="${old_primary_cluster}"
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
    wait_for_group_status_in_pool_dir "${primary_cluster}" "${pool}"/"${group0}" 'up+stopped' 0
  fi

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}"

  # force promote the group on the secondary 
  # TODO disable mirror daemon here - see slack thread https://ibm-systems-storage.slack.com/archives/C07J9Q2E268/p1739856204809159
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'
  wait_for_group_replay_stopped ${secondary_cluster} ${pool}/${group0}
  wait_for_group_replay_stopped ${primary_cluster} ${pool}/${group0}
  wait_for_group_status_in_pool_dir ${secondary_cluster} ${pool}/${group0} 'up+stopped' 0
  wait_for_group_status_in_pool_dir ${primary_cluster} ${pool}/${group0} 'up+stopped' 0

  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  mirror_group_disable "${secondary_cluster}" "${pool}/${group0}"

  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"

  # group still exists on original primary
  wait_for_group_status_in_pool_dir ${primary_cluster} ${pool}/${group0} 'up+stopped' 0

  group_image_remove ${secondary_cluster} ${pool}/${group0} ${pool}/${image_prefix}0

  wait_for_group_present "${primary_cluster}" "${pool}" "${group0}" "${image_count}"
  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" $(("${image_count}"-1))
  test_fields_in_group_info ${primary_cluster} ${pool}/${group0} 'snapshot' 'enabled' 'true'
  wait_for_group_status_in_pool_dir ${primary_cluster} ${pool}/${group0} 'up+stopped' 0

  mirror_group_enable "${secondary_cluster}" "${pool}/${group0}"
  test_fields_in_group_info ${primary_cluster} ${pool}/${group0} 'snapshot' 'enabled' 'true'
  test_fields_in_group_info ${secondary_cluster} ${pool}/${group0} 'snapshot' 'enabled' 'true'

  wait_for_group_status_in_pool_dir ${primary_cluster} ${pool}/${group0} 'up+stopped' 0
  wait_for_group_status_in_pool_dir ${secondary_cluster} ${pool}/${group0} 'up+error' 0

  # TODO - test normally fails on next line with missing images
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

  stop_mirrors "${primary_cluster}"
}

# test force unlink time
declare -a test_multiple_user_snapshot_time_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}")

test_multiple_user_snapshot_time_scenarios=1

test_multiple_user_snapshot_time()
{
  local primary_cluster=$1
  local secondary_cluster=$2
  local pool=$3
  
  local image_count
  local image_counts=(2 16)
  local results=()
  local time

  for image_count in "${image_counts[@]}"; do
    test_multiple_user_snapshot_whilst_stopped "${primary_cluster}" "${secondary_cluster}" "${pool}" "${image_count}" time
    results+=(${time})
  done

  for i in $(seq 0 "${#results[@]}"); do
    echo -e "${RED}image count:"${image_counts[$i]}" snapshot time:"${results[$i]}"${NO_COLOUR}"
  done

  if [ ${results[1]} -gt $((${results[0]}+3)) ]; then
    fail "Snapshot time isn't independent of the group image count" 
  fi
}

# test force promote scenarios
declare -a test_multiple_user_snapshot_whilst_stopped_1=("${CLUSTER2}" "${CLUSTER1}" "${pool0}" 5)

test_multiple_user_snapshot_whilst_stopped_scenarios=1

test_multiple_user_snapshot_whilst_stopped()
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

  echo "stopping daemon on secondary"
  stop_mirrors "${secondary_cluster}"
  # TODO starting the daemon on the primary seem to cause a problem with image deletion - Nithya investigating (see slack thread)
  #echo "starting daemon on primary"
  #start_mirrors "${primary_cluster}"

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

  local count
  get_group_snap_count "${primary_cluster}" "${pool}"/"${group0}" '*' count
  test "${count}" -gt 3 || { fail "snap count = ${count}"; return 1; }

  get_group_snap_count "${secondary_cluster}" "${pool}"/"${group0}" '*' count
  test "${count}" -eq 1 || { fail "snap count = ${count}"; return 1; }

  wait_for_group_present "${secondary_cluster}" "${pool}" "${group0}" "${image_count}"
  echo "starting daemon on secondary"
  start_mirrors "${secondary_cluster}"

  wait_for_group_synced "${primary_cluster}" "${pool}/${group0}"
  max_image=$((image_count-1))
  for i in $(seq 0 ${max_image}); do  
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

  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  local group_snap_id secondary_group_snap_id primary_group_snap_id
  get_newest_group_snapshot_id "${primary_cluster}" "${pool}/${group0}" primary_group_snap_id
  echo "id = ${primary_group_snap_id}"

  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}"

  # promote secondary and change data on image
  mirror_group_promote "${secondary_cluster}" "${pool}/${group0}" '--force'
  write_image "${secondary_cluster}" "${pool}" "${image_prefix}0" 10 4096
  compare_image_with_snapshot_expect_difference "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  # demote secondary again
  mirror_group_demote "${secondary_cluster}" "${pool}/${group0}"
  
  # restart daemon and request a resync from primary
  start_mirrors "${secondary_cluster}"
  mirror_group_resync ${secondary_cluster} ${pool}/${group0}

  # confirm that data on secondary again matches initial snapshot on primary
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}"
  compare_image_with_snapshot "${secondary_cluster}" "${pool}/${image_prefix}0" "${primary_cluster}" "${pool}/${image_prefix}0@${snap0}"

  # Repeat the test this time changing the data on the primary too.

  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}"

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
  mirror_group_resync ${secondary_cluster} ${pool}/${group0}

  # confirm that data on secondary again matches latest snapshot on primary
  wait_for_group_synced "${primary_cluster}" "${pool}"/"${group0}"
  wait_for_test_group_snap_present "${secondary_cluster}" "${pool}/${group0}" "${group_snap_id}" 1
  compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}0"

  # Repeat the test this time swapping the primary and secondary and resyncing back to the new secondary.
 
  # stop the daemon to prevent further syncing of snapshots
  stop_mirrors "${secondary_cluster}"

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
  mirror_group_resync ${primary_cluster} ${pool}/${group0}

  # confirm that data on secondary again matches latest snapshot on primary
  wait_for_group_synced "${secondary_cluster}" "${pool}"/"${group0}"
  wait_for_test_group_snap_present "${primary_cluster}" "${pool}/${group0}" "${group_snap_id}" 1
  compare_images "${primary_cluster}" "${secondary_cluster}" "${pool}" "${pool}" "${image_prefix}0"

  mirror_group_disable "${secondary_cluster}" "${pool}/${group0}"
  group_remove "${secondary_cluster}" "${pool}/${group0}"

  wait_for_group_not_present "${secondary_cluster}" "${pool}" "${group0}"
  wait_for_group_not_present "${primary_cluster}" "${pool}" "${group0}"

  images_remove "${secondary_cluster}" "${pool}/${image_prefix}" "${image_count}"

  # reset: start the right daemons for the next test
  stop_mirrors "${primary_cluster}"
  start_mirrors "${secondary_cluster}"
}

run_test()
{
  local test_name=$1
  local test_scenario=$2

  declare -n test_parameters="$test_name"_"$test_scenario"

  testlog "TEST:$test_name scenario:$test_scenario parameters:" "${test_parameters[@]}"
  "$test_name" "${test_parameters[@]}"
}

# exercise all scenarios that are defined for the specified test 
run_test_all_scenarios()
{
  local test_name=$1

  declare -n test_scenario_count="$test_name"_scenarios

  local loop
  for loop in $(seq 1 $test_scenario_count); do
    run_test $test_name $loop
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
  # next test is not MVP - TODO
  # run_test_all_scenarios test_images_different_pools
  run_test_all_scenarios test_create_group_with_images_then_mirror_with_regular_snapshots
  run_test_all_scenarios test_create_group_with_large_image
  run_test_all_scenarios test_create_group_with_multiple_images_do_io
  run_test_all_scenarios test_group_and_standalone_images_do_io
  run_test_all_scenarios test_create_multiple_groups_do_io
  run_test_all_scenarios test_stopped_daemon
  run_test_all_scenarios test_create_group_with_regular_snapshots_then_mirror
  run_test_all_scenarios test_image_move_group
  run_test_all_scenarios test_force_promote
  run_test_all_scenarios test_resync
  run_test_all_scenarios test_remote_namespace
  run_test_all_scenarios test_multiple_user_snapshot_whilst_stopped
  run_test_all_scenarios test_create_group_with_image_remove_then_repeat
  run_test_all_scenarios test_empty_group_omap_keys
  #run_test_all_scenarios test_group_with_clone_image
  run_test_all_scenarios test_multiple_user_snapshot_time
  run_test_all_scenarios test_force_promote_delete_group
  run_test_all_scenarios test_create_group_stop_daemon_then_recreate
  #run_test_all_scenarios test_enable_mirroring_when_duplicate_group_exists

  #FIXME: This test leaves residual groups on secondary moving it to the end.
  run_test_all_scenarios test_enable_disable_repeat
}

if [ -n "${RBD_MIRROR_SHOW_CLI_CMD}" ]; then
  set -e
else  
  set -ex
fi  

# If the tmpdir and cluster conf file exist then reuse the existing cluster
if [ -d "${RBD_MIRROR_TEMDIR}" ] && [ -f "${RBD_MIRROR_TEMDIR}"'/cluster1.conf' ]
then
  export RBD_MIRROR_USE_EXISTING_CLUSTER=1
fi

setup

# see if we need to (re)start rbd-mirror deamon
pid=$(cat "$(daemon_pid_file "${CLUSTER1}")" 2>/dev/null) || :
if [ -z "${pid}" ]
then
    start_mirrors "${CLUSTER1}"
fi
check_daemon_running "${CLUSTER1}"

# restore the arguments from the cli
set -- "${args[@]}"

for loop in $(seq 1 "${repeat_count}"); do
  echo "run number ${loop} of ${repeat_count}"
  if [ -n "${test_name}" ]; then
    if [ -n "${scenario_number}" ]; then
      run_test "${test_name}" "${scenario_number}"
    else
      run_test_all_scenarios "${test_name}"
    fi  
  else
    run_all_tests
  fi
done

exit 0
