#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -x

function wait_for_osdmap_manifest() {

  local what=${1:-"true"}

  local -a delays=($(get_timeout_delays $TIMEOUT .1))
  local -i loop=0

  for ((i=0; i < ${#delays[*]}; ++i)); do
    has_manifest=$(ceph report | jq 'has("osdmap_manifest")')
    if [[ "$has_manifest" == "$what" ]]; then
      return 0
    fi

    sleep ${delays[$i]}
  done

  echo "osdmap_manifest never outputted on report"
  ceph report
  return 1
}

function wait_for_trim() {

  local -i epoch=$1
  local -a delays=($(get_timeout_delays $TIMEOUT .1))
  local -i loop=0

  for ((i=0; i < ${#delays[*]}; ++i)); do
    fc=$(ceph report | jq '.osdmap_first_committed')
    if [[ $fc -eq $epoch ]]; then
      return 0
    fi
    sleep ${delays[$i]}
  done

  echo "never trimmed up to epoch $epoch"
  ceph report
  return 1
}

function test_osdmap() {

  local epoch=$1
  local ret=0

  tmp_map=$(mktemp)
  ceph osd getmap $epoch -o $tmp_map || return 1
  if ! osdmaptool --print $tmp_map | grep "epoch $epoch" ; then
    echo "ERROR: failed processing osdmap epoch $epoch"
    ret=1
  fi
  rm $tmp_map
  return $ret
}

function generate_osdmaps() {

  local -i num=$1

  cmds=( set unset )
  for ((i=0; i < num; ++i)); do
    ceph osd ${cmds[$((i%2))]} noup || return 1
  done
  return 0
}

function test_mon_osdmap_prune() {

  create_pool foo 32
  wait_for_clean || return 1

  ceph config set mon mon_debug_block_osdmap_trim true || return 1

  generate_osdmaps 500 || return 1

  report="$(ceph report)"
  fc=$(jq '.osdmap_first_committed' <<< $report)
  lc=$(jq '.osdmap_last_committed' <<< $report)

  [[ $((lc-fc)) -ge 500 ]] || return 1

  wait_for_osdmap_manifest || return 1

  manifest="$(ceph report | jq '.osdmap_manifest')"

  first_pinned=$(jq '.first_pinned' <<< $manifest)
  last_pinned=$(jq '.last_pinned' <<< $manifest)
  pinned_maps=( $(jq '.pinned_maps[]' <<< $manifest) )

  # validate pinned maps list
  [[ $first_pinned -eq ${pinned_maps[0]} ]] || return 1
  [[ $last_pinned -eq ${pinned_maps[-1]} ]] || return 1

  # validate pinned maps range
  [[ $first_pinned -lt $last_pinned ]] || return 1
  [[ $last_pinned -lt $lc ]] || return 1
  [[ $first_pinned -eq $fc ]] || return 1

  # ensure all the maps are available, and work as expected
  # this can take a while...

  for ((i=$first_pinned; i <= $last_pinned; ++i)); do
    test_osdmap $i || return 1
  done

  # update pinned maps state:
  #  the monitor may have pruned & pinned additional maps since we last
  #  assessed state, given it's an iterative process.
  #
  manifest="$(ceph report | jq '.osdmap_manifest')"
  first_pinned=$(jq '.first_pinned' <<< $manifest)
  last_pinned=$(jq '.last_pinned' <<< $manifest)
  pinned_maps=( $(jq '.pinned_maps[]' <<< $manifest) )

  # test trimming maps
  #
  # we're going to perform the following tests:
  #
  #  1. force trim to a pinned map
  #  2. force trim to a pinned map's previous epoch
  #  3. trim all maps except the last 200 or so.
  #

  # 1. force trim to a pinned map
  #
  [[ ${#pinned_maps[@]} -gt 10 ]] || return 1

  trim_to=${pinned_maps[1]}
  ceph config set mon mon_osd_force_trim_to $trim_to
  ceph config set mon mon_min_osdmap_epochs 100
  ceph config set mon paxos_service_trim_min 1
  ceph config set mon mon_debug_block_osdmap_trim false

  # generate an epoch so we get to trim maps
  ceph osd set noup
  ceph osd unset noup

  wait_for_trim $trim_to || return 1

  report="$(ceph report)"
  fc=$(jq '.osdmap_first_committed' <<< $report)
  [[ $fc -eq $trim_to ]] || return 1

  old_first_pinned=$first_pinned
  old_last_pinned=$last_pinned
  first_pinned=$(jq '.osdmap_manifest.first_pinned' <<< $report)
  last_pinned=$(jq '.osdmap_manifest.last_pinned' <<< $report)
  [[ $first_pinned -eq $trim_to ]] || return 1
  [[ $first_pinned -gt $old_first_pinned ]] || return 1
  [[ $last_pinned -gt $old_first_pinned ]] || return 1

  test_osdmap $trim_to || return 1
  test_osdmap $(( trim_to+1 )) || return 1

  pinned_maps=( $(jq '.osdmap_manifest.pinned_maps[]' <<< $report) )

  # 2. force trim to a pinned map's previous epoch
  #
  [[ ${#pinned_maps[@]} -gt 2 ]] || return 1
  trim_to=$(( ${pinned_maps[1]} - 1))
  ceph config set mon mon_osd_force_trim_to $trim_to

  # generate an epoch so we get to trim maps
  ceph osd set noup
  ceph osd unset noup

  wait_for_trim $trim_to || return 1

  report="$(ceph report)"
  fc=$(jq '.osdmap_first_committed' <<< $report)
  [[ $fc -eq $trim_to ]] || return 1

  old_first_pinned=$first_pinned
  old_last_pinned=$last_pinned
  first_pinned=$(jq '.osdmap_manifest.first_pinned' <<< $report)
  last_pinned=$(jq '.osdmap_manifest.last_pinned' <<< $report)
  pinned_maps=( $(jq '.osdmap_manifest.pinned_maps[]' <<< $report) )
  [[ $first_pinned -eq $trim_to ]] || return 1
  [[ ${pinned_maps[1]} -eq $(( trim_to+1)) ]] || return 1

  test_osdmap $first_pinned || return 1
  test_osdmap $(( first_pinned + 1 )) || return 1

  # 3. trim everything
  #
  ceph config set mon mon_osd_force_trim_to 0

  # generate an epoch so we get to trim maps
  ceph osd set noup
  ceph osd unset noup

  wait_for_osdmap_manifest "false" || return 1

  return 0
}

test_mon_osdmap_prune || exit 1

echo "OK"
