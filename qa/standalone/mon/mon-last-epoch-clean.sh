#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh


function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7302" # git grep '\<7105\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


function check_lec_equals_pools() {

  local pool_id=$1

  report=$(ceph report)
  lec=$(echo $report | \
    jq '.osdmap_clean_epochs.min_last_epoch_clean')

  if [[ -z "$pool_id" ]]; then
    pools=($(echo $report | \
      jq \
      ".osdmap_clean_epochs.last_epoch_clean.per_pool[] |" \
      " select(.floor == $lec) | .poolid"))

    [[ ${#pools[*]} -eq 2 ]] || ( echo $report ; return 1 )
  else
    floor=($(echo $report | \
      jq \
      ".osdmap_clean_epochs.last_epoch_clean.per_pool[] |" \
      " select(.poolid == $pool_id) | .floor"))

    [[ $lec -eq $floor ]] || ( echo $report ; return 1 )
  fi
  return 0
}

function check_lec_lower_than_pool() {

  local pool_id=$1
  [[ -z "$pool_id" ]] && ( echo "expected pool_id as parameter" ; exit 1 )

  report=$(ceph report)
  lec=$(echo $report | \
    jq '.osdmap_clean_epochs.min_last_epoch_clean')

  floor=($(echo $report | \
    jq \
    ".osdmap_clean_epochs.last_epoch_clean.per_pool[] |" \
    " select(.poolid == $pool_id) | .floor"))

  [[ $lec -lt $floor ]] || ( echo $report ; return 1 )
  return 0
}

function check_floor_pool_greater_than_pool() {

  local pool_a=$1
  local pool_b=$1
  [[ -z "$pool_a" ]] && ( echo "expected id as first parameter" ; exit 1 )
  [[ -z "$pool_b" ]] && ( echo "expected id as second parameter" ; exit 1 )

  report=$(ceph report)

  floor_a=($(echo $report | \
    jq \
    ".osdmap_clean_epochs.last_epoch_clean.per_pool[] |" \
    " select(.poolid == $pool_a) | .floor"))

  floor_b=($(echo $report | \
    jq \
    ".osdmap_clean_epochs.last_epoch_clean.per_pool[] |" \
    " select(.poolid == $pool_b) | .floor"))

  [[ $floor_a -gt $floor_b ]] || ( echo $report ; return 1 )
  return 0
}

function check_lec_honours_osd() {

  local osd=$1

  report=$(ceph report)
  lec=$(echo $report | \
    jq '.osdmap_clean_epochs.min_last_epoch_clean')

  if [[ -z "$osd" ]]; then
    osds=($(echo $report | \
      jq \
      ".osdmap_clean_epochs.osd_epochs[] |" \
      " select(.epoch >= $lec) | .id"))

    [[ ${#osds[*]} -eq 3 ]] || ( echo $report ; return 1 )
  else
    epoch=($(echo $report | \
      jq \
      ".osdmap_clean_epochs.osd_epochs[] |" \
      " select(.id == $id) | .epoch"))
    [[ ${#epoch[*]} -eq 1 ]] || ( echo $report ; return 1 )
    [[ ${epoch[0]} -ge $lec ]] || ( echo $report ; return 1 )
  fi

  return 0
}

function validate_fc() {
  report=$(ceph report)
  lec=$(echo $report | \
    jq '.osdmap_clean_epochs.min_last_epoch_clean')
  osdm_fc=$(echo $report | \
    jq '.osdmap_first_committed')

  [[ $lec -eq $osdm_fc ]] || ( echo $report ; return 1 )
  return 0
}

function get_fc_lc_diff() {
  report=$(ceph report)
  osdm_fc=$(echo $report | \
    jq '.osdmap_first_committed')
  osdm_lc=$(echo $report | \
    jq '.osdmap_last_committed')

  echo $((osdm_lc - osdm_fc))
}

function get_pool_id() {

  local pn=$1
  [[ -z "$pn" ]] && ( echo "expected pool name as argument" ; exit 1 )

  report=$(ceph report)
  pool_id=$(echo $report | \
    jq ".osdmap.pools[] | select(.pool_name == \"$pn\") | .pool")

  [[ $pool_id -ge 0 ]] || \
    ( echo "unexpected pool id for pool \'$pn\': $pool_id" ; return -1 )

  echo $pool_id
  return 0
}

function wait_for_total_num_maps() {
  # rip wait_for_health, becaue it's easier than deduplicating the code
  local -a delays=($(get_timeout_delays $TIMEOUT .1))
  local -i loop=0
  local -i v_diff=$1

  while [[ $(get_fc_lc_diff) -gt $v_diff ]]; do
    if (( $loop >= ${#delays[*]} )) ; then
      echo "maps were not trimmed"
      return 1
    fi
    sleep ${delays[$loop]}
    loop+=1
  done 
}

function TEST_mon_last_clean_epoch() {

  local dir=$1

  run_mon $dir a || return 1
  run_mgr $dir x || return 1
  run_osd $dir 0 || return 1
  run_osd $dir 1 || return 1
  run_osd $dir 2 || return 1
  osd_pid=$(cat $dir/osd.2.pid)

  sleep 5

  ceph tell 'osd.*' injectargs '--osd-beacon-report-interval 10' || exit 1
  ceph tell 'mon.*' injectargs \
    '--mon-min-osdmap-epochs 2 --paxos-service-trim-min 1' || exit 1

  create_pool foo 32
  create_pool bar 32

  foo_id=$(get_pool_id "foo")
  bar_id=$(get_pool_id "bar")

  [[ $foo_id -lt 0 ]] && ( echo "couldn't find pool 'foo' id" ; exit 1 )
  [[ $bar_id -lt 0 ]] && ( echo "couldn't find pool 'bar' id" ; exit 1 )

  # no real clue why we are getting these warnings, but let's make them go
  # away so we can be happy.

  ceph osd set-full-ratio 0.97
  ceph osd set-backfillfull-ratio 0.97

  wait_for_health_ok || exit 1

  pre_map_diff=$(get_fc_lc_diff)
  wait_for_total_num_maps 2
  post_map_diff=$(get_fc_lc_diff)

  [[ $post_map_diff -le $pre_map_diff ]] || exit 1

  pre_map_diff=$post_map_diff

  ceph osd pool set foo size 3
  ceph osd pool set bar size 3

  wait_for_health_ok || exit 1

  check_lec_equals_pools || exit 1
  check_lec_honours_osd || exit 1
  validate_fc || exit 1

  # down osd.2; expected result (because all pools' size equals 3):
  # - number of committed maps increase over 2
  # - lec equals fc
  # - lec equals osd.2's epoch
  # - all pools have floor equal to lec

  while kill $osd_pid ; do sleep 1 ; done
  ceph osd down 2
  sleep 5 # seriously, just to make sure things settle; we may not need this.

  # generate some maps
  for ((i=0; i <= 10; ++i)); do
    ceph osd set noup
    sleep 1
    ceph osd unset noup
    sleep 1
  done

  post_map_diff=$(get_fc_lc_diff)
  [[ $post_map_diff -gt 2 ]] || exit 1

  validate_fc || exit 1
  check_lec_equals_pools || exit 1
  check_lec_honours_osd 2 || exit 1

  # adjust pool 'bar' size to 2; expect:
  # - number of committed maps still over 2
  # - lec equals fc
  # - lec equals pool 'foo' floor
  # - pool 'bar' floor greater than pool 'foo'

  ceph osd pool set bar size 2

  diff_ver=$(get_fc_lc_diff)
  [[ $diff_ver -gt 2 ]] || exit 1

  validate_fc || exit 1

  check_lec_equals_pools $foo_id || exit 1
  check_lec_lower_than_pool $bar_id || exit 1

  check_floor_pool_greater_than_pool $bar_id $foo_id || exit 1

  # set pool 'foo' size to 2; expect:
  # - health_ok
  # - lec equals pools
  # - number of committed maps decreases
  # - lec equals fc

  pre_map_diff=$(get_fc_lc_diff)

  ceph osd pool set foo size 2 || exit 1
  wait_for_clean || exit 1

  check_lec_equals_pools || exit 1
  validate_fc || exit 1

  if ! wait_for_total_num_maps 2 ; then
    post_map_diff=$(get_fc_lc_diff)
    # number of maps is decreasing though, right?
    [[ $post_map_diff -lt $pre_map_diff ]] || exit 1
  fi

  # bring back osd.2; expect:
  # - health_ok
  # - lec equals fc
  # - number of committed maps equals 2
  # - all pools have floor equal to lec

  pre_map_diff=$(get_fc_lc_diff)

  activate_osd $dir 2 || exit 1
  wait_for_health_ok || exit 1
  validate_fc || exit 1
  check_lec_equals_pools || exit 1

  if ! wait_for_total_num_maps 2 ; then
    post_map_diff=$(get_fc_lc_diff)
    # number of maps is decreasing though, right?
    [[ $post_map_diff -lt $pre_map_diff ]] || exit 1
  fi

  return 0
}

main mon-last-clean-epoch "$@"
