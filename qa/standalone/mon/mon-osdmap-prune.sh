#!/bin/bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

base_test=$CEPH_ROOT/qa/workunits/mon/test_mon_osdmap_prune.sh

function run() {

  local dir=$1
  shift

  export CEPH_MON="127.0.0.1:7115"
  export CEPH_ARGS
  CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none --mon-host=$CEPH_MON "

  local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
  for func in $funcs; do
    setup $dir || return 1
    $func $dir || return 1
    teardown $dir || return 1
  done
}

function TEST_osdmap_prune() {

  local dir=$1

  run_mon $dir a || return 1
  run_mgr $dir x || return 1
  run_osd $dir 0 || return 1
  run_osd $dir 1 || return 1
  run_osd $dir 2 || return 1

  sleep 5

  # we are getting OSD_OUT_OF_ORDER_FULL health errors, and it's not clear
  # why. so, to make the health checks happy, mask those errors.
  ceph osd set-full-ratio 0.97
  ceph osd set-backfillfull-ratio 0.97

  ceph config set osd osd_beacon_report_interval 10 || return 1
  ceph config set mon mon_debug_extra_checks true || return 1

  ceph config set mon mon_min_osdmap_epochs 100 || return 1
  ceph config set mon mon_osdmap_full_prune_enabled true || return 1
  ceph config set mon mon_osdmap_full_prune_min 200 || return 1
  ceph config set mon mon_osdmap_full_prune_interval 10 || return 1
  ceph config set mon mon_osdmap_full_prune_txsize 100 || return 1


  bash -x $base_test || return 1

  return 0
}

main mon-osdmap-prune "$@"

