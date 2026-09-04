#!/usr/bin/env bash
# -*- mode:text; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
# vim: ts=8 sw=2 sts=2 expandtab
#
# Copyright (C) 2024 Red Hat <contact@redhat.com> - Crimson adaptation
#
# Crimson adaptation of osd-mapper.sh:
# test the handling of a corrupted SnapMapper DB by Scrub in a Crimson/SeaStore
# environment.
#
# The classic test (osd-mapper.sh) corrupts SnapMapper entries via:
#   ceph-kvstore-tool bluestore-kv $dir/$osd dump "p" | grep SNA_...
# and then rm/set via the same tool.
#
# In Crimson/SeaStore the SnapMapper stores its SNA_ and OBJ_ entries as
# omap keys on the per-PG "snapmapper" object (hobject_t named "snapmapper"
# with namespace INTERNAL_PG_LOCAL_NS, see src/common/hobject.h:make_snapmapper).
# Those omap keys are NOT in the RootMetaBlock that crimson-kvstore-tool
# targets; they live in SeaStore's per-object omap tree.
#
# The correct tool for reaching per-object omap in SeaStore is
# crimson-objectstore-tool, which provides list-omap / get-omap / rm-omap /
# set-omap commands (src/crimson/tools/objectstore/crimson_objectstore_tool.cc).
# This script uses those commands to perform the same SNA_ key truncation
# that the classic test performs via ceph-kvstore-tool bluestore-kv.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh

function run() {
  local dir=$1
  shift

  # Parse --debug and --noremove flags (may appear anywhere in the argument list).
  # --debug:    append --debug to CRIMSON_EXTRA_OPTS so only run_crimson_osd
  #             picks it up.  Must NOT go into EXTRA_OPTS because run_mon,
  #             run_mgr, etc. pass EXTRA_OPTS to ceph-mon/ceph-mgr which do
  #             not accept a bare --debug flag (they require --debug-<subsystem>).
  # --noremove: set NOREMOVE=1 so teardown() in ceph-helpers.sh skips "rm -fr $dir".
  local new_args=()
  for arg in "$@"; do
    case "$arg" in
      --debug)    CRIMSON_EXTRA_OPTS+=" --debug" ;;
      --noremove) NOREMOVE=1 ;;
      *)          new_args+=("$arg") ;;
    esac
  done
  set -- "${new_args[@]}"

  export CEPH_MON="127.0.0.1:7155" # git grep '\<7155\>' : there must be only one
  export CEPH_ARGS
  CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
  CEPH_ARGS+="--mon-host=$CEPH_MON "
  # Crimson requires msgr2
  CEPH_ARGS+="--ms-bind-msgr2=true --ms-bind-msgr1=false "
  # Critical: Mark pools as crimson-compatible by default
  CEPH_ARGS+="--osd_pool_default_crimson=true "
  # Disable PG autoscale for crimson (not supported yet)
  CEPH_ARGS+="--osd_pool_default_pg_autoscale_mode=off "

  export -n CEPH_CLI_TEST_DUP_COMMAND
  local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}

  local passed=()
  local failed=()
  for func in $funcs ; do
    echo "-------------- Prepare Test $func -------------------"
    if ! setup $dir ; then
      echo "SETUP FAILED for $func — skipping"
      failed+=("$func (setup failed)")
      continue
    fi
    echo "-------------- Run Test $func -----------------------"
    if $func $dir ; then
      passed+=("$func")
    else
      echo "FAILED: $func"
      failed+=("$func")
    fi
    echo "-------------- Teardown Test $func ------------------"
    teardown $dir || true
    echo "-------------- Complete Test $func ------------------"
  done

  echo ""
  echo "======== TEST RESULTS ========"
  echo "PASSED (${#passed[@]}):"
  for f in "${passed[@]}"; do echo "  [PASS] $f"; done
  echo "FAILED (${#failed[@]}):"
  for f in "${failed[@]}"; do echo "  [FAIL] $f"; done
  echo "=============================="

  if [ ${#failed[@]} -ne 0 ]; then
    return 1
  fi
}

function apply_crimson_config() {
  # Apply critical configurations that vstart.sh sets via config assimilate-conf
  # These are needed for proper Crimson operation and peering
  ceph config set mon mon_osd_reporter_subtree_level osd || return 1
  ceph config set mon mon_data_avail_warn 2 || return 1
  ceph config set mon mon_data_avail_crit 1 || return 1
  ceph config set mon mon_allow_pool_delete true || return 1
  ceph config set mon mon_allow_pool_size_one true || return 1
  ceph config set osd osd_scrub_load_threshold 2000 || return 1
  ceph config set osd osd_debug_op_order true || return 1
  ceph config set osd osd_debug_misdirected_ops true || return 1
}

# one clone & multiple snaps (according to the number of parameters)
function make_a_clone()
{
  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x
  local pool=$1
  local obj=$2
  echo $RANDOM | rados -p $pool put $obj - || return 1
  shift 2
  for snap in $@ ; do
    rados -p $pool mksnap $snap || return 1
  done
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
}

# crimson_corrupt_snapmapper_sna
#
# Crimson equivalent of the SNA_ key truncation performed in the classic test
# via 'ceph-kvstore-tool bluestore-kv $dir/$osd rm/set p ...'.
#
# In SeaStore, SnapMapper entries (SNA_ / OBJ_) are stored as omap keys on
# the per-PG "snapmapper" object (see src/common/hobject.h:make_snapmapper and
# src/crimson/osd/pg.cc:osdriver construction).  They are NOT in the
# RootMetaBlock that crimson-kvstore-tool targets.
#
# crimson-objectstore-tool provides list-omap / get-omap / rm-omap / set-omap
# commands (src/crimson/tools/objectstore/crimson_objectstore_tool.cc) that
# operate on the per-object omap tree — the right layer to reach SNA_ keys.
#
# $1: dir      — OSD data root ($dir from the test)
# $2: osd_id   — numeric OSD id
# $3: pgid     — pgid string (e.g. "2.0") used to find the snapmapper object
#
function crimson_corrupt_snapmapper_sna() {
    local dir=$1
    local osd_id=$2
    local pgid=$3
    local kvdir=$dir/$osd_id

    echo "corrupting SnapMapper DB of osd.$osd_id (data: $kvdir, pg: $pgid)"

    # The snapmapper object is named "snapmapper" in namespace "_"
    # (INTERNAL_PG_LOCAL_NS).  List its omap to find the SNA_ key for
    # snapid 3 (the snap13 clone == snapid 0x0000000000000003).
    # crimson-objectstore-tool --data-path <path> --op list-omap
    #     --object "snapmapper" --pgid <pgid>
    #
    # Key format (SnapMapper::MAPPING_PREFIX = "SNA_"):
    #   SNA_<pool>_<snapid_hex>_<hobject_str>
    # We match: SNA_<digit>_0000000000000003_
    local KY
    KY=$(crimson-objectstore-tool --data-path $kvdir \
            --pgid $pgid "snapmapper" list-omap 2>/dev/null \
         | grep -a -e 'SNA_[0-9]_0000000000000003_') || true

    if [[ -z "$KY" ]]; then
        echo "WARNING: no SNA_ key for snapid 3 found on osd.$osd_id — skipping"
        return 0
    fi

    echo "SNA key: $KY"

    local tmp_fn
    tmp_fn=$(mktemp -p /tmp --suffix="_the_val")
    echo "Value dumped in: $tmp_fn"

    # Fetch the value of the found key into a temp file.
    # get-omap takes key as positional arg1; the output file must be --file
    # (arg2 is not mapped to file for get-omap, see crimson_objectstore_tool.cc:1222-1231)
    crimson-objectstore-tool --data-path $kvdir \
        --pgid $pgid --file "$tmp_fn" "snapmapper" get-omap "$KY" 2>/dev/null || return 1

    # Truncate the key by 30 characters (same as classic test: NKY=${KY:0:-30})
    local NKY=${KY:0:-30}

    # Remove the original full-length key
    crimson-objectstore-tool --data-path $kvdir \
        --pgid $pgid "snapmapper" rm-omap "$KY" 2>/dev/null || return 1

    # Insert the truncated key with the original value
    crimson-objectstore-tool --data-path $kvdir \
        --pgid $pgid "snapmapper" set-omap "$NKY" "$tmp_fn" 2>/dev/null || return 1

    rm -f $tmp_fn
    return 0
}

function TEST_truncated_sna_record() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="3"
        ['pgs_in_pool']="4"
        ['pool_name']="test"
    )

    local extr_dbg=3
    (( extr_dbg > 1 )) && echo "Dir: $dir"
    crimson_standard_scrub_cluster $dir cluster_conf
    ceph tell osd.* config set osd_stats_update_period_not_scrubbing "1"
    ceph tell osd.* config set osd_stats_update_period_scrubbing "1"

    local osdn=${cluster_conf['osds_num']}
    local poolid=${cluster_conf['pool_id']}
    local poolname=${cluster_conf['pool_name']}
    local objname="objxxx"

    # create an object and clone it
    make_a_clone $poolname $objname snap01 snap02 || return 1
    make_a_clone $poolname $objname snap13 || return 1
    make_a_clone $poolname $objname snap24 snap25 || return 1
    echo $RANDOM | rados -p $poolname put $objname - || return 1

    # identify the PG and the primary OSD
    local pgid=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.pgid'`
    local osd=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.up[0]'`
    echo "pgid is $pgid (primary: osd.$osd)"
    # turn on the publishing of test data in the 'scrubber' section of 'pg query' output
    set_query_debug $pgid

    # verify the existence of these clones
    (( extr_dbg >= 1 )) && rados --format json-pretty -p $poolname listsnaps $objname

    # scrub the PG
    ceph pg $pgid deep-scrub || return 1

    # Wait for the scrub to finish.
    # Crimson's PGScrubber::emit_scrub_result() always emits this INFO line
    # once per scrub completion (src/crimson/osd/scrub/pg_scrubber.cc):
    #   "scrub_finish shard N num_omap_bytes = X num_omap_keys = Y"
    sleep 3
    until grep -a -q "scrub_finish shard" $dir/osd.$osd.log ; do
        sleep 0.2
    done

    ceph pg dump pgs
    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1
    sleep 5
    grep -a -q -v "ERR" $dir/osd.$osd.log || return 1

    # kill the OSDs
    kill_daemons $dir TERM osd || return 1

    (( extr_dbg >= 2 )) && crimson-objectstore-tool --data-path $dir/$osd \
        --pgid $pgid "snapmapper" list-omap 2>/dev/null

    # Count well-formed SNA_ keys before corruption — used for final verification.
    # Well-formed keys have two underscore-separated 16-hex-digit segments after
    # the pool prefix: SNA_<pool>_<16hex>_<16hex>...
    local num_sna_b4
    num_sna_b4=$(crimson-objectstore-tool --data-path $dir/$osd \
        --pgid $pgid "snapmapper" list-omap 2>/dev/null \
      | grep -a -e 'SNA_[0-9]_000000000000000[0-9]_000000000000000' \
      | wc -l)

    # Corrupt the SnapMapper on every OSD.
    # crimson-objectstore-tool operates on per-object omap — the same layer
    # where SeaStore stores SNA_ entries (unlike ceph-kvstore-tool bluestore-kv
    # which reaches them via RocksDB; crimson-kvstore-tool only targets the
    # RootMetaBlock and cannot reach per-object omap).
    for sdn in $(seq 0 $(expr $osdn - 1))
    do
        crimson_corrupt_snapmapper_sna $dir $sdn $pgid || return 1
    done

    local orig_osd_args=" ${cluster_conf['osd_args']}"
    orig_osd_args=" $(echo $orig_osd_args)"
    (( extr_dbg >= 2 )) && echo "Copied OSD args: /$orig_osd_args/ /${orig_osd_args:1}/"
    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      CEPH_ARGS="$CEPH_ARGS $orig_osd_args" activate_osd $dir $sdn
    done
    sleep 1

    for sdn in $(seq 0 $(expr $osdn - 1))
    do
      timeout 60 ceph tell osd.$sdn version
    done
    rados --format json-pretty -p $poolname listsnaps $objname

    # when scrubbing now - we expect the scrub to emit a cluster log ERR message
    # regarding SnapMapper internal inconsistency
    ceph osd unset nodeep-scrub || return 1
    ceph osd unset noscrub || return 1

    # what is the primary now?
    local cur_prim=`ceph --format=json-pretty osd map $poolname $objname | jq -r '.up[0]'`
    ceph pg dump pgs
    sleep 2
    ceph pg $pgid deep-scrub || return 1
    sleep 5
    ceph pg dump pgs
    (( extr_dbg >= 1 )) && grep -a "ERR" $dir/osd.$cur_prim.log
    grep -a -q "ERR" $dir/osd.$cur_prim.log || return 1

    # but did we fix the snap issue? let's try scrubbing again

    local prev_err_cnt=`grep -a "ERR" $dir/osd.$cur_prim.log | wc -l`
    echo "prev count: $prev_err_cnt"

    # scrub again. No errors expected this time
    ceph pg $pgid deep-scrub || return 1
    sleep 5
    ceph pg dump pgs
    (( extr_dbg >= 1 )) && grep -a "ERR" $dir/osd.$cur_prim.log
    local current_err_cnt=`grep -a "ERR" $dir/osd.$cur_prim.log | wc -l`
    (( extr_dbg >= 1 )) && echo "current count: $current_err_cnt"
    (( current_err_cnt == prev_err_cnt )) || return 1
    kill_daemons $dir TERM osd || return 1

    # Verify that the well-formed SNA_ count is restored after self-repair.
    local num_sna_full
    num_sna_full=$(crimson-objectstore-tool --data-path $dir/$cur_prim \
        --pgid $pgid "snapmapper" list-omap 2>/dev/null \
      | grep -a -e 'SNA_[0-9]_000000000000000[0-9]_000000000000000' \
      | wc -l)
    (( num_sna_full == num_sna_b4 )) || return 1
    return 0
}


main osd-mapper-crimson "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-mapper-crimson.sh"
# End:
