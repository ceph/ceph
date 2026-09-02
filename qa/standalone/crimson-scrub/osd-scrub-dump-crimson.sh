#!/usr/bin/env bash
#
# Copyright (C) 2025 Red Hat <contact@redhat.com>
#
# Crimson adaptation: Based on osd-scrub-dump.sh
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

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
  # --noremove: set NOREMOVE=1 so teardown() skips "rm -fr $dir".
  local new_args=()
  for arg in "$@"; do
    case "$arg" in
      --debug)    CRIMSON_EXTRA_OPTS+=" --debug" ;;
      --noremove) NOREMOVE=1 ;;
      *)          new_args+=("$arg") ;;
    esac
  done
  set -- "${new_args[@]}"

  export CEPH_MON="127.0.0.1:7149" # git grep '\<7149\>' : there must be only one
  export CEPH_ARGS
  CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
  CEPH_ARGS+="--mon-host=$CEPH_MON "
  # Crimson requires msgr2
  CEPH_ARGS+="--ms-bind-msgr2=true --ms-bind-msgr1=false "
  # Critical: Mark pools as crimson-compatible by default
  CEPH_ARGS+="--osd_pool_default_crimson=true "
  # Disable PG autoscale for crimson (not supported yet)
  CEPH_ARGS+="--osd_pool_default_pg_autoscale_mode=off "
  # Crimson OSD uses stateless_server (lossy) for client connections, so a
  # dropped connection leaves in-flight rados ops waiting forever.
  # Setting rados_osd_op_timeout causes librados to resubmit the op on a new
  # connection after this many seconds instead of blocking indefinitely.
  CEPH_ARGS+="--rados-osd-op-timeout=30 "

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


# collect_pool_stamps() returns a dictionary with PG IDs as keys
# and their last scrub timestamps as values.
function collect_pool_stamps() {
  local dir=$1
  local poolid="$2"
  local -n out_dict=$3
  local saved_echo_flag=${-//[^x]/}
  set +x

  while IFS="=" read -r pgid stamp; do
    out_dict["$pgid"]="$stamp"
  done < <(
  ceph --format json pg dump pgs 2>/dev/null |
      jq -r --arg poolid "$poolid" \
         '.pg_stats[] | select(.pgid | startswith($poolid + ".")) | "\(.pgid)=\(.last_scrub_stamp)"'
  )
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 0
}


# we are assuming the keys fully match
function arrays_all_values_differ() {
  local -n a1  # the 'last_stamps'(in the assumed usage)
  local -n a2  # the 'current_stamps'
  local key
  for key in "${!a1[@]}"; do
    [[ -v a2[$key] ]] || return 1                  # missing in 2nd → fail
    [[ ${a1[$key]} != "${a2[$key]}" ]] || return 1 # same value → fail
  done
  return 0  # every key in 1st exists in 2nd and all values differ
}


# wait for a whole pool to be scrubbed.
# 'last-scrub' timestamps are compared to a reference timestamps array
# (ostensibly taken just before all the PGs in the pool were instructed to
# scrub).
# All 'last-scrub' timestamps must be greater than the reference timestamps.
function wait_for_pool_scrubbed() {
  local dir=$1
  local pool_id=$2
  local -n prev_stamps=$3
  local timeout=${4:-1000}
  local saved_echo_flag=${-//[^x]/}
  set +x
  local elapsed=0
  while true; do
    local -A current_stamps
    collect_pool_stamps $dir $poolid current_stamps
    if arrays_all_values_differ prev_stamps current_stamps; then
      break
    fi
    elapsed=$((elapsed + 1))
    if [ $elapsed -gt $((timeout * 10)) ]; then
      echo "wait_for_pool_scrubbed: Timeout waiting for scrub completion after $timeout seconds"
      return 1
    fi
    sleep 0.1
  done
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 0
}


# manipulator functions:

# delete_oi: delete the object info
# Note: uses _objectstore_tool_nodown which auto-detects seastore and uses
# crimson-objectstore-tool when needed. The OSD must already be stopped.
function delete_oi()
{
  local dir=$1
  local target_osd=$2
  local obj=$3
  _objectstore_tool_nodown "$dir" "$target_osd" "$obj" rm-attr '_' || return 1
  return 0
}

# remove one copy of an object
# Note: uses _objectstore_tool_nodown which auto-detects seastore.
# The OSD must already be stopped.
function rm_object()
{
  local dir=$1
  local target_osd=$2
  local obj=$3
  _objectstore_tool_nodown "$dir" "$target_osd" "$obj" remove || return 1
  return 0
}


# applying the manipulators to objects of a specific OSD


# modify object for which the downed OSD is primary / secondary.
# Parameters:
function modify_obs_of_an_osd()
{
  local dir=$1
  local target_osd=$2
  # list of objects to modify
  local objects_to_modify=$3
  local -n o_to_pg=$4
  local manips=$5
  local extr_dbg=1

  # Skip if no objects to modify
  if [[ -z "$objects_to_modify" ]]; then
    return 0
  fi

  # the objects to modify:
  (( extr_dbg >= 1 )) && echo "Objects to modify on osd.$target_osd: ${objects_to_modify[@]}"

  # Note: all OSDs are down
  local cur_fn_index=0
  for obj in $objects_to_modify; do
    (( extr_dbg >= 2 )) && echo "Object $obj: PGID: ${o_to_pg[$obj]}"
    # get the PGID of the object
    local pgid=${o_to_pg[$obj]}
    if [[ -z "$pgid" ]]; then
      echo "ERROR: cannot find PGID for object $obj on osd.$target_osd"
      return 1
    fi
    # get the manipulator function name
    local manipulator=$(echo $manips | cut -d' ' -f$((cur_fn_index % ${#manips[@]} + 1)))
    (( extr_dbg >= 2 )) && echo "Manipulator: $manipulator"
    if declare -f "$manipulator" > /dev/null; then
      $manipulator "$dir" "$target_osd" "$obj" || return 1
    fi
    (( cur_fn_index++ ))
  done
  return 0
}


# \todo change to combine the counters from all OSDs
# Note: use the following to aggregate the JSON objects of a single OSD:
#   ceph tell osd.$osd counter dump --format=json |
#     jq '[recurse | objects | to_entries[] | select(.key | test("scrub")) | {(.key): .value}] | add' |
#     jq -s '.' >> ${fnm}_b
function dump_scrub_counters()
{
  local dir=$1
  local OSDS=$2
  local hdr_msg=$3
  local extr_dbg=1

  fnm="/tmp/dscrub_counters_`date +%d_%H%M`"
  ((extr_dbg >= 3)) && echo "$hdr_msg: Scrub counters at `date +%T.%N`" > $fnm
  for ((osd=0; osd<OSDS; osd++)); do
    echo "osd.$osd scrub counters:" >> $fnm
    all_c=$(ceph tell osd.$osd counter dump --format=json |
        jq 'recurse | objects | to_entries[] | select(.key | test("scrub"))')
    ((extr_dbg >= 3)) && echo "$all_c" >> $fnm
    echo "$all_c"
  done
}


# create K objects in the named pool, using one common random data
# file of size S.
#
# Crimson uses stateless_server (lossy) connections: the OSD closes the
# socket immediately after sending each reply, before the TCP bytes fully
# drain to the client.  When multiple rados puts run in parallel each on
# its own short-lived connection, some clients see "read eof" on the reply
# and hang indefinitely.  Write sequentially so each connection is fully
# torn down before the next begins.  The NTHR parameter is accepted but
# ignored for compatibility with the caller.
function create_objects()
{
  local dir=$1
  local poolname=$2
  local K=$3
  local S=$4
  local NTHR=$5   # ignored: Crimson requires sequential writes (see above)

  local start_times=$(date +%s%N)
  local testdata_file=$(file_with_random_data $S)

  for ((i=1; i<=K; i++)); do
    rados -p $poolname put obj${i} $testdata_file || return 1
  done
  rm $testdata_file
  local end_times=$(date +%s%N)
  local duration=$(( (end_times - start_times) / 1000000 ))
  echo "create_objects(): created $K objects in pool $poolname in $duration ms"
}


# Crimson-specific version of standard_scrub_wpq_cluster
# Creates a cluster with Crimson OSDs for scrub testing
function crimson_standard_scrub_wpq_cluster() {
  local dir=$1
  local -n conf=$2
  local osd_sleep=$3

  local OSDS=${conf['osds_num']}
  local pg_num=${conf['pgs_in_pool']}
  local poolname=${conf['pool_name']}
  local pool_default_size=${conf['pool_default_size']:-3}
  local extra_pars=${conf['extras']:-""}

  run_mon $dir a --osd_pool_default_size=$pool_default_size || return 1
  apply_crimson_config || return 1
  run_mgr $dir x --mgr_stats_period=1 || return 1

  local ceph_osd_args="--osd_objectstore=seastore \
          --osd_deep_scrub_randomize_ratio=0 \
          --osd_scrub_interval_randomize_ratio=0 \
          --osd_scrub_backoff_ratio=0.0 \
          --osd_pool_default_pg_autoscale_mode=off \
          --osd_pg_stat_report_interval_max_seconds=1 \
          --osd_pg_stat_report_interval_max_epochs=1 \
          --osd_stats_update_period_not_scrubbing=3 \
          --osd_stats_update_period_scrubbing=1 \
          --osd_scrub_retry_after_noscrub=5 \
          --osd_scrub_retry_pg_state=5 \
          --osd_scrub_retry_delay=3 \
          --osd_pool_default_size=$pool_default_size \
          --osd_op_queue=wpq \
          --osd_scrub_sleep=$osd_sleep \
          --osd_scrub_begin_hour=0 \
          --osd_scrub_end_hour=0 \
          $extra_pars"

  for osd in $(seq 0 $(expr $OSDS - 1))
  do
    run_crimson_osd $dir $osd $ceph_osd_args || return 1
  done

  if [[ "$poolname" != "nopool" ]]; then
    create_pool $poolname $pg_num $pg_num --autoscale_mode=off || return 1
    wait_for_clean || return 1
  fi

  # update the in/out 'args' with the ID of the new pool
  sleep 1
  if [[ "$poolname" != "nopool" ]]; then
    name_n_id=`ceph osd dump | awk '/^pool.*'$poolname'/ { gsub(/'"'"'/," ",$3); print $3," ", $2}'`
    echo "crimson_standard_scrub_wpq_cluster: test pool is $name_n_id"
    conf['pool_id']="${name_n_id##* }"
  fi
  conf['osd_args']=$ceph_osd_args
}


# Parameters:
# $1: the test directory
# $2: [in/out] an array of configuration values
# $3: test name to appear in the output
#
# See osd-scrub-dump.sh::corrupt_and_measure() for full parameter documentation.
#
# Crimson-specific differences:
# - Uses crimson_standard_scrub_wpq_cluster (Crimson OSDs with seastore)
# - Uses KILL signal for stopping Crimson (Seastar-based) OSDs
# - Uses _objectstore_tool_nodown which auto-detects seastore and routes to
#   crimson-objectstore-tool when available
# - activate_osd auto-detects the seastore type file and uses crimson-osd
function corrupt_and_measure()
{
  local dir=$1
  local -n alargs=$2
  local test_name=$3

  local OSDS=${alargs['osds_num']}
  local PGS=${alargs['pgs_in_pool']}
  local OBJS_PER_PG=${alargs['objects_per_pg']}
  local OBJECT_SIZE=${alargs['object_size']:-256}
  local modify_as_prim_cnt=${alargs['modify_as_prim_cnt']}
  local modify_as_repl_cnt=${alargs['modify_as_repl_cnt']}
  local stride_override=${alargs['stride']:-0}

  local objects=$(($PGS * $OBJS_PER_PG))
  # the total number of corrupted objects cannot exceed the number of objects
  if [ $(($modify_as_prim_cnt + $modify_as_repl_cnt)) -gt $objects ]; then
    echo "ERROR: too many corruptions requested ($modify_as_prim_cnt + $modify_as_repl_cnt > $objects)"
    return 1
  fi

  local extr_dbg=2 # note: 3 and above leaves some temp files around
  # must not do that: set -o nounset
  crimson_standard_scrub_wpq_cluster "$dir" alargs 0 || return 1
  orig_osd_args=" ${alargs['osd_args']}"
  orig_osd_args=" $(echo $orig_osd_args)"

  local poolid=${alargs['pool_id']}
  local poolname=${alargs['pool_name']}
  (( extr_dbg >= 1 )) && echo "Pool: $poolname : $poolid"
  # prevent scrubbing while we corrupt objects
  ceph osd pool set $poolname noscrub 1
  ceph osd pool set $poolname nodeep-scrub 1

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set -x

  # Create some objects
  create_objects "$dir" "$poolname" "$objects" "$OBJECT_SIZE" 4 || return 1

  echo "Pre-wait-forclean: $(date +%T.%N)"
  wait_for_clean || return 1
  ceph osd pool stats
  if (( stride_override > 0 )); then
    echo "Setting osd_deep_scrub_stride to $stride_override"
    ceph tell osd.* config set osd_deep_scrub_stride "$stride_override"
  fi
  sleep 5
  ceph pg dump pgs
  # the actual stride (either a default or the override value)
  (( extr_dbg >= 1 )) && echo "Getting actual osd_deep_scrub_stride: " && ceph tell osd.0 config show | jq -r '.osd_deep_scrub_stride'
  local actual_stride=$(ceph tell osd.0 config show | jq -r '.osd_deep_scrub_stride')

  echo "Pre dict creation: $(date +%T.%N)"
  local start_dict=$(date +%s%N)
  declare -A obj_to_pgid
  declare -A obj_to_primary
  declare -A obj_to_acting
  objs_to_prim_dict_fast "$dir" $poolname "obj" $objects obj_to_pgid obj_to_primary obj_to_acting
  local end_dict=$(date +%s%N)
  echo "Post dict creation: $(date +%T.%N) ($(( (end_dict - start_dict)/1000000 )) ms)"

  # create a subset of modify_as_prim_cnt+modify_as_repl_cnt objects
  # that we will corrupt. Note that no object is to have both its primary
  # and replica versions corrupted.
  local all_errors=$(($modify_as_prim_cnt + $modify_as_repl_cnt))
  # select the objects to corrupt (both primary and replica)
  mapfile -t selected_keys < <(printf "%s\n" "${!obj_to_primary[@]}" | shuf -n "$all_errors")
  # list the number of elements, and the first element
  (( extr_dbg >= 2 )) && echo "Selected keys: ${#selected_keys[@]}"
  (( extr_dbg >= 2 )) && echo "First element: ${selected_keys[0]}"
  (( extr_dbg >= 2 )) && echo "obj_to_primary: ${#obj_to_primary[@]}"

  # take the first modify_as_prim_cnt of them to corrupt their primary version.
  # the rest will be corrupted on their replicas.

  declare -A prim_objs_to_corrupt
  declare -A repl_objs_to_corrupt
  # group by the primary OSD (the dict value)
  for ((i=0; i < $modify_as_prim_cnt; i++)); do
    (( extr_dbg >= 2 )) && echo "Corrupting primary object ($1) ${selected_keys[$i]}"
    k=${selected_keys[$i]}
    prim_osd=${obj_to_primary[$k]}
    prim_objs_to_corrupt["$prim_osd"]+="$k "
  done
  for ((i=$modify_as_prim_cnt; i < $all_errors; i++)); do
    k=${selected_keys[$i]}
    # find a replica OSD to "fix"
    (( extr_dbg >= 2 )) && echo "Object $k: acting: ${obj_to_acting[$k]}"
    (( extr_dbg >= 2 )) && echo "${obj_to_acting[$k]}" | awk '{print $NF}'
    # modify either the first replica (in the acting-set order) or the
    # last one, decided based on the object sequential number
    acting_as_arr=(${obj_to_acting[$k]})
    (( extr_dbg >= 2 )) && echo "r----- ${acting_as_arr} for $k ($i)  ${acting_as_arr[1]} ${acting_as_arr[-1]} ${acting_as_arr[0]}"
    if (( i % 2 == 0 )); then
      repl_osd=${acting_as_arr[1]}
    else
      repl_osd=${acting_as_arr[-1]}
    fi
    (( extr_dbg >= 2 )) && echo "replosd ${repl_osd} for $k"
    repl_objs_to_corrupt["$repl_osd"]+="$k "
  done
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi

  # disable rescheduling of the queue due to 'no-scrub' flags
  ceph tell osd.* config set osd_scrub_backoff_ratio 0.9999

  # --------------------------  step 2: corruption of objects --------------------------

  # Crimson OSDs (Seastar-based) need KILL signal instead of TERM
  kill_daemons $dir KILL osd || return 1
  sleep 1
  for ((osd=0; osd<OSDS; osd++)); do
    modify_obs_of_an_osd "$dir" "$osd" "${prim_objs_to_corrupt[$osd]}" \
                        obj_to_pgid ${alargs['manipulations']} || return 1
    modify_obs_of_an_osd "$dir" "$osd" "${repl_objs_to_corrupt[$osd]}" \
                        obj_to_pgid ${alargs['manipulations']} || return 1
  done
  echo "osd args:"
  echo "\t$ceph_osd_args"
  echo "\tsaved: $orig_osd_args"
  for ((osd=0; osd<OSDS; osd++)); do
    # activate_osd auto-detects seastore from the type file and uses crimson-osd
    activate_osd "$dir" "$osd" $orig_osd_args || return 1
  done

  sleep 6

  # ---------------------------  step 3: scrub & measure -------------------------------

  # set the scrub parameters and the update frequency for low latencies
  ceph tell osd.* config set osd_scrub_sleep "0"
  ceph tell osd.* config set osd_max_scrubs 3  # for now, only 3 scrubs at a time
  ceph tell osd.* config set osd_stats_update_period_not_scrubbing 1
  ceph tell osd.* config set osd_stats_update_period_scrubbing 1
  ceph tell osd.* config set osd_scrub_chunk_max 5
  ceph tell osd.* config set osd_shallow_scrub_chunk_max 5
  ceph tell osd.* config set osd_scrub_backoff_ratio 0.9999
  if (( stride_override > 0 )); then
    echo "Setting osd_deep_scrub_stride to $stride_override"
    ceph tell osd.* config set osd_deep_scrub_stride "$stride_override"
  fi
  sleep 5
  ceph pg dump pgs
  # the actual stride (either a default or the override value)
  (( extr_dbg >= 1 )) && echo "Getting actual osd_deep_scrub_stride: " && ceph tell osd.0 config show | jq -r '.osd_deep_scrub_stride'

  # no auto-repair
  ceph tell osd.* config set osd_scrub_auto_repair false
  sleep 1

  #create the dictionary of the PGs in the pool
  local start_stdict=$(date +%s%N)
  echo "Pre standard dict creation: $(date +%T.%N)"
  declare -A pg_pr
  declare -A pg_ac
  declare -A pg_po
  build_pg_dicts "$dir" pg_pr pg_ac pg_po "-"
  local end_stdict=$(date +%s%N)
  echo "Post standard dict creation: $(date +%T.%N) ($(( (end_stdict - start_stdict)/1000000 )) ms)"
  (( extr_dbg >= 2 )) && echo "PGs table:"
  for pg in "${!pg_pr[@]}"; do
    (( extr_dbg >= 2 )) && echo "Got: $pg: ${pg_pr[$pg]} ( ${pg_ac[$pg]} ) ${pg_po[$pg]}"
  done

  local -A saved_last_stamp
  collect_pool_stamps $dir "$poolid" saved_last_stamp
  ceph pg dump pgs

  ceph tell osd.* config set debug_osd 10/10
  local start_time=$(date +%s%N)
  for pg in "${!pg_pr[@]}"; do
    (( extr_dbg >= 1 )) && echo "deep-scrub $pg"
    ceph pg $pg deep-scrub || return 1
  done

  # wait for the scrubs to complete
  wait_for_pool_scrubbed "$dir" "$poolid" saved_last_stamp

  local end_time=$(date +%s%N)
  local duration=$(( (end_time - start_time)/1000000 ))
  ceph tell osd.* config set debug_osd 20/20

  sleep 2
  ceph pg dump pgs
  printf 'MSR @%15s@ NAUT %3d %3d %3d %3d/%3d %7d %7d/%7d %6d\n' "$test_name" "$OSDS" "$PGS" "$OBJS_PER_PG" \
                      "$modify_as_prim_cnt" "$modify_as_repl_cnt" "$OBJECT_SIZE" "$actual_stride" \
                      "$stride_override"  "$duration"
  for pg in "${!pg_pr[@]}"; do
    echo "list-inconsistent for PG $pg"
    rados -p $poolname list-inconsistent-obj $pg --format=json-pretty | jq '.' | wc -l
  done
  for ((osd=0; osd<OSDS; osd++)); do
    ceph pg ls-by-osd $osd
  done

  # ---------------------------  step 4: repair -------------------------------

  ## if testing 'auto-repair' instead of 'repair' - uncomment the next line.
  ## And if running a pre-Tentacle version: uncomment the following line
  ## as well, to workaround a bug.
  #ceph tell osd.* config set osd_scrub_auto_repair true
  #ceph tell osd.* config set osd_scrub_auto_repair_num_errors 1000
  sleep 5
  (( extr_dbg >= 3 )) && ceph pg dump pgs --format=json-pretty | jq '.pg_stats[]' > /tmp/pg_stats.json
  (( extr_dbg >= 3 )) && ceph pg dump pgs --format=json-pretty | jq '.pg_stats[] |
                         select(.state | contains("inconsistent"))' >> /tmp/pg_stats_inconsistent.json

  collect_pool_stamps $dir "$poolid" saved_last_stamp
  ceph tell osd.* config set debug_osd 10/10
  start_time=$(date +%s%N)
  for pg in "${!pg_pr[@]}"; do
    ceph pg repair  $pg || return 1
    ## or, if auto-repairing:
    #ceph pg $pg deep-scrub || return 1
  done
  wait_for_pool_scrubbed "$dir" "$poolid" saved_last_stamp
  end_time=$(date +%s%N)
  duration=$(( (end_time - start_time)/1000000 ))
  ceph tell osd.* config set debug_osd 20/20

  sleep 5
  ceph pg dump pgs
  printf 'MSR @%15s@ REPR %3d %3d %3d %3d/%3d %7d %7d/%7d %6d\n' "$test_name" "$OSDS" "$PGS" "$OBJS_PER_PG" \
                      "$modify_as_prim_cnt" "$modify_as_repl_cnt" "$OBJECT_SIZE" "$actual_stride" \
                      "$stride_override"  "$duration"
  dump_scrub_counters "$dir" "$OSDS" "After repair"

  # -- collecting some data after the repair

  for pg in "${!pg_pr[@]}"; do
    incon_lines=$(rados -p $poolname list-inconsistent-obj $pg --format=json-pretty | jq '.' | wc -l)
    if [ "$incon_lines" -gt 5 ]; then
      echo "PG $pg still seems to have $incon_lines inconsistent objects!!!!"
      # for some reason, the list-inconsistent does not get fully updated immediately
      # (10 seconds, for example, are not enough)
      # return 1
    fi
  done

  for ((osd=0; osd<OSDS; osd++)); do
    ceph pg ls-by-osd $osd
  done
  wait_for_clean || return 1
  ceph pg dump pgs

  # ---------------------------  step 5: re-scrub, expecting no errors --------


  collect_pool_stamps $dir "$poolid" saved_last_stamp
  start_time=$(date +%s%N)
  for pg in "${!pg_pr[@]}"; do
    ceph pg deep-scrub  $pg || return 1
  done
  wait_for_pool_scrubbed "$dir" "$poolid" saved_last_stamp
  end_time=$(date +%s%N)
  duration=$(( (end_time - start_time)/1000000 ))
  printf 'MSR @%15s@ REDE %3d %3d %3d %3d/%3d %7d %7d/%7d %6d\n' "$test_name" "$OSDS" "$PGS" "$OBJS_PER_PG" \
                      "$modify_as_prim_cnt" "$modify_as_repl_cnt" "$OBJECT_SIZE" "$actual_stride" \
                      "$stride_override"  "$duration"
  sleep 3
  ceph pg dump pgs
  dump_scrub_counters "$dir" "$OSDS" "Final"
  return 0
}


# ---------------------------  the actual tests -------------------------------


function TEST_time_measurements_basic_1()
{
  # the following is enough to trigger the 'auto-repair operator-repair' bug
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="8"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="basic1"
    ['objects_per_pg']="16"
    ['modify_as_prim_cnt']="24" # elements to corrupt their Primary version
    ['modify_as_repl_cnt']="24" # elements to corrupt one of their replicas
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "basic_1" || return 1
}

function TEST_rmobj_1()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="4"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="rm1"
    ['objects_per_pg']="16"
    ['modify_as_prim_cnt']="2"
    ['modify_as_repl_cnt']="2"
    ['manipulations']="rm_object"
  )
  corrupt_and_measure "$1" cluster_conf "rmobj_1" || return 1
}

function TEST_time_measurements_basic_2()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="4"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="basic2"
    ['objects_per_pg']="16"
    ['modify_as_prim_cnt']="10"
    ['modify_as_repl_cnt']="0"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "basic_2" || return 1
}

function TEST_time_measurements_basic_2b()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="8"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="basic2b"
    ['objects_per_pg']="128"
    ['modify_as_prim_cnt']="40"
    ['modify_as_repl_cnt']="10"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "basic_2b" || return 1
}

function TEST_time_measurements_basic_2c()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="4"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="basic2c"
    ['objects_per_pg']="8"
    ['modify_as_prim_cnt']="0"
    ['modify_as_repl_cnt']="10"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "basic_2c" || return 1
}

function TEST_time_measurements_basic_3()
{
  local -A cluster_conf=(
    ['osds_num']="4"
    ['pgs_in_pool']="16"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="basic3"
    ['objects_per_pg']="128"
    ['modify_as_prim_cnt']="40"
    ['modify_as_repl_cnt']="10"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "basic_3" || return 1
}

function TEST_time_large_objects()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="8"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="large_objs"
    ['objects_per_pg']="16"
    ['object_size']="1048576" # 1MB
    ['modify_as_prim_cnt']="0"
    ['modify_as_repl_cnt']="10"
    ['manipulations']="rm_object"
  )
  corrupt_and_measure "$1" cluster_conf "large_obj" || return 1
}

function TEST_time_large_objects_2()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="8"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="large_objs"
    ['objects_per_pg']="8"
    ['object_size']="4194304" # 4MB
    ['modify_as_prim_cnt']="20"
    ['modify_as_repl_cnt']="20"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "large_obj_2" || return 1
}

function TEST_time_large_objects_2b()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="8"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="large_objs"
    ['objects_per_pg']="8"
    ['object_size']="4194300" # 4MB - 4bytes
    ['stride']="4194304" # 4MB
    ['modify_as_prim_cnt']="20"
    ['modify_as_repl_cnt']="20"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "large_obj_2b" || return 1
}


function TEST_time_large_objects_2bs1M()
{
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="8"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="large_objs"
    ['objects_per_pg']="8"
    ['object_size']="4194300" # 4MB - 4bytes
    ['stride']="1048576" # 1MB
    ['modify_as_prim_cnt']="20"
    ['modify_as_repl_cnt']="20"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "large_obj_2bs1M" || return 1
}

function TEST_time_large_objects_3()
{
  # stride size unmodified from the default configuration value
  local -A cluster_conf=(
    ['osds_num']="3"
    ['pgs_in_pool']="8"
    ['pool_name']="test"
    ['pool_default_size']="3"
    ['msg']="large_objs"
    ['objects_per_pg']="16"
    ['object_size']="12582912" # 12MB
    ['modify_as_prim_cnt']="20"
    ['modify_as_repl_cnt']="20"
    ['manipulations']="delete_oi"
  )
  corrupt_and_measure "$1" cluster_conf "large_obj_3" || return 1
}

main osd-scrub-dump-crimson "$@"

# Local Variables:
# compile-command: "cd build ; make check && \
#    ../qa/run-standalone.sh osd-scrub-dump-crimson.sh"
# End: