#!/usr/bin/env bash
# @file   scrub-helpers.sh
# @brief  a collection of bash functions useful for scrub standalone tests
#

# extract_published_sch()
#
# Use the output from both 'ceph pg dump pgs' and 'ceph pg x.x query' commands to determine
# the published scrub scheduling status of a given PG.
#
# $1: pg id
# $2: 'current' time to compare to
# $3: an additional time-point to compare to
# $4: [out] dictionary
#
function extract_published_sch() {
  local pgn="$1"
  local -n dict=$4 # a ref to the in/out dictionary
  local current_time=$2
  local extra_time=$3
  local extr_dbg=2 # note: 3 and above leave some temp files around

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  (( extr_dbg >= 3 )) && ceph pg dump pgs -f json-pretty >> /tmp/a_dmp$$
  (( extr_dbg >= 3 )) && ceph pg $1 query -f json-pretty >> /tmp/a_qry$$

  from_dmp=`ceph pg dump pgs -f json-pretty | jq -r --arg pgn "$pgn" --arg extra_dt "$extra_time" --arg current_dt "$current_time" '[
    [[.pg_stats[]] | group_by(.pg_stats)][0][0] | 
    [.[] |
    select(has("pgid") and .pgid == $pgn) |

        (.dmp_stat_part=(.scrub_schedule | if test(".*@.*") then (split(" @ ")|first) else . end)) |
        (.dmp_when_part=(.scrub_schedule | if test(".*@.*") then (split(" @ ")|last) else "0" end)) |

     [ {
       dmp_pg_state: .state,
       dmp_state_has_scrubbing: (.state | test(".*scrub.*";"i")),
       dmp_last_duration:.last_scrub_duration,
       dmp_schedule: .dmp_stat_part,
       dmp_schedule_at: .dmp_when_part,
       dmp_is_future: ( .dmp_when_part > $current_dt ),
       dmp_vs_date: ( .dmp_when_part > $extra_dt  ),
       dmp_reported_epoch: .reported_epoch,
       dmp_seq: .reported_seq
      }] ]][][][]'`

  (( extr_dbg >= 2 )) && echo "from pg dump pg: $from_dmp"
  (( extr_dbg >= 2 )) && echo "query output:"
  (( extr_dbg >= 2 )) && ceph pg $1 query -f json-pretty | awk -e '/scrubber/,/agent_state/ {print;}'

  # note: the query output for the schedule containas two dates: the first is the not-before, and
  # the second is the original target time (which is before or the same as the not-before)
  # the current line format looks like this:
  # "schedule": "scrub scheduled @ 2024-06-26T16:09:56.666 (2024-06-24T16:09:56.338)"
  from_qry=`ceph pg $1 query -f json-pretty | jq -r --arg extra_dt "$extra_time" --arg current_dt "$current_time"  --arg spt "'" '
    . |
        (.q_stat_part=((.scrubber.schedule// "-") | if test(".*@.*") then (split(" @ ")|first) else . end)) |
        (.q_when_part=((.scrubber.schedule// "0") | if test(".*@.*") then (split(" @ ")|last) else "0" end)) |
        (.q_target=((.scrubber.schedule// "0") | if test(".*@.*") then (split(" @ ")|last|split(" (")|last|split(")")|first) else "0" end)) |
        (.q_not_before=((.scrubber.schedule// "0") | if test(".*@.*") then (split(" @ ")|last|split(" (")|first) else "0" end)) |
	(.q_when_is_future=(.q_target > $current_dt)) |
	(.q_vs_date=(.q_when_part > $extra_dt)) |	
      {
        query_epoch: .epoch,
        query_seq: .info.stats.reported_seq,
        query_active: (.scrubber | if has("active") then .active else "bug" end),
        query_schedule: .q_stat_part,
        #query_schedule_at: .q_when_part,
        query_schedule_at: .q_not_before,
        query_target_at: .q_target,
        query_last_duration: .info.stats.last_scrub_duration,
        query_last_stamp: .info.history.last_scrub_stamp,
        query_last_scrub: (.info.history.last_scrub| sub($spt;"x") ),
        query_is_future: .q_when_is_future,
        query_vs_date: .q_vs_date,
        query_scrub_seq: .scrubber.test_sequence
      }
   '`
  (( extr_dbg >= 1 )) && echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key)+"]="+(.value|@sh)),")"'

  # note that using a ref to an associative array directly is tricky. Instead - we are copying:
  local -A dict_src=`echo $from_qry " " $from_dmp | jq -s -r 'add | "(",(to_entries | .[] | "["+(.key)+"]="+(.value|@sh)),")"'`
  dict=()
  for k in "${!dict_src[@]}"; do dict[$k]=${dict_src[$k]}; done

  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
}

# query the PG, until any of the conditions in the 'expected' array are met
#
# A condition may be negated by an additional entry in the 'expected' array. Its
# form should be:
#  key: the original key, with a "_neg" suffix;
#  Value: not checked
#
# $1: pg id
# $2: max retries
# $3: a date to use in comparisons
# $4: set of K/V conditions
# $5: debug message
# $6: [out] the results array
function wait_any_cond() {
  local pgid="$1"
  local retries=$2
  local cmp_date=$3
  local -n ep=$4
  local -n out_array=$6
  local -A sc_data
  local extr_dbg=2

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  local now_is=`date -I"ns"`
  (( extr_dbg >= 2 )) && echo "waiting for any condition ($5): pg:$pgid dt:$cmp_date ($retries retries)"

  for i in $(seq 1 $retries)
  do
    sleep 0.5
    extract_published_sch $pgid $now_is $cmp_date sc_data
    (( extr_dbg >= 4 )) && echo "${sc_data['dmp_last_duration']}"
    (( extr_dbg >= 4 )) && echo "----> loop:  $i  ~ ${sc_data['dmp_last_duration']}  / " ${sc_data['query_vs_date']} " /   ${sc_data['dmp_is_future']}"
    (( extr_dbg >= 2 )) && echo "--> loop:  $i ~ ${sc_data['query_active']} / ${sc_data['query_seq']} / ${sc_data['dmp_seq']} " \
                      "/ ${sc_data['query_is_future']} / ${sc_data['query_last_stamp']} / ${sc_data['query_schedule']} %%% ${!ep[@]}"

    # perform schedule_against_expected(), but with slightly different out-messages behaviour
    for k_ref in "${!ep[@]}"
    do
      (( extr_dbg >= 3 )) && echo "key is $k_ref"
      # is this a real key, or just a negation flag for another key??
      [[ $k_ref =~ "_neg" ]] && continue

      local act_val=${sc_data[$k_ref]}
      local exp_val=${ep[$k_ref]}

      # possible negation? look for a matching key
      local neg_key="${k_ref}_neg"
      (( extr_dbg >= 3 )) && echo "neg-key is $neg_key"
      if [ -v 'ep[$neg_key]' ]; then
        is_neg=1
      else
        is_neg=0
      fi

      (( extr_dbg >= 1 )) && echo "key is $k_ref: negation:$is_neg # expected: $exp_val # in actual: $act_val"
      is_eq=0
      [[ $exp_val == $act_val ]] && is_eq=1
      if (($is_eq ^ $is_neg))  
      then
        echo "$5 - '$k_ref' actual value ($act_val) matches expected ($exp_val) (negation: $is_neg)"
        for k in "${!sc_data[@]}"; do out_array[$k]=${sc_data[$k]}; done
        if [[ -n "$saved_echo_flag" ]]; then set -x; fi
        return 0
      fi
    done
  done

  echo "$5: wait_any_cond(): failure. Note: query-active=${sc_data['query_active']}"
  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 1
}


# schedule_against_expected()
#
# Compare the scrub scheduling state collected by extract_published_sch() to a set of expected values.
# All values are expected to match.
#
# $1: the published scheduling state
# $2: a set of conditions to verify
# $3: text to be echoed for a failed match
#
function schedule_against_expected() {
  local -n dict=$1 # a ref to the published state
  local -n ep=$2  # the expected results
  local extr_dbg=1

  # turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  (( extr_dbg >= 1 )) && echo "-- - comparing:"
  for k_ref in "${!ep[@]}"
  do
    local act_val=${dict[$k_ref]}
    local exp_val=${ep[$k_ref]}
    (( extr_dbg >= 1 )) && echo "key is " $k_ref "  expected: " $exp_val " in actual: " $act_val
    if [[ $exp_val != $act_val ]]
    then
      echo "$3 - '$k_ref' actual value ($act_val) differs from expected ($exp_val)"
      echo '####################################################^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^'

      if [[ -n "$saved_echo_flag" ]]; then set -x; fi
      return 1
    fi
  done

  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
  return 0
}


# Start the cluster "nodes" and create a pool for testing.
#
# The OSDs are started with a set of parameters aimed in creating a repeatable
# and stable scrub sequence:
#  - no scrub randomizations/backoffs
#  - no autoscaler
#
# $1: the test directory
# $2: [in/out] an array of configuration values
#
# The function adds/updates the configuration dictionary with the name of the
# pool created, and its ID.
#
# Argument 2 might look like this:
#
#  declare -A test_conf=( 
#    ['osds_num']="3"
#    ['pgs_in_pool']="7"
#    ['extras']="--extra1 --extra2"
#    ['pool_name']="testpl"
#  )
function standard_scrub_cluster() {
    local dir=$1
    local -n args=$2

    local OSDS=${args['osds_num']:-"3"}
    local pg_num=${args['pgs_in_pool']:-"8"}

    local poolname="${args['pool_name']:-test}"
    args['pool_name']=$poolname

    local pool_default_size=${args['pool_default_size']:-3}
    args['pool_default_size']=$pool_default_size

    local extra_pars=${args['extras']}
    local debug_msg=${args['msg']:-"dbg"}

    # turn off '-x' (but remember previous state)
    local saved_echo_flag=${-//[^x]/}
    set +x

    run_mon $dir a --osd_pool_default_size=$pool_default_size || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1

    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
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
            $extra_pars"

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd $(echo $ceph_osd_args) || return 1
    done

    if [[ "$poolname" != "nopool" ]]; then
        create_pool $poolname $pg_num $pg_num --autoscale_mode=off || return 1
        wait_for_clean || return 1
    fi

    # update the in/out 'args' with the ID of the new pool
    sleep 1
    if [[ "$poolname" != "nopool" ]]; then
        name_n_id=`ceph osd dump | awk '/^pool.*'$poolname'/ { gsub(/'"'"'/," ",$3); print $3," ", $2}'`
        echo "standard_scrub_cluster: $debug_msg: test pool is $name_n_id"
        args['pool_id']="${name_n_id##* }"
    fi
    args['osd_args']=$ceph_osd_args
    if [[ -n "$saved_echo_flag" ]]; then set -x; fi
}


# Start the cluster "nodes" and create a pool for testing - wpq version.
#
# A variant of standard_scrub_cluster() that selects the wpq scheduler and sets a value to
# osd_scrub_sleep. To be used when the test is attempting to "catch" the scrubber during an
# ongoing scrub.
#
# See standard_scrub_cluster() for more details.
#
# $1: the test directory
# $2: [in/out] an array of configuration values
# $3: osd_scrub_sleep
#
# The function adds/updates the configuration dictionary with the name of the
# pool created, and its ID.
function standard_scrub_wpq_cluster() {
    local dir=$1
    local -n conf=$2
    local osd_sleep=$3

    conf['extras']=" --osd_op_queue=wpq --osd_scrub_sleep=$osd_sleep ${conf['extras']}"

    standard_scrub_cluster $dir conf || return 1
}


# Parse the output of a 'pg dump pgs_brief' command and build a set of dictionaries:
# - pg_primary_dict: a dictionary of pgid -> acting_primary
# - pg_acting_dict: a dictionary of pgid -> acting set
# - pg_pool_dict: a dictionary of pgid -> pool
# If the input file is '-', the function will fetch the dump directly from the ceph cluster.
function build_pg_dicts {
  local dir=$1
  local -n pg_primary_dict=$2
  local -n pg_acting_dict=$3
  local -n pg_pool_dict=$4
  local infile=$5

  local extr_dbg=0 # note: 3 and above leave some temp files around

  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x

  # This jq filter extracts all required fields, creating a tab-separated output.
  local jq_filter='.pg_stats[] | [.pgid, (.acting | @sh), .acting_primary, (.pgid | split(".")[0])] | @tsv'

  # if the infile name is '-', fetch the dump directly from the ceph cluster
  if [[ $infile == "-" ]]; then
    local json_data
    json_data=$(ceph pg dump pgs_brief -f=json)
    local -r ceph_cmd_rc=$?
    if [[ $ceph_cmd_rc -ne 0 ]]; then
      echo "Error: 'ceph pg dump' command failed with return code $ceph_cmd_rc"
    fi
    (( extr_dbg >= 3 )) && echo "$json_data" > /tmp/e2

    while IFS=$'\t' read -r pgid acting acting_primary pool; do
      [[ -z "$pgid" ]] && continue
      (( extr_dbg >= 1 )) && echo "PG: $pgid  acting: $acting  primary: $acting_primary  pool: $pool"
      pg_primary_dict["$pgid"]=$acting_primary
      pg_acting_dict["$pgid"]=$acting
      pg_pool_dict["$pgid"]=$pool
    done < <(echo "$json_data" | jq -r "$jq_filter")

  else
    # Process directly from file
    while IFS=$'\t' read -r pgid acting acting_primary pool; do
      [[ -z "$pgid" ]] && continue
      (( extr_dbg >= 1 )) && echo "PG: $pgid  acting: $acting  primary: $acting_primary  pool: $pool"
      pg_primary_dict["$pgid"]=$acting_primary
      pg_acting_dict["$pgid"]=$acting
      pg_pool_dict["$pgid"]=$pool
    done < <(jq -r "$jq_filter" "$infile")
  fi

  if [[ -n "$saved_echo_flag" ]]; then set -x; fi
}


# a function that counts the number of common active-set elements between two PGs
# 1 - the first PG
# 2 - the second PG
# 3 - the dictionary of active sets
function count_common_active {
  local pg1=$1
  local pg2=$2
  local -n pg_acting_dict=$3

  local -a a1=(${pg_acting_dict[$pg1]})
  local -a a2=(${pg_acting_dict[$pg2]})

  local -i cnt=0
  for i in "${a1[@]}"; do
    for j in "${a2[@]}"; do
      if [[ $i -eq $j ]]; then
        cnt=$((cnt+1))
      fi
    done
  done

  printf '%d' "$cnt"
}


# given a PG, find another one with a disjoint active set
# - but allow a possible common Primary
# 1 - the PG
# 2 - the dictionary of active sets
# 3 - [out] - the PG with a disjoint active set
function find_disjoint_but_primary {
  local pg=$1
  local -n ac_dict=$2
  local -n p_dict=$3
  local -n res=$4

  for cand in "${!ac_dict[@]}"; do
    if [[ "$cand" != "$pg" ]]; then
      local -i common=$(count_common_active "$pg" "$cand" ac_dict)
      if [[ $common -eq 0 || ( $common -eq 1 && "${p_dict[$pg]}" == "${p_dict[$cand]}" )]]; then
        res=$cand
        return
      fi
    fi
  done
}


# A debug flag is set for the PG specified, causing the 'pg query' command to display
# an additional 'scrub sessions counter' field.
#
# $1: PG id
#
function set_query_debug() {
  local pgid=$1
  local prim_osd=`ceph pg dump pgs_brief | \
    awk -v pg="^$pgid" -n -e '$0 ~ pg { print(gensub(/[^0-9]*([0-9]+).*/,"\\\\1","g",$5)); }' `
  echo "Setting scrub debug data. Primary for $pgid is $prim_osd"
  CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.$prim_osd) \
        scrubdebug $pgid set sessions
}


# For a set of objects named <basename><i>, where i is a number from 1 to obj_num,
# query the cluster for their PGs, primary OSDs and acting sets.
# The results are stored in the following dictionaries:
# - obj_pgid_dict: object name -> pgid
# - obj_prim_dict: object name -> primary OSD
# - obj_acting_dict: object name -> acting set
function objs_to_prim_dict_fast()
{
  local dir=$1
  local poolname=$2
  local basename=$3
  local obj_num=$4
  local -n obj_pgid_dict=$5
  local -n obj_prim_dict=$6
  local -n obj_acting_dict=$7
  #turn off '-x' (but remember previous state)
  local saved_echo_flag=${-//[^x]/}
  set +x
  local extr_dbg=0

  # Read the Python output and populate the dictionaries
  while IFS=$'\t' read -r obj pgid primary acting; do
    (( extr_dbg >= 3 )) && printf "Processing object: %s, PGID: %s, Primary: %s, Acting: %s\n" \
                                "$obj" "$pgid" "$primary" "$acting"
    if [[ -n "$obj" && -n "$pgid" ]]; then
      obj_pgid_dict["$obj"]=$pgid
      obj_prim_dict["$obj"]=$primary
      obj_acting_dict["$obj"]=$acting
    fi
  done < <(python3 << EOF
import subprocess
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_object_mapping(obj_name, pool_name):
    """Get PG mapping for a single object."""
    try:
        cmd = ['ceph', '--format=json', 'osd', 'map', pool_name, obj_name]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            pgid = data.get('pgid', '')
            primary = data.get('acting_primary', '')
            acting = ' '.join(map(str, data.get('acting', [])))
            return obj_name, pgid, primary, acting
        else:
            return obj_name, '', '', ''
    except Exception:
        return obj_name, '', '', ''

# Parameters from bash variables
pool_name = "$poolname"
base_name = "$basename"
obj_count = $obj_num

# Generate object names
objects = [f"{base_name}{i}" for i in range(1, obj_count + 1)]

# Use ThreadPoolExecutor for parallel execution
max_workers = min(16, len(objects))

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_obj = {
        executor.submit(get_object_mapping, obj, pool_name): obj 
        for obj in objects
    }

    for future in as_completed(future_to_obj):
        obj_name, pgid, primary, acting = future.result()
        if pgid:
            print(f"{obj_name}\t{pgid}\t{primary}\t{acting}")
EOF
)

  if [[ $saved_echo_flag ]]; then
    set -x
  fi
}

# a version of 'objs_to_prim_dict_fast' that does not use Python.
function objs_to_prim_dict()
{
  {
    local dir=$1
    local poolname=$2
    local basename=$3
    local obj_num=$4
    local -n obj_pgid_dict=$5
    local -n obj_prim_dict=$6
    local -n obj_acting_dict=$7

    for i in $(seq 1 $obj_num ); do
        local obj="${basename}${i}"
        IFS=$'\t' read -r pgid primary_osd acting <<<$(ceph --format=json osd map $poolname $obj |\
          jq -r '"\(.pgid)\t\(.acting_primary)\t\(.acting | join(" "))"')
        obj_pgid_dict["$obj"]=$pgid
        obj_prim_dict["$obj"]=$primary_osd
        obj_acting_dict["$obj"]=$acting
    done
  } 2> /dev/null
}

