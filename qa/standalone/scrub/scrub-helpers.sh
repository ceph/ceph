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
  local extr_dbg=1 # note: 3 and above leave some temp files around

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

  from_qry=`ceph pg $1 query -f json-pretty | jq -r --arg extra_dt "$extra_time" --arg current_dt "$current_time"  --arg spt "'" '
    . |
        (.q_stat_part=((.scrubber.schedule// "-") | if test(".*@.*") then (split(" @ ")|first) else . end)) |
        (.q_when_part=((.scrubber.schedule// "0") | if test(".*@.*") then (split(" @ ")|last) else "0" end)) |
	(.q_when_is_future=(.q_when_part > $current_dt)) |
	(.q_vs_date=(.q_when_part > $extra_dt)) |	
      {
        query_epoch: .epoch,
        query_seq: .info.stats.reported_seq,
        query_active: (.scrubber | if has("active") then .active else "bug" end),
        query_schedule: .q_stat_part,
        query_schedule_at: .q_when_part,
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
    local extra_pars=${args['extras']}
    local debug_msg=${args['msg']:-"dbg"}

    # turn off '-x' (but remember previous state)
    local saved_echo_flag=${-//[^x]/}
    set +x

    run_mon $dir a --osd_pool_default_size=$OSDS || return 1
    run_mgr $dir x || return 1

    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd_scrub_interval_randomize_ratio=0 \
            --osd_scrub_backoff_ratio=0.0 \
            --osd_pool_default_pg_autoscale_mode=off \
            --osd_pg_stat_report_interval_max_seconds=1 \
            --osd_pg_stat_report_interval_max_epochs=1 \
            $extra_pars"

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd $(echo $ceph_osd_args) || return 1
    done

    create_pool $poolname $pg_num $pg_num
    wait_for_clean || return 1

    # update the in/out 'args' with the ID of the new pool
    sleep 1
    name_n_id=`ceph osd dump | awk '/^pool.*'$poolname'/ { gsub(/'"'"'/," ",$3); print $3," ", $2}'`
    echo "standard_scrub_cluster: $debug_msg: test pool is $name_n_id"
    args['pool_id']="${name_n_id##* }"
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

