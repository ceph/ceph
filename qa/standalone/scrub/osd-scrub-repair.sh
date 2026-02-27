#!/usr/bin/env bash
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
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
set -x
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/scrub/scrub-helpers.sh

if [ `uname` = FreeBSD ]; then
    # erasure coding overwrites are only tested on Bluestore
    # erasure coding on filestore is unsafe
    # http://docs.ceph.com/en/latest/rados/operations/erasure-code/#erasure-coding-with-overwrites
    use_ec_overwrite=false
else
    use_ec_overwrite=true
fi

# Test development and debugging
# Set to "yes" in order to ignore diff errors and save results to update test
getjson="no"

# Filter out mtime and local_mtime dates, version, prior_version and last_reqid (client) from any object_info.
jqfilter='def walk(f):
  . as $in
  | if type == "object" then
      reduce keys[] as $key
        ( {}; . + { ($key):  ($in[$key] | walk(f)) } ) | f
    elif type == "array" then map( walk(f) ) | f
    else f
    end;
walk(if type == "object" then del(.mtime) else . end)
| walk(if type == "object" then del(.local_mtime) else . end)
| walk(if type == "object" then del(.last_reqid) else . end)
| walk(if type == "object" then del(.version) else . end)
| walk(if type == "object" then del(.prior_version) else . end)'

sortkeys='import json; import sys ; JSON=sys.stdin.read() ; ud = json.loads(JSON) ; print(json.dumps(ud, sort_keys=True, indent=2))'

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7107" # git grep '\<7107\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd-skip-data-digest=false "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function add_something() {
    local dir=$1
    local poolname=$2
    local obj=${3:-SOMETHING}
    local scrub=${4:-noscrub}

    if [ "$scrub" = "noscrub" ];
    then
        ceph osd set noscrub || return 1
        ceph osd set nodeep-scrub || return 1
    else
        ceph osd unset noscrub || return 1
        ceph osd unset nodeep-scrub || return 1
    fi

    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put $obj $dir/ORIGINAL || return 1
}

#
# Corrupt one copy of a replicated pool
#
function TEST_corrupt_and_repair_replicated() {
    local dir=$1
    local poolname=rbd

    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    corrupt_and_repair_one $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
    # Reproduces http://tracker.ceph.com/issues/8914
    corrupt_and_repair_one $dir $poolname $(get_primary $poolname SOMETHING) || return 1
}

#
# Allow operator-initiated scrubs to be scheduled even when some recovering is still
# undergoing on the same OSD
#
function TEST_allow_oper_initiated_scrub_during_recovery() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="2"
        ['pgs_in_pool']="4"
        ['pool_name']="nopool"
        ['pool_default_size']="2"
        ['extras']="--osd_scrub_during_recovery=false \
                    --osd_debug_pretend_recovery_active=true"
    )

    standard_scrub_cluster $dir cluster_conf
    local poolname=rbd
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    oper_scrub_and_schedule $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
}

#
# Allow repair to be scheduled when some recovering is still undergoing on the same OSD
#
function TEST_allow_repair_during_recovery() {
    local dir=$1
    local poolname=rbd

    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 --osd_scrub_during_recovery=false \
                   --osd_debug_pretend_recovery_active=true || return 1
    run_osd $dir 1 --osd_scrub_during_recovery=false \
                   --osd_debug_pretend_recovery_active=true || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    corrupt_and_repair_one $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
}

#
# Skip non-repair scrub correctly during recovery
#
# Note: forgoing the automatic creation of a pool in standard_scrub_cluster as
#       the test requires a specific RBD pool.
function TEST_skip_non_repair_during_recovery() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="2"
        ['pgs_in_pool']="4"
        ['pool_name']="nopool"
        ['pool_default_size']="2"
        ['extras']="--osd_scrub_during_recovery=false --osd_debug_pretend_recovery_active=true"
    )

    standard_scrub_cluster $dir cluster_conf
    local poolname=rbd
    create_rbd_pool || return 1
    wait_for_clean || return 1

    add_something $dir $poolname || return 1
    scrub_and_not_schedule $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
}


function oper_scrub_and_schedule() {
    local dir=$1
    local poolname=$2
    local osd=$3

    #
    # 1) start an operator-initiated scrub
    #
    local pg=$(get_pg $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pg)
    ceph tell $pg scrub

    #
    # 2) Assure the scrub was executed
    #
    sleep 3
    for ((i=0; i < 3; i++)); do
        if test "$(get_last_scrub_stamp $pg)" '>' "$last_scrub" ; then
            break
        fi
        if test "$(get_last_scrub_stamp $pg)" '==' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done

    #
    # 3) Access to the file must OK
    #
    objectstore_tool $dir $osd SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function scrub_and_not_schedule() {
    local dir=$1
    local poolname=$2
    local osd=$3

    #
    # 1) start a non-repair scrub
    #
    local pg=$(get_pg $poolname SOMETHING)
    local last_scrub=$(get_last_scrub_stamp $pg)
    ceph tell $pg schedule-scrub

    #
    # 2) Assure the scrub is not scheduled
    #
    sleep 3
    for ((i=0; i < 3; i++)); do
        if test "$(get_last_scrub_stamp $pg)" '>' "$last_scrub" ; then
            return 1
        fi
        sleep 1
    done

    #
    # 3) Access to the file must OK
    #
    objectstore_tool $dir $osd SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function corrupt_and_repair_two() {
    local dir=$1
    local poolname=$2
    local first=$3
    local second=$4

    #
    # 1) remove the corresponding file from the OSDs
    #
    pids=""
    run_in_background pids objectstore_tool $dir $first SOMETHING remove
    run_in_background pids objectstore_tool $dir $second SOMETHING remove
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi

    #
    # 2) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    repair $pg
    #
    # 3) The files must be back
    #
    pids=""
    run_in_background pids objectstore_tool $dir $first SOMETHING list-attrs
    run_in_background pids objectstore_tool $dir $second SOMETHING list-attrs
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi

    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

#
# 1) add an object
# 2) remove the corresponding object from a designated OSD
# 3) repair the PG
# 4) check that the object has been restored in the designated OSD
#
function corrupt_and_repair_one() {
    local dir=$1
    local poolname=$2
    local osd=$3

    #
    # 1) remove the corresponding file from the OSD
    #
    objectstore_tool $dir $osd SOMETHING remove || return 1
    #
    # 2) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    repair $pg
    #
    # 3) The file must be back
    #
    objectstore_tool $dir $osd SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function corrupt_and_repair_erasure_coded() {
    local dir=$1
    local poolname=$2

    add_something $dir $poolname || return 1

    local primary=$(get_primary $poolname SOMETHING)
    local -a osds=($(get_osds $poolname SOMETHING | sed -e "s/$primary//"))
    local not_primary_first=${osds[0]}
    local not_primary_second=${osds[1]}

    # Reproduces http://tracker.ceph.com/issues/10017
    corrupt_and_repair_one $dir $poolname $primary  || return 1
    # Reproduces http://tracker.ceph.com/issues/10409
    corrupt_and_repair_one $dir $poolname $not_primary_first || return 1
    corrupt_and_repair_two $dir $poolname $not_primary_first $not_primary_second || return 1
    corrupt_and_repair_two $dir $poolname $primary $not_primary_first || return 1

}

function auto_repair_erasure_coded() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
            --osd-deep-scrub-interval=5 \
            --osd-scrub-max-interval=5 \
            --osd-scrub-min-interval=5 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd $dir $id $ceph_osd_args || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    # Create an EC pool
    create_ec_pool $poolname $allow_overwrites k=2 m=1 || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1
    # Wait for auto repair
    local pgid=$(get_pg $poolname SOMETHING)
    wait_for_scrub $pgid "$(get_last_scrub_stamp $pgid)"
    wait_for_clean || return 1
    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

function TEST_auto_repair_erasure_coded_appends() {
    auto_repair_erasure_coded $1 false
}

function TEST_auto_repair_erasure_coded_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        auto_repair_erasure_coded $1 true
    fi
}

# initiate a scrub, then check for the (expected) 'scrubbing' and the
# (not expected until an error was identified) 'repair'
# Arguments: osd#, pg, sleep time
function initiate_and_fetch_state() {
    local the_osd="osd.$1"
    local pgid=$2
    local last_scrub=$(get_last_scrub_stamp $pgid)

    set_config "osd" "$1" "osd_scrub_sleep"  "$3"
    set_config "osd" "$1" "osd_scrub_auto_repair" "true"

    flush_pg_stats
    date  --rfc-3339=ns

    # note: must initiate a "regular" (periodic) deep scrub - not an operator-initiated one
    env CEPH_ARGS= ceph --format json daemon $(get_asok_path $the_osd) schedule-deep-scrub "$pgid"

    # wait for 'scrubbing' to appear
    for ((i=0; i < 80; i++)); do

        st=`ceph pg $pgid query --format json | jq '.state' `
        echo $i ") state now: " $st

        case "$st" in
            *scrubbing*repair* ) echo "found scrub+repair"; return 1;; # PR #41258 should have prevented this
            *scrubbing* ) echo "found scrub"; return 0;;
            *inconsistent* ) echo "Got here too late. Scrub has already finished"; return 1;;
            *recovery* ) echo "Got here too late. Scrub has already finished."; return 1;;
            * ) echo $st;;
        esac

        if [ $((i % 10)) == 4 ]; then
            echo "loop --------> " $i
        fi
    sleep 0.3
    done

    echo "Timeout waiting for deep-scrub of " $pgid " on " $the_osd " to start"
    return 1
}

function wait_end_of_scrub() { # osd# pg
    local the_osd="osd.$1"
    local pgid=$2

    for ((i=0; i < 40; i++)); do
        st=`ceph pg $pgid query --format json | jq '.state' `
        echo "wait-scrub-end state now: " $st
        [[ $st =~ (.*scrubbing.*) ]] || break
        if [ $((i % 5)) == 4 ] ; then
            flush_pg_stats
        fi
        sleep 0.3
    done

    if [[ $st =~ (.*scrubbing.*) ]]
    then
        # a timeout
        return 1
    fi
    return 0
}


function TEST_auto_repair_bluestore_tag() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 3 seconds scrub interval
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    # Set scheduler to "wpq" until there's a reliable way to query scrub states
    # with "--osd-scrub-sleep" set to 0. The "mclock_scheduler" overrides the
    # scrub sleep to 0 and as a result the checks in the test fail.
    local ceph_osd_args="--osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0 \
            --osd-op-queue=wpq"
    for id in $(seq 0 2) ; do
        run_osd $dir $id $ceph_osd_args || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1

    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    echo "Affected PG " $pgid " w/ primary " $primary
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    initiate_and_fetch_state $primary $pgid "3.0"
    r=$?
    echo "initiate_and_fetch_state ret: " $r
    set_config "osd"  "$1"  "osd_scrub_sleep"  "0"
    if [ $r -ne 0 ]; then
        return 1
    fi

    wait_end_of_scrub "$primary" "$pgid" || return 1
    ceph pg dump pgs

    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING get-bytes $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log
}


function TEST_auto_repair_bluestore_basic() {
    local dir=$1
    local -A cluster_conf=(
        ['osds_num']="3" 
        ['pgs_in_pool']="1"
        ['pool_name']="testpool"
        ['extras']=" --osd_scrub_auto_repair=true"
    )
    standard_scrub_cluster $dir cluster_conf
    local poolid=${cluster_conf['pool_id']}
    local poolname=${cluster_conf['pool_name']}

    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1
    ceph tell osd.* config set osd_scrub_auto_repair true

    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    # note: the scrub initiated must be a "regular" (periodic) deep scrub - not an
    # operator-initiated one (as there's no 'auto-repair' for the latter)
    ceph tell $pgid schedule-deep-scrub

    # Wait for auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    wait_for_clean || return 1
    ceph pg dump pgs
    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING get-bytes $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log
}

function TEST_auto_repair_bluestore_scrub() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0 \
            --osd-scrub-backoff-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd $dir $id $ceph_osd_args || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1

    local pgid=$(get_pg $poolname SOMETHING)
    local primary=$(get_primary $poolname SOMETHING)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    ceph tell $pgid schedule-scrub

    # Wait for scrub -> auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    ceph pg dump pgs
    # Actually this causes 2 scrubs, so we better wait a little longer
    sleep 2
    ceph pg dump pgs
    sleep 2
    ceph pg dump pgs
    sleep 5
    wait_for_clean || return 1
    ceph pg dump pgs
    # Verify - the file should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log

    # This should have caused 1 object to be repaired
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "1" || return 1
}

function TEST_auto_repair_bluestore_failed() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd $dir $id $ceph_osd_args || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    for i in $(seq 1 10)
    do
      rados --pool $poolname put obj$i $dir/ORIGINAL || return 1
    done

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj1 remove || return 1
    # obj2 can't be repaired
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj2 remove || return 1
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj2 rm-attr _ || return 1

    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    ceph tell $pgid schedule-deep-scrub

    # Wait for auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    wait_for_clean || return 1
    flush_pg_stats
    grep scrub_finish $dir/osd.${primary}.log
    grep -q "scrub_finish.*still present after re-scrub" $dir/osd.${primary}.log || return 1
    ceph pg dump pgs
    ceph pg dump pgs | grep -q "^${pgid}.*+failed_repair" || return 1

    # Verify - obj1 should be back
    # Restarted osd get $ceph_osd_args passed
    objectstore_tool $dir $(get_not_primary $poolname obj1) obj1 list-attrs || return 1
    rados --pool $poolname get obj1 $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    grep scrub_finish $dir/osd.${primary}.log

    # Make it repairable
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj2 remove || return 1
    repair $pgid
    sleep 2

    flush_pg_stats
    ceph pg dump pgs
    ceph pg dump pgs | grep -q -e "^${pgid}.* active+clean " -e "^${pgid}.* active+clean+wait " || return 1
    grep scrub_finish $dir/osd.${primary}.log
}

function TEST_auto_repair_bluestore_failed_norecov() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
            --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd $dir $id $ceph_osd_args || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    for i in $(seq 1 10)
    do
      rados --pool $poolname put obj$i $dir/ORIGINAL || return 1
    done

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    # obj1 can't be repaired
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj1 remove || return 1
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj1 rm-attr _ || return 1
    # obj2 can't be repaired
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) obj2 remove || return 1
    objectstore_tool $dir $(get_primary $poolname SOMETHING) obj2 rm-attr _ || return 1
    ceph tell osd.* config set osd_scrub_auto_repair true

    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    ceph tell $pgid schedule-deep-scrub

    # Wait for auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    wait_for_clean || return 1
    flush_pg_stats
    grep -q "scrub_finish.*present with no repair possible" $dir/osd.${primary}.log || return 1
    ceph pg dump pgs
    ceph pg dump pgs | grep -q "^${pgid}.*+failed_repair" || return 1
}

function TEST_repair_stats() {
    local dir=$1
    local poolname=testpool
    local OSDS=2
    local OBJS=30
    # This need to be an even number
    local REPAIRS=20

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 $(expr $OSDS - 1)) ; do
        run_osd $dir $id $ceph_osd_args || return 1
    done

    create_pool $poolname 1 1 || return 1
    ceph osd pool set $poolname size 2
    wait_for_clean || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    for i in $(seq 1 $OBJS)
    do
      rados --pool $poolname put obj$i $dir/ORIGINAL || return 1
    done

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    local other=$(get_not_primary $poolname obj1)
    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)

    kill_daemons $dir TERM osd.$other >&2 < /dev/null || return 1
    kill_daemons $dir TERM osd.$primary >&2 < /dev/null || return 1
    for i in $(seq 1 $REPAIRS)
    do
      # Remove from both osd.0 and osd.1
      OSD=$(expr $i % 2)
      _objectstore_tool_nodown $dir $OSD obj$i remove || return 1
    done
    activate_osd $dir $primary $ceph_osd_args || return 1
    activate_osd $dir $other $ceph_osd_args || return 1
    wait_for_clean || return 1

    repair $pgid
    wait_for_clean || return 1
    ceph pg dump pgs
    flush_pg_stats

    # This should have caused 1 object to be repaired
    ceph pg $pgid query | jq '.info.stats.stat_sum'
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "$REPAIRS" || return 1

    ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats[] | select(.osd == $primary )"
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats[] | select(.osd == $primary ).num_shards_repaired")
    test "$COUNT" = "$(expr $REPAIRS / 2)" || return 1

    ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats[] | select(.osd == $other )"
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats[] | select(.osd == $other ).num_shards_repaired")
    test "$COUNT" = "$(expr $REPAIRS / 2)" || return 1

    ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum"
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum.num_shards_repaired")
    test "$COUNT" = "$REPAIRS" || return 1
}

function TEST_repair_stats_ec() {
    local dir=$1
    local poolname=testpool
    local OSDS=3
    local OBJS=30
    # This need to be an even number
    local REPAIRS=26
    local allow_overwrites=false

    # Launch a cluster with 5 seconds scrub interval
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 $(expr $OSDS - 1)) ; do
        run_osd $dir $id $ceph_osd_args || return 1
    done

    # Create an EC pool
    create_ec_pool $poolname $allow_overwrites k=2 m=1 || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    for i in $(seq 1 $OBJS)
    do
      rados --pool $poolname put obj$i $dir/ORIGINAL || return 1
    done

    # Remove the object from one shard physically
    # Restarted osd get $ceph_osd_args passed
    local other=$(get_not_primary $poolname obj1)
    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)

    kill_daemons $dir TERM osd.$other >&2 < /dev/null || return 1
    kill_daemons $dir TERM osd.$primary >&2 < /dev/null || return 1
    for i in $(seq 1 $REPAIRS)
    do
      # Remove from both osd.0 and osd.1
      OSD=$(expr $i % 2)
      _objectstore_tool_nodown $dir $OSD obj$i remove || return 1
    done
    activate_osd $dir $primary $ceph_osd_args || return 1
    activate_osd $dir $other $ceph_osd_args || return 1
    wait_for_clean || return 1

    repair $pgid
    wait_for_clean || return 1
    ceph pg dump pgs
    flush_pg_stats

    # This should have caused 1 object to be repaired
    ceph pg $pgid query | jq '.info.stats.stat_sum'
    COUNT=$(ceph pg $pgid query | jq '.info.stats.stat_sum.num_objects_repaired')
    test "$COUNT" = "$REPAIRS" || return 1

    for osd in $(seq 0 $(expr $OSDS - 1)) ; do
      if [ $osd = $other -o $osd = $primary ]; then
        repair=$(expr $REPAIRS / 2)
      else
        repair="0"
      fi

      ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats[] | select(.osd == $osd )"
      COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats[] | select(.osd == $osd ).num_shards_repaired")
      test "$COUNT" = "$repair" || return 1
    done

    ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum"
    COUNT=$(ceph pg dump --format=json-pretty | jq ".pg_map.osd_stats_sum.num_shards_repaired")
    test "$COUNT" = "$REPAIRS" || return 1
}

function corrupt_and_repair_jerasure() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 3) ; do
	run_osd $dir $id || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_ec_pool $poolname $allow_overwrites k=2 m=2 || return 1
    corrupt_and_repair_erasure_coded $dir $poolname || return 1
}

function TEST_corrupt_and_repair_jerasure_appends() {
    corrupt_and_repair_jerasure $1 false
}

function TEST_corrupt_and_repair_jerasure_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        corrupt_and_repair_jerasure $1 true
    fi
}

function corrupt_and_repair_lrc() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 9) ; do
        run_osd $dir $id || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_ec_pool $poolname $allow_overwrites k=4 m=2 l=3 plugin=lrc || return 1
    corrupt_and_repair_erasure_coded $dir $poolname || return 1
}

function TEST_corrupt_and_repair_lrc_appends() {
    corrupt_and_repair_lrc $1 false
}

function TEST_corrupt_and_repair_lrc_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        corrupt_and_repair_lrc $1 true
    fi
}

function unfound_erasure_coded() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool
    local payload=ABCDEF

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 3) ; do
        run_osd $dir $id || return 1
    done

    create_ec_pool $poolname $allow_overwrites k=2 m=2 || return 1

    add_something $dir $poolname || return 1

    local primary=$(get_primary $poolname SOMETHING)
    local -a osds=($(get_osds $poolname SOMETHING | sed -e "s/$primary//"))
    local not_primary_first=${osds[0]}
    local not_primary_second=${osds[1]}
    local not_primary_third=${osds[2]}

    #
    # 1) remove the corresponding file from the OSDs
    #
    pids=""
    run_in_background pids objectstore_tool $dir $not_primary_first SOMETHING remove
    run_in_background pids objectstore_tool $dir $not_primary_second SOMETHING remove
    run_in_background pids objectstore_tool $dir $not_primary_third SOMETHING remove
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi

    #
    # 2) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    repair $pg
    #
    # 3) check pg state
    #
    # it may take a bit to appear due to mon/mgr asynchrony
    for f in `seq 1 60`; do
	ceph -s | grep "1/1 objects unfound" && break
	sleep 1
    done
    ceph -s|grep "4 up" || return 1
    ceph -s|grep "4 in" || return 1
    ceph -s|grep "1/1 objects unfound" || return 1
}

function TEST_unfound_erasure_coded_appends() {
    unfound_erasure_coded $1 false
}

function TEST_unfound_erasure_coded_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        unfound_erasure_coded $1 true
    fi
}

#
# list_missing for EC pool
#
function list_missing_erasure_coded() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 2) ; do
        run_osd $dir $id || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_ec_pool $poolname $allow_overwrites k=2 m=1 || return 1

    # Put an object and remove the two shards (including primary)
    add_something $dir $poolname MOBJ0 || return 1
    local -a osds0=($(get_osds $poolname MOBJ0))

    # Put another object and remove two shards (excluding primary)
    add_something $dir $poolname MOBJ1 || return 1
    local -a osds1=($(get_osds $poolname MOBJ1))

    # Stop all osd daemons
    for id in $(seq 0 2) ; do
        kill_daemons $dir TERM osd.$id >&2 < /dev/null || return 1
    done

    id=${osds0[0]}
    ceph-objectstore-tool --data-path $dir/$id \
        MOBJ0 remove || return 1
    id=${osds0[1]}
    ceph-objectstore-tool --data-path $dir/$id \
        MOBJ0 remove || return 1

    id=${osds1[1]}
    ceph-objectstore-tool --data-path $dir/$id \
        MOBJ1 remove || return 1
    id=${osds1[2]}
    ceph-objectstore-tool --data-path $dir/$id \
        MOBJ1 remove || return 1

    for id in $(seq 0 2) ; do
        activate_osd $dir $id >&2 || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    # Get get - both objects should in the same PG
    local pg=$(get_pg $poolname MOBJ0)

    # Repair the PG, which triggers the recovering,
    # and should mark the object as unfound
    repair $pg

    for i in $(seq 0 120) ; do
        [ $i -lt 60 ] || return 1
        matches=$(ceph pg $pg list_unfound | egrep "MOBJ0|MOBJ1" | wc -l)
        [ $matches -eq 2 ] && break
    done
}

function TEST_list_missing_erasure_coded_appends() {
    list_missing_erasure_coded $1 false
}

function TEST_list_missing_erasure_coded_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        list_missing_erasure_coded $1 true
    fi
}

#
# Corrupt one copy of a replicated pool
#
function TEST_corrupt_scrub_replicated() {
    local dir=$1
    local poolname=csr_pool
    local total_objs=19

    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_pool foo 1 || return 1
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}
        add_something $dir $poolname $objname || return 1

        rados --pool $poolname setomapheader $objname hdr-$objname || return 1
        rados --pool $poolname setomapval $objname key-$objname val-$objname || return 1
    done

    # Increase file 1 MB + 1KB
    dd if=/dev/zero of=$dir/new.ROBJ19 bs=1024 count=1025
    rados --pool $poolname put $objname $dir/new.ROBJ19 || return 1
    rm -f $dir/new.ROBJ19

    local pg=$(get_pg $poolname ROBJ0)
    local primary=$(get_primary $poolname ROBJ0)

    # Compute an old omap digest and save oi
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) \
        config set osd_deep_scrub_update_digest_min_age 0
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.1) \
        config set osd_deep_scrub_update_digest_min_age 0
    pg_deep_scrub $pg

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}

        # Alternate corruption between osd.0 and osd.1
        local osd=$(expr $i % 2)

        case $i in
        1)
            # Size (deep scrub data_digest too)
            local payload=UVWXYZZZ
            echo $payload > $dir/CORRUPT
            objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1
            ;;

        2)
            # digest (deep scrub only)
            local payload=UVWXYZ
            echo $payload > $dir/CORRUPT
            objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1
            ;;

        3)
             # missing
             objectstore_tool $dir $osd $objname remove || return 1
             ;;

         4)
             # Modify omap value (deep scrub only)
             objectstore_tool $dir $osd $objname set-omap key-$objname $dir/CORRUPT || return 1
             ;;

         5)
            # Delete omap key (deep scrub only)
            objectstore_tool $dir $osd $objname rm-omap key-$objname || return 1
            ;;

         6)
            # Add extra omap key (deep scrub only)
            echo extra > $dir/extra-val
            objectstore_tool $dir $osd $objname set-omap key2-$objname $dir/extra-val || return 1
            rm $dir/extra-val
            ;;

         7)
            # Modify omap header (deep scrub only)
            echo -n newheader > $dir/hdr
            objectstore_tool $dir $osd $objname set-omaphdr $dir/hdr || return 1
            rm $dir/hdr
            ;;

         8)
            rados --pool $poolname setxattr $objname key1-$objname val1-$objname || return 1
            rados --pool $poolname setxattr $objname key2-$objname val2-$objname || return 1

            # Break xattrs
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir $osd $objname set-attr _key1-$objname $dir/bad-val || return 1
            objectstore_tool $dir $osd $objname rm-attr _key2-$objname || return 1
            echo -n val3-$objname > $dir/newval
            objectstore_tool $dir $osd $objname set-attr _key3-$objname $dir/newval || return 1
            rm $dir/bad-val $dir/newval
            ;;

        9)
            objectstore_tool $dir $osd $objname get-attr _ > $dir/robj9-oi
            echo -n D > $dir/change
            rados --pool $poolname put $objname $dir/change
            objectstore_tool $dir $osd $objname set-attr _ $dir/robj9-oi
            rm $dir/oi $dir/change
            ;;

          # ROBJ10 must be handled after digests are re-computed by a deep scrub below
          # ROBJ11 must be handled with config change before deep scrub
          # ROBJ12 must be handled with config change before scrubs
          # ROBJ13 must be handled before scrubs

        14)
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir 0 $objname set-attr _ $dir/bad-val || return 1
            objectstore_tool $dir 1 $objname rm-attr _ || return 1
            rm $dir/bad-val
            ;;

        15)
            objectstore_tool $dir $osd $objname rm-attr _ || return 1
            ;;

        16)
            objectstore_tool $dir 0 $objname rm-attr snapset || return 1
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir 1 $objname set-attr snapset $dir/bad-val || return 1
	    ;;

	17)
	    # Deep-scrub only (all replicas are diffent than the object info
           local payload=ROBJ17
           echo $payload > $dir/new.ROBJ17
	   objectstore_tool $dir 0 $objname set-bytes $dir/new.ROBJ17 || return 1
	   objectstore_tool $dir 1 $objname set-bytes $dir/new.ROBJ17 || return 1
	   ;;

	18)
	    # Deep-scrub only (all replicas are diffent than the object info
           local payload=ROBJ18
           echo $payload > $dir/new.ROBJ18
	   objectstore_tool $dir 0 $objname set-bytes $dir/new.ROBJ18 || return 1
	   objectstore_tool $dir 1 $objname set-bytes $dir/new.ROBJ18 || return 1
	   # Make one replica have a different object info, so a full repair must happen too
	   objectstore_tool $dir $osd $objname corrupt-info || return 1
	   ;;

	19)
	   # Set osd-max-object-size smaller than this object's size

        esac
    done

    local pg=$(get_pg $poolname ROBJ0)

    ceph tell osd.\* injectargs -- --osd-max-object-size=1048576

    inject_eio rep data $poolname ROBJ11 $dir 0 || return 1 # shard 0 of [1, 0], osd.1
    inject_eio rep mdata $poolname ROBJ12 $dir 1 || return 1 # shard 1 of [1, 0], osd.0
    inject_eio rep mdata $poolname ROBJ13 $dir 1 || return 1 # shard 1 of [1, 0], osd.0
    inject_eio rep data $poolname ROBJ13 $dir 0 || return 1 # shard 0 of [1, 0], osd.1

    pg_scrub $pg

    ERRORS=0
    declare -a s_err_strings
    err_strings[0]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:30259878:::ROBJ15:head : candidate had a missing info key"
    err_strings[1]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:33aca486:::ROBJ18:head : object info inconsistent "
    err_strings[2]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:5c7b2c47:::ROBJ16:head : candidate had a corrupt snapset"
    err_strings[3]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:5c7b2c47:::ROBJ16:head : candidate had a missing snapset key"
    err_strings[4]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:5c7b2c47:::ROBJ16:head : failed to pick suitable object info"
    err_strings[5]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:86586531:::ROBJ8:head : attr value mismatch '_key1-ROBJ8', attr name mismatch '_key3-ROBJ8', attr name mismatch '_key2-ROBJ8'"
    err_strings[6]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:bc819597:::ROBJ12:head : candidate had a stat error"
    err_strings[7]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:c0c86b1d:::ROBJ14:head : candidate had a missing info key"
    err_strings[8]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:c0c86b1d:::ROBJ14:head : candidate had a corrupt info"
    err_strings[9]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:c0c86b1d:::ROBJ14:head : failed to pick suitable object info"
    err_strings[10]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:ce3f1d6a:::ROBJ1:head : candidate size 9 info size 7 mismatch"
    err_strings[11]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:ce3f1d6a:::ROBJ1:head : size 9 != size 7 from auth oi 3:ce3f1d6a:::ROBJ1:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 3 dd 2ddbf8f5 od f5fba2c6 alloc_hint [[]0 0 0[]][)], size 9 != size 7 from shard 0"
    err_strings[12]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:d60617f9:::ROBJ13:head : candidate had a stat error"
    err_strings[13]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 3:f2a5b2a4:::ROBJ3:head : missing"
    err_strings[14]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:ffdb2004:::ROBJ9:head : candidate size 1 info size 7 mismatch"
    err_strings[15]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:ffdb2004:::ROBJ9:head : object info inconsistent "
    err_strings[16]="log_channel[(]cluster[)] log [[]ERR[]] : scrub [0-9]*[.]0 3:c0c86b1d:::ROBJ14:head : no '_' attr"
    err_strings[17]="log_channel[(]cluster[)] log [[]ERR[]] : scrub [0-9]*[.]0 3:5c7b2c47:::ROBJ16:head : can't decode 'snapset' attr .* v=3 cannot decode .* Malformed input"
    err_strings[18]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub : stat mismatch, got 19/19 objects, 0/0 clones, 18/19 dirty, 18/19 omap, 0/0 pinned, 0/0 hit_set_archive, 0/0 whiteouts, 1049713/1049720 bytes, 0/0 manifest objects, 0/0 hit_set_archive bytes."
    err_strings[19]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub 1 missing, 8 inconsistent objects"
    err_strings[20]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub 18 errors"
    err_strings[21]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:123a5f55:::ROBJ19:head : size 1049600 > 1048576 is too large"

    for err_string in "${err_strings[@]}"
    do
        if ! grep -q "$err_string" $dir/osd.${primary}.log
        then
            echo "Missing log message '$err_string'"
            ERRORS=$(expr $ERRORS + 1)
        fi
    done

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pg || return 1

    rados list-inconsistent-obj $pg > $dir/json || return 1
    # Get epoch for repair-get requests
    epoch=$(jq .epoch $dir/json)

    jq "$jqfilter" << EOF | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/checkcsjson
{
  "epoch": 181,
  "inconsistents": [
    {
      "object": {
        "name": "ROBJ1",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 3
      },
      "errors": [
        "size_mismatch"
      ],
      "union_shard_errors": [
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ1",
          "key": "",
          "snapid": -2,
          "hash": 1454963827,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'71",
        "prior_version": "26'3",
        "last_reqid": "osd.1.0:71",
        "user_version": 3,
        "size": 7,
        "mtime": "2025-04-28T11:21:52.097147-0500",
        "local_mtime": "2025-04-28T11:21:52.098703-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xf5fba2c6",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 9,
          "object_info": {
            "oid": {
              "oid": "ROBJ1",
              "key": "",
              "snapid": -2,
              "hash": 1454963827,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'71",
            "prior_version": "26'3",
            "last_reqid": "osd.1.0:71",
            "user_version": 3,
            "size": 7,
            "mtime": "2025-04-28T11:21:52.097147-0500",
            "local_mtime": "2025-04-28T11:21:52.098703-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xf5fba2c6",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ12",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 36
      },
      "errors": [],
      "union_shard_errors": [
        "stat_error"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ12",
          "key": "",
          "snapid": -2,
          "hash": 3920199997,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'69",
        "prior_version": "48'36",
        "last_reqid": "osd.1.0:69",
        "user_version": 36,
        "size": 7,
        "mtime": "2025-04-28T11:22:03.003600-0500",
        "local_mtime": "2025-04-28T11:22:03.004994-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x067f306a",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "stat_error"
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ13",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 39
      },
      "errors": [],
      "union_shard_errors": [
        "stat_error"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ13",
          "key": "",
          "snapid": -2,
          "hash": 2682806379,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'72",
        "prior_version": "50'39",
        "last_reqid": "osd.1.0:72",
        "user_version": 39,
        "size": 7,
        "mtime": "2025-04-28T11:22:04.016695-0500",
        "local_mtime": "2025-04-28T11:22:04.018373-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x6441854d",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "stat_error"
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ14",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 0
      },
      "errors": [],
      "union_shard_errors": [
        "info_missing",
        "info_corrupted"
      ],
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "info_corrupted"
          ],
          "size": 7,
          "object_info": "bad-val"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "info_missing"
          ],
          "size": 7
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ15",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 45
      },
      "errors": [],
      "union_shard_errors": [
        "info_missing"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ15",
          "key": "",
          "snapid": -2,
          "hash": 504996876,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'60",
        "prior_version": "54'45",
        "last_reqid": "osd.1.0:60",
        "user_version": 45,
        "size": 7,
        "mtime": "2025-04-28T11:22:06.013439-0500",
        "local_mtime": "2025-04-28T11:22:06.015089-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x2d2a4d6e",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "object_info": {
            "oid": {
              "oid": "ROBJ15",
              "key": "",
              "snapid": -2,
              "hash": 504996876,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'60",
            "prior_version": "54'45",
            "last_reqid": "osd.1.0:60",
            "user_version": 45,
            "size": 7,
            "mtime": "2025-04-28T11:22:06.013439-0500",
            "local_mtime": "2025-04-28T11:22:06.015089-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0x2d2a4d6e",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "info_missing"
          ],
          "size": 7
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ16",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 0
      },
      "errors": [],
      "union_shard_errors": [
        "snapset_missing",
        "snapset_corrupted"
      ],
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "snapset_missing"
          ],
          "size": 7
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "snapset_corrupted"
          ],
          "size": 7,
          "snapset": "bad-val"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ18",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 54
      },
      "errors": [
        "object_info_inconsistency"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ18",
          "key": "",
          "snapid": -2,
          "hash": 1629828556,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'61",
        "prior_version": "60'54",
        "last_reqid": "osd.1.0:61",
        "user_version": 54,
        "size": 7,
        "mtime": "2025-04-28T11:22:09.040482-0500",
        "local_mtime": "2025-04-28T11:22:09.042104-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xddc3680f",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 255,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "object_info": {
            "oid": {
              "oid": "ROBJ18",
              "key": "",
              "snapid": -2,
              "hash": 1629828556,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'61",
            "prior_version": "60'54",
            "last_reqid": "osd.1.0:61",
            "user_version": 54,
            "size": 7,
            "mtime": "2025-04-28T11:22:09.040482-0500",
            "local_mtime": "2025-04-28T11:22:09.042104-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xddc3680f",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7,
          "object_info": {
            "oid": {
              "oid": "ROBJ18",
              "key": "",
              "snapid": -2,
              "hash": 1629828556,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'61",
            "prior_version": "60'54",
            "last_reqid": "osd.1.0:61",
            "user_version": 54,
            "size": 7,
            "mtime": "2025-04-28T11:22:09.040482-0500",
            "local_mtime": "2025-04-28T11:22:09.042104-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xddc3680f",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 255,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ19",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 58
      },
      "errors": [
        "size_too_large"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ19",
          "key": "",
          "snapid": -2,
          "hash": 2868534344,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'59",
        "prior_version": "62'58",
        "last_reqid": "osd.1.0:59",
        "user_version": 58,
        "size": 1049600,
        "mtime": "2025-04-28T11:22:10.100849-0500",
        "local_mtime": "2025-04-28T11:22:10.105095-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x3dde0ef3",
        "omap_digest": "0xbffddd28",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 1049600
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 1049600
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ3",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 9
      },
      "errors": [],
      "union_shard_errors": [
        "missing"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ3",
          "key": "",
          "snapid": -2,
          "hash": 625845583,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'74",
        "prior_version": "30'9",
        "last_reqid": "osd.1.0:74",
        "user_version": 9,
        "size": 7,
        "mtime": "2025-04-28T11:21:54.118266-0500",
        "local_mtime": "2025-04-28T11:21:54.119905-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x00b35dfd",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "missing"
          ]
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ8",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 79
      },
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ8",
          "key": "",
          "snapid": -2,
          "hash": 2359695969,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "101'79",
        "prior_version": "101'78",
        "last_reqid": "client.4653.0:1",
        "user_version": 79,
        "size": 7,
        "mtime": "2025-04-28T11:23:07.031027-0500",
        "local_mtime": "2025-04-28T11:23:07.032484-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xd6be81dc",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "attrs": [
            {
              "name": "key1-ROBJ8",
              "value": "bad-val",
              "Base64": false
            },
            {
              "name": "key2-ROBJ8",
              "value": "val2-ROBJ8",
              "Base64": false
            }
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7,
          "attrs": [
            {
              "name": "key1-ROBJ8",
              "value": "val1-ROBJ8",
              "Base64": false
            },
            {
              "name": "key3-ROBJ8",
              "value": "val3-ROBJ8",
              "Base64": false
            }
          ]
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ9",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 80
      },
      "errors": [
        "object_info_inconsistency"
      ],
      "union_shard_errors": [
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ9",
          "key": "",
          "snapid": -2,
          "hash": 537189375,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "123'80",
        "prior_version": "62'77",
        "last_reqid": "client.4766.0:1",
        "user_version": 80,
        "size": 1,
        "mtime": "2025-04-28T11:23:36.079391-0500",
        "local_mtime": "2025-04-28T11:23:36.081189-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2b63260d",
        "omap_digest": "0x2eecc539",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 1,
          "object_info": {
            "oid": {
              "oid": "ROBJ9",
              "key": "",
              "snapid": -2,
              "hash": 537189375,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "123'80",
            "prior_version": "62'77",
            "last_reqid": "client.4766.0:1",
            "user_version": 80,
            "size": 1,
            "mtime": "2025-04-28T11:23:36.079391-0500",
            "local_mtime": "2025-04-28T11:23:36.081189-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2b63260d",
            "omap_digest": "0x2eecc539",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "obj_size_info_mismatch"
          ],
          "size": 1,
          "object_info": {
            "oid": {
              "oid": "ROBJ9",
              "key": "",
              "snapid": -2,
              "hash": 537189375,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'77",
            "prior_version": "42'27",
            "last_reqid": "osd.1.0:77",
            "user_version": 27,
            "size": 7,
            "mtime": "2025-04-28T11:22:00.142317-0500",
            "local_mtime": "2025-04-28T11:22:00.143807-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0x2eecc539",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        }
      ]
    }
  ]
}

EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/csjson
    multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
    if test $getjson = "yes"
    then
        jq '.' $dir/json > save1.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-obj.json || return 1
    fi

    objname=ROBJ9
    # Change data and size again because digest was recomputed
    echo -n ZZZ > $dir/change
    rados --pool $poolname put $objname $dir/change
    # Set one to an even older value
    objectstore_tool $dir 0 $objname set-attr _ $dir/robj9-oi
    rm $dir/oi $dir/change

    objname=ROBJ10
    objectstore_tool $dir 1 $objname get-attr _ > $dir/oi
    rados --pool $poolname setomapval $objname key2-$objname val2-$objname
    objectstore_tool $dir 0 $objname set-attr _ $dir/oi
    objectstore_tool $dir 1 $objname set-attr _ $dir/oi
    rm $dir/oi

    inject_eio rep data $poolname ROBJ11 $dir 0 || return 1 # shard 0 of [1, 0], osd.1
    inject_eio rep mdata $poolname ROBJ12 $dir 1 || return 1 # shard 1 of [1, 0], osd.0
    inject_eio rep mdata $poolname ROBJ13 $dir 1 || return 1 # shard 1 of [1, 0], osd.0
    inject_eio rep data $poolname ROBJ13 $dir 0 || return 1 # shard 0 of [1, 0], osd.1

    # ROBJ19 won't error this time
    ceph tell osd.\* injectargs -- --osd-max-object-size=134217728

    pg_deep_scrub $pg

    err_strings=()
    err_strings[0]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:30259878:::ROBJ15:head : candidate had a missing info key"
    err_strings[1]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:33aca486:::ROBJ18:head : data_digest 0xbd89c912 != data_digest 0x2ddbf8f5 from auth oi 3:33aca486:::ROBJ18:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 54 dd 2ddbf8f5 od ddc3680f alloc_hint [[]0 0 255[]][)], object info inconsistent "
    err_strings[2]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:33aca486:::ROBJ18:head : data_digest 0xbd89c912 != data_digest 0x2ddbf8f5 from auth oi 3:33aca486:::ROBJ18:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 54 dd 2ddbf8f5 od ddc3680f alloc_hint [[]0 0 255[]][)]"
    err_strings[3]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:33aca486:::ROBJ18:head : failed to pick suitable auth object"
    err_strings[4]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:5c7b2c47:::ROBJ16:head : candidate had a corrupt snapset"
    err_strings[5]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:5c7b2c47:::ROBJ16:head : candidate had a missing snapset key"
    err_strings[6]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:5c7b2c47:::ROBJ16:head : failed to pick suitable object info"
    err_strings[7]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:86586531:::ROBJ8:head : attr value mismatch '_key1-ROBJ8', attr name mismatch '_key3-ROBJ8', attr name mismatch '_key2-ROBJ8'"
    err_strings[8]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:87abbf36:::ROBJ11:head : candidate had a read error"
    err_strings[9]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:8aa5320e:::ROBJ17:head : data_digest 0x5af0c3ef != data_digest 0x2ddbf8f5 from auth oi 3:8aa5320e:::ROBJ17:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 51 dd 2ddbf8f5 od e9572720 alloc_hint [[]0 0 0[]][)]"
    err_strings[10]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:8aa5320e:::ROBJ17:head : data_digest 0x5af0c3ef != data_digest 0x2ddbf8f5 from auth oi 3:8aa5320e:::ROBJ17:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 51 dd 2ddbf8f5 od e9572720 alloc_hint [[]0 0 0[]][)]"
    err_strings[11]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:8aa5320e:::ROBJ17:head : failed to pick suitable auth object"
    err_strings[12]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:8b55fa4b:::ROBJ7:head : omap_digest 0xefced57a != omap_digest 0x6a73cc07 from shard 1"
    err_strings[13]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:8b55fa4b:::ROBJ7:head : omap_digest 0x6a73cc07 != omap_digest 0xefced57a from auth oi 3:8b55fa4b:::ROBJ7:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 21 dd 2ddbf8f5 od efced57a alloc_hint [[]0 0 0[]][)]"
    err_strings[14]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:a53c12e8:::ROBJ6:head : omap_digest 0x689ee887 != omap_digest 0x179c919f from shard 1, omap_digest 0x689ee887 != omap_digest 0x179c919f from auth oi 3:a53c12e8:::ROBJ6:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 18 dd 2ddbf8f5 od 179c919f alloc_hint [[]0 0 0[]][)]"
    err_strings[15]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:b1f19cbd:::ROBJ10:head : omap_digest 0xa8dd5adc != omap_digest 0xc2025a24 from auth oi 3:b1f19cbd:::ROBJ10:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 30 dd 2ddbf8f5 od c2025a24 alloc_hint [[]0 0 0[]][)]"
    err_strings[16]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:b1f19cbd:::ROBJ10:head : omap_digest 0xa8dd5adc != omap_digest 0xc2025a24 from auth oi 3:b1f19cbd:::ROBJ10:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 30 dd 2ddbf8f5 od c2025a24 alloc_hint [[]0 0 0[]][)]"
    err_strings[17]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:b1f19cbd:::ROBJ10:head : failed to pick suitable auth object"
    err_strings[18]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:bc819597:::ROBJ12:head : candidate had a stat error"
    err_strings[19]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:c0c86b1d:::ROBJ14:head : candidate had a missing info key"
    err_strings[20]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:c0c86b1d:::ROBJ14:head : candidate had a corrupt info"
    err_strings[21]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:c0c86b1d:::ROBJ14:head : failed to pick suitable object info"
    err_strings[22]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:ce3f1d6a:::ROBJ1:head : candidate size 9 info size 7 mismatch"
    err_strings[23]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:ce3f1d6a:::ROBJ1:head : data_digest 0x2d4a11c2 != data_digest 0x2ddbf8f5 from shard 0, data_digest 0x2d4a11c2 != data_digest 0x2ddbf8f5 from auth oi 3:ce3f1d6a:::ROBJ1:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 3 dd 2ddbf8f5 od f5fba2c6 alloc_hint [[]0 0 0[]][)], size 9 != size 7 from auth oi 3:ce3f1d6a:::ROBJ1:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 3 dd 2ddbf8f5 od f5fba2c6 alloc_hint [[]0 0 0[]][)], size 9 != size 7 from shard 0"
    err_strings[24]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:d60617f9:::ROBJ13:head : candidate had a read error"
    err_strings[25]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:d60617f9:::ROBJ13:head : candidate had a stat error"
    err_strings[26]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:d60617f9:::ROBJ13:head : failed to pick suitable object info"
    err_strings[27]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:e97ce31e:::ROBJ2:head : data_digest 0x578a4830 != data_digest 0x2ddbf8f5 from shard 1, data_digest 0x578a4830 != data_digest 0x2ddbf8f5 from auth oi 3:e97ce31e:::ROBJ2:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 6 dd 2ddbf8f5 od f8e11918 alloc_hint [[]0 0 0[]][)]"
    err_strings[28]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 3:f2a5b2a4:::ROBJ3:head : missing"
    err_strings[29]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:f4981d31:::ROBJ4:head : omap_digest 0xd7178dfe != omap_digest 0xe2d46ea4 from shard 1, omap_digest 0xd7178dfe != omap_digest 0xe2d46ea4 from auth oi 3:f4981d31:::ROBJ4:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 12 dd 2ddbf8f5 od e2d46ea4 alloc_hint [[]0 0 0[]][)]"
    err_strings[30]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid 3:f4bfd4d1:::ROBJ5:head : omap_digest 0x1a862a41 != omap_digest 0x6cac8f6 from shard 1"
    err_strings[31]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:f4bfd4d1:::ROBJ5:head : omap_digest 0x6cac8f6 != omap_digest 0x1a862a41 from auth oi 3:f4bfd4d1:::ROBJ5:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 15 dd 2ddbf8f5 od 1a862a41 alloc_hint [[]0 0 0[]][)]"
    err_strings[32]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:ffdb2004:::ROBJ9:head : candidate size 3 info size 7 mismatch"
    err_strings[33]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 0 soid 3:ffdb2004:::ROBJ9:head : object info inconsistent "
    err_strings[34]="log_channel[(]cluster[)] log [[]ERR[]] : deep-scrub [0-9]*[.]0 3:c0c86b1d:::ROBJ14:head : no '_' attr"
    err_strings[35]="log_channel[(]cluster[)] log [[]ERR[]] : deep-scrub [0-9]*[.]0 3:5c7b2c47:::ROBJ16:head : can't decode 'snapset' attr .* v=3 cannot decode .* Malformed input"
    err_strings[36]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 deep-scrub : stat mismatch, got 19/19 objects, 0/0 clones, 18/19 dirty, 18/19 omap, 0/0 pinned, 0/0 hit_set_archive, 0/0 whiteouts, 1049715/1049716 bytes, 0/0 manifest objects, 0/0 hit_set_archive bytes."
    err_strings[37]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 deep-scrub 1 missing, 11 inconsistent objects"
    err_strings[38]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 deep-scrub 35 errors"

    for err_string in "${err_strings[@]}"
    do
        if ! grep -q "$err_string" $dir/osd.${primary}.log
        then
            echo "Missing log message '$err_string'"
            ERRORS=$(expr $ERRORS + 1)
        fi
    done

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pg || return 1

    rados list-inconsistent-obj $pg > $dir/json || return 1
    # Get epoch for repair-get requests
    epoch=$(jq .epoch $dir/json)

    jq "$jqfilter" << EOF | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/checkcsjson
{
  "epoch": 203,
  "inconsistents": [
    {
      "object": {
        "name": "ROBJ1",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 3
      },
      "errors": [
        "data_digest_mismatch",
        "size_mismatch"
      ],
      "union_shard_errors": [
        "data_digest_mismatch_info",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ1",
          "key": "",
          "snapid": -2,
          "hash": 1454963827,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'71",
        "prior_version": "26'3",
        "last_reqid": "osd.1.0:71",
        "user_version": 3,
        "size": 7,
        "mtime": "2025-04-28T11:21:52.097147-0500",
        "local_mtime": "2025-04-28T11:21:52.098703-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xf5fba2c6",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "omap_digest": "0xf5fba2c6",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "data_digest_mismatch_info",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 9,
          "omap_digest": "0xf5fba2c6",
          "data_digest": "0x2d4a11c2",
          "object_info": {
            "oid": {
              "oid": "ROBJ1",
              "key": "",
              "snapid": -2,
              "hash": 1454963827,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'71",
            "prior_version": "26'3",
            "last_reqid": "osd.1.0:71",
            "user_version": 3,
            "size": 7,
            "mtime": "2025-04-28T11:21:52.097147-0500",
            "local_mtime": "2025-04-28T11:21:52.098703-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xf5fba2c6",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ10",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 30
      },
      "errors": [],
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ10",
          "key": "",
          "snapid": -2,
          "hash": 3174666125,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'68",
        "prior_version": "44'30",
        "last_reqid": "osd.1.0:68",
        "user_version": 30,
        "size": 7,
        "mtime": "2025-04-28T11:22:01.109390-0500",
        "local_mtime": "2025-04-28T11:22:01.110932-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xc2025a24",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xa8dd5adc",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xa8dd5adc",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ11",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 33
      },
      "errors": [],
      "union_shard_errors": [
        "read_error"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ11",
          "key": "",
          "snapid": -2,
          "hash": 1828574689,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'64",
        "prior_version": "46'33",
        "last_reqid": "osd.1.0:64",
        "user_version": 33,
        "size": 7,
        "mtime": "2025-04-28T11:22:02.079779-0500",
        "local_mtime": "2025-04-28T11:22:02.081442-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xa03cef03",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "omap_digest": "0xa03cef03",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "read_error"
          ],
          "size": 7
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ12",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 36
      },
      "errors": [],
      "union_shard_errors": [
        "stat_error"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ12",
          "key": "",
          "snapid": -2,
          "hash": 3920199997,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'69",
        "prior_version": "48'36",
        "last_reqid": "osd.1.0:69",
        "user_version": 36,
        "size": 7,
        "mtime": "2025-04-28T11:22:03.003600-0500",
        "local_mtime": "2025-04-28T11:22:03.004994-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x067f306a",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "stat_error"
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7,
          "omap_digest": "0x067f306a",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ13",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 0
      },
      "errors": [],
      "union_shard_errors": [
        "stat_error",
        "read_error"
      ],
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "stat_error"
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "read_error"
          ],
          "size": 7
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ14",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 0
      },
      "errors": [],
      "union_shard_errors": [
        "info_missing",
        "info_corrupted"
      ],
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "info_corrupted"
          ],
          "size": 7,
          "omap_digest": "0x4f14f849",
          "data_digest": "0x2ddbf8f5",
          "object_info": "bad-val"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "info_missing"
          ],
          "size": 7,
          "omap_digest": "0x4f14f849",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ15",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 45
      },
      "errors": [],
      "union_shard_errors": [
        "info_missing"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ15",
          "key": "",
          "snapid": -2,
          "hash": 504996876,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'60",
        "prior_version": "54'45",
        "last_reqid": "osd.1.0:60",
        "user_version": 45,
        "size": 7,
        "mtime": "2025-04-28T11:22:06.013439-0500",
        "local_mtime": "2025-04-28T11:22:06.015089-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x2d2a4d6e",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "omap_digest": "0x2d2a4d6e",
          "data_digest": "0x2ddbf8f5",
          "object_info": {
            "oid": {
              "oid": "ROBJ15",
              "key": "",
              "snapid": -2,
              "hash": 504996876,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'60",
            "prior_version": "54'45",
            "last_reqid": "osd.1.0:60",
            "user_version": 45,
            "size": 7,
            "mtime": "2025-04-28T11:22:06.013439-0500",
            "local_mtime": "2025-04-28T11:22:06.015089-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0x2d2a4d6e",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "info_missing"
          ],
          "size": 7,
          "omap_digest": "0x2d2a4d6e",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ16",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 0
      },
      "errors": [],
      "union_shard_errors": [
        "snapset_missing",
        "snapset_corrupted"
      ],
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "snapset_missing"
          ],
          "size": 7,
          "omap_digest": "0x8b699207",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "snapset_corrupted"
          ],
          "size": 7,
          "omap_digest": "0x8b699207",
          "data_digest": "0x2ddbf8f5",
          "snapset": "bad-val"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ17",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 51
      },
      "errors": [],
      "union_shard_errors": [
        "data_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ17",
          "key": "",
          "snapid": -2,
          "hash": 1884071249,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'65",
        "prior_version": "58'51",
        "last_reqid": "osd.1.0:65",
        "user_version": 51,
        "size": 7,
        "mtime": "2025-04-28T11:22:08.037672-0500",
        "local_mtime": "2025-04-28T11:22:08.039267-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xe9572720",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "data_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xe9572720",
          "data_digest": "0x5af0c3ef"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "data_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xe9572720",
          "data_digest": "0x5af0c3ef"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ18",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 54
      },
      "errors": [
        "object_info_inconsistency"
      ],
      "union_shard_errors": [
        "data_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ18",
          "key": "",
          "snapid": -2,
          "hash": 1629828556,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'61",
        "prior_version": "60'54",
        "last_reqid": "osd.1.0:61",
        "user_version": 54,
        "size": 7,
        "mtime": "2025-04-28T11:22:09.040482-0500",
        "local_mtime": "2025-04-28T11:22:09.042104-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xddc3680f",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 255,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "data_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xddc3680f",
          "data_digest": "0xbd89c912",
          "object_info": {
            "oid": {
              "oid": "ROBJ18",
              "key": "",
              "snapid": -2,
              "hash": 1629828556,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'61",
            "prior_version": "60'54",
            "last_reqid": "osd.1.0:61",
            "user_version": 54,
            "size": 7,
            "mtime": "2025-04-28T11:22:09.040482-0500",
            "local_mtime": "2025-04-28T11:22:09.042104-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xddc3680f",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "data_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xddc3680f",
          "data_digest": "0xbd89c912",
          "object_info": {
            "oid": {
              "oid": "ROBJ18",
              "key": "",
              "snapid": -2,
              "hash": 1629828556,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'61",
            "prior_version": "60'54",
            "last_reqid": "osd.1.0:61",
            "user_version": 54,
            "size": 7,
            "mtime": "2025-04-28T11:22:09.040482-0500",
            "local_mtime": "2025-04-28T11:22:09.042104-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xddc3680f",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 255,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ2",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 6
      },
      "errors": [
        "data_digest_mismatch"
      ],
      "union_shard_errors": [
        "data_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ2",
          "key": "",
          "snapid": -2,
          "hash": 2026323607,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'73",
        "prior_version": "28'6",
        "last_reqid": "osd.1.0:73",
        "user_version": 6,
        "size": 7,
        "mtime": "2025-04-28T11:21:53.103855-0500",
        "local_mtime": "2025-04-28T11:21:53.105331-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xf8e11918",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "data_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xf8e11918",
          "data_digest": "0x578a4830"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7,
          "omap_digest": "0xf8e11918",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ3",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 9
      },
      "errors": [],
      "union_shard_errors": [
        "missing"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ3",
          "key": "",
          "snapid": -2,
          "hash": 625845583,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'74",
        "prior_version": "30'9",
        "last_reqid": "osd.1.0:74",
        "user_version": 9,
        "size": 7,
        "mtime": "2025-04-28T11:21:54.118266-0500",
        "local_mtime": "2025-04-28T11:21:54.119905-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x00b35dfd",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "omap_digest": "0x00b35dfd",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "missing"
          ]
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ4",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 12
      },
      "errors": [
        "omap_digest_mismatch"
      ],
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ4",
          "key": "",
          "snapid": -2,
          "hash": 2360875311,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'75",
        "prior_version": "32'12",
        "last_reqid": "osd.1.0:75",
        "user_version": 12,
        "size": 7,
        "mtime": "2025-04-28T11:21:55.129051-0500",
        "local_mtime": "2025-04-28T11:21:55.130403-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xe2d46ea4",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0xd7178dfe",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7,
          "omap_digest": "0xe2d46ea4",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ5",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 15
      },
      "errors": [
        "omap_digest_mismatch"
      ],
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ5",
          "key": "",
          "snapid": -2,
          "hash": 2334915887,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'76",
        "prior_version": "34'15",
        "last_reqid": "osd.1.0:76",
        "user_version": 15,
        "size": 7,
        "mtime": "2025-04-28T11:21:56.144860-0500",
        "local_mtime": "2025-04-28T11:21:56.146359-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x1a862a41",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "omap_digest": "0x1a862a41",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0x06cac8f6",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ6",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 18
      },
      "errors": [
        "omap_digest_mismatch"
      ],
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ6",
          "key": "",
          "snapid": -2,
          "hash": 390610085,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'67",
        "prior_version": "36'18",
        "last_reqid": "osd.1.0:67",
        "user_version": 18,
        "size": 7,
        "mtime": "2025-04-28T11:21:57.145050-0500",
        "local_mtime": "2025-04-28T11:21:57.146554-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0x179c919f",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0x689ee887",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7,
          "omap_digest": "0x179c919f",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ7",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 21
      },
      "errors": [
        "omap_digest_mismatch"
      ],
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ7",
          "key": "",
          "snapid": -2,
          "hash": 3529485009,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "62'66",
        "prior_version": "38'21",
        "last_reqid": "osd.1.0:66",
        "user_version": 21,
        "size": 7,
        "mtime": "2025-04-28T11:21:58.114118-0500",
        "local_mtime": "2025-04-28T11:21:58.115639-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xefced57a",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "omap_digest": "0xefced57a",
          "data_digest": "0x2ddbf8f5"
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "size": 7,
          "omap_digest": "0x6a73cc07",
          "data_digest": "0x2ddbf8f5"
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ8",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 79
      },
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ8",
          "key": "",
          "snapid": -2,
          "hash": 2359695969,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "101'79",
        "prior_version": "101'78",
        "last_reqid": "client.4653.0:1",
        "user_version": 79,
        "size": 7,
        "mtime": "2025-04-28T11:23:07.031027-0500",
        "local_mtime": "2025-04-28T11:23:07.032484-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xd6be81dc",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 7,
          "omap_digest": "0xd6be81dc",
          "data_digest": "0x2ddbf8f5",
          "attrs": [
            {
              "name": "key1-ROBJ8",
              "value": "bad-val",
              "Base64": false
            },
            {
              "name": "key2-ROBJ8",
              "value": "val2-ROBJ8",
              "Base64": false
            }
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 7,
          "omap_digest": "0xd6be81dc",
          "data_digest": "0x2ddbf8f5",
          "attrs": [
            {
              "name": "key1-ROBJ8",
              "value": "val1-ROBJ8",
              "Base64": false
            },
            {
              "name": "key3-ROBJ8",
              "value": "val3-ROBJ8",
              "Base64": false
            }
          ]
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ9",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 81
      },
      "errors": [
        "object_info_inconsistency"
      ],
      "union_shard_errors": [
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ9",
          "key": "",
          "snapid": -2,
          "hash": 537189375,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "183'81",
        "prior_version": "62'77",
        "last_reqid": "client.5112.0:1",
        "user_version": 81,
        "size": 3,
        "mtime": "2025-04-28T11:25:06.229749-0500",
        "local_mtime": "2025-04-28T11:25:06.231242-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x1f26fb26",
        "omap_digest": "0x2eecc539",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [
            "obj_size_info_mismatch"
          ],
          "size": 3,
          "omap_digest": "0x2eecc539",
          "data_digest": "0x1f26fb26",
          "object_info": {
            "oid": {
              "oid": "ROBJ9",
              "key": "",
              "snapid": -2,
              "hash": 537189375,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "62'77",
            "prior_version": "42'27",
            "last_reqid": "osd.1.0:77",
            "user_version": 27,
            "size": 7,
            "mtime": "2025-04-28T11:22:00.142317-0500",
            "local_mtime": "2025-04-28T11:22:00.143807-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0x2eecc539",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 3,
          "omap_digest": "0x2eecc539",
          "data_digest": "0x1f26fb26",
          "object_info": {
            "oid": {
              "oid": "ROBJ9",
              "key": "",
              "snapid": -2,
              "hash": 537189375,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "183'81",
            "prior_version": "62'77",
            "last_reqid": "client.5112.0:1",
            "user_version": 81,
            "size": 3,
            "mtime": "2025-04-28T11:25:06.229749-0500",
            "local_mtime": "2025-04-28T11:25:06.231242-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "omap",
              "data_digest",
              "omap_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x1f26fb26",
            "omap_digest": "0x2eecc539",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        }
      ]
    }
  ]
}

EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/csjson
    multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
    if test $getjson = "yes"
    then
        jq '.' $dir/json > save2.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-obj.json || return 1
    fi

    repair $pg
    wait_for_clean

    # This hangs if the repair doesn't work
    timeout 30 rados -p $poolname get ROBJ17 $dir/robj17.out || return 1
    timeout 30 rados -p $poolname get ROBJ18 $dir/robj18.out || return 1
    # Even though we couldn't repair all of the introduced errors, we can fix ROBJ17
    diff -q $dir/new.ROBJ17 $dir/robj17.out || return 1
    rm -f $dir/new.ROBJ17 $dir/robj17.out || return 1
    diff -q $dir/new.ROBJ18 $dir/robj18.out || return 1
    rm -f $dir/new.ROBJ18 $dir/robj18.out || return 1

    if [ $ERRORS != "0" ];
    then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    ceph osd pool rm $poolname $poolname --yes-i-really-really-mean-it
}


#
# Test scrub errors for an erasure coded pool
#
function corrupt_scrub_erasure() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool
    local total_objs=7

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 2) ; do
        run_osd $dir $id || return 1
    done
    create_rbd_pool || return 1
    create_pool foo 1

    create_ec_pool $poolname $allow_overwrites k=2 m=1 stripe_unit=2K --force || return 1
    wait_for_clean || return 1

    for i in $(seq 1 $total_objs) ; do
        objname=EOBJ${i}
        add_something $dir $poolname $objname || return 1

        local osd=$(expr $i % 2)

        case $i in
        1)
            # Size (deep scrub data_digest too)
            local payload=UVWXYZZZ
            echo $payload > $dir/CORRUPT
            objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1
            ;;

        2)
            # Corrupt EC shard
            dd if=/dev/urandom of=$dir/CORRUPT bs=2048 count=1
            objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1
            ;;

        3)
             # missing
             objectstore_tool $dir $osd $objname remove || return 1
             ;;

        4)
            rados --pool $poolname setxattr $objname key1-$objname val1-$objname || return 1
            rados --pool $poolname setxattr $objname key2-$objname val2-$objname || return 1

            # Break xattrs
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir $osd $objname set-attr _key1-$objname $dir/bad-val || return 1
            objectstore_tool $dir $osd $objname rm-attr _key2-$objname || return 1
            echo -n val3-$objname > $dir/newval
            objectstore_tool $dir $osd $objname set-attr _key3-$objname $dir/newval || return 1
            rm $dir/bad-val $dir/newval
            ;;

        5)
            # Corrupt EC shard
            dd if=/dev/urandom of=$dir/CORRUPT bs=2048 count=2
            objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1
            ;;

        6)
            objectstore_tool $dir 0 $objname rm-attr hinfo_key || return 1
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir 1 $objname set-attr hinfo_key $dir/bad-val || return 1
            ;;

        7)
            local payload=MAKETHISDIFFERENTFROMOTHEROBJECTS
            echo $payload > $dir/DIFFERENT
            rados --pool $poolname put $objname $dir/DIFFERENT || return 1

            # Get hinfo_key from EOBJ1
            objectstore_tool $dir 0 EOBJ1 get-attr hinfo_key > $dir/hinfo
            objectstore_tool $dir 0 $objname set-attr hinfo_key $dir/hinfo || return 1
            rm -f $dir/hinfo
            ;;

        esac
    done

    local pg=$(get_pg $poolname EOBJ0)

    pg_scrub $pg

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pg || return 1

    rados list-inconsistent-obj $pg > $dir/json || return 1
    # Get epoch for repair-get requests
    epoch=$(jq .epoch $dir/json)

    jq "$jqfilter" << EOF | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/checkcsjson
{
  "epoch": 99,
  "inconsistents": [
    {
      "object": {
        "name": "EOBJ1",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 1
      },
      "errors": [
        "size_mismatch"
      ],
      "union_shard_errors": [
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ1",
          "key": "",
          "snapid": -2,
          "hash": 560836233,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "32'1",
        "prior_version": "0'0",
        "last_reqid": "client.4210.0:1",
        "user_version": 1,
        "size": 7,
        "mtime": "2025-04-28T02:18:19.605985-0500",
        "local_mtime": "2025-04-28T02:18:19.607916-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 9,
          "object_info": {
            "oid": {
              "oid": "EOBJ1",
              "key": "",
              "snapid": -2,
              "hash": 560836233,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "32'1",
            "prior_version": "0'0",
            "last_reqid": "client.4210.0:1",
            "user_version": 1,
            "size": 7,
            "mtime": "2025-04-28T02:18:19.605985-0500",
            "local_mtime": "2025-04-28T02:18:19.607916-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ3",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 3
      },
      "errors": [],
      "union_shard_errors": [
        "missing"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ3",
          "key": "",
          "snapid": -2,
          "hash": 3125668237,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "46'3",
        "prior_version": "0'0",
        "last_reqid": "client.4286.0:1",
        "user_version": 3,
        "size": 7,
        "mtime": "2025-04-28T02:18:36.149933-0500",
        "local_mtime": "2025-04-28T02:18:36.155056-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "missing"
          ]
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ4",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 6
      },
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ4",
          "key": "",
          "snapid": -2,
          "hash": 1618759290,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "54'6",
        "prior_version": "54'5",
        "last_reqid": "client.4330.0:1",
        "user_version": 6,
        "size": 7,
        "mtime": "2025-04-28T02:18:44.594720-0500",
        "local_mtime": "2025-04-28T02:18:44.596235-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "bad-val",
              "Base64": false
            },
            {
              "name": "key2-EOBJ4",
              "value": "val2-EOBJ4",
              "Base64": false
            }
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "val1-EOBJ4",
              "Base64": false
            },
            {
              "name": "key2-EOBJ4",
              "value": "val2-EOBJ4",
              "Base64": false
            }
          ]
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "val1-EOBJ4",
              "Base64": false
            },
            {
              "name": "key3-EOBJ4",
              "value": "val3-EOBJ4",
              "Base64": false
            }
          ]
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ5",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 7
      },
      "errors": [
        "size_mismatch"
      ],
      "union_shard_errors": [
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ5",
          "key": "",
          "snapid": -2,
          "hash": 2918945441,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "71'7",
        "prior_version": "0'0",
        "last_reqid": "client.4424.0:1",
        "user_version": 7,
        "size": 7,
        "mtime": "2025-04-28T02:19:03.683994-0500",
        "local_mtime": "2025-04-28T02:19:03.685505-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 4096,
          "object_info": {
            "oid": {
              "oid": "EOBJ5",
              "key": "",
              "snapid": -2,
              "hash": 2918945441,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "71'7",
            "prior_version": "0'0",
            "last_reqid": "client.4424.0:1",
            "user_version": 7,
            "size": 7,
            "mtime": "2025-04-28T02:19:03.683994-0500",
            "local_mtime": "2025-04-28T02:19:03.685505-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ6",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 8
      },
      "errors": [],
      "union_shard_errors": [
        "hinfo_missing",
        "hinfo_corrupted"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ6",
          "key": "",
          "snapid": -2,
          "hash": 3050890866,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "78'8",
        "prior_version": "0'0",
        "last_reqid": "client.4462.0:1",
        "user_version": 8,
        "size": 7,
        "mtime": "2025-04-28T02:19:11.980798-0500",
        "local_mtime": "2025-04-28T02:19:11.986446-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [
            "hinfo_missing"
          ],
          "size": 2048
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "hinfo_corrupted"
          ],
          "size": 2048,
          "hashinfo": "bad-val"
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 80717615
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 80717615
              }
            ]
          }
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ7",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 10
      },
      "errors": [
        "hinfo_inconsistency"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ7",
          "key": "",
          "snapid": -2,
          "hash": 3258066308,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "90'10",
        "prior_version": "90'9",
        "last_reqid": "client.4534.0:1",
        "user_version": 10,
        "size": 34,
        "mtime": "2025-04-28T02:19:27.775012-0500",
        "local_mtime": "2025-04-28T02:19:27.776394-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x136e4e27",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 80717615
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 80717615
              }
            ]
          }
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 1534350760
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 1534350760
              }
            ]
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 1534350760
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 1534350760
              }
            ]
          }
        }
      ]
    }
  ]
}
EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/csjson
    multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
    if test $getjson = "yes"
    then
        jq '.' $dir/json > save3.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-obj.json || return 1
    fi

    pg_deep_scrub $pg

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pg || return 1

    rados list-inconsistent-obj $pg > $dir/json || return 1
    cp $dir/json /tmp/rrr6.json
    # Get epoch for repair-get requests
    epoch=$(jq .epoch $dir/json)

    if [ "$allow_overwrites" = "true" ]
    then
      jq "$jqfilter" << EOF | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/checkcsjson
{
  "epoch": 99,
  "inconsistents": [
    {
      "object": {
        "name": "EOBJ1",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 1
      },
      "errors": [
        "size_mismatch"
      ],
      "union_shard_errors": [
        "read_error",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ1",
          "key": "",
          "snapid": -2,
          "hash": 560836233,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "33'1",
        "prior_version": "0'0",
        "last_reqid": "client.4212.0:1",
        "user_version": 1,
        "size": 7,
        "mtime": "2025-04-28T05:22:43.385320-0500",
        "local_mtime": "2025-04-28T05:22:43.387170-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000"
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "read_error",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 9,
          "object_info": {
            "oid": {
              "oid": "EOBJ1",
              "key": "",
              "snapid": -2,
              "hash": 560836233,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "33'1",
            "prior_version": "0'0",
            "last_reqid": "client.4212.0:1",
            "user_version": 1,
            "size": 7,
            "mtime": "2025-04-28T05:22:43.385320-0500",
            "local_mtime": "2025-04-28T05:22:43.387170-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000"
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ3",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 3
      },
      "errors": [],
      "union_shard_errors": [
        "missing"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ3",
          "key": "",
          "snapid": -2,
          "hash": 3125668237,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "47'3",
        "prior_version": "0'0",
        "last_reqid": "client.4288.0:1",
        "user_version": 3,
        "size": 7,
        "mtime": "2025-04-28T05:22:59.744067-0500",
        "local_mtime": "2025-04-28T05:22:59.748636-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000"
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "missing"
          ]
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000"
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ4",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 6
      },
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ4",
          "key": "",
          "snapid": -2,
          "hash": 1618759290,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "54'6",
        "prior_version": "54'5",
        "last_reqid": "client.4332.0:1",
        "user_version": 6,
        "size": 7,
        "mtime": "2025-04-28T05:23:07.959787-0500",
        "local_mtime": "2025-04-28T05:23:07.961221-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000",
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "bad-val",
              "Base64": false
            },
            {
              "name": "key2-EOBJ4",
              "value": "val2-EOBJ4",
              "Base64": false
            }
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000",
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "val1-EOBJ4",
              "Base64": false
            },
            {
              "name": "key2-EOBJ4",
              "value": "val2-EOBJ4",
              "Base64": false
            }
          ]
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000",
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "val1-EOBJ4",
              "Base64": false
            },
            {
              "name": "key3-EOBJ4",
              "value": "val3-EOBJ4",
              "Base64": false
            }
          ]
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ5",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 7
      },
      "errors": [
        "size_mismatch"
      ],
      "union_shard_errors": [
        "read_error",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ5",
          "key": "",
          "snapid": -2,
          "hash": 2918945441,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "71'7",
        "prior_version": "0'0",
        "last_reqid": "client.4432.0:1",
        "user_version": 7,
        "size": 7,
        "mtime": "2025-04-28T05:23:31.119695-0500",
        "local_mtime": "2025-04-28T05:23:31.121349-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000"
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "read_error",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 4096,
          "object_info": {
            "oid": {
              "oid": "EOBJ5",
              "key": "",
              "snapid": -2,
              "hash": 2918945441,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "71'7",
            "prior_version": "0'0",
            "last_reqid": "client.4432.0:1",
            "user_version": 7,
            "size": 7,
            "mtime": "2025-04-28T05:23:31.119695-0500",
            "local_mtime": "2025-04-28T05:23:31.121349-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000"
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ6",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 8
      },
      "errors": [],
      "union_shard_errors": [
        "read_error",
        "hinfo_missing",
        "hinfo_corrupted"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ6",
          "key": "",
          "snapid": -2,
          "hash": 3050890866,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "78'8",
        "prior_version": "0'0",
        "last_reqid": "client.4470.0:1",
        "user_version": 8,
        "size": 7,
        "mtime": "2025-04-28T05:23:39.428058-0500",
        "local_mtime": "2025-04-28T05:23:39.433754-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [
            "read_error",
            "hinfo_missing"
          ],
          "size": 2048
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "read_error",
            "hinfo_corrupted"
          ],
          "size": 2048,
          "hashinfo": "bad-val"
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000",
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 80717615
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 80717615
              }
            ]
          }
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ7",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 10
      },
      "errors": [
        "hinfo_inconsistency"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ7",
          "key": "",
          "snapid": -2,
          "hash": 3258066308,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "90'10",
        "prior_version": "90'9",
        "last_reqid": "client.4542.0:1",
        "user_version": 10,
        "size": 34,
        "mtime": "2025-04-28T05:23:55.240961-0500",
        "local_mtime": "2025-04-28T05:23:55.242367-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x136e4e27",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000",
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 80717615
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 80717615
              }
            ]
          }
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000",
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 1534350760
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 1534350760
              }
            ]
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x00000000",
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 1534350760
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 1534350760
              }
            ]
          }
        }
      ]
    }
  ]
}
EOF

    else

      jq "$jqfilter" << EOF | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/checkcsjson
{
  "epoch": 99,
  "inconsistents": [
    {
      "object": {
        "name": "EOBJ1",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 1
      },
      "errors": [
        "size_mismatch"
      ],
      "union_shard_errors": [
        "read_error",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ1",
          "key": "",
          "snapid": -2,
          "hash": 560836233,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "32'1",
        "prior_version": "0'0",
        "last_reqid": "client.4210.0:1",
        "user_version": 1,
        "size": 7,
        "mtime": "2025-04-28T02:18:19.605985-0500",
        "local_mtime": "2025-04-28T02:18:19.607916-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "read_error",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 9,
          "object_info": {
            "oid": {
              "oid": "EOBJ1",
              "key": "",
              "snapid": -2,
              "hash": 560836233,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "32'1",
            "prior_version": "0'0",
            "last_reqid": "client.4210.0:1",
            "user_version": 1,
            "size": 7,
            "mtime": "2025-04-28T02:18:19.605985-0500",
            "local_mtime": "2025-04-28T02:18:19.607916-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ2",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 2
      },
      "errors": [],
      "union_shard_errors": [
        "ec_hash_error"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ2",
          "key": "",
          "snapid": -2,
          "hash": 562812377,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "39'2",
        "prior_version": "0'0",
        "last_reqid": "client.4248.0:1",
        "user_version": 2,
        "size": 7,
        "mtime": "2025-04-28T02:18:27.810433-0500",
        "local_mtime": "2025-04-28T02:18:27.815649-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [
            "ec_hash_error"
          ],
          "size": 2048
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ3",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 3
      },
      "errors": [],
      "union_shard_errors": [
        "missing"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ3",
          "key": "",
          "snapid": -2,
          "hash": 3125668237,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "46'3",
        "prior_version": "0'0",
        "last_reqid": "client.4286.0:1",
        "user_version": 3,
        "size": 7,
        "mtime": "2025-04-28T02:18:36.149933-0500",
        "local_mtime": "2025-04-28T02:18:36.155056-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "missing"
          ]
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ4",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 6
      },
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ4",
          "key": "",
          "snapid": -2,
          "hash": 1618759290,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "54'6",
        "prior_version": "54'5",
        "last_reqid": "client.4330.0:1",
        "user_version": 6,
        "size": 7,
        "mtime": "2025-04-28T02:18:44.594720-0500",
        "local_mtime": "2025-04-28T02:18:44.596235-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f",
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "bad-val",
              "Base64": false
            },
            {
              "name": "key2-EOBJ4",
              "value": "val2-EOBJ4",
              "Base64": false
            }
          ]
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f",
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "val1-EOBJ4",
              "Base64": false
            },
            {
              "name": "key2-EOBJ4",
              "value": "val2-EOBJ4",
              "Base64": false
            }
          ]
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f",
          "attrs": [
            {
              "name": "key1-EOBJ4",
              "value": "val1-EOBJ4",
              "Base64": false
            },
            {
              "name": "key3-EOBJ4",
              "value": "val3-EOBJ4",
              "Base64": false
            }
          ]
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ5",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 7
      },
      "errors": [
        "size_mismatch"
      ],
      "union_shard_errors": [
        "read_error",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ5",
          "key": "",
          "snapid": -2,
          "hash": 2918945441,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "71'7",
        "prior_version": "0'0",
        "last_reqid": "client.4424.0:1",
        "user_version": 7,
        "size": 7,
        "mtime": "2025-04-28T02:19:03.683994-0500",
        "local_mtime": "2025-04-28T02:19:03.685505-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "read_error",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "size": 4096,
          "object_info": {
            "oid": {
              "oid": "EOBJ5",
              "key": "",
              "snapid": -2,
              "hash": 2918945441,
              "max": 0,
              "pool": 3,
              "namespace": ""
            },
            "version": "71'7",
            "prior_version": "0'0",
            "last_reqid": "client.4424.0:1",
            "user_version": 7,
            "size": 7,
            "mtime": "2025-04-28T02:19:03.683994-0500",
            "local_mtime": "2025-04-28T02:19:03.685505-0500",
            "lost": 0,
            "flags": [
              "dirty",
              "data_digest"
            ],
            "truncate_seq": 0,
            "truncate_size": 0,
            "data_digest": "0x2ddbf8f5",
            "omap_digest": "0xffffffff",
            "expected_object_size": 0,
            "expected_write_size": 0,
            "alloc_hint_flags": 0,
            "manifest": {
              "type": 0
            },
            "watchers": {},
            "shard_versions": []
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f"
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ6",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 8
      },
      "errors": [],
      "union_shard_errors": [
        "read_error",
        "hinfo_missing",
        "hinfo_corrupted"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ6",
          "key": "",
          "snapid": -2,
          "hash": 3050890866,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "78'8",
        "prior_version": "0'0",
        "last_reqid": "client.4462.0:1",
        "user_version": 8,
        "size": 7,
        "mtime": "2025-04-28T02:19:11.980798-0500",
        "local_mtime": "2025-04-28T02:19:11.986446-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x2ddbf8f5",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [
            "read_error",
            "hinfo_missing"
          ],
          "size": 2048
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [
            "read_error",
            "hinfo_corrupted"
          ],
          "size": 2048,
          "hashinfo": "bad-val"
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x04cfa72f",
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 80717615
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 80717615
              }
            ]
          }
        }
      ]
    },
    {
      "object": {
        "name": "EOBJ7",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 10
      },
      "errors": [
        "hinfo_inconsistency"
      ],
      "union_shard_errors": [
        "ec_hash_error"
      ],
      "selected_object_info": {
        "oid": {
          "oid": "EOBJ7",
          "key": "",
          "snapid": -2,
          "hash": 3258066308,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "90'10",
        "prior_version": "90'9",
        "last_reqid": "client.4534.0:1",
        "user_version": 10,
        "size": 34,
        "mtime": "2025-04-28T02:19:27.775012-0500",
        "local_mtime": "2025-04-28T02:19:27.776394-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x136e4e27",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "shard": 2,
          "errors": [
            "ec_hash_error"
          ],
          "size": 2048,
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 80717615
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 80717615
              }
            ]
          }
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x5b7455a8",
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 1534350760
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 1534350760
              }
            ]
          }
        },
        {
          "osd": 2,
          "primary": false,
          "shard": 1,
          "errors": [],
          "size": 2048,
          "omap_digest": "0xffffffff",
          "data_digest": "0x5b7455a8",
          "hashinfo": {
            "total_chunk_size": 2048,
            "cumulative_shard_hashes": [
              {
                "shard": 0,
                "hash": 1534350760
              },
              {
                "shard": 1,
                "hash": 1534491824
              },
              {
                "shard": 2,
                "hash": 1534350760
              }
            ]
          }
        }
      ]
    }
  ]
}
EOF

    fi

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/csjson
    multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
    if test $getjson = "yes"
    then
      if [ "$allow_overwrites" = "true" ]
      then
        num=4
      else
        num=5
      fi
      jq '.' $dir/json > save${num}.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-obj.json || return 1
    fi

    ceph osd pool rm $poolname $poolname --yes-i-really-really-mean-it
}

function TEST_corrupt_scrub_erasure_appends() {
    corrupt_scrub_erasure $1 false
}

function TEST_corrupt_scrub_erasure_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        corrupt_scrub_erasure $1 true
    fi
}

#
# Test to make sure that a periodic scrub won't cause deep-scrub info to be lost
# Update 2024: this functionality was removed from the code. The test will be skipped.
#
function TEST_periodic_scrub_replicated() {
    local dir=$1
    local poolname=psr_pool
    local objname=POBJ
    return 0

    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-interval-randomize-ratio=0 --osd-deep-scrub-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0"
    run_osd $dir 0 $ceph_osd_args || return 1
    run_osd $dir 1 $ceph_osd_args || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    local osd=0
    add_something $dir $poolname $objname scrub || return 1
    local primary=$(get_primary $poolname $objname)
    local pg=$(get_pg $poolname $objname)

    # Add deep-scrub only error
    local payload=UVWXYZ
    echo $payload > $dir/CORRUPT
    # Uses $ceph_osd_args for osd restart
    objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1

    # No scrub information available, so expect failure
    set -o pipefail
    !  rados list-inconsistent-obj $pg | jq '.' || return 1
    set +o pipefail

    pg_deep_scrub $pg || return 1

    # Make sure bad object found
    rados list-inconsistent-obj $pg | jq '.' | grep -q $objname || return 1

    flush_pg_stats
    local last_scrub=$(get_last_scrub_stamp $pg)
    # Fake a scheduled deep scrub
    ceph tell $pg schedule-scrub || return 1
    # Wait for schedule regular scrub
    wait_for_scrub $pg "$last_scrub"

    # It needed to be upgraded
    # update 2024: the "upgrade" functionality has been removed
    grep -q "Deep scrub errors, upgrading scrub to deep-scrub" $dir/osd.${primary}.log || return 1

    # Bad object still known
    rados list-inconsistent-obj $pg | jq '.' | grep -q $objname || return 1

    # Can't upgrade with this set
    ceph osd set nodeep-scrub
    # Let map change propagate to OSDs
    ceph tell osd.0 get_latest_osdmap
    flush_pg_stats
    sleep 5

    # Fake a schedule scrub
    ceph tell $pg schedule-scrub || return 1
    # Wait for schedule regular scrub
    # to notice scrub and skip it
    local found=false
    for i in $(seq 14 -1 0)
    do
      sleep 1
      ! grep -q "Regular scrub skipped due to deep-scrub errors and nodeep-scrub set" $dir/osd.${primary}.log || { found=true ; break; }
      echo Time left: $i seconds
    done
    test $found = "true" || return 1

    # Bad object still known
    rados list-inconsistent-obj $pg | jq '.' | grep -q $objname || return 1

    flush_pg_stats
    # Request a regular scrub and it will be done
    pg_scrub $pg
    grep -q "Regular scrub request, deep-scrub details will be lost" $dir/osd.${primary}.log || return 1

    # deep-scrub error is no longer present
    rados list-inconsistent-obj $pg | jq '.' | grep -qv $objname || return 1
}

function TEST_scrub_warning() {
    local dir=$1
    local poolname=psr_pool
    local objname=POBJ
    local scrubs=5
    local deep_scrubs=5
    local i1_day=86400
    local i7_days=$(calc $i1_day \* 7)
    local i14_days=$(calc $i1_day \* 14)
    local overdue=0.5
    local conf_overdue_seconds=$(calc $i7_days + $i1_day + \( $i7_days \* $overdue \) )
    local pool_overdue_seconds=$(calc $i14_days + $i1_day + \( $i14_days \* $overdue \) )

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x --mon_warn_pg_not_scrubbed_ratio=${overdue} --mon_warn_pg_not_deep_scrubbed_ratio=${overdue} || return 1
    run_osd $dir 0 $ceph_osd_args --osd_scrub_backoff_ratio=0 || return 1

    for i in $(seq 1 $(expr $scrubs + $deep_scrubs))
    do
      create_pool $poolname-$i 1 1 || return 1
      wait_for_clean || return 1
      if [ $i = "1" ];
      then
        ceph osd pool set $poolname-$i scrub_max_interval $i14_days
      fi
      if [ $i = $(expr $scrubs + 1) ];
      then
        ceph osd pool set $poolname-$i deep_scrub_interval $i14_days
      fi
    done

    # Only 1 osd
    local primary=0

    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1
    ceph config set global osd_scrub_interval_randomize_ratio 0
    ceph config set global osd_deep_scrub_randomize_ratio 0
    ceph config set global osd_scrub_max_interval ${i7_days}
    ceph config set global osd_deep_scrub_interval ${i7_days}

    # Fake schedule scrubs
    for i in $(seq 1 $scrubs)
    do
      if [ $i = "1" ];
      then
        overdue_seconds=$pool_overdue_seconds
      else
        overdue_seconds=$conf_overdue_seconds
      fi
      ceph tell ${i}.0 schedule-scrub $(expr ${overdue_seconds} + ${i}00) || return 1
    done
    # Fake schedule deep scrubs
    for i in $(seq $(expr $scrubs + 1) $(expr $scrubs + $deep_scrubs))
    do
      if [ $i = "$(expr $scrubs + 1)" ];
      then
        overdue_seconds=$pool_overdue_seconds
      else
        overdue_seconds=$conf_overdue_seconds
      fi
      ceph tell ${i}.0 schedule-deep-scrub $(expr ${overdue_seconds} + ${i}00) || return 1
    done
    flush_pg_stats

    ceph health
    ceph health detail
    ceph health | grep -q " pgs not deep-scrubbed in time" || return 1
    ceph health | grep -q " pgs not scrubbed in time" || return 1

    # note that the 'ceph tell pg deep-scrub' command now also sets the regular scrub
    # time-stamp. I.e. - all 'late for deep scrubbing' pgs are also late for
    # regular scrubbing. For now, we'll allow both responses.
    COUNT=$(ceph health detail | grep "not scrubbed since" | wc -l)

    if (( $COUNT != $scrubs && $COUNT != $(expr $scrubs+$deep_scrubs) )); then
      ceph health detail | grep "not scrubbed since"
      return 1
    fi
    COUNT=$(ceph health detail | grep "not deep-scrubbed since" | wc -l)
    if [ "$COUNT" != $deep_scrubs ]; then
      ceph health detail | grep "not deep-scrubbed since"
      return 1
    fi
}

#
# Corrupt snapset in replicated pool
#
function TEST_corrupt_snapset_scrub_rep() {
    local dir=$1
    local poolname=csr_pool
    local total_objs=2

    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_pool foo 1 || return 1
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}
        add_something $dir $poolname $objname || return 1

        rados --pool $poolname setomapheader $objname hdr-$objname || return 1
        rados --pool $poolname setomapval $objname key-$objname val-$objname || return 1
    done

    local pg=$(get_pg $poolname ROBJ0)
    local primary=$(get_primary $poolname ROBJ0)

    rados -p $poolname mksnap snap1
    echo -n head_of_snapshot_data > $dir/change

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}

        # Alternate corruption between osd.0 and osd.1
        local osd=$(expr $i % 2)

        case $i in
        1)
          rados --pool $poolname put $objname $dir/change
          objectstore_tool $dir $osd --head $objname clear-snapset corrupt || return 1
          ;;

        2)
          rados --pool $poolname put $objname $dir/change
          objectstore_tool $dir $osd --head $objname clear-snapset corrupt || return 1
          ;;

        esac
    done
    rm $dir/change

    pg_scrub $pg

    rados list-inconsistent-pg $poolname > $dir/json || return 1
    # Check pg count
    test $(jq '. | length' $dir/json) = "1" || return 1
    # Check pgid
    test $(jq -r '.[0]' $dir/json) = $pg || return 1

    rados list-inconsistent-obj $pg > $dir/json || return 1

    jq "$jqfilter" << EOF | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/checkcsjson
{
  "epoch": 39,
  "inconsistents": [
    {
      "object": {
        "name": "ROBJ1",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 8
      },
      "errors": [
        "snapset_inconsistency"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ1",
          "key": "",
          "snapid": -2,
          "hash": 1454963827,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "29'8",
        "prior_version": "26'3",
        "last_reqid": "client.4216.0:1",
        "user_version": 8,
        "size": 21,
        "mtime": "2025-04-28T22:25:38.106944-0500",
        "local_mtime": "2025-04-28T22:25:38.111607-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x53acb008",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 21,
          "snapset": {
            "seq": 1,
            "clones": [
              {
                "snap": 1,
                "size": 7,
                "overlap": "[]",
                "snaps": [
                  1
                ]
              }
            ]
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 21,
          "snapset": {
            "seq": 0,
            "clones": []
          }
        }
      ]
    },
    {
      "object": {
        "name": "ROBJ2",
        "nspace": "",
        "locator": "",
        "snap": "head",
        "version": 10
      },
      "errors": [
        "snapset_inconsistency"
      ],
      "union_shard_errors": [],
      "selected_object_info": {
        "oid": {
          "oid": "ROBJ2",
          "key": "",
          "snapid": -2,
          "hash": 2026323607,
          "max": 0,
          "pool": 3,
          "namespace": ""
        },
        "version": "35'10",
        "prior_version": "28'6",
        "last_reqid": "client.4246.0:1",
        "user_version": 10,
        "size": 21,
        "mtime": "2025-04-28T22:25:44.826346-0500",
        "local_mtime": "2025-04-28T22:25:44.828220-0500",
        "lost": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest"
        ],
        "truncate_seq": 0,
        "truncate_size": 0,
        "data_digest": "0x53acb008",
        "omap_digest": "0xffffffff",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "alloc_hint_flags": 0,
        "manifest": {
          "type": 0
        },
        "watchers": {},
        "shard_versions": []
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 21,
          "snapset": {
            "seq": 0,
            "clones": []
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 21,
          "snapset": {
            "seq": 1,
            "clones": [
              {
                "snap": 1,
                "size": 7,
                "overlap": "[]",
                "snaps": [
                  1
                ]
              }
            ]
          }
        }
      ]
    }
  ]
}

EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/csjson
    multidiff $dir/checkcsjson $dir/csjson || test $getjson = "yes" || return 1
    if test $getjson = "yes"
    then
        jq '.' $dir/json > save6.json
    fi

    if test "$LOCALRUN" = "yes" && which jsonschema > /dev/null;
    then
      jsonschema -i $dir/json $CEPH_ROOT/doc/rados/command/list-inconsistent-obj.json || return 1
    fi

    ERRORS=0
    declare -a err_strings
    err_strings[0]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid [0-9]*:.*:::ROBJ1:head : snapset inconsistent"
    err_strings[1]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 soid [0-9]*:.*:::ROBJ2:head : snapset inconsistent"
    err_strings[2]="log_channel[(]cluster[)] log [[]ERR[]] : scrub [0-9]*[.]0 [0-9]*:.*:::ROBJ1:1 : is an unexpected clone"
    err_strings[3]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub : stat mismatch, got 3/4 objects, 1/2 clones, 3/4 dirty, 3/4 omap, 0/0 pinned, 0/0 hit_set_archive, 0/0 whiteouts, 49/56 bytes, 0/0 manifest objects, 0/0 hit_set_archive bytes."
    err_strings[4]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub 0 missing, 2 inconsistent objects"
    err_strings[5]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub 4 errors"

    for err_string in "${err_strings[@]}"
    do
        if ! grep -q "$err_string" $dir/osd.${primary}.log
        then
            echo "Missing log message '$err_string'"
            ERRORS=$(expr $ERRORS + 1)
        fi
    done

    if [ $ERRORS != "0" ];
    then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    ceph osd pool rm $poolname $poolname --yes-i-really-really-mean-it
}

function TEST_request_scrub_priority() {
    local dir=$1
    local poolname=psr_pool
    local objname=POBJ
    local OBJECTS=64
    local PGS=8

    run_mon $dir a --osd_pool_default_size=1 --mon_allow_pool_size_one=true || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-interval-randomize-ratio=0 --osd-deep-scrub-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0"
    run_osd $dir 0 $ceph_osd_args || return 1

    create_pool $poolname $PGS $PGS || return 1
    wait_for_clean || return 1

    local osd=0
    add_something $dir $poolname $objname noscrub || return 1
    local primary=$(get_primary $poolname $objname)
    local pg=$(get_pg $poolname $objname)
    poolid=$(ceph osd dump | grep "^pool.*[']${poolname}[']" | awk '{ print $2 }')

    local otherpgs
    for i in $(seq 0 $(expr $PGS - 1))
    do
        opg="${poolid}.${i}"
        if [ "$opg" = "$pg" ]; then
          continue
        fi
        otherpgs="${otherpgs}${opg} "
        local other_last_scrub=$(get_last_scrub_stamp $pg)
        # Fake a schedule scrub
        ceph tell $opg schedule-scrub $opg || return 1
    done

    sleep 15
    flush_pg_stats

    # Force a shallow scrub and it will be done
    local last_scrub=$(get_last_scrub_stamp $pg)
    ceph tell $pg scrub || return 1

    ceph osd unset noscrub || return 1
    ceph osd unset nodeep-scrub || return 1

    wait_for_scrub $pg "$last_scrub"

    for opg in $otherpgs $pg
    do
        wait_for_scrub $opg "$other_last_scrub"
    done

    # Verify that the requested scrub ran first
    grep "log_channel.*scrub ok" $dir/osd.${primary}.log | grep -v purged_snaps | head -1 | sed 's/.*[[]DBG[]]//' | grep -q $pg || return 1
}

#
# Testing the "split scrub store" feature: shallow scrubs do not
# purge deep errors from the store.
#
# Corrupt one copy of a replicated pool, creating both shallow and deep errors.
# Then shallow-scrub the pool and verify that the deep errors are still present.
#
function TEST_dual_store_replicated_cluster() {
    local dir=$1
    local poolname=csr_pool
    local total_objs=19
    local extr_dbg=1 # note: 3 and above leave some temp files around

    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_mgr $dir x --mgr_stats_period=1 || return 1
    local ceph_osd_args="--osd-scrub-interval-randomize-ratio=0 --osd-deep-scrub-randomize-ratio=0 "
    ceph_osd_args+="--osd_scrub_backoff_ratio=0 --osd_stats_update_period_not_scrubbing=3 "
    ceph_osd_args+="--osd_stats_update_period_scrubbing=2 --osd_op_queue=wpq --osd_scrub_auto_repair=0 "
    for osd in $(seq 0 1)
    do
      run_osd $dir $osd $ceph_osd_args || return 1
    done

    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_pool foo 1 || return 1
    create_pool $poolname 1 1 || return 1
    wait_for_clean || return 1

    ceph osd pool set $poolname noscrub 1
    ceph osd pool set $poolname nodeep-scrub 1

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}
        add_something $dir $poolname $objname || return 1

        rados --pool $poolname setomapheader $objname hdr-$objname || return 1
        rados --pool $poolname setomapval $objname key-$objname val-$objname || return 1
    done

    # Increase file 1 MB + 1KB
    dd if=/dev/zero of=$dir/new.ROBJ19 bs=1024 count=1025
    rados --pool $poolname put $objname $dir/new.ROBJ19 || return 1
    rm -f $dir/new.ROBJ19

    local pg=$(get_pg $poolname ROBJ0)
    local primary=$(get_primary $poolname ROBJ0)

    # Compute an old omap digest and save oi
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.0) \
        config set osd_deep_scrub_update_digest_min_age 0
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.1) \
        config set osd_deep_scrub_update_digest_min_age 0
    pg_deep_scrub $pg

    for i in $(seq 1 $total_objs) ; do
        objname=ROBJ${i}

        # Alternate corruption between osd.0 and osd.1
        local osd=$(expr $i % 2)

        case $i in
        1)
            # Size (deep scrub data_digest too)
            local payload=UVWXYZZZ
            echo $payload > $dir/CORRUPT
            objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1
            ;;

        2)
            # digest (deep scrub only)
            local payload=UVWXYZ
            echo $payload > $dir/CORRUPT
            objectstore_tool $dir $osd $objname set-bytes $dir/CORRUPT || return 1
            ;;

        3)
             # missing
             objectstore_tool $dir $osd $objname remove || return 1
             ;;

         4)
             # Modify omap value (deep scrub only)
             objectstore_tool $dir $osd $objname set-omap key-$objname $dir/CORRUPT || return 1
             ;;

         5)
            # Delete omap key (deep scrub only)
            objectstore_tool $dir $osd $objname rm-omap key-$objname || return 1
            ;;

         6)
            # Add extra omap key (deep scrub only)
            echo extra > $dir/extra-val
            objectstore_tool $dir $osd $objname set-omap key2-$objname $dir/extra-val || return 1
            rm $dir/extra-val
            ;;

         7)
            # Modify omap header (deep scrub only)
            echo -n newheader > $dir/hdr
            objectstore_tool $dir $osd $objname set-omaphdr $dir/hdr || return 1
            rm $dir/hdr
            ;;

         8)
            rados --pool $poolname setxattr $objname key1-$objname val1-$objname || return 1
            rados --pool $poolname setxattr $objname key2-$objname val2-$objname || return 1

            # Break xattrs
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir $osd $objname set-attr _key1-$objname $dir/bad-val || return 1
            objectstore_tool $dir $osd $objname rm-attr _key2-$objname || return 1
            echo -n val3-$objname > $dir/newval
            objectstore_tool $dir $osd $objname set-attr _key3-$objname $dir/newval || return 1
            rm $dir/bad-val $dir/newval
            ;;

        9)
            objectstore_tool $dir $osd $objname get-attr _ > $dir/robj9-oi
            echo -n D > $dir/change
            rados --pool $poolname put $objname $dir/change
            objectstore_tool $dir $osd $objname set-attr _ $dir/robj9-oi
            rm $dir/oi $dir/change
            ;;

          # ROBJ10 must be handled after digests are re-computed by a deep scrub below
          # ROBJ11 must be handled with config change before deep scrub
          # ROBJ12 must be handled with config change before scrubs
          # ROBJ13 must be handled before scrubs

        14)
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir 0 $objname set-attr _ $dir/bad-val || return 1
            objectstore_tool $dir 1 $objname rm-attr _ || return 1
            rm $dir/bad-val
            ;;

        15)
            objectstore_tool $dir $osd $objname rm-attr _ || return 1
            ;;

        16)
            objectstore_tool $dir 0 $objname rm-attr snapset || return 1
            echo -n bad-val > $dir/bad-val
            objectstore_tool $dir 1 $objname set-attr snapset $dir/bad-val || return 1
	    ;;

	17)
	    # Deep-scrub only (all replicas are diffent than the object info
           local payload=ROBJ17
           echo $payload > $dir/new.ROBJ17
	   objectstore_tool $dir 0 $objname set-bytes $dir/new.ROBJ17 || return 1
	   objectstore_tool $dir 1 $objname set-bytes $dir/new.ROBJ17 || return 1
	   ;;

	18)
	    # Deep-scrub only (all replicas are diffent than the object info
           local payload=ROBJ18
           echo $payload > $dir/new.ROBJ18
	   objectstore_tool $dir 0 $objname set-bytes $dir/new.ROBJ18 || return 1
	   objectstore_tool $dir 1 $objname set-bytes $dir/new.ROBJ18 || return 1
	   # Make one replica have a different object info, so a full repair must happen too
	   objectstore_tool $dir $osd $objname corrupt-info || return 1
	   ;;

	19)
	   # Set osd-max-object-size smaller than this object's size

        esac
    done

    local pg=$(get_pg $poolname ROBJ0)

    ceph tell osd.\* injectargs -- --osd-max-object-size=1048576

    inject_eio rep data $poolname ROBJ11 $dir 0 || return 1 # shard 0 of [1, 0], osd.1
    inject_eio rep mdata $poolname ROBJ12 $dir 1 || return 1 # shard 1 of [1, 0], osd.0
    inject_eio rep data $poolname ROBJ13 $dir 0 || return 1 # shard 0 of [1, 0], osd.1

    # first sequence: the final shallow scrub should not override any of the deep errors
    pg_scrub $pg
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | python3 -c "$sortkeys" | jq '.'  > /tmp/WQR_1.json
    pg_scrub $pg
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | python3 -c "$sortkeys" | jq '.'  > /tmp/WQR_1b.json
    rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/sh1_results.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | \
        python3 -c "$sortkeys" > /tmp/WQR_1b_s.json

    pg_deep_scrub $pg
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | python3 -c "$sortkeys" | jq '.'  > /tmp/WQR_2.json
    rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/dp_results.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | \
        python3 -c "$sortkeys" > /tmp/WQR_2s.json

    pg_scrub $pg
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | python3 -c "$sortkeys" | jq '.'  > /tmp/WQR_3.json
    rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | python3 -c "$sortkeys" > $dir/sh2_results.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | \
        python3 -c "$sortkeys" > /tmp/WQR_3s.json

    diff -u $dir/dp_results.json $dir/sh2_results.json || return 1

    # inject a read error, which is a special case: the scrub encountering the read error
    # would override the previously collected shard info.
    inject_eio rep mdata $poolname ROBJ13 $dir 1 || return 1 # shard 1 of [1, 0], osd.0

    pg_deep_scrub $pg

    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | python3 -c "$sortkeys" | jq '.'  > /tmp/WQR_4.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | \
        python3 -c "$sortkeys" > /tmp/WQR_4s_w13.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | \
        jq 'del(.inconsistents[] | select(.object.name == "ROBJ13"))' | \
        jq '.inconsistents' | python3 -c "$sortkeys" > /tmp/WQR_4s_wo13.json

    rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | \
        python3 -c "$sortkeys" > $dir/dpPart2_w13_results.json
    # Remove the entry with "name":"ROBJ13" from the $dir/d*_results.json
    rados list-inconsistent-obj $pg | jq "$jqfilter" | jq 'del(.inconsistents[] | select(.object.name == "ROBJ13"))' | \
        jq '.inconsistents' | python3 -c "$sortkeys" > $dir/dpPart2_wo13_results.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | \
        python3 -c "$sortkeys" > /tmp/WQR_4s.json

    pg_scrub $pg

    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | python3 -c "$sortkeys" | jq '.'  > /tmp/WQR_5.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | \
        python3 -c "$sortkeys" > /tmp/WQR_5s_w13.json
    (( extr_dbg >= 3 )) && rados list-inconsistent-obj $pg | jq "$jqfilter" | \
        jq 'del(.inconsistents[] | select(.object.name == "ROBJ13"))' |\
        jq '.inconsistents' | python3 -c "$sortkeys" > /tmp/WQR_5s_wo13.json

    rados list-inconsistent-obj $pg | jq "$jqfilter" | jq '.inconsistents' | python3 -c "$sortkeys" > \
        $dir/sh2Part2_w13_results.json
    rados list-inconsistent-obj $pg | jq "$jqfilter" | jq 'del(.inconsistents[] | select(.object.name == "ROBJ13"))' |\
        jq '.inconsistents' | python3 -c "$sortkeys" > $dir/shPart2_wo13_results.json

    # the shallow scrub results should differ from the results of the deep
    # scrub preceding it, but the difference should be limited to ROBJ13
    diff -u $dir/dpPart2_w13_results.json $dir/sh2Part2_w13_results.json && return 1
    diff -u $dir/dpPart2_wo13_results.json $dir/shPart2_wo13_results.json || return 1

    ceph osd pool rm $poolname $poolname --yes-i-really-really-mean-it
    return 0
}


main osd-scrub-repair "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-repair.sh"
# End:
