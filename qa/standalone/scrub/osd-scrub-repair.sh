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

if [ `uname` = FreeBSD ]; then
    # erasure coding overwrites are only tested on Bluestore
    # erasure coding on filestore is unsafe
    # http://docs.ceph.com/docs/master/rados/operations/erasure-code/#erasure-coding-with-overwrites
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

sortkeys='import json; import sys ; JSON=sys.stdin.read() ; ud = json.loads(JSON) ; print json.dumps(ud, sort_keys=True, indent=2)'

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7107" # git grep '\<7107\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--osd-skip-data-digest=false "
    CEPH_ARGS+="--osd-objectstore=filestore "

    export -n CEPH_CLI_TEST_DUP_COMMAND
    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
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

    setup $dir || return 1
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

    teardown $dir || return 1
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
# 2) remove the corresponding file from a designated OSD
# 3) repair the PG
# 4) check that the file has been restored in the designated OSD
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

function create_ec_pool() {
    local pool_name=$1
    shift
    local allow_overwrites=$1
    shift

    ceph osd erasure-code-profile set myprofile crush-failure-domain=osd "$@" || return 1

    create_pool "$poolname" 1 1 erasure myprofile || return 1

    if [ "$allow_overwrites" = "true" ]; then
        ceph osd pool set "$poolname" allow_ec_overwrites true || return 1
    fi

    wait_for_clean || return 1
    return 0
}

function auto_repair_erasure_coded() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool

    # Launch a cluster with 5 seconds scrub interval
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
            --osd-deep-scrub-interval=5 \
            --osd-scrub-max-interval=5 \
            --osd-scrub-min-interval=5 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
	if [ "$allow_overwrites" = "true" ]; then
            run_osd_bluestore $dir $id $ceph_osd_args || return 1
	else
            run_osd $dir $id $ceph_osd_args || return 1
	fi
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

    # Tear down
    teardown $dir || return 1
}

function TEST_auto_repair_erasure_coded_appends() {
    auto_repair_erasure_coded $1 false
}

function TEST_auto_repair_erasure_coded_overwrites() {
    if [ "$use_ec_overwrite" = "true" ]; then
        auto_repair_erasure_coded $1 true
    fi
}

function TEST_auto_repair_bluestore_basic() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
	    --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd_bluestore $dir $id $ceph_osd_args || return 1
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
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_deep_scrub $pgid
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_scrub $pgid

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

    # Tear down
    teardown $dir || return 1
}

function TEST_auto_repair_bluestore_scrub() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
	    --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd_bluestore $dir $id $ceph_osd_args || return 1
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
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_scrub $pgid

    # Wait for scrub -> auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    ceph pg dump pgs
    # Actually this causes 2 scrubs, so we better wait a little longer
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

    # Tear down
    teardown $dir || return 1
}

function TEST_auto_repair_bluestore_failed() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
	    --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd_bluestore $dir $id $ceph_osd_args || return 1
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
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_deep_scrub $pgid
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_scrub $pgid

    # Wait for auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    wait_for_clean || return 1
    flush_pg_stats
    grep scrub_finish $dir/osd.${primary}.log
    grep -q "scrub_finish.*still present after re-scrub" $dir/osd.${primary}.log || return 1
    ceph pg dump pgs
    ceph pg dump pgs | grep -q "^$(pgid).*+failed_repair" || return 1

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

    ceph pg dump pgs
    ceph pg dump pgs | grep -q "^$(pgid).* active+clean " || return 1
    grep scrub_finish $dir/osd.${primary}.log

    # Tear down
    teardown $dir || return 1
}

function TEST_auto_repair_bluestore_failed_norecov() {
    local dir=$1
    local poolname=testpool

    # Launch a cluster with 5 seconds scrub interval
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd-scrub-auto-repair=true \
	    --osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 2) ; do
        run_osd_bluestore $dir $id $ceph_osd_args || return 1
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

    local pgid=$(get_pg $poolname obj1)
    local primary=$(get_primary $poolname obj1)
    local last_scrub_stamp="$(get_last_scrub_stamp $pgid)"
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_deep_scrub $pgid
    CEPH_ARGS='' ceph daemon $(get_asok_path osd.$primary) trigger_scrub $pgid

    # Wait for auto repair
    wait_for_scrub $pgid "$last_scrub_stamp" || return 1
    wait_for_clean || return 1
    flush_pg_stats
    grep -q "scrub_finish.*present with no repair possible" $dir/osd.${primary}.log || return 1
    ceph pg dump pgs
    ceph pg dump pgs | grep -q "^$(pgid).*+failed_repair" || return 1

    # Tear down
    teardown $dir || return 1
}

function TEST_repair_stats() {
    local dir=$1
    local poolname=testpool
    local OSDS=2
    local OBJS=30
    # This need to be an even number
    local REPAIRS=20

    # Launch a cluster with 5 seconds scrub interval
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 $(expr $OSDS - 1)) ; do
        run_osd_bluestore $dir $id $ceph_osd_args || return 1
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
    run_osd_bluestore $dir $primary $ceph_osd_args || return 1
    run_osd_bluestore $dir $other $ceph_osd_args || return 1
    wait_for_clean || return 1

    repair $pgid
    wait_for_clean || return 1
    ceph pg dump pgs

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

    # Tear down
    teardown $dir || return 1
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
    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    local ceph_osd_args="--osd_deep_scrub_randomize_ratio=0 \
            --osd-scrub-interval-randomize-ratio=0"
    for id in $(seq 0 $(expr $OSDS - 1)) ; do
        run_osd_bluestore $dir $id $ceph_osd_args || return 1
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
    run_osd_bluestore $dir $primary $ceph_osd_args || return 1
    run_osd_bluestore $dir $other $ceph_osd_args || return 1
    wait_for_clean || return 1

    repair $pgid
    wait_for_clean || return 1
    ceph pg dump pgs

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

    # Tear down
    teardown $dir || return 1
}

function corrupt_and_repair_jerasure() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 3) ; do
	if [ "$allow_overwrites" = "true" ]; then
            run_osd_bluestore $dir $id || return 1
	else
            run_osd $dir $id || return 1
	fi
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_ec_pool $poolname $allow_overwrites k=2 m=2 || return 1
    corrupt_and_repair_erasure_coded $dir $poolname || return 1

    teardown $dir || return 1
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

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 9) ; do
	if [ "$allow_overwrites" = "true" ]; then
            run_osd_bluestore $dir $id || return 1
	else
            run_osd $dir $id || return 1
	fi
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    create_ec_pool $poolname $allow_overwrites k=4 m=2 l=3 plugin=lrc || return 1
    corrupt_and_repair_erasure_coded $dir $poolname || return 1

    teardown $dir || return 1
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

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 3) ; do
	if [ "$allow_overwrites" = "true" ]; then
            run_osd_bluestore $dir $id || return 1
	else
            run_osd $dir $id || return 1
	fi
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

    teardown $dir || return 1
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

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 2) ; do
	if [ "$allow_overwrites" = "true" ]; then
            run_osd_bluestore $dir $id || return 1
	else
            run_osd $dir $id || return 1
	fi
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

    teardown $dir || return 1
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
    local total_objs=18

    setup $dir || return 1
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

        esac
    done

    local pg=$(get_pg $poolname ROBJ0)

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
    err_strings[17]="log_channel[(]cluster[)] log [[]ERR[]] : scrub [0-9]*[.]0 3:5c7b2c47:::ROBJ16:head : can't decode 'snapset' attr buffer::malformed_input: .* no longer understand old encoding version 3 < 97"
    err_strings[18]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub : stat mismatch, got 18/18 objects, 0/0 clones, 17/18 dirty, 17/18 omap, 0/0 pinned, 0/0 hit_set_archive, 0/0 whiteouts, 113/120 bytes, 0/0 manifest objects, 0/0 hit_set_archive bytes."
    err_strings[19]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub 1 missing, 7 inconsistent objects"
    err_strings[20]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 scrub 17 errors"

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

    jq "$jqfilter" << EOF | jq '.inconsistents' | python -c "$sortkeys" > $dir/checkcsjson
{
  "inconsistents": [
    {
      "shards": [
        {
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "51'58",
            "prior_version": "21'3",
            "last_reqid": "osd.1.0:57",
            "user_version": 3,
            "size": 7,
            "mtime": "",
            "local_mtime": "",
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
            "watchers": {}
          },
          "size": 9,
          "errors": [
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'58",
        "prior_version": "21'3",
        "last_reqid": "osd.1.0:57",
        "user_version": 3,
        "size": 7,
        "mtime": "2018-04-05 14:33:19.804040",
        "local_mtime": "2018-04-05 14:33:19.804839",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "size_mismatch"
      ],
      "object": {
        "version": 3,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ1"
      }
    },
    {
      "shards": [
        {
          "errors": [
            "stat_error"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'56",
        "prior_version": "43'36",
        "last_reqid": "osd.1.0:55",
        "user_version": 36,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "stat_error"
      ],
      "errors": [],
      "object": {
        "version": 36,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ12"
      }
    },
    {
      "shards": [
        {
          "errors": [
            "stat_error"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'59",
        "prior_version": "45'39",
        "last_reqid": "osd.1.0:58",
        "user_version": 39,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "stat_error"
      ],
      "errors": [],
      "object": {
        "version": 39,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ13"
      }
    },
    {
      "shards": [
        {
          "object_info": "bad-val",
          "size": 7,
          "errors": [
            "info_corrupted"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "size": 7,
          "errors": [
            "info_missing"
          ],
          "osd": 1,
          "primary": true
        }
      ],
      "union_shard_errors": [
        "info_missing",
        "info_corrupted"
      ],
      "errors": [],
      "object": {
        "version": 0,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ14"
      }
    },
    {
      "shards": [
        {
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
            "version": "51'49",
            "prior_version": "49'45",
            "last_reqid": "osd.1.0:48",
            "user_version": 45,
            "size": 7,
            "mtime": "2018-04-05 14:33:29.498969",
            "local_mtime": "2018-04-05 14:33:29.499890",
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
            "watchers": {}
          },
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "size": 7,
          "errors": [
            "info_missing"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'49",
        "prior_version": "49'45",
        "last_reqid": "osd.1.0:48",
        "user_version": 45,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "info_missing"
      ],
      "errors": [],
      "object": {
        "version": 45,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ15"
      }
    },
    {
      "errors": [],
      "object": {
      "locator": "",
      "name": "ROBJ16",
      "nspace": "",
      "snap": "head",
      "version": 0
       },
        "shards": [
      {
        "errors": [
          "snapset_missing"
        ],
        "osd": 0,
        "primary": false,
        "size": 7
      },
      {
        "errors": [
          "snapset_corrupted"
        ],
        "osd": 1,
        "primary": true,
        "snapset": "bad-val",
        "size": 7
      }
      ],
      "union_shard_errors": [
        "snapset_missing",
        "snapset_corrupted"
      ]
    },
    {
     "errors": [
       "object_info_inconsistency"
     ],
     "object": {
       "locator": "",
       "name": "ROBJ18",
       "nspace": "",
       "snap": "head"
     },
     "selected_object_info": {
       "alloc_hint_flags": 255,
       "data_digest": "0x2ddbf8f5",
       "expected_object_size": 0,
       "expected_write_size": 0,
       "flags": [
         "dirty",
         "omap",
         "data_digest",
         "omap_digest"
       ],
       "lost": 0,
       "manifest": {
         "type": 0
       },
       "oid": {
         "hash": 1629828556,
         "key": "",
         "max": 0,
         "namespace": "",
         "oid": "ROBJ18",
         "pool": 3,
         "snapid": -2
       },
       "omap_digest": "0xddc3680f",
       "size": 7,
       "truncate_seq": 0,
       "truncate_size": 0,
       "user_version": 54,
       "watchers": {}
     },
     "shards": [
       {
         "errors": [],
         "object_info": {
           "alloc_hint_flags": 0,
           "data_digest": "0x2ddbf8f5",
           "expected_object_size": 0,
           "expected_write_size": 0,
           "flags": [
             "dirty",
             "omap",
             "data_digest",
             "omap_digest"
           ],
           "lost": 0,
           "manifest": {
             "type": 0
           },
           "oid": {
             "hash": 1629828556,
             "key": "",
             "max": 0,
             "namespace": "",
             "oid": "ROBJ18",
             "pool": 3,
             "snapid": -2
           },
           "omap_digest": "0xddc3680f",
           "size": 7,
           "truncate_seq": 0,
           "truncate_size": 0,
           "user_version": 54,
           "watchers": {}
         },
         "osd": 0,
         "primary": false,
         "size": 7
       },
       {
         "errors": [],
         "object_info": {
           "alloc_hint_flags": 255,
           "data_digest": "0x2ddbf8f5",
           "expected_object_size": 0,
           "expected_write_size": 0,
           "flags": [
             "dirty",
             "omap",
             "data_digest",
             "omap_digest"
           ],
           "lost": 0,
           "manifest": {
             "type": 0
           },
           "oid": {
             "hash": 1629828556,
             "key": "",
             "max": 0,
             "namespace": "",
             "oid": "ROBJ18",
             "pool": 3,
             "snapid": -2
           },
           "omap_digest": "0xddc3680f",
           "size": 7,
           "truncate_seq": 0,
           "truncate_size": 0,
           "user_version": 54,
           "watchers": {}
         },
         "osd": 1,
         "primary": true,
         "size": 7
       }
     ],
     "union_shard_errors": []
   },
   {
      "shards": [
        {
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "errors": [
            "missing"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'61",
        "prior_version": "25'9",
        "last_reqid": "osd.1.0:60",
        "user_version": 9,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "missing"
      ],
      "errors": [],
      "object": {
        "version": 9,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ3"
      }
    },
    {
      "shards": [
        {
          "attrs": [
            {
              "Base64": false,
              "value": "bad-val",
              "name": "key1-ROBJ8"
            },
            {
              "Base64": false,
              "value": "val2-ROBJ8",
              "name": "key2-ROBJ8"
            }
          ],
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "attrs": [
            {
              "Base64": false,
              "value": "val1-ROBJ8",
              "name": "key1-ROBJ8"
            },
            {
              "Base64": false,
              "value": "val3-ROBJ8",
              "name": "key3-ROBJ8"
            }
          ],
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
      ],
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
        "version": "79'66",
        "prior_version": "79'65",
        "last_reqid": "client.4554.0:1",
        "user_version": 74,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [],
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "object": {
        "version": 66,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ8"
      }
    },
    {
      "shards": [
        {
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
            "version": "95'67",
            "prior_version": "51'64",
            "last_reqid": "client.4649.0:1",
            "user_version": 75,
            "size": 1,
            "mtime": "",
            "local_mtime": "",
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
            "watchers": {}
          },
          "size": 1,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "51'64",
            "prior_version": "37'27",
            "last_reqid": "osd.1.0:63",
            "user_version": 27,
            "size": 7,
            "mtime": "2018-04-05 14:33:25.352485",
            "local_mtime": "2018-04-05 14:33:25.353746",
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
            "watchers": {}
          },
          "size": 1,
          "errors": [
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "95'67",
        "prior_version": "51'64",
        "last_reqid": "client.4649.0:1",
        "user_version": 75,
        "size": 1,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
         "obj_size_info_mismatch"
      ],
      "errors": [
        "object_info_inconsistency"
      ],
      "object": {
        "version": 67,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ9"
      }
    }
  ],
  "epoch": 0
}
EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python -c "$sortkeys" > $dir/csjson
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
    err_strings[23]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 shard 1 soid 3:ce3f1d6a:::ROBJ1:head : data_digest 0x2d4a11c2 != data_digest 0x2ddbf8f5 from shard 0, data_digest 0x2d4a11c2 != data_digest 0x2ddbf8f5 from auth oi 3:ce3f1d6a:::ROBJ1:head[(][0-9]*'[0-9]* osd.1.0:65 dirty|omap|data_digest|omap_digest s 7 uv 3 dd 2ddbf8f5 od f5fba2c6 alloc_hint [[]0 0 0[]][)], size 9 != size 7 from auth oi 3:ce3f1d6a:::ROBJ1:head[(][0-9]*'[0-9]* osd.1.0:[0-9]* dirty|omap|data_digest|omap_digest s 7 uv 3 dd 2ddbf8f5 od f5fba2c6 alloc_hint [[]0 0 0[]][)], size 9 != size 7 from shard 0"
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
    err_strings[35]="log_channel[(]cluster[)] log [[]ERR[]] : deep-scrub [0-9]*[.]0 3:5c7b2c47:::ROBJ16:head : can't decode 'snapset' attr buffer::malformed_input: .* no longer understand old encoding version 3 < 97"
    err_strings[36]="log_channel[(]cluster[)] log [[]ERR[]] : [0-9]*[.]0 deep-scrub : stat mismatch, got 18/18 objects, 0/0 clones, 17/18 dirty, 17/18 omap, 0/0 pinned, 0/0 hit_set_archive, 0/0 whiteouts, 115/116 bytes, 0/0 manifest objects, 0/0 hit_set_archive bytes."
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

    jq "$jqfilter" << EOF | jq '.inconsistents' | python -c "$sortkeys" > $dir/checkcsjson
{
  "inconsistents": [
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xf5fba2c6",
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "51'58",
            "prior_version": "21'3",
            "last_reqid": "osd.1.0:57",
            "user_version": 3,
            "size": 7,
            "mtime": "2018-04-05 14:33:19.804040",
            "local_mtime": "2018-04-05 14:33:19.804839",
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
            "watchers": {}
          },
          "data_digest": "0x2d4a11c2",
          "omap_digest": "0xf5fba2c6",
          "size": 9,
          "errors": [
            "data_digest_mismatch_info",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'58",
        "prior_version": "21'3",
        "last_reqid": "osd.1.0:57",
        "user_version": 3,
        "size": 7,
        "mtime": "2018-04-05 14:33:19.804040",
        "local_mtime": "2018-04-05 14:33:19.804839",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "data_digest_mismatch_info",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "data_digest_mismatch",
        "size_mismatch"
      ],
      "object": {
        "version": 3,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ1"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xa8dd5adc",
          "size": 7,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xa8dd5adc",
          "size": 7,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "osd": 1,
          "primary": true
        }
      ],
      "selected_object_info": {
        "alloc_hint_flags": 0,
        "data_digest": "0x2ddbf8f5",
        "expected_object_size": 0,
        "expected_write_size": 0,
        "flags": [
          "dirty",
          "omap",
          "data_digest",
          "omap_digest"
        ],
        "lost": 0,
        "manifest": {
          "type": 0
        },
        "oid": {
          "hash": 3174666125,
          "key": "",
          "max": 0,
          "namespace": "",
          "oid": "ROBJ10",
          "pool": 3,
          "snapid": -2
        },
        "omap_digest": "0xc2025a24",
        "size": 7,
        "truncate_seq": 0,
        "truncate_size": 0,
        "user_version": 30,
        "watchers": {}
      },
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "errors": [],
      "object": {
        "version": 30,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ10"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xa03cef03",
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "size": 7,
          "errors": [
            "read_error"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'52",
        "prior_version": "41'33",
        "last_reqid": "osd.1.0:51",
        "user_version": 33,
        "size": 7,
        "mtime": "2018-04-05 14:33:26.761286",
        "local_mtime": "2018-04-05 14:33:26.762368",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "read_error"
      ],
      "errors": [],
      "object": {
        "version": 33,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ11"
      }
    },
    {
      "shards": [
        {
          "errors": [
            "stat_error"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x067f306a",
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'56",
        "prior_version": "43'36",
        "last_reqid": "osd.1.0:55",
        "user_version": 36,
        "size": 7,
        "mtime": "2018-04-05 14:33:27.460958",
        "local_mtime": "2018-04-05 14:33:27.462109",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "stat_error"
      ],
      "errors": [],
      "object": {
        "version": 36,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ12"
      }
    },
    {
      "shards": [
        {
          "errors": [
            "stat_error"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "size": 7,
          "errors": [
            "read_error"
          ],
          "osd": 1,
          "primary": true
        }
      ],
      "union_shard_errors": [
        "stat_error",
        "read_error"
      ],
      "errors": [],
      "object": {
        "version": 0,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ13"
      }
    },
    {
      "shards": [
        {
          "object_info": "bad-val",
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x4f14f849",
          "size": 7,
          "errors": [
            "info_corrupted"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x4f14f849",
          "size": 7,
          "errors": [
            "info_missing"
          ],
          "osd": 1,
          "primary": true
        }
      ],
      "union_shard_errors": [
        "info_missing",
        "info_corrupted"
      ],
      "errors": [],
      "object": {
        "version": 0,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ14"
      }
    },
    {
      "shards": [
        {
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
            "version": "51'49",
            "prior_version": "49'45",
            "last_reqid": "osd.1.0:48",
            "user_version": 45,
            "size": 7,
            "mtime": "2018-04-05 14:33:29.498969",
            "local_mtime": "2018-04-05 14:33:29.499890",
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
            "watchers": {}
          },
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x2d2a4d6e",
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x2d2a4d6e",
          "size": 7,
          "errors": [
            "info_missing"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'49",
        "prior_version": "49'45",
        "last_reqid": "osd.1.0:48",
        "user_version": 45,
        "size": 7,
        "mtime": "2018-04-05 14:33:29.498969",
        "local_mtime": "2018-04-05 14:33:29.499890",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "info_missing"
      ],
      "errors": [],
      "object": {
        "version": 45,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ15"
      }
    },
    {
      "errors": [],
      "object": {
      "locator": "",
      "name": "ROBJ16",
      "nspace": "",
      "snap": "head",
      "version": 0
       },
        "shards": [
      {
        "data_digest": "0x2ddbf8f5",
        "errors": [
          "snapset_missing"
        ],
        "omap_digest": "0x8b699207",
        "osd": 0,
        "primary": false,
        "size": 7
      },
      {
        "snapset": "bad-val",
        "data_digest": "0x2ddbf8f5",
        "errors": [
          "snapset_corrupted"
        ],
        "omap_digest": "0x8b699207",
        "osd": 1,
        "primary": true,
        "size": 7
      }
      ],
      "union_shard_errors": [
        "snapset_missing",
        "snapset_corrupted"
      ]
    },
    {
     "errors": [],
     "object": {
       "locator": "",
       "name": "ROBJ17",
       "nspace": "",
       "snap": "head"
     },
     "selected_object_info": {
       "alloc_hint_flags": 0,
       "data_digest": "0x2ddbf8f5",
       "expected_object_size": 0,
       "expected_write_size": 0,
       "flags": [
         "dirty",
         "omap",
         "data_digest",
         "omap_digest"
       ],
       "lost": 0,
       "manifest": {
         "type": 0
       },
       "oid": {
         "hash": 1884071249,
         "key": "",
         "max": 0,
         "namespace": "",
         "oid": "ROBJ17",
         "pool": 3,
         "snapid": -2
       },
       "omap_digest": "0xe9572720",
       "size": 7,
       "truncate_seq": 0,
       "truncate_size": 0,
       "user_version": 51,
       "watchers": {}
     },
     "shards": [
       {
         "data_digest": "0x5af0c3ef",
         "errors": [
           "data_digest_mismatch_info"
         ],
         "omap_digest": "0xe9572720",
         "osd": 0,
         "primary": false,
         "size": 7
       },
       {
         "data_digest": "0x5af0c3ef",
         "errors": [
           "data_digest_mismatch_info"
         ],
         "omap_digest": "0xe9572720",
         "osd": 1,
         "primary": true,
         "size": 7
       }
     ],
     "union_shard_errors": [
       "data_digest_mismatch_info"
     ]
   },
   {
     "errors": [
       "object_info_inconsistency"
     ],
     "object": {
       "locator": "",
       "name": "ROBJ18",
       "nspace": "",
       "snap": "head"
     },
     "selected_object_info": {
       "alloc_hint_flags": 255,
       "data_digest": "0x2ddbf8f5",
       "expected_object_size": 0,
       "expected_write_size": 0,
       "flags": [
         "dirty",
         "omap",
         "data_digest",
         "omap_digest"
       ],
       "lost": 0,
       "manifest": {
         "type": 0
       },
       "oid": {
         "hash": 1629828556,
         "key": "",
         "max": 0,
         "namespace": "",
         "oid": "ROBJ18",
         "pool": 3,
         "snapid": -2
       },
       "omap_digest": "0xddc3680f",
       "size": 7,
       "truncate_seq": 0,
       "truncate_size": 0,
       "user_version": 54,
       "watchers": {}
     },
     "shards": [
       {
         "data_digest": "0xbd89c912",
         "errors": [
           "data_digest_mismatch_info"
         ],
         "object_info": {
           "alloc_hint_flags": 0,
           "data_digest": "0x2ddbf8f5",
           "expected_object_size": 0,
           "expected_write_size": 0,
           "flags": [
             "dirty",
             "omap",
             "data_digest",
             "omap_digest"
           ],
           "lost": 0,
           "manifest": {
             "type": 0
           },
           "oid": {
             "hash": 1629828556,
             "key": "",
             "max": 0,
             "namespace": "",
             "oid": "ROBJ18",
             "pool": 3,
             "snapid": -2
           },
           "omap_digest": "0xddc3680f",
           "size": 7,
           "truncate_seq": 0,
           "truncate_size": 0,
           "user_version": 54,
           "watchers": {}
         },
         "omap_digest": "0xddc3680f",
         "osd": 0,
         "primary": false,
         "size": 7
       },
       {
         "data_digest": "0xbd89c912",
         "errors": [
           "data_digest_mismatch_info"
         ],
         "object_info": {
           "alloc_hint_flags": 255,
           "data_digest": "0x2ddbf8f5",
           "expected_object_size": 0,
           "expected_write_size": 0,
           "flags": [
             "dirty",
             "omap",
             "data_digest",
             "omap_digest"
           ],
           "lost": 0,
           "manifest": {
             "type": 0
           },
           "oid": {
             "hash": 1629828556,
             "key": "",
             "max": 0,
             "namespace": "",
             "oid": "ROBJ18",
             "pool": 3,
             "snapid": -2
           },
           "omap_digest": "0xddc3680f",
           "size": 7,
           "truncate_seq": 0,
           "truncate_size": 0,
           "user_version": 54,
           "watchers": {}
         },
         "omap_digest": "0xddc3680f",
         "osd": 1,
         "primary": true,
         "size": 7
       }
     ],
     "union_shard_errors": [
       "data_digest_mismatch_info"
     ]
   },
   {
     "shards": [
        {
          "data_digest": "0x578a4830",
          "omap_digest": "0xf8e11918",
          "size": 7,
          "errors": [
            "data_digest_mismatch_info"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xf8e11918",
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'60",
        "prior_version": "23'6",
        "last_reqid": "osd.1.0:59",
        "user_version": 6,
        "size": 7,
        "mtime": "2018-04-05 14:33:20.498756",
        "local_mtime": "2018-04-05 14:33:20.499704",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "data_digest_mismatch_info"
      ],
      "errors": [
        "data_digest_mismatch"
      ],
      "object": {
        "version": 6,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ2"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x00b35dfd",
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "errors": [
            "missing"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'61",
        "prior_version": "25'9",
        "last_reqid": "osd.1.0:60",
        "user_version": 9,
        "size": 7,
        "mtime": "2018-04-05 14:33:21.189382",
        "local_mtime": "2018-04-05 14:33:21.190446",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "missing"
      ],
      "errors": [],
      "object": {
        "version": 9,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ3"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xd7178dfe",
          "size": 7,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xe2d46ea4",
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'62",
        "prior_version": "27'12",
        "last_reqid": "osd.1.0:61",
        "user_version": 12,
        "size": 7,
        "mtime": "2018-04-05 14:33:21.862313",
        "local_mtime": "2018-04-05 14:33:21.863261",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "errors": [
        "omap_digest_mismatch"
      ],
      "object": {
        "version": 12,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ4"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x1a862a41",
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x06cac8f6",
          "size": 7,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'63",
        "prior_version": "29'15",
        "last_reqid": "osd.1.0:62",
        "user_version": 15,
        "size": 7,
        "mtime": "2018-04-05 14:33:22.589300",
        "local_mtime": "2018-04-05 14:33:22.590376",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "errors": [
        "omap_digest_mismatch"
      ],
      "object": {
        "version": 15,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ5"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x689ee887",
          "size": 7,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x179c919f",
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'54",
        "prior_version": "31'18",
        "last_reqid": "osd.1.0:53",
        "user_version": 18,
        "size": 7,
        "mtime": "2018-04-05 14:33:23.289188",
        "local_mtime": "2018-04-05 14:33:23.290130",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "errors": [
        "omap_digest_mismatch"
      ],
      "object": {
        "version": 18,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ6"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xefced57a",
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0x6a73cc07",
          "size": 7,
          "errors": [
            "omap_digest_mismatch_info"
          ],
          "osd": 1,
          "primary": true
        }
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
        "version": "51'53",
        "prior_version": "33'21",
        "last_reqid": "osd.1.0:52",
        "user_version": 21,
        "size": 7,
        "mtime": "2018-04-05 14:33:23.979658",
        "local_mtime": "2018-04-05 14:33:23.980731",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "omap_digest_mismatch_info"
      ],
      "errors": [
        "omap_digest_mismatch"
      ],
      "object": {
        "version": 21,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ7"
      }
    },
    {
      "shards": [
        {
          "attrs": [
            {
              "Base64": false,
              "value": "bad-val",
              "name": "key1-ROBJ8"
            },
            {
              "Base64": false,
              "value": "val2-ROBJ8",
              "name": "key2-ROBJ8"
            }
          ],
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xd6be81dc",
          "size": 7,
          "errors": [],
          "osd": 0,
          "primary": false
        },
        {
          "attrs": [
            {
              "Base64": false,
              "value": "val1-ROBJ8",
              "name": "key1-ROBJ8"
            },
            {
              "Base64": false,
              "value": "val3-ROBJ8",
              "name": "key3-ROBJ8"
            }
          ],
          "data_digest": "0x2ddbf8f5",
          "omap_digest": "0xd6be81dc",
          "size": 7,
          "errors": [],
          "osd": 1,
          "primary": true
        }
      ],
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
        "version": "79'66",
        "prior_version": "79'65",
        "last_reqid": "client.4554.0:1",
        "user_version": 74,
        "size": 7,
        "mtime": "2018-04-05 14:34:05.598688",
        "local_mtime": "2018-04-05 14:34:05.599698",
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
        "watchers": {}
      },
      "union_shard_errors": [],
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "object": {
        "version": 66,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ8"
      }
    },
    {
      "shards": [
        {
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
            "version": "51'64",
            "prior_version": "37'27",
            "last_reqid": "osd.1.0:63",
            "user_version": 27,
            "size": 7,
            "mtime": "2018-04-05 14:33:25.352485",
            "local_mtime": "2018-04-05 14:33:25.353746",
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
            "watchers": {}
          },
          "data_digest": "0x1f26fb26",
          "omap_digest": "0x2eecc539",
          "size": 3,
          "errors": [
            "obj_size_info_mismatch"
          ],
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "119'68",
            "prior_version": "51'64",
            "last_reqid": "client.4834.0:1",
            "user_version": 76,
            "size": 3,
            "mtime": "2018-04-05 14:35:01.500659",
            "local_mtime": "2018-04-05 14:35:01.502117",
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
            "watchers": {}
          },
          "data_digest": "0x1f26fb26",
          "omap_digest": "0x2eecc539",
          "size": 3,
          "errors": [],
          "osd": 1,
          "primary": true
        }
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
        "version": "119'68",
        "prior_version": "51'64",
        "last_reqid": "client.4834.0:1",
        "user_version": 76,
        "size": 3,
        "mtime": "2018-04-05 14:35:01.500659",
        "local_mtime": "2018-04-05 14:35:01.502117",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "obj_size_info_mismatch"
      ],
      "errors": [
        "object_info_inconsistency"
      ],
      "object": {
        "version": 68,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "ROBJ9"
      }
    }
  ],
  "epoch": 0
}
EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python -c "$sortkeys" > $dir/csjson
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
    teardown $dir || return 1
}


#
# Test scrub errors for an erasure coded pool
#
function corrupt_scrub_erasure() {
    local dir=$1
    local allow_overwrites=$2
    local poolname=ecpool
    local total_objs=7

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 2) ; do
	if [ "$allow_overwrites" = "true" ]; then
            run_osd_bluestore $dir $id || return 1
	else
            run_osd $dir $id || return 1
	fi
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

    jq "$jqfilter" << EOF | jq '.inconsistents' | python -c "$sortkeys" > $dir/checkcsjson
{
  "inconsistents": [
    {
      "shards": [
        {
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "27'1",
            "prior_version": "0'0",
            "last_reqid": "client.4184.0:1",
            "user_version": 1,
            "size": 7,
            "mtime": "",
            "local_mtime": "",
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
            "watchers": {}
          },
          "size": 9,
          "shard": 0,
          "errors": [
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        },
        {
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "27'1",
        "prior_version": "0'0",
        "last_reqid": "client.4184.0:1",
        "user_version": 1,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "size_mismatch"
      ],
      "object": {
        "version": 1,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ1"
      }
    },
    {
      "shards": [
        {
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
          "shard": 0,
          "errors": [
            "missing"
          ],
          "osd": 1,
          "primary": true
        },
        {
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "39'3",
        "prior_version": "0'0",
        "last_reqid": "client.4252.0:1",
        "user_version": 3,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "missing"
      ],
      "errors": [],
      "object": {
        "version": 3,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ3"
      }
    },
    {
      "shards": [
        {
          "attrs": [
            {
              "Base64": false,
              "value": "bad-val",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val2-EOBJ4",
              "name": "key2-EOBJ4"
            }
          ],
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
          "osd": 1,
          "primary": true,
          "shard": 0,
          "errors": [],
          "size": 2048,
          "attrs": [
            {
              "Base64": false,
              "value": "val1-EOBJ4",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val2-EOBJ4",
              "name": "key2-EOBJ4"
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
              "Base64": false,
              "value": "val1-EOBJ4",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val3-EOBJ4",
              "name": "key3-EOBJ4"
            }
          ]
        }
      ],
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
        "version": "45'6",
        "prior_version": "45'5",
        "last_reqid": "client.4294.0:1",
        "user_version": 6,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [],
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "object": {
        "version": 6,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ4"
      }
    },
    {
      "shards": [
        {
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "59'7",
            "prior_version": "0'0",
            "last_reqid": "client.4382.0:1",
            "user_version": 7,
            "size": 7,
            "mtime": "",
            "local_mtime": "",
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
            "watchers": {}
          },
          "size": 4096,
          "shard": 0,
          "errors": [
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        },
        {
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "59'7",
        "prior_version": "0'0",
        "last_reqid": "client.4382.0:1",
        "user_version": 7,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "size_mismatch"
      ],
      "object": {
        "version": 7,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ5"
      }
   },
   {
     "errors": [],
     "object": {
       "locator": "",
       "name": "EOBJ6",
       "nspace": "",
       "snap": "head",
       "version": 8
     },
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
        "version": "65'8",
        "prior_version": "0'0",
        "last_reqid": "client.4418.0:1",
        "user_version": 8,
        "size": 7,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
     },
     "shards": [
       {
         "errors": [
           "hinfo_missing"
         ],
         "osd": 0,
         "primary": false,
         "shard": 2,
         "size": 2048
       },
       {
         "errors": [
           "hinfo_corrupted"
         ],
         "osd": 1,
         "primary": true,
         "shard": 0,
         "hashinfo": "bad-val",
         "size": 2048
       },
       {
         "errors": [],
         "osd": 2,
         "primary": false,
         "shard": 1,
         "size": 2048,
         "hashinfo": {
           "cumulative_shard_hashes": [
            {
              "hash": 80717615,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 80717615,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
         }
       }
     ],
     "union_shard_errors": [
       "hinfo_missing",
       "hinfo_corrupted"
     ]
   },
   {
     "errors": [
       "hinfo_inconsistency"
     ],
     "object": {
       "locator": "",
       "name": "EOBJ7",
       "nspace": "",
       "snap": "head",
       "version": 10
     },
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
        "version": "75'10",
        "prior_version": "75'9",
        "last_reqid": "client.4482.0:1",
        "user_version": 10,
        "size": 34,
        "mtime": "",
        "local_mtime": "",
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
        "watchers": {}
     },
     "shards": [
       {
         "hashinfo": {
           "cumulative_shard_hashes": [
            {
              "hash": 80717615,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 80717615,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
         },
         "errors": [],
         "osd": 0,
         "primary": false,
         "shard": 2,
         "size": 2048
       },
       {
         "hashinfo": {
           "cumulative_shard_hashes": [
            {
              "hash": 1534350760,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 1534350760,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
         },
         "errors": [],
         "osd": 1,
         "primary": true,
         "shard": 0,
         "size": 2048
       },
       {
         "hashinfo": {
           "cumulative_shard_hashes": [
            {
              "hash": 1534350760,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 1534350760,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
         },
         "errors": [],
         "osd": 2,
         "primary": false,
         "shard": 1,
         "size": 2048
       }
     ],
     "union_shard_errors": []
    }
  ],
  "epoch": 0
}
EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python -c "$sortkeys" > $dir/csjson
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
    # Get epoch for repair-get requests
    epoch=$(jq .epoch $dir/json)

    if [ "$allow_overwrites" = "true" ]
    then
      jq "$jqfilter" << EOF | jq '.inconsistents' | python -c "$sortkeys" > $dir/checkcsjson
{
  "inconsistents": [
    {
      "shards": [
        {
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "27'1",
            "prior_version": "0'0",
            "last_reqid": "client.4184.0:1",
            "user_version": 1,
            "size": 7,
            "mtime": "2018-04-05 14:31:33.837147",
            "local_mtime": "2018-04-05 14:31:33.840763",
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
            "watchers": {}
          },
          "size": 9,
          "shard": 0,
          "errors": [
            "read_error",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        },
        {
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "27'1",
        "prior_version": "0'0",
        "last_reqid": "client.4184.0:1",
        "user_version": 1,
        "size": 7,
        "mtime": "2018-04-05 14:31:33.837147",
        "local_mtime": "2018-04-05 14:31:33.840763",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "read_error",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "size_mismatch"
      ],
      "object": {
        "version": 1,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ1"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
          "shard": 0,
          "errors": [
            "missing"
          ],
          "osd": 1,
          "primary": true
        },
        {
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "39'3",
        "prior_version": "0'0",
        "last_reqid": "client.4252.0:1",
        "user_version": 3,
        "size": 7,
        "mtime": "2018-04-05 14:31:46.841145",
        "local_mtime": "2018-04-05 14:31:46.844996",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "missing"
      ],
      "errors": [],
      "object": {
        "version": 3,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ3"
      }
    },
    {
      "shards": [
        {
          "attrs": [
            {
              "Base64": false,
              "value": "bad-val",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val2-EOBJ4",
              "name": "key2-EOBJ4"
            }
          ],
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
          "attrs": [
            {
              "Base64": false,
              "value": "val1-EOBJ4",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val2-EOBJ4",
              "name": "key2-EOBJ4"
            }
          ],
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 0,
          "osd": 1,
          "primary": true
        },
        {
          "attrs": [
            {
              "Base64": false,
              "value": "val1-EOBJ4",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val3-EOBJ4",
              "name": "key3-EOBJ4"
            }
          ],
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 1,
          "osd": 2,
          "primary": false
        }
      ],
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
        "version": "45'6",
        "prior_version": "45'5",
        "last_reqid": "client.4294.0:1",
        "user_version": 6,
        "size": 7,
        "mtime": "2018-04-05 14:31:54.663622",
        "local_mtime": "2018-04-05 14:31:54.664527",
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
        "watchers": {}
      },
      "union_shard_errors": [],
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "object": {
        "version": 6,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ4"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
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
            "version": "59'7",
            "prior_version": "0'0",
            "last_reqid": "client.4382.0:1",
            "user_version": 7,
            "size": 7,
            "mtime": "2018-04-05 14:32:12.929161",
            "local_mtime": "2018-04-05 14:32:12.934707",
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
            "watchers": {}
          },
          "size": 4096,
          "errors": [
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "shard": 0,
          "osd": 1,
          "primary": true
        },
        {
          "data_digest": "0x00000000",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 1,
          "osd": 2,
          "primary": false
        }
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
        "version": "59'7",
        "prior_version": "0'0",
        "last_reqid": "client.4382.0:1",
        "user_version": 7,
        "size": 7,
        "mtime": "2018-04-05 14:32:12.929161",
        "local_mtime": "2018-04-05 14:32:12.934707",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "size_mismatch"
      ],
      "object": {
        "version": 7,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ5"
      }
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
        "version": "65'8",
        "prior_version": "0'0",
        "last_reqid": "client.4418.0:1",
        "user_version": 8,
        "size": 7,
        "mtime": "2018-04-05 14:32:20.634116",
        "local_mtime": "2018-04-05 14:32:20.637999",
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
        "watchers": {}
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
            "cumulative_shard_hashes": [
            {
              "hash": 80717615,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 80717615,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
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
        "version": "75'10",
        "prior_version": "75'9",
        "last_reqid": "client.4482.0:1",
        "user_version": 10,
        "size": 34,
        "mtime": "2018-04-05 14:32:33.058782",
        "local_mtime": "2018-04-05 14:32:33.059679",
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
        "watchers": {}
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
           "cumulative_shard_hashes": [
            {
              "hash": 80717615,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 80717615,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
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
           "cumulative_shard_hashes": [
            {
              "hash": 1534350760,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 1534350760,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
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
           "cumulative_shard_hashes": [
            {
              "hash": 1534350760,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 1534350760,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
          }
        }
      ]
    }
  ],
  "epoch": 0
}
EOF

    else

      jq "$jqfilter" << EOF | jq '.inconsistents' | python -c "$sortkeys" > $dir/checkcsjson
{
  "inconsistents": [
    {
      "shards": [
        {
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "27'1",
            "prior_version": "0'0",
            "last_reqid": "client.4192.0:1",
            "user_version": 1,
            "size": 7,
            "mtime": "2018-04-05 14:30:10.688009",
            "local_mtime": "2018-04-05 14:30:10.691774",
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
            "watchers": {}
          },
          "size": 9,
          "shard": 0,
          "errors": [
            "read_error",
            "size_mismatch_info",
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        },
        {
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "27'1",
        "prior_version": "0'0",
        "last_reqid": "client.4192.0:1",
        "user_version": 1,
        "size": 7,
        "mtime": "2018-04-05 14:30:10.688009",
        "local_mtime": "2018-04-05 14:30:10.691774",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "read_error",
        "size_mismatch_info",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "size_mismatch"
      ],
      "object": {
        "version": 1,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ1"
      }
    },
    {
      "shards": [
        {
          "size": 2048,
          "errors": [
            "ec_hash_error"
          ],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 0,
          "osd": 1,
          "primary": true
        },
        {
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 1,
          "osd": 2,
          "primary": false
        }
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
        "version": "33'2",
        "prior_version": "0'0",
        "last_reqid": "client.4224.0:1",
        "user_version": 2,
        "size": 7,
        "mtime": "2018-04-05 14:30:14.152945",
        "local_mtime": "2018-04-05 14:30:14.154014",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "ec_hash_error"
      ],
      "errors": [],
      "object": {
        "version": 2,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ2"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
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
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "39'3",
        "prior_version": "0'0",
        "last_reqid": "client.4258.0:1",
        "user_version": 3,
        "size": 7,
        "mtime": "2018-04-05 14:30:18.875544",
        "local_mtime": "2018-04-05 14:30:18.880153",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "missing"
      ],
      "errors": [],
      "object": {
        "version": 3,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ3"
      }
    },
    {
      "shards": [
        {
          "attrs": [
            {
              "Base64": false,
              "value": "bad-val",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val2-EOBJ4",
              "name": "key2-EOBJ4"
            }
          ],
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
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
              "Base64": false,
              "value": "val1-EOBJ4",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val2-EOBJ4",
              "name": "key2-EOBJ4"
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
              "Base64": false,
              "value": "val1-EOBJ4",
              "name": "key1-EOBJ4"
            },
            {
              "Base64": false,
              "value": "val3-EOBJ4",
              "name": "key3-EOBJ4"
            }
          ]
        }
      ],
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
        "version": "45'6",
        "prior_version": "45'5",
        "last_reqid": "client.4296.0:1",
        "user_version": 6,
        "size": 7,
        "mtime": "2018-04-05 14:30:22.271983",
        "local_mtime": "2018-04-05 14:30:22.272840",
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
        "watchers": {}
      },
      "union_shard_errors": [],
      "errors": [
        "attr_value_mismatch",
        "attr_name_mismatch"
      ],
      "object": {
        "version": 6,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ4"
      }
    },
    {
      "shards": [
        {
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "errors": [],
          "shard": 2,
          "osd": 0,
          "primary": false
        },
        {
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
            "version": "59'7",
            "prior_version": "0'0",
            "last_reqid": "client.4384.0:1",
            "user_version": 7,
            "size": 7,
            "mtime": "2018-04-05 14:30:35.162395",
            "local_mtime": "2018-04-05 14:30:35.166390",
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
            "watchers": {}
          },
          "size": 4096,
          "shard": 0,
          "errors": [
            "size_mismatch_info",
            "ec_size_error",
            "obj_size_info_mismatch"
          ],
          "osd": 1,
          "primary": true
        },
        {
          "data_digest": "0x04cfa72f",
          "omap_digest": "0xffffffff",
          "size": 2048,
          "shard": 1,
          "errors": [],
          "osd": 2,
          "primary": false
        }
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
        "version": "59'7",
        "prior_version": "0'0",
        "last_reqid": "client.4384.0:1",
        "user_version": 7,
        "size": 7,
        "mtime": "2018-04-05 14:30:35.162395",
        "local_mtime": "2018-04-05 14:30:35.166390",
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
        "watchers": {}
      },
      "union_shard_errors": [
        "size_mismatch_info",
        "ec_size_error",
        "obj_size_info_mismatch"
      ],
      "errors": [
        "size_mismatch"
      ],
      "object": {
        "version": 7,
        "snap": "head",
        "locator": "",
        "nspace": "",
        "name": "EOBJ5"
      }
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
        "version": "65'8",
        "prior_version": "0'0",
        "last_reqid": "client.4420.0:1",
        "user_version": 8,
        "size": 7,
        "mtime": "2018-04-05 14:30:40.914673",
        "local_mtime": "2018-04-05 14:30:40.917705",
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
        "watchers": {}
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
           "cumulative_shard_hashes": [
            {
              "hash": 80717615,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 80717615,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
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
        "version": "75'10",
        "prior_version": "75'9",
        "last_reqid": "client.4486.0:1",
        "user_version": 10,
        "size": 34,
        "mtime": "2018-04-05 14:30:50.995009",
        "local_mtime": "2018-04-05 14:30:50.996112",
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
        "watchers": {}
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
           "cumulative_shard_hashes": [
            {
              "hash": 80717615,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 80717615,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
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
           "cumulative_shard_hashes": [
            {
              "hash": 1534350760,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 1534350760,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
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
           "cumulative_shard_hashes": [
            {
              "hash": 1534350760,
              "shard": 0
            },
            {
              "hash": 1534491824,
              "shard": 1
            },
            {
              "hash": 1534350760,
              "shard": 2
            }
           ],
           "total_chunk_size": 2048
          }
        }
      ]
    }
  ],
  "epoch": 0
}
EOF

    fi

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python -c "$sortkeys" > $dir/csjson
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
    teardown $dir || return 1
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
#
function TEST_periodic_scrub_replicated() {
    local dir=$1
    local poolname=psr_pool
    local objname=POBJ

    setup $dir || return 1
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
    # Fake a schedule scrub
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
             trigger_scrub $pg || return 1
    # Wait for schedule regular scrub
    wait_for_scrub $pg "$last_scrub"

    # It needed to be upgraded
    grep -q "Deep scrub errors, upgrading scrub to deep-scrub" $dir/osd.${primary}.log || return 1

    # Bad object still known
    rados list-inconsistent-obj $pg | jq '.' | grep -q $objname || return 1

    # Can't upgrade with this set
    ceph osd set nodeep-scrub
    # Let map change propagate to OSDs
    flush_pg_stats
    sleep 5

    # Fake a schedule scrub
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
             trigger_scrub $pg || return 1
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

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
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
      CEPH_ARGS='' ceph daemon $(get_asok_path osd.${primary}) \
             trigger_scrub ${i}.0 $(expr ${overdue_seconds} + ${i}00) || return 1
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
      CEPH_ARGS='' ceph daemon $(get_asok_path osd.${primary}) \
             trigger_deep_scrub ${i}.0 $(expr ${overdue_seconds} + ${i}00) || return 1
    done
    flush_pg_stats

    ceph health
    ceph health detail
    ceph health | grep -q "$deep_scrubs pgs not deep-scrubbed in time" || return 1
    ceph health | grep -q "$scrubs pgs not scrubbed in time" || return 1
    COUNT=$(ceph health detail | grep "not scrubbed since" | wc -l)
    if [ "$COUNT" != $scrubs ]; then
      ceph health detail | grep "not scrubbed since"
      return 1
    fi
    COUNT=$(ceph health detail | grep "not deep-scrubbed since" | wc -l)
    if [ "$COUNT" != $deep_scrubs ]; then
      ceph health detail | grep "not deep-scrubbed since"
      return 1
    fi
    return 0
}

#
# Corrupt snapset in replicated pool
#
function TEST_corrupt_snapset_scrub_rep() {
    local dir=$1
    local poolname=csr_pool
    local total_objs=2

    setup $dir || return 1
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

    jq "$jqfilter" << EOF | jq '.inconsistents' | python -c "$sortkeys" > $dir/checkcsjson
{
  "epoch": 34,
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
        "version": "24'8",
        "prior_version": "21'3",
        "last_reqid": "client.4195.0:1",
        "user_version": 8,
        "size": 21,
        "mtime": "2018-04-05 14:35:43.286117",
        "local_mtime": "2018-04-05 14:35:43.288990",
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
        "watchers": {}
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 21,
          "snapset": {
            "clones": [
              {
                "overlap": "[]",
                "size": 7,
                "snap": 1,
                "snaps": [
                  1
                ]
              }
            ],
            "snap_context": {
              "seq": 1,
              "snaps": [
                1
              ]
            }
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 21,
          "snapset": {
            "clones": [],
            "snap_context": {
              "seq": 0,
              "snaps": []
            }
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
        "version": "28'10",
        "prior_version": "23'6",
        "last_reqid": "client.4223.0:1",
        "user_version": 10,
        "size": 21,
        "mtime": "2018-04-05 14:35:48.326856",
        "local_mtime": "2018-04-05 14:35:48.328097",
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
        "watchers": {}
      },
      "shards": [
        {
          "osd": 0,
          "primary": false,
          "errors": [],
          "size": 21,
          "snapset": {
            "clones": [],
            "snap_context": {
              "seq": 0,
              "snaps": []
            }
          }
        },
        {
          "osd": 1,
          "primary": true,
          "errors": [],
          "size": 21,
          "snapset": {
            "clones": [
              {
                "overlap": "[]",
                "size": 7,
                "snap": 1,
                "snaps": [
                  1
                ]
              }
            ],
            "snap_context": {
              "seq": 1,
              "snaps": [
                1
              ]
            }
          }
        }
      ]
    }
  ]
}
EOF

    jq "$jqfilter" $dir/json | jq '.inconsistents' | python -c "$sortkeys" > $dir/csjson
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
    teardown $dir || return 1
}

function TEST_request_scrub_priority() {
    local dir=$1
    local poolname=psr_pool
    local objname=POBJ
    local OBJECTS=64
    local PGS=8

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
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
        CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${primary}) \
             trigger_scrub $opg || return 1
    done

    sleep 15
    flush_pg_stats

    # Request a regular scrub and it will be done
    local last_scrub=$(get_last_scrub_stamp $pg)
    ceph pg scrub $pg

    ceph osd unset noscrub || return 1
    ceph osd unset nodeep-scrub || return 1

    wait_for_scrub $pg "$last_scrub"

    for opg in $otherpgs $pg
    do
        wait_for_scrub $opg "$other_last_scrub"
    done

    # Verify that the requested scrub ran first
    grep "log_channel.*scrub ok" $dir/osd.${primary}.log | head -1 | sed 's/.*[[]DBG[]]//' | grep -q $pg || return 1

    return 0
}


main osd-scrub-repair "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#    ../qa/run-standalone.sh osd-scrub-repair.sh"
# End:
