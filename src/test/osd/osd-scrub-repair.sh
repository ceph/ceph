#!/bin/bash
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
source test/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7107"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function add_something() {
    local dir=$1
    local poolname=$2

    wait_for_clean || return 1

    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1

    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1
}

#
# Corrupt one copy of a replicated pool
#
function TEST_corrupt_and_repair_replicated() {
    local dir=$1
    local poolname=rbd

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=2 || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    add_something $dir $poolname
    corrupt_and_repair_one $dir $poolname $(get_not_primary $poolname SOMETHING) || return 1
    # Reproduces http://tracker.ceph.com/issues/8914
    corrupt_and_repair_one $dir $poolname $(get_primary $poolname SOMETHING) || return 1

    teardown $dir || return 1
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

    wait_for_clean || return 1
}

function TEST_corrupt_and_repair_erasure_coded() {
    local dir=$1
    local poolname=ecpool
    local payload=ABCDEF

    setup $dir || return 1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    wait_for_clean || return 1

    ceph osd erasure-code-profile set myprofile \
        k=2 m=2 ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 1 1 erasure myprofile \
        || return 1

    add_something $dir $poolname

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

    teardown $dir || return 1
}

function TEST_unreocvery_erasure_coded() {
    local dir=$1
    local poolname=ecpool
    local payload=ABCDEF

    setup $dir || return 1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    wait_for_clean || return 1

    ceph osd erasure-code-profile set myprofile \
      k=2 m=2 ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 1 1 erasure myprofile \
      || return 1

    add_something $dir $poolname

    local primary=$(get_primary $poolname SOMETHING)
    local -a osds=($(get_osds $poolname SOMETHING | sed -e "s/$primary//"))
    local not_primary_first=${osds[0]}
    local not_primary_second=${osds[1]}
    local not_primary_third=${osds[2]}

    #
    # 1) remove the corresponding file from the OSDs
    #
    objectstore_tool $dir $not_primary_first SOMETHING remove || return 1
    objectstore_tool $dir $not_primary_second SOMETHING remove || return 1
    objectstore_tool $dir $not_primary_third SOMETHING remove || return 1
    #
    # 2) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    repair $pg
    #
    # 3) check pg state
    #
    ceph -s|grep "4 osds: 4 up, 4 in" || return 1
    ceph -s|grep "1/1 unfound" || return 1

    teardown $dir || return 1
}

#######################################################################

##
# Return num of objects for **pgid**,
#
# @param pgid the id of the PG
# @param STDOUT the date and num of objects
# @return 0 on success, 1 on error
#
function get_pg_objects_num() {
    local pgid=$1
    ceph --format xml pg dump pgs 2>/dev/null | \
        $XMLSTARLET sel -t -m "//pg_stat[pgid='$pgid']/stat_sum/num_objects" -v .
}

#######################################################################

##
# Return state for **pgid**,
#
# @param pgid the id of the PG
# @param state of objects
# @return 0 on success, 1 on error
#
function get_pg_state() {
    local pgid=$1
    ceph --format xml pg dump pgs 2>/dev/null | \
        $XMLSTARLET sel -t -m "//pg_stat[pgid='$pgid']/state" -v .
}
#######################################################################

##
# Run stop_scrub on **pgid** and wait until it completes.
# The stop_scrub return success whenever the
# pg enter scrubbing state and end up with **get_last_scrub_stamp** function
# reports a timestamp the same with
# the one stored before starting the stop_scrub.
#
# @param pgid the id of the PG
# @return 0 on success, 1 on error
#
function stop_scrub() {
    local dir=$1
    local pgid=$2
    local last_scrub=$(get_last_scrub_stamp $pgid)
    local tmpfile="$dir/XXXXX"

    echo "XXXXX" > "$tmpfile"
    for i in `seq 0 1000`; do
       ## put objects to rbd pool
       rados put "$i" "$tmpfile" -p rbd
       pg_objects=$(get_pg_objects_num $pgid)
       if [ $pg_objects -ge 4 ] ; then
           break
       fi
    done

    ceph pg scrub $pgid
    scrubbing="NO"
    for ((i=0; i < $TIMEOUT; i++)); do
        # check pg is scrubbing or not
        state=$(get_pg_state $pgid)
        if echo "$state" | grep "scrubbing" ; then
            scrubbing="YES"
            break
        fi
        sleep 1
    done

    if test "$scrubbing" != "YES" ; then
      return 1
    fi

    ceph pg stop-scrub $pgid
    for ((i=0; i < $TIMEOUT; i++)); do
        state=$(get_pg_state $pgid)
        if echo "$state" | grep "scrubbing" ; then
            sleep 1
            continue
        fi
        if test "$last_scrub" == "$(get_last_scrub_stamp $pgid)" ; then
            return 0
        else
            return 1
        fi
    done
    return 1
}

function TEST_stop_scrub() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    run_osd $dir 0 --osd_scrub_sleep=10 --osd_scrub_chunk_min=1 --osd_scrub_chunk_max=1 || return 1
    wait_for_clean || return 1
    ceph osd set noscrub
    stop_scrub $dir 1.0 || return 1
    kill_daemons $dir KILL osd || return 1
    teardown $dir || return 1
}

#######################################################################

##
# The ceph osd set noscrub return success whenever the
# pg enter scrubbing state and end up with **get_last_scrub_stamp** function
# reports a timestamp the same with
# the one stored before starting the noscrub.
#
# @return 0 on success, 1 on error
#
function no_scrub() {
    local dir=$1
    pgs=$(ceph --format xml pg ls 2>/dev/null | xmlstarlet sel -t -m "//pg_stat/pgid" -v . -o ' ')
    local tmpfile="$dir/XXXXX"

    echo "XXXXX" > "$tmpfile"
    for i in `seq 0 1000`; do
        ## put objects to rbd pool
        rados put "$i" "$tmpfile" -p rbd
        all_pg_num_enought="YES"
        for pg in $pgs; do # number of every pg is equal or more than 4
            pg_objects=$(get_pg_objects_num $pg)
            if [ $pg_objects -lt 4 ]; then
              all_pg_num_enought="NO"
              break
            fi
        done
        if test "$all_pg_num_enought" == "YES" ; then
            break
        fi
    done

    ## schedule pg scrubbing right now
    ceph osd unset noscrub
    ceph tell osd.* injectargs "--osd_scrub_min_interval 0 --osd_scrub_max_interval 0"

    scrubbing="NO"
    local pgid=""
    local last_scrub=""
    for ((i=0; i < $TIMEOUT; i++)); do
        # check pg is scrubbing or not
        for pg in $pgs; do
            state=$(get_pg_state $pg)
            if echo "$state" | grep -v "deep" | grep "scrubbing"; then # doing scrub
                scrubbing="YES"
                pgid=$pg
                last_scrub=$(get_last_scrub_stamp $pgid)
                break
            fi
        done
        if [ -z "$pgid" ]; then
            sleep 1
        else
            break
        fi
    done

    if test "$scrubbing" != "YES" ; then
        return 1
    fi

    if [ -z "$pgid"] ; then
        return 1
    fi

    ## disable scheduled scrubbing on all OSDs **stops running scheduled scrubs
    ceph osd set noscrub

    for ((i=0; i < $TIMEOUT; i++)); do
        state=$(get_pg_state $pgid)
        if echo "$state" | grep "scrubbing" ; then ## wait for finish scrub
            sleep 1
            continue
        fi
        if test "$last_scrub" == "$(get_last_scrub_stamp $pgid)" ; then
            return 0
        else
            return 1
        fi
    done
    return 1
}
function TEST_no_scrub() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a --osd_pool_default_size=1 || return 1
    ceph osd set noscrub
    run_osd $dir 0 --osd_scrub_sleep=10 --osd_scrub_chunk_min=1 --osd_scrub_chunk_max=1 || return 1
    wait_for_clean || return 1
    no_scrub $dir || return 1
    kill_daemons $dir KILL osd || return 1
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
    objectstore_tool $dir $first SOMETHING remove || return 1
    objectstore_tool $dir $second SOMETHING remove || return 1
    #
    # 2) repair the PG
    #
    local pg=$(get_pg $poolname SOMETHING)
    repair $pg
    #
    # 3) The files must be back
    #
    objectstore_tool $dir $first SOMETHING list-attrs || return 1
    objectstore_tool $dir $second SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
}

main osd-scrub-repair "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#    test/osd/osd-scrub-repair.sh # TEST_corrupt_and_repair_replicated"
# End:
