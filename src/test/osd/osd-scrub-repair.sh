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
source $CEPH_ROOT/qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7107" # git grep '\<7107\>' : there must be only one
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
    local obj=${3:-SOMETHING}

    wait_for_clean || return 1

    ceph osd set noscrub || return 1
    ceph osd set nodeep-scrub || return 1

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
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1

    add_something $dir $poolname
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

    wait_for_clean || return 1
}

function corrupt_and_repair_erasure_coded() {
    local dir=$1
    local poolname=$2
    local profile=$3

    ceph osd pool create $poolname 1 1 erasure $profile \
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

}

function TEST_auto_repair_erasure_coded() {
    local dir=$1
    local poolname=ecpool

    # Launch a cluster with 5 seconds scrub interval
    setup $dir || return 1
    run_mon $dir a || return 1
    for id in $(seq 0 2) ; do
        run_osd $dir $id \
            --osd-scrub-auto-repair=true \
            --osd-deep-scrub-interval=5 \
            --osd-scrub-max-interval=5 \
            --osd-scrub-min-interval=5 \
            --osd-scrub-interval-randomize-ratio=0
    done

    # Create an EC pool
    ceph osd erasure-code-profile set myprofile \
        k=2 m=1 ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 8 8 erasure myprofile || return 1

    # Put an object
    local payload=ABCDEF
    echo $payload > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1
    wait_for_clean || return 1

    # Remove the object from one shard physically
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING remove || return 1
    # Wait for auto repair
    local pgid=$(get_pg $poolname SOMETHING)
    wait_for_scrub $pgid "$(get_last_scrub_stamp $pgid)"
    wait_for_clean || return 1
    # Verify - the file should be back
    objectstore_tool $dir $(get_not_primary $poolname SOMETHING) SOMETHING list-attrs || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1

    # Tear down
    teardown $dir || return 1
}

function TEST_corrupt_and_repair_jerasure() {
    local dir=$1
    local poolname=ecpool
    local profile=myprofile

    setup $dir || return 1
    run_mon $dir a || return 1
    for id in $(seq 0 3) ; do
        run_osd $dir $id || return 1
    done
    wait_for_clean || return 1

    ceph osd erasure-code-profile set $profile \
        k=2 m=2 ruleset-failure-domain=osd || return 1

    corrupt_and_repair_erasure_coded $dir $poolname $profile || return 1

    teardown $dir || return 1
}

function TEST_corrupt_and_repair_lrc() {
    local dir=$1
    local poolname=ecpool
    local profile=myprofile

    setup $dir || return 1
    run_mon $dir a || return 1
    for id in $(seq 0 9) ; do
        run_osd $dir $id || return 1
    done
    wait_for_clean || return 1

    ceph osd erasure-code-profile set $profile \
        pluing=lrc \
        k=4 m=2 l=3 \
        ruleset-failure-domain=osd || return 1

    corrupt_and_repair_erasure_coded $dir $poolname $profile || return 1

    teardown $dir || return 1
}

function TEST_unfound_erasure_coded() {
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
    ceph -s|grep "4 osds: 4 up, 4 in" || return 1
    ceph -s|grep "1/1 unfound" || return 1

    teardown $dir || return 1
}

#
# list_missing for EC pool
#
function TEST_list_missing_erasure_coded() {
    local dir=$1
    local poolname=ecpool
    local profile=myprofile

    setup $dir || return 1
    run_mon $dir a || return 1
    for id in $(seq 0 2) ; do
        run_osd $dir $id || return 1
    done
    wait_for_clean || return 1

    ceph osd erasure-code-profile set $profile \
        k=2 m=1 ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 1 1 erasure $profile \
        || return 1
    wait_for_clean || return 1

    # Put an object and remove the two shards (including primary)
    add_something $dir $poolname OBJ0 || return 1
    local -a osds=($(get_osds $poolname OBJ0))

    pids=""
    run_in_background pids objectstore_tool $dir ${osds[0]} OBJ0 remove
    run_in_background pids objectstore_tool $dir ${osds[1]} OBJ0 remove
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi


    # Put another object and remove two shards (excluding primary)
    add_something $dir $poolname OBJ1 || return 1
    local -a osds=($(get_osds $poolname OBJ1))

    pids=""
    run_in_background pids objectstore_tool $dir ${osds[1]} OBJ1 remove
    run_in_background pids objectstore_tool $dir ${osds[2]} OBJ1 remove
    wait_background pids
    return_code=$?
    if [ $return_code -ne 0 ]; then return $return_code; fi


    # Get get - both objects should in the same PG
    local pg=$(get_pg $poolname OBJ0)

    # Repair the PG, which triggers the recovering,
    # and should mark the object as unfound
    ceph pg repair $pg
    
    for i in $(seq 0 120) ; do
        [ $i -lt 60 ] || return 1
        matches=$(ceph pg $pg list_missing | egrep "OBJ0|OBJ1" | wc -l)
        [ $matches -eq 2 ] && break
    done

    teardown $dir || return 1
}


main osd-scrub-repair "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#    test/osd/osd-scrub-repair.sh # TEST_corrupt_and_repair_replicated"
# End:
