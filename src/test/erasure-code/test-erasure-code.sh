#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
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

    export CEPH_MON="127.0.0.1:7101" # git grep '\<7101\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON --mon-osd-prime-pg-temp=false"

    setup $dir || return 1
    run_mon $dir a || return 1
    # check that erasure code plugins are preloaded
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
    grep 'load: jerasure.*lrc' $dir/mon.a.log || return 1
    for id in $(seq 0 10) ; do
        run_osd $dir $id || return 1
    done
    wait_for_clean || return 1
    # check that erasure code plugins are preloaded
    CEPH_ARGS='' ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'load: jerasure.*lrc' $dir/osd.0.log || return 1
    create_erasure_coded_pool ecpool || return 1

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done

    delete_pool ecpool || return 1
    teardown $dir || return 1
}

function create_erasure_coded_pool() {
    local poolname=$1

    ceph osd erasure-code-profile set myprofile \
        ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 12 12 erasure myprofile \
        || return 1
    wait_for_clean || return 1
}

function delete_pool() {
    local poolname=$1

    ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it
}

function rados_put_get() {
    local dir=$1
    local poolname=$2
    local objname=${3:-SOMETHING}


    for marker in AAA BBB CCCC DDDD ; do
        printf "%*s" 1024 $marker
    done > $dir/ORIGINAL

    #
    # get and put an object, compare they are equal
    #
    rados --pool $poolname put $objname $dir/ORIGINAL || return 1
    rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    rm $dir/COPY

    #
    # take out an OSD used to store the object and
    # check the object can still be retrieved, which implies
    # recovery
    #
    local -a initial_osds=($(get_osds $poolname $objname))
    local last=$((${#initial_osds[@]} - 1))
    ceph osd out ${initial_osds[$last]} || return 1
    ! get_osds $poolname $objname | grep '\<'${initial_osds[$last]}'\>' || return 1
    rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    ceph osd in ${initial_osds[$last]} || return 1

    rm $dir/ORIGINAL
}

function rados_osds_out_in() {
    local dir=$1
    local poolname=$2
    local objname=${3:-SOMETHING}


    for marker in FFFF GGGG HHHH IIII ; do
        printf "%*s" 1024 $marker
    done > $dir/ORIGINAL

    #
    # get and put an object, compare they are equal
    #
    rados --pool $poolname put $objname $dir/ORIGINAL || return 1
    rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    rm $dir/COPY

    #
    # take out two OSDs used to store the object, wait for the cluster
    # to be clean (i.e. all PG are clean and active) again which
    # implies the PG have been moved to use the remaining OSDs.  Check
    # the object can still be retrieved.
    #
    wait_for_clean || return 1
    local osds_list=$(get_osds $poolname $objname)
    local -a osds=($osds_list)
    for osd in 0 1 ; do
      ceph osd out ${osds[$osd]} || return 1
    done
    wait_for_clean || return 1
    #
    # verify the object is no longer mapped to the osds that are out
    #
    for osd in 0 1 ; do
        ! get_osds $poolname $objname | grep '\<'${osds[$osd]}'\>' || return 1
    done
    rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    #
    # bring the osds back in, , wait for the cluster
    # to be clean (i.e. all PG are clean and active) again which
    # implies the PG go back to using the same osds as before
    #
    for osd in 0 1 ; do
      ceph osd in ${osds[$osd]} || return 1
    done
    wait_for_clean || return 1
    test "$osds_list" = "$(get_osds $poolname $objname)" || return 1
    rm $dir/ORIGINAL
}

function TEST_rados_put_get_lrc_advanced() {
    local dir=$1
    local poolname=pool-lrc-a
    local profile=profile-lrc-a

    ceph osd erasure-code-profile set $profile \
        plugin=lrc \
        mapping=DD_ \
        ruleset-steps='[ [ "chooseleaf", "osd", 0 ] ]' \
        layers='[ [ "DDc", "" ] ]'  || return 1
    ceph osd pool create $poolname 12 12 erasure $profile \
        || return 1

    rados_put_get $dir $poolname || return 1

    delete_pool $poolname
    ceph osd erasure-code-profile rm $profile
}

function TEST_rados_put_get_lrc_kml() {
    local dir=$1
    local poolname=pool-lrc
    local profile=profile-lrc

    ceph osd erasure-code-profile set $profile \
        plugin=lrc \
        k=4 m=2 l=3 \
        ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 12 12 erasure $profile \
        || return 1

    rados_put_get $dir $poolname || return 1

    delete_pool $poolname
    ceph osd erasure-code-profile rm $profile
}

function TEST_rados_put_get_isa() {
    if ! erasure_code_plugin_exists isa ; then
        echo "SKIP because plugin isa has not been built"
        return 0
    fi
    local dir=$1
    local poolname=pool-isa

    ceph osd erasure-code-profile set profile-isa \
        plugin=isa \
        ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 1 1 erasure profile-isa \
        || return 1

    rados_put_get $dir $poolname || return 1

    delete_pool $poolname
}

function TEST_rados_put_get_jerasure() {
    local dir=$1

    rados_put_get $dir ecpool || return 1

    local poolname=pool-jerasure
    local profile=profile-jerasure

    ceph osd erasure-code-profile set $profile \
        plugin=jerasure \
        k=4 m=2 \
        ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 12 12 erasure $profile \
        || return 1

    rados_put_get $dir $poolname || return 1
    rados_osds_out_in $dir $poolname || return 1

    delete_pool $poolname
    ceph osd erasure-code-profile rm $profile
}

function TEST_rados_put_get_shec() {
    local dir=$1

    local poolname=pool-shec
    local profile=profile-shec

    ceph osd erasure-code-profile set $profile \
        plugin=shec \
        k=2 m=1 c=1 \
        ruleset-failure-domain=osd || return 1
    ceph osd pool create $poolname 12 12 erasure $profile \
        || return 1

    rados_put_get $dir $poolname || return 1

    delete_pool $poolname
    ceph osd erasure-code-profile rm $profile
}

function TEST_alignment_constraints() {
    local payload=ABC
    echo "$payload" > $dir/ORIGINAL
    # 
    # Verify that the rados command enforces alignment constraints
    # imposed by the stripe width
    # See http://tracker.ceph.com/issues/8622
    #
    local stripe_width=$(ceph-conf --show-config-value osd_pool_erasure_code_stripe_width)
    local block_size=$((stripe_width - 1))
    dd if=/dev/zero of=$dir/ORIGINAL bs=$block_size count=2
    rados --block-size=$block_size \
        --pool ecpool put UNALIGNED $dir/ORIGINAL || return 1
    rm $dir/ORIGINAL
}

function chunk_size() {
    local stripe_width=$(ceph-conf --show-config-value osd_pool_erasure_code_stripe_width)
    eval local $(ceph osd erasure-code-profile get default | grep k=)
    echo $(($stripe_width / $k))
}

#
# By default an object will be split in two (k=2) with the first part
# of the object in the first OSD of the up set and the second part in
# the next OSD in the up set. This layout is defined by the mapping
# parameter and this function helps verify that the first and second
# part of the object are located in the OSD where they should be.
#
function verify_chunk_mapping() {
    local dir=$1
    local poolname=$2
    local first=$3
    local second=$4

    local payload=$(printf '%*s' $(chunk_size) FIRST$poolname ; printf '%*s' $(chunk_size) SECOND$poolname)
    echo -n "$payload" > $dir/ORIGINAL

    rados --pool $poolname put SOMETHING$poolname $dir/ORIGINAL || return 1
    rados --pool $poolname get SOMETHING$poolname $dir/COPY || return 1
    local -a osds=($(get_osds $poolname SOMETHING$poolname))
    for (( i = 0; i < ${#osds[@]}; i++ )) ; do
        ceph daemon osd.${osds[$i]} flush_journal
    done
    diff $dir/ORIGINAL $dir/COPY || return 1
    rm $dir/COPY

    local -a osds=($(get_osds $poolname SOMETHING$poolname))
    grep --quiet --recursive --text FIRST$poolname $dir/${osds[$first]} || return 1
    grep --quiet --recursive --text SECOND$poolname $dir/${osds[$second]} || return 1
}

function TEST_chunk_mapping() {
    local dir=$1

    #
    # mapping=DD_ is the default:
    #  first OSD (i.e. 0) in the up set has the first part of the object
    #  second OSD (i.e. 1) in the up set has the second part of the object
    #
    verify_chunk_mapping $dir ecpool 0 1 || return 1

    ceph osd erasure-code-profile set remap-profile \
        plugin=lrc \
        layers='[ [ "_DD", "" ] ]' \
        mapping='_DD' \
        ruleset-steps='[ [ "choose", "osd", 0 ] ]' || return 1
    ceph osd erasure-code-profile get remap-profile
    ceph osd pool create remap-pool 12 12 erasure remap-profile \
        || return 1

    #
    # mapping=_DD
    #  second OSD (i.e. 1) in the up set has the first part of the object
    #  third OSD (i.e. 2) in the up set has the second part of the object
    #
    verify_chunk_mapping $dir remap-pool 1 2 || return 1

    delete_pool remap-pool
    ceph osd erasure-code-profile rm remap-profile
}

main test-erasure-code "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/erasure-code/test-erasure-code.sh"
# End:
