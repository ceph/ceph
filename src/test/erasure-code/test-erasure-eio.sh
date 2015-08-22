#!/bin/bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
#
# Author: Kefu Chai <kchai@redhat.com>
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

source ../qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7112"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        run_mon $dir a || return 1
        # check that erasure code plugins are preloaded
        CEPH_ARGS='' ./ceph --admin-daemon $dir/ceph-mon.a.asok log flush || return 1
        grep 'load: jerasure.*lrc' $dir/mon.a.log || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function setup_osds() {
    local subread=$1

    for id in $(seq 0 3) ; do
        # TODO: the feature of "osd-pool-erasure-code-subread-all" is not yet supported.
        if -n osd_pool_erasure_code_subread_all__is_supported; then
            run_osd $dir $id "--osd-pool-erasure-code-subread-all=$subread" || return 1
        else
            run_osd $dir $id || return 1
        fi
    done
    wait_for_clean || return 1

    # check that erasure code plugins are preloaded
    CEPH_ARGS='' ./ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'load: jerasure.*lrc' $dir/osd.0.log || return 1
}

function create_erasure_coded_pool() {
    local poolname=$1

    ./ceph osd erasure-code-profile set myprofile \
        plugin=jerasure \
        k=2 m=1 \
        ruleset-failure-domain=osd || return 1
    ./ceph osd pool create $poolname 1 1 erasure myprofile \
        || return 1
    wait_for_clean || return 1
}

function delete_pool() {
    local poolname=$1

    ./ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it
    ./ceph osd erasure-code-profile rm myprofile
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
    ./rados --pool $poolname put $objname $dir/ORIGINAL || return 1
    ./rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    rm $dir/COPY
    #
    # take out the first OSD used to store the object and
    # check the object can still be retrieved, which implies
    # recovery
    #
    local -a initial_osds=($(get_osds $poolname $objname))
    local last=$((${#initial_osds[@]} - 1))
    ./ceph osd out ${initial_osds[$last]} || return 1
    ! get_osds $poolname $objname | grep '\<'${initial_osds[$last]}'\>' || return 1
    ./rados --pool $poolname get $objname $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1
    ./ceph osd in ${initial_osds[$last]} || return 1

    rm $dir/ORIGINAL
}

function rados_get_data_eio() {
    local dir=$1
    shift
    local shard_id=$1
    shift
    local osd_state=$1
    shift

    # inject eio to speificied shard
    # OSD with eio injection will crash at reading object
    #
    local poolname=pool-jerasure
    local objname=obj-eio-$$-$shard_id
    local -a initial_osds=($(get_osds $poolname $objname))
    local osd_id=${initial_osds[$shard_id]}
    local last=$((${#initial_osds[@]} - 1))
    # set_config osd $osd_id filestore_debug_inject_read_err true || return 1
    set_config osd $osd_id filestore_debug_inject_read_err true || return 1
    CEPH_ARGS='' ./ceph --admin-daemon $dir/ceph-osd.$osd_id.asok \
             injectdataerr $poolname $objname $shard_id || return 1
    set_config osd $osd_id filestore_fail_eio false || return 1

    rados_put_get $dir $poolname $objname || return 1
    TIMEOUT=1 wait_for_osd $osd_state $osd_id || return 1
}

#
# These two test cases try to validate the following behavior:
#  For object on EC pool, if there is one shard having read error (
#  either primary or replica), it will trigger OSD crash.
#
function TEST_rados_get_without_subreadall_eio_shard_0() {
    local dir=$1
    setup_osds false || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname || return 1
    # inject eio on primary OSD (0)
    local shard_id=0
    rados_get_data_eio $dir $shard_id down || return 1
    delete_pool $poolname
}

function TEST_rados_get_without_subreadall_eio_shard_1() {
    local dir=$1
    setup_osds false || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname || return 1
    # inject eio into replica OSD (1)
    local shard_id=1
    rados_get_data_eio $dir $shard_id down || return 1
    delete_pool $poolname
}


: <<'DISABLED_TESTS'
# this test case is aimed to test the fix of https://github.com/ceph/ceph/pull/2952
# this test case can test both client read and recovery read on EIO
# but at this moment, above pull request ONLY resolves client read on EIO
# so this case will fail at function *rados_put_get* when one OSD out
# so disable this case for now until both crashes of client read and recovery read
# on EIO to be fixed
#

function TEST_rados_get_with_subreadall_eio_shard_0() {
    local dir=$1
    local shard_id=0

    setup_osds true || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname || return 1
    # inject eio on primary OSD (0)
    local shard_id=0
    rados_get_data_eio $dir $shard_id up || return 1

    check_pg_status $pg "inconsistent" || return 1
    delete_pool $poolname
}

function TEST_rados_get_with_subreadall_eio_shard_1() {
    local dir=$1
    local shard_id=0

    setup_osds true || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname || return 1
    # inject eio on replica OSD (1)
    local shard_id=1
    rados_get_data_eio $dir $shard_id up || return 1

    # the reason to skip this check when current shardid != 0 is that the first
    # k chunks returned is not always containing current shardid, so this pg may
    # not be marked as inconsistent. However, primary OSD (when shard_id == 0) is
    # always the faster one normally, so we can check pg status.
    ## check_pg_status $pg "inconsistent" || return 1
    delete_pool $poolname
}
DISABLED_TESTS

main test-erasure-eio "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/erasure-code/test-erasure-eio.sh"
# End:
