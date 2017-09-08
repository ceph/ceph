#!/usr/bin/env bash
#
# Copyright (C) 2017 Tencent <contact@tencent.com>
#
# Author: Chang Liu <liuchang0812@gmail.com>
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

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7113" # git grep '\<7113\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_ceph_df() {
    local dir=$1
    setup $dir || return 1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1
    run_mgr $dir x || return 1

    profile+=" plugin=jerasure"
    profile+=" technique=reed_sol_van"
    profile+=" k=4"
    profile+=" m=2"
    profile+=" crush-failure-domain=osd"

    ceph osd erasure-code-profile set ec42profile ${profile}
 
    local rep_poolname=testcephdf_replicate
    local ec_poolname=testcephdf_erasurecode
    create_pool $rep_poolname 6 6 replicated
    create_pool $ec_poolname 6 6 erasure ec42profile

    local global_avail=`ceph df -f json | jq '.stats.total_avail_bytes'`
    local rep_avail=`ceph df -f json | jq '.pools | map(select(.name == "$rep_poolname"))[0].stats.max_avail'`
    local ec_avail=`ceph df -f json | jq '.pools | map(select(.name == "$ec_poolname"))[0].stats.max_avail'`

    echo "${global_avail} >= ${rep_avail}*3" | bc || return 1
    echo "${global_avail} >= ${ec_avail}*1.5" | bc || return 1

    ceph osd pool delete  $rep_poolname $rep_poolname  --yes-i-really-really-mean-it
    ceph osd pool delete  $ec_poolname $ec_poolname  --yes-i-really-really-mean-it
    ceph osd erasure-code-profile rm ec42profile
    teardown $dir || return 1
}

main osd-pool-df "$@"
