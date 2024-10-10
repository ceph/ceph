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
    flush_pg_stats

    local global_avail=`ceph df -f json | jq '.stats.total_avail_bytes'`
    local rep_avail=`ceph df -f json | jq '.pools | map(select(.name == "'$rep_poolname'"))[0].stats.max_avail'`
    local ec_avail=`ceph df -f json | jq '.pools | map(select(.name == "'$ec_poolname'"))[0].stats.max_avail'`

    echo "${global_avail} >= ${rep_avail}*3" | bc || return 1
    echo "${global_avail} >= ${ec_avail}*1.5" | bc || return 1

    ceph osd pool delete  $rep_poolname $rep_poolname  --yes-i-really-really-mean-it
    ceph osd pool delete  $ec_poolname $ec_poolname  --yes-i-really-really-mean-it
    ceph osd erasure-code-profile rm ec42profile
    teardown $dir || return 1
}

function TEST_stretched_cluster_ceph_df() {
    local dir=$1
    local OSDS=12
    local nodes=6
    setup $dir || return 1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    export CEPH_ARGS

    for osd in $(seq 0 $(expr $OSDS - 1))
    do
      run_osd $dir $osd || return 1
    done

    for dc in DC1 DC2
    do
      ceph osd crush add-bucket $dc datacenter
      ceph osd crush move $dc root=default
    done

    for node in $(seq 0 $(expr $nodes - 1))
    do
      ceph osd crush add-bucket osd-$node host
    done

    for p in $(seq  0 2)
    do
      ceph osd crush move osd-$p datacenter=DC1
    done

    for q in $(seq  3 5)
    do
      ceph osd crush move osd-$q datacenter=DC2
    done

    ceph osd crush move osd.0 host=osd-0
    ceph osd crush move osd.3 host=osd-0
    ceph osd crush move osd.1 host=osd-1
    ceph osd crush move osd.2 host=osd-1
    ceph osd crush move osd.7 host=osd-2
    ceph osd crush move osd.11 host=osd-2
    ceph osd crush move osd.4 host=osd-3
    ceph osd crush move osd.8 host=osd-3
    ceph osd crush move osd.5 host=osd-4
    ceph osd crush move osd.9 host=osd-4
    ceph osd crush move osd.6 host=osd-5
    ceph osd crush move osd.10 host=osd-5

    hostname=$(hostname -s)
    ceph osd crush remove $hostname

    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    sed 's/^# end crush map$//' $dir/crushmap.txt > $dir/crushmap_modified.txt || return 1
    cat >> $dir/crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        min_size 1
        max_size 10
        step take DC1
        step chooseleaf firstn 2 type host
        step emit
        step take DC2
        step chooseleaf firstn 2 type host
        step emit
}

rule stretch_replicated_rule {
        id 2
        type replicated
        min_size 1
        max_size 10
        step take default
        step choose firstn 0 type datacenter
        step chooseleaf firstn 2 type host
        step emit
}

# end crush map
EOF

    crushtool --compile $dir/crushmap_modified.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin  || return 1

    ceph osd crush rule ls

    local stretched_poolname=stretched_rbdpool
    local stretched_rep_poolname=stretched_replicated_rbdpool
    ceph osd pool create $stretched_poolname 32 32 stretch_rule
    ceph osd pool create $stretched_rep_poolname 32 32 stretch_replicated_rule
    ceph osd pool set $stretched_poolname size 4
    ceph osd pool set $stretched_rep_poolname size 4
    wait_for_clean || return 1

    local global_avail=`ceph df -f json | jq '.stats.total_avail_bytes'`
    local stretched_avail=`ceph df -f json | jq '.pools | map(select(.name == "'$stretched_poolname'"))[0].stats.max_avail'`
    local stretched_rep_avail=`ceph df -f json | jq '.pools | map(select(.name == "'$stretched_rep_poolname'"))[0].stats.max_avail'`

    test ${stretched_avail} == ${stretched_rep_avail} || return 1

    ceph osd pool delete  $stretched_poolname $stretched_poolname  --yes-i-really-really-mean-it
    ceph osd pool delete  $stretched_rep_poolname $stretched_rep_poolname  --yes-i-really-really-mean-it
    teardown $dir || return 1
}

main osd-pool-df "$@"
