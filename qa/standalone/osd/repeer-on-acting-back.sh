#!/usr/bin/env bash
#
# Copyright (C) 2020  ZTE Corporation <contact@zte.com.cn>
#
# Author: xie xingguo <xie.xingguo@zte.com.cn>
# Author: Yan Jun <yan.jun8@zte.com.cn>
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

    export poolname=test
    export testobjects=100
    export loglen=12
    export trim=$(expr $loglen / 2)
    export CEPH_MON="127.0.0.1:7115" # git grep '\<7115\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    # so we will not force auth_log_shard to be acting_primary
    CEPH_ARGS+="--osd_force_auth_primary_missing_objects=1000000 "
    # use small pg_log settings, so we always do backfill instead of recovery
    CEPH_ARGS+="--osd_min_pg_log_entries=$loglen --osd_max_pg_log_entries=$loglen --osd_pg_log_trim_min=$trim "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}


function TEST_repeer_on_down_acting_member_coming_back() {
    local dir=$1
    local dummyfile='/etc/fstab'

    local num_osds=6
    local osds="$(seq 0 $(expr $num_osds - 1))"
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for i in $osds
    do
      run_osd $dir $i || return 1
    done

    create_pool $poolname 1 1
    ceph osd pool set $poolname size 3
    ceph osd pool set $poolname min_size 2
    local poolid=$(ceph pg dump pools -f json | jq '.pool_stats' | jq '.[].poolid')
    local pgid=$poolid.0

    # enable required feature-bits for upmap
    ceph osd set-require-min-compat-client luminous
    # reset up to [1,2,3]
    ceph osd pg-upmap $pgid 1 2 3 || return 1

    flush_pg_stats || return 1
    wait_for_clean || return 1

    echo "writing initial objects"
    # write a bunch of objects
    for i in $(seq 1 $testobjects)
    do
      rados -p $poolname put existing_$i $dummyfile
    done

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    # reset up to [1,4,5]
    ceph osd pg-upmap $pgid 1 4 5 || return 1

    # wait for peering to complete
    sleep 2

    # make sure osd.2 belongs to current acting set
    ceph pg $pgid query | jq '.acting' | grep 2 || return 1

    # kill osd.2
    kill_daemons $dir KILL osd.2 || return 1
    ceph osd down osd.2

    # again, wait for peering to complete
    sleep 2

    # osd.2 should have been moved out from acting set
    ceph pg $pgid query | jq '.acting' | grep 2 && return 1

    # bring up osd.2
    activate_osd $dir 2 || return 1
    wait_for_osd up 2

    # again, wait for peering to complete
    sleep 2

    # primary should be able to re-add osd.2 into acting
    ceph pg $pgid query | jq '.acting' | grep 2 || return 1

    WAIT_FOR_CLEAN_TIMEOUT=20 wait_for_clean

    if ! grep -q "Active: got notify from previous acting member.*, requesting pg_temp change" $(find $dir -name '*osd*log')
    then
            echo failure
            return 1
    fi
    echo "success"

    delete_pool $poolname
    kill_daemons $dir || return 1
}

main repeer-on-acting-back "$@"

# Local Variables:
# compile-command: "make -j4 && ../qa/run-standalone.sh repeer-on-acting-back.sh"
# End:
