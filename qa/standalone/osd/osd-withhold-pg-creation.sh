#!/usr/bin/env bash
#
# Copyright (C) 2022 Red Hat <contact@redhat.com>
#
# Author: Prashant D <pdhange@redhat.com>
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

    export CEPH_MON="127.0.0.1:7155" # git grep '\<7155\>' : there must be only one
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

function TEST_withhold_pg_creation() {
    local dir=$1
    local WAIT_BEFORE_RESTART=60

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    for node in `seq 0 5`; do
      ceph osd crush add-bucket osd-node-$node host
    done

    for node in `seq 0 5`; do
      ceph osd crush move osd-node-$node root=default
    done

    for osd in `seq 0 5`; do
      ceph osd crush move osd.$osd host=osd-node-$osd
    done

    host_bucket=$(hostname -s)
    ceph osd crush remove $host_bucket

    ceph osd erasure-code-profile set myprofile k=4 m=2 crush-failure-domain=host
    ceph osd pool set noautoscale
    ceph config set mon mon_max_pg_per_osd 2000
    ceph osd pool create ecpool 512 512 erasure myprofile
    ceph osd pool create ecpool1 128 128 erasure myprofile
    ceph osd pool create ecpool2 512 512 erasure myprofile
    ceph osd pool create ecpool3 512 512 erasure myprofile
    ceph osd pool create rbd 32 32
    sleep 30
    ERRORS=0
    if ! ceph status 2>&1 | grep -q "PG creation pending"; then
      ERRORS="$(expr $ERRORS + 1)"
    fi

    for osd in  `seq 0 5`; do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) config set osd_max_pg_per_osd_hard_ratio 10
    done
    WAIT_FOR_CLEAN_TIMEOUT=60 wait_for_clean

    for osd in  `seq 0 5`; do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) config set osd_max_pg_per_osd_hard_ratio 1
    done

    ceph osd out 3
    kill_daemons $dir TERM osd.3 || return 1
    activate_osd $dir 3 || return 1
    if ! ceph status 2>&1 | grep -q "PG creation pending"; then
      ERRORS="$(expr $ERRORS + 1)"
    fi
    ceph osd in 3
    ceph osd crush move osd.3 host=osd-node-3
    ceph osd crush remove $host_bucket

    for osd in  `seq 0 5`; do
      CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.${osd}) config set osd_max_pg_per_osd_hard_ratio 10 
    done
 
    WAIT_FOR_CLEAN_TIMEOUT=60 wait_for_clean

    if [ $ERRORS -gt 0 ]; then
        echo "TEST FAILED WITH $ERRORS ERRORS"
        return 1
    fi

    echo "TEST PASSED"
    return 0
}

main osd-withhold-pg-creation "$@"
