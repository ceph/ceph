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

    export CEPH_MON="127.0.0.1:7108" # git grep '\<7108\>' : there must be only one
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

function TEST_health_detail_check_truncate() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    run_osd $dir 4 || return 1
    run_osd $dir 5 || return 1

    ceph osd erasure-code-profile set ec-profile m=2 k=2 crush-failure-domain=osd
    ceph osd pool create ec 256 256 erasure ec-profile
    wait_for_clean || 1
    ceph config set mon mon_health_detail_check_message_max_bytes 1024
    ceph config set mon mon_health_to_clog_interval 20

    ceph osd set noup
    ceph osd down 0
    ceph osd down 1
    ceph osd down 2
    ceph osd down 3
    ceph osd down 4
    ceph osd down 5
    sleep 60
  
    test "$(grep "<truncated>" $dir/log | wc -l)" -ge "1" || return 1
}

main mon-health-detail "$@"
