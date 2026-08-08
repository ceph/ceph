#!/usr/bin/env bash
#
# Copyright (C) 2026 Clyso GmbH <contact@clyso.com>
#
# Author: Frédéric Nass <frederic.nass@clyso.com>
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

    export CEPH_MON="127.0.0.1:7134" # git grep '\<7134\>' : there must be only one
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

function TEST_bluestore_mixed_min_alloc_size() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 --bluestore-min-alloc-size=4096 || return 1
    run_osd $dir 1 --bluestore-min-alloc-size=4096 || return 1

    # force an osdmap change so that the health checks are recomputed
    # from the latest committed OSD metadata
    ceph osd pool create foo 8 || return 1
    wait_for_clean || return 1
    ceph health detail
    ! ceph health detail | grep BLUESTORE_MIXED_MIN_ALLOC_SIZE || return 1

    # add an OSD with a different (legacy) allocation unit within the
    # same device class
    run_osd $dir 2 --bluestore-min-alloc-size=65536 || return 1
    ceph osd pool set foo pg_num 16 || return 1
    wait_for_health "BLUESTORE_MIXED_MIN_ALLOC_SIZE" || return 1
    ceph health detail | grep "use min_alloc_size 65536" || return 1

    # the warning can be disabled at runtime
    ceph config set mon mon_warn_on_mixed_min_alloc_size false || return 1
    ceph osd pool set foo pg_num 32 || return 1
    wait_for_health_gone "BLUESTORE_MIXED_MIN_ALLOC_SIZE" || return 1
}

main mon-bluestore-mixed-min-alloc-size "$@"
