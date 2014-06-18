#!/bin/bash
#
# Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

source test/mon/mon-test-helpers.sh
source test/osd/osd-test-helpers.sh

function run() {
    local dir=$1

    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=127.0.0.1 "

    local id=a
    call_TEST_functions $dir $id --public-addr 127.0.0.1 || return 1
}

function TEST_config_init() {
    local dir=$1

    run_mon $dir a --public-addr 127.0.0.1 \
        || return 1
    local advance=1000
    local stale=1000
    local cache=500
    run_osd $dir 0 \
        --osd-map-max-advance $advance \
        --osd-map-cache-size $cache \
        --osd-pg-epoch-persisted-max-stale $stale \
        || return 1
    CEPH_ARGS='' ./ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'is not > osd_map_max_advance' $dir/osd-0.log || return 1
    grep 'is not > osd_pg_epoch_persisted_max_stale' $dir/osd-0.log || return 1
}

function TEST_config_track() {
    local dir=$1

    run_mon $dir a --public-addr 127.0.0.1 \
        || return 1
    run_osd $dir 0 || return 1

    ! grep 'is not > osd_map_max_advance' $dir/osd-0.log || return 1
    local advance=1000
    local cache=500
    ./ceph tell osd.0 injectargs "--osd-map-cache-size $cache" || return 1
    ./ceph tell osd.0 injectargs "--osd-map-max-advance $advance" || return 1
    CEPH_ARGS='' ./ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'is not > osd_map_max_advance' $dir/osd-0.log || return 1

    ! grep 'is not > osd_pg_epoch_persisted_max_stale' $dir/osd-0.log || return 1
    local stale=1000
    ceph tell osd.0 injectargs "--osd-pg-epoch-persisted-max-stale $stale" || return 1
    CEPH_ARGS='' ./ceph --admin-daemon $dir/ceph-osd.0.asok log flush || return 1
    grep 'is not > osd_pg_epoch_persisted_max_stale' $dir/osd-0.log || return 1
}

main osd-config

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-config.sh"
# End:
